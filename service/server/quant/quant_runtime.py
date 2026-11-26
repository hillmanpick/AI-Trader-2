"""Paper-only deterministic scanning; never treats an LLM opinion as evidence."""
import json
import time

from .execution_policy import fingerprint
from .quant_strategy import VERSION, STRATEGIES, GRID_MS, COOLDOWN_MS, policy_for_quant, signal
from .quant_research import artifact_status
from .store import utcnow

EXPLORATION_STRATEGY = 'flow_momentum'


class QuantController:
    def __init__(self, engine, lab):
        self.engine, self.lab = engine, lab
        self.policy = policy_for_quant(engine.cfg)
        self.exploration = bool(engine.cfg.paper_quant_exploration)
        self.status, self.last_artifact = {}, 0
        self.report = {}
        self.last_grid = {}
        self.last_persisted_bucket = {}
        self.last_recent_refresh = 0.
        self.last_status_store = 0.
        with engine.store.connect() as c:
            c.execute('CREATE TABLE IF NOT EXISTS quant_decisions '
                      '(symbol TEXT, ts INTEGER, body TEXT NOT NULL, PRIMARY KEY(symbol,ts))')
        self.last_prune = 0

    def refresh(self):
        now = time.time()
        if now - self.last_artifact >= 30:
            self.last_artifact = now
            self.status = artifact_status(self.lab.cfg.directory, self.policy)
            self.report = self.status.get('report', {})
            if not self.status.get('ready') and self.exploration:
                self.status = self.status | {
                    'ready': True, 'approved': False, 'paper_qualified': False,
                    'exploration': True, 'entry_mode': 'bounded_forward_trial',
                    'trial_strategy': EXPLORATION_STRATEGY,
                }
        if now-self.last_status_store >= 5:
            self.last_status_store = now
        if now-self.last_recent_refresh >= 5:
            self.last_recent_refresh = now
            self.engine.store.set('quant_system', {
                **{k: v for k, v in self.status.items() if k != 'report'},
                'enabled': True, 'at': utcnow(), 'version': VERSION,
                'grid_ms': self.policy['grid_ms'],
                'horizon_ms': self.policy['horizon_ms'],
                'cooldown_ms': self.policy['cooldown_ms'],
                'selected': self.report.get('selected'), 'experiment_id': self.report.get('experiment_id'),
                'entry_mode': self.status.get('entry_mode', 'validated_systematic' if self.status.get('ready') else 'blocked'),
                'trial_strategy': self.status.get('trial_strategy'),
                'qualification': self.report.get('qualification'),
                'validation': self.report.get('validation', {}),
                'folds': self.report.get('folds', []), 'oos': self.report.get('oos', {}),
                'coverage': self.report.get('coverage', {}),
                'confidence': self.report.get('confidence', {}),
                'limitations': self.report.get('limitations', []),
                'capital': self.policy['capital'],
                'paper_unrestricted_positions': self.engine.cfg.paper_unrestricted_positions,
                'risk_per_trade_usdt': self.policy['capital'] * self.policy['risk_per_trade'],
                'token_requests': 0, 'profitability_proven': False,
                'real_execution_enabled': False,
            })
            with self.engine.store.connect() as c:
                rows = c.execute('SELECT body FROM quant_decisions ORDER BY ts DESC LIMIT 20').fetchall()
            self.engine.store.set('quant_recent_decisions', [json.loads(row[0]) for row in rows])
        if now - self.last_prune > 3600:
            self.last_prune = now
            with self.engine.store.connect() as c:
                c.execute('DELETE FROM quant_decisions WHERE ts<?', (int(now * 1000) - 7 * 86400000,))
                c.execute('DELETE FROM quant_decisions WHERE rowid IN '
                          '(SELECT rowid FROM quant_decisions ORDER BY ts DESC LIMIT -1 OFFSET 50000)')
        return {k: v for k, v in self.status.items() if k != 'report'}

    def signal(self, symbol):
        e = self.engine
        if symbol in e.store.get('positions', {}):
            return 'hold'  # Frozen local barriers manage exits, even without an artifact.
        self.refresh()
        try:
            row = self.lab.row(symbol)
        except (ValueError, KeyError, TypeError):
            return 'hold'
        grid = row['ts'] // GRID_MS
        key = 'quant_grid:' + symbol
        if self.last_grid.get(symbol) == grid:
            return 'hold'
        self.last_grid[symbol] = grid
        reader = getattr(self, 'compute_reader', None)
        observations = (reader.observations(row, self.policy, STRATEGIES, signal) if reader
                        else {n: signal(n, row, self.policy) for n in STRATEGIES})
        if reader and time.monotonic() >= reader._next_status:
            e.store.set('hybrid_compute', reader.status())
            reader._next_status = time.monotonic()+2
        selected = self.report.get('selected')
        trial = not self.status.get('paper_qualified', False) and self.exploration
        if trial:
            selected = EXPLORATION_STRATEGY
        choice = observations.get(selected, {'action': 'hold', 'reason': '没有通过验证的策略'})
        action = choice['action']
        reason = choice['reason']
        positions = e.store.get('positions', {})
        if not self.status.get('ready'):
            action, reason = 'hold', self.status.get('reason', '研究门槛未通过')
        elif action in ('long', 'short'):
            if not self.lab.gate(symbol):
                action, reason = 'hold', '实时币池、特征或采集状态过期'
            elif time.time() * 1000 < e.store.get('quant_cooldown:' + symbol, 0):
                action, reason = 'hold', '平仓后冷却，避免反复支付成本'
            else:
                journal = getattr(getattr(e, 'paper_execution', None), 'journal', None)
                pending_entry = any(not order.get('closing') for order in journal.orders(active=True)) if journal else False
                if pending_entry:
                    action, reason = 'hold', '模拟开仓订单仍在撮合，等待成交或过期'
                else:
                    side = 1 if action == 'long' else -1
                    limit = self.policy.get('same_direction_limit')
                    if limit is not None and sum((p['amount'] > 0) == (side > 0) for p in positions.values()) >= limit:
                        action, reason = 'hold', '同向组合集中度已达上限'
        body = choice | {'action': action, 'reason': reason, 'at': utcnow(),
                         'symbol': symbol,
                         'feature_ts': row['ts'], 'source': VERSION,
                         'experiment_id': self.report.get('experiment_id'),
                         'hypotheses': observations, 'price': row['price'],
                         'entry_mode': 'bounded_forward_trial' if trial else 'validated_systematic',
                         'paper_qualified': not trial, 'exploration': trial,
                         'decision_latency_ms': max(0, int(time.time()*1000)-row['ts'])}
        # Persist at most once per wall-clock second for subsecond grids; a
        # coarser grid persists once per feature grid. Avoid a zero divisor
        # when the feature interval exceeds one second.
        bucket_width = max(1,1000//GRID_MS)
        durable_bucket = grid//bucket_width*bucket_width
        if symbol not in self.last_persisted_bucket:
            self.last_persisted_bucket[symbol] = e.store.get(key, -1)
        durable_grid = self.last_persisted_bucket[symbol]
        persist = action in ('long','short') or durable_grid != durable_bucket
        if persist:
            e.store.set(key, durable_bucket)
            self.last_persisted_bucket[symbol] = durable_bucket
            e.store.set('signal:' + symbol, body)
            record_decision = getattr(e.store,'record_decision',None)
            if record_decision:
                record_decision(body)
            else:
                with e.store.connect() as c:
                    c.execute('INSERT OR IGNORE INTO quant_decisions VALUES (?,?,?)',
                              (symbol, row['ts'], json.dumps(body, ensure_ascii=False, allow_nan=False)))
        if action in ('long', 'short'):
            e.store.set('entry_context:' + symbol, {
                'quant_version': VERSION, 'quant_strategy': selected,
                'experiment_id': self.report.get('experiment_id'),
                'quant_policy_hash': fingerprint(self.policy),
                'feature_ts': row['ts'], 'signal_evidence': choice.get('evidence'),
                'model_version': VERSION + ':' + selected,
                'model_approved': False, 'paper_qualified': not trial, 'exploration': trial,
                'regime': VERSION, 'strategy': 'quant_system',
            })
        return action
