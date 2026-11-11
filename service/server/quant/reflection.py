"""Paper-only, auditable strategy feedback and bounded adaptation.

The reflector never sends exchange requests and never changes leverage,
margin, symbols, or live execution flags. It records closed trades, separates
gross edge from execution costs, and promotes only a small deterministic
candidate after enough evidence is available.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone


def _number(value, default=0.0):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if value == value and abs(value) != float('inf') else default


def _ratio(numerator, denominator):
    return numerator / denominator if denominator else 0.0


class StrategyReflector:
    """Keep a bounded trade journal and create paper-only strategy versions."""

    def __init__(self, store, cfg):
        self.store = store
        self.cfg = cfg

    def active(self):
        return self.store.get('reflection:active', {
            'version': 1,
            'overrides': {},
            'reason': '初始策略版本，等待足够的已平仓样本',
            'updated_at': None,
        })

    def apply(self, cfg):
        """Apply only a previously promoted paper override."""
        if not getattr(cfg, 'reflection_enabled', False) or cfg.mode != 'paper':
            return cfg
        active = self.active()
        overrides = active.get('overrides', {})
        if overrides.get('paper_entry_filter') in {'shadow', 'enforce'}:
            from dataclasses import replace
            return replace(cfg, paper_entry_filter=overrides['paper_entry_filter'])
        return cfg

    def allows(self, symbol):
        """Temporarily cool down symbols with repeated net losses in paper mode."""
        if not getattr(self.cfg, 'reflection_enabled', False) or self.cfg.mode != 'paper':
            return True
        blocked_until = self.active().get('overrides', {}).get('blocked_until', {}).get(symbol)
        return not self._is_blocked(blocked_until)

    @staticmethod
    def _is_blocked(blocked_until):
        if not blocked_until:
            return False
        try:
            expiry = datetime.fromisoformat(blocked_until)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return expiry > datetime.now(timezone.utc)
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _report(trades):
        trades = list(trades)
        net = sum(_number(t.get('net_pnl')) for t in trades)
        gross = sum(_number(t.get('gross_pnl')) for t in trades)
        gross_movement = sum(abs(_number(t.get('gross_pnl'))) for t in trades)
        costs = sum(_number(t.get('cost_usdt')) for t in trades)
        wins = [max(_number(t.get('net_pnl')), 0) for t in trades]
        losses = [-min(_number(t.get('net_pnl')), 0) for t in trades]
        by_symbol = defaultdict(lambda: {'trades': 0, 'net_pnl': 0.0, 'wins': 0})
        by_regime = defaultdict(lambda: {'trades': 0, 'net_pnl': 0.0, 'wins': 0})
        for trade in trades:
            for bucket, key in ((by_symbol, trade.get('symbol', 'unknown')),
                                (by_regime, trade.get('regime', 'unknown'))):
                bucket[key]['trades'] += 1
                bucket[key]['net_pnl'] += _number(trade.get('net_pnl'))
                bucket[key]['wins'] += _number(trade.get('net_pnl')) > 0
        for bucket in (by_symbol, by_regime):
            for value in bucket.values():
                value['net_pnl'] = round(value['net_pnl'], 4)
                value['win_rate'] = round(value['wins'] / value['trades'], 4) if value['trades'] else 0
                del value['wins']
        consecutive = 0
        worst_consecutive = 0
        for trade in trades:
            if _number(trade.get('net_pnl')) < 0:
                consecutive += 1
                worst_consecutive = max(worst_consecutive, consecutive)
            else:
                consecutive = 0
        return {
            'window_trades': len(trades),
            'net_pnl': round(net, 4),
            'gross_pnl': round(gross, 4),
            'cost_usdt': round(costs, 4),
            'cost_ratio': round(_ratio(costs, gross_movement), 4),
            'win_rate': round(sum(1 for v in wins if v > 0) / len(trades), 4) if trades else 0,
            'profit_factor': round(_ratio(sum(wins), sum(losses)), 4) if losses else None,
            'avg_hold_seconds': round(sum(_number(t.get('hold_seconds')) for t in trades) / len(trades), 2) if trades else 0,
            'max_consecutive_losses': worst_consecutive,
            'by_symbol': dict(sorted(by_symbol.items())),
            'by_regime': dict(sorted(by_regime.items())),
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }

    def _candidate(self, report, active):
        """Return a bounded candidate, or None when evidence is insufficient."""
        if report['window_trades'] < self.cfg.reflection_min_trades:
            return None
        overrides = active.get('overrides', {})
        current_blocks = overrides.get('blocked_until', {})
        blocked_until = {symbol: until for symbol, until in current_blocks.items()
                         if self._is_blocked(until)}
        weak_symbols = [symbol for symbol, stats in report['by_symbol'].items()
                        if stats['trades'] >= 6 and stats['net_pnl'] < 0 and stats['win_rate'] < .34]
        max_blocked = max(1, len(self.cfg.symbols) // 2)
        available_slots = max(0, max_blocked - len(blocked_until))
        eligible = [symbol for symbol in weak_symbols if symbol not in blocked_until]
        for symbol in sorted(eligible, key=lambda s: report['by_symbol'][s]['net_pnl'])[:available_slots]:
            blocked_until[symbol] = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
        if blocked_until != current_blocks:
            return {
                'version': int(active.get('version', 1)) + 1,
                'overrides': {**overrides, 'blocked_until': blocked_until},
                'reason': '滚动样本中连续亏损且胜率偏低的币种进入 12 小时冷却，冷却后自动恢复观察',
                'evidence': {'window_trades': report['window_trades'],
                             'blocked_symbols': sorted(blocked_until)},
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
        # When costs consume most of the gross movement, enforce the existing
        # deterministic entry-quality gate. This is reversible and paper-only.
        if (report['net_pnl'] < 0 and report['cost_ratio'] >= 0.5
                and overrides.get('paper_entry_filter') != 'enforce'):
            return {
                'version': int(active.get('version', 1)) + 1,
                'overrides': {**overrides, 'paper_entry_filter': 'enforce'},
                'reason': '最近窗口净亏损且手续费/滑点占毛波动至少 50%，启用成本过滤',
                'evidence': {'window_trades': report['window_trades'],
                             'net_pnl': report['net_pnl'], 'cost_ratio': report['cost_ratio']},
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
        return None

    def record_closed(self, trade):
        """Record one completed paper position and update the reflection state."""
        if not self.cfg.reflection_enabled or self.cfg.mode != 'paper':
            return self.store.get('reflection:status', {})
        trades = self.store.get('reflection:trades', [])
        if trade.get('fill_id') and any(t.get('fill_id')==trade['fill_id'] for t in trades):
            return self.store.get('reflection:status',{})
        trades.append(dict(trade))
        trades = trades[-max(self.cfg.reflection_window * 4, 200):]
        report = self._report(trades[-self.cfg.reflection_window:])
        active = self.active()
        trade_count = int(self.store.get('reflection:trade_count', 0)) + 1
        self.store.set('reflection:trade_count', trade_count)
        last_count = int(self.store.get('reflection:last_evaluated_count', 0))
        candidate = None
        if trade_count - last_count >= self.cfg.reflection_interval_trades:
            candidate = self._candidate(report, active)
            self.store.set('reflection:last_evaluated_count', trade_count)
        if candidate:
            self.store.set('reflection:active', candidate)
            self.store.event('策略反思',
                             f"候选版本 {candidate['version']} 已通过规则校验并进入模拟盘：{candidate['reason']}；"
                             f"窗口净盈亏={report['net_pnl']} USDT，成本占比={report['cost_ratio']:.2%}")
            active = candidate
        self.store.set('reflection:trades', trades)
        status = {'enabled': True, 'active': active, 'report': report,
                  'last_trade': dict(trade), 'candidate': candidate}
        self.store.set('reflection:status', status)
        return status

    def status(self):
        return self.store.get('reflection:status', {
            'enabled': bool(self.cfg.reflection_enabled),
            'active': self.active(), 'report': self._report([]),
        })

    def sync_completed(self, trades):
        """Refresh from complete durable position cycles, never depth-fill shards.

        Rebuild once on migration without promoting rules from legacy fill counts.
        Thereafter evaluate only after new complete cycles have accumulated.
        """
        if not self.cfg.reflection_enabled or self.cfg.mode != 'paper':
            return self.status()
        cursor = {'count':len(trades),'last':trades[-1]['cycle_id'] if trades else None}
        if self.store.get('reflection:cycle_cursor') == cursor:
            return self.status()
        migrating = self.store.get('reflection:cycle_cursor') is None
        active = self.active()
        if migrating and active.get('overrides',{}).get('blocked_until'):
            overrides = dict(active['overrides'])
            overrides.pop('blocked_until',None)
            active = active | {'version':int(active.get('version',1))+1,'overrides':overrides,
                'reason':'按完整持仓周期重建反思；移除旧部分成交计数产生的币种冷却'}
            self.store.set('reflection:active',active)
        report = self._report(trades[-self.cfg.reflection_window:])
        candidate = None
        last = self.store.get('reflection:last_cycle_evaluated',len(trades))
        if not migrating and len(trades)-last>=self.cfg.reflection_interval_trades:
            candidate = self._candidate(report,active)
            self.store.set('reflection:last_cycle_evaluated',len(trades))
        elif migrating:
            self.store.set('reflection:last_cycle_evaluated',len(trades))
            self.store.event('反思统计修正','已从持久化成交账本按完整开平仓重建；不把部分成交当成独立交易')
        if candidate:
            active = candidate
            self.store.set('reflection:active',candidate)
            self.store.event('策略反思',candidate['reason'])
        self.store.set('reflection:trades',trades[-max(self.cfg.reflection_window*4,200):])
        self.store.set('reflection:trade_count',len(trades))
        self.store.set('reflection:cycle_cursor',cursor)
        status = {'enabled':True,'active':active,'report':report,
                  'last_trade':trades[-1] if trades else None,'candidate':candidate,
                  'counting_basis':'completed_position_cycles_v2'}
        self.store.set('reflection:status',status)
        return status
