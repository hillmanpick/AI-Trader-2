"""Model-specific, out-of-sample portfolio replay on archived market events.

This module has no exchange client. Its ledger and order journal live only in a
temporary directory. Missing liquidity leaves real residual virtual positions;
there are no synthetic end-of-window fills or zero-cost liquidations.
"""
import math
import tempfile
import time
from pathlib import Path

from .book_replay import HistoricalBookReplay
from .correlation_risk import cap_notional
from .event_lake import EventLake
from .execution_policy import fingerprint, model_version, microstructure_plan
from .execution_state import OrderJournal, SimulatedBroker
from .lab_model import decide
from .market_ranker import MarketRanker
from .strategies import update_dynamic_exit

REPLAY_SCHEMA = 'portfolio-individual-events-v4'
KINDS = {'depthSnapshot','depthUpdate','trade','feature','feedStatus','depthStatus','candidates'}


class ModelEventReplay(HistoricalBookReplay):
    def __init__(self, journal, model, policy, start_ns, end_ns, *, stress=False, initial_cash=100000):
        self.model, self.policy, self.start_ns, self.end_ns = model, policy, start_ns, end_ns
        self.cash, self.initial_cash, self.peak = float(initial_cash),float(initial_cash),float(initial_cash)
        self.drawdown, self.fees, self.funding = 0.,0.,0.
        self.positions, self.pending, self.rows = {},{},{}
        self.closed, self.rejected, self.invalid = [],0,[]
        self.signals, self.last_signal = 0,{}
        self.ranker = MarketRanker()
        self.stress = stress
        self.measured_gaps = 0
        self.active_candidates = None
        self.candidate_removed_ns = {}
        broker = SimulatedBroker(journal,ack_ms=500 if stress else 20,
            fee_rate=policy['fee_rate'],participation=policy['paper_depth_participation']*(.5 if stress else 1),
            slippage=policy['paper_slippage']*(2 if stress else 1),on_fill=self.apply_fill)
        super().__init__(broker)

    def apply_fill(self, connection, fill, order):
        symbol, qty, price, fee = order['symbol'],float(fill['quantity']),float(fill['price']),float(fill['fee'])
        self.fees += fee
        self.cash -= fee
        if order['reduce_only']:
            p = self.positions.get(symbol)
            if not p or p['amount']*order['side'] >= 0 or qty > abs(p['amount'])+1e-9:
                raise ValueError('Replay reduce-only invariant failed')
            fraction = min(1,qty/abs(p['amount']))
            gross = (price-p['entry'])*qty*(1 if p['amount']>0 else -1)
            # Conservative absolute funding charge; never invent funding income.
            funding = qty*p['entry']*.001*max(0,fill['at_ns']-p['opened_ns'])/(28800000*1000000)
            self.cash += gross-funding
            self.funding += funding
            p['net'] += gross-fee-funding
            remaining = p['amount']+qty*order['side']
            if abs(remaining) <= max(1e-10,abs(p['amount'])*1e-10):
                self.closed.append({'symbol':symbol,'net':p['net'],'at_ns':fill['at_ns'],
                    'mae':p['mae'],'mfe':p['mfe'],'leverage':p['leverage'],
                    'reason':order['reason'],'model_version':self.model['version']})
                self.positions.pop(symbol)
            else:
                p['amount'],p['initial_margin'] = remaining,p['initial_margin']*(1-fraction)
        else:
            plan = order['plan']
            p = self.positions.get(symbol)
            if p and p['amount']*order['side'] <= 0:
                raise ValueError('Replay entry cannot reverse a position')
            old_qty = abs(p['amount']) if p else 0.
            entry = ((p['entry']*old_qty if p else 0)+price*qty)/(old_qty+qty)
            if not p:
                p = {'entry':entry,'amount':0.,'mark':price,'opened_ns':fill['at_ns'],
                     'stop_pct':plan['stop_pct'],'stop_price':entry*(1-order['side']*plan['stop_pct']),
                     'leverage':plan['leverage'],'initial_margin':0.,'plan':plan,
                     'net':0.,'mae':0.,'mfe':0.}
                self.positions[symbol] = p
            p['entry'],p['amount'] = entry,order['side']*(old_qty+qty)
            p['initial_margin'] += qty*price/plan['leverage']
            p['net'] -= fee

    def submit_close(self, symbol, now, reason):
        if symbol in self.pending or symbol not in self.positions:
            return
        p = self.positions[symbol]
        identity = f"close:{symbol}:{now}"
        self.broker.submit(identity,symbol,-1 if p['amount']>0 else 1,abs(p['amount']),now,
                           reduce_only=True,metadata={'reason':reason})
        self.pending[symbol] = identity

    def enter(self, row, now):
        cfg, symbol = self.policy,row['symbol']
        horizon_ns = cfg['horizon_seconds']*10**9
        if (now < self.start_ns or now+horizon_ns+10*10**9 >= self.end_ns
                or symbol in self.positions or symbol in self.pending
                or len(self.positions) >= cfg['max_positions']
                or any(not self.broker.journal.get(identity)['reduce_only'] for identity in self.pending.values())):
            return
        if self.active_candidates is not None and symbol not in self.active_candidates:
            return
        if (not row['meta'].get('quality') or not row['meta'].get('entry_allowed',True)
                or not 0 <= now//1000000-row['ts'] <= 10000):
            self.rejected += 1
            return
        decision = decide(self.model,row)
        if decision['action'] not in ('long','short') or self.last_signal.get(symbol)==row['ts']:
            return
        self.last_signal[symbol] = row['ts']
        self.signals += 1
        side = 1 if decision['action']=='long' else -1
        if sum((p['amount']>0)==(side>0) for p in self.positions.values()) >= 2:
            self.rejected += 1
            return
        try:
            liquidity = self.books[symbol].metrics(now//1000000,cfg['paper_depth_range_bps'])
            if liquidity['spread_bps'] > cfg['paper_max_spread_bps']:
                raise ValueError('Spread limit')
            margin = sum(p['initial_margin'] for p in self.positions.values())
            exposure = sum(abs(p['amount'])*p['mark'] for p in self.positions.values())
            maximum = min(cfg['max_notional']-exposure,
                liquidity['ask_depth' if side>0 else 'bid_depth']*cfg['paper_depth_participation'])
            maximum,_ = cap_notional(self.ranker.return_snapshot(now//1000000),self.positions,
                symbol,side,maximum,cfg['paper_max_margin']*cfg['paper_risk_fraction'],
                now//1000000,cfg['horizon_seconds'])
            plan = microstructure_plan(row,liquidity,cfg,self.positions,maximum)
            if margin+plan['margin'] > cfg['paper_max_margin']:
                raise ValueError('Margin limit')
            equity = self.cash+sum((p['mark']-p['entry'])*p['amount'] for p in self.positions.values())
            if equity-margin < plan['notional']/plan['leverage']+plan['notional']*.002:
                raise ValueError('Available collateral')
            quantity = plan['notional']/row['price']
            filters = {f['filterType']:f for f in row['meta'].get('filters',[])}
            if not filters:
                raise ValueError('Missing causal exchange filters')
            lot = filters.get('MARKET_LOT_SIZE',filters.get('LOT_SIZE',{}))
            from .binance import rounded
            step = float(lot.get('stepSize',0)) or float(filters.get('LOT_SIZE',{}).get('stepSize',0))
            if step:
                quantity = float(rounded(quantity,step))
            if (quantity<=0 or quantity<float(lot.get('minQty',0))
                    or quantity>float(lot.get('maxQty',float('inf')))
                    or quantity*row['price']<float(filters.get('MIN_NOTIONAL',{}).get('notional',0))):
                raise ValueError('Exchange quantity filter')
            identity = f"entry:{symbol}:{now}"
            self.broker.submit(identity,symbol,side,quantity,now,metadata={'plan':plan})
            self.pending[symbol] = identity
        except (KeyError,ValueError):
            self.rejected += 1

    def consume(self, event):
        started = time.perf_counter_ns()
        now, kind, symbol, payload = event['received_ns'],event['kind'],event['symbol'],event['payload']
        before = self.gaps
        super().consume(event)
        if now >= self.start_ns:
            self.measured_gaps += self.gaps-before
        if kind=='feedStatus':
            for s in payload.get('symbols',[]):
                self.ranker.invalidate(s,now//1000000)
                if s in self.positions:
                    self.invalid.append('trade_feed_disconnected_while_holding')
        if kind=='depthStatus':
            for s in payload.get('symbols', [symbol] if symbol else []):
                # Global control records include symbols outside an explicitly
                # scoped replay. They must not invalidate unrelated evidence.
                if s not in self.books and s not in self.positions and s not in self.pending:
                    continue
                if payload.get('status')=='retired':
                    removed=self.candidate_removed_ns.get(s)
                    grace=(self.policy['horizon_seconds']+45)*10**9
                    if (removed is None or now-removed<grace
                            or s in (self.active_candidates or set()) or s in self.pending):
                        self.invalid.append('unproven_or_premature_observation_retirement')
                if s in self.positions:
                    self.invalid.append('depth_feed_reset_while_holding')
        if kind=='candidates':
            updated = set(payload['symbols'])
            for s in (self.active_candidates or set())-updated:
                self.candidate_removed_ns[s]=now
            for s in updated:
                self.candidate_removed_ns.pop(s,None)
            for s in set(self.books)-updated:
                self.candidate_removed_ns.setdefault(s,now)
            self.active_candidates = updated
        if kind=='depthSnapshot' and self.active_candidates is not None and symbol not in self.active_candidates:
            self.candidate_removed_ns.setdefault(symbol,now)
        if kind=='trade' and event['source']=='websocket' and float(payload['p'])>0 and float(payload['q'])>0:
            self.ranker.trade(payload)
            if symbol in self.positions:
                self.positions[symbol]['mark'] = float(payload['p'])
        if kind in ('depthSnapshot','depthUpdate') and symbol in self.positions:
            book = self.books.get(symbol)
            if book and book.ready:
                self.positions[symbol]['mark'] = (max(book.bids)+min(book.asks))/2
        # One cached order per symbol; terminal orders release their reservation.
        if self.pending:
            self.pending = {s:i for s,i in self.pending.items()
                            if self.broker.journal.get(i)['state'] not in ('FILLED','CANCELED','EXPIRED','REJECTED')}
        for s,p in list(self.positions.items()):
            move = (p['mark']/p['entry']-1)*(1 if p['amount']>0 else -1)
            p['mae'],p['mfe'] = min(p['mae'],move),max(p['mfe'],move)
            reason = update_dynamic_exit(p,p['plan'],self.policy['fee_rate'],self.policy['paper_slippage'])
            if now-p['opened_ns'] >= self.policy['horizon_seconds']*10**9:
                reason = 'model_horizon'
            if now >= self.end_ns-10*10**9:
                reason = 'end_of_window'
            if p['initial_margin']+(p['mark']-p['entry'])*p['amount'] <= abs(p['amount'])*p['mark']*self.policy['paper_maintenance_rate']:
                self.invalid.append('approximate_liquidation_reached')
                reason = 'liquidation'
            if reason:
                self.submit_close(s,now,reason)
        if kind=='feature':
            self.enter(payload,now)
        equity = self.cash+sum((p['mark']-p['entry'])*p['amount'] for p in self.positions.values())
        self.peak = max(self.peak,equity)
        self.drawdown = max(self.drawdown,self.peak-equity)
        self.processing[-1] = (time.perf_counter_ns()-started)/1e6

    def report(self, events):
        base = super().run(events)
        # Portfolio metrics must never erase an incomplete underlying market
        # replay (for example, a book that went stale after the last fill).
        self.invalid.extend(base['invalid_reasons'])
        if self.positions or self.pending:
            self.invalid.append('unclosed_position_or_pending_order_at_end')
        if not self.reconstructed or self.measured_gaps:
            self.invalid.append('missing_depth_or_sequence_gap')
        if self.last_ns < self.end_ns-2*10**9:
            self.invalid.append('incomplete_event_window')
        wins = sum(t['net'] for t in self.closed if t['net']>0)
        losses = -sum(t['net'] for t in self.closed if t['net']<0)
        return {k:v for k,v in base.items() if k!='fills'} | {
            'schema':REPLAY_SCHEMA,'valid':not self.invalid,'invalid_reasons':sorted(set(self.invalid)),
            'net_pnl':self.cash-self.initial_cash,'trades':len(self.closed),
            'win_rate':sum(t['net']>0 for t in self.closed)/len(self.closed) if self.closed else 0,
            'profit_factor':wins/losses if losses else None,'max_drawdown':self.drawdown,
            'fees':self.fees,'funding':self.funding,'signals':self.signals,'rejected':self.rejected,
            'open_positions':len(self.positions),'start_ns':self.start_ns,'end_ns':self.end_ns,
            'funding_assumption':'absolute 10bps/8h prorated, no funding income',
            'stress':self.stress,'model_version':self.model['version'],'policy_hash':fingerprint(self.policy)}


def replay_model(lake, model, policy, start_ms, end_ms, symbols, *, stress=False):
    symbols = set(symbols)
    if not symbols or any(not isinstance(s,str) or not s.strip() for s in symbols):
        raise ValueError('Model holdout requires explicit symbols')
    if model['version'] != model_version(model) or model['training_end_ms'] >= start_ms:
        raise ValueError('Model version or chronological OOS boundary mismatch')
    warmup = max(120000,policy['horizon_seconds']*1000)
    segments = lake.segments((start_ms-warmup)*1000000,end_ms*1000000)
    if not segments or segments[0]['first_ns'] > (start_ms-65000)*1000000 or segments[-1]['last_ns'] < (end_ms-2000)*1000000:
        raise ValueError('Historical event coverage insufficient for OOS replay')
    with tempfile.TemporaryDirectory(prefix='quant-model-replay-') as directory:
        engine = ModelEventReplay(OrderJournal(Path(directory)/'orders.db'),model,policy,
                                  start_ms*1000000,end_ms*1000000,stress=stress)
        engine.expected_symbols = symbols
        report = engine.report(lake.iter_events((start_ms-warmup)*1000000,end_ms*1000000,
                                                symbols|{''},KINDS))
    report['requested_symbols'] = sorted(symbols)
    report['manifest_hash'] = fingerprint([{k:s[k] for k in ('path','sha256','first_ns','last_ns')} for s in segments])
    report['segments'] = len(segments)
    return report


def validation_passes(report, min_trades=30):
    pf = report.get('profit_factor')
    return (report.get('valid') is True and report.get('trades',0)>=min_trades
            and report.get('net_pnl',0)>0 and (pf is None or pf>=1.1)
            and report.get('max_drawdown',math.inf)<=3000)


def validate_candidate(lake, model, policy, validation_start, test_start, test_end, symbols):
    """Select threshold on validation only. Never refit on either holdout."""
    options = []
    for threshold in (.5,.6,.7):
        candidate = model | {'threshold':threshold}
        candidate['version'] = model_version(candidate)
        result = replay_model(lake,candidate,policy,validation_start,test_start,symbols)
        score = result['net_pnl'] if validation_passes(result) else -math.inf
        options.append((score,candidate,result))
    score,model,validation = max(options,key=lambda item:item[0])
    if not math.isfinite(score):
        return model,{'passed':False,'reason':'validation_event_replay_failed',
                      'validation_options':[r for _,_,r in options], 'model_version':model['version']}
    test = replay_model(lake,model,policy,test_start,test_end,symbols)
    stress = replay_model(lake,model,policy,test_start,test_end,symbols,stress=True)
    proof = {'schema':REPLAY_SCHEMA,'model_version':model['version'],'policy_hash':fingerprint(policy),
             'policy':policy,'passed':validation_passes(test) and validation_passes(stress),
             'training_end_ms':model['training_end_ms'],'validation_start_ms':validation_start,
             'test_start_ms':test_start,'test_end_ms':test_end,
             'validation':validation,'test':test,'stress':stress,
             'criteria':'valid event windows; each >=30 closed positions, net>0, PF>=1.1, DD<=3000',
             'threshold_selection':'validation only; test and stress never select threshold'}
    return model,proof
