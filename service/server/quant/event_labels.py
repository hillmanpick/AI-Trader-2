"""Causal triple barriers on individual receive-ordered trades, with gap rejection.

Only completed horizons are persisted. A hit does not end the observation:
continuity must hold until expiry so label availability cannot leak future gaps.
"""
import math
from collections import defaultdict

LABEL_SCHEMA = 'individual-trade-barriers-v1'


class EventLabeler:
    def __init__(self, horizon_ms=300000, stride_ms=30000, stale_ms=15000):
        self.horizon_ms, self.stride_ms, self.stale_ms = horizon_ms, stride_ms, stale_ms
        self.pending = defaultdict(list)
        self.last_ids, self.last_price_ns, self.last_sample, self.last_seen_ns = {}, {}, {}, {}
        self.completed = []
        self.last_symbol_ns = {}
        self.next_expiry = 2**63-1

    def invalidate(self, symbols, reason):
        for symbol in symbols:
            for sample in self.pending.get(symbol, ()):
                sample['invalid'] = reason

    def consume(self, event):
        now = event['received_ns']
        # Each symbol has its own sequenced feed; unrelated symbols may arrive
        # in any interleaving without making one another's labels invalid.
        if now < self.last_symbol_ns.get(event['symbol'],0):
            self.invalidate([event['symbol']], 'receive_clock_regressed')
        self.last_symbol_ns[event['symbol']] = max(now,self.last_symbol_ns.get(event['symbol'],0))
        kind, symbol, payload = event['kind'], event['symbol'], event['payload']
        if kind == 'feedStatus':
            self.invalidate(payload.get('symbols', ()), 'trade_feed_reconnected')
        elif kind == 'trade' and event['source'] == 'websocket':
            identity = int(payload['t'])
            previous = self.last_ids.get(symbol)
            if previous is not None and identity <= previous:
                return
            if previous is not None and identity != previous + 1:
                self.invalidate([symbol], 'individual_trade_id_gap')
            self.last_ids[symbol] = identity
            self.last_seen_ns[symbol] = now
            if self.pending.get(symbol) and self.pending[symbol][0]['end_ns']<=now:
                self.next_expiry = min(self.next_expiry,now)
            price, qty = float(payload['p']), float(payload['q'])
            if math.isfinite(price) and math.isfinite(qty) and price > 0 and qty > 0:
                prior_ns = self.last_price_ns.get(symbol)
                if prior_ns is not None and now-prior_ns > self.stale_ms*1000000:
                    self.invalidate([symbol], 'unobserved_price_interval')
                self.last_price_ns[symbol] = now
                for sample in self.pending.get(symbol, ()):
                    if sample['start_ns'] < now <= sample['end_ns']:
                        move = price/sample['price']-1
                        sample['last_price_ns'] = now
                        if sample['hit_ns'] is None and abs(move) >= sample['distance']:
                            sample['label'] = 1 if move > 0 else -1
                            sample['hit_ns'] = now
        elif kind == 'feature' and payload['meta'].get('quality'):
            ts = payload['ts']
            if ts//self.stride_ms != self.last_sample.get(symbol):
                self.last_sample[symbol] = ts//self.stride_ms
                last_price = self.last_price_ns.get(symbol, 0)
                self.pending[symbol].append({
                    'symbol':symbol, 'ts':ts, 'start_ns':now,
                    'end_ns':now+self.horizon_ms*1000000,
                    'label_end':(now+999999)//1000000+self.horizon_ms,
                    'price':payload['price'], 'distance':max(.0026, payload['meta'].get('atr_proxy',.002)*2),
                    'label':0, 'hit_ns':None, 'last_price_ns':last_price,
                    'first_trade_id':self.last_ids.get(symbol),
                    'invalid':None if 0 <= now-last_price <= self.stale_ms*1000000 else 'no_initial_trade',
                })
                self.next_expiry = min(self.next_expiry,now+self.horizon_ms*1000000)
        # Ended samples never see a price at a later receive timestamp.
        if now < self.next_expiry:
            return
        self.next_expiry = 2**63-1
        for s in list(self.pending):
            remaining = []
            for sample in self.pending[s]:
                if sample['end_ns'] > now:
                    remaining.append(sample)
                    self.next_expiry = min(self.next_expiry,sample['end_ns'])
                    continue
                if self.last_seen_ns.get(s,0)<sample['end_ns'] and now<sample['end_ns']+self.stale_ms*1000000:
                    # A subsequent ID confirms continuity across expiry. Waiting
                    # cannot change the hit label, only validate its coverage.
                    remaining.append(sample)
                    self.next_expiry = min(self.next_expiry,now+1000000000)
                    continue
                if self.last_seen_ns.get(s,0)<sample['end_ns']:
                    sample['invalid'] = sample['invalid'] or 'no_trade_confirmation_after_expiry'
                if sample['end_ns']-sample['last_price_ns'] > self.stale_ms*1000000:
                    sample['invalid'] = sample['invalid'] or 'price_stale_at_expiry'
                sample |= {'schema':LABEL_SCHEMA, 'valid':not bool(sample['invalid']),
                           'last_trade_id':self.last_ids.get(s), 'completed_ns':now,
                           'label_end':max(sample['label_end'],(now+999999)//1000000)}
                self.completed.append(sample)
            if remaining:
                self.pending[s] = remaining
            else:
                self.pending.pop(s)

    def drain(self):
        result, self.completed = self.completed, []
        return result


def backfill_labels(directory,start_ms=0,end_ms=(2**63-1)//1000000,horizon_ms=300000):
    """Recover derived labels after restart using durable events, never REST revisions."""
    from .event_lake import EventLake
    from .lab_store import LabStore
    store, lake = LabStore(directory),EventLake(directory)
    with store.connect() as c:
        symbols = {r[0] for r in c.execute('SELECT DISTINCT symbol FROM samples WHERE ts>=? AND ts<=?',(start_ms,end_ms))}
    if not symbols:
        return {'events':0,'completed':0,'valid':0}
    labeler = EventLabeler(horizon_ms)
    count, completed, valid = 0,0,0
    pending = []
    for event in lake.iter_events(max(0,start_ms-120000)*1000000,end_ms*1000000,
            symbols|{''},{'trade','feature','feedStatus'}):
        labeler.consume(event)
        count += 1
        rows = labeler.drain()
        rows = [r for r in rows if r['ts']>=start_ms]
        completed += len(rows)
        valid += sum(r['valid'] for r in rows)
        pending.extend(rows)
        if len(pending)>=1000:
            store.append_labels(pending)
            pending.clear()
    store.append_labels(pending)
    return {'events':count,'completed':completed,'valid':valid,
            'limitations':'only retained raw events; incomplete final horizons are excluded'}


def main():
    import argparse,json
    from .lab_config import LabConfig
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True)
    parser.add_argument('--start-ms',type=int,default=0)
    parser.add_argument('--end-ms',type=int,required=True)
    args = parser.parse_args()
    print(json.dumps(backfill_labels(args.directory,args.start_ms,args.end_ms,
                                     LabConfig().horizon_seconds*1000),ensure_ascii=False))


if __name__=='__main__':
    main()
