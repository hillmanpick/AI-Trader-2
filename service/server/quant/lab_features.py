"""Causal numeric vectors, with a versioned and fixed column order."""
import math
from collections import defaultdict, deque

SCHEMA = 'microstructure-v2'
FEATURES = ('return_5s', 'return_30s', 'return_60s', 'ewma_volatility',
            'spread_bps', 'depth_imbalance', 'ofi', 'trade_imbalance',
            'log_trade_volume', 'volume_anomaly', 'btc_return_60s',
            'log_bid_depth', 'log_ask_depth', 'latency_ms')


class FeatureBuilder:
    def __init__(self):
        self.history = defaultdict(lambda: deque(maxlen=900))
        self.trades = defaultdict(lambda: deque(maxlen=20000))
        self.previous = {}
        self.last_trade = {}
        self.trade_gaps = defaultdict(int)
        self.bad_until = {}

    def trade(self, event, now):
        s, identity = event['s'], int(event['a'])
        previous = self.last_trade.get(s)
        if previous is not None and identity <= previous:
            return
        if previous is not None and identity != previous+1:
            self.trade_gaps[s] += 1
            self.bad_until[s] = now+60000
        self.last_trade[s] = identity
        quantity = float(event['p'])*float(event['q'])
        if not math.isfinite(quantity) or quantity <= 0:
            raise ValueError('Invalid aggregate trade')
        self.trades[s].append((int(event['T']), quantity, -1 if event['m'] else 1))

    def reconnect(self, symbols, now):
        for s in symbols:
            self.bad_until[s] = now+60000
            self.last_trade.pop(s, None)

    def build(self, symbol, book, now, event_ms):
        history = self.history[symbol]
        if history and now <= history[-1][0]:
            raise ValueError('Non-monotonic sample timestamp')
        mid = book['mid']
        history.append((now, mid))
        def ret(seconds, rows=history):
            prior = next((p for t,p in reversed(rows) if t <= now-seconds*1000), None)
            return math.log(mid/prior) if prior else None
        r5, r30, r60 = ret(5), ret(30), ret(60)
        if r60 is None or r30 is None or r5 is None:
            raise ValueError('Feature warmup needs 60 seconds')
        if any(b[0]-a[0] > 15000 for a,b in zip(list(history)[-13:], list(history)[-12:])):
            raise ValueError('Sample gap in recent history')
        changes = [math.log(b[1]/a[1]) for a,b in zip(history, list(history)[1:])][-60:]
        variance = 0
        for change in changes:
            variance = .94*variance+.06*change*change
        trades = self.trades[symbol]
        while trades and trades[0][0] < now-60000:
            trades.popleft()
        if not trades or now-trades[-1][0] > 10000 or now < self.bad_until.get(symbol,0):
            raise ValueError('Trade stream stale, missing or warming after reconnect')
        recent = [(q,side) for t,q,side in trades if now-5000 <= t <= now]
        total = sum(q for q,_ in recent)
        imbalance = sum(q*side for q,side in recent)/total if total else 0
        baseline = sum(q for t,q,_ in trades if t <= now)/12
        previous = self.previous.get(symbol, book)
        # Best-quote OFI (price changes and queue changes), normalized by size.
        ofi = ((book['bid_qty'] if book['bid'] >= previous['bid'] else 0)
               -(previous['bid_qty'] if book['bid'] <= previous['bid'] else 0)
               -(book['ask_qty'] if book['ask'] <= previous['ask'] else 0)
               +(previous['ask_qty'] if book['ask'] >= previous['ask'] else 0))
        ofi /= max(book['bid_qty']+book['ask_qty'], 1e-12)
        self.previous[symbol] = dict(book)
        btc = self.history['BTCUSDT']
        btc_prior = next((p for t,p in reversed(btc) if t <= now-60000), None)
        if not btc or not btc_prior or now-btc[-1][0] > 10000:
            raise ValueError('BTC regime feature not ready')
        lag = max(0, now-event_ms)
        if lag > 2000 or event_ms-now > 1000:
            raise ValueError('Market event latency too high')
        vector = [r5,r30,r60,math.sqrt(variance),book['spread_bps'],book['imbalance'],
                  max(-10,min(10,ofi)),imbalance,math.log1p(total),total/baseline if baseline else 0,
                  math.log(btc[-1][1]/btc_prior), math.log1p(book['bid_depth']),math.log1p(book['ask_depth']),lag]
        if not all(math.isfinite(v) for v in vector):
            raise ValueError('Non-finite features')
        return {'symbol':symbol, 'ts':now, 'price':mid, 'vector':vector,
                'meta':book | {'schema':SCHEMA,'event_ms':event_ms,'quality':True,
                               'atr_proxy':max(.0008, math.sqrt(variance)*math.sqrt(12)),
                               'trade_volume':total,'trade_gaps':self.trade_gaps[symbol]}}
