"""All-market causal flow vectors and cross-sectional ranking before depth selection."""
import math
from collections import defaultdict, deque

MARKET_FEATURES = ('return_10s','return_60s','realized_volatility','flow_imbalance',
                   'log_notional_10s','volume_anomaly','log_volume_24h','return_24h')


class MarketRanker:
    def __init__(self):
        self.buckets = defaultdict(lambda:deque(maxlen=120))
        self.last_trade = {}
        self.ready_after = {}

    def invalidate(self,symbol,now_ms):
        self.ready_after[symbol] = now_ms+60000

    def trade(self,event):
        symbol = event['s']
        timestamp = int(event['T'])//1000
        price,quantity = float(event['p']),float(event['q'])
        if not all(math.isfinite(v) and v>0 for v in (price,quantity)):
            raise ValueError('Invalid trade price or quantity')
        buckets = self.buckets[symbol]
        if buckets and timestamp < buckets[-1]['second']:
            return  # Backfilled history does not revise current decisions.
        if not buckets or timestamp > buckets[-1]['second']:
            buckets.append({'second':timestamp,'price':price,'volume':0.,'signed':0.})
        bucket = buckets[-1]
        bucket['price'] = price
        bucket['volume'] += price*quantity
        bucket['signed'] += price*quantity*(-1 if event['m'] else 1)
        self.last_trade[symbol] = int(event['T'])

    def vector(self,symbol,ticker,now_ms):
        rows = list(self.buckets[symbol])
        if not rows or now_ms < self.ready_after.get(symbol,0) or not 0 <= now_ms-self.last_trade[symbol] <= 15000:
            return None
        now = now_ms//1000
        price = rows[-1]['price']
        prior60 = next((r['price'] for r in reversed(rows) if r['second'] <= now-60),None)
        prior10 = next((r['price'] for r in reversed(rows) if r['second'] <= now-10),None)
        if prior60 is None or prior10 is None:
            return None
        recent = [r for r in rows if now-10 < r['second'] <= now]
        history = [r for r in rows if now-60 < r['second'] <= now]
        volume = sum(r['volume'] for r in recent)
        baseline = sum(r['volume'] for r in history)/6
        flow = sum(r['signed'] for r in recent)/volume if volume else 0
        changes = [math.log(b['price']/a['price']) for a,b in zip(history,history[1:])]
        vol = math.sqrt(sum(v*v for v in changes)/max(1,len(changes)))
        return [math.log(price/prior10),math.log(price/prior60),vol,flow,
                math.log1p(volume),min(20,volume/baseline) if baseline else 0,
                math.log1p(float(ticker.get('q',0))),float(ticker.get('P',0))/100]

    def rank(self,rules,tickers,now_ms,min_volume):
        ranked = []
        for symbol in rules:
            ticker = tickers.get(symbol,{})
            volume24 = float(ticker.get('q',0))
            v = self.vector(symbol,ticker,now_ms)
            if v is None:
                ranked.append({'symbol':symbol,'ready':False,'score':None,'reason':'flow warmup or inactive trade stream'})
                continue
            # Fixed, auditable baseline: directional agreement plus activity,
            # penalizing unstable moves. Scores compare every active contract.
            momentum = math.tanh(v[0]/max(v[2]*math.sqrt(10),.0001))
            direction = .55*momentum+.45*v[3]
            score = abs(direction)*math.log1p(volume24)*min(3,math.sqrt(max(v[5],0)))/(1+v[2]*1000)
            eligible = volume24 >= min_volume
            ranked.append({'symbol':symbol,'ready':True,'eligible':eligible,
                           'score':score if eligible else 0,'bias':'long' if direction>0 else 'short',
                           'direction_score':direction,'price':self.buckets[symbol][-1]['price'],
                           'vector':v,'schema':'market-flow-v1','ts':now_ms})
        return sorted(ranked,key=lambda r:-(r['score'] if r['score'] is not None else -1))

    def return_snapshot(self,now_ms):
        end = now_ms//1000
        result = {}
        for symbol,history in self.buckets.items():
            rows = list(history)
            if len(rows)<30 or self.last_trade.get(symbol,0)<now_ms-15000 or now_ms<self.ready_after.get(symbol,0):
                continue
            prices = {r['second']:r['price'] for r in rows}
            prior = next((r['price'] for r in reversed(rows) if r['second']<=end-60),None)
            if prior is None:
                continue
            vector = {}
            for second in range(end-59,end+1):
                price = prices.get(second,prior)
                vector[str(second)] = math.log(price/prior)
                prior = price
            result[symbol] = vector
        return {'ts':now_ms,'returns':result,'schema':'one-second-causal-returns-v1'}
