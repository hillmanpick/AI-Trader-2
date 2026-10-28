"""USD-M diff-depth reconstruction. Invalid books never produce features."""
import math


class DepthBook:
    def __init__(self):
        self.bids, self.asks = {}, {}
        self.last = None
        self.ready = False
        self.bridged = False
        self.gaps = 0
        self.updated_ms = 0
        self.snapshot_ms = 0

    @staticmethod
    def levels(rows):
        result = {}
        for p, q in rows:
            p, q = float(p), float(q)
            if not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q < 0:
                raise ValueError('Malformed depth level')
            if q:
                result[p] = q
        return result

    def seed(self, snapshot, received_ms):
        self.bids, self.asks = self.levels(snapshot['bids']), self.levels(snapshot['asks'])
        self.last = int(snapshot['lastUpdateId'])
        self.ready, self.bridged = False, False
        self.updated_ms = received_ms
        self.snapshot_ms = received_ms

    def apply(self, event, received_ms):
        if self.last is None:
            return False
        end = int(event['u'])
        if end < self.last or (self.bridged and end == self.last):
            return False
        # Futures first frame must cover lastUpdateId, subsequent pu must equal u.
        valid = int(event['pu']) == self.last if self.bridged else int(event['U']) <= self.last <= end
        if not valid:
            self.ready, self.last = False, None
            self.gaps += 1
            return False
        for target, key in ((self.bids, 'b'), (self.asks, 'a')):
            for p, q in event[key]:
                p, q = float(p), float(q)
                if not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q < 0:
                    self.ready, self.last = False, None
                    raise ValueError('Malformed depth update')
                if q:
                    target[p] = q
                else:
                    target.pop(p, None)
        self.last, self.updated_ms, self.bridged = end, received_ms, True
        self.ready = bool(self.bids and self.asks and max(self.bids) < min(self.asks))
        # Retain only the nearest levels; snapshots refresh every 60 seconds.
        if len(self.bids) > 2000:
            self.bids = dict(sorted(self.bids.items(), reverse=True)[:1500])
        if len(self.asks) > 2000:
            self.asks = dict(sorted(self.asks.items())[:1500])
        return self.ready

    def metrics(self, now_ms, bps=10):
        if not self.ready or now_ms-self.updated_ms > 2000 or now_ms-self.snapshot_ms > 65000:
            raise ValueError('Depth unavailable, stale or sequence gap')
        bid, ask = max(self.bids), min(self.asks)
        mid = (bid+ask)/2
        lower, upper = mid*(1-bps/10000), mid*(1+bps/10000)
        # If snapshot coverage is thinner than the requested band, do not
        # pretend that truncated depth is a complete liquidity estimate.
        truncated = min(self.bids) > lower or max(self.asks) < upper
        buy = sum(p*q for p,q in self.asks.items() if p <= upper)
        sell = sum(p*q for p,q in self.bids.items() if p >= lower)
        return {'bid': bid, 'ask': ask, 'bid_qty': self.bids[bid], 'ask_qty': self.asks[ask],
                'mid': mid, 'spread_bps': (ask-bid)/mid*10000,
                'bid_depth': sell, 'ask_depth': buy, 'depth_bps': bps,
                'imbalance': (sell-buy)/(sell+buy) if sell+buy else 0,
                'depth_lower_bound':truncated,
                'bid_coverage_bps':(mid-min(self.bids))/mid*10000,
                'ask_coverage_bps':(max(self.asks)-mid)/mid*10000,
                'updated_ms': self.updated_ms, 'sequence': self.last}
