"""USD-M diff-depth reconstruction. Invalid books never produce features."""
import math
from collections import deque


class DepthBook:
    def __init__(self):
        self.bids, self.asks = {}, {}
        self.last = None
        self.ready = False
        self.bridged = False
        self.gaps = 0
        # Historical gap evidence is kept in ``gaps``. This flag describes
        # current live health and clears after a snapshot is bridged.
        self.sequence_gap_active = False
        self.updated_ms = 0
        self.snapshot_ms = 0
        self.ofi = deque()
        self.ofi_started_ms = None

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
        self.sequence_gap_active = False
        self.updated_ms = received_ms
        self.snapshot_ms = received_ms
        self.ofi.clear()
        self.ofi_started_ms = None

    def invalidate(self):
        """A disconnected feed cannot keep a matchable or feature-ready book."""
        self.ready, self.bridged, self.last = False, False, None
        self.ofi.clear()
        self.ofi_started_ms = None

    def apply(self, event, received_ms):
        if self.last is None:
            return False
        end = int(event['u'])
        if end < self.last or (self.bridged and end == self.last):
            return False
        # A REST snapshot may end exactly at the previous depth event. In that
        # case the first buffered Futures diff can start at U > lastUpdateId;
        # pu == last still proves continuity. Otherwise the first diff must
        # overlap the snapshot, and every later diff must chain through pu.
        first_bridge = int(event['U']) <= self.last <= end or int(event['pu']) == self.last
        valid = int(event['pu']) == self.last if self.bridged else first_bridge
        if not valid:
            self.ready, self.last = False, None
            self.gaps += 1
            self.sequence_gap_active = True
            self.ofi.clear()
            self.ofi_started_ms = None
            return False
        previous = self.best_quote() if self.ready else None
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
        if self.ready:
            self.sequence_gap_active = False
        if self.ready and received_ms >= self.snapshot_ms:
            quote = self.best_quote()
            if self.ofi_started_ms is None:
                self.ofi_started_ms = received_ms
            if previous is not None:
                bid, bid_qty, ask, ask_qty = quote
                old_bid, old_bid_qty, old_ask, old_ask_qty = previous
                flow = ((bid_qty if bid >= old_bid else 0)
                        -(old_bid_qty if bid <= old_bid else 0)
                        -(ask_qty if ask <= old_ask else 0)
                        +(old_ask_qty if ask >= old_ask else 0))
                self.ofi.append((received_ms,flow,(bid_qty+ask_qty)/2))
            self.prune_ofi(received_ms)
        elif not self.ready:
            self.ofi.clear()
            self.ofi_started_ms = None
        # Retain only the nearest levels; snapshots refresh every 60 seconds.
        if len(self.bids) > 2000:
            self.bids = dict(sorted(self.bids.items(), reverse=True)[:1500])
        if len(self.asks) > 2000:
            self.asks = dict(sorted(self.asks.items())[:1500])
        return self.ready

    def best_quote(self):
        bid, ask = max(self.bids), min(self.asks)
        return bid,self.bids[bid],ask,self.asks[ask]

    def prune_ofi(self, now_ms):
        while self.ofi and self.ofi[0][0] <= now_ms-5000:
            self.ofi.popleft()

    def metrics(self, now_ms, bps=10):
        if not self.ready or now_ms-self.updated_ms > 2000 or now_ms-self.snapshot_ms > 65000:
            raise ValueError('Depth unavailable, stale or sequence gap')
        bid, ask = max(self.bids), min(self.asks)
        self.prune_ofi(now_ms)
        # Sum every valid diff's best-queue contribution, not just sampled
        # endpoints. Future buffered frames never enter an earlier feature.
        flows = [r for r in self.ofi if r[0] <= now_ms]
        average_queue = sum(r[2] for r in flows)/len(flows) if flows else 0
        ofi = sum(r[1] for r in flows)/average_queue if average_queue else 0
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
                'ofi_5s':ofi,'ofi_events':len(flows),
                'ofi_ready':self.ofi_started_ms is not None and now_ms-self.ofi_started_ms >= 5000,
                'bid_coverage_bps':(mid-min(self.bids))/mid*10000,
                'ask_coverage_bps':(max(self.asks)-mid)/mid*10000,
                'updated_ms': self.updated_ms, 'sequence': self.last}
