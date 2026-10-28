"""Replay real historical depth snapshots/diffs and individual trades in receive order."""
import argparse
import json
import tempfile
import time
from collections import defaultdict,deque
from pathlib import Path

from .event_lake import EventLake
from .execution_state import OrderJournal,SimulatedBroker,percentile
from .order_book import DepthBook


class HistoricalBookReplay:
    def __init__(self,broker,orders=(),require_complete=False):
        self.broker = broker
        self.orders = iter(sorted(orders,key=lambda o:o['at_ns']))
        self.next_order = next(self.orders,None)
        self.books = {}
        self.buffers = defaultdict(lambda:deque(maxlen=10000))
        self.seen_ids = {}
        self.events = 0
        self.reconstructed = 0
        self.gaps = 0
        self.processing = deque(maxlen=100000)
        self.feed_latency = deque(maxlen=100000)
        self.last_ns = 0
        self.require_complete = require_complete

    def consume(self,event):
        started = time.perf_counter_ns()
        now = event['received_ns']
        if now<self.last_ns:
            raise ValueError('Historical replay must be sorted by receive timestamp')
        self.last_ns = now
        self.events += 1
        self.feed_latency.append((now-event['event_ms']*1000000)/1e6)
        kind,symbol,p = event['kind'],event['symbol'],event['payload']
        identity = event['event_id']
        dedup_key = (kind,symbol)
        # Trade/diff identities are monotone in each live feed; backfills do not
        # generate retroactive maker fills or rewrite earlier trading decisions.
        trade = None
        if kind=='depthSnapshot':
            book = self.books.setdefault(symbol,DepthBook())
            book.seed(p,now//1000000)
            for pending in self.buffers[symbol]:
                if book.last is not None and int(pending['u'])>=book.last:
                    before = book.gaps
                    book.apply(pending,now//1000000)
                    self.gaps += book.gaps-before
        elif kind=='depthUpdate':
            book = self.books.setdefault(symbol,DepthBook())
            self.buffers[symbol].append(p)
            before = book.gaps
            book.apply(p,now//1000000)
            self.gaps += book.gaps-before
            if book.ready:
                self.reconstructed += 1
        elif kind=='trade' and event['source']=='websocket':
            previous = self.seen_ids.get(dedup_key,-1)
            if int(p['t'])>previous:
                if previous>=0 and int(p['t'])!=previous+1:
                    self.gaps += 1
                self.seen_ids[dedup_key] = int(p['t'])
                if float(p['p'])>0 and float(p['q'])>0:
                    trade = p
        while self.next_order and self.next_order['at_ns']<=now:
            order = dict(self.next_order)
            if order.pop('action','submit')=='cancel':
                self.broker.cancel(order['identity'],order['at_ns'])
            else:
                self.broker.submit(**order)
            self.next_order = next(self.orders,None)
        self.broker.on_event(now,self.books,trade)
        self.processing.append((time.perf_counter_ns()-started)/1e6)

    def run(self,events):
        for event in events:
            self.consume(event)
        if self.require_complete and (self.gaps or not self.reconstructed or any(not b.ready for b in self.books.values())):
            raise ValueError('Replay window contains sequence gaps or unreconstructed depth')
        return {'events':self.events,'reconstructed_depth_updates':self.reconstructed,'sequence_gaps':self.gaps,
                'execution':self.broker.journal.statistics(),'fills':self.broker.journal.fills(),
                'local_processing':{'clock':'perf_counter_ns','sample_count':len(self.processing),
                    'p50_ms':percentile(self.processing,.5),'p95_ms':percentile(self.processing,.95),'p99_ms':percentile(self.processing,.99)},
                'observed_feed_latency':{'clock':'receive wall clock minus venue event time; includes clock offset',
                    'p50_ms':percentile(self.feed_latency,.5),'p95_ms':percentile(self.feed_latency,.95)},
                'limitations':['Displayed depth does not reveal hidden liquidity or exact queue priority',
                               'Simulated acknowledgement/fill times are not measured exchange order latencies']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',required=True,type=Path)
    parser.add_argument('--start-ms',type=int,default=0)
    parser.add_argument('--end-ms',type=int,default=(2**63-1)//1000000)
    parser.add_argument('--symbols',default='')
    parser.add_argument('--orders',type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--strict',action='store_true')
    args = parser.parse_args()
    lake = EventLake(args.directory)
    orders = json.loads(args.orders.read_text()) if args.orders else []
    with tempfile.TemporaryDirectory() as directory:
        broker = SimulatedBroker(OrderJournal(Path(directory)/'orders.db'))
        replay = HistoricalBookReplay(broker,orders,args.strict)
        report = replay.run(lake.iter_events(args.start_ms*1000000,args.end_ms*1000000,
                          set(args.symbols.split(',')) if args.symbols else None,
                          {'depthSnapshot','depthUpdate','trade'}))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='fills'},ensure_ascii=False))


if __name__=='__main__':
    main()
