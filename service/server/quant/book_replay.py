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


class ReplayValidationError(ValueError):
    """A rejected window still carries durable, inspectable diagnostics."""
    def __init__(self, report):
        super().__init__('Replay window contains sequence gaps or unreconstructed depth: '
                         + ', '.join(report['invalid_reasons']))
        self.report = report


class HistoricalBookReplay:
    def __init__(self,broker,orders=(),require_complete=False,expected_symbols=()):
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
        self.expected_symbols = set(expected_symbols)
        self.symbol_stats = defaultdict(lambda: {'events':0,'depth_updates':0,'snapshots':0,
                                                'reconstructed_depth_updates':0,'sequence_gaps':0})
        self.gap_details = []
        self.first_ns = None

    def record_gap(self, symbol, kind, now, previous, payload):
        self.gaps += 1
        self.symbol_stats[symbol]['sequence_gaps'] += 1
        if len(self.gap_details) < 100:
            self.gap_details.append({'symbol':symbol,'kind':kind,'received_ns':now,
                'previous_id':previous,
                'ids':{k:payload[k] for k in ('t','U','u','pu','lastUpdateId') if k in payload}})

    def apply_depth(self, symbol, payload, now):
        book = self.books[symbol]
        previous, before = book.last, book.gaps
        book.apply(payload,now//1000000)
        if book.gaps > before:
            self.record_gap(symbol,'depth',now,previous,payload)

    def consume(self,event):
        started = time.perf_counter_ns()
        now = event['received_ns']
        if now<self.last_ns:
            raise ValueError('Historical replay must be sorted by receive timestamp')
        self.last_ns = now
        if self.first_ns is None:
            self.first_ns = now
        self.events += 1
        self.feed_latency.append((now-event['event_ms']*1000000)/1e6)
        kind,symbol,p = event['kind'],event['symbol'],event['payload']
        stats = self.symbol_stats[symbol]
        stats['events'] += 1
        dedup_key = (kind,symbol)
        # Trade/diff identities are monotone in each live feed; backfills do not
        # generate retroactive maker fills or rewrite earlier trading decisions.
        trade = None
        if kind=='depthStatus':
            for s in p.get('symbols', [symbol] if symbol else []):
                if s in self.books:
                    self.books[s].invalidate()
                self.buffers[s].clear()
        elif kind=='depthSnapshot':
            stats['snapshots'] += 1
            book = self.books.setdefault(symbol,DepthBook())
            book.seed(p,now//1000000)
            for pending in self.buffers[symbol]:
                if book.last is not None and int(pending['u'])>=book.last:
                    self.apply_depth(symbol,pending,now)
        elif kind=='depthUpdate':
            stats['depth_updates'] += 1
            book = self.books.setdefault(symbol,DepthBook())
            self.buffers[symbol].append(p)
            self.apply_depth(symbol,p,now)
            if book.ready:
                self.reconstructed += 1
                stats['reconstructed_depth_updates'] += 1
        elif kind=='trade' and event['source']=='websocket':
            previous = self.seen_ids.get(dedup_key,-1)
            if int(p['t'])>previous:
                if previous>=0 and int(p['t'])!=previous+1:
                    self.record_gap(symbol,'trade',now,previous,p)
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
        reasons = []
        if self.gaps:
            reasons.append('sequence_gap')
        if not self.reconstructed:
            reasons.append('no_reconstructed_depth')
        symbols = {}
        for symbol in sorted((set(self.symbol_stats)|self.expected_symbols)-{''}):
            book = self.books.get(symbol)
            symbols[symbol] = dict(self.symbol_stats[symbol]) | {
                'book_ready':bool(book and book.ready), 'last_update_id':book.last if book else None,
                'depth_age_ms':(self.last_ns//1000000-book.updated_ms) if book else None}
            if symbol in self.expected_symbols and not symbols[symbol]['reconstructed_depth_updates']:
                reasons.append('missing_requested_depth:'+symbol)
            if book and not book.ready:
                reasons.append('unreconstructed_depth:'+symbol)
            if book and book.ready and self.last_ns//1000000-book.updated_ms > 2000:
                reasons.append('stale_depth_at_end:'+symbol)
        report = {'valid':not reasons,'invalid_reasons':reasons,
                'first_received_ns':self.first_ns,'last_received_ns':self.last_ns,
                'symbols':symbols,'gap_details':self.gap_details,
                'gap_details_truncated':self.gaps>len(self.gap_details),
                'events':self.events,'reconstructed_depth_updates':self.reconstructed,'sequence_gaps':self.gaps,
                'execution':self.broker.journal.statistics(),'fills':self.broker.journal.fills(),
                'local_processing':{'clock':'perf_counter_ns','sample_count':len(self.processing),
                    'p50_ms':percentile(self.processing,.5),'p95_ms':percentile(self.processing,.95),'p99_ms':percentile(self.processing,.99)},
                'observed_feed_latency':{'clock':'receive wall clock minus venue event time; includes clock offset',
                    'p50_ms':percentile(self.feed_latency,.5),'p95_ms':percentile(self.feed_latency,.95)},
                'limitations':['Displayed depth does not reveal hidden liquidity or exact queue priority',
                               'Simulated acknowledgement/fill times are not measured exchange order latencies']}
        if self.require_complete and reasons:
            raise ReplayValidationError(report)
        return report


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
    if args.start_ms < 0 or args.end_ms <= args.start_ms:
        parser.error('end-ms must be later than start-ms, with nonnegative boundaries')
    lake = EventLake(args.directory)
    orders = json.loads(args.orders.read_text()) if args.orders else []
    with tempfile.TemporaryDirectory() as directory:
        broker = SimulatedBroker(OrderJournal(Path(directory)/'orders.db'))
        symbols = {s.strip().upper() for s in args.symbols.split(',') if s.strip()}
        replay = HistoricalBookReplay(broker,orders,args.strict,symbols)
        rejected = False
        try:
            report = replay.run(lake.iter_events(args.start_ms*1000000,args.end_ms*1000000,
                              (symbols|{''}) if symbols else None,
                              {'depthSnapshot','depthUpdate','trade','depthStatus'}))
        except ReplayValidationError as exc:
            report, rejected = exc.report, True
    report['requested_window'] = {'start_ms':args.start_ms,'end_ms':args.end_ms,'symbols':sorted(symbols)}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k!='fills'},ensure_ascii=False))
    if rejected:
        parser.exit(2,'Strict replay rejected; diagnostics saved to output\n')


if __name__=='__main__':
    main()
