"""Read-only recovery: snapshot, query uncertain orders, backfill, deduplicate.

There is no assumed exactly-once delivery across a disconnect. A bounded event
queue is buffered during REST reads; missing orders are queried, never guessed
cancelled. Failure/overflow/time gaps leave the write fence closed.
"""
import queue
import threading
import time


class PrivateReconciler:
    def __init__(self, journal, exchange, symbols=(), stream=None, capacity=4096,
                 max_symbols=100, max_pages=5):
        self.journal, self.exchange, self.stream = journal,exchange,stream
        self.symbols = set(symbols)
        self.queue = queue.Queue(maxsize=capacity)
        self.wakeup = threading.Event()
        self.max_symbols, self.max_pages = max_symbols,max_pages
        self._lock = threading.RLock()
        self._generation = 0
        self._dropped = 0
        self._unrecoverable = False
        self._fault = None
        self._event_symbols = set()
        self.journal.fence('startup_reconciliation_required')

    def gap(self, reason):
        with self._lock:
            self._generation += 1
            self._fault = str(reason)[:100]
        self.journal.fence(str(reason)[:100])
        self.wakeup.set()

    def connected(self, generation):
        with self._lock:
            self._generation += 1
            self._fault = 'user_stream_connected_reconciliation_required'
        self.journal.fence('user_stream_connected_reconciliation_required')
        self.wakeup.set()

    def enqueue(self, event):
        symbol = (event.get('o') or {}).get('s')
        with self._lock:
            if symbol:
                if len(self._event_symbols) >= self.max_symbols and symbol not in self._event_symbols:
                    self._unrecoverable = True
                else:
                    self._event_symbols.add(symbol)
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            with self._lock:
                self._generation += 1
                self._dropped += 1
                self._fault = 'order_event_queue_overflow'
            self.journal.fence('order_event_queue_overflow')
        self.wakeup.set()

    def drain(self):
        count = 0
        while count < self.queue.maxsize:
            try:
                event = self.queue.get_nowait()
            except queue.Empty:
                break
            try:
                self.journal.event(event)
            except Exception:
                self.gap('order_event_invalid_or_persistence_failed')
                raise RuntimeError('Private order event could not be persisted') from None
            finally:
                self.queue.task_done()
            count += 1
        return count

    def _backfill(self, symbol, end_ms):
        # Initial audit has a disclosed five-minute boundary. On subsequent
        # recovery, keep a one-minute overlap and split into <=7-day windows.
        saved = self.journal.get('fill_cursor:'+symbol)
        initial = min(end_ms-300000,self.journal.get('first_seen_ms',end_ms-300000))
        start_ms = max(0,(saved if saved is not None else initial)-60000)
        if end_ms < start_ms:
            raise RuntimeError('Exchange clock moved backwards; cannot advance recovery cursor')
        windows = 0
        while start_ms <= end_ms:
            if windows >= self.max_pages:
                raise RuntimeError('Backfill time range exceeds this recovery budget')
            window_end = min(end_ms,start_ms+7*86400000-1)
            rows = self.exchange.user_trades(symbol,start_time=start_ms,end_time=window_end,limit=1000)
            pages, previous_id = 0,-1
            while True:
                if not isinstance(rows,list) or len(rows)>1000:
                    raise ValueError('Invalid account trade page')
                ids = [int(r['id']) for r in rows]
                if ids != sorted(set(ids)) or (ids and ids[0]<=previous_id):
                    raise RuntimeError('Trade pagination did not advance')
                for row in rows:
                    if row.get('symbol') != symbol:
                        raise ValueError('Trade page symbol mismatch')
                    # fromId pages have no time filter; observations after the
                    # window are valid fills too, but cannot advance this cursor.
                self.journal.apply_fills(rows)
                pages += 1
                if len(rows)<1000 or (rows and int(rows[-1]['time'])>window_end):
                    break
                if pages >= self.max_pages:
                    raise RuntimeError('Trade pagination exceeds recovery budget; not complete')
                previous_id = ids[-1]
                rows = self.exchange.user_trades(symbol,from_id=previous_id+1,limit=1000)
            self.journal.set('fill_cursor:'+symbol,window_end)
            start_ms = window_end+1
            windows += 1

    def run(self):
        started = time.perf_counter_ns()
        self.journal.fence('reconciling')
        with self._lock:
            generation = self._generation
            event_symbols = set(self._event_symbols)
        try:
            if not self.stream or self.stream.snapshot_status().get('status')!='connected':
                raise RuntimeError('User data stream not connected')
            if self._unrecoverable:
                raise RuntimeError('Observed symbol budget exceeded; manual review required')
            self.drain()
            token = self.stream.seed_token()
            account = self.exchange.account()
            positions = self.exchange.positions()
            if not isinstance(account,dict) or not isinstance(positions,list):
                raise ValueError('Invalid account/position snapshot')
            if not self.stream.seed(account,positions,token=token):
                raise RuntimeError('Account event raced REST snapshot; retry recovery')
            open_rows = self.exchange.open_orders()
            if not isinstance(open_rows,list):
                raise ValueError('Invalid open order snapshot')
            previous_open = self.journal.open_orders()
            for row in open_rows:
                self.journal.apply_order(row)
            open_keys = {(str(r['symbol']),str(r['orderId'])) for r in open_rows}
            queries = {(r['symbol'],r['client_id']) for r in previous_open
                       if (r['symbol'],r['order_id']) not in open_keys}
            queries |= {(r['symbol'],r['client_id']) for r in self.journal.unresolved()}
            for symbol,client_id in sorted(queries):
                if not client_id:
                    raise RuntimeError('Order lacks client identity; manual reconciliation required')
                # -2013 or a transport timeout remains unresolved. Never resend.
                row = self.exchange.query_order(client_id,symbol)
                if row.get('symbol')!=symbol or row.get('clientOrderId')!=client_id:
                    raise ValueError('Query-order identity mismatch')
                self.journal.apply_order(row)
            symbols = (self.symbols | event_symbols | {r['symbol'] for r in open_rows} |
                       {r['symbol'] for r in positions if float(r.get('positionAmt',0))} |
                       {s for s,_ in queries})
            if len(symbols)>self.max_symbols:
                raise RuntimeError('Reconciliation symbol budget exceeded; nothing silently truncated')
            end_ms = time.time_ns()//1_000_000+int(getattr(self.exchange,'offset',0))
            for symbol in sorted(symbols):
                self._backfill(symbol,end_ms)
            self.drain()
            # Store snapshots separately from the paper ledger. They do not
            # grant ownership, overwrite paper balances or trigger a real order.
            self.journal.set('account_snapshot',{'account':account,'positions':positions,'at_ms':end_ms})
            if self.journal.unresolved():
                # NEW/PARTIALLY_FILLED are healthy open orders, but SUBMITTING
                # and UNKNOWN require definitive exchange resolution.
                if any(r['state'] in {'SUBMITTING','UNKNOWN'} for r in self.journal.unresolved()):
                    raise RuntimeError('Uncertain local order remains unresolved')
            with self._lock:
                if self._generation!=generation or not self.queue.empty() or self.stream.snapshot_status().get('status')!='connected':
                    raise RuntimeError('Stream changed during recovery; reconcile again')
                if self._event_symbols-symbols:
                    raise RuntimeError('New symbol observed during recovery; backfill on next pass')
                self._fault = None
                result = {'ready':True,'reason':'tracked_symbols_reconciled','at_ms':time.time_ns()//1_000_000,
                    'duration_ms':(time.perf_counter_ns()-started)/1e6,'generation':generation,
                    'symbols':sorted(symbols),'dropped_events_total':self._dropped,
                    'scope':'orders and fills for tracked symbols; account and position REST snapshots',
                    'initial_history_minutes':6,'full_account_history_claimed':False}
                self.journal.set('reconciliation',result)
                return result
        except Exception as exc:
            # Persist only fixed local reasons/type, never requests URLs or
            # untrusted exchange error messages which can contain credentials.
            reason = str(exc) if type(exc) in {RuntimeError,ValueError} else type(exc).__name__
            if '://' in reason or len(reason)>180:
                reason = type(exc).__name__
            result = {'ready':False,'reason':reason,'at_ms':time.time_ns()//1_000_000,
                      'duration_ms':(time.perf_counter_ns()-started)/1e6,'dropped_events_total':self._dropped}
            self.journal.set('reconciliation',result)
            return result
