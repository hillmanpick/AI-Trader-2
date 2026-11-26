"""Bounded paper execution telemetry and non-blocking notification delivery.

Measurements describe local processing and simulation clocks. They never claim
exchange acknowledgements, network RTT, matching priority or profitability.
"""
from collections import defaultdict, deque, OrderedDict
from contextlib import contextmanager
import copy
import copy
import math
import json
import threading
import time


class LatencyRecorder:
    def __init__(self, capacity=1024):
        self.capacity = capacity
        self.samples = defaultdict(lambda: deque(maxlen=capacity))
        self.counts = defaultdict(int)
        self.lock = threading.Lock()

    def observe(self, stage, milliseconds):
        if not math.isfinite(milliseconds) or milliseconds < 0:
            raise ValueError('Latency must be finite and nonnegative')
        with self.lock:
            self.samples[stage].append(float(milliseconds))
            self.counts[stage] += 1

    @contextmanager
    def measure(self, stage):
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            self.observe(stage, (time.perf_counter_ns()-start)/1e6)

    def report(self):
        from .execution_state import percentile
        with self.lock:
            return {'clock':'local monotonic processing clock; data age uses host wall clock',
                    'exchange_execution_measured':False, 'capacity_per_stage':self.capacity,
                    'stages':{key:{'observations':self.counts[key], 'window_samples':len(values),
                        'p50_ms':percentile(values,.5),'p95_ms':percentile(values,.95),
                        'p99_ms':percentile(values,.99),'max_ms':max(values)}
                        for key,values in self.samples.items() if values}}


class LatestNotification:
    """One delivery thread and one latest snapshot, never an unbounded queue.

    Trade events remain durable in SQLite; coalescing these snapshots loses no
    events. An SMTP timeout cannot block the execution thread.
    """
    def __init__(self, notifier, telemetry=None, interval=10):
        self.notifier, self.telemetry, self.interval = notifier, telemetry, interval
        self.condition = threading.Condition()
        self.latest = None
        self.stopped = False
        self.errors = 0
        self.thread = threading.Thread(target=self._run, name='paper-notification', daemon=True)
        self.thread.start()

    def submit(self, positions, status):
        with self.condition:
            self.latest = copy.deepcopy(positions), status
            self.condition.notify()

    def _run(self):
        next_due = 0
        while True:
            with self.condition:
                self.condition.wait_for(lambda:self.stopped or
                    (self.latest is not None and time.monotonic() >= next_due), timeout=self.interval)
                if self.stopped:
                    return
                if self.latest is None or time.monotonic() < next_due:
                    continue
                value, self.latest = self.latest, None
            started = time.perf_counter_ns()
            try:
                self.notifier.tick(*value)
            except Exception:
                self.errors += 1
            finally:
                if self.telemetry:
                    self.telemetry.observe('notification', (time.perf_counter_ns()-started)/1e6)
                next_due = time.monotonic()+self.interval

    def close(self):
        with self.condition:
            self.stopped = True
            self.condition.notify_all()


class CycleSnapshotBus:
    """One Redis MGET per strategy scan, never reused in a later scan.

    Existing timestamp and quality gates still apply to every value. Execution
    matching keeps its original bus so it always reloads the latest book.
    """
    def __init__(self, bus, telemetry=None):
        self.bus, self.client, self.telemetry = bus, bus.client, telemetry
        self.cache = None

    def begin(self, symbols):
        import json
        self.cache = None
        keys = ['state','candidates','portfolio_returns']
        keys.extend(kind+':'+symbol for symbol in dict.fromkeys(symbols)
                    for kind in ('feature','liquidity'))
        started = time.perf_counter_ns()
        try:
            raw = self.client.mget(['quant:lab:'+key for key in keys])
            self.cache = {key:json.loads(value) if value else None for key,value in zip(keys,raw)}
        finally:
            if self.telemetry:
                self.telemetry.observe('redis_scan_snapshot',(time.perf_counter_ns()-started)/1e6)

    def end(self):
        self.cache = None

    def get(self, key):
        if self.cache is not None and key in self.cache:
            return self.cache[key]
        return self.bus.get(key)

    def __getattr__(self, name):
        return getattr(self.bus, name)


class DiagnosticBatchStore:
    """Coalesce display-only state; financial state never enters this buffer."""
    KEYS = {'config','market_unavailable','daily_loss_blocked','quant_system',
            'quant_recent_decisions','lab_gate','execution_statistics','execution_runtime','hybrid_compute'}
    PREFIXES = ('quant_grid:','signal:','sizing:','ai:')

    def __init__(self, store, cfg, telemetry=None):
        if cfg.mode!='paper' or cfg.live_enabled or cfg.api_key or cfg.api_secret:
            raise ValueError('Diagnostic batching requires credential-free paper mode')
        self.store, self.telemetry = store, telemetry
        self.values = OrderedDict()
        self.decisions = deque(maxlen=2000)
        self.dropped_diagnostics = 0
        self.cycle_cache = None

    def begin(self):
        self.cycle_cache = {}

    def end(self):
        self.cycle_cache = None

    def set(self, key, value):
        if self.cycle_cache is not None:
            self.cycle_cache[key]=copy.deepcopy(value)
        if key in self.KEYS or key.startswith(self.PREFIXES):
            if key not in self.values and len(self.values)>=512:
                self.values.popitem(last=False)
                self.dropped_diagnostics += 1
            self.values[key] = value
        else:
            # Includes positions, paper_balance, entry_context, exit intent,
            # cooldown, pause, orders and all unknown/new keys.
            return self.store.set(key,value)

    def get(self, key, default=None):
        if key in self.values:
            value=self.values[key]
        elif self.cycle_cache is not None and key in self.cycle_cache:
            value=self.cycle_cache[key]
        elif self.cycle_cache is not None:
            value=self.store.get(key,default)
            self.cycle_cache[key]=copy.deepcopy(value)
        else:
            value=self.store.get(key,default)
        return copy.deepcopy(value)

    def record_decision(self, body):
        if len(self.decisions)==self.decisions.maxlen:
            self.dropped_diagnostics += 1
        self.decisions.append((body['symbol'],body['feature_ts'],json.dumps(body,ensure_ascii=False,allow_nan=False)))

    def flush(self):
        if not self.values and not self.decisions:
            return
        started = time.perf_counter_ns()
        try:
            with self.store.connect() as c:
                c.executemany('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                    [(k,json.dumps(v,ensure_ascii=False,allow_nan=False)) for k,v in self.values.items()])
                if self.decisions:
                    c.executemany('INSERT OR IGNORE INTO quant_decisions VALUES (?,?,?)',self.decisions)
            self.values.clear()
            self.decisions.clear()
        finally:
            if self.telemetry:
                self.telemetry.observe('diagnostics_commit',(time.perf_counter_ns()-started)/1e6)

    def __getattr__(self, name):
        return getattr(self.store,name)
