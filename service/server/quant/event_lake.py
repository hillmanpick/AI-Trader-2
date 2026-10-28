"""Durable, ordered market events in checksummed, atomic Parquet segments.

Receive clocks are captured at ingress, not at a later archive flush. Individual
trade IDs and optional aggregate IDs remain distinct in the on-disk schema.
"""
import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def envelope(payload, source='websocket', received_ns=None, monotonic_ns=None):
    received_ns = time.time_ns() if received_ns is None else int(received_ns)
    kind = payload.get('e','unknown')
    identity = payload.get('t') if kind == 'trade' else payload.get('a') if kind == 'aggTrade' else payload.get('u') if kind in ('depthUpdate','bookTicker') else None
    symbol = payload.get('s','')
    event_ms = int(payload.get('E',payload.get('T',received_ns//1000000)))
    return {'event_id':f'{kind}:{symbol}:{identity}' if identity is not None else uuid.uuid4().hex,
            'kind':kind, 'symbol':symbol,'event_ms':event_ms,'received_ns':received_ns,
            'monotonic_ns':time.perf_counter_ns() if monotonic_ns is None else int(monotonic_ns),
            'source':source,'payload':payload}


def arrow_schema():
    import pyarrow as pa
    return pa.schema([
        ('event_id',pa.string()),('kind',pa.string()),('symbol',pa.string()),
        ('event_ms',pa.int64()),('received_ns',pa.int64()),('monotonic_ns',pa.int64()),
        ('source',pa.string()),('aggregate_id',pa.int64()),('first_trade_id',pa.int64()),
        ('last_trade_id',pa.int64()),('price',pa.string()),('quantity',pa.string()),
        ('buyer_maker',pa.bool_()),('first_update_id',pa.int64()),
        ('last_update_id',pa.int64()),('previous_update_id',pa.int64()),('payload',pa.string()),
    ],metadata={b'quant.schema':b'events-v1',b'quant.trade_semantics':b'individual-trade-and-explicit-aggregate'})


def column_row(event):
    p = event['payload']
    row = {k:event[k] for k in ('event_id','kind','symbol','event_ms','received_ns','monotonic_ns','source')}
    row |= {'aggregate_id':None,'first_trade_id':None,'last_trade_id':None,'price':None,
            'quantity':None,'buyer_maker':None,'first_update_id':None,'last_update_id':None,
            'previous_update_id':None}
    if event['kind'] in ('trade','aggTrade'):
        first,last = (int(p['t']),int(p['t'])) if event['kind']=='trade' else (int(p['f']),int(p['l']))
        row.update(aggregate_id=int(p['a']) if event['kind']=='aggTrade' else None,first_trade_id=first,last_trade_id=last,
                   price=str(p['p']),quantity=str(p['q']),buyer_maker=bool(p['m']))
        # Common trade fields become typed columns, rather than repeated JSON.
        extras = {k:v for k,v in p.items() if k not in ('e','s','E','T','t','a','f','l','p','q','m')}
        row['payload'] = json.dumps({'T':int(p['T']),**extras},separators=(',',':'),allow_nan=False)
    else:
        for key,field in (('U','first_update_id'),('u','last_update_id'),('pu','previous_update_id')):
            if key in p:
                row[field] = int(p[key])
        row['payload'] = json.dumps(p,separators=(',',':'),allow_nan=False)
    return row


def decode_row(row):
    p = json.loads(row['payload'])
    if row['kind'] in ('trade','aggTrade'):
        p |= {'e':row['kind'],'s':row['symbol'],'E':row['event_ms'],'p':row['price'],
              'q':row['quantity'],'m':row['buyer_maker']}
        if row['kind']=='trade':
            p['t'] = row['first_trade_id']
        else:
            p |= {'a':row['aggregate_id'],'f':row['first_trade_id'],'l':row['last_trade_id']}
    return {k:row[k] for k in ('event_id','kind','symbol','event_ms','received_ns','monotonic_ns','source')} | {'payload':p}


class EventLake:
    def __init__(self, directory, max_bytes=6*1024**3, retention_hours=336):
        self.directory = Path(directory)/'events'
        self.directory.mkdir(parents=True,exist_ok=True)
        self.path = self.directory/'catalog.db'
        self.max_bytes,self.retention_hours = max_bytes,retention_hours
        with self.connect() as c:
            c.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS segments (
                  path TEXT PRIMARY KEY, first_ns INTEGER, last_ns INTEGER,
                  rows INTEGER, bytes INTEGER, sha256 TEXT, created_ns INTEGER);
                CREATE INDEX IF NOT EXISTS segment_time ON segments(first_ns,last_ns);
                CREATE TABLE IF NOT EXISTS gaps (
                  id INTEGER PRIMARY KEY, ts_ns INTEGER, symbol TEXT, kind TEXT,
                  start_id INTEGER, end_id INTEGER, status TEXT, detail TEXT);
                CREATE TABLE IF NOT EXISTS checkpoints (
                  symbol TEXT PRIMARY KEY, trade_id INTEGER, ts_ns INTEGER);
                CREATE TABLE IF NOT EXISTS exports (
                  path TEXT PRIMARY KEY, target TEXT, ts_ns INTEGER);
            ''')

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.path,timeout=20)
        c.row_factory = sqlite3.Row
        try:
            with c:
                yield c
        finally:
            c.close()

    def recover(self):
        """Adopt fully committed orphan files after rename/catalog crash windows."""
        import pyarrow.parquet as pq
        with self.connect() as c:
            known = {r[0] for r in c.execute('SELECT path FROM segments')}
        for path in self.directory.glob('date=*/hour=*/*.parquet'):
            relative = str(path.relative_to(self.directory)).replace('\\','/')
            if relative in known:
                continue
            table = pq.ParquetFile(path).read(columns=['received_ns','kind','symbol','first_trade_id'])
            stamps = table['received_ns'].to_pylist()
            if not stamps:
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.connect() as c:
                c.execute('INSERT OR IGNORE INTO segments VALUES (?,?,?,?,?,?,?)',
                          (relative,min(stamps),max(stamps),len(stamps),path.stat().st_size,digest,time.time_ns()))
                for row in table.to_pylist():
                    if row['kind'] == 'trade':
                        self._checkpoint(c,row['symbol'],row['first_trade_id'],row['received_ns'])

    @staticmethod
    def _checkpoint(c,symbol,identity,stamp):
        c.execute('INSERT INTO checkpoints VALUES (?,?,?) ON CONFLICT(symbol) DO UPDATE SET '
                  'trade_id=MAX(trade_id,excluded.trade_id),ts_ns=MAX(ts_ns,excluded.ts_ns)',
                  (symbol,identity,stamp))

    def checkpoints(self):
        with self.connect() as c:
            return {r['symbol']:r['trade_id'] for r in c.execute('SELECT * FROM checkpoints')}

    def append(self, events):
        if not events:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq
        # A segment spans one receive-hour, so pruning never splits a file.
        groups = {}
        for event in events:
            dt = datetime.fromtimestamp(event['received_ns']/1e9,timezone.utc)
            partition = dt.strftime('date=%Y-%m-%d/hour=%H')
            groups.setdefault(partition,[]).append(column_row(event))
        for partition,rows in groups.items():
            rows.sort(key=lambda r:r['received_ns'])
            folder = self.directory/partition
            folder.mkdir(parents=True,exist_ok=True)
            path = folder/f"{rows[0]['received_ns']}-{uuid.uuid4().hex[:12]}.parquet"
            temp = path.with_suffix('.pending')
            table = pa.Table.from_pylist(rows,schema=arrow_schema())
            pq.write_table(table,temp,compression='zstd',compression_level=3,
                           use_dictionary=['kind','symbol','source'],row_group_size=10000)
            with open(temp,'r+b') as handle:
                os.fsync(handle.fileno())
            os.replace(temp,path)
            if os.name != 'nt':
                fd = os.open(folder,os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            relative = str(path.relative_to(self.directory)).replace('\\','/')
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.connect() as c:
                c.execute('INSERT INTO segments VALUES (?,?,?,?,?,?,?)',
                          (relative,rows[0]['received_ns'],rows[-1]['received_ns'],len(rows),
                           path.stat().st_size,digest,time.time_ns()))
                checkpoints = {}
                for row in rows:
                    if row['kind'] == 'trade':
                        prior = checkpoints.get(row['symbol'])
                        if prior is None or row['first_trade_id'] > prior['first_trade_id']:
                            checkpoints[row['symbol']] = row
                for row in checkpoints.values():
                    self._checkpoint(c,row['symbol'],row['first_trade_id'],row['received_ns'])

    def gap(self,symbol,kind,start,end,status='unresolved',detail=''):
        with self.connect() as c:
            c.execute('INSERT INTO gaps(ts_ns,symbol,kind,start_id,end_id,status,detail) VALUES (?,?,?,?,?,?,?)',
                      (time.time_ns(),symbol,kind,int(start),int(end),status,detail))

    def segments(self,start_ns=0,end_ns=2**63-1):
        with self.connect() as c:
            return [dict(r) for r in c.execute('SELECT * FROM segments WHERE last_ns>=? AND first_ns<=? ORDER BY first_ns,path',
                                              (start_ns,end_ns))]

    def iter_events(self,start_ns=0,end_ns=2**63-1,symbols=None,kinds=None,verify=True):
        """Merge overlapping segments by receive time with bounded record batches."""
        import heapq
        import pyarrow.parquet as pq
        import pyarrow as pa
        import pyarrow.compute as pc
        segments = self.segments(start_ns,end_ns)
        # Files from the single writer are normally disjoint. Keep only files
        # whose ranges overlap in a merge group; do not open weeks of files.
        groups = []
        for s in segments:
            if not groups or s['first_ns'] > max(x['last_ns'] for x in groups[-1]):
                groups.append([s])
            else:
                groups[-1].append(s)

        def rows(segment):
            path = self.directory/segment['path']
            if verify and hashlib.sha256(path.read_bytes()).hexdigest() != segment['sha256']:
                raise ValueError('Event segment checksum mismatch: '+segment['path'])
            for batch in pq.ParquetFile(path).iter_batches(batch_size=2048):
                mask = pc.and_(pc.greater_equal(batch['received_ns'],start_ns),pc.less_equal(batch['received_ns'],end_ns))
                if symbols:
                    mask = pc.and_(mask,pc.is_in(batch['symbol'],value_set=pa.array(sorted(symbols))))
                if kinds:
                    mask = pc.and_(mask,pc.is_in(batch['kind'],value_set=pa.array(sorted(kinds))))
                for row in batch.filter(mask).to_pylist():
                    if (start_ns <= row['received_ns'] <= end_ns and (not symbols or row['symbol'] in symbols)
                            and (not kinds or row['kind'] in kinds)):
                        yield decode_row(row)
        for group in groups:
            yield from heapq.merge(*(rows(s) for s in group),key=lambda r:r['received_ns'])

    def prune(self):
        cutoff = time.time_ns()-self.retention_hours*3600*10**9
        segments = self.segments()
        total = sum(s['bytes'] for s in segments)
        for s in segments:
            if s['last_ns'] >= cutoff and total <= self.max_bytes:
                break
            path = self.directory/s['path']
            # Retention eviction is itself evidence, never silent truncation.
            self.gap('*','retention',s['first_ns'],s['last_ns'],'evicted',s['path'])
            with self.connect() as c:
                c.execute('DELETE FROM segments WHERE path=?',(s['path'],))
            path.unlink(missing_ok=True)
            total -= s['bytes']

    def stats(self):
        with self.connect() as c:
            row = c.execute('SELECT COUNT(*) segments,COALESCE(SUM(rows),0) rows,COALESCE(SUM(bytes),0) bytes,MIN(first_ns) first_ns,MAX(last_ns) last_ns FROM segments').fetchone()
            kinds = [dict(r) for r in c.execute('SELECT kind,status,COUNT(*) count FROM gaps GROUP BY kind,status')]
            symbols = c.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0]
        return dict(row) | {'symbols_with_durable_trades':symbols,'gaps':kinds,
                            'format':'Parquet/ZSTD events-v1','max_bytes':self.max_bytes}

    def export_clickhouse(self,url,database='quant',table='events',username='',password=''):
        """Optional external sink. Insert tokens make retries of a segment idempotent."""
        import re
        import requests
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',database+table):
            raise ValueError('Invalid ClickHouse identifier')
        session = requests.Session()
        session.trust_env = False
        if username:
            session.auth = (username,password)
        def query(sql,data=None,params=None):
            r = session.post(url,params={'query':sql,**(params or {})},data=data,timeout=(5,60))
            if not r.ok:
                raise RuntimeError(f'ClickHouse HTTP {r.status_code}')
        query(f'CREATE DATABASE IF NOT EXISTS {database}')
        query(f'''CREATE TABLE IF NOT EXISTS {database}.{table} (
            event_id String,kind LowCardinality(String),symbol LowCardinality(String),
            event_ms Int64,received_ns Int64,monotonic_ns Int64,source LowCardinality(String),
            aggregate_id Nullable(Int64),first_trade_id Nullable(Int64),last_trade_id Nullable(Int64),
            price Nullable(String),quantity Nullable(String),buyer_maker Nullable(Bool),
            first_update_id Nullable(Int64),last_update_id Nullable(Int64),previous_update_id Nullable(Int64),payload String)
            ENGINE=ReplacingMergeTree ORDER BY (symbol,kind,received_ns,event_id)''')
        exported = 0
        for s in self.segments():
            with self.connect() as c:
                done = c.execute('SELECT 1 FROM exports WHERE path=? AND target=?',(s['path'],url)).fetchone()
            if done:
                continue
            path = self.directory/s['path']
            if hashlib.sha256(path.read_bytes()).hexdigest() != s['sha256']:
                raise ValueError('Corrupt export segment')
            with open(path,'rb') as body:
                query(f'INSERT INTO {database}.{table} FORMAT Parquet',body,
                      {'insert_deduplication_token':s['sha256']})
            with self.connect() as c:
                c.execute('INSERT OR REPLACE INTO exports VALUES (?,?,?)',(s['path'],url,time.time_ns()))
            exported += 1
        return exported
