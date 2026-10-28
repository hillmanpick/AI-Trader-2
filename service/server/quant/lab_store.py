"""Redis is disposable state; SQLite and bounded gzip archives hold evidence."""
import gzip
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import redis


class RealtimeBus:
    def __init__(self, url):
        self.client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=1, socket_connect_timeout=1)

    def put(self, key, value, ttl=15):
        self.client.set('quant:lab:' + key, json.dumps(value, allow_nan=False), ex=ttl)

    def get(self, key):
        raw = self.client.get('quant:lab:' + key)
        return json.loads(raw) if raw else None

    def event(self, kind, symbol=''):
        self.client.xadd('quant:lab:events', {'kind': kind, 'symbol': symbol, 'ts': str(int(time.time()*1000))}, maxlen=5000, approximate=True)


class LabStore:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'features.db'
        with self.connect() as c:
            c.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS samples (
                    symbol TEXT, ts INTEGER, price REAL, vector TEXT, meta TEXT,
                    PRIMARY KEY(symbol,ts));
                CREATE INDEX IF NOT EXISTS sample_time ON samples(ts);
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY, ts INTEGER, report TEXT);
                CREATE TABLE IF NOT EXISTS event_labels (
                    symbol TEXT, ts INTEGER, valid INTEGER, label INTEGER,
                    label_end INTEGER, body TEXT, PRIMARY KEY(symbol,ts));
                CREATE INDEX IF NOT EXISTS label_time ON event_labels(ts);
            ''')

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.path, timeout=10)
        c.execute('PRAGMA busy_timeout=10000')
        try:
            with c:
                yield c
        finally:
            c.close()

    def append(self, rows):
        with self.connect() as c:
            c.executemany('INSERT OR IGNORE INTO samples VALUES (?,?,?,?,?)',
                          [(r['symbol'], r['ts'], r['price'], json.dumps(r['vector'], allow_nan=False),
                            json.dumps(r['meta'], allow_nan=False)) for r in rows])

    def rows(self, limit=500000, stride_ms=30000):
        with self.connect() as c:
            # SQLite's MIN selects the accompanying columns from that same row.
            cursor = c.execute('SELECT symbol,MIN(ts),price,vector,meta FROM samples WHERE ts>? GROUP BY symbol,CAST(ts/? AS INTEGER) ORDER BY MIN(ts) LIMIT ?',
                               (int((time.time()-8*86400)*1000),stride_ms,limit))
            rows = []
            for s,t,p,v,m in cursor:
                meta = json.loads(m)
                # Filters are identical across thousands of samples. Keep the
                # causal fields in memory; full exchange rules remain archived.
                meta = {k:meta[k] for k in ('quality','schema','bid','ask','bid_depth','ask_depth',
                        'spread_bps','atr_proxy','funding_rate') if k in meta}
                rows.append({'symbol':s,'ts':t,'price':p,'vector':json.loads(v),'meta':meta})
        return rows

    def stats(self):
        with self.connect() as c:
            n, start, end = c.execute('SELECT COUNT(*),MIN(ts),MAX(ts) FROM samples').fetchone()
            report = c.execute('SELECT report FROM reports ORDER BY id DESC LIMIT 1').fetchone()
            labels = c.execute('SELECT COUNT(*),COALESCE(SUM(valid),0),MIN(ts),MAX(ts) FROM event_labels').fetchone()
            clean_rows = c.execute('''SELECT s.ts,s.meta,l.body
                FROM samples s JOIN event_labels l ON s.symbol=l.symbol AND s.ts=l.ts
                WHERE l.valid=1 ORDER BY s.ts''').fetchall()
        from .event_labels import LABEL_SCHEMA
        from .lab_features import SCHEMA
        clean_times = []
        for ts,meta,body in clean_rows:
            try:
                meta, label = json.loads(meta), json.loads(body)
            except (TypeError,ValueError,json.JSONDecodeError):
                continue
            # Keep this gate identical to labeled_rows; schema and label
            # versions must match before a row is counted as model evidence.
            if meta.get('schema') == SCHEMA and label.get('schema') == LABEL_SCHEMA:
                clean_times.append(ts)
        clean_start = min(clean_times) if clean_times else None
        clean_end = max(clean_times) if clean_times else None
        return {'samples': n, 'start_ms': start, 'end_ms': end,
                'days': (end-start)/86400000 if start is not None and end is not None else 0,
                'latest_report': json.loads(report[0]) if report else None,
                'event_labels':{'total':labels[0],'valid':labels[1],
                                'invalid':labels[0]-labels[1],'start_ms':labels[2],'end_ms':labels[3]},
                'clean_training':{'samples':len(clean_times),'start_ms':clean_start,'end_ms':clean_end,
                                  'days':(clean_end-clean_start)/86400000 if clean_start is not None else 0,
                                  'schema':SCHEMA,'label_schema':LABEL_SCHEMA}}

    def append_labels(self, labels):
        with self.connect() as c:
            c.executemany('INSERT OR IGNORE INTO event_labels VALUES (?,?,?,?,?,?)',
                [(r['symbol'],r['ts'],int(r['valid']),r['label'],r['label_end'],
                  json.dumps(r,allow_nan=False)) for r in labels])

    def labeled_rows(self, limit=500000):
        from .event_labels import LABEL_SCHEMA
        from .lab_features import SCHEMA
        with self.connect() as c:
            rows = c.execute('''SELECT s.symbol,s.ts,s.price,s.vector,s.meta,l.body
                FROM samples s JOIN event_labels l ON s.symbol=l.symbol AND s.ts=l.ts
                WHERE l.valid=1 ORDER BY s.ts,s.symbol LIMIT ?''',(limit,))
            result = []
            for symbol,ts,price,vector,meta,body in rows:
                meta, label = json.loads(meta),json.loads(body)
                if meta.get('schema') == SCHEMA and label.get('schema') == LABEL_SCHEMA:
                    meta = {k:meta[k] for k in ('quality','schema','bid','ask','bid_depth','ask_depth',
                            'spread_bps','atr_proxy','funding_rate') if k in meta}
                    result.append({'symbol':symbol,'ts':ts,'price':price,'vector':json.loads(vector),
                                   'meta':meta,'label':label['label'],'label_end':label['label_end'],
                                   'label_schema':label['schema']})
            return result

    def report(self, report):
        with self.connect() as c:
            c.execute('INSERT INTO reports(ts,report) VALUES (?,?)', (int(time.time()*1000), json.dumps(report, allow_nan=False)))

    def prune(self, days):
        with self.connect() as c:
            # Small periodic batches avoid long locks while collecting.
            c.execute('DELETE FROM samples WHERE rowid IN (SELECT rowid FROM samples WHERE ts<? LIMIT 50000)',
                      (int((time.time()-days*86400)*1000),))
            c.execute('DELETE FROM event_labels WHERE rowid IN (SELECT rowid FROM event_labels WHERE ts<? LIMIT 50000)',
                      (int((time.time()-days*86400)*1000),))


class RawArchive:
    def __init__(self, directory, max_mb_day):
        self.directory = Path(directory) / 'raw'
        self.directory.mkdir(parents=True, exist_ok=True)
        self.limit = max_mb_day * 1024 * 1024
        self.day = None
        self.bytes = 0
        self.dropped = 0
        self.file = None
        self.hour = None

    def append(self, payload):
        now = datetime.now(timezone.utc)
        day, hour = now.strftime('%Y%m%d'), now.strftime('%Y%m%d-%H')
        if day != self.day:
            self.day, self.bytes = day, sum(p.stat().st_size for p in self.directory.glob(day+'*.gz'))
        if self.bytes >= self.limit:
            self.dropped += 1
            return
        if hour != self.hour:
            if self.file:
                self.file.close()
            self.file = gzip.open(self.directory / (hour+'.jsonl.gz'), 'ab', compresslevel=1)
            self.hour = hour
        line = (json.dumps({'received_ms': int(time.time()*1000), 'data': payload}, separators=(',', ':'), allow_nan=False)+'\n').encode()
        self.file.write(line)
        # Persisted compressed bytes are measured after each bounded batch.

    def flush(self):
        if self.file:
            self.file.flush()
            self.bytes = sum(p.stat().st_size for p in self.directory.glob(self.day+'*.gz'))

    def prune(self, days):
        cutoff = time.time()-days*86400
        for path in self.directory.glob('*.jsonl.gz'):
            if path.stat().st_mtime < cutoff and not path.name.startswith(self.hour or 'never'):
                path.unlink()
