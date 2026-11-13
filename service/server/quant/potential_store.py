"""Small radar observations and immutable future outcomes, separate from trading."""
import json
import sqlite3
import time
import zlib
from contextlib import contextmanager
from pathlib import Path

from .potential_radar import SCHEMA,BAR_MS,HORIZON_MS


class PotentialStore:
    def __init__(self,directory):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'potential-radar.db'
        with self.connect() as c:
            c.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS radar_state (key TEXT PRIMARY KEY,body TEXT);
                CREATE TABLE IF NOT EXISTS radar_observations (
                    symbol TEXT, bucket INTEGER, ts INTEGER, start_ms INTEGER, end_ms INTEGER,
                    score REAL, body TEXT, outcome TEXT, path BLOB, retry_after INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(symbol,bucket));
                CREATE INDEX IF NOT EXISTS radar_due ON radar_observations(end_ms) WHERE outcome IS NULL;
                CREATE INDEX IF NOT EXISTS radar_time ON radar_observations(ts);
            ''')
            if 'retry_after' not in {r[1] for r in c.execute('PRAGMA table_info(radar_observations)')}:
                c.execute('ALTER TABLE radar_observations ADD COLUMN retry_after INTEGER NOT NULL DEFAULT 0')

    @contextmanager
    def connect(self):
        c=sqlite3.connect(self.path,timeout=10);c.row_factory=sqlite3.Row
        try:
            with c:yield c
        finally:c.close()

    def put(self,key,body):
        with self.connect() as c:
            c.execute('INSERT INTO radar_state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body',
                      (key,json.dumps(body,ensure_ascii=False,allow_nan=False)))

    def get(self,key,default=None):
        with self.connect() as c:row=c.execute('SELECT body FROM radar_state WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else default

    def observe(self,rows,now_ms):
        # One immutable observation per symbol/hour, including low-score controls.
        start=(now_ms//BAR_MS+1)*BAR_MS
        with self.connect() as c:
            for row in rows:
                if row.get('schema')!=SCHEMA or row['ts']!=now_ms:raise ValueError('Observation clock/schema mismatch')
                c.execute('INSERT OR IGNORE INTO radar_observations '
                    '(symbol,bucket,ts,start_ms,end_ms,score,body,outcome,path) VALUES (?,?,?,?,?,?,?,?,?)',
                    (row['symbol'],now_ms//3600000,now_ms,start,start+HORIZON_MS,row['score'],
                     json.dumps(row,ensure_ascii=False,allow_nan=False),None,None))

    def due(self,now_ms,limit=8):
        with self.connect() as c:
            return [dict(r) for r in c.execute('SELECT symbol,bucket,start_ms,end_ms FROM radar_observations '
                'WHERE outcome IS NULL AND end_ms<=? AND retry_after<=? ORDER BY end_ms LIMIT ?',
                (now_ms,now_ms,limit))]

    def defer(self,row,retry_after):
        with self.connect() as c:
            c.execute('UPDATE radar_observations SET retry_after=? WHERE symbol=? AND bucket=? AND outcome IS NULL',
                      (retry_after,row['symbol'],row['bucket']))

    def finish(self,row,result):
        result=dict(result);path=result.pop('path',None)
        if result.get('label_end')!=row['end_ms']:raise ValueError('Outcome horizon mismatch')
        with self.connect() as c:
            c.execute('UPDATE radar_observations SET outcome=?,path=? WHERE symbol=? AND bucket=? AND outcome IS NULL',
                (json.dumps(result,allow_nan=False),zlib.compress(json.dumps(path).encode()) if path else None,
                 row['symbol'],row['bucket']))

    def training_rows(self):
        with self.connect() as c:
            rows=c.execute('SELECT ts,body,outcome FROM radar_observations WHERE outcome IS NOT NULL ORDER BY ts,symbol')
            return [json.loads(r['body'])|{'outcome':json.loads(r['outcome'])} for r in rows
                    if json.loads(r['outcome']).get('valid')]

    def summary(self,now_ms=None):
        now_ms=int(time.time()*1000) if now_ms is None else now_ms
        with self.connect() as c:
            counts=c.execute("SELECT COUNT(*),SUM(outcome IS NULL),SUM(json_extract(outcome,'$.valid')=1),"
                "SUM(json_extract(outcome,'$.valid')=0),SUM(json_extract(outcome,'$.label')=1),"
                "MIN(ts),MAX(ts) FROM radar_observations").fetchone()
            recent=[json.loads(r['body'])|{'outcome':json.loads(r['outcome'])} for r in c.execute(
                'SELECT body,outcome FROM radar_observations WHERE outcome IS NOT NULL ORDER BY ts DESC LIMIT 20')]
        state=self.get('status',{})
        return state|{'training':self.get('training',{}),'stale':not 0<=now_ms-state.get('ts',0)<=180000,
            'observations':counts[0],'pending':counts[1] or 0,'valid_outcomes':counts[2] or 0,
            'invalid_outcomes':counts[3] or 0,'up_first':counts[4] or 0,'first_observation_ms':counts[5],
            'last_observation_ms':counts[6],'recent_outcomes':recent,
            'database_bytes':sum(p.stat().st_size for p in self.directory.glob('potential-radar.db*') if p.is_file()),
            'scope':'binance_usdt_perpetual_watch_only','execution_enabled':False,
            'evaluation':{'horizon_hours':4,'target_pct':10,'stop_pct':5,'entry':'next_completed_boundary_open',
                          'note':'后续路径统计是假设观察，不是模拟账户交易或已验证收益'}}

    def prune(self,now_ms,days=30):
        with self.connect() as c:
            c.execute('DELETE FROM radar_observations WHERE rowid IN (SELECT rowid FROM radar_observations '
                'WHERE ts<? ORDER BY ts LIMIT 5000)',(now_ms-days*86400000,))
