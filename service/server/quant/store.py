import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


def utcnow():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as c:
            c.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY, symbol TEXT NOT NULL, status TEXT NOT NULL,
                    payload TEXT NOT NULL, response TEXT, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY, kind TEXT NOT NULL, body TEXT NOT NULL,
                    created_at TEXT NOT NULL, mailed_at TEXT);
            ''')

    @contextmanager
    def connect(self):
        c = sqlite3.connect(self.path, timeout=15)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA synchronous=FULL')
        try:
            with c:
                yield c
        finally:
            c.close()

    def get(self, key, default=None):
        with self.connect() as c:
            r = c.execute('SELECT value FROM kv WHERE key=?', (key,)).fetchone()
        return json.loads(r['value']) if r else default

    def set(self, key, value):
        with self.connect() as c:
            c.execute('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key,json.dumps(value,ensure_ascii=False)))

    def event(self, kind, body):
        with self.connect() as c:
            c.execute('INSERT INTO events(kind,body,created_at) VALUES (?,?,?)', (kind,body,utcnow()))

    def transition(self, positions, kind, body):
        # Position transition and its notification survive a process crash together.
        with self.connect() as c:
            c.execute('INSERT INTO kv VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', ('positions',json.dumps(positions)))
            c.execute('INSERT INTO events(kind,body,created_at) VALUES (?,?,?)', (kind,body,utcnow()))

    def events(self, pending=False, limit=100):
        with self.connect() as c:
            query = 'SELECT * FROM events' + (' WHERE mailed_at IS NULL' if pending else '')
            query += ' ORDER BY id ' + ('ASC' if pending else 'DESC') + ' LIMIT ?'
            return [dict(r) for r in c.execute(query,(limit,))]

    def mark_mailed(self, ids):
        with self.connect() as c:
            c.executemany('UPDATE events SET mailed_at=? WHERE id=?', [(utcnow(),i) for i in ids])

    def intent(self, order_id, symbol, payload):
        with self.connect() as c:
            c.execute('INSERT INTO orders VALUES (?,?,?,?,?,?)', (order_id,symbol,'PENDING',json.dumps(payload),None,utcnow()))

    def order_result(self, order_id, status, response):
        with self.connect() as c:
            c.execute('UPDATE orders SET status=?,response=? WHERE id=?', (status,json.dumps(response),order_id))

    def orders(self, pending=False):
        with self.connect() as c:
            return [dict(r) for r in c.execute('SELECT * FROM orders' + (" WHERE status='PENDING'" if pending else '') + ' ORDER BY created_at DESC LIMIT 200')]

    def owns(self, symbol):
        with self.connect() as c:
            return c.execute('SELECT 1 FROM orders WHERE symbol=? LIMIT 1',(symbol,)).fetchone() is not None
