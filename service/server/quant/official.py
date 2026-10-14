"""Bounded, cached public reads. Never forward browser or account credentials."""
import hashlib
import json
import re
import threading
import time
from dataclasses import replace
from urllib.parse import urlencode

from fastapi import HTTPException

from .community import CommunityClient, OFFICIAL_URL, age_seconds
from .store import utcnow

LOCKS = [threading.Lock() for _ in range(32)]
STATIC = {
    'signals/grouped', 'signals/feed', 'profit/history', 'trending', 'claw/agents/count',
    'challenges', 'market-intel/macro-signals', 'market-intel/etf-flows',
    'market-intel/stocks/featured', 'market-intel/news',
}
PARAMS = {
    'limit': (1, 100), 'offset': (0, 10000), 'days': (1, 30),
    'market': {'all', 'crypto', 'us-stock', 'polymarket', 'forex', 'commodity', 'futures', 'cn-stock', 'hk-stock'},
    'message_type': {'operation', 'strategy', 'discussion'},
    'sort': {'new', 'active', 'hot', 'quality'}, 'metric': {'return', 'drawdown', 'risk', 'collaboration', 'quality'},
    'status': {'upcoming', 'active', 'settled'}, 'include_history': {'true', 'false'},
    'category': {'equities', 'crypto', 'macro', 'forex'},
}


def validate(path, pairs):
    dynamic = (re.fullmatch(r'signals/[1-9][0-9]{0,11}(?:/replies)?', path)
               or re.fullmatch(r'agents/[1-9][0-9]{0,11}/(?:summary|positions)', path)
               or re.fullmatch(r'market-intel/stocks/[A-Za-z0-9.^-]{1,15}/(?:latest|history)', path)
               or re.fullmatch(r'challenges/(?!me$)[A-Za-z0-9_-]{1,100}(?:/(?:leaderboard|team-leaderboard|teams|submissions))?', path))
    if path not in STATIC and not dynamic:
        raise HTTPException(404, '此接口不属于官方公开数据')
    params = {}
    for key, value in pairs:
        rule = PARAMS.get(key)
        if rule is None or key in params or len(value) > 30:
            raise HTTPException(400, '不支持的查询参数')
        if isinstance(rule, tuple):
            if not value.isascii() or not value.isdigit() or not rule[0] <= int(value) <= rule[1]:
                raise HTTPException(400, '查询范围超出限制')
            value = str(int(value))
        elif value not in rule:
            raise HTTPException(400, '不支持的筛选条件')
        params[key] = value
    return params


class OfficialReads:
    def __init__(self, cfg, store):
        self.cfg, self.store = cfg, store
        # Public views must never impersonate an account, even if a token is configured.
        self.client = CommunityClient(replace(cfg, community_token=''))
        with store.connect() as c:
            c.execute('CREATE TABLE IF NOT EXISTS official_cache (key TEXT PRIMARY KEY, payload TEXT NOT NULL, checked REAL NOT NULL)')

    def read(self, path, pairs=()):
        params = validate(path, pairs)
        if not self.cfg.community_enabled:
            raise HTTPException(503, '官方公开数据同步未启用')
        key = path + '?' + urlencode(sorted(params.items()))
        lock = LOCKS[int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(LOCKS)]
        try:
            with lock:
                return self._read(key, path, params)
        finally:
            self.client.session.close()

    def _read(self, key, path, params):
        ttl = 900 if path.startswith('market-intel/') else 60
        with self.store.connect() as c:
            row = c.execute('SELECT payload,checked FROM official_cache WHERE key=?', (key,)).fetchone()
        entry = json.loads(row['payload']) if row else {}
        retry_due = not row or time.time() - row['checked'] >= 30
        if age_seconds(entry.get('fetched_at')) > ttl and retry_due:
            try:
                data = self.client.get('/' + path, params, max_bytes=4_000_000)
                entry = {'data': data, 'fetched_at': utcnow()}
            except RuntimeError:
                entry['error'] = '官方接口暂时不可用'
            with self.store.connect() as c:
                c.execute('INSERT INTO official_cache VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,checked=excluded.checked',
                          (key, json.dumps(entry, ensure_ascii=False), time.time()))
                c.execute('DELETE FROM official_cache WHERE key IN (SELECT key FROM official_cache ORDER BY checked DESC LIMIT -1 OFFSET 128)')
        if 'data' not in entry or age_seconds(entry.get('fetched_at')) > 86400:
            raise HTTPException(503, '官方接口暂时不可用，请稍后重试')
        data = entry['data']
        return data | {'_official': {
            'source': OFFICIAL_URL, 'path': path, 'fetched_at': entry['fetched_at'],
            'stale': age_seconds(entry['fetched_at']) > ttl, 'warning': entry.get('error'),
            'data_as_of': data.get('last_updated_at') or data.get('created_at'),
            'read_only': True,
        }}

    def warm(self):
        from concurrent.futures import ThreadPoolExecutor
        catalog = [
            ('signals/grouped', {'message_type': 'operation', 'limit': '20', 'offset': '0'}),
            ('signals/feed', {'message_type': 'strategy', 'limit': '20', 'offset': '0', 'sort': 'active'}),
            ('signals/feed', {'message_type': 'discussion', 'limit': '20', 'offset': '0', 'sort': 'active'}),
            ('profit/history', {'limit': '20', 'offset': '0', 'days': '1', 'metric': 'return'}),
            ('trending', {'limit': '10'}), ('claw/agents/count', {}),
            ('challenges', {'status': 'active', 'limit': '100'}),
            ('challenges', {'status': 'settled', 'limit': '100'}),
            ('market-intel/macro-signals', {}), ('market-intel/etf-flows', {}),
            ('market-intel/stocks/featured', {'limit': '10'}), ('market-intel/news', {'limit': '12'}),
            (f'agents/{self.cfg.community_agent_id}/positions', {}),
        ]
        def fetch(item):
            try:
                # Each thread owns its requests.Session.
                OfficialReads(self.cfg, self.store).read(item[0], item[1].items())
            except (HTTPException, RuntimeError):
                pass
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(fetch, catalog))
