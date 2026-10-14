"""Read-only official community collector, separate from the trading loop."""
import json
import re
import time
from datetime import datetime, timezone

import requests

from .settings import Settings
from .store import Store, utcnow

OFFICIAL_URL = 'https://ai4trade.ai'
KINDS = ('operation', 'strategy', 'discussion')


def age_seconds(value, now=None):
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return ((now or datetime.now(timezone.utc)) - stamp).total_seconds()
    except (AttributeError, ValueError, TypeError, OverflowError):
        return float('inf')


def text(value, limit=4000):
    return str(value or '')[:limit]


def normalize(row, kind):
    if not isinstance(row, dict) or row.get('market') != 'crypto' or row.get('message_type') != kind:
        return None
    symbols = row.get('symbols')
    if not isinstance(symbols, list):
        symbols = []
    return {'signal_id': text(row.get('signal_id'), 80), 'agent_id': text(row.get('agent_id'), 40),
            'agent_name': text(row.get('agent_name'), 120), 'kind': kind,
            'symbol': text(row.get('symbol'), 30), 'symbols': [text(s, 30) for s in symbols[:10]],
            'side': text(row.get('side'), 20), 'title': text(row.get('title'), 240),
            'content': text(row.get('content')), 'created_at': text(row.get('created_at'), 60),
            'executed_at': text(row.get('executed_at'), 60)}


class CommunityClient:
    def __init__(self, cfg):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.trust_env = False

    def get(self, path, params=None, max_bytes=1_000_000):
        headers = {'Accept': 'application/json'}
        if self.cfg.community_token:
            headers['Authorization'] = 'Bearer ' + self.cfg.community_token
        try:
            with self.session.get(OFFICIAL_URL + '/api' + path, params=params, headers=headers,
                                  timeout=(4, 10), allow_redirects=False, stream=True) as response:
                if response.status_code != 200:
                    raise RuntimeError(f'官方社区接口 HTTP {response.status_code}')
                raw = bytearray()
                for chunk in response.iter_content(16384):
                    raw.extend(chunk)
                    if len(raw) > max_bytes:
                        raise RuntimeError('官方社区响应超出大小限制')
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise ValueError()
                return result
        except (requests.RequestException, ValueError):
            raise RuntimeError('官方社区暂时不可用或响应格式异常') from None

    def collect(self, store):
        previous = store.get('community', {})
        snapshot = {'source': OFFICIAL_URL, 'feeds': dict(previous.get('feeds', {})),
                    'feed_updated_at': dict(previous.get('feed_updated_at', {})),
                    'totals': dict(previous.get('totals', {})), 'top_agents': previous.get('top_agents', []),
                    'leaderboard_updated_at': previous.get('leaderboard_updated_at'), 'checked_at': utcnow()}
        errors = {}
        for kind in KINDS:
            try:
                result = self.get('/signals/feed', {'market': 'crypto', 'message_type': kind, 'limit': 30, 'sort': 'new'})
                if not isinstance(result.get('signals'), list):
                    raise RuntimeError('官方社区信号格式异常')
                snapshot['feeds'][kind] = [item for row in result['signals'][:30] if (item := normalize(row, kind))]
                snapshot['totals'][kind] = result.get('total') if isinstance(result.get('total'), int) else None
                snapshot['feed_updated_at'][kind] = utcnow()
            except RuntimeError as exc:
                errors[kind] = str(exc)
        try:
            result = self.get('/profit/history', {'limit': 10, 'include_history': 'false'})
            if not isinstance(result.get('top_agents'), list):
                raise RuntimeError('官方排行榜格式异常')
            snapshot['top_agents'] = [{key: row.get(key) for key in ('agent_id', 'name', 'total_profit_percent', 'trade_count', 'is_verified')}
                                      for row in result['top_agents'][:10] if isinstance(row, dict)]
            snapshot['leaderboard_updated_at'] = utcnow()
        except RuntimeError as exc:
            errors['leaderboard'] = str(exc)
        snapshot['errors'] = errors
        store.set('community', snapshot)


def recent_context(snapshot, symbol, now=None):
    """Only timely, relevant public text; no credentials or private account values."""
    base = symbol.removesuffix('USDT')
    items, seen = [], set()
    for kind in ('operation', 'strategy'):
        if not 0 <= age_seconds(snapshot.get('feed_updated_at', {}).get(kind), now) <= 180:
            continue
        for row in snapshot.get('feeds', {}).get(kind, []):
            if base not in [row.get('symbol'), *(row.get('symbols') or [])] and symbol not in [row.get('symbol'), *(row.get('symbols') or [])]:
                continue
            stamp = row.get('executed_at') or row.get('created_at')
            if not -60 <= age_seconds(stamp, now) <= 3600:
                continue
            content = text(row.get('content'), 1000)
            # Identical copied text is not independent market evidence.
            original = re.sub(r'^(?:\[Copied from [^\]]{1,120}\]\s*)+', '', content, flags=re.IGNORECASE)
            identity = (kind, row.get('side'), original.strip())
            if identity in seen:
                continue
            seen.add(identity)
            items.append({key: row.get(key) for key in ('signal_id', 'agent_name', 'kind', 'side', 'title', 'created_at')} | {'content': content})
            if len(items) == 8:
                return items
    return items


def main():
    import config  # noqa: F401; same dotenv setup as quant worker
    cfg = Settings()
    store = Store(cfg.db_path)
    client = CommunityClient(cfg)
    from .official import OfficialReads
    official = OfficialReads(cfg, store)
    while True:
        if cfg.community_enabled:
            try:
                client.collect(store)
                official.warm()
            except Exception:
                # Keep external response bodies and credentials out of logs.
                print('Community collection failed; previous snapshot preserved', flush=True)
        time.sleep(max(30, cfg.community_poll_seconds))


if __name__ == '__main__':
    main()
