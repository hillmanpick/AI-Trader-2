import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.official import OfficialReads, validate
from quant.settings import Settings
from quant.store import Store
from routes_quant import register_quant_routes


@pytest.fixture
def service(tmp_path):
    cfg = Settings(db_path=str(tmp_path/'quant.db'), community_enabled=True, community_token='private-official-token', operator_token='operator')
    reader = OfficialReads(cfg, Store(cfg.db_path))
    reader.client.get = MagicMock(return_value={'signals': [{'signal_id': 5}], 'total': 1})
    return reader


@pytest.mark.parametrize('path', ['positions', 'claw/agents/me', 'signals/following', 'challenges/me',
                                  'challenges/test/portfolio', 'https://other.test', '../claw/agents/me', 'agents/1/positions/../../me'])
def test_only_public_paths_allowed(path):
    with pytest.raises(HTTPException) as exc:
        validate(path, [])
    assert exc.value.status_code == 404


@pytest.mark.parametrize('pairs', [[('token', 'secret')], [('limit', '101')], [('offset', '-1')],
                                   [('limit', '20'), ('limit', '21')], [('market', 'unknown')], [('sort', 'following')]])
def test_private_or_unbounded_queries_rejected(pairs):
    with pytest.raises(HTTPException) as exc:
        validate('signals/feed', pairs)
    assert exc.value.status_code == 400


def test_public_client_drops_account_token_and_canonicalizes_cache(service):
    assert service.client.cfg.community_token == ''
    first = service.read('signals/feed', [('limit', '020'), ('sort', 'active')])
    second = service.read('signals/feed', [('sort', 'active'), ('limit', '20')])
    assert first == second
    assert first['signals'][0]['signal_id'] == 5
    assert first['_official']['read_only'] and not first['_official']['stale']
    assert service.client.get.call_count == 1
    assert 'private-official-token' not in json.dumps(first)


def expire(service, seconds):
    with service.store.connect() as c:
        row = c.execute('SELECT key,payload FROM official_cache').fetchone()
        entry = json.loads(row['payload'])
        entry['fetched_at'] = (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()
        c.execute('UPDATE official_cache SET payload=?,checked=0', (json.dumps(entry),))


def test_failure_keeps_last_success_and_backoff_does_not_masquerade_as_fresh(service):
    service.read('signals/feed')
    expire(service, 120)
    service.client.get.side_effect = RuntimeError('upstream sensitive response')
    old = service.read('signals/feed')
    assert old['signals'][0]['signal_id'] == 5 and old['_official']['stale']
    assert old['_official']['warning'] == '官方接口暂时不可用'
    assert 'sensitive' not in json.dumps(old)
    assert service.read('signals/feed') == old
    assert service.client.get.call_count == 2
    expire(service, 90000)
    with pytest.raises(HTTPException) as exc:
        service.read('signals/feed')
    assert exc.value.status_code == 503


def test_no_initial_data_is_reported_as_failure_not_empty_success(service):
    service.client.get.side_effect = RuntimeError('private response')
    for _ in range(2):
        with pytest.raises(HTTPException) as exc:
            service.read('signals/feed')
        assert exc.value.status_code == 503
    assert service.client.get.call_count == 1


def test_auth_and_read_only_route(service):
    app = FastAPI()
    with patch('routes_quant.Settings', return_value=service.cfg):
        register_quant_routes(app)
    client = TestClient(app)
    path = '/api/quant/official/signals/feed?limit=20'
    assert client.get(path).status_code == 401
    with patch('routes_quant.OfficialReads', return_value=service):
        response = client.get(path, headers={'X-Quant-Token': 'operator', 'Authorization': 'Bearer local-private-token'})
    assert response.status_code == 200
    assert service.client.get.call_args.args == ('/signals/feed', {'limit': '20'})
    assert 'local-private-token' not in json.dumps(service.client.get.call_args.kwargs)
    assert client.post(path, headers={'X-Quant-Token': 'operator'}).status_code == 405


def test_cache_is_bounded(service):
    with service.store.connect() as c:
        c.executemany('INSERT INTO official_cache VALUES (?,?,?)', [(str(i), '{}', 0) for i in range(130)])
    service.read('signals/feed')
    with service.store.connect() as c:
        assert c.execute('SELECT COUNT(*) FROM official_cache').fetchone()[0] == 128
