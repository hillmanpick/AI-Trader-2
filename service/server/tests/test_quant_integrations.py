import json
import sys
from concurrent.futures import Future
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.ai_review import DeepSeekReviewer
from quant.community import CommunityClient, normalize, recent_context
from quant.engine import Engine
from quant.settings import Settings
from quant.store import Store
from routes_quant import register_quant_routes


@pytest.fixture
def cfg(tmp_path):
    return Settings(db_path=str(tmp_path/'quant.db'), ai_enabled=True, deepseek_api_key='private-ds-key')


def model_response(decision):
    session = MagicMock()
    response = session.__enter__.return_value.post.return_value.__enter__.return_value
    response.status_code = 200
    response.iter_content.return_value = [json.dumps({'choices': [{'message': {'content': json.dumps(decision)}}]}).encode()]
    return session


@pytest.mark.parametrize('decision,expected', [
    ({'decision':'allow','confidence':.9,'reason':'行情支持'}, 'allow'),
    ({'decision':'allow','confidence':.2,'reason':'弱信号'}, 'reject'),
    ({'decision':'allow','confidence':True,'reason':'错误'}, 'reject'),
    ({'decision':'allow','confidence':.9,'reason':'错误','leverage':20}, 'reject'),
    ({'decision':'long','confidence':.9,'reason':'错误'}, 'reject'),
])
def test_model_response_cannot_set_direction_or_risk(cfg, decision, expected):
    session = model_response(decision)
    with patch('quant.ai_review.requests.Session', return_value=session):
        result = DeepSeekReviewer(cfg).review({'symbol':'BTCUSDT','action':'long'}, {})
    assert result['decision'] == expected
    payload = session.__enter__.return_value.post.call_args.kwargs['json']
    assert 'private-ds-key' not in json.dumps(payload)
    assert 'private-ds-key' not in json.dumps(result)


def test_missing_key_and_network_failure_block_entries(cfg):
    assert DeepSeekReviewer(replace(cfg,deepseek_api_key='')).review({'symbol':'BTCUSDT'}, {})['status'] == 'unconfigured'
    session = model_response({})
    session.__enter__.return_value.post.return_value.__enter__.return_value.status_code = 401
    with patch('quant.ai_review.requests.Session', return_value=session):
        result = DeepSeekReviewer(cfg).review({'symbol':'BTCUSDT'}, {})
    assert result['status'] == 'error' and result['decision'] == 'reject'


@pytest.mark.parametrize('mode,relaxed,threshold,expected,profile', [
    ('paper', True, .55, 'allow', 'paper_relaxed'),
    ('paper', False, .65, 'reject', 'standard'),
    ('live', True, .65, 'reject', 'standard'),
])
def test_relaxed_review_and_lower_threshold_apply_only_to_paper(cfg, mode, relaxed, threshold, expected, profile):
    from quant.ai_review import SYSTEM, PAPER_SYSTEM
    cfg = replace(cfg, mode=mode, paper_review_relaxed=relaxed, ai_min_confidence=threshold)
    session = model_response({'decision':'allow', 'confidence':.6, 'reason':'行情支持'})
    with patch('quant.ai_review.requests.Session', return_value=session):
        result = DeepSeekReviewer(cfg).review({'symbol':'BTCUSDT','action':'long'}, {})
    assert result['decision'] == expected
    assert result['telemetry']['review_profile'] == profile
    body = session.__enter__.return_value.post.call_args.kwargs['json']
    assert body['messages'][0]['content'] == (PAPER_SYSTEM if profile == 'paper_relaxed' else SYSTEM)


def test_explicit_model_rejection_is_not_overridden_by_relaxed_mode(cfg):
    cfg = replace(cfg, paper_review_relaxed=True, ai_min_confidence=.55)
    session = model_response({'decision':'reject', 'confidence':.9, 'reason':'明确反向证据'})
    with patch('quant.ai_review.requests.Session', return_value=session):
        assert DeepSeekReviewer(cfg).review({'symbol':'BTCUSDT','action':'long'}, {})['decision'] == 'reject'


def test_paper_review_uses_final_json_without_reasoning_and_records_usage(cfg):
    session = model_response({'decision':'allow','confidence':.9,'reason':'ok'})
    response = session.__enter__.return_value.post.return_value.__enter__.return_value
    response.iter_content.return_value = [json.dumps({'model':'deepseek-flash','usage':{
        'prompt_tokens':400,'completion_tokens':60,'prompt_cache_hit_tokens':128,'prompt_cache_miss_tokens':272},
        'choices':[{'finish_reason':'stop','message':{'content':json.dumps({'decision':'allow','confidence':.9,'reason':'ok'})}}]}).encode()]
    with patch('quant.ai_review.requests.Session',return_value=session):
        result=DeepSeekReviewer(cfg).review({'symbol':'SOLUSDT'}, {})
    body=session.__enter__.return_value.post.call_args.kwargs['json']
    assert body['thinking']=={'type':'disabled'} and body['max_tokens']==512
    assert result['decision']=='allow' and result['telemetry']['usage']['prompt_cache_hit_tokens']==128


def test_truncated_response_is_identified_and_cannot_allow_entry(cfg,caplog):
    session=model_response({})
    response=session.__enter__.return_value.post.return_value.__enter__.return_value
    response.iter_content.return_value=[json.dumps({'usage':{'completion_tokens':2048,
        'completion_tokens_details':{'reasoning_tokens':2048},'private':'private-body'},
        'choices':[{'finish_reason':'length','message':{'content':'','reasoning_content':'private-reasoning'}}]}).encode()]
    with patch('quant.ai_review.requests.Session',return_value=session):
        result=DeepSeekReviewer(cfg).review({'symbol':'SOLUSDT'}, {})
    assert result['decision']=='reject' and result['error_code']=='output_truncated'
    assert result['telemetry']['usage']['reasoning_tokens']==2048
    assert 'private' not in json.dumps(result) and 'private' not in caplog.text
    assert session.__enter__.return_value.post.call_count==1


def test_http_error_and_timeout_are_distinct_and_redacted(cfg,caplog):
    import requests
    session=model_response({})
    reply=session.__enter__.return_value.post.return_value.__enter__.return_value
    reply.status_code=429
    with patch('quant.ai_review.requests.Session',return_value=session):
        result=DeepSeekReviewer(cfg).review({'symbol':'SOLUSDT'}, {})
    assert result['error_code']=='http_error' and result['telemetry']['http_status']==429
    session.__enter__.return_value.post.side_effect=requests.Timeout('private-token-in-url')
    with patch('quant.ai_review.requests.Session',return_value=session):
        result=DeepSeekReviewer(cfg).review({'symbol':'SOLUSDT'}, {})
    assert result['error_code']=='timeout' and result['decision']=='reject'
    assert 'private' not in caplog.text and 'private' not in json.dumps(result)


def test_invalid_json_is_identified_without_logging_body(cfg,caplog):
    session=model_response({})
    response=session.__enter__.return_value.post.return_value.__enter__.return_value
    response.iter_content.return_value=[json.dumps({'choices':[{'finish_reason':'stop','message':{'content':'private invalid json'}}]}).encode()]
    with patch('quant.ai_review.requests.Session',return_value=session):
        result=DeepSeekReviewer(cfg).review({'symbol':'SOLUSDT'}, {})
    assert result['error_code']=='invalid_json' and result['decision']=='reject'
    assert 'private' not in caplog.text


def test_community_context_excludes_old_unrelated_duplicate_and_future_text():
    now = datetime(2026,10,14,2,tzinfo=timezone.utc)
    row = {'signal_id':'1','kind':'operation','symbol':'BTC','content':'buy','created_at':'2026-10-14T01:59:00Z'}
    snapshot = {'feed_updated_at':{'operation':now.isoformat()}, 'feeds':{'operation':[
        row, row | {'signal_id':'2'}, row | {'signal_id':'3','content':'[Copied from Source] buy'}, row | {'symbol':'ETH'},
        row | {'created_at':'2026-10-14T00:00:00Z'}, row | {'created_at':'2026-10-14T03:00:00Z'},
    ]}}
    assert len(recent_context(snapshot,'BTCUSDT',now)) == 1
    snapshot['feed_updated_at']['operation'] = '2026-10-14T01:00:00Z'
    assert recent_context(snapshot,'BTCUSDT',now) == []


def test_collector_keeps_last_snapshot_on_partial_failure(cfg):
    store = Store(cfg.db_path)
    store.set('community',{'feeds':{'operation':[{'signal_id':'old'}]},'feed_updated_at':{'operation':'old'}})
    client = CommunityClient(cfg)
    client.get = MagicMock(side_effect=[RuntimeError('unavailable'), {'signals':[],'total':0}, {'signals':[],'total':0}, {'top_agents':[]}])
    client.collect(store)
    snapshot = store.get('community')
    assert snapshot['feeds']['operation'][0]['signal_id'] == 'old'
    assert snapshot['feed_updated_at']['operation'] == 'old'
    assert snapshot['feeds']['strategy'] == []
    assert 'operation' in snapshot['errors']


def test_normalization_filters_market_and_limits_untrusted_content():
    assert normalize({'market':'us-stock','message_type':'operation'},'operation') is None
    row = normalize({'market':'crypto','message_type':'strategy','symbols':'BTC','content':'x'*5000},'strategy')
    assert row['symbols'] == [] and len(row['content']) == 4000


def review_engine(cfg):
    store = Store(cfg.db_path)
    api = MagicMock(offset=0)
    engine = Engine(cfg,store,api)
    engine.execute = MagicMock()
    future = Future()
    candidate = {'symbol':'BTCUSDT','action':'long','bar':123}
    engine.review_jobs['BTCUSDT'] = (future,100,candidate)
    return engine,store,future


def test_background_review_keeps_trading_loop_free_and_rechecks_current_price(cfg):
    engine,store,future = review_engine(cfg)
    with patch('quant.engine.time.monotonic',return_value=101):
        engine.finish_review('BTCUSDT',61000)
    engine.execute.assert_not_called()
    future.set_result({'decision':'allow','confidence':.9,'reason':'ok','status':'complete'})
    with patch('quant.engine.time.monotonic',return_value=102):
        engine.finish_review('BTCUSDT',62000)
    engine.execute.assert_called_once_with('BTCUSDT','long',62000,cfg.strategy,'123')
    assert engine.review_jobs == {}


def test_stale_approval_cannot_open_position(cfg):
    engine,store,future = review_engine(cfg)
    future.set_result({'decision':'allow','confidence':.9,'reason':'ok','status':'complete'})
    with patch('quant.engine.time.monotonic',return_value=200):
        engine.finish_review('BTCUSDT',62000)
    engine.execute.assert_not_called()
    assert store.get('ai:BTCUSDT')['status'] == 'expired'


@pytest.mark.parametrize('elapsed,bar_end,allowed', [(120, 1000, True), (181, 1000, False), (120, 500, False)])
def test_paper_extended_review_still_expires_at_time_or_next_candle(cfg, elapsed, bar_end, allowed):
    cfg = replace(cfg, ai_review_max_age=180)
    engine, store, future = review_engine(cfg)
    engine.review_jobs['BTCUSDT'][2]['valid_until'] = bar_end
    future.set_result({'decision':'allow','confidence':.6,'reason':'ok','status':'complete'})
    with patch('quant.engine.time.monotonic', return_value=100+elapsed), patch('quant.engine.time.time', return_value=.5):
        engine.finish_review('BTCUSDT',62000)
    assert engine.execute.called == allowed


def test_review_receives_actual_strategy_sma_periods_and_values(cfg):
    cfg = replace(cfg, fast=3, slow=10)
    engine = Engine(cfg, Store(cfg.db_path), MagicMock())
    pool = MagicMock()
    engine.review_pool = pool
    bars = [[i*1000, 0, 0, 0, i+1, 0, (i+1)*1000-1] for i in range(40)]
    engine.review_entry('BTCUSDT', 'long', 40, bars, 39000)
    candidate = pool.submit.call_args.args[1]
    assert candidate['sma'] == {'fast_period':3,'slow_period':10,'fast':39,'slow':35.5,
                                'previous_fast':38,'previous_slow':34.5}


def test_pause_is_rechecked_after_ai_approval(cfg):
    store = Store(cfg.db_path)
    store.set('paused',True)
    store.set('account',{'equity':10000,'available':10000})
    api = MagicMock()
    Engine(cfg,store,api).execute('BTCUSDT','long',60000,'sma_cross','1')
    api.quantity.assert_not_called()


def test_stop_exit_is_not_gated_by_missing_ai_key(cfg):
    cfg = replace(cfg,deepseek_api_key='')
    store = Store(cfg.db_path)
    store.set('positions',{'BTCUSDT':{'amount':.001,'entry':60000,'mark':60000,'pnl':0}})
    store.set('paper_balance',10000)
    engine = Engine(cfg,store,MagicMock())
    engine.execute('BTCUSDT','close',58000,'stop_loss','1')
    assert store.get('positions') == {}
    assert store.events()[0]['kind'] == '清仓'


def test_routes_require_operator_and_do_not_expose_secrets(tmp_path,monkeypatch):
    monkeypatch.setenv('QUANT_API_TOKEN','operator')
    monkeypatch.setenv('QUANT_DB_PATH',str(tmp_path/'q.db'))
    monkeypatch.setenv('DEEPSEEK_API_KEY','')
    monkeypatch.setenv('AI4TRADE_TOKEN','private-community-token')
    app = FastAPI()
    register_quant_routes(app)
    client = TestClient(app)
    assert client.get('/api/quant/community').status_code == 401
    assert client.post('/api/quant/ai-test',headers={'X-Quant-Token':'operator'}).status_code == 403
    assert client.post('/api/quant/ai-test',headers={'X-Quant-Token':'operator','X-Quant-Action':'operator'}).status_code == 400
    response = client.get('/api/quant/community',headers={'X-Quant-Token':'operator'})
    assert response.status_code == 200 and 'private-community-token' not in response.text
    assert response.json()['stale']['operation'] is True
