import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.binance import Binance
from quant.engine import Engine
from quant.paper import PaperMarket, paper_settings
from quant.settings import Settings
from quant.store import Store
from routes_quant import register_quant_routes


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv('QUANT_PAPER_ENABLED', 'true')
    monkeypatch.setenv('QUANT_PAPER_DB_PATH', str(tmp_path/'sim.db'))
    base = Settings(db_path=str(tmp_path/'observer.db'), mode='paper', enabled=False,
                    observation_only=True, api_key='private-key', api_secret='private-secret',
                    operator_token='operator', ai_enabled=True)
    return base, paper_settings(base)


def test_simulation_has_separate_journal_and_no_account_keys(setup, monkeypatch):
    base, cfg = setup
    assert cfg.db_path != base.db_path and cfg.mode == 'paper'
    assert cfg.enabled and not cfg.live_enabled and not cfg.observation_only
    assert not cfg.api_key and not cfg.api_secret and not cfg.community_token
    assert cfg.order_notional == 2000 and cfg.max_notional == 6000
    monkeypatch.setenv('QUANT_PAPER_DB_PATH', base.db_path)
    with pytest.raises(ValueError, match='separate'):paper_settings(base)


@pytest.mark.parametrize('legacy_limit',['500','invalid'])
def test_paper_ignores_legacy_daily_loss_limit(setup,monkeypatch,legacy_limit):
    base,_ = setup
    monkeypatch.setenv('QUANT_PAPER_MAX_DAILY_LOSS_USDT',legacy_limit)
    cfg = paper_settings(base)
    assert cfg.daily_loss==0 and not cfg.daily_loss_enabled
    assert cfg.public()['daily_loss'] is None
    assert base.daily_loss > 0


def test_paper_review_overrides_leave_observer_settings_unchanged(setup, monkeypatch):
    base, _ = setup
    monkeypatch.setenv('QUANT_PAPER_AI_MIN_CONFIDENCE', '0.55')
    monkeypatch.setenv('QUANT_PAPER_AI_REVIEW_MAX_AGE_SECONDS', '180')
    monkeypatch.setenv('QUANT_PAPER_AI_RELAXED', 'true')
    monkeypatch.setenv('QUANT_PAPER_SMA_FAST', '3')
    monkeypatch.setenv('QUANT_PAPER_SMA_SLOW', '10')
    cfg = paper_settings(base)
    assert (cfg.fast, cfg.slow, cfg.ai_min_confidence, cfg.ai_review_max_age) == (3, 10, .55, 180)
    assert cfg.public()['ai_review_profile'] == 'paper_relaxed'
    assert (base.ai_min_confidence, base.ai_review_max_age, base.paper_review_relaxed) == (.65, 90, False)
    assert base.observation_only and not base.enabled
    assert not cfg.api_key and not cfg.api_secret


@pytest.mark.parametrize('name,value', [('QUANT_PAPER_AI_MIN_CONFIDENCE', '1.1'),
                                      ('QUANT_PAPER_AI_REVIEW_MAX_AGE_SECONDS', '301')])
def test_invalid_paper_review_settings_fail_validation(setup, monkeypatch, name, value):
    base, _ = setup
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        paper_settings(base)


@pytest.mark.parametrize('method,path,signed', [
    ('POST','/fapi/v1/order',False),('DELETE','/fapi/v1/order',False),
    ('GET','/fapi/v3/account',True),('GET','/fapi/v3/positionRisk',True),
    ('POST','/fapi/v1/leverage',False),('GET','/fapi/v1/klines',True),
])
def test_paper_market_never_calls_private_or_write_endpoints(setup, method, path, signed):
    _,cfg = setup
    api = PaperMarket(cfg)
    with patch.object(Binance,'request') as network:
        with pytest.raises(RuntimeError):api.request(method,path,signed=signed)
        network.assert_not_called()


def test_misconfigured_paper_client_fails_before_network(setup):
    _,cfg = setup
    for change in ({'mode':'live'}, {'live_enabled':True}, {'api_key':'key'}, {'observation_only':True}):
        with pytest.raises(ValueError):PaperMarket(replace(cfg,**change))


def test_ai_approval_fills_virtual_order_and_never_changes_observer(setup):
    base,cfg = setup
    observer=Store(base.db_path)
    observer.set('positions', {'legacy':{'amount':.01}})
    observer.set('live_account', {'positions':[],'account':{'wallet':5}})
    paper=Store(cfg.db_path)
    paper.set('account', {'equity':10000,'available':10000})
    api=PaperMarket(cfg)
    api.rules['BTCUSDT']={'filters':[{'filterType':'LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'100'}]}
    engine=Engine(cfg,paper,api)
    from concurrent.futures import Future
    future=Future();future.set_result({'decision':'allow','confidence':.8,'status':'complete','reason':'test'})
    engine.review_jobs['BTCUSDT']=(future,100,{'symbol':'BTCUSDT','action':'long','bar':1})
    with patch('quant.engine.time.monotonic',return_value=101), patch.object(api,'request') as network:
        engine.finish_review('BTCUSDT',60000)
        engine.execute('BTCUSDT','close',61000,'test','2')
        network.assert_not_called()
    assert len(paper.orders())==2 and not paper.get('positions')
    assert paper.get('paper_balance') > 10000
    assert observer.get('positions') == {'legacy':{'amount':.01}}
    assert observer.get('live_account')['account']['wallet']==5
    assert not observer.orders()


def test_paper_api_is_authenticated_and_cannot_expose_real_credentials(setup):
    base,cfg = setup
    db=Store(cfg.db_path)
    db.set('account',{'equity':10000})
    app=FastAPI()
    with patch('routes_quant.Settings',return_value=base):register_quant_routes(app)
    client=TestClient(app)
    assert client.get('/api/quant/paper/status').status_code==401
    result=client.get('/api/quant/paper/status',headers={'X-Quant-Token':'operator'})
    assert result.status_code==200 and result.json()['account']['equity']==10000
    assert not result.json()['real_execution_enabled']
    assert 'private-key' not in result.text and 'private-secret' not in result.text
