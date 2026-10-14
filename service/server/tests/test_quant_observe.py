import json
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.ai_review import DeepSeekAdvisor
from quant.binance import Binance
from quant.engine import Engine
from quant.observe import ReadOnlyBinance, advice, collect, snapshot
from quant.settings import Settings
from quant.store import Store, utcnow
from routes_quant import register_quant_routes


@pytest.fixture
def cfg(tmp_path):
    return Settings(db_path=str(tmp_path/'q.db'), observation_only=True, ai_enabled=True,
                    deepseek_api_key='private-ds', api_key='private-binance', api_secret='private-secret', operator_token='operator')


@pytest.mark.parametrize('method,path', [('POST','/fapi/v1/order'), ('DELETE','/fapi/v1/order'),
    ('POST','/fapi/v1/leverage'), ('POST','/fapi/v1/marginType'), ('GET','/fapi/v1/listenKey')])
def test_readonly_client_rejects_all_writes_before_network(cfg, method, path):
    api = ReadOnlyBinance(cfg)
    with patch.object(Binance, 'request') as network:
        with pytest.raises(RuntimeError, match='rejects'):
            api.request(method, path, signed=True)
        network.assert_not_called()


def test_readonly_reads_production_even_with_paper_or_testnet_cfg(cfg):
    api = ReadOnlyBinance(replace(cfg, mode='testnet'))
    assert api.cfg.base_url == 'https://fapi.binance.com'
    assert not api.cfg.enabled and not api.cfg.live_enabled
    with patch.object(Binance, 'request', return_value={}) as network:
        api.account()
        network.assert_called_once_with('GET', '/fapi/v3/account', None, True)


def test_engine_observation_cannot_initialize_execute_or_change_journal(cfg):
    store = Store(cfg.db_path)
    store.set('positions', {'ETHUSDT': {'amount': 1, 'entry': 100}})
    exchange = MagicMock()
    engine = Engine(cfg, store, exchange)
    assert engine.step() == 'observing'
    with pytest.raises(RuntimeError): engine.initialize()
    with pytest.raises(RuntimeError): engine.execute('ETHUSDT', 'close', 80, 'stop', '1')
    assert store.get('positions')['ETHUSDT']['amount'] == 1
    assert not store.orders()
    assert not exchange.method_calls


def row(amount, side='BOTH'):
    return {'symbol':'ETHUSDT', 'positionAmt':str(amount), 'positionSide':side, 'entryPrice':'100',
            'markPrice':'110', 'unRealizedProfit':str(amount*10), 'liquidationPrice':'60', 'notional':str(amount*110)}


def test_real_manual_and_hedge_positions_do_not_merge_or_contaminate_paper_journal(cfg):
    store = Store(cfg.db_path)
    original = {'BTCUSDT': {'amount': 1, 'entry': 10}}
    store.set('positions', original)
    store.set('account', {'equity': 10000})
    api = MagicMock(last_sync=10**20)
    api.account.return_value = {'totalMarginBalance':'200','totalWalletBalance':'190','availableBalance':'150',
                                'totalMaintMargin':'2','assets':[{'asset':'USDT','walletBalance':'190','private':'secret'}]}
    api.positions.return_value = [row(2, 'LONG'), row(-1, 'SHORT')]
    value = collect(cfg, store, api)
    assert len(value['positions']) == 2 and value['account']['margin_ratio'] == .01
    assert store.get('positions') == original and store.get('account')['equity'] == 10000
    assert not store.events() and not store.orders()
    assert 'private' not in json.dumps(value)
    api.positions.return_value = [row(2, 'LONG')]
    collect(cfg, store, api)
    assert store.events()[0]['kind'] == '清仓'
    assert '非系统下单' in store.events()[0]['body']


def test_invalid_exchange_values_are_not_displayed(cfg):
    with pytest.raises(ValueError): snapshot({'totalWalletBalance':'NaN'}, [])


def response(decision):
    session = MagicMock()
    reply = session.__enter__.return_value.post.return_value.__enter__.return_value
    reply.status_code = 200
    reply.iter_content.return_value = [json.dumps({'model':'deepseek-flash','choices':[{'message':{'content':json.dumps(decision)}}]}).encode()]
    return session


@pytest.mark.parametrize('decision,expected', [
    ({'bias':'bullish','confidence':.9,'reason':'均线支持','risk':'震荡回撤'},'complete'),
    ({'bias':'buy','confidence':.9,'reason':'均线支持','risk':'震荡回撤'},'error'),
    ({'bias':'bullish','confidence':True,'reason':'均线支持','risk':'震荡回撤'},'error'),
    ({'bias':'bullish','confidence':.9,'reason':'均线支持','risk':'震荡回撤','leverage':20},'error'),
])
def test_advisor_json_has_no_execution_tools(cfg, decision, expected):
    session = response(decision)
    with patch('quant.ai_review.requests.Session',return_value=session):
        result = DeepSeekAdvisor(cfg).analyze({'symbol':'BTCUSDT','closed_prices':[10,11]}, {})
    assert result['status'] == expected
    post = session.__enter__.return_value.post.call_args.kwargs
    assert 'tools' not in post['json']
    assert 'private' not in json.dumps(post['json'])
    assert 'private' not in json.dumps(result)


def test_low_confidence_advice_becomes_wait(cfg):
    with patch('quant.ai_review.requests.Session',return_value=response({'bias':'bullish','confidence':.1,'reason':'weak','risk':'risk'})):
        assert DeepSeekAdvisor(cfg).analyze({'symbol':'BTCUSDT'}, {})['bias'] == 'wait'


def test_analysis_does_not_send_balance_position_sizes_or_identifiers(cfg):
    store = Store(cfg.db_path)
    store.set('live_account', snapshot({'totalWalletBalance':'998877','totalMarginBalance':'998877'}, [row(2)]))
    api = MagicMock(offset=0)
    api.closed_candles.return_value = [[i*60000, 0, 0, 0, '100', 0, (i+1)*60000-1] for i in range(80)]
    api.mark.return_value = 110
    analyzer = MagicMock()
    analyzer.analyze.return_value = {'bias':'wait','confidence':.7,'reason':'wait','risk':'risk','status':'complete'}
    with patch('quant.observe.ReadOnlyBinance',return_value=api), patch('quant.observe.DeepSeekAdvisor',return_value=analyzer):
        value = advice(cfg,store)
    assert value['status'] == 'complete'
    market = analyzer.analyze.call_args.args[0]
    assert '998877' not in json.dumps(market) and 'private' not in json.dumps(market)
    assert 'amount' not in json.dumps(market) and 'account' not in market
    assert not api.order.called and not api.prepare_account.called
    assert not store.orders()


def test_stale_account_stops_analysis(cfg):
    store = Store(cfg.db_path)
    store.set('live_account', {'status':'ok','updated_at':'2020-01-01T00:00:00Z'})
    with patch('quant.observe.DeepSeekAdvisor') as advisor:
        value = advice(cfg,store)
    assert value['status'] == 'error'
    advisor.return_value.analyze.assert_not_called()


def test_advice_can_reuse_realtime_market_client_without_closing_it(cfg):
    store = Store(cfg.db_path)
    store.set('live_account', snapshot({'totalWalletBalance':'100','totalMarginBalance':'100'}, []))
    api = MagicMock()
    api.closed_candles.return_value = [[i*60000, 0, 0, 0, '100', 0, (i+1)*60000-1] for i in range(80)]
    api.mark.return_value = 100
    analyzer = MagicMock()
    analyzer.analyze.return_value = {'bias':'wait','confidence':.7,'reason':'wait','risk':'risk','status':'complete'}
    with patch('quant.observe.DeepSeekAdvisor',return_value=analyzer):
        assert advice(cfg, store, api)['status'] == 'complete'
    api.session.close.assert_not_called()


def test_account_api_auth_and_resume_cannot_enable_execution(cfg):
    app = FastAPI()
    store = Store(cfg.db_path)
    store.set('live_account',snapshot({'totalMarginBalance':'100'}, []))
    with patch('routes_quant.Settings',return_value=cfg):register_quant_routes(app)
    client = TestClient(app)
    assert client.get('/api/quant/live-account').status_code == 401
    headers = {'X-Quant-Token':'operator','X-Quant-Action':'operator'}
    value = client.get('/api/quant/live-account',headers=headers)
    assert value.status_code == 200 and not value.json()['stale']
    assert not value.json()['execution_enabled'] and value.json()['read_only']
    assert 'private' not in value.text
    assert client.post('/api/quant/resume',headers=headers).status_code == 409
    assert not client.get('/api/quant/status',headers=headers).json()['config']['execution_enabled']
