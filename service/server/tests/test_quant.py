import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.binance import Binance, ExchangeError, rounded
from quant.engine import Engine
from quant.notifier import Notifier, quiet, report_slot
from quant.settings import Settings
from quant.store import Store
from quant.strategies import crossover, signal
from routes_quant import register_quant_routes


@pytest.fixture
def cfg(tmp_path):
    return Settings(db_path=str(tmp_path/'quant.db'),fast=2,slow=3)


def test_settings_live_requires_credentials_and_explicit_gate(cfg):
    from dataclasses import replace
    with pytest.raises(ValueError,match='credentials'):
        replace(cfg,mode='live').validate()
    with pytest.raises(ValueError,match='QUANT_LIVE_ENABLED'):
        replace(cfg,mode='live',api_key='key',api_secret='secret').validate()
    replace(cfg,mode='live',api_key='key',api_secret='secret',live_enabled=True).validate()
    with pytest.raises(ValueError):
        replace(cfg,order_notional=float('nan')).validate()
    public = replace(cfg,api_key='private-key',api_secret='private-secret',smtp_password='private-mail').public()
    assert 'private' not in json.dumps(public)


def test_sma_cross_and_short_extension(cfg):
    closes = [5,4,3,2,4,6]
    assert crossover(closes,2,3)==1
    assert signal(closes,0,cfg)=='long'
    assert signal(closes,-1,cfg)=='close'
    assert signal(closes,1,cfg)=='hold'
    descending = [1,2,3,4,2,0.5]
    assert signal(descending,0,cfg)=='short'
    assert signal(descending,1,cfg)=='close'


def test_binance_quantity_never_rounds_up(cfg):
    api = Binance(cfg)
    api.rules['BTCUSDT']={'filters':[{'filterType':'LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'100'},{'filterType':'MIN_NOTIONAL','notional':'100'}]}
    with pytest.raises(ValueError,match='will not be increased'):
        api.quantity('BTCUSDT',50,60000)
    assert api.quantity('BTCUSDT',150,60000)=='0.002'
    assert rounded(0.00299,'0.001')=='0.002'


def test_signed_error_never_exposes_credentials(cfg):
    api = Binance(cfg)
    response = MagicMock(status_code=451)
    response.json.return_value={'msg':'regional restriction'}
    api.session.request = MagicMock(return_value=response)
    with pytest.raises(ExchangeError,match='451') as error:
        api.request('POST','/fapi/v1/order',{'symbol':'BTCUSDT'},signed=True)
    assert 'signature' not in str(error.value)


def test_quiet_hours_boundaries():
    # UTC 16:00 = next day 00:00; UTC 00:00 = 08:00.
    assert quiet(datetime(2026,10,13,16,tzinfo=timezone.utc))
    assert quiet(datetime(2026,10,13,23,59,tzinfo=timezone.utc))
    assert not quiet(datetime(2026,10,14,0,tzinfo=timezone.utc))
    assert not quiet(datetime(2026,10,13,15,59,tzinfo=timezone.utc))
    assert report_slot(datetime(2026,10,14,1,59,tzinfo=timezone.utc))=='2026-10-14-08'
    assert report_slot(datetime(2026,10,14,2,tzinfo=timezone.utc))=='2026-10-14-10'


def test_night_event_is_queued_and_sent_after_eight(cfg):
    from dataclasses import replace
    cfg = replace(cfg,smtp_user='sender@qq.com',smtp_password='auth',mail_to='receiver@qq.com')
    store = Store(cfg.db_path)
    store.event('开仓','night fill')
    notifier = Notifier(cfg,store)
    with patch('quant.notifier.smtplib.SMTP_SSL') as smtp:
        notifier.tick({},'running',datetime(2026,10,13,17,tzinfo=timezone.utc))
        smtp.assert_not_called()
        assert len(store.events(pending=True))==1
        notifier.tick({},'running',datetime(2026,10,14,0,tzinfo=timezone.utc))
        assert len(store.events(pending=True))==0
        message = smtp.return_value.__enter__.return_value.send_message.call_args.args[0]
        assert 'night fill' in message.get_body(preferencelist=('plain',)).get_content()
        assert '空仓' in message.get_body(preferencelist=('plain',)).get_content()
        notifier.tick({},'running',datetime(2026,10,14,1,tzinfo=timezone.utc))
        assert len(store.events())==2


def test_mail_failure_retains_events(cfg):
    from dataclasses import replace
    cfg = replace(cfg,smtp_user='s@qq.com',smtp_password='auth',mail_to='r@qq.com')
    store = Store(cfg.db_path)
    store.event('清仓','fill')
    with patch('quant.notifier.smtplib.SMTP_SSL',side_effect=OSError):
        Notifier(cfg,store).tick({},'running',datetime(2026,10,14,0,tzinfo=timezone.utc))
    assert len(store.events(pending=True))==2
    assert store.get('mail_status')['ok'] is False


def test_timeout_leaves_intent_without_resubmit(cfg):
    from dataclasses import replace
    cfg = replace(cfg,mode='testnet')
    store,api = Store(cfg.db_path),MagicMock()
    api.order.side_effect = RuntimeError('timeout')
    engine = Engine(cfg,store,api)
    with pytest.raises(RuntimeError):
        engine.live_order('BTCUSDT',0.001,'test','bar1')
    assert len(store.orders(pending=True))==1
    api.query_order.side_effect = ExchangeError(400,-2013)
    with pytest.raises(ExchangeError):
        engine.recover_orders()
    assert api.order.call_count==1
    assert len(store.orders(pending=True))==1


def test_close_orders_are_reduce_only(cfg):
    store,api = Store(cfg.db_path),MagicMock()
    store.set('positions',{'BTCUSDT':{'amount':0.001}})
    api.order.return_value={'status':'FILLED'}
    engine = Engine(cfg,store,api)
    engine.reconcile = MagicMock()
    engine.live_order('BTCUSDT',-0.001,'close','bar')
    assert api.order.call_args.args[1]['reduceOnly']=='true'
    assert api.order.call_args.args[1]['side']=='SELL'


def test_paper_open_and_close_pnl_fees(cfg):
    store = Store(cfg.db_path)
    store.set('paper_balance',1000)
    engine = Engine(cfg,store,MagicMock())
    engine.paper_order('BTCUSDT',0.001,60000,'open','1')
    engine.paper_order('BTCUSDT',-0.001,61000,'close','2')
    assert store.get('positions')=={}
    assert store.get('paper_balance')==pytest.approx(1000+1-0.03-0.0305)
    assert [e['kind'] for e in store.events()]==['清仓','开仓']
    assert not store.orders(pending=True)


def test_daily_loss_prevents_entry_but_allows_close(cfg):
    store,api = Store(cfg.db_path),MagicMock()
    store.set('account',{'equity':1000,'available':1000})
    engine = Engine(cfg,store,api)
    assert not engine.daily_blocked(1000)
    store.set('account',{'equity':970,'available':970})
    engine.execute('BTCUSDT','long',60000,'test','1')
    api.quantity.assert_not_called()
    assert engine.daily_blocked(1100)  # breach latches until next local day
    store.set('positions',{'BTCUSDT':{'amount':0.001,'entry':60000,'mark':60000}})
    store.set('paper_balance',1000)
    engine.execute('BTCUSDT','close',60000,'close','2')
    assert store.get('positions')=={}


def test_private_api_denies_and_pause_is_persistent(tmp_path,monkeypatch):
    monkeypatch.setenv('QUANT_API_TOKEN','operator-test')
    monkeypatch.setenv('QUANT_DB_PATH',str(tmp_path/'q.db'))
    app = FastAPI()
    register_quant_routes(app)
    client = TestClient(app)
    assert client.get('/api/quant/status').status_code==401
    assert client.post('/api/quant/pause').status_code==401
    assert client.post('/api/quant/pause',headers={'X-Quant-Token':'operator-test'}).status_code==403
    headers = {'X-Quant-Token':'operator-test','X-Quant-Action':'operator'}
    assert client.post('/api/quant/pause',headers=headers).json()['paused']
    assert client.get('/api/quant/status',headers=headers).json()['paused']
    assert 'operator-test' not in client.get('/api/quant/status',headers=headers).text


def test_bollinger_qbot_rules_and_mirrored_short(cfg):
    from dataclasses import replace
    cfg = replace(cfg,strategy='bollinger')
    assert signal([100]*12+[50],0,cfg)=='long'
    assert signal([100]*12+[150],1,cfg)=='close'
    assert signal([100]*12+[150],0,cfg)=='short'
    assert signal([100]*13,0,cfg)=='hold'


def test_unfinished_candle_is_excluded(cfg):
    api = Binance(cfg)
    api.request = MagicMock(return_value=[[0,0,0,0,100,0,999],[1000,0,0,0,999,0,1999]])
    with patch('quant.binance.time.time',return_value=1.5):
        assert api.closed_candles('BTCUSDT')==[[0,0,0,0,100,0,999]]


def test_protection_failure_pauses_and_attempts_reduce_only_exit(cfg):
    store,api = Store(cfg.db_path),MagicMock()
    api.protect.side_effect=RuntimeError('failed')
    engine=Engine(cfg,store,api)
    engine.live_order=MagicMock()
    with pytest.raises(RuntimeError,match='Protective order'):
        engine.ensure_protection('BTCUSDT',{'amount':0.002,'entry':60000})
    assert store.get('paused') is True
    assert engine.live_order.call_args.args[:3]==('BTCUSDT',-0.002,'protection_failure')


def test_unknown_position_stays_blocked_after_repeated_reconciliation(cfg):
    store,api=Store(cfg.db_path),MagicMock()
    api.positions.return_value=[{'symbol':'BTCUSDT','positionAmt':'0.001','entryPrice':'60000','markPrice':'60000','unRealizedProfit':'0','positionSide':'BOTH'}]
    api.account.return_value={'totalMarginBalance':'1000','availableBalance':'900','totalWalletBalance':'1000'}
    engine=Engine(cfg,store,api)
    for _ in range(2):
        with pytest.raises(ValueError,match='Unmanaged'):
            engine.reconcile()
    api.protect.assert_not_called()


def test_filled_entry_ownership_survives_delayed_position_snapshot(cfg):
    store,api=Store(cfg.db_path),MagicMock()
    store.set('managed_symbols',['BTCUSDT'])
    api.positions.return_value=[]
    api.account.return_value={'totalMarginBalance':'1000','availableBalance':'900','totalWalletBalance':'1000'}
    engine=Engine(cfg,store,api)
    engine.reconcile()
    assert store.get('managed_symbols')==['BTCUSDT']
    api.positions.return_value=[{'symbol':'BTCUSDT','positionAmt':'0.001','entryPrice':'60000','markPrice':'60000','unRealizedProfit':'0','positionSide':'BOTH'}]
    engine.reconcile()
    api.positions.return_value=[]
    engine.reconcile()
    assert store.get('managed_symbols')==[]
    assert any(e['kind']=='清仓' for e in store.events())


def test_exposure_and_pause_prevent_opening(cfg):
    from dataclasses import replace
    cfg=replace(cfg,max_positions=1)
    store,api=Store(cfg.db_path),MagicMock()
    store.set('account',{'equity':1000,'available':1000})
    store.set('positions',{'ETHUSDT':{'amount':0.02,'entry':3000,'mark':3000}})
    engine=Engine(cfg,store,api)
    engine.execute('BTCUSDT','long',60000,'test','1')
    api.quantity.assert_not_called()
    store.set('positions',{})
    store.set('paused',True)
    engine.execute('BTCUSDT','long',60000,'test','2')
    api.quantity.assert_not_called()
