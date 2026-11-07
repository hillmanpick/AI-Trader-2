import json
import sys
import time
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock,patch

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.lab_features import FEATURES,SCHEMA
from quant.paper_autopilot import DeepSeekAutopilot,PaperAutopilot,position_identity,validate_decisions
from quant.settings import Settings
from quant.store import Store


def decision(**changes):
    return {'symbol':'BTCUSDT','action':'long','confidence':.8,'reason':'订单流与短线趋势支持',
            'margin_usdt':100,'leverage':10,'stop_loss_pct':.005,'take_profit_pct':.01,
            'hold_seconds':600} | changes


@pytest.fixture
def controller(tmp_path):
    cfg=Settings(db_path=str(tmp_path/'paper.db'),mode='paper',enabled=True,api_key='',api_secret='',
        observation_only=False,live_enabled=False,deepseek_api_key='test-ds',strategy='lab_model',
        paper_ai_autopilot=True,paper_lab_required=True,paper_dynamic_leverage=True,
        paper_dynamic_exits=True,paper_exploration_mode=True,paper_margin=2000,paper_max_margin=6000,
        paper_max_leverage=20,leverage=20,paper_min_leverage=2,paper_risk_fraction=.1,
        order_notional=40000,max_notional=120000,symbols=('BTCUSDT','ETHUSDT'))
    db=Store(cfg.db_path)
    db.set('account',{'wallet':10000,'equity':10000,'available':10000})
    db.set('paper_balance',10000)
    api=MagicMock(last_sync=time.time())
    engine=Engine(cfg,db,api)
    engine.ready=True
    lab=MagicMock()
    lab.gate.return_value=True
    lab.row.side_effect=lambda s:{'symbol':s,'price':100,'ts':int(time.time()*1000),
        'vector':[0.]*len(FEATURES),'meta':{'atr_proxy':.001,'schema':SCHEMA,'quality':True}}
    ctl=PaperAutopilot(engine,lab)
    ctl.pool.shutdown(wait=False)
    ctl.pool=MagicMock()
    future=Future()
    ctl.pool.submit.return_value=future
    yield ctl,engine,db,lab,future


@pytest.mark.parametrize('change',[
    {'symbol':'FAKEUSDT'}, {'action':'withdraw'}, {'confidence':True}, {'confidence':float('nan')},
    {'margin_usdt':-1}, {'leverage':True}, {'leverage':101}, {'stop_loss_pct':0},
    {'take_profit_pct':1}, {'hold_seconds':1}, {'hold_seconds':61.5}, {'extra':'unexpected'},
])
def test_invalid_parameters_are_rejected(change):
    with pytest.raises(ValueError):
        validate_decisions({'summary':'test','decisions':[decision(**change)]},{'BTCUSDT'})


def test_duplicate_symbols_rejected():
    with pytest.raises(ValueError):
        validate_decisions({'summary':'test','decisions':[decision(),decision()]},{'BTCUSDT','ETHUSDT'})


def test_one_portfolio_call_per_interval_no_queue_and_no_replay(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    assert ctl.pool.submit.call_count==1
    payload=ctl.pool.submit.call_args.args[1]
    assert len(payload['current']['markets'])==2
    assert list(payload['current']['markets'][0]['features'])==list(FEATURES)
    for _ in range(3): ctl.tick()
    ctl.pool.submit.assert_called_once()
    future.set_result({'status':'complete','summary':'test','decisions':[decision()]})
    ctl.tick()
    assert ctl.signal('BTCUSDT')=='long'
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('entry_context:BTCUSDT')['model_approved'] is False
    ctl.tick()
    ctl.pool.submit.assert_called_once()
    assert db.get('autopilot')['requests']==1


@pytest.mark.parametrize('mode',['expired','position_changed','price_changed','paused','low_confidence'])
def test_stale_or_conflicting_decision_never_executes(controller,mode):
    ctl,e,db,lab,future=controller
    ctl.tick()
    d=decision(confidence=.1) if mode=='low_confidence' else decision()
    future.set_result({'status':'complete','decisions':[d]})
    if mode=='expired': ctl.job[1]['started']-=100
    if mode=='position_changed': db.set('positions',{'BTCUSDT':{'opened_at':'new','amount':1,'entry':100}})
    if mode=='price_changed': lab.row.side_effect=lambda s:{'price':110,'ts':int(time.time()*1000)}
    if mode=='paused': db.set('paused',True)
    ctl.tick()
    assert ctl.signal('BTCUSDT')=='hold'
    assert not db.orders()
    e.exchange.order.assert_not_called()


def test_response_consumed_once_even_when_request_errors(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_exception(RuntimeError('not exposed'))
    ctl.tick()
    assert db.get('autopilot')['status']=='error'
    assert ctl.signal('BTCUSDT')=='hold'


def test_update_can_never_loosen_stop(controller):
    ctl,e,db,lab,future=controller
    p={'opened_at':'2026-11-07T00:00:00+00:00','amount':1,'entry':100,'mark':100,'pnl':0,
       'stop_pct':.005,'stop_price':99.9,'exit_plan':{'stop_pct':.005,'take_pct':.01}}
    db.set('positions',{'BTCUSDT':p})
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision(action='update',stop_loss_pct=.02)]})
    ctl.tick()
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('positions')['BTCUSDT']['stop_price']>=99.9
    assert db.get('positions')['BTCUSDT']['stop_pct']==.005


def test_size_and_leverage_requests_are_capped_against_fresh_risk(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision(margin_usdt=999999,leverage=100)]})
    ctl.tick()
    assert ctl.signal('BTCUSDT')=='long'
    e.lab_row=lab.row
    e.lab_risk_cap=lambda s,side,cap:(cap,{})
    e.exchange.liquidity.return_value={'spread_bps':1,'ask_depth':1e8,'bid_depth':1e8,
        'depth_bps':10,'updated_ms':int(time.time()*1000)}
    p=e.entry_profile(100,[],'BTCUSDT','long')
    assert p['margin']<=2000 and p['notional']<=e.cfg.max_notional
    assert p['leverage']<=100
    assert p['planned_risk_usdt']<=200
    e.exchange.order.assert_not_called()


@pytest.mark.parametrize('changes',[{'mode':'live'},{'api_key':'key'}, {'api_secret':'secret'},
                                  {'observation_only':True},{'live_enabled':True}])
def test_client_rejects_nonpaper_access_before_network(controller,changes):
    _,e,_,_,_=controller
    with patch('quant.paper_autopilot.requests.Session') as network:
        with pytest.raises(ValueError): DeepSeekAutopilot(replace(e.cfg,**changes))
        network.assert_not_called()


def test_client_sends_fixed_prefix_no_tools_and_usage(controller):
    _,e,_,_,_=controller
    session=MagicMock()
    reply=session.__enter__.return_value.post.return_value.__enter__.return_value
    reply.status_code=200
    reply.iter_content.return_value=[json.dumps({'model':'test','usage':{'prompt_cache_hit_tokens':128,
        'prompt_cache_miss_tokens':128},'choices':[{'finish_reason':'stop','message':{
        'content':json.dumps({'summary':'test','decisions':[decision()]})}}]}).encode()]
    with patch('quant.paper_autopilot.requests.Session',return_value=session):
        result=DeepSeekAutopilot(e.cfg).decide({'background':{},'current':{'markets':[{'symbol':'BTCUSDT'}]}})
    assert result['status']=='complete'
    assert result['telemetry']['usage']['prompt_cache_hit_rate']==.5
    body=session.__enter__.return_value.post.call_args.kwargs['json']
    assert 'tools' not in body and 'test-ds' not in json.dumps(body)
