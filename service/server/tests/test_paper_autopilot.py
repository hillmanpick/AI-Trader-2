import json
import sys
import time
from datetime import datetime, timezone
from concurrent.futures import Future
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock,patch

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.lab_features import FEATURES,SCHEMA
from quant.paper_autopilot import DIRECT_SYSTEM,DeepSeekAutopilot,PaperAutopilot,position_identity,prioritize_candidates,validate_decisions
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
    lab.bus.get.side_effect=lambda key:{'schema':'potential-radar-v1','ts':int(time.time()*1000),
        'rows':[{'symbol':'BTCUSDT','ts':int(time.time()*1000),'long_score':40,'short_score':10,
            'return_5m':.002,'return_15m':.004,'return_1h':.01,'return_4h':.02}]} if key=='potential_candidates' else None
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


def test_candidates_prioritize_positions_then_radar_and_all_market_rank():
    ordered=prioritize_candidates(
        ('ZUSDT','AUSDT','CUSDT','BUSDT','DUSDT'), {'DUSDT':{}},
        [{'symbol':'CUSDT','ready':True,'eligible':True,'score':.9},
         {'symbol':'ZUSDT','ready':True,'eligible':True,'score':.8}],
        [{'symbol':'AUSDT','long_score':90,'short_score':5},
         {'symbol':'BUSDT','long_score':5,'short_score':80}], limit=4)
    assert ordered==('DUSDT','AUSDT','BUSDT','CUSDT')


def test_ds_paper_prompt_allows_exploratory_entries_without_ignoring_costs():
    assert '没有扣费后优势就 hold' not in DIRECT_SYSTEM
    assert '至少两项独立方向证据一致' in DIRECT_SYSTEM
    assert '成本必须进入盈亏评估' in DIRECT_SYSTEM


def test_one_portfolio_call_per_interval_no_queue_and_no_replay(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    assert ctl.pool.submit.call_count==1
    payload=ctl.pool.submit.call_args.args[1]
    assert len(payload['current']['markets'])==2
    assert list(payload['current']['markets'][0]['features'])==list(FEATURES)
    assert payload['current']['markets'][0]['potential']['long_score']==40
    assert payload['background']['selection_horizon']=='1-4h'
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


def test_autopilot_can_scan_after_one_minute(controller):
    ctl,e,db,lab,future=controller
    db.set('autopilot:last_request',time.time()-61)
    ctl.tick()
    ctl.pool.submit.assert_called_once()


def test_autopilot_skips_ai_call_without_any_fresh_quote(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,symbols=('BTCUSDT',),paper_ds_authority=True)
    db.set('positions',{'BTCUSDT':{'amount':1,'entry':100,'opened_at':'2026-11-17T00:00:00+00:00'}})
    lab.row.side_effect=ValueError('feature unavailable')
    lab.management_row.return_value={'symbol':'BTCUSDT','price':None,'ts':None,'vector':None,
        'meta':{'quality':False,'entry_allowed':False,'atr_proxy':None},'management_only':True}
    ctl.tick()
    ctl.pool.submit.assert_not_called()
    assert db.get('autopilot:last_request') is None
    assert db.get('autopilot')['status']=='waiting_data'


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


@pytest.mark.parametrize('radar,action',[
    (None,'long'),
    ({'long_score':10,'short_score':40,'ts':0},'long'),
    ({'long_score':40,'short_score':40,'ts':0},'long'),
])
def test_new_position_requires_fresh_stronger_matching_horizon_score(controller,radar,action):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision(action=action)]})
    ctl.tick()
    if radar is None:
        ctx=ctl.actions['BTCUSDT'][1]
        ctx['potentials'].pop('BTCUSDT')
    else:
        radar=radar|{'ts':int(time.time()*1000)}
        ctl.actions['BTCUSDT'][1]['potentials']['BTCUSDT']=radar
    assert ctl.signal('BTCUSDT')=='hold'
    assert '1-4小时' in db.get('signal:BTCUSDT')['reason']
    assert not db.orders()


def test_fresh_matching_extreme_direction_can_reach_paper_sizing(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision()]})
    ctl.tick()
    assert ctl.signal('BTCUSDT')=='long'
    assert db.get('entry_context:BTCUSDT')['ai_parameters']['action']=='long'


def test_short_decision_and_stale_horizon_snapshot_are_checked(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision(action='short')]})
    ctl.tick()
    radar=ctl.actions['BTCUSDT'][1]['potentials']['BTCUSDT']|{'long_score':10,'short_score':40,
        'ts':int(time.time()*1000)-181000}
    ctl.actions['BTCUSDT'][1]['potentials']['BTCUSDT']=radar
    assert ctl.signal('BTCUSDT')=='hold'
    assert '1-4小时' in db.get('signal:BTCUSDT')['reason']


def test_daily_request_budget_persists_across_autopilot_instances(controller,monkeypatch):
    ctl,e,db,lab,future=controller
    day=datetime.now(timezone.utc).strftime('%Y-%m-%d')
    db.set('autopilot:daily_budget',{'day_utc':day,'requests':288})
    ctl.tick()
    assert ctl.pool.submit.call_count==0
    assert db.get('autopilot')['status']=='daily_budget_exhausted'
    assert db.get('autopilot')['daily_request_limit']==288


def test_no_horizon_candidates_and_no_positions_does_not_spend_ai_request(controller):
    ctl,e,db,lab,future=controller
    lab.bus.get.return_value=None;lab.bus.get.side_effect=None
    ctl.tick()
    ctl.pool.submit.assert_not_called()
    assert db.get('autopilot')['status']=='waiting_data'
    assert db.get('autopilot:daily_budget') is None


def test_matching_short_direction_passes_the_same_paper_only_checks(controller):
    ctl,e,db,lab,future=controller
    ctl.tick()
    future.set_result({'status':'complete','decisions':[decision(action='short')]})
    ctl.tick()
    ctl.actions['BTCUSDT'][1]['potentials']['BTCUSDT'].update(long_score=5,short_score=45)
    assert ctl.signal('BTCUSDT')=='short'
    assert db.get('entry_context:BTCUSDT')['ai_parameters']['action']=='short'
    e.exchange.order.assert_not_called()


def test_portfolio_batch_never_exceeds_twelve_and_prioritizes_held_contracts(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,symbols=tuple(f'COIN{i:02}USDT' for i in range(30)))
    db.set('positions',{'COIN29USDT':{'opened_at':'2026-11-13T00:00:00+00:00','amount':1,'entry':100}})
    ctl.tick()
    payload=ctl.pool.submit.call_args.args[1]
    assert len(payload['current']['markets'])==12
    assert payload['current']['markets'][0]['symbol']=='COIN29USDT'
    assert 'COIN29USDT' in payload['current']['positions']


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


def test_recent_cache_window_survives_controller_restart_without_resetting_totals(controller):
    from quant.paper_prompt import SCHEMA
    ctl,e,db,lab,future=controller
    db.set('autopilot',{'requests':100,'usage_total':{'prompt_tokens':90000,
        'prompt_cache_hit_tokens':10000,'prompt_cache_miss_tokens':80000}})
    db.set('autopilot:usage_recent',[{'at':'prior','prompt_schema':SCHEMA,
        'usage':{'prompt_tokens':1000,'prompt_cache_hit_tokens':600,'prompt_cache_miss_tokens':400}}])
    ctl.tick()
    future.set_result({'status':'complete','decisions':[],'telemetry':{'prompt_schema':SCHEMA,
        'usage':{'prompt_tokens':2000,'prompt_cache_hit_tokens':1000,'prompt_cache_miss_tokens':1000}}})
    ctl.tick();status=db.get('autopilot')
    assert status['usage_total']['prompt_tokens']==92000 and status['requests']==101
    assert status['cache_recent']['calls']==2
    assert status['cache_recent']['prompt_cache_hit_rate']==pytest.approx(1600/3000)
    assert len(db.get('autopilot:usage_recent'))==2


def fast_market(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,paper_fast_execution=True,paper_slippage=.0003,paper_ai_autopilot_interval=300)
    row={'symbol':'BTCUSDT','price':100,'ts':int(time.time()*1000),
         'vector':[.0001,.001,.002,.0002,1,.2,1,.2,10,1.5,0,10,10,5],
         'meta':{'atr_proxy':.003,'schema':SCHEMA,'quality':True}}
    lab.row.side_effect=lambda s:dict(row)
    return ctl,e,db,lab,future,row


@pytest.mark.parametrize('action',['long','short'])
def test_ds_authority_executes_once_without_strategy_vetoes(controller,action):
    ctl,e,db,lab,future,row=fast_market(controller)
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    lab.bus.get.side_effect=lambda k:None
    # Flat trend, adverse flow, low confidence, tiny target and active cooldown
    # are context for DS, not a second programmatic strategy decision.
    row['vector']=[0.]*len(FEATURES)
    db.set('shortline_cooldown:BTCUSDT',time.time()+120)
    ctl.tick()
    payload=ctl.pool.submit.call_args.args[1]
    assert payload['background']['decision_authority']=='deepseek'
    assert payload['current']['reinvestment_budget']['per_trade_risk_usdt']==30
    future.set_result({'status':'complete','decisions':[decision(action=action,confidence=.01,take_profit_pct=.0001)]})
    ctl.tick()
    assert ctl.signal('BTCUSDT')==action
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('entry_context:BTCUSDT')['ds_authority'] is True
    e.lab_row=lab.row;e.lab_risk_cap=lambda s,side,cap:(cap,{})
    e.exchange.liquidity.return_value={'spread_bps':1,'ask_depth':1e8,'bid_depth':1e8,
        'depth_bps':10,'updated_ms':int(time.time()*1000)}
    plan=e.entry_profile(100,[],'BTCUSDT',action)
    assert plan['take_pct']==.0001
    assert plan['planned_risk_usdt']<=30+1e-8
    assert plan['margin']<=2000
    e.exchange.order.assert_not_called()


@pytest.mark.parametrize('mode',['expired','position_changed','price_changed','paused','stale_depth'])
def test_ds_authority_preserves_execution_constraints(controller,mode):
    ctl,e,db,lab,future,row=fast_market(controller)
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    ctl.tick();future.set_result({'status':'complete','decisions':[decision()]});ctl.tick()
    if mode=='expired': ctl.actions['BTCUSDT'][1]['started']-=100
    if mode=='position_changed': db.set('positions',{'BTCUSDT':{'opened_at':'new','amount':1,'entry':100}})
    if mode=='price_changed': row['price']=110
    if mode=='paused': db.set('paused',True)
    if mode=='stale_depth':
        assert ctl.signal('BTCUSDT')=='long'
        e.exchange.liquidity.return_value={'spread_bps':1,'ask_depth':1e8,'bid_depth':1e8,
            'depth_bps':10,'updated_ms':int(time.time()*1000)-5000}
        with pytest.raises(ValueError,match='freshness'):
            e.entry_profile(100,[],'BTCUSDT','long')
    else:
        assert ctl.signal('BTCUSDT')=='hold'
    assert not db.orders()
    e.exchange.order.assert_not_called()


def test_ds_authority_can_close_after_price_jump_with_low_confidence(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    db.set('positions',{'BTCUSDT':{'opened_at':'held','amount':1,'entry':100}})
    ctl.tick();future.set_result({'status':'complete','decisions':[decision(action='close',confidence=.01)]});ctl.tick()
    row['price']=110
    assert ctl.signal('BTCUSDT')=='close'
    assert ctl.signal('BTCUSDT')=='hold'


def test_ds_authority_requires_protected_paper_account(controller):
    _,e,_,_,_=controller
    for changes in ({'paper_ai_autopilot':False},{'paper_dynamic_exits':False},
                    {'api_key':'key'},{'live_enabled':True},{'mode':'live'}):
        with pytest.raises(ValueError):
            replace(e.cfg,paper_ds_authority=True,**changes).validate()


def test_fast_plan_waits_for_two_distinct_samples_and_is_consumed_once(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    ctl.tick();future.set_result({'status':'complete','decisions':[decision()]});ctl.tick()
    assert ctl.signal('BTCUSDT')=='hold'
    assert ctl.signal('BTCUSDT')=='hold'  # Same sample is not a confirmation.
    row['ts']+=1000
    assert ctl.signal('BTCUSDT')=='long'
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('entry_context:BTCUSDT')['shortline_version']=='ai-plan-flow-v1'
    ctl.pool.submit.assert_called_once()


def test_fast_plan_can_wait_without_buying_against_order_flow(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    row['vector'][6]=-1
    ctl.tick();future.set_result({'status':'complete','decisions':[decision()]});ctl.tick()
    assert ctl.signal('BTCUSDT')=='hold'
    assert '盘口' in db.get('signal:BTCUSDT')['reason']
    row['vector'][6]=1;row['ts']+=1000
    assert ctl.signal('BTCUSDT')=='hold'
    row['ts']+=1000
    assert ctl.signal('BTCUSDT')=='long'


def test_fast_plan_must_recheck_latest_direction_and_cannot_reuse_expired_plan(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    ctl.tick();future.set_result({'status':'complete','decisions':[decision()]});ctl.tick()
    ctl.actions['BTCUSDT'][1]['started']-=120
    assert ctl.signal('BTCUSDT')=='hold'  # Still valid, waiting second sample.
    lab.bus.get.side_effect=lambda k:None
    row['ts']+=1000
    assert ctl.signal('BTCUSDT')=='hold'
    ctl.actions['BTCUSDT'][1]['started']-=200
    assert ctl.signal('BTCUSDT')=='hold'
    assert '过期' in db.get('signal:BTCUSDT')['reason']


def test_fast_plan_respects_symbol_cooldown_and_cost_filtered_sizing(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    ctl.tick();future.set_result({'status':'complete','decisions':[decision()]});ctl.tick()
    db.set('shortline_cooldown:BTCUSDT',time.time()+120)
    assert ctl.signal('BTCUSDT')=='hold'
    assert '冷却' in db.get('signal:BTCUSDT')['reason']
    db.set('shortline_cooldown:BTCUSDT',0)
    assert ctl.signal('BTCUSDT')=='hold';row['ts']+=1000
    assert ctl.signal('BTCUSDT')=='long'
    e.lab_row=lab.row;e.lab_risk_cap=lambda s,side,cap:(cap,{})
    e.exchange.liquidity.return_value={'spread_bps':1,'ask_depth':1e8,'bid_depth':1e8,
        'depth_bps':10,'updated_ms':int(time.time()*1000)}
    plan=e.entry_profile(100,[],'BTCUSDT','long')
    assert plan['planned_risk_usdt']<=30+1e-8
    assert plan['margin']<=e.cfg.paper_margin
    row['vector'][6]=-1
    with pytest.raises(ValueError,match='盘口'):
        e.entry_profile(100,[],'BTCUSDT','long')
    e.exchange.order.assert_not_called()


def test_held_contract_without_fresh_price_does_not_spend_ai_tokens(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    db.set('positions',{'BTCUSDT':{'opened_at':'held','amount':1,'entry':100,'mark':100}})
    lab.gate.return_value=False
    lab.row.side_effect=ValueError('feature gap')
    lab.management_row.return_value={'symbol':'BTCUSDT','price':None,'ts':None,'vector':None,
        'meta':{'quality':False,'atr_proxy':None},'management_only':True}
    ctl.tick()
    ctl.pool.submit.assert_not_called()
    assert db.get('autopilot:last_request') is None
    assert db.get('autopilot')['status']=='waiting_data'


def test_held_update_rejects_missing_price_without_removing_existing_stop(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    p={'opened_at':'held','amount':1,'entry':100,'mark':100,'stop_price':99,
       'stop_pct':.01,'exit_plan':{'stop_pct':.01,'take_pct':.02}}
    db.set('positions',{'BTCUSDT':p})
    lab.row.side_effect=lambda s:{'symbol':s,'price':100,'ts':int(time.time()*1000),
        'vector':[0.]*len(FEATURES),'meta':{'atr_proxy':.001,'schema':SCHEMA,'quality':True}}
    ctl.tick();future.set_result({'status':'complete','decisions':[decision(action='update')]});ctl.tick()
    lab.row.side_effect=ValueError('feature gap')
    lab.management_row.return_value={'price':None,'ts':None}
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('positions')['BTCUSDT']['stop_price']==99
    assert '新鲜报价' in db.get('signal:BTCUSDT')['reason']


def test_pending_exit_cannot_be_canceled_by_ai_update(controller):
    ctl,e,db,lab,future=controller
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    p={'opened_at':'held','amount':1,'entry':100,'mark':100,'stop_price':99,
       'stop_pct':.01,'exit_plan':{'stop_pct':.01,'take_pct':.02}}
    db.set('positions',{'BTCUSDT':p})
    e.paper_execution=MagicMock()
    e.paper_execution.exits.return_value={'BTCUSDT':{'reason':'ds_stop_loss'}}
    e.paper_execution.exit_intent.return_value={'reason':'ds_stop_loss'}
    ctl.tick();future.set_result({'status':'complete','decisions':[decision(action='update')]});ctl.tick()
    assert ctl.signal('BTCUSDT')=='hold'
    assert db.get('positions')['BTCUSDT']['stop_price']==99
    assert '已触发退出' in db.get('signal:BTCUSDT')['reason']


def test_ds_receives_explicit_round_trip_cost_not_just_fee_rate(controller):
    ctl,e,db,lab,future,row=fast_market(controller)
    e.cfg=replace(e.cfg,paper_ds_authority=True)
    ctl.tick()
    payload=ctl.pool.submit.call_args.args[1]
    costs=payload['current']['markets'][0]['execution_cost']
    assert costs['break_even_move_pct']==pytest.approx(.0017)
    assert not costs['funding_and_jump_risk_included']
    assert 'by_side' in payload['current']['realized_feedback']
