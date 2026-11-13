import json
import sys
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.paper_execution import PaperExecution
from quant.execution_policy import policy_for_settings, microstructure_plan
from quant.settings import Settings
from quant.store import Store
from quant.shortline import (VERSION,STATS_KEY,entry_check,compound_budget,compound_plan,protected_exit_price)
from quant.strategies import update_dynamic_exit


def setup(tmp_path):
    cfg=Settings(db_path=str(tmp_path/'paper.db'),paper_margin=2000,paper_max_margin=6000,
                 paper_min_leverage=2,paper_max_leverage=20,leverage=20,
                 paper_margin_is_ceiling=True,paper_slippage=.0003)
    return cfg,Store(cfg.db_path)


def market(side=1):
    return {'symbol':'BTCUSDT','price':100.,'ts':10000,
            'vector':[.0001*side,.001*side,.002*side,.0002,1,.2*side,1*side,.2*side,10,1.5,0,10,10,5],
            'meta':{'atr_proxy':.003}}


def parameters():
    return {'margin_usdt':800,'leverage':10,'stop_loss_pct':.005,'take_profit_pct':.01}


@pytest.mark.parametrize('side',[1,-1])
def test_order_flow_filter_treats_long_and_short_symmetrically(tmp_path,side):
    cfg,_=setup(tmp_path)
    row=market(side)
    action='long' if side>0 else 'short'
    assert entry_check(row,action,parameters(),cfg)['passes']
    row['vector'][6]*=-1
    assert not entry_check(row,action,parameters(),cfg)['passes']


def test_cost_filter_rejects_low_volatility_and_invalid_features(tmp_path):
    cfg,_=setup(tmp_path)
    row=market();row['meta']['atr_proxy']=.0001
    assert not entry_check(row,'long',parameters(),cfg)['passes']
    row['meta']['atr_proxy']=float('nan')
    assert not entry_check(row,'long',parameters(),cfg)['passes']
    row=market();row['vector'][0]=float('inf')
    assert not entry_check(row,'long',parameters(),cfg)['passes']


def test_compounding_uses_only_policy_net_not_deposits_or_old_profit(tmp_path):
    cfg,store=setup(tmp_path)
    baseline=compound_budget(cfg,store,{})
    store.set('paper_balance',9999999);store.set('account',{'equity':9999999})
    assert compound_budget(cfg,store,{})==baseline
    store.set(STATS_KEY,{'net':600,'peak_net':600})
    grown=compound_budget(cfg,store,{})
    assert grown['multiplier']==pytest.approx(1.1)
    assert grown['per_trade_risk_usdt']==pytest.approx(33)
    store.set(STATS_KEY,{'net':-600,'peak_net':600})
    smaller=compound_budget(cfg,store,{})
    assert 0<smaller['multiplier']<baseline['multiplier']
    # No hard daily-loss latch is introduced for paper trading.
    assert not store.get('daily_loss_blocked',False)
    store.set(STATS_KEY,{'net':0,'peak_net':0})
    p={'shortline_version':VERSION,'pnl':-60,'entry_fee':2}
    assert compound_budget(cfg,store,{'BTCUSDT':p})['usable_capital']==5938
    p['pnl']=600
    assert compound_budget(cfg,store,{'BTCUSDT':p})['usable_capital']==5998


def test_compound_plan_preserves_risk_margin_and_liquidity_caps(tmp_path):
    cfg,store=setup(tmp_path)
    liquidity={'spread_bps':1}
    budget=compound_budget(cfg,store,{})
    policy,params,maximum=compound_plan(policy_for_settings(cfg),parameters(),budget,{},100000,liquidity)
    plan=microstructure_plan(market(),liquidity,policy,{},maximum,params)
    assert plan['planned_risk_usdt']<=30+1e-8 and plan['margin']<=2000
    assert plan['notional']<=maximum+1e-8
    held={'ETHUSDT':{'initial_margin':1000,'exit_plan':{'planned_risk_usdt':90}}}
    with pytest.raises(ValueError,match='风险预算'):
        compound_plan(policy_for_settings(cfg),parameters(),budget,held,100000,liquidity)


@pytest.mark.parametrize('side',[1,-1])
def test_protected_trigger_covers_entry_fee_exit_fee_and_adverse_execution(side):
    p={'amount':2*side,'entry':100.,'entry_fee':.1,'shortline_version':VERSION,
       'exit_plan':{'liquidity':{'spread_bps':4}}}
    trigger=protected_exit_price(p,.0005,.0003)
    adverse=.0003+.0002
    actual=trigger*(1-side*adverse)
    net=2*(actual-100)*side-.1-2*actual*.0005
    assert net==pytest.approx(.04)
    p.update(mark=100*(1+side*.004),stop_pct=.01,stop_price=100*(1-side*.01))
    plan={'atr':.2,'stop_pct':.01,'take_pct':.02,'regime':'test'}
    assert update_dynamic_exit(p,plan,.0005,.0003) is None
    assert (p['stop_price']-trigger)*side>=-1e-10
    prior=p['stop_price'];p['mark']=100*(1+side*.003)
    update_dynamic_exit(p,plan,.0005,.0003)
    assert (p['stop_price']-prior)*side>=0


def test_partial_fills_update_policy_net_and_cycle_counts_atomically(tmp_path):
    cfg,store=setup(tmp_path)
    store.set('paper_balance',10000)
    engine=Engine(cfg,store,MagicMock())
    execution=PaperExecution(engine,MagicMock())
    context={'leverage':2,'shortline_version':VERSION,
             'entry_profile':{'stop_pct':.01,'take_pct':.02}}
    def fill(identity,closing,qty,price,fee,stamp):
        order={'id':identity,'symbol':'BTCUSDT','side':-1 if closing else 1,
               'closing':closing,'submitted_ns':stamp,'quantity':str(qty),
               'paper_context':context,'reason':'test'}
        f={'id':str(stamp),'quantity':str(qty),'price':str(price),'fee':str(fee),'at_ns':stamp}
        with store.connect() as c:execution.apply_fill(c,f,order)
    fill('entry',False,1,100,.05,10**9)
    fill('entry',False,1,102,.051,2*10**9)
    fill('close',True,1,103,.0515,3*10**9)
    partial=store.get(STATS_KEY)
    assert partial['completed']==0 and partial['net']==pytest.approx(1.898)
    # Restart retains the same counter; the second partial close completes one cycle.
    restarted=Store(cfg.db_path)
    assert restarted.get(STATS_KEY)==partial
    fill('close2',True,1,99,.0495,4*10**9)
    stats=store.get(STATS_KEY)
    assert stats['completed']==1 and stats['wins']==0
    assert stats['net']==pytest.approx(-.202) and stats['fees']==pytest.approx(.202)
    assert store.get('shortline_cooldown:BTCUSDT')==124
    assert store.get('positions')=={}


def test_fast_execution_cannot_enable_without_paper_ai(tmp_path):
    cfg,_=setup(tmp_path)
    with pytest.raises(ValueError,match='Shortline'):
        replace(cfg,paper_fast_execution=True).validate()
