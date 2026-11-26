"""Paper multi-position execution must retain truthful, budgeted fills."""
import sys
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.settings import Settings
from quant.engine import Engine
from quant.paper_execution import PaperExecution
from quant.quant_strategy import policy_for_quant, risk_plan
from quant.store import Store


def config(tmp_path):
    return Settings(db_path=str(tmp_path/'paper.db'), mode='paper', enabled=True,
        strategy='lab_model', paper_lab_required=True, paper_quant_system=True,
        paper_quant_exploration=True, paper_unrestricted_positions=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True,
        reflection_enabled=False, paper_margin_is_ceiling=True,
        paper_margin=100, paper_max_margin=100, max_positions=1,
        max_notional=100, order_notional=100, leverage=20,
        paper_compound_risk_fraction=.001)


def test_unrestricted_positions_are_exclusively_credential_free_paper(tmp_path):
    cfg=config(tmp_path)
    cfg.validate()
    for overrides in ({'mode':'live'}, {'api_key':'dummy'}, {'api_secret':'dummy'},
                      {'live_enabled':True}, {'paper_quant_system':False}):
        with pytest.raises(ValueError, match='Unrestricted positions'):
            replace(cfg, **overrides).validate()


def test_four_same_direction_positions_fill_past_configured_portfolio_caps(tmp_path, monkeypatch):
    cfg=config(tmp_path)
    cfg.validate()
    clock=[time.time_ns()]
    monkeypatch.setattr('quant.paper_execution.time.time_ns',lambda:clock[0])
    monkeypatch.setattr('quant.paper_execution.time.time',lambda:clock[0]/1e9)
    db=Store(cfg.db_path)
    db.set('account', {'equity':100000., 'available':100000.})
    db.set('paper_balance', 100000.)
    market=MagicMock()
    market.quantity.side_effect=lambda s,n,p:str(n/p)
    now=int(time.time()*1000)
    liquidity={'bid':99.99,'ask':100.01,'spread_bps':2.,'ask_depth':1e6,
        'bid_depth':1e6,'depth_bps':10.,'updated_ms':now}
    market.liquidity.return_value=liquidity
    engine=Engine(cfg,db,market)
    engine.lab_gate=lambda s:True
    engine.lab_row=lambda s:{'symbol':s,'price':100.,'meta':{'atr_proxy':.002}}
    engine.lab_risk_cap=MagicMock(side_effect=AssertionError('Portfolio cap called'))
    bus=MagicMock()
    bus.get.side_effect=lambda key:{'lastUpdateId':100,'updated_ms':int(time.time()*1000),
        'bids':[[99.99,1000]],'asks':[[100.01,1000]]} if key.startswith('book:') else None
    execution=PaperExecution(engine,bus)
    engine.paper_execution=execution
    for index,symbol in enumerate(('AAAUSDT','BBBUSDT','CCCUSDT','DDDUSDT')):
        db.set('entry_context:'+symbol, {'quant_version':'test','quant_strategy':'flow_momentum',
            'experiment_id':'test','model_version':'test','exploration':True})
        engine.execute(symbol,'long',100,'quant_system',str(index))
        clock[0]+=100000000
        execution.tick()
    positions=db.get('positions')
    assert len(positions)==4 and all(p['amount']>0 for p in positions.values())
    assert sum(p['initial_margin'] for p in positions.values())>cfg.paper_max_margin
    assert sum(p['amount']*p['entry'] for p in positions.values())>cfg.max_notional
    assert len(execution.journal.orders())==4
    assert all(float(o['filled'])>0 for o in execution.journal.orders())
    assert all(float(o['notional'])<=float(o['max_quote']) for o in execution.journal.orders())
    engine.lab_risk_cap.assert_not_called()
    engine.execute('AAAUSDT','long',100,'duplicate','duplicate')
    assert len(execution.journal.orders())==4
    db.set('account',{'equity':0.,'available':0.})
    engine.execute('EEEUSDT','long',100,'no_funds','no_funds')
    assert len(execution.journal.orders())==4
    market.liquidity.return_value=liquidity|{'updated_ms':0}
    engine.execute('FFFUSDT','long',100,'stale','stale')
    assert db.get('sizing:FFFUSDT')['status']=='blocked'
    market.order.assert_not_called()


def test_aggregate_risk_cap_does_not_stop_unrestricted_paper(tmp_path):
    cfg=config(tmp_path)
    row={'symbol':'ALTUSDT','price':100.,'meta':{'atr_proxy':.002}}
    liquidity={'spread_bps':2.}
    positions={'OTHERUSDT':{'exit_plan':{'planned_risk_usdt':10000},'initial_margin':10000}}
    plan=risk_plan(row,liquidity,policy_for_quant(cfg),positions,100000.)
    assert plan['notional']>0
    with pytest.raises(ValueError, match='risk exhausted'):
        risk_plan(row,liquidity,policy_for_quant(replace(cfg,paper_unrestricted_positions=False)),positions,100000.)
