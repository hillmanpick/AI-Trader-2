import sys
from pathlib import Path
from datetime import datetime,timezone
from dataclasses import replace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.settings import Settings
from quant.store import Store
from quant.paper_execution import PaperExecution
from quant.lab_execution import LabExecution
from quant.shortline import compound_budget


@pytest.fixture
def rig(tmp_path,monkeypatch):
    clock=[1800000000000000000]
    monkeypatch.setattr('quant.paper_execution.time.time_ns',lambda:clock[0])
    cfg=Settings(db_path=str(tmp_path/'paper.db'),paper_depth_participation=1,
                 strategy='lab_model',paper_ai_autopilot=True,enabled=True)
    store=Store(cfg.db_path);store.set('paper_balance',1000)
    opened=datetime.fromtimestamp(clock[0]/1e9-60,timezone.utc).isoformat()
    position={'opened_at':opened,'amount':5.,'entry':100.,'mark':100.,'pnl':0.,
              'leverage':2,'initial_margin':250.,'entry_fee':.25,'stop_pct':.01,
              'take_pct':.02,'stop_price':99.,'take_price':102.,
              'exit_plan':{'stop_pct':.01,'take_pct':.02},'ds_authority':True,
              'ds_hold_seconds':3600,'dynamic_exits':True}
    store.set('positions',{'BTCUSDT':position})
    api=MagicMock();engine=Engine(cfg,store,api);engine.ready=True
    bus=MagicMock();bus.get.return_value=None
    execution=PaperExecution(engine,bus);engine.paper_execution=execution
    def depth(qty=5,revision=100,price=100):
        bus.get.return_value={'lastUpdateId':revision,'updated_ms':clock[0]//1000000,
                              'bids':[[str(price),str(qty)]],'asks':[[str(price+.01),str(qty)]]}
    return clock,engine,store,bus,execution,depth


def test_exit_waits_for_depth_then_fills_after_rebound(rig):
    clock,e,s,b,x,depth=rig
    e.execute('BTCUSDT','close',102,'ds_take_profit','trigger')
    assert x.exit_intent('BTCUSDT')['reason']=='ds_take_profit'
    assert not x.journal.orders()
    # Trigger has disappeared, but its durable exit is still mandatory.
    depth(price=100);x.tick();clock[0]+=30000000;x.tick()
    assert not s.get('positions')
    assert not x.exits()
    assert x.journal.orders()[0]['state']=='FILLED'
    assert x.journal.orders()[0]['reason']=='ds_take_profit'
    e.exchange.order.assert_not_called()


def test_partial_exit_survives_restart_and_only_closes_residual(rig):
    clock,e,s,b,x,depth=rig
    depth(qty=2);x.submit('BTCUSDT',-5,100,'ds_stop_loss','first')
    clock[0]+=30000000;x.tick()
    assert s.get('positions')['BTCUSDT']['amount']==3
    assert x.journal.orders()[0]['state']=='EXPIRED'
    restarted=PaperExecution(e,b);e.paper_execution=restarted
    clock[0]+=2000000000;depth(qty=5,revision=101)
    restarted.tick();clock[0]+=30000000;restarted.tick()
    assert not s.get('positions') and not restarted.exits()
    assert float(restarted.journal.orders()[-1]['quantity'])==3
    assert sum(float(f['quantity']) for f in restarted.journal.fills())==5
    assert s.get('paper_balance')==pytest.approx(999.75)


def test_expired_ioc_retries_without_duplicate_or_stale_book_fills(rig):
    clock,e,s,b,x,depth=rig
    depth();x.submit('BTCUSDT',-5,100,'ds_take_profit','first')
    b.get.return_value=None;clock[0]+=30000000;x.tick()
    assert x.journal.orders()[0]['state']=='EXPIRED'
    for _ in range(5):
        clock[0]+=1000000000;x.tick()
    assert len(x.journal.orders())==1 and not x.journal.fills()
    depth(revision=101);x.tick()
    x.tick();assert len(x.journal.orders())==2
    clock[0]+=30000000;x.tick()
    assert not s.get('positions') and not x.exits()


def test_repeated_same_depth_revision_does_not_overfill(rig):
    clock,e,s,b,x,depth=rig
    depth(qty=2);x.submit('BTCUSDT',-5,100,'ds_stop_loss','one')
    clock[0]+=30000000;x.tick()
    clock[0]+=1100000000;depth(qty=2)
    x.tick()
    assert len(x.journal.orders())==1
    assert s.get('positions')['BTCUSDT']['amount']==3


def test_recovers_legacy_failed_close_for_current_position(rig):
    clock,e,s,b,x,depth=rig
    oid=x._submit('BTCUSDT',-5,100,'ds_take_profit','legacy',s.get('positions')['BTCUSDT'])
    clock[0]+=30000000;x.tick()
    assert x.journal.get(oid)['state']=='EXPIRED'
    assert not x.exit_intent('BTCUSDT')
    restarted=PaperExecution(e,b)
    assert restarted.exit_intent('BTCUSDT')['reason']=='ds_take_profit'
    depth(revision=101);restarted.tick();clock[0]+=30000000;restarted.tick()
    assert not s.get('positions')


def test_expired_intent_never_closes_a_new_position(rig):
    clock,e,s,b,x,depth=rig
    x.request_exit('BTCUSDT','ds_stop_loss')
    p=s.get('positions')['BTCUSDT'];p['opened_at']='2027-01-16T09:00:00+00:00'
    s.set('positions',{'BTCUSDT':p})
    depth();x.tick()
    assert not x.exits() and not x.journal.orders()
    assert s.get('positions')['BTCUSDT']['amount']==5


def test_undrained_exit_blocks_entries_and_liquidation_has_priority(rig):
    clock,e,s,b,x,depth=rig
    x.request_exit('BTCUSDT','ds_take_profit')
    e.execute('ETHUSDT','long',100,'deepseek_autopilot','new')
    assert not x.journal.orders()
    assert s.get('sizing:ETHUSDT')['status']=='blocked'
    x.request_exit('BTCUSDT','paper_liquidation')
    assert x.exit_intent('BTCUSDT')['reason']=='paper_liquidation'
    e.exchange.order.assert_not_called()


def test_horizon_exit_is_latched_even_without_mark_or_entry_features(rig):
    clock,e,s,b,x,depth=rig
    p=s.get('positions')['BTCUSDT'];p['opened_at']='2020-01-01T00:00:00+00:00'
    s.set('positions',{'BTCUSDT':p})
    e.exchange.last_sync=10**12;e.exchange.mark.side_effect=ValueError('no mark')
    e.lab_signal=lambda symbol:'hold'
    e.step()
    assert x.exit_intent('BTCUSDT')['reason']=='ds_horizon'
    assert not x.journal.fills()


def test_management_data_never_fabricates_missing_features_or_price(rig):
    clock,e,s,b,x,depth=rig
    lab=LabExecution.__new__(LabExecution);lab.engine=e
    lab.row=MagicMock(side_effect=ValueError('gap'))
    e.exchange.liquidity.side_effect=ValueError('missing')
    row=lab.management_row('BTCUSDT')
    assert row['management_only'] and row['vector'] is None
    assert row['price'] is None and row['ts'] is None
    assert not row['meta']['entry_allowed']


def test_compound_risk_reduction_keeps_no_daily_loss_latch(rig):
    _,e,s,_,_,_=rig
    cfg=replace(e.cfg,paper_compound_risk_fraction=.001)
    b=compound_budget(cfg,s,{})
    assert b['per_trade_risk_usdt']==6 and b['portfolio_risk_usdt']==18
    assert not s.get('daily_loss_blocked',False)
