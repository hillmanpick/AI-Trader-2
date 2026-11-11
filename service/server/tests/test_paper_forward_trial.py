import json
import sys
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.execution_policy import model_version
from quant.execution_state import OrderJournal
from quant.lab_config import LabConfig
from quant.lab_execution import LabExecution
from quant.lab_features import SCHEMA,FEATURES
from quant.paper_execution import PaperExecution
from quant.paper_trial import sync_forward_trial,forward_trial_status
from quant.settings import Settings
from quant.store import Store


def trial_settings(tmp_path):
    return Settings(db_path=str(tmp_path/'paper.db'),mode='paper',symbols=('BTCUSDT',),
        enabled=True,strategy='lab_model',paper_lab_required=True,paper_exploration_mode=True,
        paper_dynamic_leverage=True,paper_dynamic_exits=True,paper_margin_is_ceiling=True,
        paper_margin=2000,paper_max_margin=6000,leverage=20,paper_max_leverage=20,
        max_notional=120000,order_notional=40000,paper_depth_participation=.05,
        api_key='',api_secret='',live_enabled=False,ai_enabled=False,reflection_enabled=False)


def test_trial_baseline_is_idempotent_preserves_wallet_and_ignores_old_fills(tmp_path):
    cfg=trial_settings(tmp_path)
    db=Store(cfg.db_path);OrderJournal(db.path)
    db.set('paper_balance',979536.708)
    db.set('positions',{'EXISTING':{'amount':1}})
    with db.connect() as c:
        c.execute('INSERT INTO execution_orders VALUES (?,?,?,?)',
            ('old','old','FILLED',json.dumps({'submitted_ns':1,'closing':False,'symbol':'BTCUSDT','side':1})))
        c.execute('INSERT INTO execution_fills VALUES (?,?,?,?,?,?)',('old','old',1,'1','100','.05'))
    trial=sync_forward_trial(db,cfg)
    assert sync_forward_trial(db,cfg)==trial
    assert trial['baseline_fill_count']==1 and trial['baseline_wallet']==979536.708
    assert db.get('positions')=={'EXISTING':{'amount':1}}
    assert forward_trial_status(db)['fill_count']==0
    end=sync_forward_trial(db,replace(cfg,paper_exploration_mode=False))
    assert not end['active'] and end['id']==trial['id']
    restarted=sync_forward_trial(db,cfg)
    assert restarted['id']!=trial['id'] and db.get('paper_balance')==979536.708
    with pytest.raises(ValueError):
        sync_forward_trial(db,replace(cfg,api_key='dummy',api_secret='dummy'))


def test_real_model_to_engine_to_depth_journal_in_same_paper_account(tmp_path,monkeypatch):
    """Synthetic market fixture tests the complete path; it never touches production."""
    cfg=trial_settings(tmp_path);cfg.validate()
    db=Store(cfg.db_path);db.set('paper_balance',100000)
    db.set('account',{'equity':100000,'available':100000})
    market=MagicMock();market.last_sync=time.time();market.mark.return_value=100
    market.quantity.side_effect=lambda s,n,p:str(n/p)
    engine=Engine(cfg,db,market);engine.ready=True
    now=int(time.time()*1000)
    model={'schema':SCHEMA,'features':list(FEATURES),'classes':[-1,0,1],
        'mean':[0.]*len(FEATURES),'scale':[1.]*len(FEATURES),
        'coef':[[0.]*len(FEATURES) for _ in range(3)],'intercept':[-4.,-3.,4.],
        'threshold':.6,'approved':False,'trained_ms':now,'test':{},
        'execution_validation':{'passed':False,'reason':'fixture_rejected'}}
    model['version']=model_version(model)
    path=tmp_path/'model.json';path.write_text(json.dumps(model))
    row={'symbol':'BTCUSDT','ts':now,'price':100.,'vector':[0.]*len(FEATURES),
        'meta':{'quality':True,'schema':SCHEMA,'atr_proxy':.001,'spread_bps':2.}}
    liquidity={'spread_bps':2.,'ask_depth':100000.,'bid_depth':100000.,'depth_bps':10.,'updated_ms':now}
    market.liquidity.return_value=liquidity
    values={'feature:BTCUSDT':row,'state':{'ts':now,'ready_symbols':['BTCUSDT']},
        'candidates':{'ts':now,'symbols':['BTCUSDT']},
        'portfolio_returns':{'ts':now,'returns':{'BTCUSDT':{str(i):(-1 if i%2 else 1)*.00001 for i in range(60)}}},
        'book:BTCUSDT':{'lastUpdateId':100,'updated_ms':now,
            'bids':[['99.99','1000']],'asks':[['100.01','1000']]}}
    bus=MagicMock();bus.get.side_effect=values.get
    lab_cfg=LabConfig(enabled=True,directory=str(tmp_path),exploration_mode=True)
    monkeypatch.setattr('quant.lab_execution.LabConfig',lambda:lab_cfg)
    monkeypatch.setattr('quant.lab_execution.RealtimeBus',lambda url:bus)
    lab=LabExecution(engine)
    engine.lab_gate,engine.lab_signal,engine.lab_row,engine.lab_risk_cap=lab.gate,lab.signal,lab.row,lab.risk_cap
    execution=PaperExecution(engine,bus);engine.paper_execution=execution
    sync_forward_trial(db,cfg)
    engine.step()
    assert len(execution.journal.orders(active=True))==1
    assert not db.get('positions',{})  # submit is not a fill
    start=time.time_ns()
    monkeypatch.setattr('quant.paper_execution.time.time_ns',lambda:start+100000000)
    execution.tick()
    p=db.get('positions')['BTCUSDT']
    assert p['amount']>0 and 0<p['initial_margin']<=2000
    assert p['exploration'] and not p['model_approved'] and p['model_version']
    assert db.get('lab_gate')['ready'] and not db.get('lab_gate')['approved']
    evidence=forward_trial_status(db)
    assert evidence['filled_entry_orders']==1 and evidence['fill_count']>0
    assert evidence['last_fill']['model_version']==p['model_version']
    assert db.get('paper_balance')<100000 and evidence['fees_usdt']>0
    # Existing position exits must still work if the model stops being ready.
    engine.lab_gate=lambda symbol:False
    engine.execute('BTCUSDT','close',100,'test_exit','exit')
    monkeypatch.setattr('quant.paper_execution.time.time_ns',lambda:start+200000000)
    execution.tick()
    assert not db.get('positions')
    assert forward_trial_status(db)['filled_exit_orders']==1
    assert json.loads(path.read_text())==model  # never forge approval
    market.order.assert_not_called();market.closed_candles.assert_not_called()
