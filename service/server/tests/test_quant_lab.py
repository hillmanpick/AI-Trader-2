import json
import math
import sys
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.lab_config import LabConfig
from quant.lab_features import FeatureBuilder, FEATURES, SCHEMA
from quant.lab_model import probabilities, model_status
from quant.lab_replay import replay
from quant.lab_store import LabStore
from quant.lab_train import label_rows, partition, fit, train
from quant.order_book import DepthBook
from quant.strategies import dynamic_leverage_plan
from quant.settings import Settings
from quant.store import Store
from quant.engine import Engine


def bars(width=.1):
    return [[i*60000,100,100+width,100-width,100,100,(i+1)*60000-1] for i in range(40)]


def plan(width=.1,**kwargs):
    return dynamic_leverage_plan(bars(width),100,2000,.0005,.0003,2,20,.1,.005,**kwargs)


def test_dynamic_leverage_falls_with_volatility_and_never_tightens_stop():
    low,high = plan(.1),plan(.5)
    assert high['leverage'] < low['leverage']
    assert high['stop_pct'] == pytest.approx(.015)
    assert high['planned_risk_usdt'] <= 200
    assert low['notional'] == low['margin']*low['leverage']
    with pytest.raises(ValueError,match='minimum leverage'):
        plan(notional_cap=3999)
    with pytest.raises(ValueError,match='minimum leverage'):
        plan(remaining_risk=1)


def test_depth_bridge_duplicates_gap_rebuild_and_zero_deletion():
    b = DepthBook()
    b.seed({'lastUpdateId':100,'bids':[['99','10'],['98','10']],'asks':[['101','10'],['102','10']]},1000)
    assert not b.ready
    assert not b.apply({'U':90,'u':99,'pu':89,'b':[],'a':[]},1001)
    assert b.apply({'U':99,'u':101,'pu':98,'b':[['98','0']],'a':[]},1002)
    assert 98 not in b.bids
    assert not b.apply({'U':99,'u':101,'pu':98,'b':[],'a':[]},1003)
    assert b.ready
    assert not b.apply({'U':103,'u':104,'pu':102,'b':[],'a':[]},1004)
    assert b.last is None and b.gaps == 1
    with pytest.raises(ValueError):
        b.metrics(1005)


def test_truncated_depth_is_a_conservative_lower_bound_and_stale_is_blocked():
    b = DepthBook()
    b.seed({'lastUpdateId':1,'bids':[['99.99','10']],'asks':[['100.01','10']]},1000)
    b.apply({'U':1,'u':2,'pu':0,'b':[],'a':[]},1001)
    m = b.metrics(1002,10)
    assert m['depth_lower_bound'] and m['ask_depth'] == pytest.approx(1000.1)
    with pytest.raises(ValueError,match='stale'):
        b.metrics(4000)


def row(ts,price=100,symbol='BTCUSDT',label=0):
    return {'symbol':symbol,'ts':ts,'price':price,'vector':[float(label)]*len(FEATURES),
            'meta':{'quality':True,'schema':SCHEMA,'atr_proxy':.001,'bid':price*.99999,
                    'ask':price*1.00001,'spread_bps':.2,'bid_depth':1e7,'ask_depth':1e7}}


def test_labels_purge_future_horizon_and_drop_gaps():
    rows = [row(i*5000,100+i*.03) for i in range(100)]
    labels = label_rows(rows,60000)
    assert labels[0]['label'] == 1
    tr,va,te = partition(labels,150000,300000,490000)
    assert max(r['label_end'] for r in tr) < min(r['ts'] for r in va)
    assert max(r['label_end'] for r in va) < min(r['ts'] for r in te)
    gap = [row(0),row(100000),row(105000)]
    assert not label_rows(gap,60000)


def test_exported_logistic_probabilities_and_schema_gate(tmp_path):
    rows = [row(i, label=(i%3)-1) | {'label':(i%3)-1} for i in range(90)]
    model = fit(rows)
    for side in (-1,0,1):
        p = probabilities(model,[float(side)]*len(FEATURES))
        assert max(p,key=p.get) == side
        assert sum(p.values()) == pytest.approx(1)
    with pytest.raises(ValueError,match='schema'):
        probabilities(model,[0])
    cfg = LabConfig(directory=str(tmp_path))
    assert not model_status(cfg)[0]['ready']
    (tmp_path/'model.json').write_text(json.dumps(model | {'approved':True,'version':'v1',
        'trained_ms':0,'threshold':.6,'test':{}}))
    assert not model_status(cfg)[0]['ready']


def test_replay_flat_price_loses_costs_partial_fill_and_delay():
    rows = [row(i*5000) for i in range(80)]
    for r in rows:
        r['meta']['ask_depth'] = r['meta']['bid_depth'] = 300000
    result = replay(rows,[1]*len(rows),horizon_ms=30000)
    assert result['trades'] > 0 and result['net_pnl'] < 0 and result['fees'] > 0
    assert result['partial_fills'] > 0
    assert all(t['at'] > 0 for t in result['fills'])
    blocked = replay(rows,[1]*len(rows),latency_ms=9999999)
    assert blocked['trades'] == 0


def test_history_survives_and_training_refuses_insufficient_evidence(tmp_path):
    cfg = LabConfig(directory=str(tmp_path),min_samples=1000,min_days=1)
    db = LabStore(cfg.directory)
    db.append([row(int(time.time()*1000))])
    assert LabStore(cfg.directory).stats()['samples'] == 1
    assert train(cfg)['status'] == 'insufficient_data'
    assert not (tmp_path/'model.json').exists()


def test_full_training_exports_auditable_artifact_with_three_purged_folds(tmp_path):
    cfg = LabConfig(directory=str(tmp_path),min_samples=1000,min_days=1)
    start = int(time.time()*1000)-2*86400000
    samples = []
    for i in range(4000):
        price = 100 if i%300 < 100 else 100+math.sin(i/15)
        sample = row(start+i*30000,price)
        sample['vector'][0] = math.cos(i/15) if i%300 >= 100 else 0.
        samples.append(sample)
    LabStore(cfg.directory).append(samples)
    from quant.event_labels import LABEL_SCHEMA
    labels = label_rows(samples,cfg.horizon_seconds*1000)
    LabStore(cfg.directory).append_labels([r | {'valid':True,'schema':LABEL_SCHEMA} for r in labels])
    report = train(cfg)
    assert report['status'] in ('approved','rejected')
    assert len(report['folds']) == 3
    assert all(f['train_end_ms'] < f['test_start_ms'] for f in report['folds'])
    model = json.loads((tmp_path/'model.json').read_text())
    assert model['approved'] == (report['status'] == 'approved')
    assert not model['approved']  # no event holdout can be fabricated from quotes
    assert not model['execution_validation']['passed']
    assert len(model['coef'][0]) == len(FEATURES)


def dynamic_engine(tmp_path):
    cfg = Settings(db_path=str(tmp_path/'paper.db'),mode='paper',strategy='adaptive',
        leverage=20,paper_min_leverage=2,paper_max_leverage=20,paper_dynamic_leverage=True,
        paper_dynamic_exits=True,paper_margin=2000,paper_max_margin=6000,
        order_notional=40000,max_notional=120000,paper_slippage=.0003)
    api = MagicMock()
    api.liquidity.return_value = {'spread_bps':1,'ask_depth':1e7,'bid_depth':1e7,
                                 'depth_bps':10,'updated_ms':int(time.time()*1000)}
    api.quantity.side_effect = lambda s,n,p:str(n/p)
    db = Store(cfg.db_path)
    db.set('account',{'equity':100000,'available':100000})
    db.set('paper_balance',100000)
    engine = Engine(cfg,db,api)
    engine.candle_cache['BTCUSDT'] = (0,bars())
    return engine,db,api


def test_execution_recalculates_leverage_after_review_and_blocks_stale_depth(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    db.set('entry_context:BTCUSDT',{'leverage':20,'order_notional':40000})
    api.liquidity.return_value['updated_ms'] = 0
    e.execute('BTCUSDT','long',100,'test','1')
    assert not db.get('positions',{})
    api.liquidity.return_value['updated_ms'] = int(time.time()*1000)
    e.execute('BTCUSDT','long',100,'test','2')
    p = db.get('positions')['BTCUSDT']
    assert p['leverage'] < 20 and p['initial_margin'] == 2000
    assert p['exit_plan']['planned_risk_usdt'] <= 200
    api.order.assert_not_called()


def test_missing_model_gate_never_blocks_existing_position_exit(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    e.cfg = replace(e.cfg,paper_lab_required=True)
    e.execute('BTCUSDT','long',100,'test','1')
    assert not db.get('positions',{})
    db.set('positions',{'BTCUSDT':{'amount':1,'entry':100,'mark':100,'leverage':2,'initial_margin':50}})
    e.execute('BTCUSDT','close',99,'stop','2')
    assert not db.get('positions')
    api.order.assert_not_called()


def test_feature_warmup_future_frames_trade_gaps_and_causality():
    b = FeatureBuilder()
    base = 100000
    book = {'mid':100,'bid':99.99,'ask':100.01,'bid_qty':10,'ask_qty':10,
            'spread_bps':2,'imbalance':0,'bid_depth':1e6,'ask_depth':1e6}
    for i in range(15):
        now = base+i*5000
        b.trade({'s':'BTCUSDT','a':i,'p':100,'q':1,'m':False,'T':now},now)
        try:
            value = b.build('BTCUSDT',book,now,now-10)
        except ValueError:
            continue
    assert len(value['vector']) == len(FEATURES)
    assert value['meta']['quality'] and all(math.isfinite(v) for v in value['vector'])
    now += 5000
    b.trade({'s':'BTCUSDT','a':99,'p':100,'q':1,'m':False,'T':now},now)
    with pytest.raises(ValueError,match='Trade stream'):
        b.build('BTCUSDT',book,now,now)
