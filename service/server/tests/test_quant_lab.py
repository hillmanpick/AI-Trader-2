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
from quant.lab_train import label_rows, partition, fit, train, training_interval_seconds
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


def test_ds_entry_parameters_are_revalidated_and_hard_capped_by_paper_executor(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    e.cfg = replace(e.cfg,strategy='lab_model',paper_lab_required=True,paper_ai_autopilot=True,
                    paper_exploration_mode=True)
    e.lab_gate = lambda symbol: True
    e.lab_row = lambda symbol: row(int(time.time()*1000))
    e.lab_risk_cap = lambda symbol,side,maximum: (maximum,{})
    db.set('entry_context:BTCUSDT',{'ai_parameters':{'margin_usdt':999999,'leverage':100,
        'stop_loss_pct':.005,'take_profit_pct':.01,'hold_seconds':600},
        'strategy':'ds_autopilot','model_approved':False,'exploration':True})
    api.liquidity.return_value.update(ask_depth=1e8,bid_depth=1e8)
    e.execute('BTCUSDT','long',100,'deepseek_autopilot','one')
    p=db.get('positions')['BTCUSDT']
    assert p['initial_margin']==2000
    assert p['leverage']<=20
    assert p['exit_plan']['planned_risk_usdt']<=200
    assert p['stop_pct']<=.005
    api.order.assert_not_called()
    with pytest.raises(ValueError,match='minimum leverage'):
        plan(remaining_risk=1)


def test_training_watch_interval_is_bounded(monkeypatch):
    monkeypatch.setenv('QUANT_LAB_TRAIN_INTERVAL_SECONDS', '900')
    assert training_interval_seconds() == 900
    monkeypatch.setenv('QUANT_LAB_TRAIN_INTERVAL_SECONDS', '299')
    with pytest.raises(ValueError, match='between 300 and 86400'):
        training_interval_seconds()


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
    assert b.sequence_gap_active
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


def test_futures_snapshot_bridge_accepts_next_diff_when_pu_equals_snapshot_id():
    b = DepthBook()
    b.seed({'lastUpdateId':90052,'bids':[['99','10']],'asks':[['101','10']]},1000)
    # Binance USD-M can emit a merged range whose U is greater than the REST
    # snapshot id while pu proves the previous event ended exactly there.
    first = {'U':100045,'u':100058,'pu':90052,'b':[['99','9']],'a':[]}
    assert b.apply(first,1001)
    assert b.ready and b.last==100058 and b.gaps==0 and not b.sequence_gap_active
    assert b.apply({'U':100059,'u':100070,'pu':100058,'b':[],'a':[['101','8']]},1002)
    assert b.last==100070 and b.gaps==0


def test_futures_snapshot_bridge_still_rejects_unproven_first_diff():
    b = DepthBook()
    b.seed({'lastUpdateId':90052,'bids':[['99','10']],'asks':[['101','10']]},1000)
    assert not b.apply({'U':100045,'u':100058,'pu':90051,'b':[],'a':[]},1001)
    assert not b.ready and b.last is None and b.gaps==1 and b.sequence_gap_active


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
    # The shared planner sizes down before submission, rather than submitting
    # an oversized order and calling the predictable truncation a partial fill.
    assert all(t['margin']*t['leverage'] <= 15000 for t in result['fills'])
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


def test_stats_separates_clean_training_schema_from_raw_samples(tmp_path):
    db = LabStore(str(tmp_path))
    samples = [row(i * 30000, price=100 + i * .01) for i in range(8)]
    legacy = samples[-1] | {'meta': samples[-1]['meta'] | {'schema': 'microstructure-v2'}}
    db.append(samples[:-1] + [legacy])
    from quant.event_labels import LABEL_SCHEMA
    labels = label_rows(samples[:-1], 60000)
    db.append_labels([r | {'valid': True, 'schema': LABEL_SCHEMA} for r in labels])
    stats = db.stats()
    assert stats['samples'] == 8
    assert stats['clean_training']['samples'] == len(labels)
    assert stats['clean_training']['schema'] == SCHEMA


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
    assert model['threshold'] == report['folds'][-1]['threshold']


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


def test_exploration_keeps_rejection_and_checks_age_integrity(tmp_path):
    from quant.execution_policy import model_version
    samples = [row(i,label=(i%3)-1) | {'label':(i%3)-1} for i in range(90)]
    model = fit(samples) | {'approved':False,'trained_ms':int(time.time()*1000),'threshold':.6,'test':{}}
    model['version'] = model_version(model)
    path = tmp_path/'model.json'
    path.write_text(json.dumps(model))
    strict = LabConfig(directory=str(tmp_path),exploration_mode=False)
    assert not model_status(strict)[0]['ready']
    cfg = replace(strict,exploration_mode=True,exploration_threshold=.45)
    status, candidate = model_status(cfg)
    assert status['ready'] and status['exploration'] and not candidate['approved']
    assert candidate['threshold'] == .45 and candidate['artifact_version'] == model['version']
    assert json.loads(path.read_text()) == model
    assert not model_status(cfg,now=model['trained_ms']+49*3600000)[0]['ready']
    model['coef'][0][0] += 1
    path.write_text(json.dumps(model))
    assert not model_status(cfg)[0]['ready']


def test_lab_step_uses_features_without_requesting_closed_candles(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    e.base_cfg = replace(e.base_cfg,strategy='lab_model',paper_lab_required=True,symbols=('BTCUSDT',))
    e.cfg = e.base_cfg
    e.ready = True
    api.last_sync = time.time()
    api.mark.return_value = 100
    api.closed_candles.side_effect = ValueError('no candles available')
    e.candle_cache.clear()
    e.lab_signal = MagicMock(return_value='long')
    e.execute = MagicMock()
    e.step()
    api.closed_candles.assert_not_called()
    e.execute.assert_called_once()


def test_lab_sizing_without_candles_shrinks_exploration_to_real_depth(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    e.cfg = replace(e.cfg,strategy='lab_model',paper_lab_required=True,paper_exploration_mode=True)
    e.candle_cache.clear()
    e.lab_gate = lambda symbol: True
    e.lab_row = lambda symbol: row(int(time.time()*1000))
    e.lab_risk_cap = lambda symbol,side,maximum: (maximum,{})
    api.liquidity.return_value['ask_depth'] = 3220
    e.execute('BTCUSDT','long',100,'lab_model','1')
    p = db.get('positions')['BTCUSDT']
    assert p['initial_margin'] < 2000
    assert abs(p['amount'])*100 <= 161.000001
    assert p['leverage'] >= 2
    api.order.assert_not_called()


def test_lab_rejects_stale_or_invalid_redis_feature(tmp_path):
    from quant.lab_execution import LabExecution
    lab = LabExecution.__new__(LabExecution)
    lab.bus = MagicMock()
    lab.bus.get.return_value = row(int(time.time()*1000)-11000)
    with pytest.raises(ValueError,match='stale'):
        lab.row('BTCUSDT')
    lab.bus.get.return_value = row(int(time.time()*1000))
    assert lab.row('BTCUSDT')['meta']['quality']
    lab.bus.get.return_value['meta']['schema'] = 'bad'
    with pytest.raises(ValueError):
        lab.row('BTCUSDT')


def test_systematic_lab_rejects_features_older_than_event_tick():
    from quant.lab_execution import LabExecution, FAST_FEATURE_MAX_AGE_MS
    lab = LabExecution.__new__(LabExecution)
    lab.quant = object()
    lab.bus = MagicMock()
    current = row(int(time.time()*1000)-FAST_FEATURE_MAX_AGE_MS-1)
    current['fast'] = {'ready':True}
    lab.bus.get.return_value = current
    with pytest.raises(ValueError,match='stale'):
        lab.row('BTCUSDT')
    current['ts'] = int(time.time()*1000)
    current['fast'] = {'ready':False}
    with pytest.raises(ValueError,match='Subsecond'):
        lab.row('BTCUSDT')


def test_systematic_lab_accepts_recent_websocket_feature_within_1500ms():
    from quant.lab_execution import LabExecution,FAST_FEATURE_MAX_AGE_MS
    assert FAST_FEATURE_MAX_AGE_MS==1500
    lab=LabExecution.__new__(LabExecution)
    lab.quant=object()
    lab.bus=MagicMock()
    current=row(int(time.time()*1000)-1000)
    current['fast']={'ready':True}
    lab.bus.get.return_value=current
    assert lab.row('BTCUSDT')['symbol']=='BTCUSDT'


def test_exploration_rejects_live_or_credentials(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    cfg = replace(e.cfg,strategy='lab_model',paper_lab_required=True,paper_exploration_mode=True)
    cfg.validate()
    with pytest.raises(ValueError):
        replace(cfg,api_key='test',api_secret='test').validate()
    with pytest.raises(ValueError):
        replace(cfg,mode='live',live_enabled=True,api_key='test',api_secret='test').validate()


def test_lab_cost_gate_uses_same_target_floor_as_sizing(tmp_path,monkeypatch):
    from quant.lab_execution import LabExecution
    e,db,api = dynamic_engine(tmp_path)
    lab = LabExecution.__new__(LabExecution)
    lab.engine,lab.last_decision = e,{}
    lab.model = {'version':'test'}
    lab.status = {'exploration':True}
    lab.gate = lambda symbol: True
    sample = row(int(time.time()*1000))
    sample['meta']['atr_proxy'] = .0008
    lab.row = lambda symbol: sample
    monkeypatch.setattr('quant.lab_execution.decide',lambda model,row: {'action':'long','confidence':.6})
    assert lab.signal('BTCUSDT') == 'long'
    lab.last_decision.clear()
    sample['meta']['spread_bps'] = 10
    assert lab.signal('BTCUSDT') == 'hold'
    assert db.get('signal:BTCUSDT')['reason'] == 'Cost exceeds expected target space'


def test_small_depth_order_filter_does_not_abort_other_candidates(tmp_path):
    e,db,api = dynamic_engine(tmp_path)
    api.quantity.side_effect = ValueError('Order below minimum notional; will not be increased')
    e.execute('BTCUSDT','long',100,'test','1')
    assert not db.get('positions',{})
    assert db.get('sizing:BTCUSDT')['status'] == 'blocked'
    assert 'minimum notional' in db.get('sizing:BTCUSDT')['reason']


def test_feature_warmup_future_frames_trade_gaps_and_causality():
    b = FeatureBuilder()
    base = 100000
    book = {'mid':100,'bid':99.99,'ask':100.01,'bid_qty':10,'ask_qty':10,
            'spread_bps':2,'imbalance':0,'bid_depth':1e6,'ask_depth':1e6,
            'ofi_5s':0.0,'ofi_events':12,'ofi_ready':True}
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


def test_event_features_track_subsecond_returns_and_coalesce_same_millisecond():
    builder = FeatureBuilder()
    base = 2_000_000
    output = None
    for i in range(701):
        now = base+i*100
        price = 100*(1+i*.000001)
        builder.trade({'s':'BTCUSDT','a':i+1,'p':price,'q':1,'m':False,'T':now},now)
        book = {'mid':price,'bid':price-.005,'ask':price+.005,'bid_qty':12,'ask_qty':8,
                'spread_bps':1.,'imbalance':.2,'bid_depth':10000,'ask_depth':9000,
                'ofi_5s':.5,'ofi_events':50,'ofi_ready':True,'ofi_1s':.4,'ofi_1s_events':8}
        try:
            output = builder.build('BTCUSDT',book,now,now)
        except ValueError as exc:
            assert 'warmup' in str(exc)
    assert output['fast']['ready']
    assert output['fast']['return_250ms'] > 0
    assert output['fast']['return_1s'] > 0
    history_len = len(builder.history['BTCUSDT'])
    same_ms = builder.build('BTCUSDT',book,now,now)
    assert len(builder.history['BTCUSDT']) == history_len
    assert same_ms['fast']['ready']
