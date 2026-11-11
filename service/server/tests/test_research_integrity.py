import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.settings import Settings
from quant.store import Store
from quant.execution_state import OrderJournal
from quant.execution_policy import microstructure_plan,policy_for_settings
from quant.lab_replay import replay
from quant.lab_features import SCHEMA,FEATURES
from quant.lab_store import LabStore
from quant.paper_performance import paper_performance
from quant.reflection import StrategyReflector


def quote(ts, depth=1000):
    return {'symbol':'ALTUSDT','ts':ts,'price':100.,'vector':[0.]*len(FEATURES),
            'meta':{'bid':99.99,'ask':100.01,'bid_depth':depth,'ask_depth':depth,
                    'spread_bps':2.,'atr_proxy':.001,'quality':True,'schema':SCHEMA}}


def test_thin_book_replay_and_shared_entry_plan_use_same_margin_ceiling():
    cfg=Settings(paper_margin=2000,paper_max_margin=6000,paper_min_leverage=2,
                 paper_max_leverage=20,paper_depth_participation=.05,paper_exploration_mode=True)
    p=policy_for_settings(cfg)
    row=quote(0)
    plan=microstructure_plan(row,row['meta'],p,{},50)
    assert plan['margin']==25 and plan['notional']==50 and plan['leverage']==2
    result=replay([quote(i*5000) for i in range(12)],[1]*12,policy=p,horizon_ms=15000)
    assert result['trades']>0 and result['net_pnl']<0
    assert all(t['margin']==25 and t['leverage']==2 for t in result['fills'])
    blocked=replay([quote(i*5000) for i in range(12)],[1]*12,
                   policy=p|{'paper_exploration_mode':False})
    assert blocked['trades']==0
    assert blocked['rejection_reasons']['risk_or_liquidity']>0
    assert blocked['rejected']==sum(blocked['rejection_reasons'].values())


def test_sampled_quote_gap_invalidates_research_result():
    result=replay([quote(t,1000000) for t in (0,5000,10000,100000)],[1]*4)
    assert not result['valid'] and result['data_gaps_while_holding']['ALTUSDT']==1


def test_execution_quotes_do_not_depend_on_future_label_availability(tmp_path):
    db=LabStore(tmp_path)
    rows=[quote(i*5000) for i in range(6)]
    db.append(rows)
    # No event labels exist. Execution must still observe all causal quotes.
    assert db.labeled_rows()==[]
    assert [r['ts'] for r in db.market_rows(0,30000,stride_ms=5000)]==list(range(0,30000,5000))


def test_reflection_counts_one_position_across_partial_entries_and_closes(tmp_path):
    cfg=Settings(db_path=str(tmp_path/'paper.db'),mode='paper',reflection_enabled=True)
    db=Store(cfg.db_path);OrderJournal(cfg.db_path)
    def add(identity,closing,qty,price,fee,ns):
        order={'id':identity,'symbol':'BTCUSDT','side':-1 if closing else 1,
               'closing':closing,'reason':'exit' if closing else 'lab_model',
               'paper_context':{'leverage':10,'regime':'microstructure'}}
        with db.connect() as c:
            c.execute('INSERT OR IGNORE INTO execution_orders VALUES (?,?,?,?)',
                      (identity,identity,'FILLED',json.dumps(order)))
            c.execute('INSERT INTO execution_fills VALUES (?,?,?,?,?,?)',
                      (str(ns),identity,ns,str(qty),str(price),str(fee)))
    add('entry',False,1,100,.05,10**9)
    add('entry',False,1,102,.051,2*10**9)
    add('close-a',True,1,103,.0515,3*10**9)
    partial=paper_performance(db,include_trades=True)
    assert partial['completed_trades']==0 and partial['completed_positions']==[]
    add('close-b',True,1,99,.0495,4*10**9)
    full=paper_performance(db,include_trades=True)
    assert full['completed_trades']==1
    trade=full['completed_positions'][0]
    assert trade['gross_pnl']==0 and trade['net_pnl']==pytest.approx(-.202)
    assert trade['cost_usdt']==pytest.approx(.202) and trade['hold_seconds']==3
    db.set('reflection:active',{'version':5,'overrides':{'blocked_until':{'BTCUSDT':'2999-01-01'},
                                                       'paper_entry_filter':'enforce'}})
    ref=StrategyReflector(db,cfg)
    status=ref.sync_completed(full['completed_positions'])
    assert status['report']['window_trades']==1
    assert status['report']['avg_hold_seconds']==3
    assert 'blocked_until' not in status['active']['overrides']
    assert status['active']['overrides']['paper_entry_filter']=='enforce'
    ref.sync_completed(full['completed_positions'])
    assert db.get('reflection:trade_count')==1


def test_fixed_strategy_study_never_selects_on_test_and_never_enables_orders(monkeypatch):
    from quant import strategy_research as study
    seen=[]
    def fake_fit(rows):
        seen.append(max(r['label_end'] for r in rows))
        return {'schema':SCHEMA,'features':list(FEATURES),'classes':[-1,0,1],
                'mean':[0.]*len(FEATURES),'scale':[1.]*len(FEATURES),
                'coef':[[0.]*len(FEATURES)]*3,'intercept':[0.,0.,0.]}
    monkeypatch.setattr(study,'fit',fake_fit)
    rows=[quote(i*5000) for i in range(1200)]
    labels=[r|{'label':i%3-1,'label_end':r['ts']+300000} for i,r in enumerate(rows)]
    report=study.evaluate(rows,labels,policy_for_settings(Settings()),300000)
    assert not report['promotion_allowed'] and not report['execution_enabled']
    for observed,fold in zip(seen,report['folds']):
        assert observed<fold['train_end_ms']
        assert fold['selected_by_validation'] is None
        assert not fold['selected_passed']


def test_frozen_study_input_preserves_unlabeled_quotes_and_neutral_labels(tmp_path):
    import gzip
    from quant.strategy_research import load_input
    path=tmp_path/'input.gz'
    with gzip.open(path,'wt',encoding='utf-8') as f:
        f.write(json.dumps(quote(0)|{'label':0,'label_end':300000})+'\n')
        f.write(json.dumps(quote(5000))+'\n')
    rows,labels,manifest=load_input(path,0,10000)
    assert len(rows)==2 and len(labels)==1 and labels[0]['label']==0
    assert manifest['quotes']==2 and len(manifest['sha256'])==64
    with pytest.raises(ValueError,match='bounds'):load_input(path,1000,10000)


def test_history_statistics_bound_memory_and_do_not_block_status_readers(tmp_path, monkeypatch):
    import threading
    import tracemalloc
    from quant.event_labels import LABEL_SCHEMA
    db=LabStore(tmp_path)
    sample=quote(0)
    sample['meta']['padding']='x'*8192
    db.append([sample|{'ts':i} for i in range(3000)])
    db.append_labels([{'symbol':'ALTUSDT','ts':i,'valid':True,'label':0,
                      'label_end':i+300000,'schema':LABEL_SCHEMA} for i in range(3000)])
    with db.connect() as c:
        c.execute("UPDATE samples SET meta='malformed' WHERE ts=0")
        c.execute("UPDATE event_labels SET body='[]' WHERE ts=1")
    tracemalloc.start()
    result=db.stats()
    _,peak=tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert result['clean_training']['samples']==2998
    assert peak<4*1024*1024  # old fetchall retained >24MB before parsing
    entered,release=threading.Event(),threading.Event()
    original=LabStore.stats
    calls=[]
    def delayed(self,cache_seconds=0):
        if not cache_seconds:
            calls.append(1);entered.set()
            assert release.wait(5)
            return result
        return original(self,cache_seconds)
    monkeypatch.setattr(LabStore,'stats',delayed)
    try:
        assert db.stats(cache_seconds=60)['status']=='calculating'
        assert entered.wait(2)
        assert db.stats(cache_seconds=60)['stats_refreshing']
        assert len(calls)==1
    finally:
        release.set()
