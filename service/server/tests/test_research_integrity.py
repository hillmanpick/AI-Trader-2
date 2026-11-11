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
    strict=p|{'paper_exploration_mode':False,'paper_margin_is_ceiling':True}
    bounded=microstructure_plan(row,row['meta'],strict,{},50)
    assert bounded['margin']==25 and bounded['notional']==50
    assert bounded['planned_risk_usdt']<=25*cfg.paper_risk_fraction
    allowed=replay([quote(i*5000) for i in range(12)],[1]*12,policy=strict,horizon_ms=15000)
    assert allowed['trades']>0 and allowed['net_pnl']<0
    # Smaller sizing never means a rejected model can skip the approval gate.
    from quant.execution_policy import fingerprint
    assert fingerprint(strict)!=fingerprint(p)


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


def test_exit_observations_fill_feature_gaps_without_entering_training(tmp_path):
    db=LabStore(tmp_path)
    db.append([quote(0,1000000),quote(5000,1000000)])
    db.append_quotes([quote(t,1000000) for t in range(0,65000,5000)])
    rows=list(db.market_rows(0,65000,stride_ms=5000))
    assert len(rows)==13 and rows[0]['vector']
    assert all(not r['vector'] and not r['meta']['entry_allowed'] for r in rows[2:])
    assert db.labeled_rows()==[]
    observed=[]
    result=replay(rows,(),signal_fn=lambda r:observed.append(r['ts']) or 1,horizon_ms=30000)
    assert observed==[0,5000] and result['valid'] and result['trades']==1
    assert result['fills'][0]['reason']=='timeout'
    from quant.strategy_research import load_snapshot
    exported,labels,_=load_snapshot(db.path,0,65000,stride_ms=5000)
    assert exported==rows and not labels


def test_exit_only_quote_cancels_pending_entry_and_never_creates_one():
    rows=[quote(0),quote(5000),quote(10000)]
    for r in rows[1:]:r['meta']['entry_allowed']=False
    result=replay(rows,[1,1,1])
    assert result['trades']==0
    assert result['rejection_reasons']=={'entry_observation_only':1}


def test_feature_warmup_quote_can_fill_prior_signal_only_while_still_a_candidate():
    signal=quote(0,1000000)
    observations=[quote(t,1000000) for t in range(5000,65000,5000)]
    for row in observations:
        row['vector']=[]
        row['meta'].update(entry_allowed=False,candidate=True)
        row['meta'].pop('atr_proxy')
    observed=[]
    result=replay([signal]+observations,(),horizon_ms=30000,
        signal_fn=lambda row:observed.append(row['ts']) or 1)
    assert observed==[0] and result['trades']==1 and result['valid']
    assert result['fills'][0]['reason']=='timeout'


def test_readiness_distinguishes_administrative_pause_data_and_failed_strategy(tmp_path):
    from dataclasses import replace
    from quant.lab_config import LabConfig
    from quant.opening_readiness import paper_readiness
    paper=Settings(enabled=True,strategy='lab_model',paper_lab_required=True)
    lab=LabConfig(enabled=True,directory=str(tmp_path))
    model={'ready':False,'reason':'样本外检验未通过','version':'test'}
    report={'status':'rejected','folds':[{'test':{'valid':False,'net_pnl':-15.}}],
        'execution_validation':{'reason':'sampled_research_gate_failed'}}
    live={'ts':100000,'ready_symbols':['BTCUSDT']}
    result=paper_readiness(paper,lab,model,report,live,True,False,100001)
    assert result['armed'] and result['automatic_after_validation'] and not result['entry_ready']
    assert [b['code'] for b in result['blockers']]==['model_unvalidated']
    assert result['sampled_test_pnl']==[-15.] and result['sampled_tests_with_gaps']==1
    assert paper_readiness(paper,lab,{'ready':True},{},live,True,False,100001)['entry_ready']
    stopped=paper_readiness(replace(paper,enabled=False),lab,model,report,{},False,True,100001)
    codes={b['code'] for b in stopped['blockers']}
    assert {'paper_disabled','operator_paused','collector_stale','redis_unavailable'}<=codes


def test_forward_trial_readiness_preserves_failed_validation_as_warning(tmp_path):
    from dataclasses import replace
    from quant.lab_config import LabConfig
    from quant.opening_readiness import paper_readiness
    paper=Settings(enabled=True,strategy='lab_model',paper_lab_required=True,paper_exploration_mode=True)
    lab=LabConfig(enabled=True,directory=str(tmp_path),exploration_mode=True)
    model={'ready':True,'approved':False,'exploration':True,'version':'trial'}
    state={'ts':100000,'ready_symbols':['BTCUSDT']}
    result=paper_readiness(paper,lab,model,{'status':'rejected'},state,True,False,100001)
    assert result['entry_ready'] and result['mode']=='forward_trial'
    assert not result['validated_entry_ready'] and not result['automatic_after_validation']
    assert result['validation_warnings'][0]['code']=='model_unvalidated'
    assert not paper_readiness(paper,lab,model,{},state,True,True,100001)['entry_ready']
    assert not paper_readiness(paper,lab,model,{},state,False,False,100001)['entry_ready']
    blocked=paper_readiness(replace(paper,paper_exploration_mode=False),lab,model,{},state,True,False,100001)
    assert not blocked['entry_ready'] and blocked['blockers'][0]['code']=='trial_not_authorized'


def test_bounded_training_score_cache_matches_uncached_decisions_for_all_thresholds():
    from quant.lab_train import SampledModelSignals
    from quant.lab_model import decide
    model={'schema':SCHEMA,'features':list(FEATURES),'classes':[-1,0,1],
        'mean':[0.]*len(FEATURES),'scale':[1.]*len(FEATURES),
        'coef':[[.1]*len(FEATURES),[0.]*len(FEATURES),[-.1]*len(FEATURES)],
        'intercept':[.4,0.,-.2],'version':'test'}
    scorer=SampledModelSignals(model,max_rows=10)
    rows=[quote(i*5000) for i in range(10)]
    for i,r in enumerate(rows):r['vector'][0]=i-5
    for threshold in (.45,.5,.55,.6,.65,.7):
        for r in rows:
            action=decide(model|{'threshold':threshold},r)['action']
            assert scorer.direction(r,threshold)=={'long':1,'short':-1,'hold':0}[action]
    assert scorer.inferences==10 and scorer.hits==50
    scorer.direction(quote(100000),.5)
    assert len(scorer.cache)==10
    other=SampledModelSignals(model|{'intercept':[-10,-10,10]})
    assert other.direction(rows[0],.7)==1


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
