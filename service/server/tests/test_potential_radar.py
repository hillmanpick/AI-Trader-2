import json
import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.potential_radar import BAR_MS,HORIZON_MS,FEATURES,closed_bars,candidate,evaluate_path
from quant.potential_store import PotentialStore
from quant.potential_train import chronological_split,predict,model_status,version,train
from quant.potential_worker import PublicMarket,Radar

NOW=1800000000000


def raw_bars(count=300,end=NOW,move=.0002,quote=100000):
    rows=[];price=100.
    for i in range(count):
        t=end-(count-i)*BAR_MS;close=price*(1+move)
        rows.append([t,price,max(price,close)*1.0001,min(price,close)*.9999,close,0,t+BAR_MS-1,
                     quote,0,0,quote*.65,0]);price=close
    return rows


def flow(symbol='ALTUSDT',ts=NOW):
    return {'symbol':symbol,'ts':ts,'price':110.,'vector':[.001,.002,.0001,.5,10.,3.,math.log1p(1e8),.1],
            'ready':True,'schema':'market-flow-v1','direction_score':.7}


def test_score_uses_closed_history_and_missing_context_is_not_neutral_confirmation():
    raw=raw_bars();bars=closed_bars(raw,NOW)
    a=candidate(flow(),bars,bars,NOW)
    assert a['schema']=='potential-radar-v1' and not a['execution_enabled'] and a['prediction'] is None
    assert not a['features']['oi_available'] and not a['features']['spot_available']
    assert a['components']['open_interest']==0 and a['components']['spot_confirmation']==0
    assert a['score_kind']=='rule_score_not_probability'
    assert a['short_score']>=0 and a['timing']['return_15m']==pytest.approx(a['features']['return_15m'])
    unfinished=[NOW,1,1e9,1,1e9,0,NOW+BAR_MS-1,1e15,0,0,1e15,0]
    assert closed_bars(raw+[unfinished],NOW)==bars
    with pytest.raises(ValueError):closed_bars(raw[:100]+raw[101:],NOW)
    with pytest.raises(ValueError):candidate(flow(ts=NOW-21000),bars,bars,NOW)
    with pytest.raises(ValueError):candidate(flow(),bars,bars[:-1],NOW)


def test_oi_is_quantity_change_and_funding_crowding_penalizes_score():
    bars=closed_bars(raw_bars(),NOW)
    oi=[{'timestamp':NOW-(12-i)*BAR_MS,'sumOpenInterest':100+i} for i in range(13)]
    normal=candidate(flow(),bars,bars,NOW,oi=oi,funding={'time':NOW,'lastFundingRate':.0001},spot=bars)
    crowded=candidate(flow(),bars,bars,NOW,oi=oi,funding={'time':NOW,'lastFundingRate':.002},spot=bars)
    assert normal['features']['oi_change_1h']==pytest.approx(.12)
    assert normal['score']>crowded['score'] and crowded['features']['funding_available']
    old=candidate(flow(),bars,bars,NOW,oi=oi[:-1],funding={'time':NOW-180000,'lastFundingRate':.002})
    assert not old['features']['oi_available'] and not old['features']['funding_available']


def test_future_barriers_require_complete_horizon_and_reject_intrabar_ambiguity():
    raw=raw_bars(48,move=0);start=raw[0][0]
    with pytest.raises(ValueError,match='incomplete'):evaluate_path(raw,start,NOW-1)
    result=evaluate_path(raw,start,NOW)
    assert result['valid'] and result['outcome']=='timeout' and result['net_return']<0
    assert len(result['path'])==48
    up=[list(r) for r in raw];up[1][2]=112;up[1][4]=111
    assert evaluate_path(up,start,NOW)['outcome']=='up_first'
    both=[list(r) for r in raw];both[1][2]=112;both[1][3]=90
    assert not evaluate_path(both,start,NOW)['valid']
    with pytest.raises(ValueError):evaluate_path(raw[:-1],start,NOW)
    down=[list(r) for r in raw];down[1][3]=94;down[1][4]=94
    assert evaluate_path(down,start,NOW)['outcome']=='down_first'


def test_observations_and_outcomes_are_immutable_and_never_make_account_orders(tmp_path):
    db=PotentialStore(tmp_path);bars=closed_bars(raw_bars(),NOW)
    first=candidate(flow(),bars,bars,NOW);db.observe([first],NOW)
    db.observe([first|{'score':99}],NOW)
    assert db.summary(NOW)['observations']==1 and not db.due(NOW)
    due=db.due(NOW+HORIZON_MS+BAR_MS)[0]
    result={'valid':True,'label':1,'label_end':due['end_ms'],'net_return':.08,'path':[[1,2,3]]}
    db.finish(due,result);db.finish(due,result|{'label':0})
    assert db.summary(NOW)['up_first']==1 and db.training_rows()[0]['score']==first['score']
    with db.connect() as c:
        assert not c.execute("SELECT name FROM sqlite_master WHERE name='execution_orders'").fetchone()
    assert train(tmp_path)['status']=='insufficient_forward_labels'
    assert not (tmp_path/'potential-model.json').exists()


def test_chronological_split_purges_overlapping_labels_across_coins():
    rows=[{'symbol':s,'ts':NOW+i*3600000,'outcome':{'label_end':NOW+i*3600000+HORIZON_MS}}
          for i in range(300) for s in ('AAAUSDT','BBBUSDT')]
    tr,va,te=chronological_split(rows)
    assert max(r['outcome']['label_end'] for r in tr)<min(r['ts'] for r in va)
    assert max(r['outcome']['label_end'] for r in va)<min(r['ts'] for r in te)
    assert {r['ts'] for r in tr}.isdisjoint(r['ts'] for r in va+te)


def test_bad_or_cyclic_model_cannot_publish_probability(tmp_path):
    m={'schema':'potential-gradient-boosting-v1','feature_schema':'potential-radar-v1',
        'features':list(FEATURES),'trained_ms':NOW,'training_end_ms':1,'approved':True,
        'initial_logit':0.,'learning_rate':.05,'calibration':{'slope':1.,'intercept':0.},
        'trees':[{'left':[0],'right':[0],'feature':[0],'threshold':[0.],'value':[0.]}]}
    m['version']=version(m)
    m['evaluation']={'passed':True,'version':m['version'],'validation_start_ms':2,'test_start_ms':3,'test_end_ms':4,
        'selected':40,'precision':.5,'lift':2,'brier':.1,'constant_brier':.2,'test_used_for_tuning':False}
    (tmp_path/'potential-model.json').write_text(json.dumps(m))
    assert not model_status(tmp_path,NOW)[0]['ready']


def test_safe_json_tree_prediction_matches_sklearn_without_validation_leakage():
    from sklearn.ensemble import GradientBoostingClassifier
    import numpy as np
    x=np.array([[math.sin(i+j) for j in range(len(FEATURES))] for i in range(200)])
    y=np.array([int(a[0]+a[1]>.2) for a in x])
    clf=GradientBoostingClassifier(n_estimators=5,max_depth=2,learning_rate=.05,random_state=7).fit(x,y)
    trees=[]
    for t in (e.tree_ for e in clf.estimators_[:,0]):
        trees.append({'left':t.children_left.tolist(),'right':t.children_right.tolist(),
            'feature':t.feature.tolist(),'threshold':t.threshold.tolist(),'value':t.value[:,0,0].tolist()})
    p=sum(y)/len(y)
    model={'features':list(FEATURES),'initial_logit':math.log(p/(1-p)),'learning_rate':.05,'trees':trees}
    assert [predict(model,list(r)) for r in x]==pytest.approx(clf.predict_proba(x)[:,1],abs=1e-12)


def test_worker_reuses_fresh_redis_flow_and_only_calls_public_gets(tmp_path,monkeypatch):
    import asyncio
    monkeypatch.setattr('quant.potential_worker.time.time',lambda:NOW/1000)
    class Client:
        maximum=100;remaining=100;requests=0
        async def get(self,market,path,params=None):
            self.requests+=1
            assert path in PublicMarket.PATHS[market]
            if path.endswith('premiumIndex'):return []
            if path.endswith('openInterestHist'):return []
            return raw_bars()
    class Bus:
        def put(self,key,value,ttl=None):self.last=(key,value,ttl)
        def get(self,key):
            return {'ts':NOW,'rows':[flow()]} if key=='ranking' else None
    db=PotentialStore(tmp_path);radar=Radar(Client(),Bus(),db)
    asyncio.run(radar.tick())
    status=db.summary(NOW)
    assert status['status']=='running' and status['enriched_candidates']==1
    assert status['observations']==1 and not status['execution_enabled']
    assert status['candidates'][0]['prediction'] is None
    assert radar.bus.last[0]=='potential_candidates'


def test_request_budget_includes_rotating_controls_and_leaders(tmp_path,monkeypatch):
    import asyncio
    monkeypatch.setattr('quant.potential_worker.time.time',lambda:NOW/1000)
    class Client:
        maximum=12;remaining=12;requests=0
        async def get(self,market,path,params=None):
            assert self.remaining>0, 'Radar overspent its public request budget'
            self.remaining-=1;self.requests+=1
            if path.endswith('premiumIndex'):return []
            if path.endswith('openInterestHist'):return []
            return raw_bars()
    class Bus:
        def put(self,key,value,ttl=None):self.last=(key,value,ttl)
        def get(self,key):
            return {'ts':NOW,'rows':[flow(f'Z{i:02}USDT') for i in range(20)]+[flow('AAAUSDT')]} if key=='ranking' else None
    db=PotentialStore(tmp_path);client=Client();radar=Radar(client,Bus(),db)
    asyncio.run(radar.tick())
    assert client.requests<=12
    with db.connect() as c:
        seen={r[0] for r in c.execute('SELECT symbol FROM radar_observations')}
    assert {'AAAUSDT','Z00USDT','Z01USDT'}<=seen
    before=client.requests
    asyncio.run(radar.tick())
    assert client.requests-before<=12
    with db.connect() as c:
        seen={r[0] for r in c.execute('SELECT symbol FROM radar_observations')}
    assert {'Z02USDT','Z03USDT'}<=seen
    assert radar.bus.last[0]=='potential_candidates'


def test_missing_history_retry_does_not_block_other_due_observations(tmp_path):
    db=PotentialStore(tmp_path);bars=closed_bars(raw_bars(),NOW)
    for symbol in ('AAAUSDT','BBBUSDT'):
        db.observe([candidate(flow(symbol),bars,bars,NOW)],NOW)
    now=NOW+HORIZON_MS+BAR_MS
    first=db.due(now,limit=1)[0]
    db.defer(first,now+900000)
    assert db.due(now,limit=1)[0]['symbol']!=first['symbol']
    assert len(db.due(now+900000))==2
