"""Purged chronological radar training; safe JSON trees, no paid AI or orders."""
import argparse
import hashlib
import json
import math
import struct
import time
from pathlib import Path

from .potential_radar import SCHEMA,FEATURES,HORIZON_MS

MODEL_SCHEMA='potential-gradient-boosting-v1'


def sigmoid(value):
    return 1/(1+math.exp(-max(-500,min(500,value))))


def predict(model,vector):
    if model.get('features')!=list(FEATURES) or len(vector)!=len(FEATURES) or not all(math.isfinite(v) for v in vector):
        raise ValueError('Radar prediction schema/vector mismatch')
    # Match sklearn's float32 tree comparisons without importing numpy in the worker.
    vector=[struct.unpack('f',struct.pack('f',v))[0] for v in vector]
    if not all(math.isfinite(v) for v in vector):raise ValueError('Float32 feature overflow')
    raw=model['initial_logit']
    for tree in model['trees']:
        node=0
        while tree['left'][node]!=-1:
            node=tree['left'][node] if vector[tree['feature'][node]]<=tree['threshold'][node] else tree['right'][node]
        raw+=model['learning_rate']*tree['value'][node]
    calibration=model.get('calibration',{'slope':1.,'intercept':0.})
    return sigmoid(calibration['slope']*raw+calibration['intercept'])


def validate_model(model):
    if not 1<=len(model['trees'])<=100 or not 0<model['learning_rate']<=1:
        raise ValueError('Invalid tree ensemble')
    if not all(math.isfinite(model[k]) for k in ('initial_logit','learning_rate')):
        raise ValueError('Non-finite model parameters')
    if not all(math.isfinite(model['calibration'][k]) for k in ('slope','intercept')):
        raise ValueError('Non-finite calibration')
    for t in model['trees']:
        size=len(t['left'])
        if not 1<=size<=31 or any(len(t[k])!=size for k in ('right','feature','threshold','value')):
            raise ValueError('Malformed tree')
        for i in range(size):
            if not math.isfinite(t['threshold'][i]) or not math.isfinite(t['value'][i]):
                raise ValueError('Non-finite tree')
            if t['left'][i]==-1:
                if t['right'][i]!=-1:raise ValueError('Malformed leaf')
            elif (not all(type(t[k][i]) is int and i<t[k][i]<size for k in ('left','right'))
                    or type(t['feature'][i]) is not int or not 0<=t['feature'][i]<len(FEATURES)):
                raise ValueError('Malformed or cyclic tree')


def version(model):
    payload={k:v for k,v in model.items() if k not in ('version','approved','evaluation')}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,allow_nan=False).encode()).hexdigest()[:24]


def model_status(directory,now_ms=None):
    now_ms=int(time.time()*1000) if now_ms is None else now_ms
    try:
        model=json.loads((Path(directory)/'potential-model.json').read_text(encoding='utf-8'))
        if (model['schema']!=MODEL_SCHEMA or model['feature_schema']!=SCHEMA or model['version']!=version(model)
                or model['features']!=list(FEATURES) or not 0<=now_ms-model['trained_ms']<=7*86400000):
            raise ValueError('Model identity/schema/age mismatch')
        if not model['approved']:
            return {'ready':False,'reason':'排序模型未通过独立时间测试，继续显示规则分数',
                    'evaluation':model.get('evaluation'),'version':model['version']},None
        proof=model['evaluation']
        if (proof.get('version')!=model['version'] or not proof.get('passed') or
                not model['training_end_ms']<proof['validation_start_ms']<proof['test_start_ms']<proof['test_end_ms']
                or proof.get('selected',0)<30 or proof.get('precision',0)<.35 or proof.get('lift',0)<1.5
                or not proof.get('brier',1)<proof.get('constant_brier',0) or proof.get('test_used_for_tuning') is not False):
            raise ValueError('Model holdout evidence mismatch')
        validate_model(model)
        predict(model,[0.]*len(FEATURES))
        return {'ready':True,'reason':'已通过雷达历史排序与概率测试；不授权交易',
                'version':model['version'],'evaluation':proof},model
    except (OSError,ValueError,KeyError,TypeError,IndexError,OverflowError):
        return {'ready':False,'reason':'积累完整前向标签后再训练；当前分数不是上涨概率'},None


def chronological_split(rows):
    ordered=sorted(rows,key=lambda r:(r['ts'],r['symbol']))
    start,end=ordered[0]['ts'],ordered[-1]['ts']+1
    a,b=int(start+.6*(end-start)),int(start+.8*(end-start))
    train=[r for r in ordered if r['outcome']['label_end']<a]
    validation=[r for r in ordered if r['ts']>=a and r['outcome']['label_end']<b]
    test=[r for r in ordered if r['ts']>=b]
    return train,validation,test


def metrics(rows,probabilities,threshold,prior):
    labels=[r['outcome']['label'] for r in rows]
    selected=[(r,p) for r,p in zip(rows,probabilities) if p>=threshold]
    precision=sum(r['outcome']['label'] for r,p in selected)/len(selected) if selected else 0.
    baseline=sum(labels)/len(labels)
    return {'samples':len(rows),'positives':sum(labels),'selected':len(selected),'precision':precision,
        'base_rate':baseline,'lift':precision/baseline if baseline else 0.,
        'brier':sum((p-y)**2 for p,y in zip(probabilities,labels))/len(labels),
        'constant_brier':sum((prior-y)**2 for y in labels)/len(labels),
        'hypothetical_mean_net_return':sum(r['outcome']['net_return'] for r,p in selected)/len(selected) if selected else None}


def train(directory):
    from .potential_store import PotentialStore
    store=PotentialStore(directory);rows=store.training_rows()
    now=int(time.time()*1000)
    rows=[r for r in rows if r.get('schema')==SCHEMA and r['outcome']['label_end']<=now
          and len(r['vector'])==len(FEATURES) and all(math.isfinite(v) for v in r['vector'])]
    insufficient={'status':'insufficient_forward_labels','samples':len(rows),
        'note':'训练不会生成虚假概率或修改任何交易开关'}
    if len(rows)<1000 or rows[-1]['ts']-rows[0]['ts']<7*86400000:
        store.put('training',insufficient);return insufficient
    tr,va,te=chronological_split(rows)
    if any(len(group)<100 or min(sum(r['outcome']['label']==side for r in group) for side in (0,1))<10 for group in (tr,va,te)):
        store.put('training',insufficient);return insufficient
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    import numpy as np
    clf=GradientBoostingClassifier(n_estimators=60,max_depth=2,learning_rate=.05,min_samples_leaf=20,random_state=7)
    clf.fit([r['vector'] for r in tr],[r['outcome']['label'] for r in tr])
    prior=sum(r['outcome']['label'] for r in tr)/len(tr)
    trees=[]
    for estimator in clf.estimators_[:,0]:
        t=estimator.tree_
        trees.append({'left':t.children_left.tolist(),'right':t.children_right.tolist(),
                      'feature':t.feature.tolist(),'threshold':t.threshold.tolist(),'value':t.value[:,0,0].tolist()})
    model={'schema':MODEL_SCHEMA,'feature_schema':SCHEMA,'features':list(FEATURES),'trained_ms':now,
        'training_end_ms':max(r['outcome']['label_end'] for r in tr),'training_prior':prior,
        'initial_logit':math.log(prior/(1-prior)),'learning_rate':.05,'trees':trees}
    raw=clf.decision_function([r['vector'] for r in va]).reshape(-1,1)
    # Calibration and decision threshold are learned only on validation dates.
    calibrator=LogisticRegression(C=1,max_iter=300).fit(raw,[r['outcome']['label'] for r in va])
    model['calibration']={'slope':float(calibrator.coef_[0,0]),'intercept':float(calibrator.intercept_[0])}
    vp=[predict(model,r['vector']) for r in va]
    threshold=sorted(vp)[max(0,int(.9*len(vp))-1)]
    model['threshold']=threshold;model['version']=version(model)
    tp=[predict(model,r['vector']) for r in te]
    report=metrics(te,tp,threshold,prior)
    report|={'version':model['version'],'validation_start_ms':min(r['ts'] for r in va),
        'test_start_ms':min(r['ts'] for r in te),'test_end_ms':max(r['outcome']['label_end'] for r in te),
        'test_used_for_tuning':False,'horizon_ms':HORIZON_MS,'sample_scope':'enriched leaders and rotating controls',
        'criteria':'held-out selected>=30, precision>=0.35, lift>=1.5, Brier below training-prior baseline; no trading approval'}
    report['passed']=bool(report['selected']>=30 and report['precision']>=.35 and report['lift']>=1.5
                          and report['brier']<report['constant_brier'])
    model['approved']=report['passed'];model['evaluation']=report
    path=Path(directory)/'potential-model.json';temp=path.with_suffix('.pending')
    temp.write_text(json.dumps(model,allow_nan=False),encoding='utf-8');temp.replace(path)
    result={'status':'approved_for_radar' if report['passed'] else 'rejected','at_ms':now,'evaluation':report}
    store.put('training',result);return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--directory',required=True)
    args=parser.parse_args();print(json.dumps(train(args.directory),ensure_ascii=False))
