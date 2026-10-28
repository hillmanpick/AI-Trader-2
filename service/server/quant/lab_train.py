"""Purged walk-forward training. No random split, exchange keys or orders."""
import argparse
import json
import os
import time
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

from .lab_config import LabConfig
from .lab_features import FEATURES, SCHEMA
from .lab_model import probabilities
from .lab_replay import replay
from .lab_store import LabStore
from .event_labels import LABEL_SCHEMA
from .execution_policy import current_policy, model_version
from .event_validation import validate_candidate
from .event_lake import EventLake


def label_rows(rows, horizon_ms):
    groups = defaultdict(list)
    for row in rows:
        if row['meta'].get('quality') and row['meta'].get('schema') == SCHEMA:
            groups[row['symbol']].append(row)
    result = []
    for group in groups.values():
        for i,row in enumerate(group):
            end = row['ts']+horizon_ms
            if group[-1]['ts'] < end:
                continue
            distance = max(.0026, row['meta'].get('atr_proxy',.002)*2)
            label, complete, previous = 0,False,row['ts']
            for j in range(i+1,len(group)):
                later = group[j]
                if later['ts']-previous > 45000:
                    break
                if later['ts']>end:
                    # A future sample beyond expiry cannot retroactively hit
                    # a price barrier. Use the last known price only within a
                    # bounded observation tolerance; never require an exact
                    # millisecond match that would remove every neutral label.
                    complete = end-previous <= 45000
                    break
                previous = later['ts']
                move = later['price']/row['price']-1
                if abs(move) >= distance:
                    label,complete = (1 if move>0 else -1),True
                    break
                if later['ts'] >= end:
                    complete = True
                    break
            if complete:
                # Purge full horizon even when a barrier was hit early.
                result.append(row | {'label':label,'label_end':end})
    return sorted(result,key=lambda r:(r['ts'],r['symbol']))


def partition(rows, train_end, validation_end, test_end):
    train = [r for r in rows if r['label_end'] < train_end]
    validation = [r for r in rows if r['ts'] >= train_end and r['label_end'] < validation_end]
    test = [r for r in rows if validation_end <= r['ts'] and r['label_end'] < test_end]
    return train,validation,test


def fit(rows):
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    import numpy as np
    x = np.array([r['vector'] for r in rows])
    y = np.array([r['label'] for r in rows])
    if len(set(y)) < 3:
        raise ValueError('Training needs long, short and neutral samples')
    scaler = StandardScaler().fit(x)
    clf = LogisticRegression(C=.1,max_iter=500).fit(scaler.transform(x),y)
    from sklearn.cluster import MiniBatchKMeans
    standardized = scaler.transform(x)
    cluster_count = min(16,len(np.unique(standardized,axis=0)))
    vector_model = MiniBatchKMeans(n_clusters=cluster_count,batch_size=1024,n_init=3,random_state=7).fit(standardized)
    cluster_labels = vector_model.predict(standardized)
    counts = np.ones((cluster_count,len(clf.classes_)))
    class_columns = {label:i for i,label in enumerate(clf.classes_)}
    for cluster,label in zip(cluster_labels,y):
        counts[cluster,class_columns[label]] += 1
    counts /= counts.sum(axis=1,keepdims=True)
    return {'schema':SCHEMA,'features':list(FEATURES),'classes':[int(c) for c in clf.classes_],
            'mean':scaler.mean_.tolist(),'scale':scaler.scale_.tolist(),
            'coef':clf.coef_.tolist(),'intercept':clf.intercept_.tolist(),
            'vector_model':{'kind':'standardized-prototype-v1','blend':.15,
                'centers':vector_model.cluster_centers_.tolist(),'class_probabilities':counts.tolist()}}


def predictions(model,rows,threshold):
    result = []
    for row in rows:
        p = probabilities(model,row['vector'])
        side = max(p,key=p.get)
        result.append(side if p[side] >= threshold else 0)
    return result


def compact(report):
    return {k:v for k,v in report.items() if k != 'fills'}


def passes(report):
    return (report['trades'] >= 30 and report['net_pnl'] > 0
            and (report['profit_factor'] or 0) >= 1.1 and report['max_drawdown'] <= 3000)


def train(cfg, gradient=False):
    store = LabStore(cfg.directory)
    stats = store.stats()
    if stats['samples'] < cfg.min_samples or stats['days'] < cfg.min_days:
        report = {'status':'insufficient_data','samples':stats['samples'],'days':stats['days'],
                  'required_samples':cfg.min_samples,'required_days':cfg.min_days,'at_ms':int(time.time()*1000)}
        store.report(report)
        return report
    # Sampled-price labels remain a diagnostic helper, never training truth.
    # Online labels survive raw-file retention and include full-horizon ID checks.
    rows = store.labeled_rows()
    coverage_days = len({r['ts']//30000 for r in rows})*30/86400
    if (len(rows) < cfg.min_samples or (rows[-1]['ts']-rows[0]['ts'])/86400000 < cfg.min_days
            or coverage_days < cfg.min_days*.75):
        report = {'status':'insufficient_clean_labels','samples':len(rows),'coverage_days':coverage_days,
                  'at_ms':int(time.time()*1000)}
        store.report(report)
        return report
    start,end = rows[0]['ts'],rows[-1]['label_end']+1
    test_start = end-cfg.event_validation_seconds*1000
    validation_start = test_start-cfg.event_validation_seconds*1000
    research = [r for r in rows if r['label_end'] < validation_start]
    if len(research)<1000 or validation_start-start < cfg.horizon_seconds*10000:
        report = {'status':'insufficient_event_holdout','at_ms':int(time.time()*1000),
                  'samples':len(rows),'required_window_seconds':cfg.event_validation_seconds}
        store.report(report)
        return report
    # All exploratory folds end before the final untouched event holdouts.
    end_research = validation_start
    span = end_research-start
    folds, final_model = [],None
    for train_fraction,val_fraction,test_fraction in ((.4,.55,.7),(.55,.7,.85),(.7,.85,1.)):
        tr,va,te = partition(research,start+span*train_fraction,start+span*val_fraction,start+span*test_fraction)
        if min(len(tr),len(va),len(te)) < 100:
            raise ValueError('Not enough independent chronological fold samples')
        model = fit(tr)
        # Select threshold using validation only; test never chooses parameters.
        options = []
        for threshold in (.45,.5,.55,.6,.65,.7):
            r = replay(va,predictions(model,va,threshold),horizon_ms=cfg.horizon_seconds*1000)
            options.append((r['net_pnl'] if r['trades'] >= 30 else -1e99,threshold,r))
        _,threshold,validation = max(options,key=lambda x:x[0])
        test = replay(te,predictions(model,te,threshold),horizon_ms=cfg.horizon_seconds*1000)
        stress = replay(te,predictions(model,te,threshold),slippage=.0006,latency_ms=1500,horizon_ms=cfg.horizon_seconds*1000)
        folds.append({'train_samples':len(tr),'validation_samples':len(va),'test_samples':len(te),
                      'train_end_ms':max(r['label_end'] for r in tr),'test_start_ms':te[0]['ts'],
                      'threshold':threshold,'validation':compact(validation),'test':compact(test),'stress':compact(stress)})
        final_model = model | {'threshold':threshold,'test':compact(test)}
    research_passed = all(passes(f['test']) and f['stress']['net_pnl'] > 0 and f['validation']['net_pnl'] > 0 for f in folds)
    # Fit a single final model before both event holdouts, with its own exact
    # version. A fold model's result can never approve newly refitted weights.
    final_model = fit(research) | {'threshold':.6,'label_schema':LABEL_SCHEMA,
        'training_end_ms':max(r['label_end'] for r in research)}
    final_model['version'] = model_version(final_model)
    proof = {'passed':False,'model_version':final_model['version'],
             'reason':'sampled_research_gate_failed'}
    if research_passed:
        try:
            final_model,proof = validate_candidate(EventLake(cfg.directory),final_model,
                current_policy(cfg.horizon_seconds),validation_start,test_start,end,
                {r['symbol'] for r in rows if r['ts']>=validation_start})
        except (ValueError,OSError) as exc:
            proof = {'passed':False,'model_version':final_model['version'],
                     'reason':'event_validation_unavailable','error':type(exc).__name__}
    approved = research_passed and proof['passed']
    report = {'status':'approved' if approved else 'rejected','schema':SCHEMA,'at_ms':int(time.time()*1000),
              'folds':folds,'samples':len(rows),'coverage_days':coverage_days,'method':'expanding purged walk-forward; full-horizon embargo; validation-only threshold',
              'criteria':'each test >=30 trades, net>0, PF>=1.1, DD<=3000; stress net>0',
              'gradient':{'enabled':False},'training_stride_seconds':30,
              'label_schema':LABEL_SCHEMA,'execution_validation':proof,
              'replay_limits':'sampled folds are preliminary; final model requires separate individual-event holdouts'}
    if gradient:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.metrics import balanced_accuracy_score
        g = HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,l2_regularization=10,random_state=7)
        g.fit([r['vector'] for r in tr],[r['label'] for r in tr])
        report['gradient'] = {'enabled':True,'validation_balanced_accuracy':float(balanced_accuracy_score(
            [r['label'] for r in va],g.predict([r['vector'] for r in va]))),
            'promotion':'research only; must repeat net-of-cost OOS replay before execution'}
    final_model |= {'approved':approved,'trained_ms':report['at_ms'],'execution_validation':proof,
                    'test':proof.get('test',{})}
    destination = Path(cfg.directory)/'model.json'
    temp = destination.with_suffix('.tmp')
    temp.write_text(json.dumps(final_model,allow_nan=False),encoding='utf-8')
    os.replace(temp,destination)
    report['model_version'] = final_model['version']
    store.report(report)
    (Path(cfg.directory)/'latest-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    return report


def main():
    import config  # noqa: F401
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gradient',action='store_true')
    parser.add_argument('--watch',action='store_true')
    args = parser.parse_args()
    cfg = LabConfig().validate()
    while True:
        try:
            if not args.watch or cfg.auto_train:
                print(json.dumps(train(cfg,args.gradient),ensure_ascii=False),flush=True)
        except Exception as exc:
            print(json.dumps({'status':'training_error','error':type(exc).__name__}),flush=True)
        if not args.watch:
            break
        time.sleep(21600)


if __name__ == '__main__':
    main()
