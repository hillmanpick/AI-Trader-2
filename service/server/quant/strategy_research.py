"""Read-only, bounded strategy comparison. Never writes a model or paper order.

All settings and candidates are frozen before evaluation. Selection uses only
validation, never test. Sampled results cannot enable trading; individual-event
holdouts and a frozen prospective paper trial remain mandatory.
"""
import argparse
import hashlib
import gzip
import json
import sqlite3
from pathlib import Path

from .execution_policy import current_policy, fingerprint
from .lab_features import FEATURES,SCHEMA
from .event_labels import LABEL_SCHEMA
from .lab_model import decide
from .lab_train import fit
from .lab_replay import replay

CANDIDATES = ('numeric_fixed_060','cost_filtered_momentum','flow_confirmed_reversal')


def rule_signal(name,row,policy):
    f=dict(zip(FEATURES,row['vector']))
    cost=2*(policy['fee_rate']+policy['paper_slippage'])+row['meta']['spread_bps']/10000
    move=f['return_60s']
    if not row['meta'].get('quality') or abs(move)<1.5*cost:
        return 0
    side=1 if move>0 else -1
    flow=(f['ofi']+f['trade_imbalance'])/2
    if name=='cost_filtered_momentum':
        return side if side*f['return_30s']>0 and side*flow>.15 else 0
    if name=='flow_confirmed_reversal':
        return -side if side*flow<-.25 and side*f['return_5s']<0 else 0
    raise ValueError('Unknown fixed strategy')


def eligible(report):
    pf=report.get('profit_factor')
    return bool(report.get('valid') and report['trades']>=30 and report['net_pnl']>0
                and (pf is None or pf>=1.1) and report['max_drawdown']<=3000)


def evaluate(rows,training,policy,horizon_ms):
    if len(rows)<1000 or len(training)<1000:
        raise ValueError('Insufficient chronological research samples')
    start,end=rows[0]['ts'],rows[-1]['ts']+1
    span=end-start
    folds=[]
    for a,b,c in ((.4,.55,.7),(.55,.7,.85),(.7,.85,1.)):
        train_end,val_end,test_end=(int(start+span*x) for x in (a,b,c))
        tr=[r for r in training if r['label_end']<train_end]
        va=[r for r in rows if train_end<=r['ts']<val_end]
        te=[r for r in rows if val_end<=r['ts']<test_end]
        model=fit(tr)|{'threshold':.6,'version':'research-only'}
        def signal(name):
            if name=='numeric_fixed_060':
                def numeric(row):
                    action=decide(model,row)['action']
                    return 1 if action=='long' else -1 if action=='short' else 0
                return numeric
            return lambda row:rule_signal(name,row,policy)
        def run(name,quotes,stress=False):
            p=dict(policy)
            if stress:p['paper_slippage']*=2
            result=replay(quotes,(),policy=p,signal_fn=signal(name),horizon_ms=horizon_ms,
                          latency_ms=1500 if stress else 500)
            return {k:v for k,v in result.items() if k not in ('fills','policy','limitations')}
        validation={name:run(name,va) for name in CANDIDATES}
        available=[name for name in CANDIDATES if eligible(validation[name])]
        selected=max(available,key=lambda name:validation[name]['net_pnl']) if available else None
        # Candidate definitions are immutable. Test outcomes only accept/reject
        # the validation-selected candidate, never change which one is selected.
        test={name:run(name,te) for name in CANDIDATES}
        stress={name:run(name,te,True) for name in CANDIDATES}
        folds.append({'train_samples':len(tr),'train_end_ms':train_end,
            'validation_end_ms':val_end,'test_end_ms':test_end,
            'validation':validation,'selected_by_validation':selected,
            'test':test,'stress':stress,
            'selected_passed':bool(selected and eligible(test[selected]) and eligible(stress[selected]))})
    return {'schema':'fixed-strategy-research-v1','status':'research_only',
        'candidates':list(CANDIDATES),'folds':folds,'policy':policy,
        'criteria':'validation selection only; >=30 trades, net>0, PF>=1.1, DD<=3000, continuous samples',
        'all_selected_folds_passed':all(f['selected_passed'] for f in folds),
        'promotion_allowed':False,'execution_enabled':False,
        'limitations':['Sampled quotes are preliminary, not tick replay or live profitability evidence',
            'No parameter search, model overwrite or simulated-account writes',
            'Historical quantity filters and uninterrupted event holdouts remain required',
            'Fixed candidate comparison is research; fresh prospective evidence is still required']}


def load_snapshot(database,start_ms,end_ms,stride_ms=30000,max_rows=180000):
    connection=sqlite3.connect('file:'+str(database)+'?mode=ro',uri=True)
    rows,training,last,filter_cache=[],[],{},{}
    digest=hashlib.sha256()
    try:
        connection.execute('PRAGMA query_only=ON')
        for symbol,ts,price,vector,meta,label,valid,label_end,body in connection.execute(
            'SELECT s.symbol,s.ts,s.price,s.vector,s.meta,l.label,l.valid,l.label_end,l.body '
            'FROM samples s LEFT JOIN event_labels l ON s.symbol=l.symbol AND s.ts=l.ts '
            'WHERE s.ts>=? AND s.ts<? ORDER BY s.ts,s.symbol',(start_ms,end_ms)):
            if ts//stride_ms==last.get(symbol):continue
            last[symbol]=ts//stride_ms
            meta=json.loads(meta)
            if not meta.get('quality') or meta.get('schema')!=SCHEMA:continue
            # Preserve causal exchange rules when available; no future-label
            # predicate may remove quotes from the execution path.
            meta={k:meta[k] for k in ('quality','schema','bid','ask','bid_depth','ask_depth',
                'spread_bps','atr_proxy','funding_rate','filters') if k in meta}
            if 'filters' in meta:
                key=(symbol,json.dumps(meta['filters'],sort_keys=True))
                meta['filters']=filter_cache.setdefault(key,meta['filters'])
            row={'symbol':symbol,'ts':ts,'price':price,'vector':json.loads(vector),'meta':meta}
            digest.update(json.dumps(row,sort_keys=True,separators=(',',':')).encode())
            rows.append(row)
            if valid and body and json.loads(body).get('schema')==LABEL_SCHEMA:
                training.append(row|{'label':label,'label_end':label_end})
                digest.update(json.dumps({'symbol':symbol,'ts':ts,'label':label,'label_end':label_end},
                                         sort_keys=True).encode())
            if len(rows)>max_rows:
                raise ValueError('Research row bound exceeded; explicitly choose a shorter window')
    finally:connection.close()
    return rows,training,{'sha256':digest.hexdigest(),'quotes':len(rows),'labels':len(training),
        'requested_start_ms':start_ms,'requested_end_ms':end_ms,'stride_ms':stride_ms}


def load_input(path,start_ms,end_ms,max_rows=350000):
    """Use a frozen gzip export for local computation on small-server projects."""
    rows,training,filters=[],[],{}
    digest=hashlib.sha256()
    previous=None
    with gzip.open(path,'rb') as source:
        for raw in source:
            digest.update(raw)
            row=json.loads(raw)
            if not start_ms<=row['ts']<end_ms:
                raise ValueError('Frozen input outside declared chronological bounds')
            order=(row['ts'],row['symbol'])
            if previous and order<=previous:
                raise ValueError('Frozen input not chronological or has duplicate quotes')
            previous=order
            label,end=row.pop('label',None),row.pop('label_end',None)
            if 'filters' in row['meta']:
                key=(row['symbol'],json.dumps(row['meta']['filters'],sort_keys=True))
                row['meta']['filters']=filters.setdefault(key,row['meta']['filters'])
            rows.append(row)
            if label is not None:training.append(row|{'label':label,'label_end':end})
            if len(rows)>max_rows:raise ValueError('Frozen research input exceeds explicit row bound')
    return rows,training,{'sha256':digest.hexdigest(),'quotes':len(rows),'labels':len(training),
        'requested_start_ms':start_ms,'requested_end_ms':end_ms,'source':'frozen_gzip_export'}


def main():
    import config  # noqa: F401
    parser=argparse.ArgumentParser(description=__doc__)
    source=parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--database',type=Path)
    source.add_argument('--input',type=Path)
    parser.add_argument('--policy-json',type=Path)
    parser.add_argument('--start-ms',type=int,required=True)
    parser.add_argument('--end-ms',type=int,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--stride-ms',type=int,default=30000)
    args=parser.parse_args()
    if not 1000<=args.stride_ms<=30000 or args.end_ms<=args.start_ms:
        parser.error('Explicit chronological bounds and stride <=30 seconds are required')
    if args.input:
        rows,labels,manifest=load_input(args.input,args.start_ms,args.end_ms)
    else:
        rows,labels,manifest=load_snapshot(args.database,args.start_ms,args.end_ms,args.stride_ms)
    print(json.dumps({'status':'snapshot_loaded','manifest':manifest}),flush=True)
    from .lab_config import LabConfig
    cfg=LabConfig()
    policy=json.loads(args.policy_json.read_text(encoding='utf-8')) if args.policy_json else current_policy(cfg.horizon_seconds)
    report=evaluate(rows,labels,policy,policy['horizon_seconds']*1000)
    report['manifest']=manifest
    report['code_hashes']={name:hashlib.sha256((Path(__file__).parent/name).read_bytes()).hexdigest()
        for name in ('strategy_research.py','execution_policy.py','lab_replay.py','lab_model.py','lab_train.py','strategies.py')}
    report['experiment_id']=fingerprint({'manifest':manifest,'candidates':CANDIDATES,
                                         'policy':report['policy'],'code':report['code_hashes']})
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'status':report['status'],'experiment_id':report['experiment_id'],
        'all_selected_folds_passed':report['all_selected_folds_passed'],
        'promotion_allowed':False}),flush=True)


if __name__=='__main__':main()
