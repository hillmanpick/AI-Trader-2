"""Read-only execution evidence. No credentials, orders or exchange requests."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

from .execution_state import percentile


def summarize(values):
    return {'count':len(values),'p50_ms':percentile(values,.5),'p95_ms':percentile(values,.95),
            'p99_ms':percentile(values,.99),'max_ms':max(values) if values else None}


def report(database, since_ns=0, limit=1000):
    if not 1 <= limit <= 10000:
        raise ValueError('Evidence query must be bounded')
    path=Path(database).resolve()
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=5) as c:
        c.execute('PRAGMA query_only=ON')
        c.execute('BEGIN')
        orders=[]
        examined=0
        # Rowid descending reflects actual journal insert order. Bound the scan
        # and disclose that limit instead of claiming full-history coverage.
        for (body,) in c.execute('SELECT body FROM execution_orders ORDER BY rowid DESC LIMIT ?',(limit,)):
            examined+=1
            order=json.loads(body)
            if order['submitted_ns']>=since_ns:
                orders.append(order)
        def get(key):
            row=c.execute('SELECT value FROM kv WHERE key=?',(key,)).fetchone()
            return json.loads(row[0]) if row else None
        runtime,worker=get('execution_runtime'),get('worker')
        positions=get('positions') or {}
    ack=[(o['ack_ns']-o['submitted_ns'])/1e6 for o in orders if o.get('ack_ns') is not None]
    fill=[(o['first_fill_ns']-o['submitted_ns'])/1e6 for o in orders if o.get('first_fill_ns') is not None]
    states={s:sum(o['state']==s for o in orders) for s in sorted({o['state'] for o in orders})}
    attempts=len(orders)
    unmatched=sum(float(o['filled'])==0 for o in orders)
    evidence={'schema':'paper-execution-evidence-v1','at':datetime.now(timezone.utc).isoformat(),
        'scope':{'database':str(path),'since_ns':since_ns,'latest_rows_examined':examined,'limit':limit,
                 'window_orders':attempts,'simulation_only':True},
        'orders':{'states':states,'zero_fill_orders':unmatched,
                  'zero_fill_ratio':unmatched/attempts if attempts else None,
                  'closing_orders':sum(bool(o.get('closing')) for o in orders)},
        'simulated_acknowledgement':summarize(ack),'simulated_first_fill':summarize(fill),
        'runtime':runtime,'worker':{k:(worker or {}).get(k) for k in ('status','error','heartbeat')},
        'open_positions':len(positions),'exchange_execution_measured':False,
        'limitations':['Simulation acknowledgement includes the configured 20 ms delay',
             'Local timestamps require host clock health; not exchange receipt timestamps',
             'Filled-order latency alone excludes zero-fill and expired orders; both are reported',
             'No authenticated exchange trading WebSocket, network RTT or matching priority is tested']}
    enough=len(orders)>=30 and len(ack)>=30
    p95=evidence['simulated_acknowledgement']['p95_ms']
    evidence['local_100ms_target']={'enough_samples':enough,'minimum_orders':30,
        'met':bool(enough and p95 is not None and p95<=100 and unmatched/attempts<=.1),
        'criteria':'>=30 orders and acknowledgements; ack p95<=100ms and zero-fill ratio<=10%',
        'exchange_level_hft':False}
    return evidence


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True)
    parser.add_argument('--since-seconds',type=int,default=600)
    parser.add_argument('--limit',type=int,default=1000)
    parser.add_argument('--output')
    args=parser.parse_args()
    if args.since_seconds<0:
        parser.error('since-seconds cannot be negative')
    data=report(args.database,time.time_ns()-args.since_seconds*10**9,args.limit)
    text=json.dumps(data,ensure_ascii=False,indent=2)
    if args.output:
        Path(args.output).write_text(text,encoding='utf-8')
    print(text)


if __name__=='__main__':
    main()
