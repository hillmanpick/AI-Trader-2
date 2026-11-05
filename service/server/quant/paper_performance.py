"""Read-only PnL reconstruction from the durable fills, independent of deposits."""
import json
from datetime import timedelta

from .mail_format import local_time


def paper_performance(store, now=None):
    local = local_time(now)
    end_ns = int(local.timestamp()*1e9)
    day_ns = int(local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1e9)
    recent_ns = int((local-timedelta(hours=2)).timestamp()*1e9)
    with store.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='execution_fills'").fetchone():
            return None
        rows = c.execute('SELECT f.quantity,f.price,f.fee,f.at_ns,o.body FROM execution_fills f '
                         'JOIN execution_orders o ON o.id=f.order_id WHERE f.at_ns<=? ORDER BY f.at_ns,f.rowid',
                         (end_ns,)).fetchall()
    positions = {}
    result = {'scope':'盘口模拟成交账本（不含旧策略历史和追加资金）','net':0.,'today_net':0.,'recent_net':0.,'fees':0.}
    for quantity,price,fee,stamp,raw in rows:
        o = json.loads(raw)
        qty,price,fee = float(quantity),float(price),float(fee)
        result['fees'] += fee
        old = positions.get(o['symbol'])
        if o.get('closing'):
            if not old or qty>old['quantity']+1e-8:
                # Incomplete ledger history must never be presented as profit.
                return None
            ratio = min(1.,qty/old['quantity'])
            gross = (price-old['entry'])*qty*old['side']
            if o.get('reason') == 'paper_liquidation':
                gross = max(gross-fee,-old['margin']*ratio)+fee
            net = gross-old['fee']*ratio-fee
            result['net'] += net
            if stamp>=day_ns:
                result['today_net'] += net
            if stamp>=recent_ns:
                result['recent_net'] += net
            if ratio>=1-1e-10:
                positions.pop(o['symbol'])
            else:
                old['quantity'] -= qty
                old['fee'] *= 1-ratio
                old['margin'] *= 1-ratio
        else:
            leverage = o.get('paper_context',{}).get('leverage',1)
            total = (old['quantity'] if old else 0)+qty
            positions[o['symbol']] = {
                'quantity':total,'side':o['side'],
                'entry':((old['quantity']*old['entry'] if old else 0)+qty*price)/total,
                'fee':(old['fee'] if old else 0)+fee,
                'margin':(old['margin'] if old else 0)+qty*price/leverage}
    return result
