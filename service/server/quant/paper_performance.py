"""Read-only PnL reconstruction from the durable fills, independent of deposits."""
import json
from datetime import datetime, timedelta, timezone

from .mail_format import local_time


def paper_performance(store, now=None, *, include_trades=False):
    local = local_time(now)
    end_ns = int(local.timestamp()*1e9)
    day_ns = int(local.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1e9)
    yesterday_ns = int((local.replace(hour=0,minute=0,second=0,microsecond=0)-timedelta(days=1)).timestamp()*1e9)
    recent_ns = int((local-timedelta(hours=2)).timestamp()*1e9)
    with store.connect() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='execution_fills'").fetchone():
            return None
        rows = c.execute('SELECT f.quantity,f.price,f.fee,f.at_ns,o.body FROM execution_fills f '
                         'JOIN execution_orders o ON o.id=f.order_id WHERE f.at_ns<=? ORDER BY f.at_ns,f.rowid',
                         (end_ns,)).fetchall()
    positions, completed = {}, []
    result = {'scope':'盘口模拟成交账本（不含旧策略历史和追加资金）','net':0.,'today_net':0.,'recent_net':0.,'fees':0.,
              'yesterday_net':0.,'gross':0.,'funding_cost':0.,'completed_trades':0,'wins':0,'win_rate':0.}
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
            funding = 0.
            context = old['context']
            if context.get('quant_version'):
                from .quant_strategy import funding_charge
                funding = funding_charge(qty,old['entry'],context['entry_profile'],
                                         old['opened_ns']//1000000,stamp//1000000)
                result['funding_cost'] += funding
            net = gross-old['fee']*ratio-fee-funding
            old['cycle_net'] += net
            old['cycle_gross'] += gross
            old['cycle_cost'] += old['fee']*ratio+fee+funding
            result['gross'] += gross
            result['net'] += net
            if stamp>=day_ns:
                result['today_net'] += net
            elif stamp>=yesterday_ns:
                result['yesterday_net'] += net
            if stamp>=recent_ns:
                result['recent_net'] += net
            if ratio>=1-1e-10:
                result['completed_trades'] += 1
                result['wins'] += int(old['cycle_net']>0)
                context = old['context']
                completed.append({'cycle_id':old['cycle_id'],'symbol':o['symbol'],
                    'side':'long' if old['side']>0 else 'short',
                    'at':datetime.fromtimestamp(stamp/1e9,timezone.utc).isoformat(),
                    'hold_seconds':(stamp-old['opened_ns'])/1e9,
                    'gross_pnl':old['cycle_gross'],'cost_usdt':old['cycle_cost'],
                    'net_pnl':old['cycle_net'],'exit_reason':o.get('reason'),
                    'model_version':context.get('model_version'),
                    'decision_authority':'quant_system' if context.get('quant_version') else 'deepseek' if context.get('ds_authority') else 'legacy',
                    'strategy_version':context.get('quant_version') or context.get('strategy_version',1),
                    'regime':context.get('regime','unknown')})
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
                'cycle_net':old['cycle_net'] if old else 0.,
                'cycle_gross':old['cycle_gross'] if old else 0.,
                'cycle_cost':old['cycle_cost'] if old else 0.,
                'cycle_id':old['cycle_id'] if old else o['id'],
                'opened_ns':old['opened_ns'] if old else stamp,
                'context':old['context'] if old else o.get('paper_context',{}),
                'margin':(old['margin'] if old else 0)+qty*price/leverage}
    if result['completed_trades']:
        result['win_rate'] = result['wins']/result['completed_trades']
    if include_trades:
        result['completed_positions'] = completed
    return result


def decision_feedback(store,limit=100):
    """Causal realized outcomes only; a win fraction is not a prediction."""
    report = paper_performance(store,include_trades=True)
    trades = [t for t in (report or {}).get('completed_positions',[])
              if t.get('decision_authority')=='deepseek'][-limit:]
    def summarize(rows):
        wins = [t['net_pnl'] for t in rows if t['net_pnl']>0]
        losses = [t['net_pnl'] for t in rows if t['net_pnl']<=0]
        return {'trades':len(rows),'wins':len(wins),'gross':sum(t['gross_pnl'] for t in rows),
                'fees':sum(t['cost_usdt'] for t in rows),'net':sum(t['net_pnl'] for t in rows),
                'average_win':sum(wins)/len(wins) if wins else None,
                'average_loss':sum(losses)/len(losses) if losses else None}
    return {'scope':'last completed DS-authority positions; descriptive, not validated forecasts',
            'total':summarize(trades),
            'by_side':{side:summarize([t for t in trades if t['side']==side]) for side in ('long','short')},
            'by_exit':{reason:summarize([t for t in trades if t['exit_reason']==reason])
                       for reason in sorted({t['exit_reason'] for t in trades})}}
