"""Chronological quote replay, shared portfolio, latency and partial fills."""
from collections import Counter
from .strategies import update_dynamic_exit
from .execution_policy import microstructure_plan


def replay(rows, predictions, *, fee=.0005, slippage=.0003, latency_ms=500,
           margin=2000, max_margin=6000, risk_fraction=.10, min_leverage=2,
           max_leverage=20, max_positions=3, depth_participation=.05,
           horizon_ms=300000, max_spread_bps=10, funding_fallback=.001,
           policy=None, signal_fn=None):
    cfg = {'fee_rate':fee,'paper_slippage':slippage,'paper_margin':margin,
           'paper_max_margin':max_margin,'paper_risk_fraction':risk_fraction,
           'paper_min_leverage':min_leverage,'paper_max_leverage':max_leverage,
           'paper_major_max_leverage':max_leverage,'paper_maintenance_rate':.005,
           'paper_risk_buffer':.001,'paper_depth_participation':depth_participation,
           'paper_max_spread_bps':max_spread_bps,'max_positions':max_positions,
           'max_notional':max_margin*max_leverage,'paper_exploration_mode':False}
    cfg.update(policy or {})
    fee, slippage = cfg['fee_rate'],cfg['paper_slippage']
    margin,max_margin = cfg['paper_margin'],cfg['paper_max_margin']
    max_positions,max_spread_bps = cfg['max_positions'],cfg['paper_max_spread_bps']
    depth_participation = cfg['paper_depth_participation']
    reasons, gaps = Counter(),Counter()
    cash, peak, drawdown = 100000., 100000., 0.
    positions, pending, latest, trades = {}, {}, {}, []
    rejected, partial, fees = 0, 0, 0.

    def fill(row,side):
        return row['meta']['ask' if side > 0 else 'bid']*(1+side*slippage)

    def close(s,row,reason):
        nonlocal cash,fees
        p = positions.pop(s)
        px = fill(row,-p['side'])
        cost = p['qty']*px*fee
        gross = (px-p['entry'])*p['qty']*p['side']
        # Conservative funding fallback, prorated; never credits funding income.
        funding = p['qty']*p['entry']*abs(row.get('meta',{}).get('funding_rate',funding_fallback))*max(0,row['ts']-p['ts'])/28800000
        net = max(gross-cost-funding,-p['margin'])-p['entry_fee']
        cash += net+p['entry_fee']
        fees += cost+funding
        trades.append({'symbol':s,'net':net,'reason':reason,'mae':p['mae'],'mfe':p['mfe'],
                       'leverage':p['leverage'],'margin':p['margin'],'at':row['ts']})

    pairs = ((r,signal_fn(r)) for r in rows) if signal_fn else zip(rows,predictions)
    for row,pred in pairs:
        s,t,price,meta = row['symbol'],row['ts'],row['price'],row['meta']
        if s in positions and s in latest and t-latest[s]['ts']>45000:
            gaps[s] += 1
        latest[s] = row
        if s in positions:
            p = positions[s]
            move = (price/p['entry']-1)*p['side']
            p['mae'],p['mfe'] = min(p['mae'],move),max(p['mfe'],move)
            p['mark'] = price
            exit_reason = update_dynamic_exit(p,{'stop_pct':p['stop'],'take_pct':p['take'],
                              'atr':0.,'regime':'microstructure'},fee,slippage)
            if exit_reason or t-p['ts'] >= horizon_ms:
                close(s,row,exit_reason or 'timeout')
                pending.pop(s,None)
        order = pending.get(s)
        if order and t >= order['due']:
            pending.pop(s,None)
            # A long sample gap cannot stand in for a timely fill.
            if t-order['due'] > 45000 or meta['spread_bps'] > max_spread_bps:
                rejected += 1
                reasons['late_quote' if t-order['due']>45000 else 'spread'] += 1
                continue
            used_margin = sum(p['margin'] for p in positions.values())
            if s in positions or len(positions) >= max_positions:
                rejected += 1
                reasons['position_limit'] += 1
                continue
            side = order['side']
            if sum(p['side'] == side for p in positions.values()) >= 2:
                rejected += 1
                reasons['direction_concentration'] += 1
                continue
            stop, take = order['stop'],order['take']
            depth = meta['ask_depth' if side > 0 else 'bid_depth']*depth_participation
            exposure = sum(abs(p['amount'])*p['mark'] for p in positions.values())
            try:
                plan = microstructure_plan(row,meta,cfg,positions,min(depth,cfg['max_notional']-exposure))
            except (ValueError,KeyError) as exc:
                rejected += 1
                reasons['cost' if 'cost' in str(exc).lower() else 'risk_or_liquidity'] += 1
                continue
            leverage,notional = plan['leverage'],plan['notional']
            stop,take = plan['stop_pct'],plan['take_pct']
            if used_margin+plan['margin']>max_margin or cash-used_margin<plan['margin']+notional*.002:
                rejected += 1
                reasons['collateral'] += 1
                continue
            actual = min(notional,depth)
            if actual < 10:
                rejected += 1
                reasons['minimum_notional'] += 1
                continue
            if actual < notional:
                partial += 1
            px = fill(row,side)
            quantity = actual/px
            filters = {f['filterType']:f for f in meta.get('filters',[])}
            lot = filters.get('MARKET_LOT_SIZE',filters.get('LOT_SIZE',{}))
            step = float(lot.get('stepSize',0)) or float(filters.get('LOT_SIZE',{}).get('stepSize',0))
            if step:
                from .binance import rounded
                quantity = float(rounded(quantity,step))
            if (quantity <= 0 or quantity < float(lot.get('minQty',0))
                    or quantity > float(lot.get('maxQty',float('inf')))
                    or quantity*px < float(filters.get('MIN_NOTIONAL',{}).get('notional',0))):
                rejected += 1
                reasons['exchange_quantity'] += 1
                continue
            actual = quantity*px
            entry_fee = actual*fee
            cash -= entry_fee
            fees += entry_fee
            positions[s] = {'entry':px,'qty':quantity,'side':side,'ts':t,'stop':stop,'take':take,
                            'margin':actual/leverage,'leverage':leverage,'entry_fee':entry_fee,'mae':0.,'mfe':0.,
                            'amount':side*quantity,'mark':price,'stop_pct':stop,
                            'stop_price':px*(1-side*stop),'exit_plan':plan}
        if s not in positions and s not in pending and pred in (-1,1):
            stop = max(.0008,meta.get('atr_proxy',.002)*1.5)
            if stop >= .5:
                continue
            pending[s] = {'due':t+latency_ms,'side':pred,'stop':stop,'take':max(.0026,stop*2)}
        equity = cash+sum((latest[k]['price']-p['entry'])*p['qty']*p['side'] for k,p in positions.items())
        peak = max(peak,equity)
        drawdown = max(drawdown,peak-equity)
    for s in list(positions):
        close(s,latest[s],'end')
    drawdown = max(drawdown,peak-cash)
    wins = sum(t['net'] for t in trades if t['net'] > 0)
    losses = -sum(t['net'] for t in trades if t['net'] < 0)
    return {'net_pnl':cash-100000,'trades':len(trades),'win_rate':sum(t['net']>0 for t in trades)/len(trades) if trades else 0,
            'profit_factor':wins/losses if losses else None,'max_drawdown':drawdown,'fees':fees,
            'partial_fills':partial,'rejected':rejected,'rejection_reasons':dict(reasons),
            'valid':not gaps,'data_gaps_while_holding':dict(gaps),'research_only':True,
            'policy':cfg,'latency_ms':latency_ms,
            'mae_mean':sum(t['mae'] for t in trades)/len(trades) if trades else 0,
            'mfe_mean':sum(t['mfe'] for t in trades)/len(trades) if trades else 0,
            'funding':'absolute rate prorated, conservative fallback 10bps/8h',
            'limitations':['Sampled quotes cannot reconstruct intrasecond paths or queue priority',
                           'Visible depth participation approximates impact and partial fills',
                           'Training replay omits per-sample exchange quantity filters to bound RAM',
                           'Survivorship and candidate-selection history limits remain'],
            'fills':trades[-200:]}
