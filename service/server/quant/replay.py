"""Read-only, conservative OHLC replay. No keys, model calls or order endpoints.

Run: python -m quant.replay --data-dir /tmp/replay --output /tmp/report.json
The fixed candidate rule is evaluated on two chronological windows, not tuned.
"""
import argparse
import json
import math
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

from .features import indicators, entry_quality
from .settings import Settings
from .strategies import signal, volatility_plan, update_dynamic_exit


def download(symbols, days, directory):
    directory.mkdir(parents=True,exist_ok=True)
    with requests.Session() as session:
        session.trust_env=False
        def read(path,params=None):
            r=session.get('https://fapi.binance.com'+path,params=params,timeout=(5,20))
            r.raise_for_status()
            return r.json()
        end=int(read('/fapi/v1/time')['serverTime'])//60000*60000-1
        for symbol in symbols:
            start=end-days*86400000+1; bars=[]
            while start < end:
                rows=read('/fapi/v1/klines',{'symbol':symbol,'interval':'1m',
                          'startTime':start,'endTime':end,'limit':1000})
                if not rows:
                    raise ValueError('History incomplete: '+symbol)
                bars.extend(rows)
                start=int(rows[-1][0])+60000
            (directory/(symbol+'.json')).write_text(json.dumps(bars),encoding='utf-8')
            print(symbol,'closed_bars',len(bars),flush=True)


def replay(data,cfg,enforce=False):
    """Signals see only preceding closes; entries fill at the next open.

    Intrabar stop/take ambiguity resolves stop first. Existing stops cannot be
    moved using the same bar's future high. Trade-price OHLC approximates marks.
    """
    symbols=list(data)
    size=min(map(len,data.values()))
    warmup=max(40,cfg.slow+2,cfg.boll_period,21)
    if size <= warmup:
        raise ValueError('Not enough history for replay')
    stamps=[int(r[0]) for r in data[symbols[0]]]
    for symbol,rows in data.items():
        if [int(r[0]) for r in rows] != stamps:
            raise ValueError('History is not aligned across symbols')
        if any(b-a != 60000 for a,b in zip(stamps,stamps[1:])):
            raise ValueError('History contains candle gaps')
        for row in rows:
            o,h,l,c=(float(row[n]) for n in (1,2,3,4))
            if not all(math.isfinite(v) and v>0 for v in (o,h,l,c)) or not l<=min(o,c)<=max(o,c)<=h:
                raise ValueError('Invalid OHLC history')
    cash=cfg.paper_cash; positions={}; pending={}; fees=0; trades=[]; blocked=0
    peak=cash; drawdown=0; risk_day=None; day_equity=cash; daily_block=False

    def equity(marks):
        return cash+sum((marks[s]-p['entry'])*p['amount'] for s,p in positions.items())

    def close(symbol,mark,reason,stamp):
        nonlocal cash,fees
        p=positions.pop(symbol); side=1 if p['amount']>0 else -1
        fill=mark*(1-side*cfg.paper_slippage)
        fee=abs(p['amount'])*fill*cfg.fee_rate
        gross=(fill-p['entry'])*p['amount']
        if reason=='liquidation':gross=max(gross-fee,-p['initial_margin'])+fee
        cash+=gross-fee;fees+=fee
        trades.append({'at':stamp,'symbol':symbol,'reason':reason,
                       'net':gross-fee-p['entry_fee']})

    for i in range(warmup,size):
        stamp=stamps[i]
        opens={s:float(data[s][i][1]) for s in symbols}
        closes={s:float(data[s][i][4]) for s in symbols}
        day=datetime.fromtimestamp(stamp/1000,timezone(timedelta(hours=8))).date()
        if day != risk_day:
            risk_day=day;day_equity=equity(opens);daily_block=False
        daily_block=daily_block or day_equity-equity(opens)>=cfg.daily_loss
        for s in symbols:
            action=pending.get(s)
            if action=='close' and s in positions:
                close(s,opens[s],'rule_or_close_check',stamp)
            if action not in ('long','short') or s in positions or daily_block:
                continue
            collateral=sum(p['initial_margin'] for p in positions.values())
            notional=sum(abs(p['amount'])*opens[k] for k,p in positions.items())
            if (len(positions)>=cfg.max_positions or collateral+cfg.paper_margin>cfg.paper_max_margin
                    or notional+cfg.order_notional>cfg.max_notional
                    or equity(opens)-collateral<cfg.paper_margin+cfg.order_notional*.002):
                continue
            history=data[s][max(0,i-240):i]
            plan=volatility_plan(history,opens[s],cfg.leverage,cfg.paper_maintenance_rate)
            f=indicators([r[4] for r in history],cfg)
            q=entry_quality(f,opens[s],action,cfg,plan)
            if enforce and not q['passes']:
                blocked+=1;continue
            side=1 if action=='long' else -1
            fill=opens[s]*(1+side*cfg.paper_slippage)
            amount=side*cfg.order_notional/opens[s]
            fee=abs(amount)*fill*cfg.fee_rate;fees+=fee;cash-=fee
            positions[s]={'amount':amount,'entry':fill,'mark':fill,'pnl':0,
                          'initial_margin':cfg.paper_margin,'entry_fee':fee,
                          'stop_pct':plan['stop_pct'],
                          'stop_price':fill*(1-side*plan['stop_pct']),
                          'take_price':fill*(1+side*plan['take_pct'])}
        pending={}
        for s,p in list(positions.items()):
            o,h,l,c=(float(data[s][i][n]) for n in (1,2,3,4))
            long=p['amount']>0; qty=abs(p['amount']); direction=1 if long else -1
            liq=(p['entry']-direction*p['initial_margin']/qty)/(1-direction*cfg.paper_maintenance_rate)
            if (long and o<=liq) or (not long and o>=liq):
                close(s,o,'liquidation',stamp);continue
            stop=p['stop_price']; take=p['take_price']
            if (long and l<=stop) or (not long and h>=stop):
                close(s,min(o,stop) if long else max(o,stop),'stop',stamp);continue
            if (long and h>=take) or (not long and l<=take):
                close(s,take,'take',stamp);continue
            p['mark']=c
            # Only the now-closed bar can update next bar's trailing levels.
            history=data[s][max(0,i-239):i+1]
            plan=volatility_plan(history,c,cfg.leverage,cfg.paper_maintenance_rate)
            if update_dynamic_exit(p,plan,cfg.fee_rate,cfg.paper_slippage):
                pending[s]='close'
        net_equity=equity(closes)
        peak=max(peak,net_equity);drawdown=max(drawdown,peak-net_equity)
        daily_block=daily_block or day_equity-net_equity>=cfg.daily_loss
        for s in symbols:
            if pending.get(s)=='close':continue
            history=data[s][max(0,i-239):i+1]
            action=signal([float(r[4]) for r in history],positions.get(s,{}).get('amount',0),cfg)
            if action!='hold':pending[s]=action
    # Include exit costs for an apples-to-apples terminal valuation.
    for s in list(positions):close(s,float(data[s][-1][4]),'end_of_window',stamps[-1])
    peak=max(peak,cash);drawdown=max(drawdown,peak-cash)
    wins=[t['net'] for t in trades if t['net']>0];losses=[-t['net'] for t in trades if t['net']<0]
    return {'start_ms':stamps[warmup],'end_ms':stamps[-1],'net_pnl':round(cash-cfg.paper_cash,4),
            'max_drawdown_usdt':round(drawdown,4),'fees_usdt':round(fees,4),
            'trades':len(trades),'win_rate':len(wins)/len(trades) if trades else 0,
            'profit_factor':sum(wins)/sum(losses) if losses else None,
            'liquidations':sum(t['reason']=='liquidation' for t in trades),'cost_rejected':blocked}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--download-days',type=int,default=0)
    parser.add_argument('--symbols',default='BTCUSDT,ETHUSDT,SOLUSDT,NEARUSDT,XRPUSDT,DOGEUSDT,SUIUSDT,1000PEPEUSDT')
    args=parser.parse_args();symbols=args.symbols.split(',')
    if args.download_days:download(symbols,args.download_days,args.data_dir)
    data={s:json.loads((args.data_dir/(s+'.json')).read_text()) for s in symbols}
    cfg=replace(Settings(),symbols=tuple(symbols),mode='paper',strategy='adaptive',interval='1m',fast=3,slow=10,
                leverage=100,paper_margin=2000,paper_max_margin=6000,order_notional=200000,max_notional=600000,
                max_positions=3,paper_dynamic_exits=True,paper_slippage=.0003,boll_deviation=1.5,
                daily_loss=500,paper_cash=10000,fee_rate=.0005,poll_seconds=5,db_path='/tmp/quant-replay.db')
    cfg.validate()
    split=min(map(len,data.values()))//2
    report={'method':'fixed-rule chronological comparison; no parameter fitting',
            'limitations':['No AI decisions: isolates deterministic filter only',
                           'Funding and order book impact not modeled; fixed adverse slippage 3 bps',
                           'Trade OHLC approximates mark; 1 minute stop-first OHLC cannot reconstruct 5 second fills',
                           'Cold cache and API savings are not measured by replay',
                           'Risk bracket and quantity-step approximations; not evidence of future profitability'],
            'windows':{}}
    for name,part in [('earlier',{s:rows[:split] for s,rows in data.items()}),
                      ('later',{s:rows[max(0,split-240):] for s,rows in data.items()})]:
        # Later window includes warmup, but no trades before its actual start.
        if name=='later':part={s:rows[200:] for s,rows in part.items()}
        baseline=replay(part,cfg,False);candidate=replay(part,cfg,True)
        report['windows'][name]={'baseline':baseline,'candidate':candidate}
        print(name,json.dumps(report['windows'][name]),flush=True)
    report['candidate_consistently_better']=all(
        w['candidate']['net_pnl']>w['baseline']['net_pnl'] and
        w['candidate']['max_drawdown_usdt']<=w['baseline']['max_drawdown_usdt'] and
        w['candidate']['trades']>=20 for w in report['windows'].values())
    report['deployment_recommendation']='shadow; require forward paper evidence before enforcing'
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__':main()
