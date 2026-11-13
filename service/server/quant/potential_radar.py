"""Causal, explainable upside watchlist. Scores are not probabilities or orders."""
import math

SCHEMA = 'potential-radar-v1'
BAR_MS = 300000
HORIZON_MS = 4*3600000
FEATURES = ('return_15m','return_1h','return_4h','relative_btc_1h','volume_multiple',
    'flow_15m','fast_flow','fast_volume_multiple','breakout_distance','volatility',
    'oi_change_1h','funding_rate','spot_return_1h','spot_flow_15m',
    'oi_available','funding_available','spot_available')


def finite(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('Non-finite market value')
    return result


def closed_bars(raw, now_ms, minimum=73, require_fresh=True):
    bars=[]
    for row in raw:
        start,end=int(row[0]),int(row[6])
        if end>=now_ms:
            continue  # Never use an unfinished candle, including in backfills.
        o,h,l,c,q,taker=(finite(row[i]) for i in (1,2,3,4,7,10))
        if (start%BAR_MS or end!=start+BAR_MS-1 or min(o,h,l,c)<=0
                or h<max(o,c,l) or l>min(o,c,h) or q<0 or not 0<=taker<=q+1e-6*max(1,q)):
            raise ValueError('Invalid five-minute candle')
        bars.append({'ts':start,'open':o,'high':h,'low':l,'close':c,'quote':q,'taker':taker})
    if len(bars)<minimum or any(b['ts']-a['ts']!=BAR_MS for a,b in zip(bars,bars[1:])):
        raise ValueError('Insufficient or discontinuous closed candles')
    if require_fresh and not 0<=now_ms-(bars[-1]['ts']+BAR_MS)<=2*BAR_MS:
        raise ValueError('Closed candle history stale')
    return bars


def fast_valid(row, now_ms):
    try:
        return bool(row.get('ready') and row.get('schema')=='market-flow-v1'
            and 0<=now_ms-int(row['ts'])<=20000 and len(row['vector'])==8
            and all(math.isfinite(float(v)) for v in row['vector']) and finite(row['price'])>0)
    except (TypeError,ValueError,KeyError):
        return False


def clamp(v):
    return max(0.,min(1.,v))


def candidate(row, bars, btc_bars, now_ms, oi=None, funding=None, spot=None, liquidity=None,
              min_volume=5000000):
    if not fast_valid(row,now_ms):
        raise ValueError('Public trade features stale or invalid')
    price=finite(row['price']); v=list(map(finite,row['vector']))
    volume24=math.expm1(v[6])
    if volume24<min_volume:
        raise ValueError('24-hour liquidity below radar minimum')
    # Candle returns use only completed bars, with equal clocks for BTC.
    if btc_bars[-1]['ts']!=bars[-1]['ts']:
        raise ValueError('BTC comparison clock mismatch')
    ret=lambda n:bars[-1]['close']/bars[-1-n]['close']-1
    r15,r1,r4=ret(3),ret(12),ret(48)
    relative=r1-(btc_bars[-1]['close']/btc_bars[-13]['close']-1)
    recent=sum(b['quote'] for b in bars[-3:]); baseline=sum(b['quote'] for b in bars[-15:-3])/4
    multiple=recent/baseline if baseline>0 else 0.
    flow=2*sum(b['taker'] for b in bars[-3:])/recent-1 if recent>0 else 0.
    prior_high=max(b['high'] for b in bars[-73:-3]); breakout=price/prior_high-1
    changes=[math.log(b['close']/a['close']) for a,b in zip(bars[-13:-1],bars[-12:])]
    volatility=math.sqrt(sum(x*x for x in changes)/len(changes))
    oi_change=0.; oi_ok=False
    if oi:
        values=sorted(oi,key=lambda x:int(x['timestamp']))
        stamps=[int(x['timestamp']) for x in values]
        amounts=[finite(x['sumOpenInterest']) for x in values]
        if (len(values)>=13 and 0<=now_ms-stamps[-1]<=2*BAR_MS and min(amounts)>0
                and all(b-a==BAR_MS for a,b in zip(stamps,stamps[1:]))):
            oi_change=amounts[-1]/amounts[-13]-1;oi_ok=True
    funding_ok=bool(funding and 0<=now_ms-int(funding.get('time',0))<=120000)
    rate=finite(funding['lastFundingRate']) if funding_ok else 0.
    spot_ok=bool(spot and spot[-1]['ts']==bars[-1]['ts'])
    spot_return=spot[-1]['close']/spot[-13]['close']-1 if spot_ok else 0.
    spot_quote=sum(b['quote'] for b in spot[-3:]) if spot_ok else 0.
    spot_flow=2*sum(b['taker'] for b in spot[-3:])/spot_quote-1 if spot_quote>0 else 0.
    features=dict(zip(FEATURES,(r15,r1,r4,relative,multiple,flow,v[3],v[5],breakout,volatility,
        oi_change,rate,spot_return,spot_flow,float(oi_ok),float(funding_ok),float(spot_ok))))
    components={'volume':20*clamp(math.log2(max(multiple,1))/3),
        'relative_strength':20*clamp(relative/.03),
        'trend':15*clamp(r1/.04) if r15>0 else 0.,
        'buy_flow':15*clamp((flow+v[3])/2),
        'breakout':10. if -.005<=breakout<=.08 and r1>0 else 0.,
        'open_interest':10*clamp(oi_change/.1) if oi_ok and r1>0 else 0.,
        'spot_confirmation':10*clamp(spot_return/.03) if spot_ok and spot_flow>0 else 0.}
    reasons=[];risks=[];penalty=0.
    if multiple>=2:reasons.append(f'15 分钟成交额为此前基准的 {multiple:.2f} 倍')
    if relative>=.01:reasons.append(f'1 小时强于 BTC {relative*100:.2f} 个百分点')
    if flow>.1 and v[3]>0:reasons.append('15 分钟与实时主动买盘同向')
    if components['breakout']:reasons.append('接近或突破此前整理区高点')
    if oi_ok and oi_change>.02 and r1>0:reasons.append('价格上行且合约未平仓数量增加；不单独解释为多头流入')
    if spot_ok and spot_return>0 and spot_flow>0:reasons.append('同名现货上涨且主动买盘为正')
    if not oi_ok:risks.append('持仓量数据缺失或过期')
    if not funding_ok:risks.append('资金费率数据缺失或过期')
    if not spot_ok:risks.append('缺少可比现货数据；不使用名称不同的代币替代')
    if funding_ok and rate>.001:penalty+=15;risks.append('资金费率偏高，注意拥挤多头')
    if breakout>.08 or v[7]>.5:penalty+=15;risks.append('已明显拉升，存在追高风险')
    if r15<0:penalty+=10;risks.append('最近 15 分钟回落，尚无持续上行确认')
    book_ok=bool(liquidity and 0<=now_ms-int(liquidity.get('updated_ms',0))<=2000)
    if not book_ok:risks.append('当前无连续盘口核验，不据此判断可成交数量')
    elif finite(liquidity['spread_bps'])>10:penalty+=20;risks.append('实时价差较宽')
    last=bars[-1]
    if last['high']/max(last['open'],last['close'])-1>max(.01,volatility*2):
        penalty+=10;risks.append('最近完成 K 线有较长上影')
    prior_low=min(b['low'] for b in bars[-73:-3]);breakdown=price/prior_low-1
    short_components={'volume':components['volume'],'relative_weakness':20*clamp(-relative/.03),
        'trend':15*clamp(-r1/.04) if r15<0 else 0.,'sell_flow':15*clamp(-(flow+v[3])/2),
        'breakdown':10. if -.08<=breakdown<=.005 and r1<0 else 0.,
        'open_interest':10*clamp(oi_change/.1) if oi_ok and r1<0 else 0.,
        'spot_confirmation':10*clamp(-spot_return/.03) if spot_ok and spot_flow<0 else 0.}
    short_penalty=0.;short_risks=[]
    if funding_ok and rate<-.001:short_penalty+=15;short_risks.append('负资金费率偏高，注意拥挤空头与轧空')
    if breakdown<-.08 or v[7]<-.5:short_penalty+=15;short_risks.append('已明显下跌，存在追空反弹风险')
    if r15>0:short_penalty+=10;short_risks.append('最近15分钟反弹，尚无持续下行确认')
    if book_ok and finite(liquidity['spread_bps'])>10:short_penalty+=20;short_risks.append('实时价差较宽')
    if min(last['open'],last['close'])/last['low']-1>max(.01,volatility*2):
        short_penalty+=10;short_risks.append('最近完成K线有较长下影')
    short_score=round(max(0,sum(short_components.values())-short_penalty),2)
    timing={'return_5m':ret(1),'return_15m':r15,'range_high_1h':max(b['high'] for b in bars[-12:]),
        'range_low_1h':min(b['low'] for b in bars[-12:]),'last_closed_high':last['high'],
        'last_closed_low':last['low'],'last_closed_close':last['close'],'breakdown_distance':breakdown,
        'history_end_ms':last['ts']+BAR_MS-1}
    return {'symbol':row['symbol'],'ts':now_ms,'price':price,'score':round(max(0,sum(components.values())-penalty),2),
        'short_score':short_score,'short_components':short_components,'short_risks':short_risks,'timing':timing,
        'score_kind':'rule_score_not_probability','reasons':reasons or ['尚无足够上涨共振信号'],
        'risks':risks,'features':features,'vector':[features[k] for k in FEATURES],
        'components':components,'penalty':penalty,'volume_24h_usdt':volume24,
        'history_end_ms':bars[-1]['ts']+BAR_MS-1,'depth_verified':book_ok,'schema':SCHEMA,
        'prediction':None,'execution_enabled':False}


def evaluate_path(raw, start_ms, now_ms, target=.10, stop=.05, fee=.0005, slippage=.0003):
    """Hypothetical next-five-minute-open entry; ambiguous intrabar paths rejected."""
    end_ms=start_ms+HORIZON_MS
    if now_ms<end_ms:
        raise ValueError('Future observation horizon incomplete')
    rows=[r for r in closed_bars(raw,now_ms,minimum=1,require_fresh=False) if start_ms<=r['ts']<end_ms]
    if len(rows)!=HORIZON_MS//BAR_MS or rows[0]['ts']!=start_ms or rows[-1]['ts']+BAR_MS!=end_ms:
        raise ValueError('Future price path missing candles')
    entry=rows[0]['open']*(1+slippage); high=entry*(1+target);low=entry*(1-stop)
    outcome='timeout';exit_price=rows[-1]['close'];label=0
    for r in rows:
        up,down=r['high']>=high,r['low']<=low
        if up and down:
            return {'valid':False,'reason':'both_barriers_in_one_candle','label_end':end_ms}
        if down:outcome='down_first';exit_price=min(low,r['open']);break
        if up:outcome='up_first';exit_price=high;label=1;break
    actual_exit=exit_price*(1-slippage)
    net=(actual_exit-entry)/entry-fee*(1+actual_exit/entry)
    return {'valid':True,'label':label,'outcome':outcome,'label_end':end_ms,'entry':entry,
        'exit':actual_exit,'net_return':net,'max_up':max(r['high'] for r in rows)/entry-1,
        'max_drawdown':min(r['low'] for r in rows)/entry-1,
        'path':[[r[k] for k in ('ts','open','high','low','close')] for r in rows],
        'assumption':'next 5m open, long only, fixed 10%/5% barriers, 4h; not a paper fill'}
