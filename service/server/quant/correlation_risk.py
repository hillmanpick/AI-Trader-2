"""Aligned return covariance and signed portfolio risk, including hedge offsets."""
import math


def portfolio_risk(returns,exposures,horizon_seconds=300,shrinkage=.2,min_points=30):
    symbols = [s for s,n in exposures.items() if n]
    if not symbols:
        return {'sigma_usdt':0.,'correlations':{},'points':0}
    if any(s not in returns for s in symbols):
        raise ValueError('Missing portfolio return history')
    stamps = set.intersection(*(set(returns[s]) for s in symbols))
    stamps = sorted(stamps)
    if len(stamps)<min_points:
        raise ValueError('Insufficient aligned covariance observations')
    data = {s:[returns[s][t] for t in stamps] for s in symbols}
    if any(not math.isfinite(v) for row in data.values() for v in row):
        raise ValueError('Non-finite portfolio returns')
    weights = [.94**(len(stamps)-1-i) for i in range(len(stamps))]
    total = sum(weights)
    weights = [w/total for w in weights]
    means = {s:sum(w*v for w,v in zip(weights,data[s])) for s in symbols}
    covariance = {}
    for a in symbols:
        for b in symbols:
            cov = sum(w*(x-means[a])*(y-means[b]) for w,x,y in zip(weights,data[a],data[b]))
            covariance[a,b] = cov*(1 if a==b else 1-shrinkage)
    variance = sum(float(exposures[a])*float(exposures[b])*covariance[a,b] for a in symbols for b in symbols)
    correlations = {}
    for a in symbols:
        correlations[a] = {}
        for b in symbols:
            denominator = math.sqrt(max(0,covariance[a,a]*covariance[b,b]))
            correlations[a][b] = covariance[a,b]/denominator if denominator else (1. if a==b else 0.)
    return {'sigma_usdt':math.sqrt(max(0,variance)*horizon_seconds),'correlations':correlations,
            'points':len(stamps),'method':'aligned EWMA covariance with diagonal shrinkage',
            'horizon_seconds':horizon_seconds}


def cap_notional(snapshot,positions,symbol,side,maximum,risk_limit,now_ms,horizon_seconds=300):
    if side not in (-1,1) or any(not math.isfinite(v) or v<=0 for v in (maximum,risk_limit,horizon_seconds)):
        raise ValueError('Invalid portfolio risk limit')
    if not snapshot or not 0<=now_ms-snapshot['ts']<=15000:
        raise ValueError('Portfolio return snapshot is stale')
    returns = {s:{int(k):float(v) for k,v in values.items()} for s,values in snapshot['returns'].items()}
    exposures = {s:p['amount']*p['mark'] for s,p in positions.items()}
    def assess(notional):
        return portfolio_risk(returns,exposures | {symbol:exposures.get(symbol,0)+side*notional},horizon_seconds)
    base = assess(0)
    high = assess(maximum)
    if high['sigma_usdt']<=risk_limit:
        return maximum,high
    if base['sigma_usdt']>risk_limit:
        # A risk-reducing hedge may be allowed only if the complete requested
        # hedge brings risk below budget. Do not assume monotonicity across zero.
        raise ValueError('Existing portfolio covariance risk exceeds budget')
    low,upper = 0.,maximum
    for _ in range(30):
        middle = (low+upper)/2
        if assess(middle)['sigma_usdt']<=risk_limit:
            low = middle
        else:
            upper = middle
    return low,assess(low)
