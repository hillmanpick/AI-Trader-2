"""Deterministic, closed-candle features and paper-only entry cost checks."""
import math
from statistics import fmean, pstdev

from .strategies import market_regime, volatility_plan


def indicators(closes, cfg, bars=None):
    values = [float(v) for v in closes]
    if any(not math.isfinite(v) or v <= 0 for v in values):
        raise ValueError('Invalid feature prices')
    minimum = max(21, cfg.slow+2, cfg.boll_period)
    if len(values) < minimum:
        return {'status': 'insufficient', 'samples': len(values), 'recent_closes': values[-6:]}
    window = values[-21:]
    travel = sum(abs(b-a) for a,b in zip(window,window[1:]))
    mean, std = fmean(values[-cfg.boll_period:]), pstdev(values[-cfg.boll_period:])
    returns = [(b/a-1)*10000 for a,b in zip(window,window[1:])]
    result = {
        'status':'ok', 'recent_closes':values[-6:],
        'sma':{'fast_period':cfg.fast, 'slow_period':cfg.slow,
               'fast':fmean(values[-cfg.fast:]), 'slow':fmean(values[-cfg.slow:]),
               'previous_fast':fmean(values[-cfg.fast-1:-1]),
               'previous_slow':fmean(values[-cfg.slow-1:-1])},
        'bollinger':{'period':cfg.boll_period, 'deviation':cfg.boll_deviation,
                     'mean':mean, 'upper':mean+std*cfg.boll_deviation,
                     'lower':mean-std*cfg.boll_deviation,
                     'z_score':(values[-1]-mean)/std if std else 0},
        'trend':{'regime':market_regime(values),
                 'efficiency':abs(window[-1]-window[0])/travel if travel else 0,
                 'returns_bps':{str(n):(values[-1]/values[-1-n]-1)*10000 for n in (1,3,10,20)}},
        'volatility':{'close_return_std_bps':pstdev(returns)},
    }
    if bars is not None:
        plan = volatility_plan(bars, values[-1], cfg.leverage, cfg.paper_maintenance_rate)
        result['volatility'].update(atr14=plan['atr'], atr_bps=plan['atr']/values[-1]*10000)
        high,low,op,close = (float(bars[-1][i]) for i in (2,3,1,4))
        if not math.isfinite(op) or op <= 0 or not low <= min(op,close) <= max(op,close) <= high:
            raise ValueError('Invalid feature candle')
        span = high-low
        result['last_candle'] = {'body_fraction':(close-op)/span if span else 0,
                                'upper_wick_fraction':(high-max(op,close))/span if span else 0,
                                'lower_wick_fraction':(min(op,close)-low)/span if span else 0}
    return result


def entry_quality(features, price, action, cfg, plan):
    """Potential target distance is an upper bound, never a profit forecast."""
    if features.get('status') != 'ok' or action not in ('long','short'):
        raise ValueError('Entry assessment needs closed-candle indicators')
    if not math.isfinite(price) or price <= 0:
        raise ValueError('Invalid entry assessment mark')
    direction = 1 if action == 'long' else -1
    target = plan['take_pct']
    regime = features['trend']['regime']
    if regime == 'range':
        target = min(target,(features['bollinger']['mean']/price-1)*direction)
    costs = 2*(cfg.fee_rate+cfg.paper_slippage)
    stop = plan['stop_pct']
    net_reward = target-costs
    net_risk = stop+costs
    ratio = net_reward/net_risk
    drift = (price/features['recent_closes'][-1]-1)*direction
    reason = ('target_below_cost' if net_reward <= 0 else
              'weak_net_reward_risk' if ratio < .5 else
              'entry_chased' if drift > stop else 'ok')
    return {'status':reason, 'passes':reason == 'ok', 'regime':regime,
            'round_trip_cost_bps':costs*10000, 'target_distance_bps':target*10000,
            'stop_distance_bps':stop*10000, 'net_reward_risk':ratio,
            'favorable_drift_bps':drift*10000,
            'funding':'not_modeled', 'target_is_forecast':False}
