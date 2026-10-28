"""Adapted Qbot rules; only CLOSED candles may be passed by the worker.

SMA: qbot/strategies/sma_cross_strategy_bt.py (10/30 crossover).
BOLL: qbot/strategies/boll_strategy_bt.py (13 bars, two std deviations).
Short-side mirroring and contract risk management are additions, not Qbot claims.
"""
from statistics import fmean, pstdev
import math


def crossover(closes, fast=10, slow=30):
    if len(closes) < slow + 2:
        return 0
    # Backtrader CrossOver remembers the last non-zero difference across ties.
    current = fmean(closes[-fast:]) - fmean(closes[-slow:])
    if current == 0:
        return 0
    for end in range(len(closes)-1, slow-1, -1):
        prior = fmean(closes[end-fast:end]) - fmean(closes[end-slow:end])
        if prior:
            return 1 if prior < 0 < current else -1 if prior > 0 > current else 0
    return 0


def signal(closes, amount, cfg):
    if cfg.strategy == 'adaptive':
        return adaptive_signal(closes, amount, cfg)
    if cfg.strategy == 'sma_cross':
        cross = crossover(closes, cfg.fast, cfg.slow)
        if amount > 0 and cross < 0:
            return 'close'
        if amount < 0 and cross > 0:
            return 'close'
        if amount == 0:
            return 'long' if cross > 0 else 'short' if cross < 0 and cfg.allow_short else 'hold'
    elif len(closes) >= cfg.boll_period:
        window = closes[-cfg.boll_period:]
        mean, width = fmean(window), pstdev(window) * cfg.boll_deviation
        if not width:
            return 'hold'
        lower, upper = mean-width, mean+width
        if (amount > 0 and closes[-1] > upper) or (amount < 0 and closes[-1] < lower):
            return 'close'
        if amount == 0:
            return 'long' if closes[-1] < lower else 'short' if closes[-1] > upper and cfg.allow_short else 'hold'
    return 'hold'


def market_regime(closes):
    """20-bar efficiency ratio separates directional moves from oscillation."""
    if len(closes) < 21:
        return 'unknown'
    window = closes[-21:]
    travel = sum(abs(b-a) for a,b in zip(window, window[1:]))
    efficiency = abs(window[-1]-window[0])/travel if travel else 0
    return 'trend' if efficiency >= .35 else 'range'


def adaptive_signal(closes, amount, cfg):
    if len(closes) < max(21, cfg.slow+2, cfg.boll_period):
        return 'hold'
    cross = crossover(closes, cfg.fast, cfg.slow)
    # A reversal exits a position in either regime. Never flip in one order.
    if (amount > 0 and cross < 0) or (amount < 0 and cross > 0):
        return 'close'
    if market_regime(closes) == 'trend':
        if not amount:
            return 'long' if cross > 0 else 'short' if cross < 0 and cfg.allow_short else 'hold'
        return 'hold'
    window = closes[-cfg.boll_period:]
    mean, width = fmean(window), pstdev(window)*cfg.boll_deviation
    if (amount > 0 and closes[-1] >= mean) or (amount < 0 and closes[-1] <= mean):
        return 'close'
    # Require distance to the mean to exceed round-trip costs plus 10 bps.
    minimum = 2*(cfg.fee_rate+cfg.paper_slippage)+.001
    if not amount and width > 0 and width/mean >= minimum:
        return 'long' if closes[-1] < mean-width else 'short' if closes[-1] > mean+width and cfg.allow_short else 'hold'
    return 'hold'


def volatility_plan(bars, price, leverage, maintenance_rate):
    """ATR14 distances, with a stop below the approximate isolated liquidation move."""
    if len(bars) < 15 or not math.isfinite(price) or price <= 0:
        raise ValueError('Insufficient volatility data')
    ranges = []
    for i in range(len(bars)-14, len(bars)):
        high, low, previous = float(bars[i][2]), float(bars[i][3]), float(bars[i-1][4])
        if not all(math.isfinite(v) and v > 0 for v in (high,low,previous)) or high < low:
            raise ValueError('Invalid volatility data')
        ranges.append(max(high-low,abs(high-previous),abs(low-previous)))
    atr = fmean(ranges)
    liquidation_buffer = (1/leverage-maintenance_rate)*.5
    stop = min(max(1.5*atr/price, .0008), .01, liquidation_buffer)
    take = min(max(3*atr/price, .0026), .02)
    if stop <= 0:
        raise ValueError('No stop room before liquidation')
    return {'atr':atr,'stop_pct':stop,'take_pct':take,'regime':market_regime([float(r[4]) for r in bars])}


def dynamic_leverage_plan(bars, price, margin, fee_rate, slippage,
                          min_leverage, max_leverage, risk_fraction,
                          maintenance_rate, risk_buffer=.001,
                          notional_cap=float('inf'), remaining_risk=float('inf'), volatility_override=None):
    """Choose bounded leverage from ATR stop distance and a margin risk budget."""
    if volatility_override is not None:
        plan = dict(volatility_override)
        if any(not math.isfinite(plan[k]) or plan[k] <= 0 for k in ('atr','stop_pct','take_pct')):
            raise ValueError('Invalid model volatility plan')
    else:
        plan = volatility_plan(bars, price, 1, maintenance_rate)
        plan['stop_pct'] = max(1.5*plan['atr']/price, .0008)
        plan['take_pct'] = max(3*plan['atr']/price, .0026)
    return size_risk_plan(plan,margin,fee_rate,slippage,min_leverage,max_leverage,
                          risk_fraction,maintenance_rate,risk_buffer,notional_cap,remaining_risk)


def size_risk_plan(plan,margin,fee_rate,slippage,min_leverage,max_leverage,
                   risk_fraction,maintenance_rate,risk_buffer=.001,
                   notional_cap=float('inf'),remaining_risk=float('inf')):
    """Identical numerical risk sizing for event replay and paper execution."""
    if (not math.isfinite(margin) or margin <= 0
            or not 1 <= min_leverage <= max_leverage <= 100
            or not math.isfinite(risk_fraction) or not 0 < risk_fraction <= 1
            or any(not math.isfinite(v) or v < 0 for v in (fee_rate,slippage,risk_buffer))
            or math.isnan(notional_cap) or math.isnan(remaining_risk)
            or notional_cap <= 0 or remaining_risk <= 0):
        raise ValueError('Invalid dynamic leverage inputs')
    if plan['stop_pct'] >= .5:
        raise ValueError('Volatility too high for a supported risk plan')
    round_trip_cost = 2 * (fee_rate + slippage)
    risk_budget = min(margin * risk_fraction, remaining_risk)
    risk_per_notional = plan['stop_pct'] + round_trip_cost + risk_buffer
    target_notional = min(risk_budget / risk_per_notional, notional_cap)
    leverage = min(max_leverage, int(math.floor(target_notional / margin)))
    while leverage >= min_leverage:
        liquidation_room = (1/leverage-maintenance_rate)*.5
        if risk_per_notional < liquidation_room:
            break
        leverage -= 1
    if leverage < min_leverage:
        raise ValueError('Risk or liquidity budget cannot support minimum leverage')
    notional = margin * leverage
    return plan | {
        'leverage': leverage,
        'margin': margin,
        'notional': notional,
        'risk_budget': risk_budget,
        'estimated_stop_loss': notional * plan['stop_pct'],
        'estimated_round_trip_cost': notional * round_trip_cost,
        'risk_buffer': risk_buffer,
        'planned_risk_usdt': notional * risk_per_notional,
        'liquidation_room_pct': (1/leverage-maintenance_rate)*.5,
        'sizing_method': 'atr_stop_risk_budget',
    }


def update_dynamic_exit(position, plan, fee_rate, slippage):
    """Stops only tighten; targets follow the last closed candle's ATR."""
    long = position['amount'] > 0
    direction = 1 if long else -1
    entry, mark = position['entry'], position['mark']
    stop_distance = min(plan['stop_pct'], position['stop_pct'])
    initial = entry*(1-direction*position['stop_pct'])
    previous = position.get('stop_price',initial)
    trailing = mark*(1-direction*stop_distance)
    best = max(previous,trailing) if long else min(previous,trailing)
    # Once favorable movement covers costs plus a small cushion, protect break-even.
    costs = 2*(fee_rate+slippage)
    if (mark/entry-1)*direction > costs+.001:
        breakeven = entry*(1+direction*costs)
        best = max(best,breakeven) if long else min(best,breakeven)
    position['stop_price'] = best
    position['take_price'] = entry*(1+direction*plan['take_pct'])
    position['atr'] = plan['atr']
    position['exit_regime'] = plan['regime']
    if (long and mark <= best) or (not long and mark >= best):
        return 'dynamic_stop'
    if (long and mark >= position['take_price']) or (not long and mark <= position['take_price']):
        return 'dynamic_take_profit'
    return None
