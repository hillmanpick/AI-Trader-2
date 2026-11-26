"""Frozen, causal rules and shared risk/exit logic. No LLM or exchange writes."""
import math

from .lab_features import FEATURES, SCHEMA
from .execution_policy import policy_for_settings

VERSION = 'systematic-flow-event-v1'
# These hypotheses are fixed before evaluation, not tuned on the test period.
STRATEGIES = ('flow_momentum', 'pullback_resume', 'flow_reversal')
HORIZON_MS = 30000
COOLDOWN_MS = 1000
GRID_MS = 100


def policy_for_quant(cfg):
    return policy_for_settings(cfg, HORIZON_MS // 1000) | {
        'quant_version': VERSION, 'capital': cfg.paper_compound_capital,
        'risk_per_trade': cfg.paper_compound_risk_fraction,
        'horizon_ms': HORIZON_MS, 'cooldown_ms': COOLDOWN_MS,
        'grid_ms': GRID_MS, 'same_direction_limit': None if cfg.paper_unrestricted_positions else cfg.max_positions,
        'paper_unrestricted_positions': cfg.paper_unrestricted_positions,
    }


def signal(name, row, policy):
    """Return evidence and a direction, never an invented win probability."""
    if name not in STRATEGIES:
        raise ValueError('Unknown frozen hypothesis')
    hold = {'action': 'hold', 'side': 0, 'score': 0., 'strategy': name}
    meta, vector = row.get('meta', {}), row.get('vector', [])
    if (not meta.get('quality') or meta.get('schema') != SCHEMA
            or not meta.get('entry_allowed', True) or len(vector) != len(FEATURES)
            or not all(type(v) in (int, float) and math.isfinite(v) for v in vector)):
        return hold | {'reason': '特征质量不完整'}
    fast = row.get('fast', {})
    fast_fields = ('return_250ms','return_1s','trade_imbalance_1s','ofi_1s','microprice_edge_bps')
    if not fast.get('ready') or not all(math.isfinite(fast.get(k, float('nan'))) for k in fast_fields):
        return hold | {'reason': '亚秒级盘口或订单流预热中'}
    f = dict(zip(FEATURES, vector))
    cost = 2 * (policy['fee_rate'] + policy['paper_slippage']) + f['spread_bps'] / 10000
    atr = meta.get('atr_proxy', 0)
    if (not math.isfinite(atr) or not .0001 <= atr < .05
            or not 0 <= f['spread_bps'] <= policy['paper_max_spread_bps']
            or not 0 <= f['latency_ms'] <= 2000):
        return hold | {'reason': '波动、价差或时延不合格'}
    trend = 1 if fast['return_1s'] > 0 else -1
    side = -trend if name == 'flow_reversal' else trend
    evidence = {
        'momentum_30s': side * f['return_30s'],
        'momentum_60s': side * f['return_60s'],
        'order_flow': side * f['ofi'],
        'active_trade_flow': side * f['trade_imbalance'],
        'book_imbalance': side * f['depth_imbalance'],
        'volume_ratio': f['volume_anomaly'], 'round_trip_cost_pct': cost,
        'return_250ms': fast['return_250ms'], 'return_1s': fast['return_1s'],
        'order_flow_1s': fast['ofi_1s'], 'trade_flow_1s': fast['trade_imbalance_1s'],
        'microprice_edge_bps': fast['microprice_edge_bps'],
    }
    if max(abs(f['return_60s']), 3 * atr) < 2.5 * cost:
        return hold | {'reason': '近期波动空间不足以覆盖交易成本', 'evidence': evidence}
    confirmed = (side * fast['ofi_1s'] > .15 and side * fast['trade_imbalance_1s'] > .15
                 and side * fast['microprice_edge_bps'] > 0
                 and side * f['depth_imbalance'] > .03 and f['volume_anomaly'] >= 1.2)
    if name == 'flow_momentum':
        confirmed &= side * fast['return_250ms'] > 0 and side * fast['return_1s'] > 0
    elif name == 'pullback_resume':
        confirmed &= side * f['return_30s'] < 0 and side * f['return_5s'] > 0
    else:
        confirmed &= (abs(fast['return_1s']) > max(2 * cost, atr/10)
                      and side * fast['return_250ms'] > 0 and side * fast['ofi_1s'] > .5
                      and side * fast['trade_imbalance_1s'] > .25)
    if side * fast['return_250ms'] > max(.0005, 2.5 * f['ewma_volatility']):
        confirmed = False
    # Avoid fighting a material BTC move with a correlated altcoin position.
    if row['symbol'] != 'BTCUSDT' and side * f['btc_return_60s'] < -cost:
        confirmed = False
    return {'action': ('long' if side > 0 else 'short') if confirmed else 'hold',
            'side': side if confirmed else 0, 'strategy': name,
            'score': abs(f['return_60s']) / cost + min(3., abs(f['ofi'])) if confirmed else 0.,
            'reason': '方向、主动成交与盘口共同确认' if confirmed else '方向或订单流未共同确认',
            'evidence': evidence}


def risk_plan(row, liquidity, policy, positions, maximum):
    """Size quote exposure from stop risk; margin is a ceiling, not a target."""
    cost = 2 * (policy['fee_rate'] + policy['paper_slippage']) + liquidity['spread_bps'] / 10000
    atr = float(row['meta']['atr_proxy'])
    stop = max(.0015, 1.5 * atr, 1.5 * cost)
    target = max(2 * stop, 3 * cost)
    if not 0 < stop < .05 or target >= .15:
        raise ValueError('Quant volatility outside frozen risk bounds')
    used = sum(p.get('exit_plan', p.get('plan', {})).get('planned_risk_usdt',
               p.get('initial_margin', p.get('margin', 0))) for p in positions.values())
    capital = policy['capital']
    unrestricted = policy.get('paper_unrestricted_positions', False)
    risk = capital * policy['risk_per_trade']
    if not unrestricted:
        risk = min(risk, capital * policy['risk_per_trade'] * 3 - used)
    if risk <= 0:
        raise ValueError('Quant portfolio risk exhausted')
    per_quote = stop + cost + policy['paper_risk_buffer']
    upper = (policy['paper_major_max_leverage'] if row['symbol'] in ('BTCUSDT', 'ETHUSDT')
             else policy['paper_max_leverage'])
    leverage = min(upper, max(policy['paper_min_leverage'], int(.05 / per_quote)))
    while leverage >= policy['paper_min_leverage']:
        if per_quote < (1 / leverage - policy['paper_maintenance_rate']) * .5:
            break
        leverage -= 1
    if leverage < policy['paper_min_leverage']:
        raise ValueError('Insufficient liquidation room')
    used_margin = sum(p.get('initial_margin', p.get('margin', 0)) for p in positions.values())
    notional = min(maximum, risk / per_quote, policy['paper_margin'] * leverage)
    if not unrestricted:
        notional = min(notional, (policy['paper_max_margin'] - used_margin) * leverage)
    if not math.isfinite(notional) or notional < 10:
        raise ValueError('Quant liquidity or collateral insufficient')
    return {'atr': atr * row['price'], 'stop_pct': stop, 'take_pct': target,
            'leverage': leverage, 'notional': notional, 'margin': notional / leverage,
            'planned_risk_usdt': notional * per_quote, 'risk_budget': risk,
            'estimated_stop_loss': notional * stop, 'estimated_round_trip_cost': notional * cost,
            'risk_buffer': policy['paper_risk_buffer'], 'regime': VERSION,
            'horizon_ms': HORIZON_MS, 'sizing_method': 'fixed_fraction_stop_risk',
            'round_trip_cost_pct': cost,
            'funding_rate_abs': abs(float(row['meta'].get('funding_rate', .001)))}


def funding_charge(quantity, entry, plan, opened_ms, closed_ms):
    """Conservative research accrual, not a claim about exchange settlement."""
    rate = plan.get('funding_rate_abs', .001)
    if not math.isfinite(rate) or rate < 0:
        raise ValueError('Invalid simulated funding assumption')
    return abs(quantity) * entry * rate * max(0, closed_ms - opened_ms) / 28800000


def exit_reason(position, now_ms):
    """The identical barrier/trailing rule is called in replay and paper."""
    side = 1 if position['amount'] > 0 else -1
    entry, mark = position['entry'], position['mark']
    plan = position['exit_plan']
    stop = position.get('stop_price', entry * (1 - side * plan['stop_pct']))
    take = entry * (1 + side * plan['take_pct'])
    # Check previously active barriers before moving the stop.
    if side * (mark - stop) <= 0:
        return 'quant_stop'
    if side * (mark - take) >= 0:
        return 'quant_target'
    move = side * (mark / entry - 1)
    if move >= plan['stop_pct']:
        trail = mark * (1 - side * plan['stop_pct'])
        stop = max(stop, trail) if side > 0 else min(stop, trail)
    position['stop_price'], position['take_price'] = stop, take
    if now_ms - position['quant_opened_ms'] >= plan.get('horizon_ms', HORIZON_MS):
        return 'quant_horizon'
    return None
