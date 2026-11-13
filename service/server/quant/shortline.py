"""Experimental paper execution rules; a filter is not a profitability proof."""
import math

from .lab_features import FEATURES

VERSION = 'ai-plan-flow-v1'
STATS_KEY = 'shortline:' + VERSION


def entry_check(row, action, parameters, cfg):
    if action not in ('long','short'):
        return {'passes':False,'reason':'非开仓方向'}
    values = dict(zip(FEATURES, row.get('vector', ())))
    if len(values) != len(FEATURES) or not all(math.isfinite(v) for v in values.values()):
        return {'passes': False, 'reason': '短线特征不完整'}
    side = 1 if action == 'long' else -1
    cost = 2 * (cfg.fee_rate + cfg.paper_slippage) + max(0, values['spread_bps']) / 10000
    stop, target = parameters['stop_loss_pct'], parameters['take_profit_pct']
    atr = row['meta']['atr_proxy']
    if (not all(math.isfinite(v) and v>0 for v in (stop,target,atr))
            or values['spread_bps']<0):
        return {'passes':False,'reason':'波动、成本或保护参数无效'}
    # This is an observed volatility budget, NOT a forecast or win probability.
    space = min(target, 3 * atr)
    net_rr = (target - cost) / (stop + cost)
    reason = None
    if space < 2.5 * cost or net_rr < 1.2:
        reason = '观察到的波动空间或扣费后盈亏比不足'
    elif side * values['return_30s'] <= 0 or side * values['return_60s'] <= 0:
        reason = '30秒与60秒走势尚未支持AI方向'
    elif side * values['ofi'] <= 0 or side * values['trade_imbalance'] <= 0:
        reason = '盘口与主动成交流尚未同时确认'
    elif values['volume_anomaly'] < 1:
        reason = '成交量低于近期基线'
    elif side * values['return_5s'] > max(.001, 3 * values['ewma_volatility']):
        reason = '短时冲击过大，等待回踩而非追价'
    return {'passes': reason is None, 'reason': reason or '方向、订单流和成本过滤通过',
            'round_trip_cost_pct': cost, 'observed_move_budget_pct': space,
            'net_reward_risk': net_rr, 'validated_edge': False, 'version': VERSION}


def compound_budget(cfg, store, positions):
    """Reinvest this policy's settled PnL; deposits and old strategies add nothing."""
    stats = store.get(STATS_KEY, {})
    capital = cfg.paper_compound_capital
    open_loss = sum(min(0., p.get('pnl', 0)) - p.get('entry_fee', 0)
                    for p in positions.values() if p.get('shortline_version') == VERSION)
    net = float(stats.get('net', 0))
    usable = max(0., capital + net + open_loss)
    peak = capital + max(0., float(stats.get('peak_net', 0)))
    drawdown = max(0., (peak - usable) / peak)
    # Reduce continuously under drawdown; never increase size after a loss.
    multiplier = min(2., usable / capital) * max(.25, 1 - 5 * drawdown)
    return {'version': VERSION, 'base_capital': capital, 'realized_net': net,
            'open_loss_and_entry_fees': open_loss, 'usable_capital': usable,
            'multiplier': multiplier, 'drawdown': drawdown,
            'per_trade_risk_usdt': min(usable * .005, capital * .005 * multiplier),
            'portfolio_risk_usdt': min(usable * .015, capital * .015 * multiplier),
            'source': 'settled_policy_pnl_excluding_deposits', 'profitability_proven': False}


def compound_plan(policy, parameters, budget, positions, maximum, liquidity):
    """Apply original absolute ceilings in addition to the compounding budget."""
    policy, parameters = dict(policy), dict(parameters)
    cost = 2 * (policy['fee_rate'] + policy['paper_slippage']) + liquidity['spread_bps'] / 10000
    risk_per_quote = parameters['stop_loss_pct'] + cost + policy['paper_risk_buffer']
    risk_used = sum(float(p.get('exit_plan', {}).get('planned_risk_usdt', p.get('initial_margin', 0)))
                    for p in positions.values())
    risk_available = min(budget['per_trade_risk_usdt'], budget['portfolio_risk_usdt'] - risk_used)
    if risk_available <= 0:
        raise ValueError('复投组合风险预算已占用，等待现有仓位退出')
    maximum = min(maximum, risk_available / risk_per_quote)
    used_margin = sum(p.get('initial_margin', 0) for p in positions.values())
    margin = min(policy['paper_margin'], policy['paper_max_margin'] - used_margin,
                 parameters['margin_usdt'] * budget['multiplier'], maximum / policy['paper_min_leverage'])
    if margin <= 0:
        raise ValueError('复投保证金不足')
    policy['paper_margin'] = margin
    parameters['margin_usdt'] = margin
    # The shared risk sizing must honor the smaller risk even when margin shrinks.
    policy['paper_risk_fraction'] = min(policy['paper_risk_fraction'], risk_available / margin)
    return policy, parameters, maximum


def protected_exit_price(position, fee_rate, slippage):
    """Estimate a net-positive trigger after entry fee and adverse exit costs."""
    quantity = abs(position['amount'])
    spread = position.get('exit_plan', {}).get('liquidity', {}).get('spread_bps', 0) / 20000
    adverse = max(0, slippage) + max(0, spread)
    cushion = position['entry'] * .0002
    entry_fee = position.get('entry_fee', quantity * position['entry'] * fee_rate) / quantity
    if position['amount'] > 0:
        return (position['entry'] + entry_fee + cushion) / ((1 - fee_rate) * (1 - adverse))
    return (position['entry'] - entry_fee - cushion) / ((1 + fee_rate) * (1 + adverse))
