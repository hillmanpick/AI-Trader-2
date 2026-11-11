"""Credential-free execution assumptions bound to each validation proof."""
import hashlib
import json


def policy_for_settings(cfg, horizon_seconds=300):
    return {k:getattr(cfg,k) for k in (
        'fee_rate','paper_slippage','paper_margin','paper_margin_is_ceiling','paper_max_margin','paper_min_leverage',
        'paper_max_leverage','paper_risk_fraction','paper_risk_buffer','paper_maintenance_rate',
        'paper_major_max_leverage',
        'paper_depth_participation','paper_depth_range_bps','paper_max_spread_bps',
        'max_positions','max_notional','paper_exploration_mode')} | {'horizon_seconds':horizon_seconds,
            'entry_plan_schema':'shared-microstructure-v3-quote-budget',
            'major_symbol_max_leverage':cfg.paper_major_max_leverage,
            'major_symbols':['BTCUSDT','ETHUSDT']}


def microstructure_plan(row, liquidity, policy, positions, maximum, ai_parameters=None):
    """The same bounded entry sizing for research, event replay and paper orders.

    An explicit sizing policy can treat margin as a ceiling independently of
    model approval. Legacy fixed-margin mode still requires the full budget.
    A narrow book never authorizes synthetic liquidity or a relaxed minimum lot.
    The caller supplies the causal covariance-adjusted notional ceiling.
    """
    from .strategies import size_risk_plan
    minimum, maximum_leverage = leverage_bounds(policy, row['symbol'])
    atr = row['meta']['atr_proxy']
    volatility = {'atr':atr*row['price'], 'stop_pct':max(.0008,1.5*atr),
                  'take_pct':max(.0026,3*atr), 'regime':'microstructure'}
    margin = policy['paper_margin']
    if policy.get('paper_margin_is_ceiling',False) or policy.get('paper_exploration_mode', False):
        margin = min(margin, maximum/minimum)
    if ai_parameters:
        margin = min(margin, float(ai_parameters['margin_usdt']))
        maximum_leverage = min(maximum_leverage, int(ai_parameters['leverage']))
        volatility.update(stop_pct=float(ai_parameters['stop_loss_pct']),
                          take_pct=float(ai_parameters['take_profit_pct']), regime='ds_autopilot')
    cost = 2*(policy['fee_rate']+policy['paper_slippage'])+liquidity['spread_bps']/10000
    if volatility['take_pct'] <= cost*1.5:
        raise ValueError('Target does not cover trading costs')
    risk = sum(float(p.get('exit_plan',p.get('plan',{})).get('planned_risk_usdt',
               p.get('initial_margin',p.get('margin',0)))) for p in positions.values())
    return size_risk_plan(volatility,margin,policy['fee_rate'],
        policy['paper_slippage']+liquidity['spread_bps']/20000, minimum,maximum_leverage,
        policy['paper_risk_fraction'],policy['paper_maintenance_rate'],policy['paper_risk_buffer'],
        maximum,policy['paper_max_margin']*policy['paper_risk_fraction']-risk)


def leverage_bounds(policy,symbol):
    cap = int(policy.get('major_symbol_max_leverage',policy['paper_max_leverage'])) if symbol in set(policy.get('major_symbols',())) else policy['paper_max_leverage']
    return policy['paper_min_leverage'],cap


def current_policy(horizon_seconds=300):
    from .settings import Settings
    from .paper import paper_settings
    return policy_for_settings(paper_settings(Settings()),horizon_seconds)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def model_version(model):
    keys = ('schema','features','classes','mean','scale','coef','intercept',
            'vector_model','threshold','label_schema','training_end_ms')
    return fingerprint({k:model.get(k) for k in keys})[:24]
