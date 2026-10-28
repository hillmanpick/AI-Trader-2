"""Credential-free execution assumptions bound to each validation proof."""
import hashlib
import json


def policy_for_settings(cfg, horizon_seconds=300):
    return {k:getattr(cfg,k) for k in (
        'fee_rate','paper_slippage','paper_margin','paper_max_margin','paper_min_leverage',
        'paper_max_leverage','paper_risk_fraction','paper_risk_buffer','paper_maintenance_rate',
        'paper_major_max_leverage',
        'paper_depth_participation','paper_depth_range_bps','paper_max_spread_bps',
        'max_positions','max_notional')} | {'horizon_seconds':horizon_seconds,
            'major_symbol_max_leverage':cfg.paper_major_max_leverage,
            'major_symbols':['BTCUSDT','ETHUSDT']}


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
