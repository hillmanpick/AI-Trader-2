import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.execution_policy import fingerprint
from quant.lab_features import FEATURES, SCHEMA
from quant.quant_research import artifact_status, evaluate, qualification
from quant.quant_strategy import HORIZON_MS, STRATEGIES, policy_for_quant, signal
from quant.settings import Settings


def sample_row(ts=1000, vector=None, symbol='ALTUSDT'):
    values = [0.] * len(FEATURES) if vector is None else vector
    return {'symbol': symbol, 'ts': ts, 'price': 100., 'vector': values,
            'fast': {'return_250ms':.0001,'return_1s':.001,'trade_imbalance_1s':.5,
                     'ofi_1s':1.,'ofi_1s_events':8,'microprice_edge_bps':.1,'ready':True},
            'meta': {'quality': True, 'schema': SCHEMA, 'entry_allowed': True,
                     'bid': 99.99, 'ask': 100.01, 'bid_depth': 100000.,
                     'ask_depth': 100000., 'spread_bps': 2., 'atr_proxy': .002,
                     'filters': [{'filterType': 'LOT_SIZE', 'minQty': '0.001', 'stepSize': '0.001'},
                                 {'filterType': 'MIN_NOTIONAL', 'notional': '5'}]}}


def test_signal_requires_independent_flow_price_volume_and_cost_confirmation():
    cfg = Settings(mode='paper')
    policy = policy_for_quant(cfg)
    values = dict.fromkeys(FEATURES, 0.)
    values.update(return_5s=.0001, return_30s=.001, return_60s=.002,
                  ewma_volatility=.001, spread_bps=1., depth_imbalance=.4,
                  ofi=1., trade_imbalance=.5, volume_anomaly=2.,
                  btc_return_60s=.001, latency_ms=10.)
    row = sample_row(vector=[values[k] for k in FEATURES])
    assert signal('flow_momentum', row, policy)['action'] == 'long'
    row['fast']['ready'] = False
    assert signal('flow_momentum', row, policy)['reason'] == '亚秒级盘口或订单流预热中'
    row['fast']['ready'] = True
    row['fast']['trade_imbalance_1s'] = -.5
    assert signal('flow_momentum', row, policy)['action'] == 'hold'
    row['fast']['trade_imbalance_1s'] = .5
    row['fast']['trade_imbalance_1s'] = -.5
    assert signal('flow_momentum', row, policy)['action'] == 'hold'
    with pytest.raises(ValueError, match='Unknown frozen hypothesis'):
        signal('unfrozen_candidate', row, policy)


def test_signal_is_fail_closed_for_stale_schema_and_insufficient_move():
    cfg = Settings(mode='paper')
    policy = policy_for_quant(cfg)
    row = sample_row()
    assert signal(STRATEGIES[0], row, policy)['action'] == 'hold'
    row['meta']['schema'] = 'wrong'
    assert signal(STRATEGIES[0], row, policy)['reason'] == '特征质量不完整'


def test_systematic_mode_rejects_ds_and_exploration_overrides():
    invalid = Settings(mode='paper', strategy='lab_model', paper_lab_required=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True, paper_quant_system=True,
        paper_ai_autopilot=True)
    with pytest.raises(ValueError, match='Systematic execution'):
        invalid.validate()
    valid = Settings(mode='paper', strategy='lab_model', paper_lab_required=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True, paper_quant_system=True,
        reflection_enabled=False, paper_margin=2000, paper_max_margin=6000,
        paper_margin_is_ceiling=True, paper_max_leverage=20, leverage=20,
        paper_maintenance_rate=.005, paper_compound_risk_fraction=.001)
    valid.validate()


def test_systematic_exploration_is_paper_only_and_cannot_be_live():
    base = dict(mode='paper', strategy='lab_model', paper_lab_required=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True, paper_quant_system=True,
        paper_quant_exploration=True, reflection_enabled=False, paper_margin=2000,
        paper_max_margin=6000, paper_margin_is_ceiling=True, paper_max_leverage=20,
        leverage=20, paper_maintenance_rate=.005, paper_compound_risk_fraction=.001)
    Settings(**base).validate()
    with pytest.raises(ValueError):
        Settings(**(base | {'mode':'live','api_key':'key','api_secret':'secret'})).validate()


def test_quant_controller_can_run_preregistered_trial_without_approval(tmp_path):
    from unittest.mock import MagicMock
    import time
    from quant.quant_runtime import QuantController, EXPLORATION_STRATEGY
    cfg = Settings(mode='paper', strategy='lab_model', paper_lab_required=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True, paper_quant_system=True,
        paper_quant_exploration=True, reflection_enabled=False, paper_margin=2000,
        paper_max_margin=6000, paper_margin_is_ceiling=True, paper_max_leverage=20,
        leverage=20, paper_maintenance_rate=.005, paper_compound_risk_fraction=.001)
    engine = MagicMock()
    engine.cfg, engine.store = cfg, MagicMock()
    engine.store.get.side_effect = lambda key, default=None: default
    engine.store.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    engine.paper_execution.journal.orders.return_value = []
    lab = MagicMock()
    lab.cfg.directory = str(tmp_path)
    row = sample_row(int(time.time()*1000))
    values = dict.fromkeys(FEATURES, 0.)
    values.update(return_5s=.0001, return_30s=.001, return_60s=.002,
                  ewma_volatility=.001, spread_bps=1., depth_imbalance=.4,
                  ofi=1., trade_imbalance=.5, volume_anomaly=2.,
                  btc_return_60s=.001, latency_ms=10.)
    row['vector'] = [values[k] for k in FEATURES]
    lab.row.return_value, lab.gate.return_value = row, True
    controller = QuantController(engine, lab)
    action = controller.signal('ALTUSDT')
    assert action == 'long'
    status = engine.store.set.call_args_list[0].args[1]
    assert status['ready'] and not status['paper_qualified'] and status['exploration']
    assert controller.report == {}
    assert controller.status['trial_strategy'] == EXPLORATION_STRATEGY


def test_quant_hold_signal_skips_entry_only_gate_and_pending_order_scan(tmp_path, monkeypatch):
    from unittest.mock import MagicMock
    import time
    from quant.quant_runtime import QuantController
    cfg = Settings(mode='paper', strategy='lab_model', paper_lab_required=True,
        paper_dynamic_leverage=True, paper_dynamic_exits=True, paper_quant_system=True,
        paper_quant_exploration=True, reflection_enabled=False, paper_margin=2000,
        paper_max_margin=6000, paper_margin_is_ceiling=True, paper_max_leverage=20,
        leverage=20, paper_maintenance_rate=.005, paper_compound_risk_fraction=.001)
    engine = MagicMock()
    engine.cfg, engine.store = cfg, MagicMock()
    engine.store.get.side_effect = lambda key, default=None: default
    engine.store.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
    lab = MagicMock()
    row = sample_row(int(time.time()*1000))
    lab.row.return_value = row
    controller = QuantController(engine, lab)
    monkeypatch.setattr('quant.quant_runtime.signal', lambda name, row, policy: {
        'action':'hold', 'reason':'无方向确认', 'strategy':name})

    assert controller.signal('ALTUSDT') == 'hold'
    lab.gate.assert_not_called()
    engine.paper_execution.journal.orders.assert_not_called()


def test_research_requires_purged_time_ordered_cost_stress_and_rejects_flat_history():
    start = 1_760_000_000_000
    rows = [sample_row(start + i * 30000) for i in range(2881)]
    cfg = Settings(mode='paper')
    policy = policy_for_quant(cfg)
    report = evaluate(rows, policy, {'sha256': 'fixture', 'quotes': len(rows)}, now_ms=rows[-1]['ts'] + 1000)
    assert report['selected'] is None
    assert report['status'] == 'rejected'
    assert not report['qualification']['passed']
    assert report['folds'] == []
    assert report['paper_only'] and not report['profitability_proven']
    rows[2]['ts'] = rows[1]['ts']
    with pytest.raises(ValueError, match='chronological'):
        evaluate(rows, policy, {}, now_ms=rows[-1]['ts'] + 1000)


def test_research_artifact_requires_current_code_and_exact_policy(tmp_path):
    cfg = Settings(mode='paper')
    policy = policy_for_quant(cfg)
    assert not artifact_status(tmp_path, policy, now_ms=1_800_000_000_000)['ready']


def test_promotion_needs_all_three_oos_folds_stress_and_block_confidence():
    metrics = {'valid': True, 'trades': 30, 'net_pnl': 50., 'profit_factor': 1.3, 'max_drawdown': 20.}
    report = {'selected': STRATEGIES[0], 'policy': {'capital': 6000},
              'folds': [{'test': metrics, 'stress': metrics} for _ in range(3)],
              'oos': {'trades': 90, 'span_hours': 72},
              'confidence': {'blocks': 24, 'lower_mean_usdt': 0.01}}
    assert qualification(report)['passed']
    report['folds'][1]['stress'] = metrics | {'net_pnl': -1.}
    assert not qualification(report)['passed']
