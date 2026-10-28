import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant.reflection import StrategyReflector
from quant.engine import Engine
from quant.settings import Settings
from quant.store import Store


def trade(symbol, net, cost=1, regime='range'):
    return {
        'symbol': symbol, 'side': 'long', 'strategy_version': 1,
        'regime': regime, 'exit_reason': 'stop_loss', 'hold_seconds': 60,
        'gross_pnl': net + cost, 'cost_usdt': cost, 'net_pnl': net,
    }


def test_reflection_waits_for_samples_and_promotes_cost_filter(tmp_path):
    cfg = Settings(db_path=str(tmp_path / 'reflection.db'), mode='paper',
                   reflection_enabled=True, reflection_min_trades=5,
                   reflection_window=10, reflection_interval_trades=5)
    store = Store(cfg.db_path)
    reflector = StrategyReflector(store, cfg)
    for _ in range(4):
        status = reflector.record_closed(trade('BTCUSDT', -10, 8))
    assert status['active']['version'] == 1
    status = reflector.record_closed(trade('BTCUSDT', -10, 8))
    assert status['active']['version'] == 2
    assert status['active']['overrides']['paper_entry_filter'] == 'enforce'
    assert status['report']['window_trades'] == 5
    assert status['report']['net_pnl'] == -50
    assert status['report']['cost_ratio'] > .5
    assert reflector.apply(cfg).paper_entry_filter == 'enforce'


def test_cost_ratio_uses_absolute_per_trade_gross_moves():
    report = StrategyReflector._report([
        trade('BTCUSDT', 9, 1), trade('ETHUSDT', -11, 1),
    ])
    assert report['gross_pnl'] == 0
    assert report['cost_ratio'] == pytest.approx(2 / 20)


def test_reflection_does_not_apply_to_live_or_change_leverage(tmp_path):
    paper = Settings(db_path=str(tmp_path / 'paper.db'), mode='paper',
                     reflection_enabled=True, reflection_min_trades=5,
                     reflection_window=10, reflection_interval_trades=5,
                     leverage=100)
    store = Store(paper.db_path)
    reflector = StrategyReflector(store, paper)
    for _ in range(5):
        reflector.record_closed(trade('ETHUSDT', -10, 8))
    live = replace(paper, mode='live', leverage=20, reflection_enabled=True,
                   api_key='key', api_secret='secret', paper_entry_filter='shadow')
    applied = StrategyReflector(store, live).apply(live)
    assert applied.mode == 'live'
    assert applied.leverage == 20
    assert applied.paper_entry_filter == 'shadow'


def test_reflection_cools_down_weak_symbols_and_automatically_expires(tmp_path):
    cfg = Settings(db_path=str(tmp_path / 'reflection.db'), mode='paper',
                   symbols=('BTCUSDT', 'ETHUSDT'), reflection_enabled=True,
                   reflection_min_trades=5, reflection_window=20,
                   reflection_interval_trades=5)
    store = Store(cfg.db_path)
    reflector = StrategyReflector(store, cfg)
    for _ in range(10):
        status = reflector.record_closed(trade('BTCUSDT', -2, 1))
    assert status['active']['overrides']['blocked_until'].get('BTCUSDT')
    assert not reflector.allows('BTCUSDT')
    assert reflector.allows('ETHUSDT')

    expired = {'BTCUSDT': '2000-01-01T00:00:00+00:00'}
    store.set('reflection:active', {'version': 2, 'overrides': {'blocked_until': expired}})
    assert reflector.allows('BTCUSDT')


def test_reflection_preserves_cost_filter_and_does_not_extend_active_cooldown(tmp_path):
    cfg = Settings(db_path=str(tmp_path / 'reflection.db'), mode='paper',
                   symbols=('BTCUSDT', 'ETHUSDT'), reflection_enabled=True,
                   reflection_min_trades=5, reflection_window=20,
                   reflection_interval_trades=5)
    store = Store(cfg.db_path)
    reflector = StrategyReflector(store, cfg)
    expires = '2999-01-01T00:00:00+00:00'
    active = {'version': 3, 'overrides': {
        'paper_entry_filter': 'enforce', 'blocked_until': {'BTCUSDT': expires}}}
    report = reflector._report([trade('BTCUSDT', -2, 1)] * 10)
    candidate = reflector._candidate(report, active)
    assert candidate is None
    assert reflector._candidate({'window_trades': 20, 'net_pnl': -1, 'cost_ratio': .8,
                                 'by_symbol': {}}, active) is None


def test_reflection_counter_continues_after_trade_journal_is_bounded(tmp_path):
    cfg = Settings(db_path=str(tmp_path / 'reflection.db'), mode='paper',
                   reflection_enabled=True, reflection_min_trades=5,
                   reflection_window=10, reflection_interval_trades=1)
    store = Store(cfg.db_path)
    store.set('reflection:trades', [trade('ETHUSDT', 1)] * 200)
    store.set('reflection:trade_count', 200)
    store.set('reflection:last_evaluated_count', 200)
    StrategyReflector(store, cfg).record_closed(trade('BTCUSDT', 1))
    assert store.get('reflection:trade_count') == 201
    assert store.get('reflection:last_evaluated_count') == 201


def test_paper_close_writes_net_trade_attribution(tmp_path):
    cfg = Settings(db_path=str(tmp_path / 'paper.db'), mode='paper',
                   reflection_enabled=True, reflection_min_trades=5,
                   reflection_window=10, reflection_interval_trades=5,
                   fee_rate=.0005, paper_slippage=0)
    store = Store(cfg.db_path)
    store.set('paper_balance', 10_000)
    engine = Engine(cfg, store, object())
    engine.paper_order('BTCUSDT', 10, 100, 'open', 'open-1')
    engine.paper_order('BTCUSDT', -10, 101, 'take_profit', 'close-1')
    status = store.get('reflection:status')
    assert status['report']['window_trades'] == 1
    assert status['last_trade']['gross_pnl'] == 10
    assert status['last_trade']['cost_usdt'] == pytest.approx(1.005)
    assert status['last_trade']['net_pnl'] == pytest.approx(8.995)
