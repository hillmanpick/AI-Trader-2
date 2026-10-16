import sys
from pathlib import Path
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.engine import Engine
from quant.paper import paper_settings, PaperMarket
from quant.settings import Settings
from quant.store import Store
from quant.strategies import market_regime, signal
from quant.strategies import volatility_plan, update_dynamic_exit


@pytest.fixture
def cfg(tmp_path):
    return Settings(db_path=str(tmp_path/'paper.db'), mode='paper', strategy='adaptive',
                    fast=3, slow=10, interval='1m', leverage=10, poll_seconds=5,
                    order_notional=2000, max_notional=6000, max_positions=3,
                    paper_slippage=.0003, boll_deviation=1.5, stop_pct=.008, take_pct=.016)


def test_paper_profile_is_isolated_and_validated(cfg, monkeypatch):
    for key, value in {'STRATEGY':'adaptive','INTERVAL':'1m','LEVERAGE':'10',
                       'POLL_SECONDS':'5','SLIPPAGE_BPS':'3','BOLL_DEVIATION':'1.5'}.items():
        monkeypatch.setenv('QUANT_PAPER_'+key, value)
    base = replace(cfg, mode='paper', strategy='sma_cross', leverage=1, paper_slippage=0,
                   observation_only=True, api_key='private', api_secret='private', enabled=False)
    paper = paper_settings(base)
    assert (paper.strategy, paper.interval, paper.leverage, paper.poll_seconds) == ('adaptive','1m',10,5)
    assert paper.paper_slippage == .0003 and paper.boll_deviation == 1.5
    assert not paper.api_key and not paper.api_secret and not paper.live_enabled
    assert base.leverage == 1 and base.observation_only and not base.enabled
    assert base.strategy == 'sma_cross'
    PaperMarket(paper)


@pytest.mark.parametrize('mode', ['live','testnet'])
def test_adaptive_cannot_run_in_real_exchange_modes(cfg, mode):
    with pytest.raises(ValueError, match='paper-only'):
        replace(cfg, mode=mode).validate()


@pytest.mark.parametrize('change', [{'leverage':101}, {'poll_seconds':1},
                                   {'leverage':0},
                                   {'paper_slippage':float('nan')}, {'paper_slippage':-.01}])
def test_bad_profile_fails_validation(cfg, change):
    with pytest.raises(ValueError):
        replace(cfg, **change).validate()


def test_trend_crossover_and_mirrored_short(cfg):
    bullish = [100-i for i in range(20)]+[130]
    bearish = [200-x for x in bullish]
    assert market_regime(bullish) == 'trend'
    assert signal(bullish, 0, cfg) == 'long'
    assert signal(bearish, 0, cfg) == 'short'
    assert signal(bullish, -1, cfg) == 'close'
    assert signal(bearish, 1, cfg) == 'close'
    assert signal(bearish, 0, replace(cfg, allow_short=False)) == 'hold'


def test_range_entries_and_mean_exit(cfg):
    low = [100,101,100,99]*6+[97]
    high = [200-x for x in low]
    assert market_regime(low) == 'range'
    assert signal(low, 0, cfg) == 'long'
    assert signal(high, 0, cfg) == 'short'
    assert signal([100,101,100,99]*6+[100.5], 1, cfg) == 'close'
    assert signal([100,101,100,99]*6+[99.5], -1, cfg) == 'close'


def test_tiny_ranges_do_not_trade_below_cost_floor(cfg):
    closes = [100+(x-100)*.001 for x in ([100,101,100,99]*6+[97])]
    assert signal(closes, 0, cfg) == 'hold'
    assert signal([100]*40, 0, cfg) == 'hold'
    assert signal([100]*3, 0, cfg) == 'hold'


@pytest.mark.parametrize('side', [1, -1])
def test_slippage_is_adverse_on_entry_and_exit_and_fees_apply(cfg, side):
    db = Store(cfg.db_path)
    db.set('paper_balance', 10000)
    engine = Engine(cfg, db, MagicMock())
    engine.paper_order('XRPUSDT', 20*side, 100, 'open', '1')
    p = db.get('positions')['XRPUSDT']
    entry = 100*(1+side*.0003)
    assert p['entry'] == pytest.approx(entry)
    assert p['leverage'] == 10 and p['stop_pct'] == .008
    assert db.get('account')['available'] == pytest.approx(10000-20*entry*.0005-20*entry/10)
    engine.paper_order('XRPUSDT', -20*side, 101, 'close', '2')
    exit_price = 101*(1-side*.0003)
    expected = 10000+(exit_price-entry)*20*side-20*(entry+exit_price)*.0005
    assert db.get('paper_balance') == pytest.approx(expected)
    assert db.get('positions') == {}


def test_old_position_margin_is_not_retroactively_leveraged(cfg):
    db = Store(cfg.db_path)
    db.set('positions', {'BTCUSDT':{'amount':.02,'entry':50000,'mark':50000,'pnl':0}})
    db.set('paper_balance', 10000)
    Engine(cfg, db, MagicMock()).paper_order('XRPUSDT', 20, 100, 'open', '1')
    account = db.get('account')
    assert account['available'] == pytest.approx(account['equity']-1000-2000*1.0003/10)


def test_initialize_freezes_legacy_risk_before_100x_config_change(cfg):
    cfg = replace(cfg,leverage=100,paper_dynamic_exits=True)
    db = Store(cfg.db_path)
    db.set('config',{'leverage':1,'stop_pct':.03,'take_pct':.06})
    db.set('positions',{'BTCUSDT':{'amount':.02,'entry':50000,'mark':50000,'pnl':0}})
    Engine(cfg,db,MagicMock()).initialize()
    p = db.get('positions')['BTCUSDT']
    assert (p['leverage'],p['initial_margin'],p['stop_pct'],p['take_pct']) == (1,1000,.03,.06)
    assert p['dynamic_exits'] is False
    assert db.get('config')['leverage'] == 100
    # Repeated startup must not reinterpret old collateral using the new profile.
    Engine(cfg,db,MagicMock()).initialize()
    assert db.get('positions')['BTCUSDT'] == p


def test_ten_times_leverage_does_not_multiply_notional(cfg):
    db = Store(cfg.db_path)
    db.set('account', {'equity':10000,'available':10000})
    api = MagicMock()
    api.quantity.return_value = '20'
    engine = Engine(cfg, db, api)
    engine.execute('XRPUSDT', 'long', 100, 'test', '1')
    api.quantity.assert_called_once_with('XRPUSDT', 2000, 100)
    assert db.get('positions')['XRPUSDT']['amount'] == 20
    api.order.assert_not_called()


def test_adaptive_review_has_regime_and_can_use_four_slots(cfg):
    cfg = replace(cfg, ai_enabled=True, deepseek_api_key='test')
    engine = Engine(cfg, Store(cfg.db_path), MagicMock())
    pool = MagicMock()
    engine.review_pool = pool
    closes = [100,101,100,99]*6+[97]
    bars = [[i*60000,0,0,0,x,0,(i+1)*60000-1] for i,x in enumerate(closes)]
    for symbol in ('BTCUSDT','ETHUSDT','DOGEUSDT','XRPUSDT'):
        assert engine.review_entry(symbol, 'long', 97, bars, 1) is True
    candidate = pool.submit.call_args.args[1]
    assert candidate['regime'] == 'range' and candidate['range']['period'] == 13
    assert engine.review_entry('SUIUSDT','long',97,bars,1) is False


def test_busy_review_retries_same_bar_without_losing_candidate(cfg):
    cfg = replace(cfg, symbols=('XRPUSDT',), ai_enabled=True)
    db = Store(cfg.db_path)
    db.set('bar:XRPUSDT', 1)
    api = MagicMock(offset=0, last_sync=10**20)
    api.mark.return_value = 100
    import time
    end = int(time.time()*1000)-1
    api.closed_candles.return_value = [[end-60000,0,0,0,100,0,end] for _ in range(40)]
    engine = Engine(cfg,db,api)
    engine.ready = True
    with patch('quant.engine.signal', return_value='long'), patch.object(engine,'review_entry', return_value=False):
        engine.step()
    assert db.get('bar:XRPUSDT') == 1


def test_margin_sizing_is_2000_times_100_and_observer_stays_unchanged(cfg,monkeypatch):
    for key,value in {'LEVERAGE':'100','MARGIN_USDT':'2000','MAX_MARGIN_USDT':'6000',
                      'STRATEGY':'adaptive','DYNAMIC_EXITS':'true'}.items():
        monkeypatch.setenv('QUANT_PAPER_'+key,value)
    base = replace(cfg, strategy='sma_cross', leverage=1, observation_only=True,enabled=False)
    paper = paper_settings(base)
    assert paper.order_notional == 200000 and paper.max_notional == 600000
    assert paper.paper_margin == 2000 and paper.leverage == 100 and paper.paper_dynamic_exits
    assert base.leverage == 1 and base.observation_only and not base.enabled


@pytest.mark.parametrize('mode',['testnet','live'])
def test_100x_capability_is_simulation_only(cfg,mode):
    with pytest.raises(ValueError, match='stop/take/leverage'):
        replace(cfg,mode=mode,strategy='sma_cross',leverage=100).validate()


def test_exact_margin_reserved_and_total_margin_gate(cfg):
    cfg = replace(cfg, leverage=100,paper_margin=2000,paper_max_margin=6000,
                  order_notional=200000,max_notional=600000,max_positions=4)
    db = Store(cfg.db_path)
    db.set('account', {'equity':10000,'available':10000})
    engine=Engine(cfg,db,MagicMock())
    for i in range(3):
        engine.paper_order(str(i)+'USDT',2000,100,'open',str(i))
    account=db.get('account')
    assert account['equity']-account['available'] == pytest.approx(6000)
    # Even favorable marks lowering gross notional cannot free reserved margin.
    p=db.get('positions')
    for pos in p.values(): pos['mark']=1
    db.set('positions',p)
    engine.execute('NEWUSDT','long',100,'open','new')
    engine.exchange.quantity.assert_not_called()


@pytest.mark.parametrize('side,mark',[(1,99.4),(-1,100.6),(1,95),(-1,105)])
def test_isolated_liquidation_before_other_exits_cannot_draw_other_collateral(cfg,side,mark):
    cfg=replace(cfg,symbols=('BTCUSDT',),leverage=100,paper_margin=2000,paper_max_margin=6000,
                order_notional=200000,max_notional=600000,paper_slippage=0,enabled=False)
    db=Store(cfg.db_path);db.set('paper_balance',10000)
    api=MagicMock(offset=0,last_sync=10**20);api.mark.return_value=mark
    engine=Engine(cfg,db,api);engine.ready=True
    engine.paper_order('BTCUSDT',2000*side,100,'open','1')
    balance=db.get('paper_balance')
    import time
    end=int(time.time()*1000)-1
    api.closed_candles.return_value=[[end-60000,100,101,99,100,0,end] for _ in range(40)]
    engine.step()
    assert not db.get('positions')
    liquidation=db.orders()[0]
    assert 'paper_liquidation' in liquidation['payload']
    assert db.get('paper_balance') >= balance-2000
    assert not api.order.called


def test_atr_plan_scales_with_volatility_and_caps_stop_before_100x_liquidation(cfg):
    bars=[[i,100,101,99,100,1,i+1] for i in range(30)]
    plan=volatility_plan(bars,100,100,.005)
    assert plan['atr'] == 2
    assert plan['stop_pct'] == pytest.approx(.0025)
    assert plan['take_pct'] == .02
    quiet=[[i,100,100.01,99.99,100,1,i+1] for i in range(30)]
    low=volatility_plan(quiet,100,100,.005)
    assert low['stop_pct'] < plan['stop_pct']
    assert low['take_pct'] < plan['take_pct']
    with pytest.raises(ValueError):volatility_plan([],100,100,.005)


@pytest.mark.parametrize('side',[1,-1])
def test_dynamic_stop_only_tightens_and_survives_volatility_expansion(cfg,side):
    p={'amount':side,'entry':100,'mark':100,'stop_pct':.0025}
    plan={'stop_pct':.0025,'take_pct':.02,'atr':1,'regime':'trend'}
    assert update_dynamic_exit(p,plan,.0005,.0003) is None
    first=p['stop_price']
    p['mark']=100*(1+side*.004)
    assert update_dynamic_exit(p,plan,.0005,.0003) is None
    protected=p['stop_price']
    assert (protected-first)*side > 0
    p['mark']=100*(1+side*.003)
    wider=plan|{'stop_pct':.008,'atr':3}
    update_dynamic_exit(p,wider,.0005,.0003)
    assert (p['stop_price']-protected)*side >= 0
    p['mark']=100*(1-side*.001)
    assert update_dynamic_exit(p,plan,.0005,.0003) == 'dynamic_stop'


def test_dynamic_stop_is_used_by_worker_loop(cfg):
    cfg=replace(cfg,symbols=('BTCUSDT',),paper_dynamic_exits=True, enabled=False)
    db=Store(cfg.db_path);api=MagicMock(offset=0,last_sync=10**20)
    engine=Engine(cfg,db,api);engine.ready=True
    plan={'stop_pct':.0025,'take_pct':.02,'atr':.1,'regime':'trend'}
    db.set('volatility:BTCUSDT',plan)
    engine.paper_order('BTCUSDT',1,100,'open','1')
    p=db.get('positions');p['BTCUSDT']['stop_price']=100.1;db.set('positions',p)
    api.mark.return_value=100
    engine.step()
    assert not db.get('positions')
    assert 'dynamic_stop' in db.orders()[0]['payload']
    api.closed_candles.assert_not_called()
