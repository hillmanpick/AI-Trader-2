"""Observe the real USD-M account and analyze markets without creating an Engine."""
import math
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from .ai_review import DeepSeekAdvisor
from .binance import Binance, ExchangeError
from .community import age_seconds
from .notifier import Notifier
from .store import utcnow
from .strategies import signal
from .features import indicators
from .market_stream import MarketStreamMixin

READ_PATHS = {'/fapi/v1/time', '/fapi/v3/account', '/fapi/v3/positionRisk',
              '/fapi/v1/premiumIndex', '/fapi/v1/klines'}


class ReadOnlyBinance(Binance):
    def __init__(self, cfg):
        # A paper/testnet execution configuration must not redirect real account reads.
        super().__init__(replace(cfg, mode='live', observation_only=True, enabled=False, live_enabled=False))

    def request(self, method, path, params=None, signed=False):
        if method != 'GET' or path not in READ_PATHS:
            raise RuntimeError('Read-only account client rejects this endpoint')
        return super().request(method, path, params, signed)


class RealtimeReadOnlyBinance(MarketStreamMixin, ReadOnlyBinance):
    pass


def numeric(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('Invalid exchange numeric value')
    return result


def snapshot(account, rows):
    summary = {target: numeric(account.get(source, 0)) for target, source in {
        'wallet': 'totalWalletBalance', 'equity': 'totalMarginBalance', 'available': 'availableBalance',
        'unrealized': 'totalUnrealizedProfit', 'initial_margin': 'totalInitialMargin',
        'maintenance_margin': 'totalMaintMargin',
    }.items()}
    summary['margin_ratio'] = summary['maintenance_margin']/summary['equity'] if summary['equity'] > 0 else None
    assets = [{key: row.get(key) for key in ('asset', 'walletBalance', 'unrealizedProfit', 'marginBalance', 'availableBalance')}
              for row in account.get('assets', []) if numeric(row.get('walletBalance', 0)) or numeric(row.get('unrealizedProfit', 0))]
    positions = []
    for row in rows:
        amount = numeric(row['positionAmt'])
        if not amount:
            continue
        positions.append({
            'symbol': str(row['symbol'])[:30], 'position_side': str(row.get('positionSide', 'BOTH'))[:10],
            'amount': amount, 'entry': numeric(row['entryPrice']), 'mark': numeric(row['markPrice']),
            'pnl': numeric(row['unRealizedProfit']), 'liquidation': numeric(row.get('liquidationPrice', 0)),
            'notional': numeric(row.get('notional', amount*numeric(row['markPrice']))),
            'initial_margin': numeric(row.get('positionInitialMargin', row.get('initialMargin', 0))),
            'isolated_wallet': numeric(row.get('isolatedWallet', 0)),
            'margin_type': 'isolated' if numeric(row.get('isolatedWallet', 0)) else 'cross',
        })
    return {'status': 'ok', 'source': 'Binance USDⓈ-M production', 'read_only': True,
            'account': summary, 'assets': assets, 'positions': positions,
            'updated_at': utcnow(), 'checked_at': utcnow()}


def collect(cfg, store, api):
    if not (cfg.api_key and cfg.api_secret):
        raise RuntimeError('币安实盘 API Key/Secret 尚未配置')
    if api.last_sync < time.time()-1800:
        api.sync_time()
    # Read the entire real account, including manual and hedge positions; never adopt ownership.
    value = snapshot(api.account(), api.positions())
    previous = store.get('live_account', {})
    if previous.get('updated_at'):
        before = {(p['symbol'], p['position_side']): p['amount'] for p in previous.get('positions', [])}
        after = {(p['symbol'], p['position_side']): p['amount'] for p in value['positions']}
        for key in before.keys() | after.keys():
            old, new = before.get(key, 0), after.get(key, 0)
            if old != new:
                kind = '开仓' if not old else '清仓' if not new else '持仓变更'
                store.event(kind, f'币安实盘观察（非系统下单）\n{key[0]} / {key[1]}：{old} → {new}')
    store.set('live_account', value)
    return value


def advice(cfg, store, market_api=None):
    api = market_api or ReadOnlyBinance(cfg)
    advisor = DeepSeekAdvisor(cfg)
    live = store.get('live_account', {})
    if live.get('status') != 'ok' or age_seconds(live.get('updated_at')) > 90:
        return {'status': 'error', 'reason': '实盘账户数据过期或不可用，暂不生成分析', 'at': utcnow()}
    result = {'model': cfg.deepseek_model, 'status': 'complete', 'read_only': True,
              'at': utcnow(), 'account_as_of': live['updated_at'], 'symbols': {}}
    # Avoid sending account balances, identifiers, or credentials to the model.
    community = store.get('community', {}) if cfg.community_enabled else {}
    try:
        api.sync_time()
        for symbol in cfg.symbols[:6]:
            bars = api.closed_candles(symbol)
            price = api.mark(symbol)
            closes = [numeric(row[4]) for row in bars[-max(40, cfg.slow+2, cfg.boll_period):]]
            minimum = max(cfg.slow+2, cfg.boll_period)
            if len(closes) < minimum or price <= 0 or age_seconds(live.get('updated_at')) > 120:
                raise ValueError('Market data unavailable')
            relevant = [p for p in live['positions'] if p['symbol'] == symbol]
            context = {
                'symbol': symbol, 'interval': cfg.interval, 'price': price, 'closed_prices': closes,
                'indicators': indicators(closes,cfg,bars),
                'candle_closed_at': int(bars[-1][6]), 'strategy': cfg.strategy,
                'rule_candidate': signal(closes, 0, cfg),
                'position_context': [{'side': 'long' if p['amount'] > 0 else 'short', 'entry': p['entry'],
                                      'move_percent': (price/p['entry']-1)*100*(1 if p['amount'] > 0 else -1) if p['entry'] > 0 else None}
                                     for p in relevant],
            }
            decision = advisor.analyze(context, community)
            result['symbols'][symbol] = decision | {'price': price, 'rule_candidate': context['rule_candidate']}
            if decision['status'] != 'complete':
                result['status'] = 'partial'
    except Exception:
        result.update(status='error', reason='行情或模型暂时不可用，本次分析未完成')
    finally:
        if market_api is None:
            api.session.close()
    result['completed_at'] = utcnow()
    return result


def run(cfg, store):
    cfg.validate()
    api = RealtimeReadOnlyBinance(cfg)
    api._start_market_stream()
    notifier = Notifier(replace(cfg, mode='live'), store)
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='market-advice')
    future, next_advice = None, 0
    while True:
        error = None
        live = None
        try:
            live = collect(cfg, store, api)
            if future is not None and future.done():
                try:
                    value = future.result()
                except Exception:
                    value = {'status': 'error', 'reason': 'AI 分析任务失败', 'at': utcnow()}
                store.set('ai_advice', value)
                future = None
            if cfg.ai_enabled and future is None and time.monotonic() >= next_advice:
                store.set('ai_advice_job', {'status': 'pending', 'started_at': utcnow()})
                future = pool.submit(advice, cfg, store, api)
                next_advice = time.monotonic()+cfg.ai_advice_seconds
            if future is None:
                store.set('ai_advice_job', {'status': 'idle'})
        except Exception as exc:
            if isinstance(exc, ExchangeError):
                error = str(exc)
            else:
                error = '币安实盘只读同步失败，请检查密钥权限或网络连接'
            previous = store.get('live_account', {})
            store.set('live_account', previous | {'status': 'error', 'error': error, 'checked_at': utcnow(), 'read_only': True})
        store.set('worker', {'status': 'observing' if not error else 'observation_error', 'error': error, 'heartbeat': utcnow(),
                             'market_stream': api.market_stream.snapshot()})
        if not error and live is not None:
            positions = {f"{p['symbol']} / {p['position_side']}": p for p in live['positions']}
            # A mail failure must not invalidate a successful account snapshot.
            try:
                notifier.tick(positions, 'observing', account_snapshot=live['account'], slot_namespace='live_observation')
            except Exception:
                logging.warning('Observation notification processing failed')
        time.sleep(30 if error else max(15, cfg.poll_seconds))
