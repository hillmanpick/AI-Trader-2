"""Independent paper configuration and a public-market-only exchange client."""
import os
from dataclasses import replace
from pathlib import Path

from .binance import Binance
from .settings import Settings
from .market_stream import MarketStreamMixin

PUBLIC_PATHS = {'/fapi/v1/time', '/fapi/v1/exchangeInfo', '/fapi/v1/premiumIndex', '/fapi/v1/klines', '/fapi/v1/depth'}


def paper_settings(base):
    path = os.getenv('QUANT_PAPER_DB_PATH', str(Path(base.db_path).with_name('quant-aggressive-paper.db')))
    if Path(path).resolve() == Path(base.db_path).resolve():
        raise ValueError('Paper simulation needs a separate database from real account observation')
    leverage = int(os.getenv('QUANT_PAPER_LEVERAGE', '1'))
    margin = float(os.getenv('QUANT_PAPER_MARGIN_USDT', '0'))
    max_margin = float(os.getenv('QUANT_PAPER_MAX_MARGIN_USDT', '0'))
    dynamic = os.getenv('QUANT_PAPER_DYNAMIC_LEVERAGE','false').lower() == 'true'
    max_leverage = int(os.getenv('QUANT_PAPER_MAX_LEVERAGE', str(leverage)))
    if dynamic:
        leverage = max_leverage
    cfg = replace(base, mode='paper', observation_only=False, live_enabled=False,
                  enabled=os.getenv('QUANT_PAPER_ENABLED', 'false').lower() == 'true',
                  api_key='', api_secret='', community_token='', db_path=path,
                  symbols=tuple(s.strip().upper() for s in os.getenv('QUANT_PAPER_SYMBOLS', 'BTCUSDT,ETHUSDT,SOLUSDT').split(',') if s.strip()),
                  strategy=os.getenv('QUANT_PAPER_STRATEGY', 'sma_cross'), interval=os.getenv('QUANT_PAPER_INTERVAL', '5m'),
                  fast=int(os.getenv('QUANT_PAPER_SMA_FAST', '5')), slow=int(os.getenv('QUANT_PAPER_SMA_SLOW', '15')),
                  ai_min_confidence=float(os.getenv('QUANT_PAPER_AI_MIN_CONFIDENCE', str(base.ai_min_confidence))),
                  ai_review_max_age=int(os.getenv('QUANT_PAPER_AI_REVIEW_MAX_AGE_SECONDS', str(base.ai_review_max_age))),
                  paper_review_relaxed=os.getenv('QUANT_PAPER_AI_RELAXED', 'false').lower() == 'true',
                  order_notional=margin*leverage if margin else float(os.getenv('QUANT_PAPER_ORDER_NOTIONAL_USDT', '2000')),
                  max_notional=(float(os.getenv('QUANT_PAPER_MAX_TOTAL_NOTIONAL_USDT', str(max_margin*leverage)))
                                if dynamic else max_margin*leverage if max_margin else float(os.getenv('QUANT_PAPER_MAX_TOTAL_NOTIONAL_USDT', '6000'))),
                  paper_margin=margin, paper_max_margin=max_margin,
                  paper_maintenance_rate=float(os.getenv('QUANT_PAPER_MAINTENANCE_RATE', '.005')),
                  paper_dynamic_exits=os.getenv('QUANT_PAPER_DYNAMIC_EXITS','false').lower() == 'true',
                  paper_dynamic_leverage=dynamic,
                  paper_min_leverage=int(os.getenv('QUANT_PAPER_MIN_LEVERAGE', str(min(2,max_leverage)))),
                  paper_max_leverage=max_leverage,
                  paper_major_max_leverage=int(os.getenv('QUANT_PAPER_MAJOR_MAX_LEVERAGE', '100')),
                  paper_risk_fraction=float(os.getenv('QUANT_PAPER_RISK_FRACTION', '.10')),
                  paper_risk_buffer=float(os.getenv('QUANT_PAPER_RISK_BUFFER_BPS', '10'))/10000,
                  paper_depth_participation=float(os.getenv('QUANT_PAPER_DEPTH_PARTICIPATION', '.05')),
                  paper_max_spread_bps=float(os.getenv('QUANT_PAPER_MAX_SPREAD_BPS', '10')),
                  paper_depth_range_bps=float(os.getenv('QUANT_PAPER_DEPTH_RANGE_BPS', '10')),
                  paper_entry_filter=os.getenv('QUANT_PAPER_ENTRY_FILTER','shadow'),
                  paper_lab_required=os.getenv('QUANT_PAPER_LAB_REQUIRED','false').lower() == 'true',
                  paper_exploration_mode=os.getenv('QUANT_PAPER_EXPLORATION_MODE','false').lower() == 'true',
                  paper_summary_only=os.getenv('QUANT_PAPER_MAIL_SUMMARY_ONLY','true').lower() == 'true',
                  reflection_enabled=os.getenv('QUANT_PAPER_REFLECTION_ENABLED','true').lower() == 'true',
                  reflection_min_trades=int(os.getenv('QUANT_PAPER_REFLECTION_MIN_TRADES', str(base.reflection_min_trades))),
                  reflection_window=int(os.getenv('QUANT_PAPER_REFLECTION_WINDOW', str(base.reflection_window))),
                  reflection_interval_trades=int(os.getenv('QUANT_PAPER_REFLECTION_INTERVAL_TRADES', str(base.reflection_interval_trades))),
                  max_positions=int(os.getenv('QUANT_PAPER_MAX_POSITIONS', '3')),
                  leverage=leverage,
                  paper_slippage=float(os.getenv('QUANT_PAPER_SLIPPAGE_BPS', '0'))/10000,
                  boll_deviation=float(os.getenv('QUANT_PAPER_BOLL_DEVIATION', str(base.boll_deviation))),
                  stop_pct=float(os.getenv('QUANT_PAPER_STOP_LOSS_PCT', '0.03')),
                  take_pct=float(os.getenv('QUANT_PAPER_TAKE_PROFIT_PCT', '0.06')),
                  daily_loss=0,
                  paper_cash=float(os.getenv('QUANT_PAPER_BALANCE_USDT', '10000')),
                  poll_seconds=int(os.getenv('QUANT_PAPER_POLL_SECONDS', '15')))
    cfg.validate()
    return cfg


class PaperMarket(Binance):
    def __init__(self, cfg):
        if cfg.mode != 'paper' or cfg.live_enabled or cfg.observation_only or cfg.api_key or cfg.api_secret:
            raise ValueError('Paper market requires paper mode with no Binance credentials')
        super().__init__(cfg)

    def request(self, method, path, params=None, signed=False):
        if method != 'GET' or path not in PUBLIC_PATHS or signed:
            raise RuntimeError('Paper market rejects private endpoints and exchange writes')
        return super().request(method, path, params, signed)


class RealtimePaperMarket(MarketStreamMixin, PaperMarket):
    def liquidity(self, symbol):
        from .lab_config import LabConfig
        from .lab_store import RealtimeBus
        cfg = LabConfig()
        if cfg.enabled:
            row = RealtimeBus(cfg.redis_url).get('liquidity:'+symbol)
            if not row or __import__('time').time()*1000-row['updated_ms'] > 2000:
                raise ValueError('Live depth not ready')
            return row
        if self.cfg.paper_lab_required:
            raise ValueError('Lab collection is not enabled')
        from .order_book import DepthBook
        book = DepthBook()
        now = int(__import__('time').time()*1000)
        book.seed(self.request('GET','/fapi/v1/depth',{'symbol':symbol,'limit':1000}),now)
        book.ready = bool(book.bids and book.asks and max(book.bids) < min(book.asks))
        return book.metrics(now,self.cfg.paper_depth_range_bps)
