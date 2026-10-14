"""Independent paper configuration and a public-market-only exchange client."""
import os
from dataclasses import replace
from pathlib import Path

from .binance import Binance
from .settings import Settings
from .market_stream import MarketStreamMixin

PUBLIC_PATHS = {'/fapi/v1/time', '/fapi/v1/exchangeInfo', '/fapi/v1/premiumIndex', '/fapi/v1/klines'}


def paper_settings(base):
    path = os.getenv('QUANT_PAPER_DB_PATH', str(Path(base.db_path).with_name('quant-aggressive-paper.db')))
    if Path(path).resolve() == Path(base.db_path).resolve():
        raise ValueError('Paper simulation needs a separate database from real account observation')
    cfg = replace(base, mode='paper', observation_only=False, live_enabled=False,
                  enabled=os.getenv('QUANT_PAPER_ENABLED', 'false').lower() == 'true',
                  api_key='', api_secret='', community_token='', db_path=path,
                  symbols=tuple(s.strip().upper() for s in os.getenv('QUANT_PAPER_SYMBOLS', 'BTCUSDT,ETHUSDT,SOLUSDT').split(',') if s.strip()),
                  strategy='sma_cross', interval=os.getenv('QUANT_PAPER_INTERVAL', '5m'),
                  fast=int(os.getenv('QUANT_PAPER_SMA_FAST', '5')), slow=int(os.getenv('QUANT_PAPER_SMA_SLOW', '15')),
                  order_notional=float(os.getenv('QUANT_PAPER_ORDER_NOTIONAL_USDT', '2000')),
                  max_notional=float(os.getenv('QUANT_PAPER_MAX_TOTAL_NOTIONAL_USDT', '6000')),
                  max_positions=int(os.getenv('QUANT_PAPER_MAX_POSITIONS', '3')), leverage=1,
                  stop_pct=float(os.getenv('QUANT_PAPER_STOP_LOSS_PCT', '0.03')),
                  take_pct=float(os.getenv('QUANT_PAPER_TAKE_PROFIT_PCT', '0.06')),
                  daily_loss=float(os.getenv('QUANT_PAPER_MAX_DAILY_LOSS_USDT', '500')),
                  paper_cash=float(os.getenv('QUANT_PAPER_BALANCE_USDT', '10000')),
                  poll_seconds=15)
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
    pass
