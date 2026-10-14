"""Virtual fills only. This worker has no Binance account or order access."""
import fcntl
import logging
import time

import config  # load environment before constructing settings

from .binance import ExchangeError
from .engine import Engine
from .notifier import Notifier
from .paper import RealtimePaperMarket, paper_settings
from .settings import Settings
from .store import Store, utcnow


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    base = Settings()
    cfg = paper_settings(base)
    store = Store(cfg.db_path)
    source = Store(base.db_path)
    lock = open(cfg.db_path+'.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    store.set('config', cfg.public())
    market = RealtimePaperMarket(cfg)
    market._start_market_stream()
    engine = Engine(cfg, store, market)
    notifier = Notifier(cfg, store)
    last_error = None
    while True:
        status, error = 'blocked', None
        try:
            # Copy sanitized public community data, never observer balances/positions.
            if cfg.community_enabled:
                store.set('community', source.get('community', {}))
            status = engine.step()
            last_error = None
        except Exception as exc:
            error = str(exc)[:200] if isinstance(exc, ExchangeError) else '模拟行情或策略处理失败，请检查服务日志与配置'
            if error != last_error:
                logging.warning('%s', error)
                store.event('模拟运行告警', error)
                last_error = error
        store.set('worker', {'status': status, 'error': error, 'heartbeat': utcnow(),
                             'market_stream': market.market_stream.snapshot()})
        try:
            notifier.tick(store.get('positions', {}), status)
        except Exception:
            logging.warning('Paper notification processing failed')
        time.sleep(30 if error else cfg.poll_seconds)


if __name__ == '__main__':
    main()
