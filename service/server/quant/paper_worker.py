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
    market = RealtimePaperMarket(cfg)
    market._start_market_stream()
    engine = Engine(cfg, store, market)
    lab = None
    if cfg.paper_lab_required:
        from .lab_execution import LabExecution
        lab = LabExecution(engine)
        engine.lab_gate = lab.gate
        engine.lab_signal = lab.signal
        engine.lab_row = lab.row
        engine.lab_risk_cap = lab.risk_cap
        from .paper_execution import PaperExecution
        engine.paper_execution = PaperExecution(engine,lab.bus)
        lab.start_event_listener(market.market_stream.data_ready)
    notifier = Notifier(cfg, store)
    last_error = None
    while True:
        status, error = 'blocked', None
        try:
            # Copy sanitized public community data, never observer balances/positions.
            if cfg.community_enabled:
                store.set('community', source.get('community', {}))
            if lab:
                lab.refresh()
                engine.paper_execution.tick()
                if lab.autopilot:
                    lab.autopilot.tick()
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
        if error:
            time.sleep(30)
        elif lab:
            # Wake on public-market events, with a one-second bound for exits.
            market.market_stream.data_ready.wait(timeout=1)
            market.market_stream.data_ready.clear()
            time.sleep(.2)
        else:
            time.sleep(cfg.poll_seconds)


if __name__ == '__main__':
    main()
