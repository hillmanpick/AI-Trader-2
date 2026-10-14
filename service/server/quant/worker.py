"""Run with python -m quant.worker from service/server."""
import fcntl
import logging
import time

import config  # load project .env before settings

from .settings import Settings
from .store import Store, utcnow
from .binance import Binance
from .engine import Engine
from .notifier import Notifier


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    cfg = Settings()
    store = Store(cfg.db_path)
    store.set('config',cfg.public())
    # One process is allowed to trade this journal at a time, including restarts.
    lock = open(cfg.db_path+'.lock','a')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if cfg.observation_only:
        from .observe import run
        run(cfg, store)
        return
    engine = Engine(cfg,store,Binance(cfg))
    notifier = Notifier(cfg,store)
    last_error = None
    while True:
        status,error = 'blocked',None
        try:
            status = engine.step()
            if last_error:
                store.event('服务恢复','行情和账户检查恢复正常')
            last_error = None
        except Exception as exc:
            # All exchange errors are sanitized in Binance.request.
            error = str(exc)[:300]
            if error != last_error:
                logging.error('%s',error)
                store.event('运行告警',error)
                last_error = error
        store.set('worker',{'status':status,'error':error,'heartbeat':utcnow()})
        try:
            notifier.tick(store.get('positions',{}),status)
        except Exception:
            logging.error('Notification processing failed')
        time.sleep(max(cfg.poll_seconds,60 if error else 5))


if __name__ == '__main__':
    main()
