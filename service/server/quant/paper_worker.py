"""Virtual fills only. This worker has no Binance account or order access."""
import fcntl
import logging
import os
import time

import config  # load environment before constructing settings

from .binance import ExchangeError
from .engine import Engine
from .notifier import Notifier
from .paper import RealtimePaperMarket, paper_settings
from .settings import Settings
from .store import Store, utcnow
from .rest_guard import RestCooldown
from .execution_runtime import CycleSnapshotBus, LatestNotification, DiagnosticBatchStore


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
        if lab.quant and os.getenv('QUANT_COMPUTE_ENABLED', 'false').lower() == 'true':
            from .compute_bridge import install
            install(lab.quant, enabled=True)
        from .paper_trial import sync_forward_trial
        sync_forward_trial(store,cfg)
        lab.start_event_listener(market.market_stream.data_ready)
    notifier = Notifier(cfg, store)
    telemetry = engine.paper_execution.telemetry if lab else None
    diagnostics = DiagnosticBatchStore(store,cfg,telemetry) if lab else None
    if diagnostics:
        engine.store = diagnostics
        engine.reflection.store = diagnostics
    # Matching retains the direct Redis bus. Only strategy reads share a
    # per-scan MGET snapshot and its original freshness checks.
    cycle_bus = CycleSnapshotBus(lab.bus,telemetry) if lab else None
    if cycle_bus:
        # Strategy decisions share one coherent scan snapshot. Sizing and
        # matching independently read the latest executable depth at submit.
        market.execution_bus = lab.bus
        lab.bus = cycle_bus
    notifications = LatestNotification(notifier,telemetry)
    last_error = None
    last_heartbeat = 0.
    while True:
        status, error = 'blocked', None
        try:
            if lab:
                market.market_stream.data_ready.clear()
            # Copy sanitized public community data, never observer balances/positions.
            if cfg.community_enabled:
                store.set('community', source.get('community', {}))
            if lab:
                engine.paper_execution.tick()
                if engine.paper_execution.journal.orders(active=True):
                    # A 20 ms acknowledgement must be serviced before slow
                    # strategy/REST/reporting work, not after its IOC expires.
                    time.sleep(.025)
                    engine.paper_execution.tick()
                try:
                    lab.refresh()
                    if lab.autopilot:
                        lab.autopilot.tick()
                except Exception:
                    # Entry analysis outages cannot interrupt existing protection.
                    logging.warning('Paper entry analysis unavailable; protection continues')
            started = time.perf_counter_ns()
            try:
                if cycle_bus:
                    cycle_bus.begin(engine.cfg.symbols)
                if diagnostics:
                    diagnostics.begin()
                status = engine.step()
            finally:
                if diagnostics:
                    diagnostics.end()
                if cycle_bus:
                    cycle_bus.end()
                if diagnostics:
                    diagnostics.flush()
                if telemetry:
                    telemetry.observe('engine_scan',(time.perf_counter_ns()-started)/1e6)
            if lab:
                # Orders are created inside engine.step(). Match them against
                # the same fresh depth snapshot before an IOC acknowledgement
                # window can expire while the next loop is doing REST work.
                engine.paper_execution.tick()
                if engine.paper_execution.journal.orders(active=True):
                    time.sleep(.025)
                    engine.paper_execution.tick()
                if diagnostics:
                    diagnostics.flush()
            last_error = None
        except Exception as exc:
            error = str(exc)[:200] if isinstance(exc, (ExchangeError, RestCooldown)) else '模拟行情或策略处理失败，请检查服务日志与配置'
            if error != last_error:
                logging.exception('%s', error)
                store.event('模拟运行告警', error)
                last_error = error
        if time.monotonic()-last_heartbeat >= 1 or error:
            store.set('worker', {'status': status, 'error': error, 'heartbeat': utcnow(),
                                 'market_stream': market.market_stream.snapshot(),
                                 'execution_runtime':{'enabled':bool(lab),'real_execution_enabled':False}})
            notifications.submit(store.get('positions', {}),status)
            last_heartbeat = time.monotonic()
        if error:
            time.sleep(30)
        elif lab:
            # Depth events wake this worker directly; the timeout bounds recovery.
            market.market_stream.data_ready.wait(timeout=.1)
        else:
            time.sleep(cfg.poll_seconds)


if __name__ == '__main__':
    main()
