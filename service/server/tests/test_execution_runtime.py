"""Execution priority, stale signal rejection, bounded telemetry and I/O isolation."""
import json
from pathlib import Path
import sys
import threading
import time
from unittest.mock import MagicMock

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.execution_runtime import LatencyRecorder, LatestNotification, CycleSnapshotBus, DiagnosticBatchStore
from quant.paper_execution import PaperExecution
from quant.engine import Engine
from quant.settings import Settings
from quant.store import Store
from quant.market_stream import MarketStream


def fixture(tmp_path,monkeypatch):
    monkeypatch.setenv('QUANT_PAPER_PRIORITY_EXECUTION','true')
    cfg=Settings(db_path=str(tmp_path/'paper.db'),paper_depth_participation=1,reflection_enabled=False)
    store=Store(cfg.db_path)
    store.set('paper_balance',10000)
    store.set('entry_context:BTCUSDT',{'feature_ts':time.time()*1000,'leverage':10,
              'entry_profile':{'stop_pct':.01,'take_pct':.02,'notional':102}})
    bus=MagicMock()
    bus.get.side_effect=lambda key:{'lastUpdateId':time.time_ns(), 'updated_ms':int(time.time()*1000),
        'bids':[['99','100']],'asks':[['101','100']]}
    engine=Engine(cfg,store,MagicMock())
    execution=PaperExecution(engine,bus)
    return store,execution


def test_entry_is_durably_matched_before_submit_returns(tmp_path,monkeypatch):
    store,execution=fixture(tmp_path,monkeypatch)
    order=execution.submit('BTCUSDT',1,100,'test','once')
    assert execution.journal.get(order)['state']=='FILLED'
    assert store.get('positions')['BTCUSDT']['amount']==1
    assert len(execution.journal.fills())==1
    assert execution.journal.get(order)['ack_ns']>=execution.journal.get(order)['ack_due_ns']
    # Real elapsed time, not artificially setting simulated time to the due date.
    assert execution.journal.get(order)['ack_ns']<=time.time_ns()


def test_stale_and_future_entry_signals_never_create_an_order(tmp_path,monkeypatch):
    store,execution=fixture(tmp_path,monkeypatch)
    context=store.get('entry_context:BTCUSDT')
    for stamp in (time.time()*1000-3000,time.time()*1000+5000,None):
        store.set('entry_context:BTCUSDT',context|{'feature_ts':stamp})
        assert execution.submit('BTCUSDT',1,100,'test',str(stamp)) is None
        assert not execution.journal.orders()
        assert store.get('sizing:BTCUSDT')['status']=='blocked'


def test_exit_ignores_signal_age_but_requires_real_depth(tmp_path,monkeypatch):
    store,execution=fixture(tmp_path,monkeypatch)
    execution.submit('BTCUSDT',1,100,'open','once')
    store.set('entry_context:BTCUSDT',{'feature_ts':0})
    execution.request_exit('BTCUSDT','stop_loss')
    execution.tick()
    assert store.get('positions')=={}
    assert len(execution.journal.fills())==2
    assert not execution.exits()


def test_no_depth_never_generates_synthetic_fill(tmp_path,monkeypatch):
    store,execution=fixture(tmp_path,monkeypatch)
    execution.bus.get.side_effect=lambda key:None
    oid=execution.submit('BTCUSDT',1,100,'test','no-depth')
    assert execution.journal.get(oid)['state']=='EXPIRED'
    assert execution.journal.fills()==[]
    assert not store.get('positions',{})


def test_latency_windows_are_bounded_and_do_not_claim_exchange_latency():
    telemetry=LatencyRecorder(capacity=5)
    for i in range(20):
        telemetry.observe('scan',i)
    report=telemetry.report()
    assert report['stages']['scan']=={'observations':20,'window_samples':5,
            'p50_ms':17,'p95_ms':18.8,'p99_ms':18.96,'max_ms':19}
    assert report['exchange_execution_measured'] is False
    with pytest.raises(ValueError):
        telemetry.observe('scan',float('nan'))


def test_snapshot_is_one_batch_and_never_reused_next_scan():
    bus=MagicMock()
    bus.client.mget.return_value=[json.dumps({'ts':1})]*5
    snapshot=CycleSnapshotBus(bus)
    snapshot.begin(['BTCUSDT','BTCUSDT'])
    assert snapshot.get('feature:BTCUSDT')=={'ts':1}
    assert snapshot.get('liquidity:BTCUSDT')=={'ts':1}
    bus.get.assert_not_called()
    assert len(bus.client.mget.call_args.args[0])==5
    snapshot.end()
    snapshot.get('feature:BTCUSDT')
    bus.get.assert_called_once_with('feature:BTCUSDT')
    bus.client.mget.side_effect=ConnectionError('offline')
    with pytest.raises(ConnectionError):
        snapshot.begin(['BTCUSDT'])
    assert snapshot.cache is None


def test_slow_notification_cannot_block_execution_or_build_backlog():
    entered,release,done=threading.Event(),threading.Event(),threading.Event()
    def deliver(*args):
        entered.set()
        release.wait(timeout=3)
        done.set()
    notifier=MagicMock()
    notifier.tick.side_effect=deliver
    queue=LatestNotification(notifier,interval=.01)
    try:
        queue.submit({'BTCUSDT':{'amount':1}},'running')
        assert entered.wait(timeout=1)
        for i in range(100):
            queue.submit({'BTCUSDT':{'amount':i}},'running')
        assert queue.latest==({'BTCUSDT':{'amount':99}},'running')
        assert not done.is_set()
    finally:
        queue.close()
        release.set()
        queue.thread.join(timeout=1)


def test_hot_mark_never_blocks_on_rest_and_start_skips_unused_candles():
    exchange=MagicMock()
    exchange.cfg.strategy='lab_model'
    stream=MarketStream(exchange,['BTCUSDT'],'1m')
    with pytest.raises(ValueError,match='REST disabled'):
        stream.mark('BTCUSDT',allow_rest=False)
    exchange._rest_mark.assert_not_called()
    stream.marks['BTCUSDT']=100
    stream.updated['mark:BTCUSDT']=time.monotonic()
    assert stream.mark('BTCUSDT',allow_rest=False)==100
    stream._run=lambda:None
    stream.start()
    stream.stop()
    exchange._rest_closed_candles.assert_not_called()


def test_probe_reports_unfilled_orders_and_cannot_approve_tiny_sample(tmp_path,monkeypatch):
    from quant.execution_probe import report
    store,execution=fixture(tmp_path,monkeypatch)
    execution.submit('BTCUSDT',1,100,'test','real-fill')
    # Both missing-depth and good executions count in the denominator.
    execution.bus.get.side_effect=lambda key:None
    execution.request_exit('BTCUSDT','test-exit')
    execution.tick()
    before=len(execution.journal.fills())
    output=report(store.path)
    assert output['scope']['window_orders']==1
    assert output['exchange_execution_measured'] is False
    assert output['local_100ms_target']['met'] is False
    assert len(execution.journal.fills())==before
    with pytest.raises(ValueError):
        report(store.path,limit=10001)


def test_only_diagnostics_are_delayed_financial_and_unknown_keys_write_immediately(tmp_path):
    from dataclasses import replace
    cfg=Settings(db_path=str(tmp_path/'paper.db'))
    store=Store(cfg.db_path)
    batch=DiagnosticBatchStore(store,cfg)
    batch.set('signal:BTCUSDT',{'action':'hold'})
    assert batch.get('signal:BTCUSDT')=={'action':'hold'}
    assert store.get('signal:BTCUSDT') is None
    for key in ('positions','paper_balance','paper_exit:BTCUSDT','entry_context:BTCUSDT',
                'quant_cooldown:BTCUSDT','paused','new_financial_key'):
        batch.set(key,{'persisted':True})
        assert store.get(key)=={'persisted':True}
    batch.flush()
    assert store.get('signal:BTCUSDT')=={'action':'hold'}
    with pytest.raises(ValueError):
        DiagnosticBatchStore(store,replace(cfg,mode='live',live_enabled=True))


def test_decision_batch_preserves_unique_rows_and_bounded_buffer(tmp_path):
    store=Store(str(tmp_path/'paper.db'))
    cfg=Settings(db_path=store.path)
    with store.connect() as c:
        c.execute('CREATE TABLE quant_decisions (symbol TEXT,ts INTEGER,body TEXT,PRIMARY KEY(symbol,ts))')
    batch=DiagnosticBatchStore(store,cfg)
    batch.record_decision({'symbol':'BTCUSDT','feature_ts':100,'action':'hold'})
    batch.record_decision({'symbol':'BTCUSDT','feature_ts':100,'action':'hold'})
    batch.flush()
    with store.connect() as c:
        assert c.execute('SELECT COUNT(*) FROM quant_decisions').fetchone()[0]==1
    for i in range(600):
        batch.set('signal:'+str(i),{'action':'hold'})
    assert len(batch.values)==512 and batch.dropped_diagnostics==88


def test_cycle_cache_reuses_reads_but_keeps_financial_write_through(tmp_path):
    cfg=Settings(db_path=str(tmp_path/'paper.db'))
    store=Store(cfg.db_path)
    store.set('positions',{'BTCUSDT':{'amount':1}})
    store.set('signal:BTCUSDT',{'action':'hold'})
    batch=DiagnosticBatchStore(store,cfg)
    batch.begin()
    first=batch.get('positions')
    first['BTCUSDT']['amount']=999
    assert batch.get('positions')['BTCUSDT']['amount']==1
    batch.set('positions',{'BTCUSDT':{'amount':2}})
    batch.set('signal:BTCUSDT',{'action':'long'})
    assert store.get('positions')['BTCUSDT']['amount']==2
    assert store.get('signal:BTCUSDT')['action']=='hold'
    batch.end()
    batch.flush()
    assert store.get('signal:BTCUSDT')['action']=='long'
