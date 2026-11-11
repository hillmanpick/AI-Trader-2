import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.event_lake import EventLake,envelope,decode_row
from quant.execution_state import OrderJournal,SimulatedBroker
from quant.book_replay import HistoricalBookReplay, ReplayValidationError
from quant.order_book import DepthBook
from quant.market_ranker import MarketRanker
from quant.correlation_risk import portfolio_risk,cap_notional
from quant.lab_train import label_rows
from quant.lab_features import FEATURES,SCHEMA


def event(kind,ns,**payload):
    return envelope({'e':kind,'s':'BTCUSDT','E':ns//1000000,**payload},received_ns=ns,monotonic_ns=ns)


def book(now=0):
    b = DepthBook()
    b.seed({'lastUpdateId':100,'bids':[['99','5'],['98','10']],'asks':[['101','2'],['102','3']]},now)
    b.apply({'U':100,'u':101,'pu':99,'b':[],'a':[]},now)
    return b


def test_parquet_preserves_exact_trade_ids_decimal_strings_and_time_order(tmp_path):
    lake = EventLake(tmp_path)
    trade = event('trade',2000000000,t=123,p='0.0000012300',q='123456.123400',m=False,T=1999)
    late = event('depthUpdate',4000000000,U=1,u=2,pu=0,b=[['1','2']],a=[['2','3']])
    middle = event('trade',3000000000,t=124,p='0.0000012400',q='12.0000',m=True,T=2999)
    lake.append([trade,late])
    lake.append([middle])
    rows = list(lake.iter_events())
    assert [r['received_ns'] for r in rows] == [2000000000,3000000000,4000000000]
    assert rows[0]['payload']['p'] == '0.0000012300'
    assert rows[0]['payload']['q'] == '123456.123400'
    assert rows[0]['payload']['t'] == 123
    import pyarrow.parquet as pq
    stored = pq.read_table(lake.directory/lake.segments()[0]['path'])
    assert stored.schema.field('trade_time_ms')
    assert stored.column('trade_time_ms').to_pylist()[0] == 1999
    assert stored.column('payload').to_pylist()[0] == '{}'
    assert lake.checkpoints()['BTCUSDT'] == 124
    path = lake.directory/lake.segments()[0]['path']
    with open(path,'ab') as f:
        f.write(b'corrupt')
    with pytest.raises(ValueError,match='checksum'):
        list(lake.iter_events())


def test_v1_trade_payload_remains_readable_after_typed_column_upgrade():
    row = {'event_id':'trade:BTCUSDT:7','kind':'trade','symbol':'BTCUSDT',
           'event_ms':1000,'received_ns':1000000000,'monotonic_ns':1000000000,
           'source':'websocket','aggregate_id':None,'first_trade_id':7,
           'last_trade_id':7,'price':'10.00','quantity':'1.00','buyer_maker':False,
           'payload':'{"T":999}'}
    decoded = decode_row(row)
    assert decoded['payload']['T'] == 999
    assert decoded['payload']['t'] == 7


def test_parquet_orphan_recovery_and_retention_are_auditable(tmp_path):
    lake = EventLake(tmp_path,max_bytes=1)
    lake.append([event('trade',2000000000,t=1,p='1',q='1',m=True,T=2000)])
    with lake.connect() as c:
        c.execute('DELETE FROM segments')
        c.execute('DELETE FROM checkpoints')
    lake.recover()
    assert lake.stats()['rows'] == 1
    assert lake.checkpoints()['BTCUSDT'] == 1
    lake.prune()
    assert lake.stats()['rows'] == 0
    assert lake.stats()['gaps'][0]['status'] == 'evicted'


def test_market_ioc_walks_depth_partially_fills_and_never_reuses_liquidity(tmp_path):
    journal = OrderJournal(tmp_path/'orders.db')
    broker = SimulatedBroker(journal,ack_ms=10,participation=1)
    broker.submit('one','BTCUSDT',1,'10',0)
    broker.on_event(5000000,{'BTCUSDT':book(5)})
    assert journal.get('one')['state']=='SUBMITTED'
    b = book(20)
    broker.on_event(20000000,{'BTCUSDT':b})
    first = journal.get('one')
    assert first['state']=='EXPIRED' and first['filled']=='5.0'
    assert len(journal.fills()) == 2
    broker.submit('two','BTCUSDT',1,'10',20000000)
    broker.on_event(40000000,{'BTCUSDT':b})
    assert journal.get('two')['filled']=='0'
    stats = journal.statistics()
    assert stats['first_fill']['p50_ms']==20
    assert stats['partial_orders']==1


def test_depth_metrics_accumulate_intermediate_best_queue_flow():
    b = DepthBook()
    b.seed({'lastUpdateId':100,'bids':[['99','10']],'asks':[['101','10']]},0)
    b.apply({'U':100,'u':101,'pu':99,'b':[['99','20']],'a':[]},1000)
    b.apply({'U':102,'u':102,'pu':101,'b':[['99','10']],'a':[]},3000)
    b.apply({'U':103,'u':103,'pu':102,'b':[['99','30']],'a':[]},6000)
    metrics = b.metrics(6001)
    assert metrics['ofi_ready'] and metrics['ofi_events'] == 2
    assert metrics['ofi_5s'] == pytest.approx(10/15)


def test_passive_limit_uses_trade_queue_and_cancel_can_lose_race(tmp_path):
    journal = OrderJournal(tmp_path/'orders.db')
    broker = SimulatedBroker(journal,ack_ms=0,cancel_ms=20,participation=1)
    broker.submit('maker','BTCUSDT',1,'2',0,order_type='LIMIT',limit_price='99',tif='GTC')
    b = book(0)
    broker.on_event(0,{'BTCUSDT':b})
    assert journal.get('maker')['queue_ahead']=='5.0'
    broker.cancel('maker',1000000)
    trade = {'s':'BTCUSDT','t':1,'q':'6','p':'99','m':True}
    broker.on_event(5000000,{'BTCUSDT':b},trade)
    assert journal.get('maker')['filled']=='1.0'
    broker.on_event(6000000,{'BTCUSDT':b},trade)
    assert journal.get('maker')['filled']=='1.0'
    broker.on_event(10000000,{'BTCUSDT':b},{**trade,'t':2,'q':'1'})
    assert journal.get('maker')['state']=='FILLED'
    assert journal.get('maker')['filled']=='2.0'
    broker.on_event(30000000,{'BTCUSDT':b})
    assert journal.get('maker')['state']=='FILLED'


def test_order_idempotency_restart_and_atomic_ledger_callback(tmp_path):
    path = tmp_path/'orders.db'
    journal = OrderJournal(path)
    def ledger(c,fill,order):
        c.execute('CREATE TABLE IF NOT EXISTS test_ledger(id TEXT PRIMARY KEY,quantity TEXT)')
        c.execute('INSERT INTO test_ledger VALUES (?,?)',(fill['id'],fill['quantity']))
    broker = SimulatedBroker(journal,ack_ms=0,participation=1,on_fill=ledger)
    original = broker.submit('same','BTCUSDT',1,'1',0)
    assert broker.submit('same','BTCUSDT',1,'1',1)==original
    with pytest.raises(ValueError,match='different request'):
        broker.submit('same','BTCUSDT',1,'2',0)
    broker.on_event(0,{'BTCUSDT':book()})
    restarted = SimulatedBroker(OrderJournal(path),ack_ms=0,on_fill=ledger)
    restarted.on_event(1000000,{'BTCUSDT':book()})
    assert len(restarted.journal.fills())==1
    with journal.connect() as c:
        assert c.execute('SELECT COUNT(*) FROM test_ledger').fetchone()[0]==1


def test_restart_does_not_sell_the_same_displayed_liquidity_to_another_order(tmp_path):
    path = tmp_path/'restart-depth.db'
    journal = OrderJournal(path)
    broker = SimulatedBroker(journal,ack_ms=0,participation=1)
    b = book(0)
    broker.submit('first','BTCUSDT',1,'10',0)
    broker.on_event(0,{'BTCUSDT':b})
    assert journal.get('first')['filled']=='5.0'
    restarted = SimulatedBroker(OrderJournal(path),ack_ms=0,participation=1)
    restarted.submit('second','BTCUSDT',1,'10',1000000)
    restarted.on_event(1000000,{'BTCUSDT':b})
    assert restarted.journal.get('second')['filled']=='0'
    b.apply({'U':102,'u':102,'pu':101,'b':[],'a':[]},2)
    restarted.submit('new-revision','BTCUSDT',1,'1',2000000)
    restarted.on_event(2000000,{'BTCUSDT':b})
    assert restarted.journal.get('new-revision')['state']=='FILLED'


def test_restart_does_not_advance_a_passive_queue_twice_for_the_same_trade(tmp_path):
    path = tmp_path/'restart-queue.db'
    broker = SimulatedBroker(OrderJournal(path),ack_ms=0,participation=1)
    broker.submit('maker','BTCUSDT',1,'2',0,order_type='LIMIT',limit_price='99',tif='GTC')
    b = book(0)
    broker.on_event(0,{'BTCUSDT':b})
    trade = {'s':'BTCUSDT','t':1,'q':'4','p':'99','m':True}
    broker.on_event(1000000,{'BTCUSDT':b},trade)
    assert broker.journal.get('maker')['queue_ahead']=='1.0'
    restarted = SimulatedBroker(OrderJournal(path),ack_ms=0,participation=1)
    restarted.on_event(2000000,{'BTCUSDT':b},trade)
    assert restarted.journal.get('maker')['queue_ahead']=='1.0'
    assert restarted.journal.get('maker')['filled']=='0'
    restarted.on_event(3000000,{'BTCUSDT':b},{**trade,'t':2,'q':'2'})
    assert restarted.journal.get('maker')['filled']=='1.0'


def test_missing_book_preserves_consumption_and_cancel_survives_restart(tmp_path):
    path = tmp_path/'missing-book.db'
    broker = SimulatedBroker(OrderJournal(path),ack_ms=0,cancel_ms=20,participation=1)
    broker.submit('resting','BTCUSDT',1,'10',0,order_type='LIMIT',limit_price=102,tif='GTC')
    b = book(0)
    broker.on_event(0,{'BTCUSDT':b})
    assert broker.journal.get('resting')['state']=='PARTIALLY_FILLED'
    broker.on_event(1000000,{})
    broker.cancel('resting',2000000)
    restarted = SimulatedBroker(OrderJournal(path),ack_ms=0,cancel_ms=20,participation=1)
    restarted.submit('second','BTCUSDT',1,'10',3000000)
    restarted.on_event(3000000,{'BTCUSDT':b})
    assert restarted.journal.get('second')['filled']=='0'
    assert restarted.journal.get('resting')['filled']=='5.0'
    restarted.on_event(22000000,{'BTCUSDT':b})
    assert restarted.journal.get('resting')['state']=='CANCELED'
    assert restarted.journal.statistics()['cancel']['p50_ms']==20


def test_failed_multilevel_match_rolls_back_fills_ledger_and_checkpoints(tmp_path):
    path = tmp_path/'rollback-match.db'
    journal = OrderJournal(path)
    with journal.connect() as c:
        c.execute('CREATE TABLE ledger(id TEXT PRIMARY KEY)')
    fail = True
    def callback(c,fill,order):
        c.execute('INSERT INTO ledger VALUES (?)',(fill['id'],))
        if fail and fill['price']=='102.0':
            raise RuntimeError('injected second-level callback failure')
    broker = SimulatedBroker(journal,ack_ms=0,participation=1,on_fill=callback)
    broker.submit('atomic','BTCUSDT',1,'10',0)
    b = book(0)
    with pytest.raises(RuntimeError,match='second-level'):
        broker.on_event(1000000,{'BTCUSDT':b})
    assert journal.get('atomic')['state']=='SUBMITTED'
    assert journal.fills()==[]
    assert broker.consumed=={} and broker.last_trade=={}
    assert journal.match_state()=={'consumed':[],'last_trade':{}}
    with journal.connect() as c:
        assert c.execute('SELECT COUNT(*) FROM ledger').fetchone()[0]==0
    fail = False
    restarted = SimulatedBroker(OrderJournal(path),ack_ms=0,participation=1,on_fill=callback)
    restarted.on_event(1000000,{'BTCUSDT':b})
    assert restarted.journal.get('atomic')['filled']=='5.0'
    assert len(restarted.journal.fills())==2


def test_process_exit_mid_match_leaves_no_partial_commit(tmp_path):
    import os
    import subprocess
    path = tmp_path/'crash.db'
    script = '''
import os,sys
from quant.execution_state import OrderJournal,SimulatedBroker
from quant.order_book import DepthBook
journal=OrderJournal(sys.argv[1])
def fail(c,fill,order):
    if fill['price']=='102.0': os._exit(23)
b=DepthBook()
b.seed({'lastUpdateId':100,'bids':[['99','5']],'asks':[['101','2'],['102','3']]},0)
b.apply({'U':100,'u':101,'pu':99,'b':[],'a':[]},0)
broker=SimulatedBroker(journal,ack_ms=0,participation=1,on_fill=fail)
broker.submit('crash','BTCUSDT',1,'10',0)
broker.on_event(1000000,{'BTCUSDT':b})
'''
    env = os.environ | {'PYTHONPATH':str(Path(__file__).resolve().parents[1])}
    result = subprocess.run([sys.executable,'-c',script,str(path)],env=env,timeout=15,
                            capture_output=True,text=True)
    assert result.returncode==23, result.stderr
    journal=OrderJournal(path)
    assert journal.get('crash')['state']=='SUBMITTED'
    assert journal.fills()==[]
    assert journal.match_state()=={'consumed':[],'last_trade':{}}
    broker=SimulatedBroker(journal,ack_ms=0,participation=1)
    broker.on_event(1000000,{'BTCUSDT':book(0)})
    assert journal.get('crash')['filled']=='5.0'


def test_book_replay_requires_snapshot_bridge_and_reports_measured_processing(tmp_path):
    lake = EventLake(tmp_path)
    ns = 1000000000
    events = [
        event('depthUpdate',ns,U=99,u=101,pu=98,b=[],a=[]),
        event('depthSnapshot',ns+1000000,lastUpdateId=100,bids=[['99','5']],asks=[['101','2']]),
        event('depthUpdate',ns+2000000,U=102,u=103,pu=101,b=[],a=[]),
        event('trade',ns+30000000,t=1,T=1030,p='101',q='1',m=False),
    ]
    lake.append(events)
    broker = SimulatedBroker(OrderJournal(tmp_path/'orders.db'),ack_ms=10,participation=1)
    order = {'identity':'test','symbol':'BTCUSDT','side':1,'quantity':'1','at_ns':ns+2000000}
    replay = HistoricalBookReplay(broker,[order],True)
    report = replay.run(lake.iter_events())
    assert report['sequence_gaps']==0 and report['reconstructed_depth_updates']>0
    assert report['execution']['states']['FILLED']==1
    assert report['local_processing']['p95_ms']>=0
    assert report['execution']['first_fill']['p50_ms']==28
    with pytest.raises(ValueError,match='sorted'):
        replay.consume(events[0])


def test_archived_replay_accepts_exact_previous_update_link_after_snapshot(tmp_path):
    ns = 1000000000
    events = [
        event('depthSnapshot',ns,lastUpdateId=11947828490052,
              bids=[['99','5']],asks=[['101','2']]),
        event('depthUpdate',ns+1000000,U=11947828510045,u=11947828516458,
              pu=11947828490052,b=[],a=[]),
        event('depthUpdate',ns+2000000,U=11947828516459,u=11947828520000,
              pu=11947828516458,b=[],a=[]),
    ]
    replay = HistoricalBookReplay(SimulatedBroker(OrderJournal(tmp_path/'snapshot-link.db')),
                                  require_complete=True,expected_symbols={'BTCUSDT'})
    report = replay.run(events)
    assert report['valid'] and report['sequence_gaps']==0
    assert report['symbols']['BTCUSDT']['last_update_id']==11947828520000
    assert report['symbols']['BTCUSDT']['reconstructed_depth_updates']==2


def test_depth_disconnect_prevents_replay_fills_until_new_snapshot_bridge(tmp_path):
    journal = OrderJournal(tmp_path/'disconnect.db')
    replay = HistoricalBookReplay(SimulatedBroker(journal,ack_ms=0))
    ns = 1000000000
    replay.consume(event('depthSnapshot',ns,lastUpdateId=100,
                         bids=[['99','5']],asks=[['101','2']]))
    replay.consume(event('depthUpdate',ns+1000000,U=100,u=101,pu=99,b=[],a=[]))
    assert replay.books['BTCUSDT'].ready
    replay.broker.submit('must-not-fill','BTCUSDT',1,'1',ns+2000000)
    replay.consume(envelope({'e':'depthStatus','s':'','symbols':['BTCUSDT'],
                            'status':'disconnected'},'control',ns+2000000))
    assert not replay.books['BTCUSDT'].ready
    assert journal.get('must-not-fill')['state']=='EXPIRED'
    assert not journal.fills() and not replay.buffers['BTCUSDT']
    # A diff alone cannot recover the old connection's book.
    replay.consume(event('depthUpdate',ns+3000000,U=102,u=103,pu=101,b=[],a=[]))
    assert not replay.books['BTCUSDT'].ready
    replay.consume(event('depthSnapshot',ns+4000000,lastUpdateId=102,
                         bids=[['99','5']],asks=[['101','2']]))
    assert replay.books['BTCUSDT'].ready


def test_collector_rejects_late_snapshot_from_replaced_connection(tmp_path):
    import asyncio
    from unittest.mock import MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.bus = MagicMock()
        collector.books['BTCUSDT'] = book()
        started,release = asyncio.Event(),asyncio.Event()
        async def slow_snapshot(*args):
            started.set()
            await release.wait()
            return {'lastUpdateId':500,'bids':[['99','5']],'asks':[['101','2']]}
        collector.read = slow_snapshot
        collector.seed_needed('BTCUSDT')
        old = collector.snapshot_tasks['BTCUSDT']
        await started.wait()
        collector.books['BTCUSDT'] = replacement = DepthBook()
        # The late task must not remove a newer snapshot task's registration.
        newer = asyncio.create_task(asyncio.Event().wait())
        collector.snapshot_tasks['BTCUSDT'] = newer
        release.set()
        await old
        assert replacement.last is None
        assert collector.snapshot_tasks['BTCUSDT'] is newer
        assert collector.event_queue.empty()
        await collector.reset_depth_feed(('BTCUSDT',),'disconnected')
        assert newer.cancelled() and 'BTCUSDT' not in collector.snapshot_tasks
        status = collector.event_queue.get_nowait()
        assert status['kind']=='depthStatus' and status['payload']['status']=='disconnected'
        collector.bus.client.delete.assert_called_once_with(
            'quant:lab:book:BTCUSDT','quant:lab:liquidity:BTCUSDT')
    asyncio.run(check())


def test_collector_depth_gap_is_archived_and_snapshot_recovers_without_reusing_diffs(tmp_path):
    import asyncio,time
    from unittest.mock import MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.bus = MagicMock()
        now = time.time_ns()
        collector.books['BTCUSDT'] = book(now//1000000)
        release = asyncio.Event()
        async def snapshot(*args):
            await release.wait()
            return {'lastUpdateId':104,'bids':[['99','5']],'asks':[['101','2']]}
        collector.read = snapshot
        gap={'e':'depthUpdate','s':'BTCUSDT','E':now//1000000,
             'U':104,'u':104,'pu':103,'b':[],'a':[]}
        await collector.accept_depth(gap,now)
        assert not collector.books['BTCUSDT'].ready
        update = collector.event_queue.get_nowait()
        evidence = collector.event_queue.get_nowait()
        assert update['received_ns']==now
        assert evidence['payload']['previous_u']==101 and evidence['payload']['pu']==103
        await asyncio.sleep(0)
        await collector.accept_depth(gap | {'U':104,'u':105,'pu':104},now+1000000)
        release.set()
        await collector.snapshot_tasks['BTCUSDT']
        assert collector.books['BTCUSDT'].ready and collector.books['BTCUSDT'].last==105
        assert not collector.buffers['BTCUSDT']
        await collector.reset_depth_feed(('BTCUSDT',),'disconnected')
        assert not collector.books['BTCUSDT'].ready
    asyncio.run(check())


def test_collector_snapshot_successor_does_not_emit_false_gap_or_request_reseed(tmp_path):
    import asyncio
    from collections import deque
    from unittest.mock import MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        b = DepthBook()
        b.seed({'lastUpdateId':90052,'bids':[['99','5']],'asks':[['101','2']]},1000)
        collector.books['BTCUSDT'] = b
        collector.buffers['BTCUSDT'] = deque(maxlen=10000)
        collector.seed_needed = MagicMock()
        frame = {'e':'depthUpdate','s':'BTCUSDT','E':1001,
                 'U':100045,'u':100058,'pu':90052,'b':[['99','4']],'a':[]}
        await collector.accept_depth(frame,1001000000)
        assert b.ready and b.last == 100058 and b.gaps == 0
        collector.seed_needed.assert_not_called()
        archived = collector.event_queue.get_nowait()
        assert archived['kind'] == 'depthUpdate' and archived['received_ns'] == 1001000000
        assert collector.event_queue.empty()
    asyncio.run(check())


def test_candidate_rotation_rebases_feature_trade_cursor_without_calling_it_a_gap(tmp_path):
    import asyncio
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.selected = ('BTCUSDT',)
        collector.trade_checkpoints['ETHUSDT'] = 100
        collector.builder.last_trade['ETHUSDT'] = 7
        collector.builder.trade_gaps['ETHUSDT'] = 4
        collector.builder.trades['ETHUSDT'].append((1,10.,1))
        collector.builder.history['ETHUSDT'].append((1,100.))
        collector.prepare_selected(('BTCUSDT','ETHUSDT'),2000)
        collector.selected = ('BTCUSDT','ETHUSDT')
        assert collector.builder.last_trade['ETHUSDT'] == 100
        assert collector.builder.trade_gaps['ETHUSDT'] == 4
        assert not collector.builder.trades['ETHUSDT']
        assert not collector.builder.history['ETHUSDT']
        assert collector.builder.bad_until['ETHUSDT'] == 62000
        await collector.accept_trade({'e':'trade','s':'ETHUSDT','E':2001,
                                      'T':2001,'t':101,'p':'100','q':'1','m':False},2001000000)
        assert collector.builder.last_trade['ETHUSDT'] == 101
        assert collector.builder.trade_gaps['ETHUSDT'] == 4
        assert collector.event_queue.qsize() == 1
    asyncio.run(check())


def test_candidate_rotation_keeps_depth_subscription_during_label_grace(tmp_path):
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    collector = Collector(LabConfig(directory=str(tmp_path), horizon_seconds=300))
    collector.selected = ('BTCUSDT', 'ETHUSDT')
    first = collector.depth_subscription_symbols(('BTCUSDT', 'ETHUSDT'), 100000)
    assert set(first) == {'BTCUSDT', 'ETHUSDT'}
    second = collector.depth_subscription_symbols(('BTCUSDT', 'SOLUSDT'), 101000)
    assert set(second) == {'BTCUSDT', 'ETHUSDT', 'SOLUSDT'}
    collector.selected = ('BTCUSDT', 'SOLUSDT')
    # A held position remains subscribed after the grace window.
    third = collector.depth_subscription_symbols(('BTCUSDT', 'SOLUSDT'), 500001,
                                                  held=('ETHUSDT',))
    assert set(third) == {'BTCUSDT', 'ETHUSDT', 'SOLUSDT'}
    fourth = collector.depth_subscription_symbols(('BTCUSDT', 'SOLUSDT'), 500001)
    assert set(fourth) == {'BTCUSDT', 'SOLUSDT'}


def test_quote_observation_survives_feature_warmup_and_candidate_rotation(tmp_path):
    import asyncio,time
    from unittest.mock import MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.bus=MagicMock()
        now=int(time.time()*1000)
        collector.selected=('BTCUSDT',)
        collector.depth_symbols=('BTCUSDT','ETHUSDT')
        for symbol in collector.depth_symbols:collector.books[symbol]=book(now)
        rows,quotes,blocked=await collector.collect_observations(now)
        assert not rows and len(quotes)==2 and len(blocked)==2
        collector.flush(rows,[],quotes)
        market=list(collector.store.market_rows(now,now+1))
        assert len(market)==2 and all(not r['meta']['entry_allowed'] for r in market)
        assert collector.event_queue.empty()
        assert not collector.bus.put.called
        collector.trade_archive_until['ETHUSDT']=now+345000
        collector.builder.last_trade['ETHUSDT']=100
        collector.builder.history['ETHUSDT'].append((now,100.))
        collector.prepare_selected(('BTCUSDT','ETHUSDT'),now+1)
        assert list(collector.builder.history['ETHUSDT'])==[(now,100.)]
        collector.selected=('BTCUSDT',)
        await collector.accept_trade({'e':'trade','s':'ETHUSDT','E':now,'T':now,
            't':101,'p':'100','q':'1','m':False},now*1000000)
        assert collector.builder.last_trade['ETHUSDT']==101
        collector.store.append([{'symbol':'ETHUSDT','ts':now,'price':100.,
            'vector':[0.]*len(FEATURES),'meta':{'quality':True,'schema':SCHEMA}}])
        restored=Collector(LabConfig(directory=str(tmp_path)))
        assert restored.depth_grace_until['ETHUSDT']>=now+345000
        assert not restored.books
        # The maintenance cadence can retain a depth feed past its minimum
        # grace. Trade evidence must cover that same actual observation period.
        collector.trade_archive_until['ETHUSDT']=now-1
        assert collector.archives_trade('ETHUSDT',now)
        collector.prepare_selected(('BTCUSDT','ETHUSDT'),now+2)
        assert list(collector.builder.history['ETHUSDT'])==[(now,100.)]
    asyncio.run(check())


@pytest.mark.parametrize('ack_error',[False,True])
def test_depth_rotation_waits_for_ack_and_reconnects_on_rejection(tmp_path,monkeypatch,ack_error):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock,MagicMock
    import aiohttp
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.depth_symbols=('BTCUSDT',)
        collector.depth_update_event=asyncio.Event()
        collector.reset_depth_feed=AsyncMock()
        collector.drop_depth_feed=AsyncMock()
        sent=[]
        count=0
        class Socket:
            async def send_json(self,payload):sent.append(payload)
            async def receive(self):
                nonlocal count
                count+=1
                if count==1:
                    collector.request_depth_subscriptions(('ETHUSDT',))
                    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT,
                        data=json.dumps({'data':{'e':'bookTicker','s':'BTCUSDT'}}))
                if count in (2,3):
                    assert collector.depth_confirmed==(('BTCUSDT',) if count==2 else ('BTCUSDT','ETHUSDT'))
                    assert not collector.drop_depth_feed.called
                    payload=({'id':sent[count-2]['id'],'code':2,'msg':'Rejected'} if ack_error
                        else {'id':sent[count-2]['id'],'result':None})
                    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=json.dumps(payload))
                assert collector.depth_confirmed==('ETHUSDT',)
                collector.drop_depth_feed.assert_awaited_once_with({'BTCUSDT'})
                raise asyncio.CancelledError
        class Context:
            async def __aenter__(self):return Socket()
            async def __aexit__(self,*args):return False
        collector.session=MagicMock()
        collector.session.ws_connect.return_value=Context()
        async def stop(delay):raise asyncio.CancelledError
        monkeypatch.setattr('quant.lab_worker.asyncio.sleep',stop)
        with pytest.raises(asyncio.CancelledError):await collector.public_stream(('BTCUSDT',))
        assert [p['method'] for p in sent]==['SUBSCRIBE','UNSUBSCRIBE']
        assert collector.reconnects==int(ack_error)
        assert collector.depth_commands_acked==(0 if ack_error else 2)
        assert collector.reset_depth_feed.call_args.args[1]=='disconnected'
    asyncio.run(check())


def test_text_silent_depth_socket_has_a_real_watchdog(tmp_path,monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock,MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.depth_symbols=('BTCUSDT',)
        collector.reset_depth_feed=AsyncMock()
        clock=[0.]
        # Patch the module's clock, not asyncio's scheduling clock.
        from types import SimpleNamespace
        import time
        monkeypatch.setattr('quant.lab_worker.time',SimpleNamespace(
            monotonic=lambda:clock[0],time_ns=time.time_ns,time=time.time))
        class Socket:
            async def receive(self):
                clock[0]+=16
                raise asyncio.TimeoutError
        class Context:
            async def __aenter__(self):return Socket()
            async def __aexit__(self,*args):return False
        collector.session=MagicMock()
        collector.session.ws_connect.return_value=Context()
        async def stop(delay):raise asyncio.CancelledError
        monkeypatch.setattr('quant.lab_worker.asyncio.sleep',stop)
        with pytest.raises(asyncio.CancelledError):await collector.public_stream(('BTCUSDT',))
        assert collector.reconnects==1
        assert collector.reset_depth_feed.call_args.args==({'BTCUSDT'},'disconnected')
    asyncio.run(check())


def test_parquet_trade_archive_tracks_candidates_and_label_grace(tmp_path):
    import asyncio
    import time
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path), horizon_seconds=300))
        collector.selected = ('BTCUSDT',)
        now = int(time.time() * 1000)
        trade = {'e':'trade','s':'BTCUSDT','E':now,'T':now,'t':1,'p':'100','q':'1','m':False}
        await collector.record_event(trade | {'s':'ETHUSDT'})
        assert collector.event_queue.empty()
        await collector.record_event(trade,received_ns=now*1000000)
        assert collector.event_queue.qsize() == 1

        collector.prepare_selected(('ETHUSDT',), now + 1000)
        collector.selected = ('ETHUSDT',)
        await collector.record_event(trade | {'t':2},received_ns=(now+2000)*1000000)
        assert collector.event_queue.qsize() == 2
        after_grace = now + 1000 + 300000 + 45001
        await collector.record_event(trade | {'t':3},received_ns=after_grace*1000000)
        assert collector.event_queue.qsize() == 2
        await collector.record_event({'e':'feedStatus','s':'','symbols':['BTCUSDT','ETHUSDT'],
                                      'status':'disconnected'},received_ns=after_grace*1000000)
        collector.event_queue.get_nowait()
        collector.event_queue.get_nowait()
        status = collector.event_queue.get_nowait()
        assert status['kind'] == 'feedStatus'
        assert status['payload']['symbols'] == ['ETHUSDT']
    asyncio.run(check())


def test_non_candidate_trades_still_rank_without_archiving_or_backfill(tmp_path):
    import asyncio
    from unittest.mock import patch
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.selected = ('ETHUSDT',)
        collector.trade_checkpoints['BTCUSDT'] = 100
        trade = {'e':'trade','s':'BTCUSDT','E':2000,'T':2000,'t':10000,'p':'100','q':'1','m':False}
        with patch.object(collector, 'repair_trade_gap') as repair:
            await collector.accept_trade(trade, 2000000000)
        assert collector.builder.trade_gaps['BTCUSDT'] == 0
        assert collector.trade_checkpoints['BTCUSDT'] == 10000
        assert 'BTCUSDT' in collector.trade_seen_this_process
        assert collector.ranker.last_trade['BTCUSDT'] == 2000
        assert collector.event_queue.empty()
        repair.assert_not_called()
    asyncio.run(check())


def test_archiving_grace_recovers_pending_labels_after_collector_restart(tmp_path):
    import time
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    from quant.lab_store import LabStore
    now = int(time.time() * 1000)
    LabStore(tmp_path).append([{'symbol':'ETHUSDT','ts':now-1000,'price':100.,
                               'vector':[0.]*len(FEATURES),'meta':{'quality':True}}])
    collector = Collector(LabConfig(directory=str(tmp_path),horizon_seconds=300))
    assert collector.selected == ()
    assert collector.archives_trade('ETHUSDT',now)
    assert not collector.archives_trade('ETHUSDT',now+345000)


def test_event_lake_pruning_leaves_headroom_and_records_eviction(tmp_path):
    import time
    lake = EventLake(tmp_path)
    now = time.time_ns()
    lake.append([event('trade',now,t=1,p='100',q='1',m=True,T=now//1000000)])
    first_size = lake.stats()['bytes']
    lake.append([event('trade',now+1000000,t=2,p='100',q='1',m=True,T=now//1000000+1)])
    both = lake.stats()['bytes']
    lake.max_bytes = both + 100
    lake.prune(reserve_bytes=first_size+100)
    assert lake.stats()['bytes'] <= lake.max_bytes-first_size-100
    assert lake.stats()['rows'] == 1
    assert lake.checkpoints()['BTCUSDT'] == 2
    assert any(g['status']=='evicted' for g in lake.stats()['gaps'])


def test_depth_gap_control_event_is_durable_catalog_evidence(tmp_path):
    import asyncio
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        await collector.record_event({'e':'depthStatus','s':'BTCUSDT','status':'sequence_gap',
                                      'previous_u':101,'U':104,'u':105,'pu':103},'control')
        writer = asyncio.create_task(collector.event_writer())
        try:
            await asyncio.wait_for(collector.event_queue.join(),timeout=10)
        finally:
            writer.cancel()
            await asyncio.gather(writer,return_exceptions=True)
        with collector.lake.connect() as c:
            row = c.execute("SELECT * FROM gaps WHERE kind='depth'").fetchone()
        assert row['symbol']=='BTCUSDT' and row['status']=='unresolved'
        assert (row['start_id'],row['end_id'])==(101,103)
        assert json.loads(row['detail'])['pu']==103
        archived, = collector.lake.iter_events(kinds={'depthStatus'})
        assert archived['payload']['previous_u']==101
    asyncio.run(check())


def test_strict_replay_requires_depth_for_every_requested_symbol(tmp_path):
    events = [event('depthSnapshot',1000000000,lastUpdateId=100,
                    bids=[['99','5']],asks=[['101','2']]),
              event('depthUpdate',1001000000,U=100,u=101,pu=99,b=[],a=[])]
    broker = SimulatedBroker(OrderJournal(tmp_path/'orders.db'))
    replay = HistoricalBookReplay(broker,require_complete=True,
                                   expected_symbols={'BTCUSDT','ETHUSDT'})
    with pytest.raises(ReplayValidationError) as exc:
        replay.run(events)
    report = exc.value.report
    assert 'missing_requested_depth:ETHUSDT' in report['invalid_reasons']
    assert report['symbols']['BTCUSDT']['book_ready']
    assert report['symbols']['ETHUSDT']['events']==0


def test_replay_keeps_gap_evidence_even_after_snapshot_recovers_book(tmp_path):
    broker = SimulatedBroker(OrderJournal(tmp_path/'orders.db'))
    replay = HistoricalBookReplay(broker,require_complete=True,expected_symbols={'BTCUSDT'})
    events = [event('depthSnapshot',1000000000,lastUpdateId=100,bids=[['99','5']],asks=[['101','2']]),
              event('depthUpdate',1001000000,U=100,u=101,pu=99,b=[],a=[]),
              event('depthUpdate',1002000000,U=103,u=104,pu=102,b=[],a=[]),
              event('depthSnapshot',1003000000,lastUpdateId=103,bids=[['99','5']],asks=[['101','2']]),
              event('depthUpdate',1004000000,U=105,u=106,pu=104,b=[],a=[])]
    with pytest.raises(ReplayValidationError) as exc:
        replay.run(events)
    report = exc.value.report
    assert not report['valid'] and report['sequence_gaps']==1
    assert report['symbols']['BTCUSDT']['book_ready']
    detail, = report['gap_details']
    assert detail['kind']=='depth' and detail['previous_id']==101
    assert detail['ids']['pu']==102


def test_strict_cli_writes_rejected_window_and_keeps_failure_exit(monkeypatch,tmp_path):
    from quant.book_replay import main
    lake = EventLake(tmp_path)
    lake.append([event('trade',2000000000,t=1,p='100',q='1',m=False,T=2000)])
    output = tmp_path/'rejected.json'
    monkeypatch.setattr(sys,'argv',['book_replay','--directory',str(tmp_path),'--start-ms','1900',
        '--end-ms','2100','--symbols','BTCUSDT,ETHUSDT','--strict','--output',str(output)])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code==2
    report = json.loads(output.read_text(encoding='utf-8'))
    assert not report['valid']
    assert report['requested_window']['symbols']==['BTCUSDT','ETHUSDT']
    assert 'missing_requested_depth:BTCUSDT' in report['invalid_reasons']
    assert report['execution']['orders']==0


def test_all_market_ranker_uses_only_received_flow_and_invalidates_gaps():
    ranker = MarketRanker()
    rules = {'BTCUSDT':{},'ETHUSDT':{},'QUIETUSDT':{}}
    tickers = {s:{'q':'100000000','P':'1'} for s in rules}
    for second in range(70):
        for s,d in [('BTCUSDT',1),('ETHUSDT',-1)]:
            ranker.trade({'s':s,'T':second*1000,'p':100+d*second*.01,'q':'1','m':d<0})
    ranking = ranker.rank(rules,tickers,69000,1)
    by_symbol = {r['symbol']:r for r in ranking}
    assert by_symbol['BTCUSDT']['bias']=='long'
    assert by_symbol['ETHUSDT']['bias']=='short'
    assert not by_symbol['QUIETUSDT']['ready']
    before = ranker.vector('BTCUSDT',tickers['BTCUSDT'],69000)
    ranker.trade({'s':'BTCUSDT','T':1000,'p':10000,'q':'999','m':True})
    assert ranker.vector('BTCUSDT',tickers['BTCUSDT'],69000)==before
    ranker.invalidate('BTCUSDT',69000)
    assert ranker.vector('BTCUSDT',tickers['BTCUSDT'],69000) is None


def test_covariance_distinguishes_correlated_positions_and_signed_hedges():
    import math
    returns = {s:{i:.001*math.sin(i/3) for i in range(60)} for s in ('BTCUSDT','ETHUSDT')}
    same = portfolio_risk(returns,{'BTCUSDT':20000,'ETHUSDT':20000})
    hedge = portfolio_risk(returns,{'BTCUSDT':20000,'ETHUSDT':-20000})
    assert hedge['sigma_usdt'] < same['sigma_usdt']
    assert same['correlations']['BTCUSDT']['ETHUSDT'] > .7
    snapshot = {'ts':100000,'returns':returns}
    positions = {'BTCUSDT':{'amount':200,'mark':100}}
    base = portfolio_risk(returns,{'BTCUSDT':20000})['sigma_usdt']
    cap,report = cap_notional(snapshot,positions,'ETHUSDT',1,40000,base*1.2,100001)
    assert 0 < cap < 40000
    assert report['sigma_usdt'] <= base*1.2+1e-6
    with pytest.raises(ValueError,match='stale'):
        cap_notional(snapshot,positions,'ETHUSDT',1,40000,1000,120000)
    with pytest.raises(ValueError,match='Missing'):
        portfolio_risk(returns,{'UNKNOWNUSDT':100})


def test_triple_barrier_never_uses_a_price_after_label_expiry():
    def sample(ts,price):
        return {'symbol':'BTCUSDT','ts':ts,'price':price,'vector':[0.]*len(FEATURES),
                'meta':{'quality':True,'schema':SCHEMA,'atr_proxy':.001}}
    rows = [sample(0,100),sample(30000,100),sample(61000,200),sample(90000,200)]
    labels = label_rows(rows,60000)
    assert next(r for r in labels if r['ts']==0)['label']==0
    exact = label_rows([sample(0,100),sample(30000,100),sample(60000,100)],60000)
    assert exact[0]['label']==0 and exact[0]['label_end']==60000


def test_zero_value_source_frames_keep_trade_ids_without_reconnecting(tmp_path):
    import asyncio
    import time
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.selected = ('BTCUSDT',)
        stamp = time.time_ns()
        source = {'e':'trade','s':'BTCUSDT','E':stamp//1000000,'T':stamp//1000000,
                  't':100,'p':'0','q':'0','m':False,'X':'NA'}
        await collector.accept_trade(source,stamp)
        assert collector.trade_checkpoints['BTCUSDT']==100
        assert collector.trade_notices==1
        assert not collector.ranker.last_trade
        valid = source | {'t':101,'p':'100','q':'1','X':'MARKET'}
        await collector.accept_trade(valid,stamp+1000)
        assert collector.ranker.last_trade['BTCUSDT']==stamp//1000000
        assert not collector.repairs
        assert collector.event_queue.qsize()==2
    asyncio.run(check())


def test_book_ticker_projection_is_not_written_to_durable_event_lake(tmp_path):
    import asyncio
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        collector.selected = ('BTCUSDT',)
        await collector.record_event({'e':'bookTicker','s':'BTCUSDT','E':1,'u':2,
                                      'b':'100','B':'1','a':'101','A':'1'})
        assert collector.event_queue.empty()
        await collector.record_event({'e':'trade','s':'BTCUSDT','E':1,'T':1,
                                      't':1,'p':'100','q':'1','m':False})
        assert collector.event_queue.qsize()==1
    asyncio.run(check())


def test_paper_partial_fills_update_wallet_margin_and_reduce_only_atomically(tmp_path):
    import time
    from unittest.mock import MagicMock,patch
    from quant.engine import Engine
    from quant.settings import Settings
    from quant.store import Store
    from quant.paper_execution import PaperExecution
    cfg = Settings(db_path=str(tmp_path/'paper.db'),paper_depth_participation=1)
    db = Store(cfg.db_path)
    db.set('paper_balance',10000)
    plan = {'stop_pct':.01,'take_pct':.02,'planned_risk_usdt':100}
    db.set('entry_context:BTCUSDT',{'leverage':10,'entry_profile':plan})
    engine = Engine(cfg,db,MagicMock())
    bus = MagicMock()
    execution = PaperExecution(engine,bus)
    engine.paper_execution = execution
    start = time.time_ns()
    raw = {'lastUpdateId':100,'updated_ms':start//1000000,
           'bids':[['99','5'],['98','10']],'asks':[['101','2'],['102','3']]}
    bus.get.return_value = raw
    with patch('quant.paper_execution.time.time_ns',return_value=start):
        execution.submit('BTCUSDT',10,100,'open','one')
    with patch('quant.paper_execution.time.time_ns',return_value=start+30000000):
        execution.tick()
    p = db.get('positions')['BTCUSDT']
    assert p['amount']==5 and p['entry']==pytest.approx(101.6)
    assert p['initial_margin']==pytest.approx(50.8)
    assert db.get('paper_balance')==pytest.approx(10000-508*.0005)
    assert execution.journal.orders()[0]['state']=='EXPIRED'
    notices = [e for e in db.events() if e['kind']=='开仓成交']
    assert len(notices)==1  # two depth fills become one order notice
    assert '做多' in notices[0]['body'] and '10 倍' in notices[0]['body']
    assert '本次投入保证金：50.8 USDT' in notices[0]['body']
    raw['lastUpdateId']=101
    raw['updated_ms']=(start+40000000)//1000000
    with patch('quant.paper_execution.time.time_ns',return_value=start+40000000):
        execution.submit('BTCUSDT',-5,99,'close','two')
    with patch('quant.paper_execution.time.time_ns',return_value=start+70000000):
        execution.tick()
    assert not db.get('positions')
    assert db.get('paper_balance')==pytest.approx(10000+(99-101.6)*5-(508+495)*.0005)
    assert len(execution.journal.fills())==3
    assert len(db.get('execution_closed_fills'))==1
    assert db.get('execution_statistics')['states']=={'EXPIRED':1,'FILLED':1}
    notice = next(e for e in db.events() if e['kind']=='清仓成交')
    assert '平多' in notice['body'] and '已实现毛盈亏：-13 USDT' in notice['body']
    assert '双边手续费合计：0.5015 USDT' in notice['body']
    assert '已实现净盈亏：-13.5015 USDT' in notice['body']
    from quant.paper_performance import paper_performance
    from datetime import datetime,timezone
    performance = paper_performance(db,datetime.fromtimestamp(start/1e9+1,timezone.utc))
    assert performance['net']==pytest.approx(-13.5015)
    assert performance['recent_net']==pytest.approx(-13.5015)
    assert paper_performance(db,datetime.fromtimestamp(start/1e9+86401,timezone.utc))['yesterday_net']==pytest.approx(-13.5015)
    assert performance['fees']==pytest.approx(.5015)
    db.set('paper_balance',1000000)  # cash additions never change trading PnL
    assert paper_performance(db,datetime.fromtimestamp(start/1e9+1,timezone.utc))['net']==pytest.approx(-13.5015)
    assert paper_performance(db,datetime.fromtimestamp(start/1e9+86401,timezone.utc))['recent_net']==0
    before = len(db.events())
    execution.tick()
    assert len(db.events())==before


def test_sampled_model_approval_cannot_skip_event_execution_validation(tmp_path):
    import time
    from quant.lab_config import LabConfig
    from quant.lab_model import model_status
    model = {'schema':SCHEMA,'features':list(FEATURES),'approved':True,'version':'model-a',
             'trained_ms':int(time.time()*1000)}
    path = tmp_path/'model.json'
    path.write_text(json.dumps(model))
    status,_ = model_status(LabConfig(directory=str(tmp_path)))
    assert not status['ready'] and '逐事件' in status['reason']
    path.write_text(json.dumps(model | {'execution_validation':{'passed':True,'model_version':'different'}}))
    assert not model_status(LabConfig(directory=str(tmp_path)))[0]['ready']


def test_execution_book_publisher_does_not_wait_for_feature_sampling(tmp_path):
    import asyncio,time
    from unittest.mock import MagicMock
    from quant.lab_config import LabConfig
    from quant.lab_worker import Collector
    async def check():
        collector = Collector(LabConfig(directory=str(tmp_path)))
        now = int(time.time()*1000)
        collector.selected = ('BTCUSDT',)
        collector.books['BTCUSDT'] = book(now)
        collector.bus = MagicMock()
        task = asyncio.create_task(collector.fast_books())
        await asyncio.sleep(.05)
        task.cancel()
        await asyncio.gather(task,return_exceptions=True)
        pipe = collector.bus.client.pipeline.return_value
        keys = [call.args[0] for call in pipe.set.call_args_list]
        assert keys==['quant:lab:book:BTCUSDT','quant:lab:liquidity:BTCUSDT']
        assert pipe.execute.call_count==1
        assert collector.bus.event.call_args.args==('depth',)
    asyncio.run(check())


def test_clickhouse_export_is_checksummed_and_idempotent(tmp_path):
    from unittest.mock import MagicMock,patch
    import requests
    lake = EventLake(tmp_path)
    lake.append([event('trade',2000000000,t=1,p='1.00',q='2.00',m=False,T=2000)])
    session = MagicMock()
    response = MagicMock(ok=True,status_code=200)
    session.post.return_value = response
    with patch.object(requests,'Session',return_value=session):
        assert lake.export_clickhouse('http://clickhouse:8123') == 1
        assert lake.export_clickhouse('http://clickhouse:8123') == 0
    queries = [call.kwargs.get('params',{}).get('query','') for call in session.post.call_args_list]
    assert any('CREATE TABLE IF NOT EXISTS' in query for query in queries)
    assert any('INSERT INTO quant.events FORMAT Parquet' in query for query in queries)

    # A second table on the same endpoint must receive its own export; the
    # catalog target is endpoint + database + table, not only the URL.
    with patch.object(requests,'Session',return_value=session):
        assert lake.export_clickhouse('http://clickhouse:8123', table='other_events') == 1
        # Returning to the original target must retain its checkpoint.
        assert lake.export_clickhouse('http://clickhouse:8123') == 0


def test_clickhouse_export_cli_requires_explicit_sink(monkeypatch, tmp_path):
    from quant.event_export import export_once
    from quant.lab_config import LabConfig
    monkeypatch.delenv('QUANT_CLICKHOUSE_URL', raising=False)
    with pytest.raises(RuntimeError, match='not configured'):
        export_once(LabConfig(directory=str(tmp_path)))


def test_clickhouse_export_status_is_redacted_and_tracks_pending_segments(tmp_path):
    from unittest.mock import MagicMock,patch
    import requests
    lake = EventLake(tmp_path)
    lake.append([event('trade',2000000000,t=1,p='1.00',q='2.00',m=False,T=2000)])
    with patch.object(requests,'Session',side_effect=AssertionError('status must not connect')):
        empty = lake.export_status()
    assert empty == {'configured':False,'target_hash':None,'segments':1,
                      'exported_segments':0,'pending_segments':1,'last_export_ns':None}
    session = MagicMock()
    session.post.return_value = MagicMock(ok=True,status_code=200)
    url = 'http://operator:private-password@clickhouse:8123'
    with patch.object(requests,'Session',return_value=session):
        assert lake.export_clickhouse(url,database='quant',table='events')==1
    with patch.object(requests,'Session',side_effect=AssertionError('status must not connect')):
        status = lake.export_status(url,'quant','events')
    assert status['configured'] and status['exported_segments']==1 and status['pending_segments']==0
    assert status['last_export_ns']>0
    assert all(value not in json.dumps(status) for value in (url,'private-password','operator','clickhouse:8123'))
    assert all(value not in lake.path.read_bytes() for value in (url.encode(),b'private-password'))
    other = lake.export_status(url,'quant','another_table')
    assert other['exported_segments']==0 and other['pending_segments']==1
    assert other['target_hash']!=status['target_hash']
    lake.append([event('trade',3000000000,t=2,p='1.00',q='2.00',m=False,T=3000)])
    status = lake.export_status(url)
    assert (status['segments'],status['exported_segments'],status['pending_segments'])==(2,1,1)
    # Pruned segments are no longer pending or counted as retained exports.
    lake.retention_hours = 0
    lake.prune()
    status = lake.export_status(url)
    assert (status['segments'],status['exported_segments'],status['pending_segments'])==(0,0,0)
    assert status['last_export_ns'] is None


def test_clickhouse_failed_insert_remains_pending_for_retry(tmp_path):
    from unittest.mock import MagicMock,patch
    import requests
    lake = EventLake(tmp_path)
    lake.append([event('trade',2000000000,t=1,p='1.00',q='2.00',m=False,T=2000)])
    session = MagicMock()
    ok = MagicMock(ok=True,status_code=200)
    def fail_insert(*args,**kwargs):
        is_insert = kwargs['params']['query'].startswith('INSERT INTO')
        return MagicMock(ok=False,status_code=503) if is_insert else ok
    session.post.side_effect = fail_insert
    with patch.object(requests,'Session',return_value=session):
        with pytest.raises(RuntimeError,match='ClickHouse HTTP 503'):
            lake.export_clickhouse('http://clickhouse:8123')
        assert session.post.call_args.kwargs['params']['query'].startswith('INSERT INTO')
        status = lake.export_status('http://clickhouse:8123')
        assert status['exported_segments']==0 and status['pending_segments']==1
        session.post.side_effect = None
        session.post.return_value = ok
        assert lake.export_clickhouse('http://clickhouse:8123')==1
        assert lake.export_clickhouse('http://clickhouse:8123')==0


def test_legacy_export_checkpoint_migration_preserves_previous_target(tmp_path):
    lake = EventLake(tmp_path)
    lake.append([event('trade',2000000000,t=1,p='1.00',q='2.00',m=False,T=2000)])
    path = lake.segments()[0]['path']
    target = lake._export_target('http://clickhouse:8123','quant','events')
    with lake.connect() as c:
        c.execute('DROP TABLE exports')
        c.execute('CREATE TABLE exports (path TEXT PRIMARY KEY,target TEXT,ts_ns INTEGER)')
        c.execute('INSERT INTO exports VALUES (?,?,?)',(path,target,123))
    migrated = EventLake(tmp_path)
    assert migrated.export_status('http://clickhouse:8123')['exported_segments']==1
    with migrated.connect() as c:
        c.execute('INSERT INTO exports VALUES (?,?,?)',(path,'different-target',456))
        assert c.execute('SELECT COUNT(*) FROM exports').fetchone()[0]==2
    # Re-opening does not repeat the migration or lose either target.
    assert EventLake(tmp_path).export_status('http://clickhouse:8123')['last_export_ns']==123


def test_clickhouse_transport_error_does_not_expose_url_credentials(tmp_path):
    import traceback
    from unittest.mock import MagicMock,patch
    import requests
    lake = EventLake(tmp_path)
    session = MagicMock()
    sink_url = 'http://operator:private-password@sink:8123'
    session.post.side_effect = requests.ConnectionError(sink_url+' refused')
    with patch.object(requests,'Session',return_value=session):
        with pytest.raises(RuntimeError,match='ClickHouse request failed') as error:
            lake.export_clickhouse(sink_url)
    rendered = ''.join(traceback.format_exception(error.type,error.value,error.tb))
    assert 'private-password' not in rendered and 'operator:' not in rendered


@pytest.mark.parametrize('failure',[RuntimeError,OSError,ValueError])
def test_clickhouse_watch_retries_remote_failure_without_leaking_details(monkeypatch,capsys,failure):
    from quant import event_export
    monkeypatch.setattr(event_export,'export_once',
                        lambda cfg=None: (_ for _ in ()).throw(failure('https://operator:private-password@sink')))
    monkeypatch.setattr(event_export.time,'sleep',lambda seconds: (_ for _ in ()).throw(KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        event_export.watch(10)
    output = capsys.readouterr().out
    assert 'retrying' in output and 'checkpoints retained' in output
    assert json.loads(output)['error_type']==failure.__name__
    assert 'private-password' not in output and 'operator:' not in output


@pytest.mark.parametrize('interval',[0,9,3601])
def test_clickhouse_watch_rejects_unsafe_interval(interval):
    from quant.event_export import watch
    with pytest.raises(ValueError,match='between 10 and 3600'):
        watch(interval)
