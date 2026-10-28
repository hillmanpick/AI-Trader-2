import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.event_lake import EventLake,envelope
from quant.execution_state import OrderJournal,SimulatedBroker
from quant.book_replay import HistoricalBookReplay
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
    assert lake.checkpoints()['BTCUSDT'] == 124
    path = lake.directory/lake.segments()[0]['path']
    with open(path,'ab') as f:
        f.write(b'corrupt')
    with pytest.raises(ValueError,match='checksum'):
        list(lake.iter_events())


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
