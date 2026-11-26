import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import hmac
import json
from pathlib import Path
import sys
import time
from unittest.mock import AsyncMock, MagicMock

import aiohttp
from aiohttp import web
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from quant.account_stream import AccountStream
from quant.private_gateway import (PrivateGateway, GatewayConfig, GatewayRejected,
    GatewayUnavailable, UnknownExecution, DemoExecutor, signed_params)
from quant.private_journal import PrivateJournal
from quant.private_reconcile import PrivateReconciler
from quant.observe import ReadOnlyBinance
from quant.settings import Settings


def order(status='NEW', executed='0', at=1000, identity=12, client='qt_example', symbol='BTCUSDT'):
    return {'symbol':symbol,'orderId':identity,'clientOrderId':client,'status':status,
            'origQty':'1','executedQty':executed,'updateTime':at,'side':'BUY','type':'LIMIT'}


def event(status='PARTIALLY_FILLED', executed='.4', trade_id=7, at=2000):
    return {'e':'ORDER_TRADE_UPDATE','E':at,'T':at,'o':{'s':'BTCUSDT','i':12,'c':'qt_example',
        'X':status,'q':'1','z':executed,'S':'BUY','o':'LIMIT','ap':'100','ps':'BOTH',
        'R':False,'T':at,'x':'TRADE','l':'.4','L':'100','t':trade_id,'n':'.02','N':'USDT','rp':'0'}}


@pytest.fixture
def journal(tmp_path):
    return PrivateJournal(tmp_path/'private.db','testnet','not-a-real-key')


def test_private_journal_binds_environment_and_account(tmp_path):
    p=tmp_path/'p.db'
    PrivateJournal(p,'production','key-a')
    with pytest.raises(ValueError,match='mismatch'):
        PrivateJournal(p,'testnet','key-a')
    with pytest.raises(ValueError,match='mismatch'):
        PrivateJournal(p,'production','key-b')


def test_duplicate_and_out_of_order_events_keep_cumulative_state_and_fills(journal):
    journal.event(event())
    journal.event(event())
    journal.event(event('FILLED','1',8,3000))
    journal.apply_order(order('NEW','0',1000))
    journal.apply_order(order('PARTIALLY_FILLED','.4',4000))
    report=journal.summary()
    assert report['fills']==2 and report['order_states']=={'FILLED':1}
    assert journal.intent('qt_example') is None  # manually observed, never adopted


@pytest.mark.parametrize('quantity,filled',[('NaN','0'),('1','Infinity'),('-1','0'),('1','2'),('1','-.1')])
def test_bad_exchange_quantities_cannot_enter_journal(journal,quantity,filled):
    with pytest.raises(ValueError):
        journal.apply_order(order()|{'origQty':quantity,'executedQty':filled})
    assert journal.summary()['order_states']=={}


def test_order_intent_is_idempotent_but_cannot_change_parameters(journal):
    params={'symbol':'BTCUSDT','side':'BUY','type':'MARKET','quantity':'1'}
    assert journal.reserve('qt_example',params)
    assert not journal.reserve('qt_example',params)
    with pytest.raises(ValueError,match='reused'):
        journal.reserve('qt_example',params|{'quantity':'2'})
    journal.event(event('FILLED','1'))
    journal.intent_state('qt_example','UNKNOWN')
    assert journal.intent('qt_example')['state']=='FILLED'


def test_signing_is_sorted_and_does_not_mutate_or_log_secrets():
    values={'symbol':'BTCUSDT','reduceOnly':True,'quantity':'0.001'}
    result=signed_params(values,'key','secret',123)
    payload='apiKey=key&quantity=0.001&recvWindow=5000&reduceOnly=true&symbol=BTCUSDT&timestamp=123'
    assert result['signature']==hmac.new(b'secret',payload.encode(),hashlib.sha256).hexdigest()
    assert 'apiKey' not in values and 'secret' not in repr(GatewayConfig(api_key='private-key',api_secret='secret'))
    with pytest.raises(ValueError):
        signed_params({'timestamp':12},'key','secret',123)


@asynccontextmanager
async def rpc_server(handler, cfg=None):
    async def route(request):
        ws=web.WebSocketResponse()
        await ws.prepare(request)
        await handler(ws)
        return ws
    app=web.Application()
    app.router.add_get('/ws',route)
    runner=web.AppRunner(app,shutdown_timeout=.1)
    await runner.setup()
    site=web.TCPSite(runner,'127.0.0.1',0)
    await site.start()
    port=site._server.sockets[0].getsockname()[1]
    class RedirectSession:
        def __init__(self,**kwargs):
            self.inner=aiohttp.ClientSession(**kwargs)
        def ws_connect(self,url,**kwargs):
            return self.inner.ws_connect(f'http://127.0.0.1:{port}/ws',**kwargs)
        async def close(self):
            await self.inner.close()
    gateway=PrivateGateway(cfg or GatewayConfig(),session_factory=RedirectSession)
    try:
        await gateway.connect()
        yield gateway
    finally:
        await gateway.close()
        await runner.cleanup()


def test_real_websocket_correlates_out_of_order_rpc_responses():
    async def run():
        async def handler(ws):
            one,two=await ws.receive_json(),await ws.receive_json()
            for msg in (two,one):
                await ws.send_json({'id':msg['id'],'status':200,'result':{'symbol':msg['params']['symbol']}})
            async for _ in ws:
                pass
        async with rpc_server(handler) as gateway:
            a,b=await asyncio.gather(gateway.request('depth',{'symbol':'BTCUSDT'}),gateway.request('depth',{'symbol':'ETHUSDT'}))
            assert a['symbol']=='BTCUSDT' and b['symbol']=='ETHUSDT'
            assert gateway.status()['latency']['stages']['rpc:depth']['observations']==2
    asyncio.run(run())


@pytest.mark.parametrize('disconnect',[False,True])
def test_lost_order_response_is_unknown_and_never_resent(disconnect):
    async def run():
        received=[]
        async def handler(ws):
            received.append(await ws.receive_json())
            if disconnect:
                await ws.close()
            else:
                async for _ in ws:
                    pass
        cfg=GatewayConfig(api_key='test-key',api_secret='test-secret',writes_enabled=True,timeout=.05)
        async with rpc_server(handler,cfg) as gateway:
            with pytest.raises(UnknownExecution):
                await gateway.request('order.place',{'symbol':'BTCUSDT','newClientOrderId':'qt_example'})
            assert len(received)==1 and gateway.status()['pending_requests']==0
    asyncio.run(run())


def test_production_write_is_rejected_before_any_connection():
    async def run():
        gateway=PrivateGateway(GatewayConfig(environment='production'))
        with pytest.raises(PermissionError):
            await gateway.request('order.place',{})
        assert not gateway.connected
    asyncio.run(run())
    with pytest.raises(ValueError,match='Production'):
        GatewayConfig(environment='production',writes_enabled=True)


def test_public_rtt_and_exchange_rate_budget_block_writes():
    async def run():
        received=[]
        async def handler(ws):
            async for message in ws:
                body=message.json()
                received.append(body)
                await ws.send_json({'id':body['id'],'status':200,'result':{},'rateLimits':[
                    {'rateLimitType':'REQUEST_WEIGHT','interval':'MINUTE','intervalNum':1,'limit':100,'count':95}]})
        cfg=GatewayConfig(api_key='key',api_secret='secret',writes_enabled=True)
        async with rpc_server(handler,cfg) as gateway:
            await gateway.request('depth',{'symbol':'BTCUSDT'})
            assert 'apiKey' not in received[0]['params']
            with pytest.raises(GatewayUnavailable,match='rate budget'):
                await gateway.request('order.place',{})
            assert len(received)==1
    asyncio.run(run())


@pytest.mark.parametrize('status,code,error',[(400,-1102,GatewayRejected),(503,-1007,UnknownExecution),(429,-1003,GatewayRejected)])
def test_exchange_errors_are_sanitized(status,code,error):
    async def run():
        async def handler(ws):
            async for message in ws:
                body=message.json()
                await ws.send_json({'id':body['id'],'status':status,'error':{'code':code,'msg':'private-secret'}})
        cfg=GatewayConfig(api_key='key',api_secret='secret',writes_enabled=True)
        async with rpc_server(handler,cfg) as gateway:
            with pytest.raises(error) as caught:
                await gateway.request('order.place',{})
            assert 'private-secret' not in str(caught.value)
    asyncio.run(run())


def test_demo_executor_requires_fresh_reconciliation_and_preserves_unknown(journal):
    async def run():
        gateway=MagicMock(cfg=GatewayConfig(writes_enabled=True))
        gateway.request=AsyncMock(side_effect=UnknownExecution('lost response'))
        executor=DemoExecutor(gateway,journal)
        params={'symbol':'BTCUSDT','side':'BUY','type':'MARKET','quantity':'1'}
        with pytest.raises(PermissionError):
            await executor.place('qt_example',params)
        gateway.request.assert_not_called()
        journal.set('reconciliation',{'ready':True,'at_ms':time.time()*1000})
        with pytest.raises(UnknownExecution):
            await executor.place('qt_example',params)
        assert journal.intent('qt_example')['state']=='UNKNOWN'
        assert not journal.get('reconciliation')['ready']
        journal.set('reconciliation',{'ready':True,'at_ms':time.time()*1000})
        with pytest.raises(PermissionError,match='reserved'):
            await executor.place('qt_example',params)
        assert gateway.request.call_count==1
        with pytest.raises(PermissionError,match='unowned'):
            await executor.cancel('manual_order')
    asyncio.run(run())


def test_readonly_client_allows_order_queries_but_not_cancels(tmp_path):
    api=ReadOnlyBinance(Settings(db_path=str(tmp_path/'q.db')))
    with pytest.raises(RuntimeError,match='rejects'):
        api.cancel_order('qt_example','BTCUSDT')
    with pytest.raises(ValueError,match='cannot be combined'):
        api.user_trades('BTCUSDT',from_id=1,start_time=2)


def test_proven_unsent_demo_request_does_not_become_an_unresolvable_unknown(journal):
    async def run():
        gateway=MagicMock(cfg=GatewayConfig(writes_enabled=True))
        gateway.request=AsyncMock(side_effect=GatewayUnavailable('pacing budget exhausted'))
        journal.set('reconciliation',{'ready':True,'at_ms':time.time()*1000})
        with pytest.raises(GatewayUnavailable):
            await DemoExecutor(gateway,journal).place('qt_example',
                {'symbol':'BTCUSDT','side':'BUY','type':'MARKET','quantity':'1'})
        assert journal.intent('qt_example')['state']=='NOT_SENT'
        assert journal.unresolved()==[]
    asyncio.run(run())


def test_demo_kill_switch_blocks_entries_but_allows_owned_cancel(journal):
    async def run():
        gateway=MagicMock(cfg=GatewayConfig(writes_enabled=True))
        gateway.request=AsyncMock(return_value=order('CANCELED'))
        params={'symbol':'BTCUSDT','side':'BUY','type':'MARKET','quantity':'1'}
        journal.reserve('qt_example',params)
        journal.set('reconciliation',{'ready':True,'at_ms':time.time()*1000})
        journal.kill()
        executor=DemoExecutor(gateway,journal)
        with pytest.raises(PermissionError):
            await executor.place('qt_another',params)
        await executor.cancel('qt_example')
        assert journal.intent('qt_example')['state']=='CANCELED'
        assert gateway.request.call_count==1
    asyncio.run(run())


def setup_recovery(journal, symbols=('BTCUSDT',), capacity=4096):
    exchange=MagicMock(offset=0)
    exchange.account.return_value={'totalWalletBalance':'100'}
    exchange.positions.return_value=[]
    exchange.open_orders.return_value=[]
    exchange.user_trades.return_value=[]
    stream=AccountStream(exchange)
    stream._connection_opened()
    reconciler=PrivateReconciler(journal,exchange,symbols,stream=stream,capacity=capacity)
    stream.on_order=reconciler.enqueue
    stream.on_gap=reconciler.gap
    stream.on_connected=reconciler.connected
    return exchange,stream,reconciler


def test_reconnect_invalidates_seed_even_when_previous_snapshot_was_recent(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    assert reconciler.run()['ready']
    assert stream.can_serve()
    stream._gap('test_disconnect')
    stream._connection_opened()
    assert not stream.can_serve() and not journal.get('reconciliation')['ready']
    assert reconciler.run()['ready']


def test_snapshot_racing_account_event_is_rejected(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    def account():
        stream._message(json.dumps({'e':'ACCOUNT_UPDATE','T':5000,'a':{'B':[{'a':'USDT','wb':'150','cw':'140'}],'P':[]}}))
        return {'totalWalletBalance':'100'}
    exchange.account.side_effect=account
    result=reconciler.run()
    assert not result['ready'] and 'raced' in result['reason']
    assert stream.snapshot()[0]['totalWalletBalance']=='150'


def test_disconnect_during_recovery_cannot_publish_ready(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    def trades(*args,**kwargs):
        stream._gap('during_recovery')
        return []
    exchange.user_trades.side_effect=trades
    assert not reconciler.run()['ready']


def test_missing_open_order_is_queried_not_assumed_cancelled(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    journal.apply_order(order())
    exchange.query_order.return_value=order('FILLED','1',3000)
    assert reconciler.run()['ready']
    exchange.query_order.assert_called_once_with('qt_example','BTCUSDT')
    assert journal.summary()['order_states']=={'FILLED':1}


def test_unknown_order_not_found_stays_fenced_and_is_never_resent(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    journal.reserve('qt_example',{'symbol':'BTCUSDT'})
    journal.intent_state('qt_example','UNKNOWN')
    from quant.binance import ExchangeError
    exchange.query_order.side_effect=ExchangeError(400,-2013)
    assert not reconciler.run()['ready']
    assert journal.intent('qt_example')['state']=='UNKNOWN'
    exchange.order.assert_not_called()
    exchange.cancel_order.assert_not_called()


def test_recovery_and_websocket_fill_overlap_deduplicates(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    stream._message(json.dumps(event()))
    exchange.query_order.return_value=order('PARTIALLY_FILLED','.4',2000)
    exchange.user_trades.return_value=[{'symbol':'BTCUSDT','id':7,'orderId':12,'time':2000,'qty':'.4','price':'100'}]
    assert reconciler.run()['ready']
    assert journal.summary()['fills']==1
    assert journal.summary()['order_states']=={'PARTIALLY_FILLED':1}


def test_queue_overflow_is_visible_and_requires_full_recovery(journal):
    exchange,stream,reconciler=setup_recovery(journal,capacity=1)
    reconciler.enqueue(event())
    reconciler.enqueue(event('FILLED','1',8,3000))
    assert not journal.get('reconciliation')['ready']
    exchange.query_order.return_value=order('FILLED','1',3000)
    result=reconciler.run()
    assert result['ready'] and result['dropped_events_total']==1


def test_order_events_wake_the_observer_without_waiting_for_poll_interval(journal):
    _,stream,reconciler=setup_recovery(journal)
    assert not reconciler.wakeup.is_set()
    stream._message(json.dumps(event()))
    assert reconciler.wakeup.is_set()
    reconciler.drain()
    assert journal.summary()['fills']==1
    reconciler.wakeup.clear()
    stream.on_account=reconciler.wakeup.set
    stream._message(json.dumps({'e':'ACCOUNT_UPDATE','T':6000,'a':{'B':[],'P':[]}}))
    assert reconciler.wakeup.is_set()


def test_full_trade_pages_are_followed_by_fromid_and_cursor_only_advances_when_complete(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    now=time.time_ns()//1_000_000
    page=[{'symbol':'BTCUSDT','id':i,'orderId':12,'time':now-10,'qty':'.001','price':'100'} for i in range(1000)]
    exchange.user_trades.side_effect=[page,[]]
    assert reconciler.run()['ready']
    assert exchange.user_trades.call_args_list[1].kwargs=={'from_id':1000,'limit':1000}
    assert journal.get('fill_cursor:BTCUSDT')>=now
    assert journal.summary()['fills']==1000


def test_stalled_trade_page_leaves_cursor_and_recovery_incomplete(journal):
    exchange,stream,reconciler=setup_recovery(journal)
    now=time.time_ns()//1_000_000
    page=[{'symbol':'BTCUSDT','id':i,'orderId':12,'time':now-10,'qty':'.001','price':'100'} for i in range(1000)]
    exchange.user_trades.return_value=page
    assert not reconciler.run()['ready']
    assert journal.get('fill_cursor:BTCUSDT') is None


def test_account_out_of_order_balance_event_does_not_regress_state():
    stream=AccountStream(MagicMock())
    for at,balance in [(3000,'200'),(2000,'100')]:
        stream._message(json.dumps({'e':'ACCOUNT_UPDATE','T':at,'a':{'B':[{'a':'USDT','wb':balance}],'P':[]}}))
    assert stream.snapshot()[0]['totalWalletBalance']=='200'
