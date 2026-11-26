"""USD-M WebSocket RPC with bounded in-flight requests and no write retries.

Production is hard read-only in this adapter. Only the explicitly selected
testnet transport can submit/cancel orders, through DemoExecutor below.
"""
import asyncio
from collections import Counter, deque
from dataclasses import dataclass, field
import hashlib
import hmac
import math
import re
import time
import uuid

import aiohttp

from .execution_runtime import LatencyRecorder


ENDPOINTS = {'production':'wss://ws-fapi.binance.com/ws-fapi/v1',
             'testnet':'wss://testnet.binancefuture.com/ws-fapi/v1'}
READ_METHODS = {'depth','order.status','account.status','v2/account.status',
                'account.position','v2/account.position'}
WRITE_METHODS = {'order.place','order.cancel'}


class GatewayUnavailable(RuntimeError):
    pass


class UnknownExecution(RuntimeError):
    """May have reached the exchange; reconcile, never blindly resend."""


class GatewayRejected(RuntimeError):
    def __init__(self, status, code):
        self.status, self.code = status, code
        # Never expose arbitrary server error messages or signed payloads.
        super().__init__(f'Binance WebSocket request rejected: status {status}, code {code}')


@dataclass(frozen=True)
class GatewayConfig:
    environment: str = 'testnet'
    api_key: str = field(default='', repr=False)
    api_secret: str = field(default='', repr=False)
    writes_enabled: bool = False
    timeout: float = 5.0
    max_pending: int = 8
    min_write_interval: float = .25

    def __post_init__(self):
        if self.environment not in ENDPOINTS:
            raise ValueError('Unknown private gateway environment')
        if self.writes_enabled and self.environment != 'testnet':
            raise ValueError('Production private order writes remain disabled')
        if not math.isfinite(self.timeout) or not 0 < self.timeout <= 30 or not 1 <= self.max_pending <= 64:
            raise ValueError('Invalid private gateway bounds')
        if not math.isfinite(self.min_write_interval) or self.min_write_interval < .1:
            raise ValueError('Private write pacing must be at least 100ms')


def signed_params(params, api_key, secret, timestamp_ms):
    if not api_key or not secret:
        raise ValueError('Private gateway credentials not configured')
    if {'apiKey','signature','timestamp','recvWindow'} & set(params):
        raise ValueError('Authentication fields are managed by the gateway')
    result = dict(params, apiKey=api_key, timestamp=int(timestamp_ms), recvWindow=5000)
    for key,value in list(result.items()):
        if isinstance(value,bool):
            result[key] = 'true' if value else 'false'
        elif isinstance(value,(dict,list,tuple)) or (isinstance(value,float) and not math.isfinite(value)):
            raise ValueError('Invalid WebSocket parameter')
    payload = '&'.join(f'{key}={result[key]}' for key in sorted(result))
    result['signature'] = hmac.new(secret.encode(),payload.encode(),hashlib.sha256).hexdigest()
    return result


class PrivateGateway:
    def __init__(self, cfg, session_factory=None, telemetry=None):
        self.cfg = cfg
        self._factory = session_factory or aiohttp.ClientSession
        self.telemetry = telemetry or LatencyRecorder()
        self._session = self._ws = self._reader_task = None
        self._pending = {}
        self._slots = asyncio.Semaphore(cfg.max_pending)
        self._send_lock = asyncio.Lock()
        self._next_write = self._rate_block_until = 0.
        self._limits = deque(maxlen=16)
        self.counters = Counter()
        self.offset_ms = 0

    async def connect(self):
        if self.connected:
            return
        await self.close()
        started = time.perf_counter_ns()
        self._session = self._factory(timeout=aiohttp.ClientTimeout(total=None,sock_connect=10),trust_env=False)
        try:
            self._ws = await self._session.ws_connect(ENDPOINTS[self.cfg.environment],heartbeat=20,
                autoping=True,max_msg_size=2*1024*1024)
            self._reader_task = asyncio.create_task(self._read())
            self.counters['connections'] += 1
            self.telemetry.observe('gateway_connect',(time.perf_counter_ns()-started)/1e6)
        except Exception:
            await self.close()
            raise GatewayUnavailable('Private WebSocket connection unavailable') from None

    @property
    def connected(self):
        return self._ws is not None and not self._ws.closed and self._reader_task is not None and not self._reader_task.done()

    async def close(self):
        task,self._reader_task = self._reader_task,None
        if task:
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        self._ws = self._session = None
        self._fail_pending()

    def _fail_pending(self):
        for future,method,sent,_ in list(self._pending.values()):
            if not future.done():
                error = UnknownExecution('WebSocket lost; reconcile order before any retry') if method in WRITE_METHODS and sent else GatewayUnavailable('WebSocket disconnected')
                future.set_exception(error)

    def _rate_limits(self, rows):
        durations = {'SECOND':1,'MINUTE':60,'HOUR':3600,'DAY':86400}
        now = time.time()
        for row in rows:
            interval = durations.get(row.get('interval'))
            if interval and row.get('limit',0)>0 and row.get('count',0)>=row['limit']*.9:
                seconds = interval*int(row.get('intervalNum',1))
                if seconds>0:
                    self._rate_block_until = max(self._rate_block_until,(int(now//seconds)+1)*seconds)
            self._limits.append({k:row[k] for k in ('rateLimitType','interval','intervalNum','limit','count') if k in row})

    async def _read(self):
        try:
            async for message in self._ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        break
                    continue
                try:
                    body = message.json()
                    self._rate_limits(body.get('rateLimits',[]))
                    item = self._pending.get(body.get('id'))
                    if item is None:
                        self.counters['unmatched_responses'] += 1
                        continue
                    future,method,_,started = item
                    if not future.done():
                        self.telemetry.observe('rpc:'+method,(time.perf_counter_ns()-started)/1e6)
                        future.set_result(body)
                except (ValueError,TypeError,KeyError,AttributeError):
                    self.counters['invalid_frames'] += 1
        except (aiohttp.ClientError,OSError):
            self.counters['connection_errors'] += 1
        finally:
            self._fail_pending()

    async def request(self, method, params=None):
        mutation = method in WRITE_METHODS
        if method not in READ_METHODS | WRITE_METHODS:
            raise ValueError('Private WebSocket method not allowlisted')
        if mutation and (self.cfg.environment!='testnet' or not self.cfg.writes_enabled):
            raise PermissionError('Private order writes disabled; production remains read-only')
        if not self.connected:
            raise GatewayUnavailable('Connect and reconcile before requesting')
        # Bound both queueing and network wait, not merely the on-wire timeout.
        try:
            await asyncio.wait_for(self._slots.acquire(),self.cfg.timeout)
        except asyncio.TimeoutError:
            raise GatewayUnavailable('Private WebSocket in-flight budget exhausted') from None
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        sent = False
        try:
            async with self._send_lock:
                if mutation:
                    if time.time() < self._rate_block_until:
                        raise GatewayUnavailable('Exchange rate budget exhausted; reconcile before next write')
                    if time.monotonic() < self._next_write:
                        raise GatewayUnavailable('Private order pacing budget exhausted')
                if not self.connected:
                    raise GatewayUnavailable('WebSocket disconnected before send')
                values = dict(params or {})
                if method!='depth':
                    values = signed_params(values,self.cfg.api_key,self.cfg.api_secret,
                        time.time_ns()//1_000_000+self.offset_ms)
                started = time.perf_counter_ns()
                self._pending[identity] = (future,method,True,started)
                # Treat partial/failed transport writes conservatively as sent.
                sent = True
                if mutation:
                    self._next_write = time.monotonic()+self.cfg.min_write_interval
                await asyncio.wait_for(self._ws.send_json({'id':identity,'method':method,'params':values}),self.cfg.timeout)
                self.counters['sent:'+method] += 1
            body = await asyncio.wait_for(future,self.cfg.timeout)
            status = int(body.get('status',500))
            code = (body.get('error') or {}).get('code')
            if status >= 400:
                self.counters['rejected:'+method] += 1
                if status in {418,429}:
                    self._rate_block_until = max(self._rate_block_until,time.time()+60)
                if mutation and (status>=500 or code in {-1006,-1007}):
                    raise UnknownExecution('Exchange result unknown; query by client order ID')
                raise GatewayRejected(status,code)
            self.counters['completed:'+method] += 1
            return body.get('result')
        except (asyncio.TimeoutError,aiohttp.ClientError,OSError):
            self.counters['timeouts_or_transport_errors:'+method] += 1
            if mutation and sent:
                raise UnknownExecution('Order response unavailable; do not resend') from None
            raise GatewayUnavailable('Private WebSocket response unavailable') from None
        finally:
            self._pending.pop(identity,None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()  # retrieve disconnect error even if send failed
            self._slots.release()

    def status(self):
        return {'environment':self.cfg.environment,'connected':self.connected,
            'production_writes_enabled':False,'demo_writes_enabled':self.cfg.writes_enabled,
            'pending_requests':len(self._pending),'counters':dict(self.counters),
            'rate_block_seconds':max(0,self._rate_block_until-time.time()),
            'rate_limits':list(self._limits),'latency':self.telemetry.report(),
            'measurement_scope':'client request round trip; not matching-engine internal latency'}


class DemoExecutor:
    """Explicitly armed testnet execution, independent of existing strategies."""
    def __init__(self, gateway, journal):
        if gateway.cfg.environment!='testnet' or journal.environment!='testnet':
            raise PermissionError('Demo execution cannot use production credentials/journal')
        self.gateway, self.journal = gateway,journal

    def _ready(self):
        state = self.journal.get('reconciliation',{})
        if (self.journal.get('kill_switch',False) or not state.get('ready') or
            not 0 <= time.time()*1000-state.get('at_ms',0) <= 15000):
            raise PermissionError('Demo entry fenced; reconciliation is stale or kill switch active')

    async def place(self, client_id, params):
        from decimal import Decimal
        from .private_journal import decimal_text
        self._ready()
        if not self.gateway.cfg.writes_enabled:
            raise PermissionError('Demo order writes not armed')
        if not re.fullmatch(r'qt_[a-zA-Z0-9_-]{1,32}',client_id):
            raise ValueError('Invalid system client order ID')
        if (not re.fullmatch(r'[A-Z0-9]{5,30}',str(params.get('symbol',''))) or
            params.get('side') not in {'BUY','SELL'} or params.get('type') not in {'MARKET','LIMIT'} or
            params.get('positionSide','BOTH')!='BOTH' or Decimal(decimal_text(params.get('quantity')))<=0):
            raise ValueError('Invalid demo order')
        if params['type']=='LIMIT' and (Decimal(decimal_text(params.get('price')))<=0 or
            params.get('timeInForce') not in {'GTC','IOC','FOK'}):
            raise ValueError('Invalid demo limit order')
        if not self.journal.reserve(client_id,params):
            raise PermissionError('Order ID already reserved; query existing order instead of resending')
        try:
            result = await self.gateway.request('order.place',dict(params,newClientOrderId=client_id,newOrderRespType='ACK'))
            if not isinstance(result,dict) or result.get('clientOrderId')!=client_id or result.get('symbol')!=params['symbol']:
                raise UnknownExecution('Order acknowledgement identity mismatch; reconcile')
            self.journal.apply_order(result,'websocket_ack')
            return result
        except GatewayRejected:
            self.journal.intent_state(client_id,'REJECTED')
            raise
        except GatewayUnavailable:
            # Unlike UnknownExecution, this error guarantees the write did
            # not enter send_json (rate budget, pacing, connection or slot).
            self.journal.intent_state(client_id,'NOT_SENT')
            self.journal.fence('gateway_unavailable_before_submit')
            raise
        except (Exception,asyncio.CancelledError):
            self.journal.intent_state(client_id,'UNKNOWN')
            self.journal.fence('order_submit_unknown; reconcile client order ID')
            raise

    async def cancel(self, client_id):
        intent = self.journal.intent(client_id)
        if not intent:
            raise PermissionError('Cannot cancel an unowned/manual order')
        # Kill switch must allow risk reduction via cancelling our own demo order.
        try:
            result = await self.gateway.request('order.cancel',{'symbol':intent['symbol'],'origClientOrderId':client_id})
            if not isinstance(result,dict) or result.get('clientOrderId')!=client_id or result.get('symbol')!=intent['symbol']:
                raise UnknownExecution('Cancel acknowledgement identity mismatch; reconcile')
            self.journal.apply_order(result,'websocket_cancel')
            return result
        except (Exception,asyncio.CancelledError):
            self.journal.fence('cancel_result_unresolved; reconcile order')
            raise
