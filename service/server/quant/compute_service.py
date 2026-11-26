"""Authenticated public-feature computation bridge; no exchange/order methods."""
import asyncio
from collections import Counter
import hmac
import json
import logging
import os
from pathlib import Path
import time
import uuid

from aiohttp import web, WSMsgType
import redis.asyncio as redis_async

from .compute_bridge import (PREFIX, canonical, strategy_bundle, job_id,
                             validate_observations, RESULT_TTL_MS)
from .execution_runtime import LatencyRecorder
from .execution_state import percentile
from collections import deque


RENEW = """
local raw=redis.call('GET',KEYS[1])
if not raw then return 0 end
if cjson.decode(raw).generation~=ARGV[1] then return 0 end
redis.call('SET',KEYS[1],ARGV[2],'PX',5000)
return 1
"""
COMMIT = """
local raw=redis.call('GET',KEYS[1])
if not raw then return 0 end
local head=cjson.decode(raw)
if head.generation~=ARGV[1] or head.session~=ARGV[2] or not head.connected then return 0 end
redis.call('SET',KEYS[2],ARGV[3],'PX',ARGV[4])
return 1
"""


class Bridge:
    def __init__(self, client, policy, token, symbols=(), max_rtt_ms=750, shadow=False):
        if len(token) < 32:
            raise ValueError('Compute service requires a dedicated 32+ character token')
        self.client, self.policy, self.token = client, policy, token
        self.default_symbols = tuple(symbols)
        self.bundle = strategy_bundle()
        self.generation = uuid.uuid4().hex
        self.session = None
        self.ws = None
        self.node = None
        self.max_rtt_ms = max_rtt_ms
        self.shadow = shadow
        self.metrics = LatencyRecorder()
        self.counts = Counter()
        self.pending = {}
        self._seen = {}
        self._last_head = 0.
        self.session_rtts = deque(maxlen=100)

    def admission(self):
        p95=percentile(self.session_rtts,.95)
        return {'eligible':len(self.session_rtts)>=20 and p95 is not None and p95<=100,
                'samples':len(self.session_rtts),'p95_ms':p95,
                'required_samples':20,'max_p95_ms':100}

    def state(self):
        return {'at_ms': time.time_ns()//1_000_000, 'generation': self.generation,
                'session': self.session, 'node': self.node, 'connected': bool(self.ws and not self.ws.closed),
                'code_id': self.bundle['code_id'], 'executor': 'server_only',
                'admission':self.admission(),
                'real_execution_enabled': False, 'shadow': self.shadow, 'counters': dict(self.counts),
                'latency': self.metrics.report()}

    async def start(self):
        await self.client.set(PREFIX+'head', canonical(self.state()), px=5000)

    async def heartbeat(self):
        if time.monotonic()-self._last_head < .5:
            return
        ok = await self.client.eval(RENEW, 1, PREFIX+'head', self.generation, canonical(self.state()))
        if not ok:
            # A Redis restart loses cache/coordination only; never overwrite a
            # different live bridge. Previously cached results expire or miss.
            ok = await self.client.set(PREFIX+'head',canonical(self.state()),px=5000,nx=True)
        if not ok:
            raise RuntimeError('Compute bridge generation superseded')
        self._last_head = time.monotonic()

    def authorize(self, request):
        token = request.headers.get('X-Compute-Token', '')
        if not token or not hmac.compare_digest(token, self.token):
            raise web.HTTPUnauthorized()

    async def bundle_route(self, request):
        self.authorize(request)
        return web.json_response(self.bundle)

    async def status_route(self, request):
        self.authorize(request)
        return web.json_response(self.state())

    async def make_batch(self):
        from .quant_strategy import STRATEGIES
        # Public-feature input only: no balances, positions, credentials or .env.
        candidates_raw = await self.client.get('quant:lab:candidates')
        candidates = json.loads(candidates_raw) if candidates_raw else {}
        now = time.time_ns()//1_000_000
        if not 0 <= now-candidates.get('ts',0) <= 650000:
            return None
        symbols = list(dict.fromkeys(candidates.get('symbols', self.default_symbols)))[:40]
        raws = await self.client.mget(['quant:lab:feature:'+s for s in symbols]) if symbols else []
        jobs = []
        live_keys = set()
        for symbol,raw in zip(symbols,raws):
            if not raw:
                continue
            try:
                row = json.loads(raw)
                if row.get('symbol')!=symbol or not 0 <= now-row['ts'] <= 1500:
                    continue
                identity = job_id(row,self.policy,self.bundle['code_id'])
                live_keys.add(symbol)
                if self._seen.get(symbol)==identity:
                    continue
                self._seen[symbol]=identity
                jobs.append({'job_id':identity,'row':row})
            except (ValueError,KeyError,TypeError):
                self.counts['invalid_features']+=1
        self._seen={s:v for s,v in self._seen.items() if s in live_keys}
        if not jobs:
            return None
        batch = {'type':'jobs','batch_id':uuid.uuid4().hex,'generation':self.generation,
                 'session':self.session,'code_id':self.bundle['code_id'],
                 'strategies':list(STRATEGIES),'policy':self.policy,'jobs':jobs}
        if len(canonical(batch).encode()) > 512*1024:
            self.counts['oversize_input_batches']+=1
            return None
        return batch

    async def accept(self, batch, result, sent_ns):
        from .quant_strategy import STRATEGIES
        rtt=(time.perf_counter_ns()-sent_ns)/1e6
        self.metrics.observe('compute_round_trip',rtt)
        self.session_rtts.append(rtt)
        if (not isinstance(result,dict) or result.get('type')!='results'
                or result.get('batch_id')!=batch['batch_id']
                or result.get('generation')!=self.generation or result.get('session')!=self.session
                or result.get('code_id')!=self.bundle['code_id']):
            self.counts['rejected_identity']+=1
            return False
        if rtt>self.max_rtt_ms:
            self.counts['rejected_late']+=1
            return False
        values=result.get('results')
        expected={j['job_id']:j for j in batch['jobs']}
        if not isinstance(values,dict) or set(values)!=set(expected):
            self.counts['rejected_job_set']+=1
            return False
        # Validate the whole response before making any result visible.
        for value in values.values():
            validate_observations(value,STRATEGIES)
        now=time.time_ns()//1_000_000
        for identity,value in values.items():
            row=expected[identity]['row']
            if not 0 <= now-row['ts'] <= 2000:
                self.counts['rejected_stale_feature']+=1
                continue
            if not self.shadow:
                record={'job_id':identity,'generation':self.generation,'session':self.session,
                        'at_ms':now,'valid_until_ms':min(now+RESULT_TTL_MS,row['ts']+2000),
                        'observations':value}
                ok=await self.client.eval(COMMIT,2,PREFIX+'head',PREFIX+'result:'+identity,
                    self.generation,self.session,canonical(record),RESULT_TTL_MS)
                if not ok:
                    raise RuntimeError('Compute session lost publication fence')
            self.counts['accepted_jobs']+=1
        return True

    async def ws_route(self, request):
        self.authorize(request)
        if self.ws is not None:
            raise web.HTTPConflict(text='Another compute client is connected')
        node=request.headers.get('X-Compute-Node','local')
        if len(node)>80 or any(c.isspace() for c in node):
            raise web.HTTPBadRequest()
        ws=web.WebSocketResponse(heartbeat=10,max_msg_size=512*1024,receive_timeout=5)
        self.ws=ws
        self.session=uuid.uuid4().hex
        self.node=node
        self._seen.clear()
        self.session_rtts.clear()
        self._last_head=0.
        try:
            await ws.prepare(request)
            self.counts['connections']+=1
            await ws.send_json({'type':'hello','generation':self.generation,'session':self.session,
                                'code_id':self.bundle['code_id'],'executor':'server_only'})
            await self.heartbeat()
            while not ws.closed:
                await self.heartbeat()
                batch=await self.make_batch()
                if not batch:
                    await asyncio.sleep(.05)
                    continue
                started=time.perf_counter_ns()
                await asyncio.wait_for(ws.send_json(batch),1)
                message=await asyncio.wait_for(ws.receive(),1.5)
                if message.type!=WSMsgType.TEXT:
                    break
                await self.accept(batch,message.json(),started)
        except (asyncio.TimeoutError,ConnectionError,ValueError,TypeError,KeyError,RuntimeError) as exc:
            logging.warning('Compute protocol failure: %s',type(exc).__name__)
            self.counts['client_disconnect_or_protocol_error']+=1
        finally:
            self.ws=None
            self.session=None
            self.node=None
            self._last_head=0.
            try:
                await self.heartbeat()
            except Exception:
                pass
            await ws.close()
        return ws

    async def ticker(self):
        while True:
            try:
                await self.heartbeat()
            except Exception:
                self.counts['coordination_errors']+=1
                if self.ws is not None:
                    await self.ws.close()
            await asyncio.sleep(.5)

    def app(self):
        app=web.Application(client_max_size=512*1024)
        app.router.add_get('/bundle',self.bundle_route)
        app.router.add_get('/status',self.status_route)
        app.router.add_get('/compute',self.ws_route)
        async def lifespan(app):
            await self.start()
            task=asyncio.create_task(self.ticker())
            yield
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
            if self.ws:
                await self.ws.close()
        app.cleanup_ctx.append(lifespan)
        return app


def main():
    import config  # existing server env only
    from .settings import Settings
    from .paper import paper_settings
    from .quant_strategy import policy_for_quant
    cfg=paper_settings(Settings())
    if cfg.mode!='paper' or cfg.api_key or cfg.api_secret or cfg.live_enabled:
        raise ValueError('Compute bridge is restricted to credential-free paper execution')
    token_path=os.environ['QUANT_COMPUTE_TOKEN_FILE']
    token=Path(token_path).read_text().strip()
    client=redis_async.Redis.from_url(os.getenv('QUANT_REDIS_URL','redis://redis:6379/1'),
        decode_responses=True,socket_timeout=1,socket_connect_timeout=1)
    bridge=Bridge(client,policy_for_quant(cfg),token,cfg.symbols,
                  shadow=os.getenv('QUANT_COMPUTE_SHADOW','true').lower()=='true')
    # Docker publishes this only on 127.0.0.1; local clients use SSH forwarding.
    web.run_app(bridge.app(),host='0.0.0.0',port=8765,access_log=None)


if __name__=='__main__':
    main()
