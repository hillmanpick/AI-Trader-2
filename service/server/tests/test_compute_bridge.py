import asyncio
import copy
import json
import os
from pathlib import Path
import sys
import time
from unittest.mock import MagicMock

import aiohttp
from aiohttp import web
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'deploy'))
from quant.compute_bridge import (ComputeReader, PREFIX, canonical, job_id,
                                  strategy_bundle, validate_observations)
from quant.compute_service import Bridge
from quant.quant_strategy import STRATEGIES, signal, policy_for_quant
from quant.settings import Settings
from local_compute_worker import load_evaluator, evaluate_batch


def example(ts=None):
    from quant.lab_features import FEATURES, SCHEMA
    f=dict.fromkeys(FEATURES,0.)
    f.update(return_5s=.0001,return_30s=.001,return_60s=.002,
        ewma_volatility=.001,spread_bps=1.,depth_imbalance=.4,ofi=1.,
        trade_imbalance=.5,volume_anomaly=2.,btc_return_60s=.001,latency_ms=10.)
    return {'symbol':'BTCUSDT','ts':ts or time.time_ns()//1_000_000,'price':100.,
        'vector':[f[k] for k in FEATURES],
        'fast':{'return_250ms':.0001,'return_1s':.001,'trade_imbalance_1s':.5,
                'ofi_1s':1.,'ofi_1s_events':8,'microprice_edge_bps':.1,'ready':True},
        'meta':{'quality':True,'schema':SCHEMA,'entry_allowed':True,'atr_proxy':.002}}


def valid_cache():
    row=example(10000)
    policy=policy_for_quant(Settings())
    bundle=strategy_bundle()
    identity=job_id(row,policy,bundle['code_id'])
    head={'generation':'generation-a','session':'session-a','code_id':bundle['code_id'],
          'connected':True,'at_ms':10050,'admission':{'eligible':True}}
    result={'job_id':identity,'generation':'generation-a','session':'session-a',
            'at_ms':10050,'valid_until_ms':11500,
            'observations':{n:signal(n,row,policy) for n in STRATEGIES}}
    return row,policy,bundle,head,result


def test_exact_local_result_used_without_recalculating():
    row,policy,bundle,head,result=valid_cache()
    client=MagicMock()
    client.mget.return_value=[canonical(head),canonical(result)]
    fallback=MagicMock()
    reader=ComputeReader(client,bundle['code_id'],enabled=True,now_ms=lambda:10100)
    assert reader.observations(row,policy,STRATEGIES,fallback)==result['observations']
    fallback.assert_not_called()
    assert reader.status()['counters']=={'local_hits':1}


@pytest.mark.parametrize('change',[
    {'generation':'old-generation'}, {'session':'old-session'}, {'job_id':'wrong-input'},
    {'at_ms':20000}, {'at_ms':1}, {'valid_until_ms':10001},
    {'observations':{'made_up':{}}},
])
def test_invalid_late_or_superseded_result_falls_back_immediately(change):
    row,policy,bundle,head,result=valid_cache()
    client=MagicMock()
    client.mget.return_value=[canonical(head),canonical(result|change)]
    fallback=MagicMock(side_effect=signal)
    reader=ComputeReader(client,bundle['code_id'],enabled=True,now_ms=lambda:10100)
    assert reader.observations(row,policy,STRATEGIES,fallback)=={n:signal(n,row,policy) for n in STRATEGIES}
    assert fallback.call_count==len(STRATEGIES) and reader.last_source=='server'


@pytest.mark.parametrize('head_change',[{'connected':False},{'at_ms':1},{'code_id':'different-code'},
                                      {'admission':{'eligible':False}}])
def test_disconnected_or_old_coordinator_cannot_authorize_local_results(head_change):
    row,policy,bundle,head,result=valid_cache()
    client=MagicMock()
    client.mget.return_value=[canonical(head|head_change),canonical(result)]
    fallback=MagicMock(side_effect=signal)
    reader=ComputeReader(client,bundle['code_id'],True,now_ms=lambda:10100)
    reader.observations(row,policy,STRATEGIES,fallback)
    assert fallback.call_count==len(STRATEGIES)


def test_redis_outage_does_not_pause_strategy_or_position_management():
    row=example()
    client=MagicMock()
    client.mget.side_effect=RuntimeError('cache outage')
    reader=ComputeReader(client,enabled=True)
    expected={n:signal(n,row,policy_for_quant(Settings())) for n in STRATEGIES}
    assert reader.observations(row,policy_for_quant(Settings()),STRATEGIES,signal)==expected
    assert reader.last_reason=='cache_unavailable_or_invalid'


def test_input_policy_and_code_all_change_job_identity():
    row=example()
    policy=policy_for_quant(Settings())
    baseline=job_id(row,policy,'code-a')
    assert baseline!=job_id(row|{'price':101},policy,'code-a')
    assert baseline!=job_id(row,policy|{'fee_rate':.002},'code-a')
    assert baseline!=job_id(row,policy,'code-b')


def test_local_evaluator_uses_same_frozen_module_and_rejects_changed_bundle(tmp_path):
    bundle=strategy_bundle()
    evaluator=load_evaluator(bundle,tmp_path)
    row=example()
    policy=policy_for_quant(Settings())
    identity=job_id(row,policy,bundle['code_id'])
    batch={'type':'jobs','code_id':bundle['code_id'],'strategies':list(STRATEGIES),
           'batch_id':'batch1','generation':'g1','session':'s1','policy':policy,
           'jobs':[{'row':row,'job_id':identity}]}
    result=evaluate_batch(evaluator,batch,bundle['code_id'])
    assert result['results'][identity]=={n:signal(n,row,policy) for n in STRATEGIES}
    with pytest.raises(ValueError,match='hash mismatch'):
        evaluate_batch(evaluator,batch|{'policy':policy|{'fee_rate':.002}},bundle['code_id'])
    with pytest.raises(ValueError,match='identity mismatch'):
        load_evaluator(bundle|{'sources':bundle['sources']|{'../escape.py':'x'}},tmp_path)


def test_observation_shape_cannot_directly_request_close_or_leverage():
    row=example()
    observations={n:signal(n,row,policy_for_quant(Settings())) for n in STRATEGIES}
    for action in ('close','buy','leverage'):
        invalid=copy.deepcopy(observations)
        invalid[STRATEGIES[0]]['action']=action
        with pytest.raises(ValueError):
            validate_observations(invalid,STRATEGIES)


def redis_url():
    url=os.getenv('QUANT_COMPUTE_TEST_REDIS_URL','')
    if not url:
        pytest.skip('Requires explicitly isolated Redis test container')
    if not url.endswith('/15'):
        raise ValueError('Integration tests require isolated Redis DB 15')
    return url


def test_real_websocket_auth_and_compute_disconnect_fallback(tmp_path):
    url=redis_url()
    async def scenario():
        import redis.asyncio as redis_async
        import redis
        client=redis_async.Redis.from_url(url,decode_responses=True)
        sync=redis.Redis.from_url(url,decode_responses=True)
        await client.flushdb()
        policy=policy_for_quant(Settings())
        bridge=Bridge(client,policy,'x'*40,shadow=False)
        runner=web.AppRunner(bridge.app(),shutdown_timeout=.2)
        await runner.setup()
        site=web.TCPSite(runner,'127.0.0.1',0)
        await site.start()
        port=site._server.sockets[0].getsockname()[1]
        base=f'http://127.0.0.1:{port}'
        headers={'X-Compute-Token':'x'*40,'X-Compute-Node':'test-local'}
        try:
            row=example()
            await client.set('quant:lab:candidates',canonical({'ts':row['ts'],'symbols':['BTCUSDT']}))
            await client.set('quant:lab:feature:BTCUSDT',canonical(row))
            async with aiohttp.ClientSession() as session:
                async with session.get(base+'/status') as response:
                    assert response.status==401
                async with session.ws_connect(base+'/compute',headers=headers) as ws:
                    hello=await ws.receive_json(timeout=2)
                    batch=await ws.receive_json(timeout=2)
                    with pytest.raises(aiohttp.WSServerHandshakeError) as duplicate:
                        await session.ws_connect(base+'/compute',headers=headers)
                    assert duplicate.value.status==409
                    evaluator=load_evaluator(bridge.bundle,tmp_path)
                    bridge.session_rtts.extend([1.]*20)  # prior successful calibration samples
                    await ws.send_json(evaluate_batch(evaluator,batch,hello['code_id']))
                    for _ in range(30):
                        if bridge.counts['accepted_jobs']: break
                        await asyncio.sleep(.01)
                    bridge._last_head=0
                    await bridge.heartbeat()
                    reader=ComputeReader(sync,hello['code_id'],enabled=True)
                    observations=reader.observations(row,policy,STRATEGIES,signal)
                    assert reader.last_source=='local'
                    assert observations=={n:signal(n,row,policy) for n in STRATEGIES}
                for _ in range(30):
                    state=json.loads(await client.get(PREFIX+'head'))
                    if not state['connected']: break
                    await asyncio.sleep(.02)
                reader.observations(row,policy,STRATEGIES,signal)
                assert reader.last_source=='server'
        finally:
            await runner.cleanup()
            await client.aclose()
            sync.close()
    asyncio.run(scenario())


def test_redis_generation_fence_and_late_response_are_rejected():
    url=redis_url()
    async def scenario():
        import redis.asyncio as redis_async
        client=redis_async.Redis.from_url(url,decode_responses=True)
        await client.flushdb()
        policy=policy_for_quant(Settings())
        first=Bridge(client,policy,'x'*40)
        await first.start()
        second=Bridge(client,policy,'x'*40)
        await second.start()
        with pytest.raises(RuntimeError,match='superseded'):
            await first.heartbeat()
        row=example()
        identity=job_id(row,policy,second.bundle['code_id'])
        second.session='current-session'
        batch={'batch_id':'b','jobs':[{'job_id':identity,'row':row}]}
        body={'type':'results','batch_id':'b','session':'old-session',
              'generation':second.generation,'code_id':second.bundle['code_id'],
              'results':{identity:{n:signal(n,row,policy) for n in STRATEGIES}}}
        assert not await second.accept(batch,body,time.perf_counter_ns())
        body['session']='current-session'
        assert not await second.accept(batch,body,time.perf_counter_ns()-2_000_000_000)
        assert await client.get(PREFIX+'result:'+identity) is None
        await client.aclose()
    asyncio.run(scenario())
