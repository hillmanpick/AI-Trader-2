import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.binance import Binance, ExchangeError
from quant.lab_config import LabConfig
from quant.lab_worker import Collector
from quant.observe import ReadOnlyBinance
from quant.order_book import DepthBook
from quant.potential_worker import PublicMarket
from quant.rest_guard import RestCooldown, RestGuard
from quant.settings import Settings


@pytest.fixture(autouse=True)
def isolated_guard(monkeypatch):
    monkeypatch.delenv('QUANT_REST_GUARD_REDIS_URL',raising=False)


@pytest.mark.parametrize('status,minimum', [(403,300),(418,900),(429,60),(451,1800)])
def test_denial_stops_network_until_cooldown_then_allows_one_recovery(status,minimum):
    with patch('quant.rest_guard.time.monotonic', return_value=100) as clock:
        guard = RestGuard()
        guard.observe(status)
        assert guard.remaining() == minimum
        with pytest.raises(RestCooldown):
            guard.check()
        clock.return_value += minimum
        guard.check()
        guard.observe(status)
        assert guard.remaining() == min(1800, minimum*2)


@pytest.mark.parametrize('retry', ['9000', 'Thu, 01 Jan 1970 02:30:00 GMT'])
def test_retry_after_is_honored_even_longer_than_local_backoff(retry):
    with patch('quant.rest_guard.time.monotonic', return_value=100), patch('quant.rest_guard.time.time',return_value=0):
        guard=RestGuard()
        guard.observe(429, {'Retry-After':retry})
        assert guard.remaining() == 9000


def test_edge_html_denial_has_safe_message_and_blocks_followup_calls():
    api=Binance(Settings(api_key='private-key',api_secret='private-secret'))
    response=MagicMock(status_code=403,headers={'Server':'CloudFront'})
    response.json.side_effect=ValueError('signed response content must not appear')
    api.session.request=MagicMock(return_value=response)
    with pytest.raises(ExchangeError,match='403') as error:
        api.account()
    assert 'private' not in str(error.value) and 'signed' not in str(error.value)
    for _ in range(20):
        with pytest.raises(RestCooldown):
            api.positions()
    api.session.request.assert_called_once()


def test_other_service_and_restart_respect_shared_cooldown():
    with patch('quant.rest_guard.time.monotonic',return_value=100), patch('quant.rest_guard.time.time',return_value=1000):
        for _ in range(2):
            guard=RestGuard()
            guard._shared=MagicMock()
            guard._shared.get.return_value=json.dumps({'until':6400,'status':418})
            with pytest.raises(RestCooldown) as error:
                guard.check()
            assert error.value.status==418 and error.value.retry_after==5400
            # Repeated depth events use a one-second Redis read cache.
            for _ in range(100):guard.remaining()
            guard._shared.get.assert_called_once()


def test_redis_failure_cannot_erase_local_denial_cooldown():
    guard=RestGuard();guard._shared=MagicMock()
    guard._shared.get.side_effect=RuntimeError('redis unavailable')
    guard._shared.eval.side_effect=RuntimeError('redis unavailable')
    guard.observe(403)
    with pytest.raises(RestCooldown):guard.check()
    assert guard.remaining()>299


def test_denial_publishes_no_credentials_or_urls_to_shared_guard():
    guard=RestGuard();guard._shared=MagicMock();guard._shared.get.return_value=None
    guard.observe(418,{'Retry-After':'5000'})
    call=guard._shared.eval.call_args.args
    assert call[0]==RestGuard.EXTEND and call[2]==RestGuard.SHARED_KEY
    assert call[-1]==418 and call[-2]>0


def test_optional_leverage_uses_bounded_cache_and_does_not_repeat_failed_reads():
    api=ReadOnlyBinance(Settings())
    with patch('quant.observe.time.monotonic',return_value=100) as clock:
        api.request=MagicMock(return_value=[{'symbol':'BTCUSDT','leverage':20}])
        assert api.symbol_config()[0]['leverage']==20
        for _ in range(20):
            api.symbol_config()
        api.request.assert_called_once()
        clock.return_value=161
        api.request.side_effect=ExchangeError(500)
        with pytest.raises(ExchangeError):
            api.symbol_config()
        assert api.symbol_config()==[]
        assert api.request.call_count==2


def test_collector_failure_cannot_be_retried_on_every_depth_event(tmp_path):
    async def scenario():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.books['BTCUSDT']=DepthBook()
        collector.read=AsyncMock(side_effect=RuntimeError('offline'))
        collector.seed_needed('BTCUSDT')
        await collector.snapshot_tasks['BTCUSDT']
        for _ in range(100):
            collector.seed_needed('BTCUSDT')
        assert not collector.snapshot_tasks
        assert collector.read.await_count==1
    asyncio.run(scenario())


def test_collector_gateway_denial_pauses_other_symbols_and_all_rest(tmp_path):
    async def scenario():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.rest_guard.observe(403)
        collector.session=MagicMock()
        with pytest.raises(RestCooldown):
            await collector.read('/fapi/v1/exchangeInfo')
        collector.seed_needed('ETHUSDT')
        assert not collector.snapshot_tasks
        collector.session.get.assert_not_called()
    asyncio.run(scenario())


def test_collector_http_denial_arms_cooldown_before_other_readers(tmp_path):
    async def scenario():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.session=MagicMock()
        response=collector.session.get.return_value.__aenter__.return_value
        response.status=403
        response.headers={}
        response.raise_for_status=MagicMock(side_effect=RuntimeError('blocked'))
        with pytest.raises(RuntimeError,match='blocked'):
            await collector.read('/fapi/v1/depth',{'symbol':'BTCUSDT','limit':1000})
        with pytest.raises(RestCooldown):
            await collector.read('/fapi/v1/exchangeInfo')
        collector.session.get.assert_called_once()
    asyncio.run(scenario())


def test_collector_spaces_requests_from_concurrent_snapshot_tasks(tmp_path):
    async def scenario():
        collector=Collector(LabConfig(directory=str(tmp_path)))
        collector.session=MagicMock()
        response=collector.session.get.return_value.__aenter__.return_value
        response.status=200;response.headers={};response.json=AsyncMock(return_value={})
        response.raise_for_status=MagicMock()
        clock=[100.]
        starts=[]
        collector.session.get.side_effect=lambda *a,**k: (starts.append(clock[0]) or context)
        context=MagicMock()
        context.__aenter__=AsyncMock(return_value=response)
        async def advance(seconds):clock[0]+=seconds
        with patch('quant.lab_worker.time.monotonic',side_effect=lambda:clock[0]), patch('quant.lab_worker.asyncio.sleep',side_effect=advance):
            await asyncio.gather(*(collector.read('/fapi/v1/depth',{'symbol':s}) for s in ['BTCUSDT','ETHUSDT','SUIUSDT']))
        assert starts==[100.,101.,102.]
    asyncio.run(scenario())


def test_radar_gateway_denial_stops_remaining_batch_requests():
    async def scenario():
        session=MagicMock()
        response=session.get.return_value.__aenter__.return_value
        response.status=403
        response.headers={}
        market=PublicMarket(session)
        for _ in range(12):
            with pytest.raises(ValueError):
                await market.get('futures','/fapi/v1/klines')
        assert market.requests==1
        session.get.assert_called_once()
    asyncio.run(scenario())
