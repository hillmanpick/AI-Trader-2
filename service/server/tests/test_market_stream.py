import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.market_stream import MarketStream


def test_mark_price_event_is_cached_without_rest_request():
    exchange = MagicMock(offset=0)
    stream = MarketStream(exchange, ('BTCUSDT',), '5m')
    stream._message('{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate","s":"BTCUSDT","p":"60000"}}')
    assert stream.mark('BTCUSDT') == 60000
    exchange._rest_mark.assert_not_called()


def test_kline_event_replaces_same_open_time_and_keeps_closed_rows():
    exchange = MagicMock(offset=0)
    stream = MarketStream(exchange, ('BTCUSDT',), '5m')
    stream.candles['BTCUSDT'] = [[0, '1', '2', '0.5', '1.5', '2', 1]] * 40
    stream._message('{"data":{"e":"kline","s":"BTCUSDT","k":{"t":1000,"o":"1","h":"3","l":"1","c":"2","v":"4","T":1001,"x":true}}}')
    stream._message('{"data":{"e":"kline","s":"BTCUSDT","k":{"t":1000,"o":"1","h":"4","l":"1","c":"3","v":"5","T":1001,"x":true}}}')
    rows = [row for row in stream.candles['BTCUSDT'] if int(row[0]) == 1000]
    assert len(rows) == 1 and rows[0][4] == '3'


def test_stream_snapshot_reports_connection_state():
    exchange = MagicMock(offset=0)
    stream = MarketStream(exchange, ('BTCUSDT', 'ETHUSDT'), '5m')
    stream.state['status'] = 'connected'
    stream.marks['BTCUSDT'] = 60000
    stream.updated['mark:BTCUSDT'] = __import__('time').monotonic()
    stream.marks['ETHUSDT'] = 3000
    stream.updated['mark:ETHUSDT'] = __import__('time').monotonic()
    assert stream.snapshot()['status'] == 'connected'
    assert stream.snapshot()['symbols'] == ['BTCUSDT', 'ETHUSDT']


def test_rest_seed_is_temporarily_usable_before_websocket_events():
    exchange = MagicMock(offset=0)
    exchange._rest_closed_candles.return_value = [[0, '1', '2', '0.5', '1.5', '2', 1]] * 40
    stream = MarketStream(exchange, ('BTCUSDT',), '5m')
    stream.start = lambda: None
    stream.candles['BTCUSDT'] = exchange._rest_closed_candles.return_value
    stream.updated['candle:BTCUSDT'] = __import__('time').monotonic()
    assert len(stream.closed_candles('BTCUSDT')) == 40


def test_connected_socket_without_market_events_is_not_reported_as_live():
    stream = MarketStream(MagicMock(offset=0), ('BTCUSDT',), '5m')
    stream.state['status'] = 'connected'
    state = stream.snapshot()
    assert state['status'] == 'no_market_events'
    assert state['fresh_marks'] == 0


def test_open_position_can_expand_public_subscription():
    stream = MarketStream(MagicMock(offset=0), ('BTCUSDT',), '5m')
    assert stream.add_symbols(['AGTUSDT']) is True
    assert stream.add_symbols(['AGTUSDT']) is False
    assert stream.symbols == ('BTCUSDT', 'AGTUSDT')
    assert stream._restart_requested is True


def test_market_route_and_no_event_reconnect_backoff(monkeypatch):
    import asyncio
    import quant.market_stream as module

    stream = MarketStream(MagicMock(offset=0), ('BTCUSDT',), '5m')
    urls, delays = [], []

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def receive(self):
            raise asyncio.TimeoutError

    class Session:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def ws_connect(self, url, **kwargs):
            urls.append(url)
            return Socket()

    async def backoff(delay):
        delays.append(delay)
        if len(delays) == 3:
            stream._stop.set()

    monkeypatch.setattr(module.aiohttp, 'ClientSession', Session)
    monkeypatch.setattr(module.asyncio, 'sleep', backoff)
    asyncio.run(stream._consume())
    assert all(url == 'wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s/btcusdt@kline_5m' for url in urls)
    assert delays == [1, 2, 4]
    assert stream.snapshot()['status'] == 'reconnecting'
    assert stream.snapshot()['reconnects'] == 3
