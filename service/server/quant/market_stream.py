"""Binance public market stream with REST seeding and reconnect fallback."""
import asyncio
import json
import logging
import threading
import time
from datetime import datetime, timezone

import aiohttp


class MarketStream:
    def __init__(self, exchange, symbols, interval):
        self.exchange = exchange
        self.symbols = tuple(symbols)
        self.interval = interval
        self.marks = {}
        self.candles = {symbol: [] for symbol in self.symbols}
        self.updated = {}
        self.rest_fallbacks = 0
        self.state = {'status': 'starting', 'source': 'Binance public WebSocket', 'reconnects': 0}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        for symbol in self.symbols:
            try:
                self.candles[symbol] = list(self.exchange._rest_closed_candles(symbol))[-499:]
                self.updated['candle:' + symbol] = time.monotonic()
            except Exception:
                logging.warning('WebSocket REST candle seed failed for %s', symbol)
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name='binance-market-stream', daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def _run(self):
        try:
            asyncio.run(self._consume())
        except Exception:
            logging.exception('Binance market WebSocket stopped')
            with self._lock:
                self.state = {'status': 'stopped', 'source': 'Binance public WebSocket', 'reconnects': self.state['reconnects']}

    async def _consume(self):
        streams = []
        for symbol in self.symbols:
            lower = symbol.lower()
            streams.extend((f'{lower}@markPrice@1s', f'{lower}@kline_{self.interval}'))
        url = 'wss://fstream.binance.com/market/stream?streams=' + '/'.join(streams)
        delay = 1
        while not self._stop.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20, autoping=True, autoclose=True) as ws:
                        with self._lock:
                            self.state = {'status': 'connected', 'source': 'Binance public WebSocket', 'reconnects': self.state['reconnects']}
                        while not self._stop.is_set():
                            try:
                                message = await asyncio.wait_for(ws.receive(), timeout=15)
                            except asyncio.TimeoutError:
                                # A TCP/WebSocket connection can remain open while no events arrive.
                                # Force reconnect so the REST fallback does not stay stale.
                                raise
                            if self._stop.is_set():
                                break
                            if message.type == aiohttp.WSMsgType.TEXT:
                                self._message(message.data)
                                if any(time.monotonic() - self.updated.get('mark:' + s, 0) <= 5 for s in self.symbols):
                                    delay = 1
                            elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                raise ConnectionError('Market WebSocket closed')
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                with self._lock:
                    self.state = {'status': 'reconnecting', 'source': 'Binance public WebSocket', 'reconnects': self.state['reconnects'] + 1,
                                  'error': type(exc).__name__}
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    def _message(self, raw):
        try:
            payload = json.loads(raw)
            data = payload.get('data', payload)
            event = data.get('e')
            if event == 'markPriceUpdate':
                symbol, price = data['s'], float(data['p'])
                if price > 0:
                    with self._lock:
                        self.marks[symbol] = price
                        self.updated['mark:' + symbol] = time.monotonic()
            elif event == 'kline':
                kline = data['k']
                symbol = data['s']
                row = [kline['t'], kline['o'], kline['h'], kline['l'], kline['c'], kline['v'], kline['T']]
                with self._lock:
                    rows = self.candles.setdefault(symbol, [])
                    existing = next((i for i, row0 in enumerate(rows) if int(row0[0]) == int(row[0])), None)
                    if existing is None:
                        rows.append(row)
                    else:
                        rows[existing] = row
                    rows.sort(key=lambda item: int(item[0]))
                    self.candles[symbol] = rows[-499:]
                    self.updated['candle:' + symbol] = time.monotonic()
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            logging.debug('Ignored malformed Binance market event')

    def mark(self, symbol):
        with self._lock:
            value, updated = self.marks.get(symbol), self.updated.get('mark:' + symbol, 0)
        if value is not None and time.monotonic() - updated <= 5:
            return value
        with self._lock:
            self.rest_fallbacks += 1
        return self.exchange._rest_mark(symbol)

    def closed_candles(self, symbol):
        now = int(time.time() * 1000) + self.exchange.offset
        with self._lock:
            rows = list(self.candles.get(symbol, []))
            updated = self.updated.get('candle:' + symbol, 0)
        if rows and len(rows) >= 40 and time.monotonic() - updated <= 90:
            return [row for row in rows if int(row[6]) < now]
        with self._lock:
            self.rest_fallbacks += 1
        return self.exchange._rest_closed_candles(symbol)

    def snapshot(self):
        with self._lock:
            fresh_marks = sum(time.monotonic() - self.updated.get('mark:' + s, 0) <= 5 for s in self.symbols)
            status = self.state['status']
            if status == 'connected' and not fresh_marks:
                status = 'no_market_events'
            return dict(self.state) | {'status': status, 'updated_at': datetime.now(timezone.utc).isoformat(),
                                       'symbols': list(self.symbols), 'fresh_marks': fresh_marks,
                                       'rest_fallbacks': self.rest_fallbacks}


class MarketStreamMixin:
    def _start_market_stream(self):
        self.market_stream = MarketStream(self, self.cfg.symbols, self.cfg.interval)
        self.market_stream.start()

    def _stop_market_stream(self):
        if getattr(self, 'market_stream', None):
            self.market_stream.stop()

    def _rest_mark(self, symbol):
        return super().mark(symbol)

    def _rest_closed_candles(self, symbol):
        return super().closed_candles(symbol)

    def mark(self, symbol):
        return self.market_stream.mark(symbol) if getattr(self, 'market_stream', None) else super().mark(symbol)

    def closed_candles(self, symbol):
        return self.market_stream.closed_candles(symbol) if getattr(self, 'market_stream', None) else super().closed_candles(symbol)
