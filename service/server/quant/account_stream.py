"""Read-only Binance USD-M user data stream with REST reconciliation fallback."""
import asyncio
import json
import logging
import threading
import time

import aiohttp


class AccountStream:
    """Consume ACCOUNT_UPDATE events without exposing order or account-write methods."""

    def __init__(self, exchange):
        self.exchange = exchange
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._account = {}
        self._positions = {}
        self._seeded = False
        self._connected = False
        self._listen_key = None
        self._last_rest_sync = 0.0
        self._last_event = 0.0
        self._state = {'status': 'starting', 'source': 'Binance private User Data WebSocket', 'reconnects': 0}

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name='binance-account-stream', daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def seed(self, account, rows):
        """Seed and periodically replace state from a complete signed REST snapshot."""
        positions = {}
        for row in rows:
            key = (str(row.get('symbol', '')), str(row.get('positionSide', 'BOTH')))
            if key[0] and float(row.get('positionAmt', 0) or 0):
                positions[key] = dict(row)
        with self._lock:
            self._account = json.loads(json.dumps(account))
            self._positions = positions
            self._seeded = True
            self._last_rest_sync = time.monotonic()

    def can_serve(self):
        with self._lock:
            return self._seeded and self._connected and not self._needs_rest_locked()

    def needs_rest(self):
        with self._lock:
            return not self._seeded or not self._connected or self._needs_rest_locked()

    def _needs_rest_locked(self):
        return time.monotonic() - self._last_rest_sync >= 300

    def snapshot(self):
        with self._lock:
            return (json.loads(json.dumps(self._account)),
                    [dict(row) for row in self._positions.values()])

    def _run(self):
        try:
            asyncio.run(self._consume())
        except Exception:
            logging.exception('Binance account WebSocket stopped')
            with self._lock:
                self._connected = False
                self._state = {'status': 'stopped', 'source': 'Binance private User Data WebSocket',
                               'reconnects': self._state['reconnects']}

    async def _consume(self):
        delay = 1
        while not self._stop.is_set():
            listen_key = None
            try:
                listen_key = await asyncio.to_thread(self.exchange.create_listen_key)
                with self._lock:
                    self._listen_key = listen_key
                    self._connected = False
                url = f'wss://fstream.binance.com/ws/{listen_key}'
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20, autoping=True, autoclose=True) as ws:
                        with self._lock:
                            self._connected = True
                            self._state = {'status': 'connected', 'source': 'Binance private User Data WebSocket',
                                           'reconnects': self._state['reconnects']}
                        delay = 1
                        keepalive = asyncio.create_task(self._keepalive(listen_key))
                        try:
                            while not self._stop.is_set():
                                message = await asyncio.wait_for(ws.receive(), timeout=35)
                                if message.type == aiohttp.WSMsgType.TEXT:
                                    self._message(message.data)
                                elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    raise ConnectionError('Account WebSocket closed')
                        finally:
                            keepalive.cancel()
                            await asyncio.gather(keepalive, return_exceptions=True)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError, RuntimeError, ValueError) as exc:
                with self._lock:
                    self._connected = False
                    self._state = {'status': 'reconnecting', 'source': 'Binance private User Data WebSocket',
                                   'reconnects': self._state['reconnects'] + 1,
                                   'error': type(exc).__name__}
                if not self._stop.is_set():
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

    async def _keepalive(self, listen_key):
        while not self._stop.is_set():
            await asyncio.sleep(25 * 60)
            if self._stop.is_set():
                return
            try:
                await asyncio.to_thread(self.exchange.keepalive_listen_key, listen_key)
            except Exception:
                logging.warning('Binance account WebSocket listenKey keepalive failed')

    def _message(self, raw):
        try:
            payload = json.loads(raw)
            event = payload.get('e')
            if event == 'listenKeyExpired':
                raise RuntimeError('Binance account listenKey expired')
            if event != 'ACCOUNT_UPDATE':
                return
            account = payload.get('a') or {}
            with self._lock:
                for balance in account.get('B', []):
                    asset = str(balance.get('a', ''))
                    if not asset:
                        continue
                    assets = self._account.setdefault('assets', [])
                    current = next((row for row in assets if row.get('asset') == asset), None)
                    if current is None:
                        current = {'asset': asset}
                        assets.append(current)
                    if 'wb' in balance:
                        current['walletBalance'] = balance['wb']
                    if 'cw' in balance:
                        current['availableBalance'] = balance['cw']
                    if asset == 'USDT' and 'wb' in balance:
                        self._account['totalWalletBalance'] = balance['wb']
                for position in account.get('P', []):
                    symbol = str(position.get('s', ''))
                    side = str(position.get('ps', 'BOTH'))
                    key = (symbol, side)
                    amount = float(position.get('pa', 0) or 0)
                    if not symbol:
                        continue
                    current = self._positions.get(key, {'symbol': symbol, 'positionSide': side})
                    current.update({'positionAmt': str(amount), 'entryPrice': position.get('ep', current.get('entryPrice', 0)),
                                    'unRealizedProfit': position.get('up', current.get('unRealizedProfit', 0)),
                                    'isolatedWallet': position.get('iw', current.get('isolatedWallet', 0)),
                                    'marginType': position.get('mt', current.get('marginType', 'cross'))})
                    if amount:
                        self._positions[key] = current
                    else:
                        self._positions.pop(key, None)
                self._last_event = time.monotonic()
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            logging.debug('Ignored malformed Binance account event')

    def snapshot_status(self):
        with self._lock:
            return dict(self._state) | {
                'seeded': self._seeded,
                'last_event_at_monotonic': self._last_event or None,
                'rest_reconcile_seconds': 300,
            }
