"""Bound retries after exchange denials without retaining URLs or credentials."""
import math
import json
import os
import threading
import time
from collections.abc import Mapping
from email.utils import parsedate_to_datetime


class RestCooldown(RuntimeError):
    def __init__(self, status, seconds):
        self.status = status
        self.retry_after = max(1, math.ceil(seconds))
        super().__init__(f'Binance REST HTTP {status}：接口拒绝后冷却中，{self.retry_after} 秒后重试')


class RestGuard:
    SHARED_KEY = 'quant:binance:rest-cooldown'
    EXTEND = """
local old = redis.call('GET', KEYS[1])
local deadline = tonumber(ARGV[1])
if not old or tonumber(cjson.decode(old)['until']) < deadline then
    redis.call('SET', KEYS[1], cjson.encode({['until']=deadline, status=tonumber(ARGV[2])}))
    redis.call('EXPIREAT', KEYS[1], math.ceil(deadline))
end
return 1
"""

    def __init__(self):
        self.until = 0.
        self.status = None
        self.failures = 0
        self._lock = threading.RLock()
        self._shared_next_read = 0.
        self._shared = None
        url = os.getenv('QUANT_REST_GUARD_REDIS_URL', '')
        if url:
            import redis
            self._shared = redis.Redis.from_url(url, decode_responses=True,
                                                socket_timeout=.2, socket_connect_timeout=.2)

    def _refresh_shared(self):
        now = time.monotonic()
        if self._shared is None or now < self._shared_next_read:
            return
        self._shared_next_read = now + 1
        try:
            raw = self._shared.get(self.SHARED_KEY)
            if raw:
                value = json.loads(raw)
                deadline = now + max(0., float(value['until']) - time.time())
                if deadline > self.until:
                    self.until, self.status = deadline, int(value['status'])
        except Exception:
            # Redis is an optimization; retain the local safety timer if down.
            self._shared_next_read = now + 5

    def remaining(self):
        with self._lock:
            self._refresh_shared()
            return max(0., self.until - time.monotonic())

    def check(self):
        with self._lock:
            if self.remaining() > 0:
                raise RestCooldown(self.status, self.remaining())

    def observe(self, status, headers=None):
        with self._lock:
            minimum = {403: 300, 418: 900, 429: 60, 451: 1800}.get(status)
            if minimum is None:
                if status < 400:
                    self.failures = 0
                return
            headers = headers if isinstance(headers, Mapping) else {}
            raw = headers.get('Retry-After', '')
            try:
                retry = float(raw)
            except (TypeError, ValueError):
                try:
                    retry = parsedate_to_datetime(raw).timestamp() - time.time()
                except (TypeError, ValueError, OverflowError):
                    retry = 0
            if not math.isfinite(retry):
                retry = 0
            # Honor an exchange-supplied longer delay, including HTTP dates.
            delay = max(min(1800, minimum * 2 ** min(self.failures, 5)), retry)
            self.failures += 1
            self.status = status
            self.until = max(self.until, time.monotonic() + delay)
            if self._shared is not None:
                try:
                    deadline = time.time() + self.remaining()
                    self._shared.eval(self.EXTEND, 1, self.SHARED_KEY, deadline, self.status)
                except Exception:
                    pass
