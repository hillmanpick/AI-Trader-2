"""Optional local computation, with the server as the only paper order writer.

Exact input, policy and strategy-code hashes bind each cached result. A missing,
expired or superseded result causes immediate server computation, never a wait
or an order. This module contains no exchange client or account credentials.
"""
import hashlib
import json
import math
import time
from collections import Counter
from pathlib import Path

PREFIX = 'quant:compute:v1:'
FILES = ('quant_strategy.py', 'lab_features.py', 'execution_policy.py')
RESULT_TTL_MS = 1500


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def strategy_bundle():
    root = Path(__file__).parent
    sources = {name: (root / name).read_text(encoding='utf-8') for name in FILES}
    return {'code_id': digest(sources), 'sources': sources}


def job_id(row, policy, code_id):
    return digest({'row': row, 'policy': policy, 'code_id': code_id})


def validate_observations(observations, strategies):
    if not isinstance(observations, dict) or set(observations) != set(strategies):
        raise ValueError('Strategy result set does not match the frozen strategies')
    if len(canonical(observations).encode()) > 32768:
        raise ValueError('Strategy result too large')
    for name, value in observations.items():
        if (not isinstance(value, dict) or value.get('strategy', name) != name
                or value.get('action') not in {'long', 'short', 'hold'}
                or type(value.get('side')) is not int
                or value['side'] != {'long': 1, 'short': -1, 'hold': 0}[value['action']]
                or type(value.get('score')) not in (int, float)
                or not math.isfinite(value['score']) or not isinstance(value.get('reason'), str)):
            raise ValueError('Malformed strategy observation')
    return observations


class ComputeReader:
    """Use an exact, current local result; compute synchronously on every miss."""
    def __init__(self, client, code_id=None, enabled=False, now_ms=None, telemetry=None):
        self.client, self.enabled = client, enabled
        self.code_id = code_id or strategy_bundle()['code_id']
        self.now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        self.telemetry = telemetry
        self.counts = Counter()
        self.last_reason = 'disabled' if not enabled else 'no_local_result'
        self.last_source = 'server'
        self.last_path = {}
        self._next_status = 0.

    def observations(self, row, policy, strategies, evaluate):
        selected = None
        if self.enabled:
            started = time.perf_counter_ns()
            try:
                identity = job_id(row, policy, self.code_id)
                raw_state, raw_result = self.client.mget([PREFIX+'head', PREFIX+'result:'+identity])
                if raw_state:
                    path=json.loads(raw_state)
                    self.last_path={k:path.get(k) for k in ('at_ms','node','connected','admission','counters')}
                else:
                    self.last_path={'connected':False,'admission':{'eligible':False}}
                if raw_state and raw_result:
                    state, result = json.loads(raw_state), json.loads(raw_result)
                    now = self.now_ms()
                    if (state.get('connected') is True and state.get('code_id') == self.code_id
                            and state.get('admission',{}).get('eligible') is True
                            and result.get('job_id') == identity
                            and result.get('generation') == state.get('generation')
                            and result.get('session') == state.get('session')
                            and 0 <= now-state['at_ms'] <= 3000
                            and 0 <= now-result['at_ms'] <= RESULT_TTL_MS
                            and now <= result['valid_until_ms']
                            and 0 <= now-row['ts'] <= 2000):
                        selected = validate_observations(result['observations'], strategies)
                        self.last_reason = 'exact_local_result'
                    else:
                        self.last_reason = ('local_latency_not_qualified'
                            if not state.get('admission',{}).get('eligible')
                            else 'stale_or_superseded_result')
                else:
                    self.last_reason = 'no_local_result'
            except Exception:
                # Coordination cannot interrupt position management or fallback.
                self.last_reason = 'cache_unavailable_or_invalid'
            finally:
                if self.telemetry:
                    self.telemetry.observe('local_compute_lookup', (time.perf_counter_ns()-started)/1e6)
        if selected is not None:
            self.counts['local_hits'] += 1
            self.last_source = 'local'
            return selected
        self.counts['server_fallbacks'] += 1
        self.last_source = 'server'
        return {name: evaluate(name, row, policy) for name in strategies}

    def status(self):
        return {'enabled': self.enabled, 'decision_source': self.last_source,
                'reason': self.last_reason, 'counters': dict(self.counts),
                'local_path': self.last_path,
                'code_id': self.code_id, 'executor': 'server_only',
                'local_has_exchange_credentials': False, 'real_execution_enabled': False}


def install(controller, enabled=False):
    # Reuse the worker's original direct Redis client. Sizing, freshness,
    # model gates and order writes remain in the existing server code.
    telemetry = getattr(getattr(controller.engine, 'paper_execution', None), 'telemetry', None)
    reader = ComputeReader(controller.lab.bus.client, enabled=enabled, telemetry=telemetry)
    controller.compute_reader = reader
    controller.engine.store.set('hybrid_compute',reader.status())
    return reader
