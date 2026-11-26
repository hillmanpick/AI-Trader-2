"""Read-only Binance public futures depth-stream path and freshness probe."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time

import aiohttp

from .execution_state import percentile

MARKET_URL = 'wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms'


def distribution(values):
    return {'count': len(values), 'p50_ms': percentile(values, .5),
            'p95_ms': percentile(values, .95), 'p99_ms': percentile(values, .99),
            'max_ms': max(values) if values else None}


def eligible(report, min_frames, max_age_p95_ms):
    age = report.get('exchange_event_age', {}).get('p95_ms')
    return bool(report.get('connected') and report.get('frames', 0) >= min_frames
                and age is not None and age <= max_age_p95_ms
                and report.get('protocol_errors', 0) == 0)


async def run_probe(duration_seconds=20, min_frames=30, max_age_p95_ms=250,
                    connect_timeout=5, session_factory=None):
    if not 1 <= duration_seconds <= 120 or not 1 <= min_frames <= 10000:
        raise ValueError('Probe duration or sample count outside bounds')
    if not math.isfinite(max_age_p95_ms) or not 1 <= max_age_p95_ms <= 5000:
        raise ValueError('Invalid freshness threshold')
    factory = session_factory or aiohttp.ClientSession
    report = {'schema': 'market-path-probe-v1',
        'at': datetime.now(timezone.utc).isoformat(),
        'source': 'Binance USD-M public depth WebSocket', 'url': MARKET_URL,
        'connected': False, 'frames': 0, 'protocol_errors': 0, 'connect_ms': None,
        'exchange_event_age': distribution([]), 'frame_interval': distribution([]),
        'minimum_frames': min_frames, 'max_allowed_event_age_p95_ms': max_age_p95_ms,
        'orders_sent': 0, 'credentials_used': False, 'fallback_recommended': 'server'}
    ages, intervals = [], []
    started = time.perf_counter_ns()
    try:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=connect_timeout, sock_read=None)
        async with factory(timeout=timeout, trust_env=False) as session:
            async with session.ws_connect(MARKET_URL, heartbeat=20, autoping=True,
                    autoclose=True, max_msg_size=1024*1024) as ws:
                report['connected'] = True
                report['connect_ms'] = (time.perf_counter_ns()-started)/1e6
                deadline = time.monotonic()+duration_seconds
                previous = None
                while time.monotonic() < deadline and len(ages) < min_frames:
                    try:
                        message = await asyncio.wait_for(ws.receive(),
                            timeout=min(3, max(.01, deadline-time.monotonic())))
                    except asyncio.TimeoutError:
                        report['receive_timeout'] = True
                        break
                    if message.type != aiohttp.WSMsgType.TEXT:
                        if message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            report['disconnect_type'] = message.type.name
                            break
                        continue
                    try:
                        data = message.json().get('data', {})
                        if (data.get('e') != 'depthUpdate' or data.get('s') != 'BTCUSDT'
                                or type(data.get('E')) is not int
                                or not isinstance(data.get('b'), list)
                                or not isinstance(data.get('a'), list)):
                            raise ValueError('Unexpected depth frame')
                        age = time.time_ns()//1_000_000-data['E']
                        if age < -1000 or age > 60000:
                            raise ValueError('Invalid exchange event timestamp')
                        ages.append(max(0, age))
                        now = time.perf_counter_ns()
                        if previous is not None:
                            intervals.append((now-previous)/1e6)
                        previous = now
                    except (ValueError, TypeError, KeyError, AttributeError):
                        report['protocol_errors'] += 1
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        report['error'] = 'market_websocket_unavailable'
    report['elapsed_ms'] = (time.perf_counter_ns()-started)/1e6
    report['frames'] = len(ages)
    report['exchange_event_age'] = distribution(ages)
    report['frame_interval'] = distribution(intervals)
    report['eligible_for_local_primary'] = eligible(report, min_frames, max_age_p95_ms)
    report['fallback_recommended'] = 'local' if report['eligible_for_local_primary'] else 'server'
    report['limitations'] = [
        'Public depth receive-age is not signed account RPC RTT or order acknowledgement latency',
        'A short probe does not qualify a node for sole execution leadership',
        'Soak testing and failover fencing are required before changing runtime roles']
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration-seconds', type=int, default=20)
    parser.add_argument('--min-frames', type=int, default=30)
    parser.add_argument('--max-age-p95-ms', type=float, default=250)
    parser.add_argument('--connect-timeout', type=float, default=5)
    parser.add_argument('--output')
    args = parser.parse_args()
    result = asyncio.run(run_probe(args.duration_seconds, args.min_frames,
        args.max_age_p95_ms, args.connect_timeout))
    text = json.dumps(result, ensure_ascii=False, allow_nan=False, indent=2)
    if args.output:
        Path(args.output).write_text(text, encoding='utf-8')
    print(text)


if __name__ == '__main__':
    main()
