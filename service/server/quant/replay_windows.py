"""Find diagnostic multi-symbol replay windows without relaxing strict validation.

This is a data-quality tool, not a model holdout selector. Choosing historical
windows for their continuity cannot replace fixed, chronological model holdouts.
No exchange client or existing simulation ledger is used.
"""
import argparse
import json
import tempfile
from collections import deque
from pathlib import Path

from .book_replay import HistoricalBookReplay, ReplayValidationError
from .event_lake import EventLake
from .execution_policy import fingerprint
from .execution_state import OrderJournal, SimulatedBroker


KINDS = {'depthSnapshot', 'depthUpdate', 'trade', 'depthStatus', 'feedStatus'}


def snapshot_candidates(events, symbols, start_ns, end_ns, duration_ns,
                        snapshot_span_ns, limit):
    """Keep the latest bounded set of starts containing every required snapshot."""
    latest, candidates, last_start, count = {}, deque(maxlen=limit), -1, 0
    for event in events:
        kind, symbol, payload = event['kind'], event['symbol'], event['payload']
        if kind == 'depthStatus':
            for affected in payload.get('symbols', [symbol] if symbol else []):
                latest.pop(affected, None)
        elif kind == 'depthSnapshot' and symbol in symbols:
            latest[symbol] = event['received_ns']
            if symbols <= latest.keys():
                first, last = min(latest.values()), max(latest.values())
                # Match the millisecond CLI boundaries and include ALL snapshots,
                # including the earliest one; starting at the latest loses books.
                start = first // 1_000_000 * 1_000_000
                if (last-first <= snapshot_span_ns and last <= start+duration_ns and start >= start_ns
                        and start+duration_ns <= end_ns and start > last_start):
                    candidates.append(start)
                    last_start = start
                    count += 1
    return list(reversed(candidates)), count


def find_windows(lake, symbols, start_ns, end_ns, *, duration_seconds=180,
                 snapshot_span_seconds=65, max_attempts=20):
    symbols = set(symbols)
    if (not symbols or '' in symbols or start_ns < 0 or end_ns <= start_ns
            or type(duration_seconds) is not int or duration_seconds <= 0
            or duration_seconds*10**9 > end_ns-start_ns
            or type(snapshot_span_seconds) is not int or snapshot_span_seconds <= 0
            or type(max_attempts) is not int or not 1 <= max_attempts <= 100):
        raise ValueError('Explicit symbols, valid boundaries and bounded search settings are required')
    duration_ns = duration_seconds*10**9
    candidates, count = snapshot_candidates(
        lake.iter_events(start_ns, end_ns, symbols | {''}, {'depthSnapshot', 'depthStatus'}),
        symbols, start_ns, end_ns, duration_ns, snapshot_span_seconds*10**9, max_attempts)
    attempts, selected = [], None
    for start in candidates:
        end = start+duration_ns
        interruptions = []

        def events():
            for event in lake.iter_events(start, end, symbols | {''}, KINDS):
                if event['kind'] in ('depthStatus', 'feedStatus'):
                    p = event['payload']
                    affected = symbols & set(p.get('symbols', [event['symbol']] if event['symbol'] else []))
                    if affected:
                        # A later snapshot cannot erase a recorded feed reset.
                        if len(interruptions) < 100:
                            interruptions.append({'kind': event['kind'], 'status': p.get('status'),
                                'received_ns': event['received_ns'], 'symbols': sorted(affected)})
                yield event

        with tempfile.TemporaryDirectory(prefix='quant-replay-window-') as directory:
            replay = HistoricalBookReplay(
                SimulatedBroker(OrderJournal(Path(directory)/'orders.db')),
                require_complete=True, expected_symbols=symbols)
            try:
                report = replay.run(events())
            except ReplayValidationError as exc:
                report = exc.report
        reasons = list(report['invalid_reasons'])
        if interruptions:
            reasons.append('recorded_feed_reset')
        if report['last_received_ns'] < end-2*10**9:
            reasons.append('incomplete_event_window')
        for symbol in sorted(symbols):
            book = replay.books.get(symbol)
            if book and book.ready and end//1_000_000-book.updated_ms > 2000:
                reasons.append('stale_depth_at_requested_end:'+symbol)
        report.update(valid=not reasons, invalid_reasons=sorted(set(reasons)),
                      requested_window={'start_ms': start//1_000_000, 'end_ms': end//1_000_000,
                                        'symbols': sorted(symbols)}, feed_resets=interruptions)
        segments = lake.segments(start, end)
        report['manifest_hash'] = fingerprint(
            [{k: s[k] for k in ('path', 'sha256', 'first_ns', 'last_ns')} for s in segments])
        report['segments'] = len(segments)
        report.pop('fills', None)
        attempts.append(report)
        if report['valid']:
            selected = report
            break
    return {'schema': 'strict-replay-window-search-v1', 'found': selected is not None,
            'status': 'found' if selected else 'no_valid_window_in_bounded_search',
            'search': {'start_ns': start_ns, 'end_ns': end_ns, 'symbols': sorted(symbols),
                       'duration_seconds': duration_seconds, 'snapshot_span_seconds': snapshot_span_seconds,
                       'max_attempts': max_attempts, 'snapshot_candidates': count,
                       'candidate_limit_applied': count > max_attempts,
                       'order': 'latest snapshot candidates first'},
            'selected': selected, 'attempts': attempts,
            'limitations': ['Diagnostic continuity search only; never selects model holdout windows',
                            'No profitability, live latency or exchange execution claim',
                            'No valid result does not prove all historical windows are invalid']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--symbols', required=True)
    parser.add_argument('--lookback-seconds', type=int, default=1800)
    parser.add_argument('--end-ms', type=int)
    parser.add_argument('--duration-seconds', type=int, default=180)
    parser.add_argument('--snapshot-span-seconds', type=int, default=65)
    parser.add_argument('--max-attempts', type=int, default=20)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if not 1 <= args.lookback_seconds <= 86400:
        parser.error('lookback-seconds must be between 1 and 86400')
    lake = EventLake(args.directory)
    last_ns = lake.stats()['last_ns']
    if last_ns is None:
        parser.error('No stored events')
    end_ns = args.end_ms*1_000_000 if args.end_ms is not None else last_ns
    try:
        report = find_windows(lake, {s.strip().upper() for s in args.symbols.split(',') if s.strip()},
            max(0, end_ns-args.lookback_seconds*10**9), end_ns,
            duration_seconds=args.duration_seconds, snapshot_span_seconds=args.snapshot_span_seconds,
            max_attempts=args.max_attempts)
    except ValueError as exc:
        parser.error(str(exc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'found': report['found'], 'search': report['search'],
                      'attempts': len(report['attempts']),
                      'selected_window': report['selected']['requested_window'] if report['found'] else None}))
    if not report['found']:
        parser.exit(2, 'No valid window in bounded search; diagnostics saved to output\n')


if __name__ == '__main__':
    main()
