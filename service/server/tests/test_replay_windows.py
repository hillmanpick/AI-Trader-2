import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quant.event_lake import EventLake, envelope
from quant.replay_windows import find_windows, main


def event(kind, ms, symbol='BTCUSDT', **payload):
    return envelope({'e': kind, 's': symbol, 'E': ms, **payload},
                    received_ns=ms*1_000_000, monotonic_ns=ms*1_000_000)


def snapshot(ms, symbol='BTCUSDT', identity=100):
    return event('depthSnapshot', ms, symbol, lastUpdateId=identity,
                 bids=[['99', '5']], asks=[['101', '5']])


def update(ms, symbol='BTCUSDT', identity=101, previous=100, first=100):
    return event('depthUpdate', ms, symbol, U=first, u=identity, pu=previous, b=[], a=[])


def window(ms, bad=False):
    return [snapshot(ms), snapshot(ms+10, 'ETHUSDT'),
            update(ms+20), update(ms+30, 'ETHUSDT'),
            update(ms+2900, identity=104 if bad else 102, previous=103 if bad else 101, first=102),
            update(ms+2910, 'ETHUSDT', identity=102, previous=101, first=102)]


def test_search_tries_older_complete_multisymbol_window_after_latest_sequence_gap(tmp_path):
    lake = EventLake(tmp_path)
    lake.append(window(1000)+window(11000, bad=True))
    result = find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 15_000_000_000, duration_seconds=3)
    assert result['found']
    assert result['search']['snapshot_candidates'] == 2
    assert any(a['sequence_gaps'] > 0 for a in result['attempts'][:-1])
    chosen = result['selected']
    assert chosen['requested_window'] == {'start_ms': 1000, 'end_ms': 4000,
                                         'symbols': ['BTCUSDT', 'ETHUSDT']}
    assert chosen['symbols']['BTCUSDT']['reconstructed_depth_updates'] == 2
    assert chosen['symbols']['ETHUSDT']['reconstructed_depth_updates'] == 2
    assert chosen['execution']['orders'] == 0
    assert chosen['sequence_gaps'] == 0
    assert len(chosen['manifest_hash']) == 64 and chosen['segments'] == 1


def test_search_never_drops_required_symbols_or_shortens_requested_duration(tmp_path):
    lake = EventLake(tmp_path)
    lake.append(window(1000))
    missing = find_windows(lake, {'BTCUSDT', 'ETHUSDT', 'MISSINGUSDT'}, 0, 8_000_000_000,
                           duration_seconds=3)
    assert not missing['found'] and missing['attempts'] == []
    short = find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 8_000_000_000, duration_seconds=6)
    assert not short['found']
    assert 'incomplete_event_window' in short['attempts'][0]['invalid_reasons']
    assert short['attempts'][0]['requested_window']['end_ms'] == 7000
    assert 'stale_depth_at_requested_end:BTCUSDT' in short['attempts'][0]['invalid_reasons']


def test_search_rejects_recorded_feed_reset_even_when_snapshot_recovers(tmp_path):
    lake = EventLake(tmp_path)
    events = [snapshot(1000), update(1020),
              event('depthStatus', 1500, symbol='', symbols=['BTCUSDT'], status='disconnected'),
              snapshot(1600), update(1620),
              update(3900, identity=102, previous=101, first=102)]
    lake.append(events)
    result = find_windows(lake, {'BTCUSDT'}, 0, 4_000_000_000, duration_seconds=3)
    assert not result['found']
    attempt, = result['attempts']
    assert attempt['sequence_gaps'] == 0 and attempt['symbols']['BTCUSDT']['book_ready']
    assert attempt['invalid_reasons'] == ['recorded_feed_reset']
    assert attempt['feed_resets'][0]['status'] == 'disconnected'


def test_search_is_bounded_and_does_not_claim_exhaustive_failure(tmp_path):
    lake = EventLake(tmp_path)
    lake.append(window(1000)+window(11000, bad=True))
    result = find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 15_000_000_000,
                          duration_seconds=3, max_attempts=1)
    assert not result['found'] and len(result['attempts']) == 1
    assert result['search']['candidate_limit_applied']
    assert result['status'] == 'no_valid_window_in_bounded_search'


def test_search_ignores_other_symbol_control_events_but_checks_trade_sequence(tmp_path):
    lake = EventLake(tmp_path)
    events = window(1000)+[
        event('depthStatus', 2000, symbol='', symbols=['OTHERUSDT'], status='disconnected'),
        event('trade', 2100, t=1, p='100', q='1', m=False, T=2100),
        event('trade', 2200, t=3, p='100', q='1', m=False, T=2200)]
    lake.append(sorted(events, key=lambda e: e['received_ns']))
    result = find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 4_000_000_000, duration_seconds=3)
    assert not result['found']
    attempt, = result['attempts']
    assert attempt['feed_resets'] == []
    assert attempt['sequence_gaps'] == 1
    assert attempt['gap_details'][0]['kind'] == 'trade'


def test_search_cli_saves_missing_coverage_diagnostics_and_returns_failure(tmp_path, monkeypatch):
    lake = EventLake(tmp_path)
    lake.append(window(1000))
    output = tmp_path/'search.json'
    monkeypatch.setattr(sys, 'argv', ['replay_windows', '--directory', str(tmp_path), '--symbols',
        'BTCUSDT,MISSINGUSDT', '--end-ms', '8000', '--lookback-seconds', '8',
        '--duration-seconds', '3', '--output', str(output)])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    report = json.loads(output.read_text())
    assert not report['found'] and report['search']['symbols'] == ['BTCUSDT', 'MISSINGUSDT']


def test_search_does_not_reuse_snapshots_from_before_feed_reset(tmp_path):
    lake = EventLake(tmp_path)
    lake.append([snapshot(1000),
        event('depthStatus', 1005, symbol='', symbols=['BTCUSDT'], status='disconnected'),
        snapshot(1010, 'ETHUSDT'), update(1020, 'ETHUSDT')])
    result = find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 5_000_000_000, duration_seconds=3)
    assert not result['found'] and result['search']['snapshot_candidates'] == 0


def test_search_never_bypasses_parquet_checksum_failure(tmp_path):
    lake = EventLake(tmp_path)
    lake.append(window(1000))
    with (lake.directory/lake.segments()[0]['path']).open('ab') as f:
        f.write(b'corrupt')
    with pytest.raises(ValueError, match='checksum'):
        find_windows(lake, {'BTCUSDT', 'ETHUSDT'}, 0, 5_000_000_000, duration_seconds=3)


@pytest.mark.parametrize('settings', [{'duration_seconds': 0}, {'duration_seconds': 20},
                                   {'max_attempts': 0}, {'max_attempts': 101},
                                   {'snapshot_span_seconds': 0}])
def test_search_rejects_invalid_limits(tmp_path, settings):
    with pytest.raises(ValueError):
        find_windows(EventLake(tmp_path), {'BTCUSDT'}, 0, 10_000_000_000, **settings)
