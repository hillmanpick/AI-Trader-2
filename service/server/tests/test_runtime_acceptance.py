import json
import os
import sys
from unittest.mock import patch


SERVER_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

from quant import runtime_acceptance


def payload(model_ready=False):
    return {
        "enabled": True,
        "collector": {
            "status": "collecting", "universe_count": 2,
            "trade_symbols_seen": 2, "all_market_trades": True,
            "errors": 0, "raw_dropped": 0, "ready_symbols": ["BTCUSDT", "ETHUSDT"],
            "depth_gaps": 0, "depth_gap_active": 0, "depth_gap_symbols": [], "blocked": {},
        },
        "redis_connected": True,
        "history": {"clean_training": {"samples": 10000, "days": 7}},
        "model": {"ready": model_ready, "reason": "test"},
        "event_history": {"format": "Parquet/ZSTD events-v2", "rows": 10,
                           "bytes": 100, "max_bytes": 1000,
                           "clickhouse": {"configured": False}},
        "event_replay": {"strict": True, "valid": True, "sequence_gaps": 0, "events": 10},
    }, {
        "config": {"paper_dynamic_leverage": True, "paper_margin": 2000,
                   "paper_max_margin": 6000, "paper_major_max_leverage": 100,
                   "paper_min_leverage": 2, "paper_max_leverage": 20},
        "paused": True, "positions": {},
    }, {
        "read_only": True, "execution_enabled": False,
        "account_stream": {"status": "connected"},
    }


def test_runtime_acceptance_passes_only_with_model_gate(capsys):
    with patch.object(runtime_acceptance, "get", side_effect=payload(model_ready=True)):
        assert runtime_acceptance.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["failed"] is False
    assert all(check["ok"] for check in report["checks"])


def test_runtime_acceptance_keeps_model_gate_hard(capsys):
    with patch.object(runtime_acceptance, "get", side_effect=payload(model_ready=False)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"model_oos_gate"}


def test_runtime_acceptance_allows_candidate_rotation_warmup(capsys):
    lab, paper, live = payload(model_ready=False)
    lab["collector"]["ready_symbols"] = ["BTCUSDT"]
    lab["collector"]["blocked"] = {"ETHUSDT": "Feature warmup needs 60 seconds"}
    with patch.object(runtime_acceptance, "get", side_effect=(lab, paper, live)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"model_oos_gate"}


def test_runtime_acceptance_allows_depth_feature_warmup(capsys):
    lab, paper, live = payload(model_ready=False)
    lab["collector"]["ready_symbols"] = ["BTCUSDT"]
    lab["collector"]["blocked"] = {"ETHUSDT": "Event OFI needs five seconds of continuous depth"}
    with patch.object(runtime_acceptance, "get", side_effect=(lab, paper, live)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"model_oos_gate"}


def test_runtime_acceptance_allows_recovered_historical_depth_gap(capsys):
    lab, paper, live = payload(model_ready=False)
    lab["collector"]["depth_gaps"] = 2
    lab["collector"]["depth_gap_active"] = 0
    with patch.object(runtime_acceptance, "get", side_effect=(lab, paper, live)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"model_oos_gate"}


def test_runtime_acceptance_blocks_active_depth_gap(capsys):
    lab, paper, live = payload(model_ready=False)
    lab["collector"]["depth_gaps"] = 2
    lab["collector"]["depth_gap_active"] = 1
    lab["collector"]["depth_gap_symbols"] = ["BTCUSDT"]
    with patch.object(runtime_acceptance, "get", side_effect=(lab, paper, live)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"candidate_books_ready", "model_oos_gate"}


def test_runtime_acceptance_allows_liquidity_filtered_candidate(capsys):
    lab, paper, live = payload(model_ready=False)
    lab["collector"]["ready_symbols"] = ["BTCUSDT"]
    lab["collector"]["blocked"] = {"ONUSDT": "Spread above limit"}
    with patch.object(runtime_acceptance, "get", side_effect=(lab, paper, live)):
        assert runtime_acceptance.main() == 1
    report = json.loads(capsys.readouterr().out)
    failed = {check["name"] for check in report["checks"] if not check["ok"]}
    assert failed == {"model_oos_gate"}
