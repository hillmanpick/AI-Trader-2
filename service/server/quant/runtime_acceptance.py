"""Read-only runtime acceptance check for the quant services."""
import json
import os
import sys
import urllib.error
import urllib.request


def get(path):
    base = os.getenv("QUANT_ACCEPTANCE_URL", "http://localhost:8000").rstrip("/")
    token = os.getenv("QUANT_API_TOKEN", "")
    request = urllib.request.Request(base + path, headers={"X-Quant-Token": token})
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def check(report, condition, name, detail=None, hard=True):
    report["checks"].append({"name": name, "ok": bool(condition), "detail": detail})
    if hard and not condition:
        report["failed"] = True


def main():
    report = {"schema": "quant-runtime-acceptance-v1", "failed": False, "checks": []}
    try:
        lab = get("/api/quant/lab/status")
        paper = get("/api/quant/paper/status")
        live = get("/api/quant/live-account")
    except (OSError, ValueError, urllib.error.HTTPError) as exc:
        print(json.dumps({"schema": report["schema"], "failed": True,
                          "error": type(exc).__name__}, ensure_ascii=False))
        return 2

    collector = lab.get("collector") or {}
    history = lab.get("history") or {}
    event_history = lab.get("event_history") or {}
    paper_config = paper.get("config") or {}
    universe = int(collector.get("universe_count") or 0)
    ready = collector.get("ready_symbols") or []

    check(report, lab.get("enabled") is True and collector.get("status") == "collecting",
          "collector_running", collector.get("status"))
    check(report, universe > 0 and int(collector.get("trade_symbols_seen") or 0) >= universe,
          "full_market_trade_coverage", {"universe": universe,
          "trade_symbols_seen": collector.get("trade_symbols_seen")})
    check(report, bool(collector.get("all_market_trades")) and not collector.get("errors")
          and not collector.get("raw_dropped"), "trade_ingest_health",
          {"errors": collector.get("errors"), "raw_dropped": collector.get("raw_dropped")})
    blocked = collector.get("blocked") or {}
    transient = tuple(("Feature warmup", "warming after reconnect", "stale, missing or warming"))
    hard_blocked = {symbol: reason for symbol, reason in blocked.items()
                    if not any(marker.lower() in str(reason).lower() for marker in transient)}
    check(report, bool(ready) and not collector.get("depth_gaps") and not hard_blocked,
          "candidate_books_ready", {"ready": len(ready), "candidate_count": len(collector.get("candidates") or []),
          "transient_blocked": {s: r for s, r in blocked.items() if s not in hard_blocked},
          "hard_blocked": hard_blocked, "depth_gaps": collector.get("depth_gaps")})
    check(report, lab.get("redis_connected") is True, "redis_connected")
    check(report, str(event_history.get("format", "")).startswith("Parquet/ZSTD")
          and int(event_history.get("rows") or 0) > 0
          and int(event_history.get("bytes") or 0) <= int(event_history.get("max_bytes") or 0),
          "event_lake_bounded", {"rows": event_history.get("rows"),
          "bytes": event_history.get("bytes"), "max_bytes": event_history.get("max_bytes")})
    model = lab.get("model") or {}
    check(report, model.get("ready") is True, "model_oos_gate", model)
    check(report, paper_config.get("paper_dynamic_leverage") is True
          and paper_config.get("paper_margin") == 2000
          and paper_config.get("paper_max_margin") == 6000
          and paper_config.get("paper_major_max_leverage") == 100,
          "dynamic_leverage_policy", {k: paper_config.get(k) for k in (
              "paper_dynamic_leverage", "paper_min_leverage", "paper_max_leverage",
              "paper_major_max_leverage", "paper_margin", "paper_max_margin")})
    check(report, paper.get("paused") is True and not paper.get("positions"),
          "paper_safety_state", {"paused": paper.get("paused"),
          "positions": len(paper.get("positions") or {})})
    check(report, live.get("read_only") is True and live.get("execution_enabled") is False,
          "live_read_only", {"read_only": live.get("read_only"),
          "execution_enabled": live.get("execution_enabled")})
    check(report, (live.get("account_stream") or {}).get("status") == "connected",
          "private_account_stream", live.get("account_stream"))

    report["summary"] = {"clean_samples": (history.get("clean_training") or {}).get("samples"),
                          "clean_days": (history.get("clean_training") or {}).get("days"),
                          "model_ready": model.get("ready"),
                          "clickhouse_configured": (event_history.get("clickhouse") or {}).get("configured")}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
