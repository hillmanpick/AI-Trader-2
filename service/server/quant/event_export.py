"""Export verified Parquet event segments to an external ClickHouse sink."""
import json
import os
import time

from .event_lake import EventLake
from .lab_config import LabConfig


def export_once(cfg=None):
    cfg = cfg or LabConfig()
    url = os.getenv('QUANT_CLICKHOUSE_URL', '').strip()
    if not url:
        raise RuntimeError('QUANT_CLICKHOUSE_URL is not configured')
    lake = EventLake(cfg.directory, int(cfg.event_max_gb * 1024 ** 3), cfg.retention_days * 24)
    count = lake.export_clickhouse(
        url,
        database=os.getenv('QUANT_CLICKHOUSE_DATABASE', 'quant'),
        table=os.getenv('QUANT_CLICKHOUSE_TABLE', 'events'),
        username=os.getenv('QUANT_CLICKHOUSE_USER', ''),
        password=os.getenv('QUANT_CLICKHOUSE_PASSWORD', ''),
    )
    return {'exported_segments': count, 'format': 'Parquet/ZSTD events-v2 (v1 readable)'}


def watch(interval=None, cfg=None):
    """Continuously retry export without making an unset sink fatal."""
    interval = int(os.getenv('QUANT_CLICKHOUSE_EXPORT_INTERVAL_SECONDS', '60') if interval is None else interval)
    if not 10 <= interval <= 3600:
        raise ValueError('ClickHouse export interval must be between 10 and 3600 seconds')
    while True:
        try:
            result = export_once(cfg)
        except Exception as exc:
            # A remote outage must not kill the exporter; credentials and URL
            # are deliberately absent from the log.
            result = {'exported_segments': 0, 'status': 'retrying',
                      'reason': 'ClickHouse export failed; checkpoints retained',
                      'error_type': type(exc).__name__}
        print(json.dumps(result, ensure_ascii=False), flush=True)
        time.sleep(interval)


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--interval', type=int)
    args = parser.parse_args()
    # Do not print the URL or credentials; the catalog stores only a target hash.
    if args.watch:
        watch(args.interval)
    else:
        print(json.dumps(export_once(), ensure_ascii=False))


if __name__ == '__main__':
    main()
