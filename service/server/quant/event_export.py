"""Export verified Parquet event segments to an external ClickHouse sink."""
import json
import os

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


def main():
    # Do not print the URL or credentials; the catalog stores only a target hash.
    print(json.dumps(export_once(), ensure_ascii=False))


if __name__ == '__main__':
    main()
