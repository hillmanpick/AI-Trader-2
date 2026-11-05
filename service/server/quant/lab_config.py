"""Explicit limits for public market research; no exchange credentials."""
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class LabConfig:
    enabled: bool = field(default_factory=lambda: os.getenv('QUANT_LAB_ENABLED', 'false').lower() == 'true')
    redis_url: str = field(default_factory=lambda: os.getenv('QUANT_REDIS_URL', 'redis://redis:6379/1'))
    directory: str = field(default_factory=lambda: os.getenv('QUANT_LAB_DIR', '/data/lab'))
    candidates: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_CANDIDATES', '12')))
    min_volume: float = field(default_factory=lambda: float(os.getenv('QUANT_LAB_MIN_VOLUME_USDT', '20000000')))
    sample_seconds: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_SAMPLE_SECONDS', '5')))
    retention_days: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_RETENTION_DAYS', '14')))
    raw_mb_day: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_RAW_MB_DAY', '512')))
    max_spread_bps: float = field(default_factory=lambda: float(os.getenv('QUANT_PAPER_MAX_SPREAD_BPS', '10')))
    depth_bps: float = field(default_factory=lambda: float(os.getenv('QUANT_PAPER_DEPTH_RANGE_BPS', '10')))
    horizon_seconds: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_HORIZON_SECONDS', '300')))
    min_samples: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_MIN_SAMPLES', '10000')))
    min_days: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_MIN_DAYS', '7')))
    max_model_age_hours: int = field(default_factory=lambda: int(os.getenv('QUANT_LAB_MODEL_MAX_AGE_HOURS', '48')))
    auto_train: bool = field(default_factory=lambda: os.getenv('QUANT_LAB_AUTO_TRAIN', 'true').lower() == 'true')
    # Paper-only research switch. It may inspect a rejected model, but it never
    # makes that model eligible for live/testnet execution.
    exploration_mode: bool = field(default_factory=lambda: os.getenv('QUANT_PAPER_EXPLORATION_MODE', 'false').lower() == 'true')
    exploration_threshold: float = field(default_factory=lambda: float(os.getenv('QUANT_PAPER_EXPLORATION_THRESHOLD', '.45')))
    event_max_gb: float = field(default_factory=lambda: float(os.getenv('QUANT_EVENT_MAX_GB','2')))
    trade_shard_size: int = field(default_factory=lambda: int(os.getenv('QUANT_TRADE_SHARD_SIZE','128')))
    all_market_trades: bool = field(default_factory=lambda: os.getenv('QUANT_ALL_MARKET_TRADES','true').lower() == 'true')
    market_data_key: str = field(default_factory=lambda: os.getenv('BINANCE_MARKET_DATA_KEY',''))
    event_validation_seconds: int = field(default_factory=lambda: int(os.getenv('QUANT_EVENT_VALIDATION_SECONDS','3600')))

    def validate(self):
        import math
        if not 1 <= self.candidates <= 40 or not 1 <= self.sample_seconds <= 60:
            raise ValueError('Invalid lab subscription/sample limits')
        if not 1 <= self.retention_days <= 90 or not 16 <= self.raw_mb_day <= 4096:
            raise ValueError('Invalid lab retention/storage limits')
        if not 30 <= self.horizon_seconds <= 3600 or self.min_samples < 1000 or self.min_days < 1:
            raise ValueError('Invalid training evidence limits')
        if any(not math.isfinite(v) or v <= 0 for v in (self.min_volume, self.max_spread_bps, self.depth_bps)):
            raise ValueError('Invalid universe/liquidity limits')
        if not math.isfinite(self.event_max_gb) or not .1 <= self.event_max_gb <= 1024 or not 1 <= self.trade_shard_size <= 200:
            raise ValueError('Invalid event lake/shard limits')
        if not math.isfinite(self.exploration_threshold) or not .4 <= self.exploration_threshold <= .9:
            raise ValueError('Invalid paper exploration threshold')
        if not 1800 <= self.event_validation_seconds <= 86400:
            raise ValueError('Invalid event validation window')
        return self
