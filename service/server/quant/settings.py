import os
import math
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    mode: str = field(default_factory=lambda: os.getenv('QUANT_MODE', 'paper'))
    enabled: bool = field(default_factory=lambda: os.getenv('QUANT_ENABLED', 'true').lower() == 'true')
    live_enabled: bool = field(default_factory=lambda: os.getenv('QUANT_LIVE_ENABLED', 'false').lower() == 'true')
    observation_only: bool = field(default_factory=lambda: os.getenv('QUANT_OBSERVATION_ONLY', 'false').lower() == 'true')
    api_key: str = field(default_factory=lambda: os.getenv('BINANCE_API_KEY', ''))
    api_secret: str = field(default_factory=lambda: os.getenv('BINANCE_API_SECRET', ''))
    operator_token: str = field(default_factory=lambda: os.getenv('QUANT_API_TOKEN', ''))
    db_path: str = field(default_factory=lambda: os.getenv('QUANT_DB_PATH', 'service/server/data/quant.db'))
    symbols: tuple = field(default_factory=lambda: tuple(s.strip().upper() for s in os.getenv('QUANT_SYMBOLS', 'BTCUSDT,ETHUSDT').split(',') if s.strip()))
    strategy: str = field(default_factory=lambda: os.getenv('QUANT_STRATEGY', 'sma_cross'))
    interval: str = field(default_factory=lambda: os.getenv('QUANT_INTERVAL', '15m'))
    fast: int = field(default_factory=lambda: int(os.getenv('QUANT_SMA_FAST', '10')))
    slow: int = field(default_factory=lambda: int(os.getenv('QUANT_SMA_SLOW', '30')))
    boll_period: int = field(default_factory=lambda: int(os.getenv('QUANT_BOLL_PERIOD', '13')))
    boll_deviation: float = field(default_factory=lambda: float(os.getenv('QUANT_BOLL_DEVIATION', '2')))
    allow_short: bool = field(default_factory=lambda: os.getenv('QUANT_ALLOW_SHORT', 'true').lower() == 'true')
    leverage: int = field(default_factory=lambda: int(os.getenv('QUANT_LEVERAGE', '1')))
    order_notional: float = field(default_factory=lambda: float(os.getenv('QUANT_ORDER_NOTIONAL_USDT', '50')))
    max_notional: float = field(default_factory=lambda: float(os.getenv('QUANT_MAX_TOTAL_NOTIONAL_USDT', '100')))
    max_positions: int = field(default_factory=lambda: int(os.getenv('QUANT_MAX_POSITIONS', '2')))
    stop_pct: float = field(default_factory=lambda: float(os.getenv('QUANT_STOP_LOSS_PCT', '0.02')))
    take_pct: float = field(default_factory=lambda: float(os.getenv('QUANT_TAKE_PROFIT_PCT', '0.04')))
    daily_loss: float = field(default_factory=lambda: float(os.getenv('QUANT_MAX_DAILY_LOSS_USDT', '20')))
    paper_cash: float = field(default_factory=lambda: float(os.getenv('QUANT_PAPER_BALANCE_USDT', '10000')))
    fee_rate: float = field(default_factory=lambda: float(os.getenv('QUANT_PAPER_FEE_RATE', '0.0005')))
    poll_seconds: int = field(default_factory=lambda: int(os.getenv('QUANT_POLL_SECONDS', '10')))
    community_enabled: bool = field(default_factory=lambda: os.getenv('AI4TRADE_ENABLED', 'false').lower() == 'true')
    community_token: str = field(default_factory=lambda: os.getenv('AI4TRADE_TOKEN', ''))
    community_agent_id: str = field(default_factory=lambda: os.getenv('AI4TRADE_AGENT_ID', ''))
    community_poll_seconds: int = field(default_factory=lambda: int(os.getenv('AI4TRADE_POLL_SECONDS', '60')))
    ai_enabled: bool = field(default_factory=lambda: os.getenv('QUANT_AI_ENABLED', 'false').lower() == 'true')
    deepseek_api_key: str = field(default_factory=lambda: os.getenv('DEEPSEEK_API_KEY', ''))
    deepseek_base_url: str = field(default_factory=lambda: os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com'))
    deepseek_model: str = field(default_factory=lambda: os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'))
    ai_min_confidence: float = field(default_factory=lambda: float(os.getenv('QUANT_AI_MIN_CONFIDENCE', '0.65')))
    ai_review_max_age: int = field(default_factory=lambda: int(os.getenv('QUANT_AI_REVIEW_MAX_AGE_SECONDS', '90')))
    ai_advice_seconds: int = field(default_factory=lambda: int(os.getenv('QUANT_AI_ADVICE_SECONDS', '900')))
    paper_review_relaxed: bool = False
    paper_slippage: float = 0
    paper_margin: float = 0
    paper_margin_is_ceiling: bool = False
    paper_max_margin: float = 0
    paper_maintenance_rate: float = .005
    paper_dynamic_exits: bool = False
    paper_dynamic_leverage: bool = False
    paper_min_leverage: int = 2
    paper_max_leverage: int = 20
    paper_major_max_leverage: int = 100
    paper_risk_fraction: float = .10
    paper_risk_buffer: float = .001
    paper_depth_participation: float = .05
    paper_max_spread_bps: float = 10
    paper_depth_range_bps: float = 10
    paper_entry_filter: str = 'shadow'
    paper_lab_required: bool = False
    paper_exploration_mode: bool = False
    paper_ai_autopilot: bool = False
    paper_ai_autopilot_interval: int = 60
    paper_summary_only: bool = False
    reflection_enabled: bool = field(default_factory=lambda: os.getenv('QUANT_REFLECTION_ENABLED', 'false').lower() == 'true')
    reflection_min_trades: int = field(default_factory=lambda: int(os.getenv('QUANT_REFLECTION_MIN_TRADES', '12')))
    reflection_window: int = field(default_factory=lambda: int(os.getenv('QUANT_REFLECTION_WINDOW', '50')))
    reflection_interval_trades: int = field(default_factory=lambda: int(os.getenv('QUANT_REFLECTION_INTERVAL_TRADES', '10')))
    smtp_host: str = 'smtp.qq.com'
    smtp_port: int = 465
    smtp_user: str = field(default_factory=lambda: os.getenv('QQ_SMTP_USER', ''))
    smtp_password: str = field(default_factory=lambda: os.getenv('QQ_SMTP_AUTH_CODE', ''))
    mail_to: str = field(default_factory=lambda: os.getenv('QQ_MAIL_TO', ''))

    @property
    def base_url(self):
        return 'https://demo-fapi.binance.com' if self.mode == 'testnet' else 'https://fapi.binance.com'

    @property
    def mail_ready(self):
        return bool(self.smtp_user and self.smtp_password and self.mail_to)

    @property
    def daily_loss_enabled(self):
        return self.mode == 'live'

    def validate(self):
        if self.mode not in {'paper', 'testnet', 'live'}:
            raise ValueError('QUANT_MODE must be paper/testnet/live')
        if self.strategy not in {'sma_cross', 'bollinger', 'adaptive', 'lab_model'}:
            raise ValueError('Unknown strategy')
        if self.strategy == 'adaptive' and self.mode != 'paper':
            raise ValueError('Adaptive strategy is paper-only')
        if self.strategy == 'lab_model' and (self.mode != 'paper' or not self.paper_lab_required or not self.paper_dynamic_leverage):
            raise ValueError('Lab model requires isolated paper mode, readiness gate and dynamic leverage')
        if self.paper_lab_required and self.mode != 'paper':
            raise ValueError('Lab execution is paper-only')
        if self.paper_exploration_mode and (self.mode != 'paper' or self.strategy != 'lab_model' or self.api_key or self.api_secret):
            raise ValueError('Exploration requires credential-free paper lab mode')
        if self.paper_ai_autopilot and (self.mode != 'paper' or self.api_key or self.api_secret or self.live_enabled
                or self.observation_only or self.strategy != 'lab_model' or not self.paper_lab_required):
            raise ValueError('Paper AI autopilot requires credential-free lab mode')
        if not 60 <= self.paper_ai_autopilot_interval <= 3600:
            raise ValueError('Paper AI autopilot interval must be 60 to 3600 seconds')
        if self.paper_entry_filter not in {'shadow','enforce'}:
            raise ValueError('Paper entry filter must be shadow/enforce')
        if self.reflection_min_trades < 5 or self.reflection_window < self.reflection_min_trades or self.reflection_interval_trades < 1:
            raise ValueError('Invalid strategy reflection limits')
        if not math.isfinite(self.paper_slippage) or not 0 <= self.paper_slippage <= .01:
            raise ValueError('Invalid paper slippage')
        if any(not math.isfinite(v) or v < 0 for v in (self.paper_margin,self.paper_max_margin)):
            raise ValueError('Invalid paper margin')
        if bool(self.paper_margin) != bool(self.paper_max_margin) or self.paper_margin > self.paper_max_margin:
            raise ValueError('Invalid paper margin limits')
        if self.paper_margin and not self.paper_dynamic_leverage and (not math.isclose(self.order_notional,self.paper_margin*self.leverage)
                                  or not math.isclose(self.max_notional,self.paper_max_margin*self.leverage)):
            raise ValueError('Paper margin and notional disagree')
        if not self.symbols or len(set(self.symbols)) != len(self.symbols) or any(not s.isalnum() or not s.endswith('USDT') for s in self.symbols):
            raise ValueError('Unique USDT symbols required')
        if self.interval not in {'1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d'}:
            raise ValueError('Unsupported kline interval')
        if not 1 <= self.fast < self.slow <= 200 or not 2 <= self.boll_period <= 200:
            raise ValueError('Invalid indicator periods')
        values = [self.order_notional,self.max_notional,self.stop_pct,self.take_pct,self.paper_cash,self.fee_rate,self.boll_deviation]
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('Risk settings must be finite and positive')
        if not math.isfinite(self.daily_loss) or self.daily_loss < 0 or (self.daily_loss_enabled and self.daily_loss == 0):
            raise ValueError('Live daily loss limit must be finite and positive')
        if not 0 < self.stop_pct < 0.5 or not 0 < self.take_pct < 1 or not 1 <= self.leverage <= (100 if self.mode == 'paper' else 20):
            raise ValueError('Invalid stop/take/leverage')
        if not math.isfinite(self.paper_maintenance_rate) or not 0 < self.paper_maintenance_rate < 1/self.leverage:
            raise ValueError('Invalid simulated maintenance margin')
        if (type(self.paper_min_leverage) is not int or type(self.paper_max_leverage) is not int
                or type(self.paper_major_max_leverage) is not int
                or not 1 <= self.paper_min_leverage <= self.paper_max_leverage <= 100
                or not self.paper_min_leverage <= self.paper_major_max_leverage <= 100
                or not math.isfinite(self.paper_risk_fraction)
                or not 0 < self.paper_risk_fraction <= 1):
            raise ValueError('Invalid dynamic leverage limits')
        if (not math.isfinite(self.paper_risk_buffer) or not 0 <= self.paper_risk_buffer <= .05
                or not math.isfinite(self.paper_depth_participation) or not 0 < self.paper_depth_participation <= 1
                or any(not math.isfinite(v) or not 0 < v <= 100
                       for v in (self.paper_max_spread_bps,self.paper_depth_range_bps))):
            raise ValueError('Invalid dynamic leverage liquidity limits')
        if self.paper_dynamic_leverage:
            if self.mode != 'paper' or not self.paper_margin or not self.paper_dynamic_exits:
                raise ValueError('Dynamic leverage requires paper mode, reserved margin and dynamic exits')
            if self.leverage != self.paper_max_leverage or self.paper_maintenance_rate >= 1/self.paper_max_leverage:
                raise ValueError('Dynamic leverage cap and maintenance margin disagree')
        if self.order_notional > self.max_notional or self.max_positions < 1 or self.poll_seconds < 5:
            raise ValueError('Invalid exposure or polling limits')
        if self.mode != 'paper' and not (self.api_key and self.api_secret):
            raise ValueError('Binance credentials are missing')
        if self.mode == 'live' and not self.live_enabled:
            raise ValueError('Live trading requires QUANT_LIVE_ENABLED=true')
        if not math.isfinite(self.ai_min_confidence) or not 0 <= self.ai_min_confidence <= 1 or not 10 <= self.ai_review_max_age <= 300:
            raise ValueError('Invalid AI confidence or review expiry')
        if self.community_poll_seconds < 30:
            raise ValueError('Community polling must be at least 30 seconds')
        if self.ai_advice_seconds < 300:
            raise ValueError('AI advice interval must be at least 300 seconds')
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def public(self):
        return {k: getattr(self,k) for k in ('mode','enabled','symbols','strategy','interval','fast','slow','boll_period','allow_short','leverage','order_notional','max_notional','max_positions','stop_pct','take_pct','daily_loss')} | {
            'credentials_configured': bool(self.api_key and self.api_secret),
            'mail_configured': self.mail_ready,
            'community_enabled': self.community_enabled,
            'community_agent_id': self.community_agent_id,
            'ai_enabled': self.ai_enabled,
            'ai_configured': bool(self.deepseek_api_key),
            'ai_model': self.deepseek_model,
            'observation_only': self.observation_only,
            'ai_advice_seconds': self.ai_advice_seconds,
            'ai_min_confidence': self.ai_min_confidence,
            'paper_slippage_bps': self.paper_slippage * 10000,
            'paper_margin': self.paper_margin,
            'paper_margin_is_ceiling': self.paper_margin_is_ceiling,
            'paper_max_margin': self.paper_max_margin,
            'paper_maintenance_rate': self.paper_maintenance_rate,
            'paper_dynamic_exits': self.paper_dynamic_exits,
            'paper_dynamic_leverage': self.paper_dynamic_leverage,
            'paper_min_leverage': self.paper_min_leverage,
            'paper_max_leverage': self.paper_max_leverage,
            'paper_major_max_leverage': self.paper_major_max_leverage,
            'paper_risk_fraction': self.paper_risk_fraction,
            'paper_risk_buffer_bps': self.paper_risk_buffer * 10000,
            'paper_depth_participation': self.paper_depth_participation,
            'paper_max_spread_bps': self.paper_max_spread_bps,
            'paper_depth_range_bps': self.paper_depth_range_bps,
            'paper_entry_filter': self.paper_entry_filter,
            'paper_lab_required': self.paper_lab_required,
            'paper_exploration_mode': self.paper_exploration_mode,
            'paper_ai_autopilot': self.paper_ai_autopilot,
            'paper_ai_autopilot_interval_seconds': self.paper_ai_autopilot_interval,
            'paper_summary_only': self.paper_summary_only,
            'reflection_enabled': self.reflection_enabled and self.mode == 'paper',
            'reflection_min_trades': self.reflection_min_trades,
            'reflection_window': self.reflection_window,
            'reflection_interval_trades': self.reflection_interval_trades,
            'poll_seconds': self.poll_seconds,
            'ai_review_max_age_seconds': self.ai_review_max_age,
            'ai_review_profile': 'paper_relaxed' if self.mode == 'paper' and self.paper_review_relaxed else 'standard',
            'execution_enabled': self.enabled and not self.observation_only and (self.mode != 'live' or self.live_enabled),
            'daily_loss_enabled': self.daily_loss_enabled,
            'daily_loss': self.daily_loss if self.daily_loss_enabled else None,
            'quiet_hours': 'UTC+8 00:00–08:00',
            'report_hours': '08,10,12,14,16,18,20,22',
        }
