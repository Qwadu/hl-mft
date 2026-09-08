from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Mode = Literal["record", "paper", "live"]


class Secrets(BaseSettings):
    """Loaded from environment / .env only. Never serialized."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    hl_account_address: str = ""
    hl_agent_private_key: str = ""
    hl_testnet: bool = False
    lse_api_key: str = ""
    dashboard_token: str = "change-me"


class UniverseConfig(BaseModel):
    mode: Literal["static", "dynamic"] = "dynamic"
    static_coins: list[str] = Field(default_factory=lambda: ["ZEC", "SOL", "ETH"])
    top_n: int = 10
    min_day_volume_usd: float = 20_000_000
    max_tick_bps: float = 5.0
    blacklist_prefixes: list[str] = Field(default_factory=lambda: ["xyz:", "@"])
    blacklist: list[str] = Field(default_factory=list)
    always_include: list[str] = Field(default_factory=lambda: ["ZEC"])
    refresh_hours: float = 24.0


class FeedConfig(BaseModel):
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hl_info_url: str = "https://api.hyperliquid.xyz/info"
    l2_levels: int = 20
    binance_enabled: bool = True
    binance_ws_url: str = "wss://fstream.binance.com/stream"
    lse_enabled: bool = True
    lse_ws_url: str = "wss://data-ws.londonstrategicedge.com"
    stale_after_s: float = 10.0


class FeatureConfig(BaseModel):
    obi_levels: list[int] = Field(default_factory=lambda: [1, 3, 5, 10, 20])
    cvd_windows_s: list[float] = Field(default_factory=lambda: [1.0, 5.0, 30.0])
    zscore_window_s: float = 300.0
    book_shape_dt_s: float = 2.0
    vol_window_s: float = 60.0
    min_emit_dt_s: float = 0.1  # throttle for bbo/trade-triggered feature vectors (l2 always emits)


class StrategyConfig(BaseModel):
    weights: dict[str, float] = Field(
        default_factory=lambda: {
            "obi_5": 0.8,
            "obi_20": 0.4,
            "microprice_bps": 0.6,
            "cvd_5": 1.0,
            "cvd_30": 0.5,
            "leadlag_bps": 1.2,
            "book_shape": 0.3,
        }
    )
    # bounds matter: these fields are editable at runtime from the dashboard (/api/params)
    z_clip: float = Field(default=4.0, gt=0, le=20)
    theta_enter: float = Field(default=1.5, gt=0, le=50)
    theta_taker: float = Field(default=3.0, gt=0, le=100)
    theta_exit: float = Field(default=0.3, ge=0, le=50)
    take_profit_bps: float = Field(default=8.0, gt=0, le=1000)
    stop_loss_vol_mult: float = Field(default=2.0, ge=0, le=20)
    min_stop_bps: float = Field(default=6.0, gt=0, le=500)
    max_hold_s: float = Field(default=180.0, gt=0, le=86_400)
    passive_ttl_s: float = Field(default=3.0, gt=0, le=300)
    cooldown_s: float = Field(default=10.0, ge=0, le=3600)
    max_spread_bps: float = Field(default=6.0, gt=0, le=200)
    require_ref: bool = False
    exit_style: Literal["taker", "maker_then_taker"] = "maker_then_taker"
    enabled_coins: list[str] = Field(default_factory=list)  # empty = all universe

    @model_validator(mode="after")
    def _ordered_thresholds(self) -> StrategyConfig:
        if not self.theta_exit < self.theta_enter <= self.theta_taker:
            raise ValueError("need theta_exit < theta_enter <= theta_taker")
        return self


class RiskConfig(BaseModel):
    leverage: int = Field(default=5, ge=1, le=50)
    isolated: bool = True
    risk_per_trade_pct: float = Field(default=2.0, gt=0, le=10)
    max_positions: int = Field(default=10, ge=1, le=50)
    max_gross_leverage: float = Field(default=8.0, gt=0, le=25)
    daily_loss_stop_pct: float = Field(
        default=10.0, gt=0, le=50
    )  # loss limits can be tightened, never disabled
    max_drawdown_stop_pct: float = Field(default=25.0, gt=0, le=80)
    max_notional_per_position_usd: float = Field(default=50.0, ge=10, le=1_000_000)
    min_notional_usd: float = Field(default=10.5, ge=10, le=1_000_000)  # HL minimum order is $10
    max_actions_per_minute: int = Field(default=60, ge=1, le=1000)
    # part of the budget only kill/flatten/stale cancels may use
    emergency_actions_reserve: int = Field(default=15, ge=0)
    reconcile_interval_s: float = Field(default=5.0, ge=1, le=300)

    @model_validator(mode="after")
    def _reserve_fits(self) -> RiskConfig:
        if self.emergency_actions_reserve >= self.max_actions_per_minute:
            raise ValueError("emergency_actions_reserve must be < max_actions_per_minute")
        if self.min_notional_usd > self.max_notional_per_position_usd:
            raise ValueError("min_notional_usd must be <= max_notional_per_position_usd")
        return self


class FeesConfig(BaseModel):
    maker_bps: float = 1.5
    taker_bps: float = 4.5
    # HL order-priority fee for IOC (taker) orders, charged in HYPE from undelegated staking balance as a
    # fraction of filled notional; 0-8 bps buys ~45 ms of end-to-end latency per bp (per HL docs). 0 = off.
    taker_priority_fee_bps: float = 0.0


class PaperConfig(BaseModel):
    equity_start: float = 500.0
    latency_ms: float = 60.0
    taker_slippage_bps: float = 0.5


class RecorderConfig(BaseModel):
    enabled: bool = True
    root: Path = Path("data")
    flush_rows: int = 5_000
    flush_interval_s: float = 30.0
    max_pending_batches: int = 20  # rows kept in memory after failed flushes before the oldest are dropped
    record_l2: bool = True
    record_trades: bool = True
    record_ref: bool = True
    record_features: bool = True


class MetricsConfig(BaseModel):
    port: int = 9108


class DashboardConfig(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080


class AppConfig(BaseModel):
    mode: Mode = "record"
    state_db: Path = Path("data/state.db")
    universe: UniverseConfig = UniverseConfig()
    feeds: FeedConfig = FeedConfig()
    features: FeatureConfig = FeatureConfig()
    strategy: StrategyConfig = StrategyConfig()
    risk: RiskConfig = RiskConfig()
    fees: FeesConfig = FeesConfig()
    paper: PaperConfig = PaperConfig()
    recorder: RecorderConfig = RecorderConfig()
    metrics: MetricsConfig = MetricsConfig()
    dashboard: DashboardConfig = DashboardConfig()

    @classmethod
    def load(cls, path: Path | None) -> AppConfig:
        if path is None or not path.exists():
            return cls()
        with path.open() as f:
            raw = yaml.safe_load(f) or {}
        return cls.model_validate(raw)

    def dump(self, path: Path) -> None:
        path.write_text(yaml.safe_dump(self.model_dump(mode="json"), sort_keys=False))
