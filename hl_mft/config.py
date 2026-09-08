from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field
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
    z_clip: float = 4.0
    theta_enter: float = 1.5
    theta_taker: float = 3.0
    theta_exit: float = 0.3
    take_profit_bps: float = 8.0
    stop_loss_vol_mult: float = 2.0
    min_stop_bps: float = 6.0
    max_hold_s: float = 180.0
    passive_ttl_s: float = 3.0
    cooldown_s: float = 10.0
    max_spread_bps: float = 6.0
    require_ref: bool = False
    exit_style: Literal["taker", "maker_then_taker"] = "maker_then_taker"
    enabled_coins: list[str] = Field(default_factory=list)  # empty = all universe


class RiskConfig(BaseModel):
    leverage: int = 5
    isolated: bool = True
    risk_per_trade_pct: float = 2.0
    max_positions: int = 10
    max_gross_leverage: float = 8.0
    daily_loss_stop_pct: float = 10.0
    max_drawdown_stop_pct: float = 25.0
    max_notional_per_position_usd: float = 50.0
    min_notional_usd: float = 10.5
    max_actions_per_minute: int = 60
    emergency_actions_reserve: int = 15  # part of the budget only kill/flatten/stale cancels may use
    reconcile_interval_s: float = 5.0


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
