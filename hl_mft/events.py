from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True)
class Level:
    px: float
    sz: float
    n: int


@dataclass(slots=True)
class L2Update:
    """Full-depth snapshot from Hyperliquid `l2Book` (up to 20 levels per side)."""

    coin: str
    ts_ms: int
    recv_ns: int
    bids: list[Level]
    asks: list[Level]


@dataclass(slots=True)
class Trade:
    coin: str
    ts_ms: int
    recv_ns: int
    px: float
    sz: float
    is_buy: bool  # aggressor side
    tid: int
    buyer: str = ""
    seller: str = ""


@dataclass(slots=True)
class Bbo:
    coin: str
    ts_ms: int
    recv_ns: int
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float


@dataclass(slots=True)
class AssetCtx:
    coin: str
    recv_ns: int
    funding: float
    open_interest: float
    day_ntl_vlm: float
    mark_px: float
    oracle_px: float
    mid_px: float
    premium: float


@dataclass(slots=True)
class RefTick:
    """Reference price from another venue (Binance / LSE)."""

    source: str
    coin: str
    recv_ns: int
    bid: float
    ask: float
    ts_ms: int = 0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


OrderKind = Literal["maker", "taker"]
OrderStatus = Literal["open", "filled", "cancelled", "expired", "rejected"]


@dataclass(slots=True)
class OrderRequest:
    coin: str
    side: int  # +1 buy, -1 sell
    sz: float  # base units, positive
    px: float  # limit price (for taker: marketable limit / IOC)
    kind: OrderKind
    cid: str
    reduce_only: bool = False
    ttl_s: float = 0.0  # maker orders only; 0 = no TTL
    tag: str = ""


@dataclass(slots=True)
class Fill:
    coin: str
    cid: str
    recv_ns: int
    px: float
    sz: float  # signed
    fee: float  # USD, positive = paid
    maker: bool
    tag: str = ""


@dataclass(slots=True)
class OrderEvent:
    coin: str
    cid: str
    status: OrderStatus
    recv_ns: int
    reason: str = ""


@dataclass(slots=True)
class FeatureVector:
    coin: str
    recv_ns: int
    mid: float
    spread_bps: float
    values: dict[str, float] = field(default_factory=dict)
    zscores: dict[str, float] = field(default_factory=dict)
    score: float = 0.0
