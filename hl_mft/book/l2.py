from __future__ import annotations

from ..events import Bbo, L2Update, Level


class OrderBook:
    """Snapshot-based L2 book (Hyperliquid pushes full snapshots, no deltas)."""

    __slots__ = ("coin", "bids", "asks", "ts_ms", "recv_ns", "tick")

    def __init__(self, coin: str, tick: float = 0.0) -> None:
        self.coin = coin
        self.bids: list[Level] = []
        self.asks: list[Level] = []
        self.ts_ms = 0
        self.recv_ns = 0
        self.tick = tick

    def apply(self, upd: L2Update) -> None:
        self.bids = upd.bids
        self.asks = upd.asks
        self.ts_ms = upd.ts_ms
        self.recv_ns = upd.recv_ns

    def apply_bbo(self, upd: Bbo) -> None:
        """Merge a best-bid/offer tick into the last snapshot (bbo arrives far more often than l2Book)."""
        if not self.ready:
            return
        self.bids = self._merge_top(self.bids, Level(upd.bid_px, upd.bid_sz, 0), is_bid=True)
        self.asks = self._merge_top(self.asks, Level(upd.ask_px, upd.ask_sz, 0), is_bid=False)
        self.ts_ms = upd.ts_ms
        self.recv_ns = upd.recv_ns

    @staticmethod
    def _merge_top(levels: list[Level], top: Level, is_bid: bool) -> list[Level]:
        # drop levels that are now strictly better than the new best (they were consumed/cancelled)
        i = 0
        while i < len(levels) and ((levels[i].px > top.px) if is_bid else (levels[i].px < top.px)):
            i += 1
        rest = levels[i:]
        if rest and abs(rest[0].px - top.px) < 1e-12:
            rest = rest[1:]
        return [top, *rest]

    @property
    def ready(self) -> bool:
        return bool(self.bids and self.asks)

    @property
    def best_bid(self) -> Level:
        return self.bids[0]

    @property
    def best_ask(self) -> Level:
        return self.asks[0]

    @property
    def mid(self) -> float:
        return (self.bids[0].px + self.asks[0].px) / 2.0

    @property
    def spread(self) -> float:
        return self.asks[0].px - self.bids[0].px

    @property
    def spread_bps(self) -> float:
        m = self.mid
        return (self.spread / m) * 1e4 if m else 0.0

    def microprice(self) -> float:
        b, a = self.bids[0], self.asks[0]
        tot = b.sz + a.sz
        if tot <= 0:
            return self.mid
        return (b.px * a.sz + a.px * b.sz) / tot

    def imbalance(self, levels: int) -> float:
        """(sum bid sz - sum ask sz) / total over top `levels`; in [-1, 1]."""
        bs = sum(lv.sz for lv in self.bids[:levels])
        as_ = sum(lv.sz for lv in self.asks[:levels])
        tot = bs + as_
        return (bs - as_) / tot if tot > 0 else 0.0

    def depth_notional(self, bps: float) -> tuple[float, float]:
        """USD size resting within `bps` of mid on each side."""
        m = self.mid
        lo, hi = m * (1 - bps / 1e4), m * (1 + bps / 1e4)
        bid_ntl = sum(lv.px * lv.sz for lv in self.bids if lv.px >= lo)
        ask_ntl = sum(lv.px * lv.sz for lv in self.asks if lv.px <= hi)
        return bid_ntl, ask_ntl

    def total_size(self, levels: int) -> tuple[float, float]:
        return sum(lv.sz for lv in self.bids[:levels]), sum(lv.sz for lv in self.asks[:levels])
