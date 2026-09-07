from __future__ import annotations

import time
from dataclasses import dataclass, field

from . import clock, metrics


@dataclass(slots=True)
class Position:
    coin: str
    size: float = 0.0  # signed, base units
    entry_px: float = 0.0
    opened_ns: int = 0
    entry_score: float = 0.0
    realized: float = 0.0
    fees: float = 0.0
    n_fills: int = 0

    @property
    def side(self) -> int:
        return (self.size > 0) - (self.size < 0)

    def notional(self, px: float) -> float:
        return abs(self.size) * px

    def upnl(self, px: float) -> float:
        return self.size * (px - self.entry_px)

    def apply_fill(self, px: float, sz: float, fee: float) -> float:
        """sz signed. Returns realized pnl from this fill (excl fee)."""
        realized = 0.0
        if self.size == 0 or (self.size > 0) == (sz > 0):
            tot = self.size + sz
            self.entry_px = (self.entry_px * self.size + px * sz) / tot if tot != 0 else px
            self.size = tot
            if self.opened_ns == 0:
                self.opened_ns = clock.now_ns()
        else:
            closing = min(abs(sz), abs(self.size)) * (1 if self.size > 0 else -1)
            realized = closing * (px - self.entry_px)
            self.size += sz
            if abs(self.size) < 1e-12:
                self.size = 0.0
                self.opened_ns = 0
            elif (self.size > 0) != (closing > 0):
                self.entry_px = px  # flipped
                self.opened_ns = clock.now_ns()
        self.realized += realized
        self.fees += fee
        self.n_fills += 1
        return realized


@dataclass(slots=True)
class Portfolio:
    equity_start: float
    positions: dict[str, Position] = field(default_factory=dict)
    realized_total: float = 0.0
    fees_total: float = 0.0
    day_start_equity: float = 0.0
    day_key: str = ""
    peak_equity: float = 0.0
    marks: dict[str, float] = field(default_factory=dict)
    trades_closed: int = 0
    wins: int = 0

    def pos(self, coin: str) -> Position:
        p = self.positions.get(coin)
        if p is None:
            p = self.positions[coin] = Position(coin)
        return p

    def mark(self, coin: str, px: float) -> None:
        self.marks[coin] = px

    def open_positions(self) -> list[Position]:
        return [p for p in self.positions.values() if p.size != 0]

    @property
    def unrealized(self) -> float:
        return sum(p.upnl(self.marks.get(p.coin, p.entry_px)) for p in self.open_positions())

    @property
    def gross_notional(self) -> float:
        return sum(p.notional(self.marks.get(p.coin, p.entry_px)) for p in self.open_positions())

    @property
    def equity(self) -> float:
        return self.equity_start + self.realized_total - self.fees_total + self.unrealized

    def sync_equity(self, actual: float) -> None:
        """Re-anchor so `equity` matches the exchange account value (funding, external trades)."""
        self.equity_start += actual - self.equity

    def set_position(self, coin: str, size: float, entry_px: float) -> None:
        p = self.pos(coin)
        p.size = size
        p.entry_px = entry_px
        if size != 0 and p.opened_ns == 0:
            p.opened_ns = clock.now_ns()
        if size == 0:
            p.opened_ns = 0

    def roll_day(self, now: float | None = None) -> None:
        key = time.strftime("%Y-%m-%d", time.gmtime(now or clock.now_s()))
        if key != self.day_key:
            self.day_key = key
            self.day_start_equity = self.equity
        self.peak_equity = max(self.peak_equity, self.equity)

    @property
    def daily_pnl(self) -> float:
        return self.equity - self.day_start_equity if self.day_start_equity else 0.0

    @property
    def drawdown_pct(self) -> float:
        return (self.peak_equity - self.equity) / self.peak_equity * 100 if self.peak_equity > 0 else 0.0

    def on_fill(self, coin: str, px: float, sz: float, fee: float, maker: bool) -> float:
        p = self.pos(coin)
        before = p.size
        realized = p.apply_fill(px, sz, fee)
        self.realized_total += realized
        self.fees_total += fee
        if before != 0 and p.size == 0:
            self.trades_closed += 1
            if realized - fee > 0:
                self.wins += 1
        self.mark(coin, px)
        metrics.orders_filled.labels(coin=coin, maker=str(maker).lower()).inc()
        self.publish_metrics()
        return realized

    def publish_metrics(self) -> None:
        for p in self.positions.values():
            px = self.marks.get(p.coin, p.entry_px)
            metrics.position_size.labels(coin=p.coin).set(p.size)
            metrics.position_notional.labels(coin=p.coin).set(p.notional(px))
            metrics.unrealized_pnl.labels(coin=p.coin).set(p.upnl(px))
        metrics.realized_pnl_total.set(self.realized_total - self.fees_total)
        metrics.daily_pnl.set(self.daily_pnl)
        metrics.account_value.set(self.equity)

    def snapshot(self) -> dict[str, object]:
        return {
            "equity": round(self.equity, 4),
            "equity_start": self.equity_start,
            "realized": round(self.realized_total, 4),
            "fees": round(self.fees_total, 4),
            "unrealized": round(self.unrealized, 4),
            "daily_pnl": round(self.daily_pnl, 4),
            "drawdown_pct": round(self.drawdown_pct, 3),
            "gross_notional": round(self.gross_notional, 2),
            "trades_closed": self.trades_closed,
            "win_rate": round(self.wins / self.trades_closed, 3) if self.trades_closed else None,
            "positions": [
                {
                    "coin": p.coin,
                    "size": p.size,
                    "entry_px": p.entry_px,
                    "mark": self.marks.get(p.coin),
                    "upnl": round(p.upnl(self.marks.get(p.coin, p.entry_px)), 4),
                    "age_s": round((clock.now_ns() - p.opened_ns) / 1e9, 1) if p.opened_ns else 0,
                    "entry_score": round(p.entry_score, 2),
                }
                for p in self.open_positions()
            ],
        }
