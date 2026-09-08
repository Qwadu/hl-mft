from __future__ import annotations

import time
from collections import deque

from . import metrics
from .config import RiskConfig
from .logging_setup import get_logger
from .portfolio import Portfolio

log = get_logger(__name__)


class ActionBudget:
    """Sliding-window limiter on exchange actions (orders/cancels) per minute.

    `reserve` actions per minute are kept back for emergency use (kill / flatten / stale-feed
    cancels), which take with `emergency=True` and may use the whole window.
    """

    def __init__(self, per_minute: int, reserve: int = 0) -> None:
        self.per_minute = per_minute
        self.reserve = reserve
        self._q: deque[float] = deque()

    def _prune(self) -> None:
        cutoff = time.monotonic() - 60.0
        while self._q and self._q[0] < cutoff:
            self._q.popleft()

    def left(self) -> int:
        self._prune()
        n = self.per_minute - len(self._q)
        metrics.actions_budget_left.set(n)
        return n

    def take(self, n: int = 1, emergency: bool = False) -> bool:
        if self.left() - (0 if emergency else self.reserve) < n:
            return False
        now = time.monotonic()
        for _ in range(n):
            self._q.append(now)
        return True


class RiskManager:
    def __init__(self, cfg: RiskConfig, pf: Portfolio) -> None:
        self.cfg = cfg
        self.pf = pf
        self.killed = False
        self.kill_reason = ""
        self.paused = False  # manual pause from dashboard: no new entries, exits allowed
        self.budget = ActionBudget(cfg.max_actions_per_minute, cfg.emergency_actions_reserve)
        self.pending: dict[str, float] = {}  # coin -> worst-case notional of an unfilled entry

    def reserve(self, coin: str, notional: float) -> None:
        self.pending[coin] = notional

    def release(self, coin: str) -> None:
        self.pending.pop(coin, None)

    def trip(self, reason: str) -> None:
        if not self.killed:
            self.killed = True
            self.kill_reason = reason
            metrics.kill_switch.set(1)
            log.error("kill_switch_tripped", reason=reason)

    def reset(self) -> None:
        self.killed = False
        self.kill_reason = ""
        metrics.kill_switch.set(0)

    def check_limits(self) -> None:
        pf = self.pf
        pf.roll_day()
        if pf.day_start_equity > 0:
            dl = pf.daily_pnl / pf.day_start_equity * 100
            if dl <= -self.cfg.daily_loss_stop_pct:
                self.trip(f"daily loss {dl:.2f}% <= -{self.cfg.daily_loss_stop_pct}%")
        if pf.drawdown_pct >= self.cfg.max_drawdown_stop_pct:
            self.trip(f"drawdown {pf.drawdown_pct:.2f}% >= {self.cfg.max_drawdown_stop_pct}%")

    def can_open(self, coin: str) -> tuple[bool, str]:
        if self.killed:
            return False, "killed"
        if self.paused:
            return False, "paused"
        pf = self.pf
        held = {p.coin for p in pf.open_positions()}
        n_slots = len(held | {c for c in self.pending if c != coin})
        if n_slots >= self.cfg.max_positions:
            return False, "max_positions"
        pending_ntl = sum(v for c, v in self.pending.items() if c != coin)
        if (
            pf.gross_notional + pending_ntl + self.cfg.max_notional_per_position_usd
            > self.cfg.max_gross_leverage * pf.equity
        ):
            return False, "gross_leverage"
        return True, ""

    def size_notional(self, px: float, stop_bps: float) -> float:
        """Position notional such that hitting the stop loses risk_per_trade_pct of equity, capped."""
        eq = self.pf.equity
        risk_usd = eq * self.cfg.risk_per_trade_pct / 100
        ntl = risk_usd / max(stop_bps / 1e4, 1e-4)
        ntl = min(ntl, self.cfg.max_notional_per_position_usd, eq * self.cfg.leverage)
        if ntl < self.cfg.min_notional_usd:
            return 0.0
        return ntl
