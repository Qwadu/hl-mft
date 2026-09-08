from typing import Any

import pytest

from hl_mft.bus import EventBus
from hl_mft.config import AppConfig, RiskConfig, Secrets
from hl_mft.events import OrderEvent, OrderKind, OrderRequest
from hl_mft.execution.live import LiveBroker
from hl_mft.feeds.hl_info import HLInfo, PerpMeta
from hl_mft.risk import ActionBudget


def meta(coin: str) -> PerpMeta:
    return PerpMeta(coin, 0, 3, 50, False, 1e6, 100.0, 1e5, 0.0)


class FakeExchange:
    """Stands in for hyperliquid.exchange.Exchange; `leverage_ok` toggles update_leverage success."""

    def __init__(self) -> None:
        self.leverage_ok = True
        self.leverage_calls = 0
        self.orders = 0
        self.cancels = 0

    def update_leverage(self, lev: int, coin: str, is_cross: bool) -> dict[str, Any]:
        self.leverage_calls += 1
        if not self.leverage_ok:
            raise RuntimeError("boom")
        return {"status": "ok"}

    def bulk_orders(self, reqs: list[Any], builder: Any, grouping: Any) -> dict[str, Any]:
        self.orders += 1
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": self.orders}}]}}}

    def cancel_by_cloid(self, coin: str, cloid: Any) -> dict[str, Any]:
        self.cancels += 1
        return {"status": "ok", "response": {"data": {"statuses": ["success"]}}}


def mk(per_minute: int = 5, reserve: int = 2) -> tuple[LiveBroker, FakeExchange, list[OrderEvent]]:
    bus = EventBus()
    events: list[OrderEvent] = []
    bus.subscribe(OrderEvent, events.append)
    cfg = AppConfig(mode="live", risk=RiskConfig(max_actions_per_minute=per_minute))
    ex = FakeExchange()
    b = LiveBroker(
        Secrets(hl_account_address="0xabc"), cfg, bus, HLInfo(cfg.feeds.hl_info_url), {"A": meta("A")}, ex
    )
    b.budget = ActionBudget(per_minute, reserve)
    return b, ex, events


def req(cid: str, kind: OrderKind = "maker", reduce_only: bool = False) -> OrderRequest:
    return OrderRequest("A", 1, 0.5, 100.0, kind, cid, reduce_only=reduce_only, ttl_s=5.0, tag="t")


@pytest.mark.asyncio
async def test_failed_leverage_rejects_and_retries() -> None:
    b, ex, events = mk()
    ex.leverage_ok = False
    assert not await b.place(req("e1"))
    assert ex.orders == 0
    assert events[-1].status == "rejected" and events[-1].reason == "leverage_not_set"
    assert "A" not in b._lev_set
    ex.leverage_ok = True
    assert await b.place(req("e2"))
    assert ex.leverage_calls == 2 and ex.orders == 1 and "A" in b._lev_set
    # configured now: the next order does not call update_leverage again
    assert await b.place(req("e3"))
    assert ex.leverage_calls == 2


@pytest.mark.asyncio
async def test_reduce_only_skips_leverage_setup() -> None:
    b, ex, _ = mk()
    ex.leverage_ok = False
    assert await b.place(req("x1", kind="taker", reduce_only=True))
    assert ex.leverage_calls == 0


@pytest.mark.asyncio
async def test_cancel_uses_shared_budget_and_emergency_reserve() -> None:
    b, ex, events = mk(per_minute=4, reserve=2)
    b._lev_set.add("A")
    assert await b.place(req("e1")) and await b.place(req("e2"))
    assert b.budget is not None and b.budget.take(
        2
    )  # what the strategy spends for two entries; 2 left == reserve
    assert not await b.cancel("A", "e1", "ttl")  # ordinary cancel may not eat the reserve
    assert ex.cancels == 0 and "e1" in b.orders["A"]
    assert await b.cancel("A", "e1", "stale_feed")  # emergency cancel may
    assert ex.cancels == 1 and "e1" not in b.orders["A"]
    assert await b.cancel_all(reason="kill_switch") == 1
    assert b.budget is not None and b.budget.left() == 0
    assert not await b.cancel("A", "nope", "kill_switch")  # unknown order: no action spent


@pytest.mark.asyncio
async def test_entries_cannot_starve_emergency_capacity() -> None:
    b, ex, _ = mk(per_minute=3, reserve=2)
    b._lev_set.add("A")
    budget = b.budget
    assert budget is not None
    assert budget.take()  # strategy entry
    assert not budget.take()  # second entry blocked: only the reserve is left
    assert budget.take(emergency=True) and budget.take(emergency=True)
    assert not budget.take(emergency=True)


@pytest.mark.asyncio
async def test_leverage_setup_consumes_budget() -> None:
    b, ex, events = mk(per_minute=3, reserve=1)
    assert await b.place(req("e1"))  # leverage call spends one action
    assert ex.leverage_calls == 1 and b.budget is not None and b.budget.left() == 2
    b._lev_set.clear()
    b.budget.take()  # only the reserve is left now
    assert not await b.place(req("e2"))  # leverage setup is not an emergency: deferred, order rejected
    assert ex.leverage_calls == 1 and ex.orders == 1
    assert events[-1].reason == "leverage_not_set"
