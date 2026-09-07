import pytest

from hl_mft import clock
from hl_mft.bus import EventBus
from hl_mft.config import RiskConfig, StrategyConfig
from hl_mft.events import OrderRequest
from hl_mft.execution.base import OpenOrder
from hl_mft.feeds.hl_info import PerpMeta
from hl_mft.portfolio import Portfolio
from hl_mft.risk import RiskManager
from hl_mft.strategy.flow import FlowStrategy

NS = 10**9


def meta(coin: str) -> PerpMeta:
    return PerpMeta(coin, 0, 3, 50, False, 1e6, 100.0, 1e5, 0.0)


class StubBroker:
    """Records orders; `quotes` is the REST-style fallback (None -> unknown), `fail` rejects placement."""

    def __init__(self, quotes: dict[str, tuple[float, float]], fail: set[str] | None = None) -> None:
        self.quotes = quotes
        self.fail = fail or set()
        self.placed: list[OrderRequest] = []
        self.cancelled: list[str] = []
        self.resting: dict[str, dict[str, OpenOrder]] = {}

    async def place(self, req: OrderRequest) -> bool:
        if req.coin in self.fail:
            return False
        self.placed.append(req)
        return True

    async def cancel(self, coin: str, cid: str, reason: str = "cancel") -> bool:
        self.cancelled.append(cid)
        return self.resting.get(coin, {}).pop(cid, None) is not None

    async def cancel_all(self, coin: str | None = None, reason: str = "cancel_all") -> int:
        n = 0
        for c in list(self.resting):
            if coin is None or c == coin:
                for cid in list(self.resting[c]):
                    await self.cancel(c, cid, reason)
                    n += 1
        return n

    def open_orders(self, coin: str) -> list[OpenOrder]:
        return list(self.resting.get(coin, {}).values())

    async def quote(self, coin: str) -> tuple[float, float] | None:
        return self.quotes.get(coin)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


@pytest.fixture
def mk():  # type: ignore[no-untyped-def]
    clock.set_sim_time(NS)

    def build(broker: StubBroker, coins: list[str]) -> tuple[FlowStrategy, Portfolio]:
        pf = Portfolio(equity_start=1000.0)
        risk = RiskManager(RiskConfig(), pf)
        st = FlowStrategy(StrategyConfig(), EventBus(), broker, pf, risk, {c: meta(c) for c in coins})
        return st, pf

    yield build
    clock.set_sim_time(None)


async def test_flatten_reconciled_position_without_features(mk) -> None:  # type: ignore[no-untyped-def]
    # ZEC was imported from the exchange at startup; it never produced a FeatureVector
    broker = StubBroker({"ZEC": (49.9, 50.1)})
    strat, pf = mk(broker, ["ZEC"])
    pf.set_position("ZEC", 2.0, 50.0)

    failed = await strat.flatten_all("kill_switch")

    assert failed == []
    assert len(broker.placed) == 1
    o = broker.placed[0]
    assert o.coin == "ZEC" and o.side == -1 and o.sz == 2.0 and o.kind == "taker" and o.reduce_only
    assert o.px < 49.9  # crosses the bid
    assert strat.state("ZEC").exit_cid == o.cid


async def test_flatten_reports_positions_it_cannot_price_or_send(mk) -> None:  # type: ignore[no-untyped-def]
    broker = StubBroker({"B": (10.0, 10.1), "C": (10.0, 10.1)}, fail={"B"})
    strat, pf = mk(broker, ["A", "B"])
    pf.set_position("A", -1.0, 5.0)  # no quote anywhere
    pf.set_position("B", 1.0, 10.0)  # priced, but placement rejected
    pf.set_position("C", 1.0, 10.0)  # no meta

    failed = await strat.flatten_all("manual_kill")

    assert sorted(failed) == ["A", "B", "C"]
    assert broker.placed == []
    errs = {(e["coin"], e.get("err")) for e in strat.events if e["what"] == "exit_failed"}
    assert errs == {("A", "no_quote"), ("C", "no_meta")}
    assert strat.state("B").exit_cid == ""


async def test_flatten_does_not_respam_inflight_taker_exit(mk) -> None:  # type: ignore[no-untyped-def]
    broker = StubBroker({"X": (99.0, 101.0)})
    strat, pf = mk(broker, ["X"])
    pf.set_position("X", 1.0, 100.0)

    assert await strat.flatten_all("kill_switch") == []
    assert await strat.flatten_all("kill_switch") == []
    assert len(broker.placed) == 1

    clock.set_sim_time(NS + 6 * NS)
    assert await strat.flatten_all("kill_switch") == []
    assert len(broker.placed) == 2


async def test_flatten_cancels_resting_entries_everywhere(mk) -> None:  # type: ignore[no-untyped-def]
    broker = StubBroker({"X": (99.0, 101.0)})
    strat, pf = mk(broker, ["X", "Y"])
    pf.set_position("X", 1.0, 100.0)
    entry = OrderRequest("Y", 1, 1.0, 10.0, "maker", "e-1")
    broker.resting["Y"] = {"e-1": OpenOrder(entry, NS)}
    strat.state("Y").entry_cid = "e-1"

    assert await strat.flatten_all("manual_flatten") == []
    assert "e-1" in broker.cancelled and broker.open_orders("Y") == []
    assert [o.coin for o in broker.placed] == ["X"]
