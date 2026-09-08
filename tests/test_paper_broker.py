import pytest

from hl_mft import clock
from hl_mft.bus import EventBus
from hl_mft.config import FeesConfig, PaperConfig
from hl_mft.events import Fill, L2Update, Level, OrderEvent, OrderRequest, Trade
from hl_mft.execution.paper import PaperBroker

NS = 10**9


def l2(coin: str, ns: int, bid: float, ask: float, sz: float = 5.0) -> L2Update:
    return L2Update(
        coin,
        ns // 10**6,
        ns,
        [Level(bid, sz, 3), Level(bid - 1, sz, 3)],
        [Level(ask, sz, 3), Level(ask + 1, sz, 3)],
    )


@pytest.fixture
def env():  # type: ignore[no-untyped-def]
    bus = EventBus()
    broker = PaperBroker(
        PaperConfig(latency_ms=50.0, taker_slippage_bps=0.0), FeesConfig(maker_bps=1.0, taker_bps=4.0), bus
    )
    fills: list[Fill] = []
    evs: list[OrderEvent] = []
    bus.subscribe(Fill, fills.append)
    bus.subscribe(OrderEvent, evs.append)
    yield bus, broker, fills, evs
    clock.set_sim_time(None)


async def test_maker_rejected_when_crossing(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0))
    ok = await broker.place(OrderRequest("X", 1, 1.0, 101.0, "maker", "c1"))
    assert not ok and evs[-1].status == "rejected" and evs[-1].reason == "post_only_cross"


async def test_maker_queue_then_fill(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0, sz=5.0))
    assert await broker.place(OrderRequest("X", 1, 2.0, 100.0, "maker", "c1", ttl_s=10))
    # before latency elapses nothing happens
    await bus.publish(Trade("X", 0, NS + 10**6, 100.0, 10.0, False, 1, "", ""))
    assert not fills
    # queue ahead = 5.0; a 4.0 sell at our price does not reach us
    await bus.publish(Trade("X", 0, NS + NS, 100.0, 4.0, False, 2, "", ""))
    assert not fills
    # next 3.0 exhausts the queue (1.0 left) and spills 2.0 into us -> full fill
    await bus.publish(Trade("X", 0, NS + NS, 100.0, 3.0, False, 3, "", ""))
    assert len(fills) == 1 and fills[0].sz == 2.0 and fills[0].maker and fills[0].px == 100.0
    assert abs(fills[0].fee - 100.0 * 2.0 * 1e-4) < 1e-12
    assert evs[-1].status == "filled"
    assert broker.open_orders("X") == []


async def test_maker_ttl_expiry(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0))
    assert await broker.place(OrderRequest("X", -1, 1.0, 101.0, "maker", "c1", ttl_s=1.0))
    await bus.publish(l2("X", NS + 2 * NS, 100.0, 101.0))
    assert evs[-1].status == "expired" and not fills


async def test_maker_ttl_expiry_on_trade_only_interval(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0))
    assert await broker.place(OrderRequest("X", -1, 1.0, 101.0, "maker", "c1", ttl_s=1.0))
    # no book update since placement; a trade through our price arrives after the TTL
    await bus.publish(Trade("X", 0, NS + 2 * NS, 102.0, 5.0, True, 1, "", ""))
    assert evs[-1].status == "expired" and not fills and broker.open_orders("X") == []


async def test_taker_walks_book(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0, sz=1.0))
    # marketable limit at 102 for 1.5: 1.0@101 + 0.5@102
    assert await broker.place(OrderRequest("X", 1, 1.5, 102.0, "taker", "t1"))
    await bus.publish(l2("X", NS + NS, 100.0, 101.0, sz=1.0))
    assert len(fills) == 1
    f = fills[0]
    assert not f.maker and f.sz == 1.5
    assert abs(f.px - (101.0 + 0.5 * 102.0) / 1.5) < 1e-9
    assert abs(f.fee - f.px * 1.5 * 4e-4) < 1e-9


async def test_cancel_all(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0))
    await bus.publish(l2("Y", NS, 10.0, 10.1))
    await broker.place(OrderRequest("X", 1, 1.0, 100.0, "maker", "a"))
    await broker.place(OrderRequest("Y", 1, 1.0, 10.0, "maker", "b"))
    assert await broker.cancel_all("X") == 1
    assert broker.open_orders("X") == [] and len(broker.open_orders("Y")) == 1
    assert await broker.cancel_all() == 1


async def test_cancel_pending_taker_prevents_fill(env) -> None:  # type: ignore[no-untyped-def]
    bus, broker, fills, evs = env
    clock.set_sim_time(NS)
    await bus.publish(l2("X", NS, 100.0, 101.0))
    assert await broker.place(OrderRequest("X", 1, 1.0, 105.0, "taker", "t1"))
    assert await broker.cancel("X", "t1")
    await bus.publish(l2("X", NS + NS, 100.0, 101.0))
    assert fills == [] and evs[-1].status == "cancelled"
