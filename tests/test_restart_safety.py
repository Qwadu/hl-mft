"""Behaviour across process restarts: risk baselines, imported positions, orphan orders, feed dead-man."""

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from hl_mft import clock
from hl_mft.bus import EventBus
from hl_mft.config import RiskConfig, StrategyConfig
from hl_mft.events import FeatureVector
from hl_mft.execution.live import CLOID_PREFIX, to_cloid
from hl_mft.feeds.hyperliquid_ws import HyperliquidFeed
from hl_mft.portfolio import Portfolio
from hl_mft.risk import RiskManager
from hl_mft.state import StateStore
from hl_mft.strategy.flow import FlowStrategy
from tests.test_flatten import StubBroker, meta
from tests.test_live_broker import mk as mk_live

NS = 10**9
DAY0 = 1_700_000_000.0  # 2023-11-14 22:13 UTC


def _fv(coin: str, mid: float, rv_bps: float = 0.0, ns: int = NS) -> FeatureVector:
    return FeatureVector(coin=coin, recv_ns=ns, mid=mid, spread_bps=2.0, values={"rv_bps": rv_bps})


# -- risk baselines --------------------------------------------------------------------------------


def test_risk_baselines_survive_restart(tmp_path: Path) -> None:
    clock.set_sim_time(int(DAY0 * NS))
    try:
        db = tmp_path / "state.db"
        pf = Portfolio(equity_start=1000.0)
        rm = RiskManager(
            RiskConfig(daily_loss_stop_pct=10, max_drawdown_stop_pct=25), pf, StateStore(db, "t")
        )
        assert pf.day_start_equity == 1000.0 and pf.peak_equity == 1000.0
        pf.realized_total = 200.0  # equity 1200 -> new high-water mark
        rm.check_limits()
        pf.realized_total = 130.0  # -7% today: not yet a daily stop
        rm.check_limits()
        assert not rm.killed
        rm.store.close()  # type: ignore[union-attr]

        # process restarts the same day with the account now at 1130
        pf2 = Portfolio(equity_start=1130.0)
        rm2 = RiskManager(
            RiskConfig(daily_loss_stop_pct=10, max_drawdown_stop_pct=25), pf2, StateStore(db, "t")
        )
        assert pf2.day_start_equity == 1000.0  # not re-anchored to 1130
        assert pf2.peak_equity == 1200.0  # high-water mark kept
        pf2.realized_total = -40.0  # equity 1090: -9% vs 1200 peak, -9% day... still ok
        rm2.check_limits()
        assert not rm2.killed
        pf2.realized_total = -235.0  # equity 895: -10.5% on the day
        rm2.check_limits()
        assert rm2.killed and "daily loss" in rm2.kill_reason
        rm2.store.close()  # type: ignore[union-attr]

        # a third restart cannot clear the kill switch by itself
        pf3 = Portfolio(equity_start=895.0)
        rm3 = RiskManager(RiskConfig(), pf3, StateStore(db, "t"))
        assert rm3.killed and pf3.peak_equity == 1200.0
        rm3.reset()
        rm3.store.close()  # type: ignore[union-attr]
        rm4 = RiskManager(RiskConfig(), Portfolio(equity_start=895.0), StateStore(db, "t"))
        assert not rm4.killed

        # next UTC day: the daily anchor rolls to current equity, the peak does not drop
        clock.set_sim_time(int((DAY0 + 86_400) * NS))
        pf5 = Portfolio(equity_start=900.0)
        rm5 = RiskManager(RiskConfig(), pf5, StateStore(db, "t"))
        assert pf5.day_start_equity == 900.0 and pf5.peak_equity == 1200.0
        assert rm5.store is not None
        rm5.store.close()
    finally:
        clock.set_sim_time(None)


def test_state_namespaces_do_not_mix(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    RiskManager(RiskConfig(), Portfolio(equity_start=500.0), StateStore(db, "live:testnet:0xa")).trip("x")
    rm = RiskManager(RiskConfig(), Portfolio(equity_start=500.0), StateStore(db, "live:mainnet:0xa"))
    assert not rm.killed


# -- imported positions get a stop ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconciled_position_gets_stop_on_first_tick() -> None:
    clock.set_sim_time(NS)
    try:
        broker = StubBroker({"ZEC": (49.9, 50.1)})
        pf = Portfolio(equity_start=1000.0)
        risk = RiskManager(RiskConfig(), pf)
        cfg = StrategyConfig(min_stop_bps=6.0, stop_loss_vol_mult=2.0)
        strat = FlowStrategy(cfg, EventBus(), broker, pf, risk, {"ZEC": meta("ZEC")})
        pf.set_position("ZEC", 2.0, 50.0)  # imported by reconcile: strategy never sized it
        assert strat.state("ZEC").stop_bps == 0.0

        await strat.on_features(_fv("ZEC", 50.0, rv_bps=5.0))
        assert strat.state("ZEC").stop_bps == 10.0  # max(2*5, 6)
        assert broker.placed == []

        await strat.on_features(_fv("ZEC", 50.0 * (1 - 0.0011), rv_bps=5.0, ns=NS + 1))  # -11 bps
        assert len(broker.placed) == 1
        o = broker.placed[0]
        assert o.reduce_only and o.kind == "taker" and o.side == -1 and o.sz == 2.0
        assert strat.state("ZEC").exit_reason == "stop"
    finally:
        clock.set_sim_time(None)


# -- orphan exchange orders -------------------------------------------------------------------------


class _Info:
    def __init__(self, orders: list[dict[str, Any]]) -> None:
        self.orders = orders

    async def clearinghouse_state(self, user: str) -> dict[str, Any]:
        return {"assetPositions": [], "marginSummary": {"accountValue": "100.0"}}

    async def open_orders(self, user: str) -> list[dict[str, Any]]:
        return self.orders


@pytest.mark.asyncio
async def test_reconcile_cancels_orders_from_previous_process() -> None:
    b, ex, _ = mk_live(per_minute=10, reserve=2)
    b.pf = Portfolio(equity_start=100.0)
    b._lev_set.add("A")
    assert await b.place(mk_req("mine"))
    mine = next(iter(b.cloid_to_cid))
    b.info = _Info(  # type: ignore[assignment]
        [
            {"coin": "A", "oid": 1, "cloid": mine},  # ours: kept
            {"coin": "A", "oid": 2, "cloid": to_cloid("old-1").to_raw()},  # previous instance: cancelled
            {"coin": "B", "oid": 3, "cloid": to_cloid("old-2").to_raw()},  # previous instance, other coin
            {"coin": "A", "oid": 4},  # manual UI order without cloid: untouched
            {"coin": "A", "oid": 5, "cloid": "0x" + "ab" * 16},  # another client's cloid: untouched
        ]
    )
    assert mine.startswith(CLOID_PREFIX) and len(mine) == 34
    await b.reconcile()
    assert ex.cancels == 2
    assert "mine" in b.orders["A"]


@pytest.mark.asyncio
async def test_orphan_cancel_uses_emergency_budget() -> None:
    b, ex, _ = mk_live(per_minute=3, reserve=2)
    b.pf = Portfolio(equity_start=100.0)
    assert b.budget is not None and b.budget.take()  # ordinary capacity gone, only the reserve is left
    b.info = _Info([{"coin": "A", "oid": 2, "cloid": to_cloid("old").to_raw()}])  # type: ignore[assignment]
    await b.reconcile()
    assert ex.cancels == 1


def mk_req(cid: str):  # type: ignore[no-untyped-def]
    from tests.test_live_broker import req

    return req(cid)


# -- feed dead-man only trusts market data ----------------------------------------------------------


@pytest.mark.asyncio
async def test_feed_staleness_ignores_non_market_frames() -> None:
    feed = HyperliquidFeed("wss://x", EventBus(), ["A"], user="0xabc", stale_after_s=10.0)
    assert feed.stale
    await feed.on_message(b'{"channel":"pong"}')
    await feed.on_message(b'{"channel":"subscriptionResponse","data":{}}')
    await feed.on_message(b'{"channel":"orderUpdates","data":[]}')
    await feed.on_message(
        b'{"channel":"activeAssetCtx","data":{"coin":"A","ctx":{"funding":"0","openInterest":"1",'
        b'"dayNtlVlm":"1","markPx":"1","oraclePx":"1","midPx":"1","premium":"0"}}}'
    )
    assert feed.stale  # nothing above carries a tradable price
    await feed.on_message(
        b'{"channel":"bbo","data":{"coin":"A","time":1,"bbo":[{"px":"1","sz":"1","n":1},{"px":"2","sz":"1","n":1}]}}'
    )
    assert not feed.stale
    feed.last_data_ns -= int(11 * NS)
    await feed.on_message(b'{"channel":"pong"}')
    assert feed.stale


@pytest.mark.asyncio
async def test_feed_reports_silent_coins() -> None:
    feed = HyperliquidFeed("wss://x", EventBus(), ["A", "B"], stale_after_s=10.0)
    feed._connect_ns = feed.last_data_ns = feed.last_msg_ns = 1  # "connected" long ago
    assert feed.stale_coins() == ["A", "B"]
    await feed.on_message(
        b'{"channel":"trades","data":[{"coin":"A","time":1,"px":"1","sz":"1","side":"B","tid":1}]}'
    )
    assert feed.stale_coins() == ["B"]
    await feed.on_message(
        b'{"channel":"l2Book","data":{"coin":"B","time":1,"levels":[[{"px":"1","sz":"1","n":1}],[{"px":"2","sz":"1","n":1}]]}}'
    )
    assert feed.stale_coins() == [] and not feed.stale
    feed._coin_data_ns["A"] -= int(11 * NS)
    assert feed.stale_coins() == ["A"]


# -- runtime parameter bounds -----------------------------------------------------------------------


def test_config_bounds_reject_unsafe_values() -> None:
    for bad in (
        {"daily_loss_stop_pct": 0},
        {"daily_loss_stop_pct": -5},
        {"max_drawdown_stop_pct": 100},
        {"leverage": 0},
        {"max_actions_per_minute": 10, "emergency_actions_reserve": 10},
        {"min_notional_usd": 100.0, "max_notional_per_position_usd": 50.0},
    ):
        with pytest.raises(ValidationError):
            RiskConfig(**bad)
    for bad_s in ({"min_stop_bps": 0}, {"theta_enter": 0.1, "theta_exit": 0.3}, {"passive_ttl_s": -1}):
        with pytest.raises(ValidationError):
            StrategyConfig(**bad_s)
    RiskConfig(daily_loss_stop_pct=5, max_actions_per_minute=30, emergency_actions_reserve=10)
