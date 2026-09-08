from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from pathlib import Path
from typing import TYPE_CHECKING

from . import metrics
from .bus import EventBus
from .config import AppConfig, Secrets
from .events import L2Update, RefTick, Trade
from .execution.base import Broker
from .execution.paper import PaperBroker
from .features.engine import FeatureEngine
from .feeds.base import ReconnectingWsFeed
from .feeds.binance_ws import BinanceFeed, available_symbols
from .feeds.hl_info import HLInfo, PerpMeta
from .feeds.hyperliquid_ws import HyperliquidFeed
from .feeds.lse_ws import LSEFeed
from .feeds.symbols import hl_to_binance
from .logging_setup import get_logger
from .portfolio import Portfolio
from .recorder.parquet import ParquetRecorder
from .risk import RiskManager
from .state import StateStore
from .strategy.flow import FlowStrategy
from .universe import fetch_universe

if TYPE_CHECKING:
    from .execution.live import LiveBroker

log = get_logger(__name__)


TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"
TESTNET_INFO_URL = "https://api.hyperliquid-testnet.xyz/info"


class App:
    def _select_network(self) -> None:
        """Keep market data, info and signing on the same network as HL_TESTNET says."""
        feeds = self.cfg.feeds
        if self.secrets.hl_testnet:
            if feeds.hl_ws_url != TESTNET_WS_URL or feeds.hl_info_url != TESTNET_INFO_URL:
                log.warning("hl_testnet_override_urls", ws=TESTNET_WS_URL, info=TESTNET_INFO_URL)
            feeds.hl_ws_url = TESTNET_WS_URL
            feeds.hl_info_url = TESTNET_INFO_URL
        elif self.cfg.mode == "live" and ("testnet" in feeds.hl_ws_url or "testnet" in feeds.hl_info_url):
            raise SystemExit(
                "feeds point at testnet but HL_TESTNET is not set; refusing to sign mainnet orders"
            )

    def __init__(self, cfg: AppConfig, secrets: Secrets, config_path: Path | None = None) -> None:
        self.cfg = cfg
        self.secrets = secrets
        self.config_path = config_path
        self._select_network()
        self.bus = EventBus()
        self.info = HLInfo(cfg.feeds.hl_info_url)
        self.coins: list[str] = []
        self.metas: dict[str, PerpMeta] = {}
        self.feeds: list[ReconnectingWsFeed] = []
        self.hl: HyperliquidFeed | None = None
        self.binance: BinanceFeed | None = None
        self.lse: LSEFeed | None = None
        self.engine: FeatureEngine | None = None
        self.recorder: ParquetRecorder | None = None
        self.broker: Broker | None = None
        self.live: LiveBroker | None = None
        self.pf: Portfolio | None = None
        self.risk: RiskManager | None = None
        self.strategy: FlowStrategy | None = None
        self.started_at = time.time()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._counts = {"l2": 0, "trades": 0, "ref": 0}

    # -- setup ---------------------------------------------------------------
    async def setup(self) -> None:
        cfg = self.cfg
        metrics.serve(cfg.metrics.port)
        self.coins, self.metas = await fetch_universe(self.info, cfg.universe)
        # trading state (incl. live reconciliation) comes first so coins with existing exposure are
        # part of the universe before any feed subscribes and before the strategy sees an event
        if cfg.mode in ("paper", "live"):
            await self._setup_trading()
        metrics.universe_size.set(len(self.coins))

        self.bus.subscribe(L2Update, self._count_l2)
        self.bus.subscribe(Trade, self._count_trade)
        self.bus.subscribe(RefTick, self._count_ref)

        self.engine = FeatureEngine(cfg.features, self.bus, self.coins)
        if cfg.recorder.enabled:
            self.recorder = ParquetRecorder(cfg.recorder, self.bus)

        user = self.secrets.hl_account_address if cfg.mode == "live" else None
        self.hl = HyperliquidFeed(
            cfg.feeds.hl_ws_url, self.bus, self.coins, user=user, stale_after_s=cfg.feeds.stale_after_s
        )
        self.feeds.append(self.hl)
        if self.live:
            self.hl.user_handlers.append(self.live.on_user_message)
        if cfg.feeds.binance_enabled:
            try:
                avail = await available_symbols()
                bcoins = [c for c in self.coins if (hl_to_binance(c) or "") in avail]
            except Exception as e:  # noqa: BLE001
                log.warning("binance_exchange_info_failed", err=repr(e))
                bcoins = self.coins
            self.binance = BinanceFeed(cfg.feeds.binance_ws_url, self.bus, bcoins, cfg.feeds.stale_after_s)
            self.feeds.append(self.binance)
        if cfg.feeds.lse_enabled and self.secrets.lse_api_key:
            self.lse = LSEFeed(cfg.feeds.lse_ws_url, self.secrets.lse_api_key, self.bus, self.coins)
            self.feeds.append(self.lse)

    async def _setup_trading(self) -> None:
        cfg = self.cfg
        store: StateStore | None = None
        if cfg.mode == "paper":
            self.pf = Portfolio(equity_start=cfg.paper.equity_start)
            self.broker = PaperBroker(cfg.paper, cfg.fees, self.bus)
        else:
            from .execution.live import LiveBroker

            if not self.secrets.hl_agent_private_key or not self.secrets.hl_account_address:
                raise SystemExit("live mode requires HL_ACCOUNT_ADDRESS and HL_AGENT_PRIVATE_KEY")
            self.live = LiveBroker(self.secrets, cfg, self.bus, self.info, self.metas)
            eq = await self.live.account_value()
            self.pf = Portfolio(equity_start=eq)
            self.broker = self.live
            net = "testnet" if self.secrets.hl_testnet else "mainnet"
            # daily anchor / high-water mark / kill state survive restarts; keyed per account+network
            store = StateStore(cfg.state_db, f"live:{net}:{self.secrets.hl_account_address.lower()}")
        assert self.pf is not None and self.broker is not None
        self.pf.roll_day()
        self.risk = RiskManager(cfg.risk, self.pf, store)
        self.strategy = FlowStrategy(cfg.strategy, self.bus, self.broker, self.pf, self.risk, self.metas)
        if self.live:
            self.live.attach(self.pf, self.strategy)
            await self.live.reconcile()
            held = [p.coin for p in self.pf.open_positions()]
            for c in held:
                if c not in self.coins:
                    self.coins.append(c)
            if held:
                log.warning("startup_positions_imported", coins=held, equity=round(self.pf.equity, 2))
            self.risk.check_limits()

    def _count_l2(self, e: L2Update) -> None:
        self._counts["l2"] += 1
        metrics.l2_updates.labels(coin=e.coin).inc()

    def _count_trade(self, e: Trade) -> None:
        self._counts["trades"] += 1
        metrics.trades_seen.labels(coin=e.coin, side="buy" if e.is_buy else "sell").inc()

    def _count_ref(self, e: RefTick) -> None:
        self._counts["ref"] += 1
        metrics.ref_ticks.labels(source=e.source, coin=e.coin).inc()

    # -- run -----------------------------------------------------------------
    async def run(self) -> None:
        await self.setup()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stop.set)

        if self.recorder:
            self.recorder.start()
        if self.broker:
            await self.broker.start()
        for f in self.feeds:
            f.start()
        self._tasks.append(asyncio.create_task(self._housekeeping(), name="housekeeping"))
        self._tasks.append(asyncio.create_task(self._universe_refresh(), name="universe"))
        if self.cfg.dashboard.enabled:
            from .dashboard.server import serve_dashboard

            self._tasks.append(asyncio.create_task(serve_dashboard(self), name="dashboard"))
        log.info("app_started", mode=self.cfg.mode, coins=self.coins)
        await self._stop.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        log.info("app_stopping")
        if self.strategy and self.cfg.mode == "live" and self.broker:
            await self.broker.cancel_all(reason="shutdown")
        for t in self._tasks:
            t.cancel()
        for f in self.feeds:
            await f.stop()
        if self.broker:
            await self.broker.stop()
        if self.recorder:
            await self.recorder.stop()
        if self.risk and self.risk.store:
            self.risk.store.close()
        await self.info.close()
        log.info("app_stopped")

    async def _housekeeping(self) -> None:
        last = time.monotonic()
        while True:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            metrics.event_loop_lag.observe(max(0.0, now - last - 1.0))
            last = now
            for f in self.feeds:
                metrics.feed_connected.labels(feed=f.name).set(1 if f.connected else 0)
                metrics.feed_stale.labels(feed=f.name).set(1 if f.stale else 0)
            if self.recorder:
                for t, n in self.recorder.stats().items():
                    metrics.recorder_rows.labels(table=t).set(n)
            if self.risk and self.pf:
                self.risk.check_limits()
                self.pf.publish_metrics()
                if self.hl and self.cfg.mode == "live" and self.broker:
                    # dead-man: no market data -> pull resting orders (whole feed, or per silent coin)
                    if self.hl.stale:
                        n = await self.broker.cancel_all(reason="stale_feed")
                        if n:
                            log.warning("stale_feed_cancelled_orders", n=n)
                    else:
                        for c in self.hl.stale_coins():
                            n = await self.broker.cancel_all(c, reason="stale_feed")
                            if n:
                                log.warning("stale_coin_cancelled_orders", coin=c, n=n)
                if self.risk.killed and self.strategy:
                    await self.strategy.flatten_all("kill_switch")

    async def _universe_refresh(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.universe.refresh_hours * 3600)
            try:
                coins, metas = await fetch_universe(self.info, self.cfg.universe)
            except Exception as e:  # noqa: BLE001
                log.warning("universe_refresh_failed", err=repr(e))
                continue
            self.metas.update(metas)
            # keep coins with open positions
            if self.pf:
                for p in self.pf.open_positions():
                    if p.coin not in coins:
                        coins.append(p.coin)
            if set(coins) == set(self.coins):
                continue
            self.coins = coins
            metrics.universe_size.set(len(coins))
            if self.engine:
                self.engine.set_coins(coins)
            if self.hl:
                await self.hl.set_coins(coins)
            if self.binance:
                await self.binance.restart_with(coins)
            if self.lse:
                await self.lse.restart_with(coins)

    # -- status for dashboard ------------------------------------------------
    def status(self) -> dict[str, object]:
        return {
            "mode": self.cfg.mode,
            "uptime_s": round(time.time() - self.started_at),
            "coins": self.coins,
            "feeds": {
                f.name: {"connected": f.connected, "stale": f.stale, "reconnects": f.reconnects}
                for f in self.feeds
            },
            "counts": dict(self._counts),
            "recorder": self.recorder.stats() if self.recorder else None,
            "risk": {
                "killed": self.risk.killed,
                "kill_reason": self.risk.kill_reason,
                "paused": self.risk.paused,
                "actions_left": self.risk.budget.left(),
            }
            if self.risk
            else None,
            "strategy_enabled": self.strategy.enabled if self.strategy else None,
        }
