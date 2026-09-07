from __future__ import annotations

import asyncio
import time
from typing import Any

import orjson
from websockets.asyncio.client import ClientConnection

from ..bus import EventBus
from ..events import AssetCtx, Bbo, L2Update, Level, Trade
from ..logging_setup import get_logger
from .base import ReconnectingWsFeed

log = get_logger(__name__)


class HyperliquidFeed(ReconnectingWsFeed):
    """Market-data WS: l2Book + trades + bbo + activeAssetCtx per coin, plus optional user channels."""

    name = "hyperliquid"

    def __init__(
        self,
        url: str,
        bus: EventBus,
        coins: list[str],
        user: str | None = None,
        stale_after_s: float = 10.0,
    ) -> None:
        super().__init__(url, stale_after_s)
        self.bus = bus
        self.coins: list[str] = list(coins)
        self.user = user
        self.msg_count = 0
        self.user_handlers: list[Any] = []

    # -- subscriptions -------------------------------------------------------
    def _subs_for(self, coin: str) -> list[dict[str, Any]]:
        return [
            {"type": "l2Book", "coin": coin},
            {"type": "trades", "coin": coin},
            {"type": "bbo", "coin": coin},
            {"type": "activeAssetCtx", "coin": coin},
        ]

    async def _send(self, ws: ClientConnection, method: str, sub: dict[str, Any]) -> None:
        await ws.send(orjson.dumps({"method": method, "subscription": sub}).decode())

    async def on_connect(self, ws: ClientConnection) -> None:
        for coin in self.coins:
            for sub in self._subs_for(coin):
                await self._send(ws, "subscribe", sub)
        if self.user:
            for sub in (
                {"type": "userFills", "user": self.user},
                {"type": "orderUpdates", "user": self.user},
                {"type": "clearinghouseState", "user": self.user},
            ):
                await self._send(ws, "subscribe", sub)
        log.info("hl_subscribed", coins=len(self.coins), user=bool(self.user))

    async def set_coins(self, coins: list[str]) -> None:
        """Hot-swap universe: unsubscribe removed, subscribe added."""
        new, old = set(coins), set(self.coins)
        ws = self._ws
        if ws is not None and self.connected:
            for c in old - new:
                for sub in self._subs_for(c):
                    await self._send(ws, "unsubscribe", sub)
            for c in new - old:
                for sub in self._subs_for(c):
                    await self._send(ws, "subscribe", sub)
        self.coins = list(coins)

    async def heartbeat(self, ws: ClientConnection) -> None:
        while True:
            await asyncio.sleep(30)
            await ws.send('{"method":"ping"}')

    # -- parsing -------------------------------------------------------------
    async def on_message(self, raw: str | bytes) -> None:
        recv_ns = time.time_ns()
        msg = orjson.loads(raw)
        ch = msg.get("channel")
        data = msg.get("data")
        self.msg_count += 1
        if ch == "l2Book":
            lv = data["levels"]
            await self.bus.publish(
                L2Update(
                    coin=data["coin"],
                    ts_ms=int(data["time"]),
                    recv_ns=recv_ns,
                    bids=[Level(float(x["px"]), float(x["sz"]), int(x["n"])) for x in lv[0]],
                    asks=[Level(float(x["px"]), float(x["sz"]), int(x["n"])) for x in lv[1]],
                )
            )
        elif ch == "trades":
            for t in data:
                users = t.get("users") or ["", ""]
                await self.bus.publish(
                    Trade(
                        coin=t["coin"],
                        ts_ms=int(t["time"]),
                        recv_ns=recv_ns,
                        px=float(t["px"]),
                        sz=float(t["sz"]),
                        is_buy=t["side"] == "B",
                        tid=int(t["tid"]),
                        buyer=users[0],
                        seller=users[1] if len(users) > 1 else "",
                    )
                )
        elif ch == "bbo":
            b, a = data["bbo"]
            if b is None or a is None:
                return
            await self.bus.publish(
                Bbo(
                    coin=data["coin"],
                    ts_ms=int(data["time"]),
                    recv_ns=recv_ns,
                    bid_px=float(b["px"]),
                    bid_sz=float(b["sz"]),
                    ask_px=float(a["px"]),
                    ask_sz=float(a["sz"]),
                )
            )
        elif ch == "activeAssetCtx":
            c = data["ctx"]
            await self.bus.publish(
                AssetCtx(
                    coin=data["coin"],
                    recv_ns=recv_ns,
                    funding=float(c["funding"]),
                    open_interest=float(c["openInterest"]),
                    day_ntl_vlm=float(c["dayNtlVlm"]),
                    mark_px=float(c["markPx"]),
                    oracle_px=float(c["oraclePx"]),
                    mid_px=float(c["midPx"]) if c.get("midPx") else 0.0,
                    premium=float(c["premium"]) if c.get("premium") else 0.0,
                )
            )
        elif ch in ("userFills", "orderUpdates", "clearinghouseState", "user"):
            for h in self.user_handlers:
                await h(ch, data)
        elif ch in ("subscriptionResponse", "pong"):
            return
        elif ch == "error":
            log.error("hl_ws_error", data=data)
