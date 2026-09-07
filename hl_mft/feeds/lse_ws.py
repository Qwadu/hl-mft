from __future__ import annotations

import time
from datetime import datetime

import orjson
from websockets.asyncio.client import ClientConnection

from ..bus import EventBus
from ..events import RefTick
from ..logging_setup import get_logger
from .base import ReconnectingWsFeed
from .symbols import hl_to_lse

log = get_logger(__name__)

MAX_SYMBOLS = 16  # registered tier limit per connection


class LSEFeed(ReconnectingWsFeed):
    """London Strategic Edge composite tick feed (spot). Fallback reference for coins absent on Binance."""

    name = "lse"

    def __init__(
        self, url: str, api_key: str, bus: EventBus, coins: list[str], stale_after_s: float = 30.0
    ) -> None:
        super().__init__(url, stale_after_s)
        self.api_key = api_key
        self.bus = bus
        self.sym_to_coin: dict[str, str] = {}
        self.set_coins_sync(coins)
        self.msg_count = 0

    def set_coins_sync(self, coins: list[str]) -> None:
        self.sym_to_coin = {}
        for c in coins[:MAX_SYMBOLS]:
            s = hl_to_lse(c)
            if s:
                self.sym_to_coin[s] = c

    async def restart_with(self, coins: list[str]) -> None:
        self.set_coins_sync(coins)
        if self._ws is not None:
            await self._ws.close()

    async def on_connect(self, ws: ClientConnection) -> None:
        await ws.recv()  # welcome
        await ws.send(orjson.dumps({"action": "auth", "api_key": self.api_key}).decode())
        ack = orjson.loads(await ws.recv())
        if ack.get("type") != "authenticated":
            raise RuntimeError(f"LSE auth failed: {str(ack)[:200]}")
        for s in self.sym_to_coin:
            await ws.send(orjson.dumps({"action": "subscribe", "symbol": s}).decode())
        log.info("lse_subscribed", symbols=len(self.sym_to_coin))

    async def on_message(self, raw: str | bytes) -> None:
        recv_ns = time.time_ns()
        msg = orjson.loads(raw)
        if msg.get("type") != "tick":
            if msg.get("type") == "error":
                log.warning("lse_error", msg=msg)
            return
        coin = self.sym_to_coin.get(msg.get("symbol", ""))
        if coin is None:
            return
        bid, ask = msg.get("bid"), msg.get("ask")
        if bid is None or ask is None:
            px = msg.get("price")
            if px is None:
                return
            bid = ask = px
        ts_ms = 0
        ts = msg.get("ts")
        if isinstance(ts, str):
            try:
                ts_ms = int(datetime.fromisoformat(ts).timestamp() * 1000)
            except ValueError:
                ts_ms = 0
        self.msg_count += 1
        await self.bus.publish(
            RefTick(source="lse", coin=coin, recv_ns=recv_ns, bid=float(bid), ask=float(ask), ts_ms=ts_ms)
        )
