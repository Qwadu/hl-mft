from __future__ import annotations

import time

import aiohttp
import orjson
from websockets.asyncio.client import ClientConnection

from ..bus import EventBus
from ..events import RefTick
from ..logging_setup import get_logger
from .base import ReconnectingWsFeed
from .symbols import hl_to_binance

log = get_logger(__name__)

EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"


async def available_symbols() -> set[str]:
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s, s.get(EXCHANGE_INFO) as r:
        r.raise_for_status()
        info = await r.json()
    return {
        x["symbol"]
        for x in info["symbols"]
        if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
    }


class BinanceFeed(ReconnectingWsFeed):
    """Binance USDT-M futures bookTicker (best bid/ask, ~ms cadence) as lead-lag reference."""

    name = "binance"

    def __init__(self, url: str, bus: EventBus, coins: list[str], stale_after_s: float = 10.0) -> None:
        super().__init__(url, stale_after_s)
        self.bus = bus
        self.sym_to_coin: dict[str, str] = {}
        self.set_coins_sync(coins)
        self.msg_count = 0

    def set_coins_sync(self, coins: list[str]) -> None:
        self.sym_to_coin = {}
        for c in coins:
            s = hl_to_binance(c)
            if s:
                self.sym_to_coin[s] = c
        streams = "/".join(f"{s.lower()}@bookTicker" for s in self.sym_to_coin)
        base = self.url.split("?")[0]
        self.url = f"{base}?streams={streams}" if streams else base

    async def restart_with(self, coins: list[str]) -> None:
        self.set_coins_sync(coins)
        if self._ws is not None:
            await self._ws.close()  # reconnect loop picks up the new URL

    async def on_connect(self, ws: ClientConnection) -> None:
        log.info("binance_subscribed", symbols=len(self.sym_to_coin))

    async def on_message(self, raw: str | bytes) -> None:
        recv_ns = time.time_ns()
        msg = orjson.loads(raw)
        d = msg.get("data")
        if not d or d.get("e") != "bookTicker":
            return
        coin = self.sym_to_coin.get(d["s"])
        if coin is None:
            return
        self.msg_count += 1
        self.mark_data()
        await self.bus.publish(
            RefTick(
                source="binance",
                coin=coin,
                recv_ns=recv_ns,
                bid=float(d["b"]),
                ask=float(d["a"]),
                ts_ms=int(d.get("T") or d.get("E") or 0),
            )
        )
