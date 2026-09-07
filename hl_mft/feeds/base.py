from __future__ import annotations

import asyncio
import random
import time
from abc import ABC, abstractmethod

import websockets
from websockets.asyncio.client import ClientConnection

from ..logging_setup import get_logger

log = get_logger(__name__)


class ReconnectingWsFeed(ABC):
    """Base for WS feeds: exponential backoff reconnect, staleness tracking, clean shutdown."""

    name: str = "ws"

    def __init__(self, url: str, stale_after_s: float = 10.0) -> None:
        self.url = url
        self.stale_after_s = stale_after_s
        self.last_msg_ns = 0
        self.connected = False
        self.reconnects = 0
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._ws: ClientConnection | None = None

    # -- lifecycle -----------------------------------------------------------
    def start(self) -> asyncio.Task[None]:
        self._task = asyncio.create_task(self._run(), name=f"feed:{self.name}")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

    @property
    def stale(self) -> bool:
        if self.last_msg_ns == 0:
            return True
        return (time.monotonic_ns() - self.last_msg_ns) / 1e9 > self.stale_after_s

    # -- to implement --------------------------------------------------------
    @abstractmethod
    async def on_connect(self, ws: ClientConnection) -> None: ...

    @abstractmethod
    async def on_message(self, raw: str | bytes) -> None: ...

    async def heartbeat(self, ws: ClientConnection) -> None:  # noqa: B027
        """Optional periodic app-level ping; default no-op (websockets does protocol pings)."""
        await asyncio.Event().wait()

    # -- loop ----------------------------------------------------------------
    async def _run(self) -> None:
        backoff = 0.5
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20, max_size=8 * 1024 * 1024, open_timeout=15
                ) as ws:
                    self._ws = ws
                    self.connected = True
                    self.last_msg_ns = time.monotonic_ns()
                    log.info("ws_connected", feed=self.name, url=self.url)
                    await self.on_connect(ws)
                    backoff = 0.5
                    hb = asyncio.create_task(self.heartbeat(ws))
                    try:
                        async for raw in ws:
                            self.last_msg_ns = time.monotonic_ns()
                            await self.on_message(raw)
                    finally:
                        hb.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self._stop.is_set():
                    break
                log.warning("ws_disconnected", feed=self.name, err=repr(e), retry_in_s=round(backoff, 2))
            finally:
                self.connected = False
                self._ws = None
            self.reconnects += 1
            await asyncio.sleep(backoff + random.uniform(0, 0.25))
            backoff = min(backoff * 2, 30.0)
