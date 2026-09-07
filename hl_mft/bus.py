from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

from .logging_setup import get_logger

log = get_logger(__name__)

Handler = Callable[[Any], Awaitable[None] | None]


class EventBus:
    """Minimal in-process pub/sub keyed by event class. Sync handlers run inline; async are awaited."""

    def __init__(self) -> None:
        self._handlers: dict[type, list[Handler]] = defaultdict(list)

    def subscribe(self, event_type: type, handler: Handler) -> None:
        self._handlers[event_type].append(handler)

    async def publish(self, event: Any) -> None:
        for h in self._handlers.get(type(event), ()):
            try:
                res = h(event)
                if asyncio.iscoroutine(res):
                    await res
            except Exception:  # noqa: BLE001 - never let one handler kill the feed loop
                log.exception("handler_failed", handler=getattr(h, "__qualname__", repr(h)))
