from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..events import OrderRequest


@dataclass(slots=True)
class OpenOrder:
    req: OrderRequest
    placed_ns: int
    filled_sz: float = 0.0
    oid: int = 0  # exchange id (live only)

    @property
    def remaining(self) -> float:
        return self.req.sz - self.filled_sz

    @property
    def expires_ns(self) -> int:
        return self.placed_ns + int(self.req.ttl_s * 1e9) if self.req.ttl_s > 0 else 0


class Broker(Protocol):
    async def place(self, req: OrderRequest) -> bool: ...
    async def cancel(self, coin: str, cid: str, reason: str = "cancel") -> bool: ...
    async def cancel_all(self, coin: str | None = None, reason: str = "cancel_all") -> int: ...
    def open_orders(self, coin: str) -> list[OpenOrder]: ...
    async def quote(self, coin: str) -> tuple[float, float] | None:
        """Current (bid, ask) for a coin, or None if unknown — used when no feature state exists."""
        ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
