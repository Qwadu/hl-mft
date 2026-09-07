from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp

from ..logging_setup import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class PerpMeta:
    coin: str
    asset_index: int
    sz_decimals: int
    max_leverage: int
    only_isolated: bool
    day_ntl_vlm: float
    mark_px: float
    open_interest: float
    funding: float

    @property
    def px_decimals(self) -> int:
        # Hyperliquid perps: prices have up to 5 significant figures and at most (6 - szDecimals) decimals.
        return 6 - self.sz_decimals


class HLInfo:
    """Thin async client for the `/info` endpoint (public, unauthenticated)."""

    def __init__(self, url: str) -> None:
        self.url = url
        self._session: aiohttp.ClientSession | None = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def post(self, body: dict[str, Any]) -> Any:
        s = await self._sess()
        async with s.post(self.url, json=body) as r:
            r.raise_for_status()
            return await r.json()

    async def meta_and_ctxs(self) -> list[PerpMeta]:
        meta, ctxs = await self.post({"type": "metaAndAssetCtxs"})
        out: list[PerpMeta] = []
        for i, (u, c) in enumerate(zip(meta["universe"], ctxs, strict=False)):
            if u.get("isDelisted"):
                continue
            out.append(
                PerpMeta(
                    coin=u["name"],
                    asset_index=i,
                    sz_decimals=int(u["szDecimals"]),
                    max_leverage=int(u["maxLeverage"]),
                    only_isolated=bool(u.get("onlyIsolated", False)),
                    day_ntl_vlm=float(c["dayNtlVlm"]),
                    mark_px=float(c["markPx"]),
                    open_interest=float(c["openInterest"]),
                    funding=float(c["funding"]),
                )
            )
        return out

    async def l2_book(self, coin: str) -> Any:
        return await self.post({"type": "l2Book", "coin": coin})

    async def clearinghouse_state(self, user: str) -> Any:
        return await self.post({"type": "clearinghouseState", "user": user})

    async def open_orders(self, user: str) -> Any:
        return await self.post({"type": "frontendOpenOrders", "user": user})

    async def user_fees(self, user: str) -> Any:
        return await self.post({"type": "userFees", "user": user})

    async def user_rate_limit(self, user: str) -> Any:
        return await self.post({"type": "userRateLimit", "user": user})
