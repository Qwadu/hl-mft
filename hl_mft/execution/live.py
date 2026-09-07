from __future__ import annotations

import asyncio
import hashlib
import time
from typing import TYPE_CHECKING, Any

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants
from hyperliquid.utils.types import Cloid

from .. import metrics
from ..bus import EventBus
from ..config import AppConfig, Secrets
from ..events import Fill, OrderEvent, OrderRequest, OrderStatus
from ..feeds.hl_info import HLInfo, PerpMeta
from ..logging_setup import get_logger
from .base import OpenOrder

if TYPE_CHECKING:
    from ..portfolio import Portfolio
    from ..strategy.flow import FlowStrategy

log = get_logger(__name__)


def to_cloid(cid: str) -> Cloid:
    h = hashlib.sha1(cid.encode()).hexdigest()[:32]
    return Cloid("0x" + h)


class LiveBroker:
    """Hyperliquid execution via the official SDK (sync HTTP, run in threads).

    - maker  -> limit Alo (post-only), TTL enforced locally by cancel-by-cloid
    - taker  -> limit Ioc (marketable), never Gtc so nothing rests unintentionally
    - fills / order status arrive via the user WS channels (userFills, orderUpdates)
    - periodic reconciliation against clearinghouseState + frontendOpenOrders
    """

    def __init__(
        self, secrets: Secrets, cfg: AppConfig, bus: EventBus, info: HLInfo, metas: dict[str, PerpMeta]
    ) -> None:
        self.cfg = cfg
        self.bus = bus
        self.info = info
        self.metas = metas
        self.address = secrets.hl_account_address
        wallet = Account.from_key(secrets.hl_agent_private_key)
        base = constants.TESTNET_API_URL if secrets.hl_testnet else constants.MAINNET_API_URL
        self.ex = Exchange(wallet, base, account_address=self.address)
        self.orders: dict[str, dict[str, OpenOrder]] = {}
        self.cloid_to_cid: dict[str, str] = {}
        self.pf: Portfolio | None = None
        self.strategy: FlowStrategy | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._lev_set: set[str] = set()
        self.last_reconcile: dict[str, Any] = {}

    def attach(self, pf: Portfolio, strategy: FlowStrategy) -> None:
        self.pf = pf
        self.strategy = strategy

    # -- lifecycle -----------------------------------------------------------
    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._ttl_loop(), name="live-ttl"))
        self._tasks.append(asyncio.create_task(self._reconcile_loop(), name="live-reconcile"))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()

    async def account_value(self) -> float:
        st = await self.info.clearinghouse_state(self.address)
        return float(st["marginSummary"]["accountValue"])

    def open_orders(self, coin: str) -> list[OpenOrder]:
        return list(self.orders.get(coin, {}).values())

    # -- orders --------------------------------------------------------------
    async def _ensure_leverage(self, coin: str) -> None:
        if coin in self._lev_set:
            return
        meta = self.metas.get(coin)
        lev = min(self.cfg.risk.leverage, meta.max_leverage if meta else self.cfg.risk.leverage)
        try:
            r = await asyncio.to_thread(self.ex.update_leverage, lev, coin, not self.cfg.risk.isolated)
            log.info("leverage_set", coin=coin, leverage=lev, isolated=self.cfg.risk.isolated, resp=r)
        except Exception as e:  # noqa: BLE001
            log.warning("leverage_set_failed", coin=coin, err=repr(e))
        self._lev_set.add(coin)

    async def place(self, req: OrderRequest) -> bool:
        await self._ensure_leverage(req.coin)
        cloid = to_cloid(req.cid)
        self.cloid_to_cid[cloid.to_raw()] = req.cid
        tif = "Alo" if req.kind == "maker" else "Ioc"
        o = OpenOrder(req, time.time_ns())
        self.orders.setdefault(req.coin, {})[req.cid] = o
        metrics.orders_sent.labels(coin=req.coin, kind=req.kind).inc()
        try:
            resp = await asyncio.to_thread(
                self.ex.order,
                req.coin,
                req.side > 0,
                req.sz,
                req.px,
                {"limit": {"tif": tif}},
                req.reduce_only,
                cloid,
            )
        except Exception as e:  # noqa: BLE001
            self.orders[req.coin].pop(req.cid, None)
            await self._emit(req, "rejected", repr(e)[:120])
            return False
        st = self._first_status(resp)
        if st is None or "error" in st:
            self.orders[req.coin].pop(req.cid, None)
            await self._emit(req, "rejected", str(st.get("error") if st else resp)[:160])
            return False
        if "resting" in st:
            o.oid = int(st["resting"]["oid"])
            await self._emit(req, "open")
        elif "filled" in st:
            o.oid = int(st["filled"]["oid"])
            await self._emit(req, "open")
            # fill details (fee, maker flag) arrive on userFills; IOC leftovers are cancelled by the exchange
        return True

    @staticmethod
    def _first_status(resp: Any) -> dict[str, Any] | None:
        try:
            if resp.get("status") != "ok":
                return {"error": str(resp)}
            sts = resp["response"]["data"]["statuses"]
            return sts[0] if sts else None
        except (KeyError, AttributeError, IndexError, TypeError):
            return {"error": str(resp)}

    async def cancel(self, coin: str, cid: str, reason: str = "cancel") -> bool:
        o = self.orders.get(coin, {}).get(cid)
        if o is None:
            return False
        try:
            resp = await asyncio.to_thread(self.ex.cancel_by_cloid, coin, to_cloid(cid))
        except Exception as e:  # noqa: BLE001
            log.warning("cancel_failed", coin=coin, cid=cid, err=repr(e))
            return False
        st = self._first_status(resp)
        ok = resp.get("status") == "ok" and (st is None or "error" not in st)
        if ok:
            self.orders[coin].pop(cid, None)
            await self._emit(o.req, "cancelled", reason)
        else:
            # typically "order already filled/cancelled" - orderUpdates will settle the state
            log.info("cancel_noop", coin=coin, cid=cid, resp=str(resp)[:160])
        return bool(ok)

    async def cancel_all(self, coin: str | None = None, reason: str = "cancel_all") -> int:
        n = 0
        for c in list(self.orders):
            if coin is not None and c != coin:
                continue
            for cid in list(self.orders[c]):
                if await self.cancel(c, cid, reason):
                    n += 1
        return n

    async def _emit(self, req: OrderRequest, status: OrderStatus, reason: str = "") -> None:
        await self.bus.publish(OrderEvent(req.coin, req.cid, status, time.time_ns(), reason))

    # -- user WS channels ----------------------------------------------------
    async def on_user_message(self, ch: str, data: Any) -> None:
        if ch == "userFills":
            if data.get("isSnapshot"):
                return
            for f in data.get("fills", []):
                await self._on_fill(f)
        elif ch == "orderUpdates":
            for u in data:
                await self._on_order_update(u)

    async def _on_fill(self, f: dict[str, Any]) -> None:
        coin = f["coin"]
        cloid = f.get("cloid")
        cid = self.cloid_to_cid.get(cloid or "", "")
        sz = float(f["sz"]) * (1 if f["side"] == "B" else -1)
        fee = float(f.get("fee", 0.0))
        maker = not bool(f.get("crossed", True))
        o = self.orders.get(coin, {}).get(cid)
        tag = o.req.tag if o else "external"
        if o is not None:
            o.filled_sz += abs(sz)
            if o.remaining <= 1e-12:
                self.orders[coin].pop(cid, None)
                await self._emit(o.req, "filled")
        await self.bus.publish(Fill(coin, cid, time.time_ns(), float(f["px"]), sz, fee, maker, tag))

    async def _on_order_update(self, u: dict[str, Any]) -> None:
        od = u.get("order", {})
        coin = od.get("coin", "")
        cid = self.cloid_to_cid.get(od.get("cloid") or "", "")
        status = u.get("status", "")
        o = self.orders.get(coin, {}).get(cid)
        if o is None:
            return
        if status == "open":
            o.oid = int(od.get("oid", o.oid))
        elif status == "filled":
            pass  # settled by userFills
        elif status in ("canceled", "marginCanceled", "rejected", "liquidatedCanceled", "reduceOnlyCanceled"):
            self.orders[coin].pop(cid, None)
            st: OrderStatus = "rejected" if status == "rejected" else "cancelled"
            await self._emit(o.req, st, status)

    # -- background ----------------------------------------------------------
    async def _ttl_loop(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            now = time.time_ns()
            for coin in list(self.orders):
                for o in list(self.orders[coin].values()):
                    if o.expires_ns and now >= o.expires_ns:
                        await self.cancel(coin, o.req.cid, "ttl")

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.risk.reconcile_interval_s)
            try:
                await self.reconcile()
            except Exception as e:  # noqa: BLE001
                log.warning("reconcile_failed", err=repr(e))

    async def reconcile(self) -> None:
        if self.pf is None:
            return
        st, oo = await asyncio.gather(
            self.info.clearinghouse_state(self.address), self.info.open_orders(self.address)
        )
        ex_pos: dict[str, tuple[float, float]] = {}
        for ap in st.get("assetPositions", []):
            p = ap["position"]
            szi = float(p["szi"])
            if szi != 0:
                ex_pos[p["coin"]] = (szi, float(p.get("entryPx") or 0.0))
        for coin, (szi, entry) in ex_pos.items():
            lp = self.pf.pos(coin)
            if abs(lp.size - szi) > 1e-9:
                log.warning("position_mismatch", coin=coin, local=lp.size, exchange=szi)
                self.pf.set_position(coin, szi, entry)
        for lp in self.pf.open_positions():
            if lp.coin not in ex_pos:
                log.warning("position_mismatch", coin=lp.coin, local=lp.size, exchange=0.0)
                self.pf.set_position(lp.coin, 0.0, 0.0)
        ex_cloids = {o.get("cloid") for o in oo if o.get("cloid")}
        for coin in list(self.orders):
            for o in list(self.orders[coin].values()):
                age_s = (time.time_ns() - o.placed_ns) / 1e9
                if age_s > 3.0 and to_cloid(o.req.cid).to_raw() not in ex_cloids:
                    self.orders[coin].pop(o.req.cid, None)
                    await self._emit(o.req, "cancelled", "reconcile_missing")
        self.pf.sync_equity(float(st["marginSummary"]["accountValue"]))
        self.last_reconcile = {
            "t": time.time(),
            "positions": len(ex_pos),
            "open_orders": len(oo),
            "withdrawable": st.get("withdrawable"),
        }
