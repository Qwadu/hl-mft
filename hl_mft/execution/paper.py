from __future__ import annotations

from .. import clock, metrics
from ..book.l2 import OrderBook
from ..bus import EventBus
from ..config import FeesConfig, PaperConfig
from ..events import Bbo, Fill, L2Update, OrderEvent, OrderRequest, OrderStatus, Trade
from ..logging_setup import get_logger
from .base import OpenOrder

log = get_logger(__name__)


class _PaperOrder(OpenOrder):
    __slots__ = ("active_ns", "queue_ahead")

    def __init__(self, req: OrderRequest, placed_ns: int, active_ns: int, queue_ahead: float) -> None:
        super().__init__(req, placed_ns)
        self.active_ns = active_ns
        self.queue_ahead = queue_ahead


class PaperBroker:
    """Fill simulator driven by live L2 + trades.

    maker: rests at limit px; fills when (a) a trade prints through the price, (b) trades at the price
    exhaust the queue ahead (size resting at that level when we joined), or (c) the book crosses the px.
    taker: walks the current book immediately (after simulated latency), pays taker fee + slippage.
    """

    def __init__(self, cfg: PaperConfig, fees: FeesConfig, bus: EventBus) -> None:
        self.cfg = cfg
        self.fees = fees
        self.bus = bus
        self.books: dict[str, OrderBook] = {}
        self.orders: dict[str, dict[str, _PaperOrder]] = {}
        self.pending_taker: list[_PaperOrder] = []
        bus.subscribe(L2Update, self._on_l2)
        bus.subscribe(Bbo, self._on_bbo)
        bus.subscribe(Trade, self._on_trade)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def open_orders(self, coin: str) -> list[OpenOrder]:
        return list(self.orders.get(coin, {}).values())

    def _book(self, coin: str) -> OrderBook:
        b = self.books.get(coin)
        if b is None:
            b = self.books[coin] = OrderBook(coin)
        return b

    async def place(self, req: OrderRequest) -> bool:
        now = clock.now_ns()
        book = self._book(req.coin)
        if not book.ready:
            await self._emit(req, "rejected", now, "no_book")
            return False
        active = now + int(self.cfg.latency_ms * 1e6)
        queue = 0.0
        if req.kind == "maker":
            levels = book.bids if req.side > 0 else book.asks
            for lv in levels:
                if abs(lv.px - req.px) < 1e-12:
                    queue = lv.sz
                    break
            # post-only: reject if it would cross
            opp = book.best_ask.px if req.side > 0 else book.best_bid.px
            if (req.side > 0 and req.px >= opp) or (req.side < 0 and req.px <= opp):
                await self._emit(req, "rejected", now, "post_only_cross")
                return False
        o = _PaperOrder(req, now, active, queue)
        self.orders.setdefault(req.coin, {})[req.cid] = o
        metrics.orders_sent.labels(coin=req.coin, kind=req.kind).inc()
        await self._emit(req, "open", now)
        if req.kind == "taker":
            self.pending_taker.append(o)
        return True

    async def cancel(self, coin: str, cid: str, reason: str = "cancel") -> bool:
        o = self.orders.get(coin, {}).pop(cid, None)
        if o is None:
            return False
        if o in self.pending_taker:
            self.pending_taker.remove(o)
        await self._emit(o.req, "cancelled", clock.now_ns(), reason)
        return True

    async def cancel_all(self, coin: str | None = None, reason: str = "cancel_all") -> int:
        n = 0
        for c in list(self.orders):
            if coin is not None and c != coin:
                continue
            for cid in list(self.orders[c]):
                if await self.cancel(c, cid, reason):
                    n += 1
        return n

    # -- market data --------------------------------------------------------
    async def _on_l2(self, e: L2Update) -> None:
        book = self._book(e.coin)
        book.apply(e)
        await self._on_book(e.coin, book, e.recv_ns)

    async def _on_bbo(self, e: Bbo) -> None:
        book = self._book(e.coin)
        if not book.ready:
            return
        book.apply_bbo(e)
        await self._on_book(e.coin, book, e.recv_ns)

    async def _on_book(self, coin: str, book: OrderBook, now: int) -> None:
        # taker orders execute once latency has elapsed
        for o in [o for o in self.pending_taker if o.req.coin == coin and now >= o.active_ns]:
            self.pending_taker.remove(o)
            await self._exec_taker(o, book, now)
        for o in list(self.orders.get(coin, {}).values()):
            if o.req.kind != "maker":
                continue
            if o.expires_ns and now >= o.expires_ns:
                self.orders[coin].pop(o.req.cid, None)
                await self._emit(o.req, "expired", now, "ttl")
                continue
            if now < o.active_ns:
                continue
            crossed = (o.req.side > 0 and book.best_ask.px <= o.req.px) or (
                o.req.side < 0 and book.best_bid.px >= o.req.px
            )
            if crossed:
                await self._fill(o, o.req.px, o.remaining, True, now)

    async def _on_trade(self, t: Trade) -> None:
        for o in list(self.orders.get(t.coin, {}).values()):
            if o.req.kind != "maker" or t.recv_ns < o.active_ns:
                continue
            side = o.req.side
            # our buy fills when a seller aggresses at/below our price (trade side is aggressor)
            if side > 0 and not t.is_buy and t.px <= o.req.px:
                if t.px < o.req.px:
                    await self._fill(o, o.req.px, o.remaining, True, t.recv_ns)
                else:
                    await self._queue_step(o, t)
            elif side < 0 and t.is_buy and t.px >= o.req.px:
                if t.px > o.req.px:
                    await self._fill(o, o.req.px, o.remaining, True, t.recv_ns)
                else:
                    await self._queue_step(o, t)

    async def _queue_step(self, o: _PaperOrder, t: Trade) -> None:
        if o.queue_ahead > 0:
            o.queue_ahead -= t.sz
            if o.queue_ahead > 0:
                return
            spill = -o.queue_ahead
            o.queue_ahead = 0.0
        else:
            spill = t.sz
        if spill > 0:
            await self._fill(o, o.req.px, min(spill, o.remaining), True, t.recv_ns)

    async def _exec_taker(self, o: _PaperOrder, book: OrderBook, now: int) -> None:
        levels = book.asks if o.req.side > 0 else book.bids
        need = o.remaining
        cost = 0.0
        got = 0.0
        for lv in levels:
            if o.req.side > 0 and lv.px > o.req.px:
                break
            if o.req.side < 0 and lv.px < o.req.px:
                break
            take = min(lv.sz, need - got)
            cost += take * lv.px
            got += take
            if got >= need - 1e-12:
                break
        if got <= 0:
            self.orders[o.req.coin].pop(o.req.cid, None)
            await self._emit(o.req, "cancelled", now, "ioc_no_liquidity")
            return
        avg = cost / got
        avg *= 1 + o.req.side * self.cfg.taker_slippage_bps / 1e4
        await self._fill(o, avg, got, False, now)
        if o.req.cid in self.orders.get(o.req.coin, {}):
            self.orders[o.req.coin].pop(o.req.cid, None)
            await self._emit(o.req, "cancelled", now, "ioc_partial")

    async def _fill(self, o: _PaperOrder, px: float, sz: float, maker: bool, now: int) -> None:
        if sz <= 0:
            return
        fee_bps = self.fees.maker_bps if maker else self.fees.taker_bps
        fee = px * sz * fee_bps / 1e4
        o.filled_sz += sz
        metrics.paper_trades.labels(coin=o.req.coin, side="buy" if o.req.side > 0 else "sell").inc()
        await self.bus.publish(Fill(o.req.coin, o.req.cid, now, px, o.req.side * sz, fee, maker, o.req.tag))
        if o.remaining <= 1e-12:
            self.orders[o.req.coin].pop(o.req.cid, None)
            await self._emit(o.req, "filled", now)

    async def _emit(self, req: OrderRequest, status: OrderStatus, now: int, reason: str = "") -> None:
        await self.bus.publish(OrderEvent(req.coin, req.cid, status, now, reason))
