from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field

from .. import clock, metrics
from ..bus import EventBus
from ..config import StrategyConfig
from ..events import FeatureVector, Fill, OrderEvent, OrderRequest
from ..execution.base import Broker
from ..execution.rounding import px_tick, round_px, round_sz
from ..feeds.hl_info import PerpMeta
from ..logging_setup import get_logger
from ..portfolio import Portfolio
from ..risk import RiskManager

log = get_logger(__name__)

_cid_seq = itertools.count(int(time.time()) % 100000 * 1000)


def next_cid(prefix: str) -> str:
    return f"{prefix}-{next(_cid_seq)}"


def fv_quote(fv: FeatureVector) -> tuple[float, float]:
    half = fv.spread_bps / 2e4
    return fv.mid * (1 - half), fv.mid * (1 + half)


def compute_score(fv: FeatureVector, cfg: StrategyConfig) -> float:
    s = 0.0
    clip = cfg.z_clip
    for name, w in cfg.weights.items():
        z = fv.zscores.get(name)
        if z is None:
            continue
        s += w * max(-clip, min(clip, z))
    return s


@dataclass(slots=True)
class CoinState:
    coin: str
    score: float = 0.0
    last_fv: FeatureVector | None = None
    entry_cid: str = ""
    exit_cid: str = ""
    exit_taker: bool = False
    exit_deadline_ns: int = 0
    exit_sent_ns: int = 0
    exit_reason: str = ""
    cooldown_until_ns: int = 0
    stop_bps: float = 0.0
    log: list[dict[str, object]] = field(default_factory=list)


class FlowStrategy:
    """Flow-following micro-momentum: score = Σ w·z(feature); enter with flow, exit on reversal/TP/stop."""

    def __init__(
        self,
        cfg: StrategyConfig,
        bus: EventBus,
        broker: Broker,
        pf: Portfolio,
        risk: RiskManager,
        metas: dict[str, PerpMeta],
    ) -> None:
        self.cfg = cfg
        self.bus = bus
        self.broker = broker
        self.pf = pf
        self.risk = risk
        self.metas = metas
        self.states: dict[str, CoinState] = {}
        self.enabled = True
        self.events: list[dict[str, object]] = []
        self.max_events = 500
        bus.subscribe(FeatureVector, self.on_features)
        bus.subscribe(Fill, self.on_fill)
        bus.subscribe(OrderEvent, self.on_order_event)

    def state(self, coin: str) -> CoinState:
        st = self.states.get(coin)
        if st is None:
            st = self.states[coin] = CoinState(coin)
        return st

    def _log(self, coin: str, what: str, **kw: object) -> None:
        ev: dict[str, object] = {"t": clock.now_s(), "coin": coin, "what": what, **kw}
        self.events.append(ev)
        if len(self.events) > self.max_events:
            del self.events[: self.max_events // 5]
        log.info("strategy_event", **ev)

    # -- events --------------------------------------------------------------
    async def on_fill(self, f: Fill) -> None:
        self.pf.on_fill(f.coin, f.px, f.sz, f.fee, f.maker)
        st = self.state(f.coin)
        if f.cid == st.entry_cid:
            self.pf.pos(f.coin).entry_score = st.score
        self._log(f.coin, "fill", cid=f.cid, px=f.px, sz=f.sz, fee=round(f.fee, 5), maker=f.maker, tag=f.tag)
        self.risk.check_limits()

    async def on_order_event(self, e: OrderEvent) -> None:
        st = self.state(e.coin)
        if e.status in ("filled", "cancelled", "expired", "rejected"):
            if e.cid == st.entry_cid:
                st.entry_cid = ""
                self.risk.release(e.coin)
                if e.status != "filled":
                    st.cooldown_until_ns = e.recv_ns + int(self.cfg.cooldown_s * 1e9 / 2)
            elif e.cid == st.exit_cid:
                st.exit_cid = ""
            if e.status == "rejected":
                metrics.orders_rejected.labels(coin=e.coin, reason=e.reason).inc()
                self._log(e.coin, "rejected", cid=e.cid, reason=e.reason)

    async def on_features(self, fv: FeatureVector) -> None:
        st = self.state(fv.coin)
        st.last_fv = fv
        fv.score = st.score = compute_score(fv, self.cfg)
        metrics.score_gauge.labels(coin=fv.coin).set(st.score)
        metrics.mid_px.labels(coin=fv.coin).set(fv.mid)
        metrics.spread_bps.labels(coin=fv.coin).set(fv.spread_bps)
        self.pf.mark(fv.coin, fv.mid)
        if not self.enabled:
            return
        if self.cfg.enabled_coins and fv.coin not in self.cfg.enabled_coins:
            return
        pos = self.pf.pos(fv.coin)
        now = fv.recv_ns
        if pos.size != 0:
            await self._manage_position(st, fv, pos.size, pos.entry_px, pos.opened_ns)
        else:
            await self._maybe_enter(st, fv, now)

    # -- entries -------------------------------------------------------------
    async def _maybe_enter(self, st: CoinState, fv: FeatureVector, now: int) -> None:
        cfg = self.cfg
        score = st.score
        if st.entry_cid:
            # pending passive entry: pull it if signal faded or flipped
            side = 1 if self._pending_side(st) > 0 else -1
            if score * side < cfg.theta_exit:
                await self.broker.cancel(fv.coin, st.entry_cid, "signal_faded")
            return
        if st.exit_cid:
            return
        if now < st.cooldown_until_ns:
            return
        if abs(score) < cfg.theta_enter:
            return
        if fv.spread_bps > cfg.max_spread_bps:
            return
        if cfg.require_ref and fv.values.get("ref_age_ms", -1) < 0:
            return
        ok, why = self.risk.can_open(fv.coin)
        if not ok:
            return
        meta = self.metas.get(fv.coin)
        if meta is None:
            return
        rv = fv.values.get("rv_bps", 0.0)
        stop_bps = max(cfg.stop_loss_vol_mult * rv, cfg.min_stop_bps)
        ntl = self.risk.size_notional(fv.mid, stop_bps)
        if ntl <= 0:
            return
        side = 1 if score > 0 else -1
        sz = round_sz(ntl / fv.mid, meta.sz_decimals)
        if sz <= 0:
            return
        taker = abs(score) >= cfg.theta_taker
        cf_book = fv.values
        bid = fv.mid * (1 - fv.spread_bps / 2e4)
        ask = fv.mid * (1 + fv.spread_bps / 2e4)
        if taker:
            # marketable limit a few ticks through the touch (IOC)
            tick = px_tick(fv.mid, meta.sz_decimals)
            px = (ask + 3 * tick) if side > 0 else (bid - 3 * tick)
        else:
            px = bid if side > 0 else ask
        px = round_px(px, meta.sz_decimals)
        if not self.risk.budget.take():
            return
        cid = next_cid("e")
        st.entry_cid = cid
        st.stop_bps = stop_bps
        self.risk.reserve(fv.coin, sz * px)
        req = OrderRequest(
            coin=fv.coin,
            side=side,
            sz=sz,
            px=px,
            kind="taker" if taker else "maker",
            cid=cid,
            ttl_s=0.0 if taker else cfg.passive_ttl_s,
            tag="entry",
        )
        self._log(
            fv.coin,
            "enter",
            side=side,
            kind=req.kind,
            px=px,
            sz=sz,
            score=round(score, 2),
            stop_bps=round(stop_bps, 1),
            obi5=round(cf_book.get("obi_5", 0), 3),
            ll=round(cf_book.get("leadlag_bps", 0), 2),
        )
        if not await self.broker.place(req):
            st.entry_cid = ""
            self.risk.release(fv.coin)

    def _pending_side(self, st: CoinState) -> int:
        for o in self.broker.open_orders(st.coin):
            if o.req.cid == st.entry_cid:
                return o.req.side
        return 1 if st.score > 0 else -1

    # -- exits ---------------------------------------------------------------
    async def _manage_position(
        self, st: CoinState, fv: FeatureVector, size: float, entry: float, opened_ns: int
    ) -> None:
        cfg = self.cfg
        side = 1 if size > 0 else -1
        now = fv.recv_ns
        pnl_bps = side * (fv.mid - entry) / entry * 1e4
        held_s = (now - opened_ns) / 1e9 if opened_ns else 0.0
        reason = ""
        urgent = False
        if self.risk.killed:
            reason, urgent = "kill_switch", True
        elif pnl_bps <= -st.stop_bps and st.stop_bps > 0:
            reason, urgent = "stop", True
        elif held_s >= cfg.max_hold_s:
            reason, urgent = "max_hold", True
        elif side * st.score <= -cfg.theta_exit:
            reason, urgent = "reversal", side * st.score <= -cfg.theta_enter
        elif pnl_bps >= cfg.take_profit_bps:
            reason = "take_profit"

        if st.exit_cid:
            # passive exit resting: escalate to taker when TTL passes or situation becomes urgent
            if not st.exit_taker and (urgent or (st.exit_deadline_ns and now >= st.exit_deadline_ns)):
                await self.broker.cancel(fv.coin, st.exit_cid, "escalate")
                st.exit_cid = ""
                await self._send_exit(
                    st, fv_quote(fv), now, side, abs(size), reason or st.exit_reason, taker=True
                )
            return
        if st.entry_cid:
            await self.broker.cancel(fv.coin, st.entry_cid, "in_position")
        if not reason:
            return
        taker = urgent or cfg.exit_style == "taker"
        await self._send_exit(st, fv_quote(fv), now, side, abs(size), reason, taker=taker)

    async def _send_exit(
        self,
        st: CoinState,
        quote: tuple[float, float],
        now: int,
        side: int,
        sz: float,
        reason: str,
        taker: bool,
    ) -> bool:
        meta = self.metas.get(st.coin)
        if meta is None:
            self._log(st.coin, "exit_failed", reason=reason, err="no_meta")
            return False
        if not self.risk.budget.take(emergency=taker):
            self._log(st.coin, "exit_failed", reason=reason, err="rate_budget")
            return False
        bid, ask = quote
        tick = px_tick((bid + ask) / 2, meta.sz_decimals)
        if taker:
            px = (bid - 5 * tick) if side > 0 else (ask + 5 * tick)
        else:
            px = ask if side > 0 else bid
        px = round_px(px, meta.sz_decimals)
        cid = next_cid("x")
        st.exit_cid = cid
        st.exit_taker = taker
        st.exit_reason = reason
        st.exit_sent_ns = now
        st.exit_deadline_ns = 0 if taker else now + int(self.cfg.passive_ttl_s * 1e9)
        st.cooldown_until_ns = now + int(self.cfg.cooldown_s * 1e9)
        req = OrderRequest(
            coin=st.coin,
            side=-side,
            sz=round_sz(sz, meta.sz_decimals),
            px=px,
            kind="taker" if taker else "maker",
            cid=cid,
            reduce_only=True,
            ttl_s=0.0 if taker else self.cfg.passive_ttl_s * 2,
            tag=f"exit:{reason}",
        )
        self._log(st.coin, "exit", reason=reason, kind=req.kind, px=px, sz=req.sz, score=round(st.score, 2))
        if not await self.broker.place(req):
            st.exit_cid = ""
            return False
        return True

    async def flatten_all(self, reason: str = "manual", retry_after_s: float = 5.0) -> list[str]:
        """Taker-exit every open position; returns coins that could not be sent (caller must alert).

        Works without feature state (falls back to the broker's quote) so reconciled positions in
        coins outside the universe are covered. A taker exit already in flight is not re-sent until
        `retry_after_s` has passed.
        """
        now = clock.now_ns()
        failed: list[str] = []
        positions = {p.coin: p for p in self.pf.open_positions()}
        for coin in list(self.states):
            if coin not in positions:
                await self.broker.cancel_all(coin=coin, reason=reason)
        for coin, p in positions.items():
            st = self.state(coin)
            inflight = bool(st.exit_cid) and st.exit_taker and now - st.exit_sent_ns < retry_after_s * 1e9
            for o in self.broker.open_orders(coin):
                if not (inflight and o.req.cid == st.exit_cid):
                    await self.broker.cancel(coin, o.req.cid, reason)
            if inflight:
                continue
            st.entry_cid = ""
            st.exit_cid = ""
            self.risk.release(coin)
            fv = st.last_fv
            fresh = fv is not None and now - fv.recv_ns < 10e9
            quote = fv_quote(fv) if fv is not None and fresh else await self.broker.quote(p.coin)
            if quote is None:
                self._log(p.coin, "exit_failed", reason=reason, err="no_quote")
                failed.append(p.coin)
                continue
            if not await self._send_exit(st, quote, now, p.side, abs(p.size), reason, taker=True):
                failed.append(p.coin)
        if failed:
            metrics.flatten_failures.inc(len(failed))
            log.error("flatten_incomplete", reason=reason, coins=failed)
        return failed

    def snapshot(self) -> dict[str, object]:
        out: dict[str, object] = {}
        for c, st in self.states.items():
            fv = st.last_fv
            out[c] = {
                "score": round(st.score, 3),
                "mid": fv.mid if fv else None,
                "spread_bps": round(fv.spread_bps, 2) if fv else None,
                "entry_cid": st.entry_cid,
                "exit_cid": st.exit_cid,
                "z": {k: round(v, 2) for k, v in fv.zscores.items()} if fv else {},
                "f": {k: round(v, 4) for k, v in fv.values.items()} if fv else {},
            }
        return out
