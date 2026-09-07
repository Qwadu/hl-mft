from __future__ import annotations

from collections import deque

from ..book.l2 import OrderBook
from ..bus import EventBus
from ..config import FeatureConfig
from ..events import Bbo, FeatureVector, L2Update, RefTick, Trade
from .rolling import RollingStats, TimeWindowSum

REF_MAX_AGE_NS = 5_000_000_000


class CoinFeatures:
    """Per-coin state: book, trade-flow windows, reference prices, z-score stats."""

    def __init__(self, coin: str, cfg: FeatureConfig) -> None:
        self.coin = coin
        self.cfg = cfg
        self.book = OrderBook(coin)
        self.cvd = {w: TimeWindowSum(w) for w in cfg.cvd_windows_s}
        self.vol = {w: TimeWindowSum(w) for w in cfg.cvd_windows_s}
        self.ref: dict[str, RefTick] = {}
        self.mid_hist: deque[tuple[int, float]] = deque()
        self.shape_hist: deque[tuple[int, float, float]] = deque()
        self.stats: dict[str, RollingStats] = {}
        self.last_vector: FeatureVector | None = None
        self.last_trade_ns = 0
        self.rv_bps = 0.0  # realized vol proxy (bps per sqrt-second) from mid changes

    def on_trade(self, t: Trade) -> None:
        ntl = t.px * t.sz
        signed = ntl if t.is_buy else -ntl
        for w in self.cfg.cvd_windows_s:
            self.cvd[w].add(t.recv_ns, signed)
            self.vol[w].add(t.recv_ns, ntl)
        self.last_trade_ns = t.recv_ns

    def on_ref(self, r: RefTick) -> None:
        self.ref[r.source] = r

    def best_ref(self, now_ns: int) -> RefTick | None:
        # Binance preferred (perp vs perp); LSE spot composite as fallback
        for src in ("binance", "lse"):
            r = self.ref.get(src)
            if r is not None and now_ns - r.recv_ns < REF_MAX_AGE_NS:
                return r
        return None

    def _z(self, name: str, v: float, now_ns: int) -> float:
        st = self.stats.get(name)
        if st is None:
            st = self.stats[name] = RollingStats(self.cfg.zscore_window_s)
        z = st.z(v)
        st.add(now_ns, v)
        return z

    def compute(self, now_ns: int) -> FeatureVector | None:
        b = self.book
        if not b.ready:
            return None
        mid = b.mid
        if mid <= 0:
            return None
        vals: dict[str, float] = {}
        for k in self.cfg.obi_levels:
            vals[f"obi_{k}"] = b.imbalance(k)
        vals["microprice_bps"] = (b.microprice() - mid) / mid * 1e4

        for w in self.cfg.cvd_windows_s:
            v = self.vol[w].value(now_ns)
            vals[f"cvd_{int(w)}"] = self.cvd[w].value(now_ns) / v if v > 0 else 0.0
            vals[f"vol_{int(w)}"] = v

        ref = self.best_ref(now_ns)
        vals["leadlag_bps"] = (ref.mid - mid) / mid * 1e4 if ref else 0.0
        vals["ref_age_ms"] = (now_ns - ref.recv_ns) / 1e6 if ref else -1.0

        # book-shape: change of top-5 bid/ask size over book_shape_dt_s, normalized by current total
        bs, as_ = b.total_size(5)
        self.shape_hist.append((now_ns, bs, as_))
        cutoff = now_ns - int(self.cfg.book_shape_dt_s * 1e9)
        while len(self.shape_hist) > 1 and self.shape_hist[1][0] <= cutoff:
            self.shape_hist.popleft()
        _, bs0, as0 = self.shape_hist[0]
        tot = bs + as_
        vals["book_shape"] = ((bs - bs0) - (as_ - as0)) / tot if tot > 0 else 0.0

        # realized vol proxy from mid path over vol_window_s
        self.mid_hist.append((now_ns, mid))
        vcut = now_ns - int(self.cfg.vol_window_s * 1e9)
        while len(self.mid_hist) > 1 and self.mid_hist[0][0] < vcut:
            self.mid_hist.popleft()
        if len(self.mid_hist) >= 5:
            hi = max(m for _, m in self.mid_hist)
            lo = min(m for _, m in self.mid_hist)
            span_s = max((self.mid_hist[-1][0] - self.mid_hist[0][0]) / 1e9, 1.0)
            self.rv_bps = (hi - lo) / mid * 1e4 / (span_s**0.5)
        vals["rv_bps"] = self.rv_bps
        vals["spread_bps"] = b.spread_bps

        zs = {
            k: self._z(k, v, now_ns)
            for k, v in vals.items()
            if not k.startswith(("vol_", "ref_age", "rv_", "spread"))
        }
        fv = FeatureVector(
            coin=self.coin, recv_ns=now_ns, mid=mid, spread_bps=b.spread_bps, values=vals, zscores=zs
        )
        self.last_vector = fv
        return fv


class FeatureEngine:
    """Maintains CoinFeatures from raw events and emits FeatureVector.

    HL pushes full l2Book snapshots only every few seconds, so the book top is refreshed from the
    `bbo` channel and vectors are emitted on l2/bbo/trade, throttled to `min_emit_dt_s` per coin.
    """

    def __init__(self, cfg: FeatureConfig, bus: EventBus, coins: list[str]) -> None:
        self.cfg = cfg
        self.bus = bus
        self.coins: dict[str, CoinFeatures] = {c: CoinFeatures(c, cfg) for c in coins}
        self._min_dt_ns = int(cfg.min_emit_dt_s * 1e9)
        bus.subscribe(L2Update, self._on_l2)
        bus.subscribe(Bbo, self._on_bbo)
        bus.subscribe(Trade, self._on_trade)
        bus.subscribe(RefTick, self._on_ref)

    def ensure(self, coin: str) -> CoinFeatures:
        cf = self.coins.get(coin)
        if cf is None:
            cf = self.coins[coin] = CoinFeatures(coin, self.cfg)
        return cf

    def set_coins(self, coins: list[str]) -> None:
        for c in coins:
            self.ensure(c)
        for c in list(self.coins):
            if c not in coins:
                del self.coins[c]

    async def _emit(self, cf: CoinFeatures, now_ns: int, force: bool) -> None:
        lv = cf.last_vector
        if not force and lv is not None and now_ns - lv.recv_ns < self._min_dt_ns:
            return
        fv = cf.compute(now_ns)
        if fv is not None:
            await self.bus.publish(fv)

    async def _on_l2(self, e: L2Update) -> None:
        cf = self.coins.get(e.coin)
        if cf is None:
            return
        cf.book.apply(e)
        await self._emit(cf, e.recv_ns, force=True)

    async def _on_bbo(self, e: Bbo) -> None:
        cf = self.coins.get(e.coin)
        if cf is None:
            return
        cf.book.apply_bbo(e)
        await self._emit(cf, e.recv_ns, force=False)

    async def _on_trade(self, e: Trade) -> None:
        cf = self.coins.get(e.coin)
        if cf is None:
            return
        cf.on_trade(e)
        await self._emit(cf, e.recv_ns, force=False)

    def _on_ref(self, e: RefTick) -> None:
        cf = self.coins.get(e.coin)
        if cf is not None:
            cf.on_ref(e)
