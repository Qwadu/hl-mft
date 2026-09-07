from __future__ import annotations

import math

from .config import UniverseConfig
from .feeds.hl_info import HLInfo, PerpMeta
from .logging_setup import get_logger

log = get_logger(__name__)


def tick_bps(m: PerpMeta) -> float:
    """Approximate minimum price increment in bps given HL's 5-significant-figure rule."""
    px = m.mark_px
    if px <= 0:
        return 1e9
    exp = math.floor(math.log10(px))
    tick = 10 ** (exp - 4)  # 5 sig figs
    tick = max(tick, 10 ** (-m.px_decimals))
    return float(tick) / px * 1e4


def select_universe(metas: list[PerpMeta], cfg: UniverseConfig) -> list[str]:
    if cfg.mode == "static":
        return list(cfg.static_coins)
    cands: list[tuple[float, PerpMeta]] = []
    for m in metas:
        if m.coin in cfg.blacklist or any(m.coin.startswith(p) for p in cfg.blacklist_prefixes):
            continue
        if m.day_ntl_vlm < cfg.min_day_volume_usd:
            continue
        if tick_bps(m) > cfg.max_tick_bps:
            continue
        # rank by volume with OI as a tiebreaker-ish multiplier
        score = m.day_ntl_vlm * (1.0 + min(m.open_interest * m.mark_px / max(m.day_ntl_vlm, 1.0), 2.0))
        cands.append((score, m))
    cands.sort(key=lambda x: -x[0])
    coins = [m.coin for _, m in cands[: cfg.top_n]]
    for c in cfg.always_include:
        if c not in coins:
            coins.append(c)
    return coins


async def fetch_universe(info: HLInfo, cfg: UniverseConfig) -> tuple[list[str], dict[str, PerpMeta]]:
    metas = await info.meta_and_ctxs()
    by_coin = {m.coin: m for m in metas}
    coins = [c for c in select_universe(metas, cfg) if c in by_coin]
    log.info("universe_selected", coins=coins)
    return coins, by_coin
