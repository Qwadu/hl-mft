from __future__ import annotations

import math
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal


def round_px(px: float, sz_decimals: int, sig_figs: int = 5, max_decimals: int = 6) -> float:
    """Hyperliquid perp price rule: <= 5 significant figures and <= (6 - szDecimals) decimals."""
    if px <= 0:
        return px
    decimals_cap = max_decimals - sz_decimals
    exp = math.floor(math.log10(px))
    sig_decimals = max(sig_figs - 1 - exp, 0)
    d = min(sig_decimals, decimals_cap)
    q = Decimal(1).scaleb(-d)
    return float(Decimal(repr(px)).quantize(q, rounding=ROUND_HALF_EVEN))


def round_sz(sz: float, sz_decimals: int) -> float:
    q = Decimal(1).scaleb(-sz_decimals)
    return float(Decimal(repr(sz)).quantize(q, rounding=ROUND_DOWN))


def px_tick(px: float, sz_decimals: int) -> float:
    exp = math.floor(math.log10(px)) if px > 0 else 0
    return max(10.0 ** (exp - 4), 10.0 ** (-(6 - sz_decimals)))
