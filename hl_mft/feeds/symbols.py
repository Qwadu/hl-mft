from __future__ import annotations

# Hyperliquid coin -> Binance USDT-perp symbol. "k"-prefixed HL coins are 1000x units, matching
# Binance's "1000XXX" contracts, so the price scale is identical.
_BINANCE_OVERRIDES: dict[str, str] = {
    "kPEPE": "1000PEPEUSDT",
    "kBONK": "1000BONKUSDT",
    "kSHIB": "1000SHIBUSDT",
    "kFLOKI": "1000FLOKIUSDT",
    "kLUNC": "1000LUNCUSDT",
    "kNEIRO": "1000NEIROUSDT",
    "kDOGS": "1000DOGSUSDT",
}

_LSE_OVERRIDES: dict[str, str] = {}


def hl_to_binance(coin: str) -> str | None:
    if coin in _BINANCE_OVERRIDES:
        return _BINANCE_OVERRIDES[coin]
    if ":" in coin or coin.startswith("@") or coin.startswith("k"):
        return None
    return f"{coin}USDT"


def hl_to_lse(coin: str) -> str | None:
    if coin in _LSE_OVERRIDES:
        return _LSE_OVERRIDES[coin]
    if ":" in coin or coin.startswith("@") or coin.startswith("k"):
        return None
    return f"{coin}/USD"
