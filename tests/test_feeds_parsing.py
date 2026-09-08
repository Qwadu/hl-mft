import orjson

from hl_mft.bus import EventBus
from hl_mft.events import L2Update, RefTick, Trade
from hl_mft.feeds.binance_ws import BinanceFeed
from hl_mft.feeds.hyperliquid_ws import HyperliquidFeed
from hl_mft.feeds.lse_ws import LSEFeed
from hl_mft.feeds.symbols import hl_to_binance, hl_to_lse


def test_symbol_mapping() -> None:
    assert hl_to_binance("BTC") == "BTCUSDT"
    assert hl_to_binance("kPEPE") == "1000PEPEUSDT"
    assert hl_to_binance("xyz:GOLD") is None
    assert hl_to_lse("ZEC") == "ZEC/USD"


async def test_hl_l2_and_trades() -> None:
    bus = EventBus()
    feed = HyperliquidFeed("ws://x", bus, ["ZEC"])
    out: list[object] = []
    bus.subscribe(L2Update, out.append)
    bus.subscribe(Trade, out.append)
    await feed.on_message(
        orjson.dumps(
            {
                "channel": "l2Book",
                "data": {
                    "coin": "ZEC",
                    "time": 1700000000000,
                    "levels": [
                        [{"px": "40.1", "sz": "10", "n": 2}, {"px": "40.0", "sz": "5", "n": 1}],
                        [{"px": "40.2", "sz": "3", "n": 1}],
                    ],
                },
            }
        )
    )
    await feed.on_message(
        orjson.dumps(
            {
                "channel": "trades",
                "data": [
                    {
                        "coin": "ZEC",
                        "side": "A",
                        "px": "40.1",
                        "sz": "1.5",
                        "time": 1,
                        "tid": 7,
                        "users": ["a", "b"],
                    }
                ],
            }
        )
    )
    l2, tr = out
    assert isinstance(l2, L2Update) and l2.bids[0].px == 40.1 and len(l2.asks) == 1
    assert isinstance(tr, Trade) and not tr.is_buy and tr.sz == 1.5 and tr.buyer == "a"


async def test_hl_user_channel_dispatch() -> None:
    bus = EventBus()
    feed = HyperliquidFeed("ws://x", bus, ["ZEC"], user="0xabc")
    seen: list[tuple[str, object]] = []

    async def h(ch: str, data: object) -> None:
        seen.append((ch, data))

    feed.user_handlers.append(h)
    await feed.on_message(orjson.dumps({"channel": "userFills", "data": {"isSnapshot": True, "fills": []}}))
    assert seen == [("userFills", {"isSnapshot": True, "fills": []})]


async def test_binance_book_ticker() -> None:
    bus = EventBus()
    feed = BinanceFeed("ws://x", bus, ["BTC", "kPEPE"], 10.0)
    out: list[RefTick] = []
    bus.subscribe(RefTick, out.append)
    await feed.on_message(
        orjson.dumps(
            {
                "stream": "1000pepeusdt@bookTicker",
                "data": {
                    "e": "bookTicker",
                    "s": "1000PEPEUSDT",
                    "b": "0.0100",
                    "a": "0.0101",
                    "E": 5,
                    "T": 4,
                },
            }
        )
    )
    assert len(out) == 1 and out[0].coin == "kPEPE" and out[0].source == "binance" and out[0].ask == 0.0101


async def test_lse_tick() -> None:
    bus = EventBus()
    feed = LSEFeed("ws://x", "k", bus, ["ZEC"])
    out: list[RefTick] = []
    bus.subscribe(RefTick, out.append)
    await feed.on_message(
        orjson.dumps(
            {
                "type": "tick",
                "symbol": "ZEC/USD",
                "ts": "2026-01-01T00:00:00Z",
                "price": 40.0,
                "bid": 39.9,
                "ask": 40.1,
                "volume": 1,
            }
        )
    )
    assert len(out) == 1 and out[0].coin == "ZEC" and out[0].bid == 39.9 and out[0].source == "lse"
