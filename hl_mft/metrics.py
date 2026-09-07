from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

NS = "hlmft"

feed_messages = Counter(f"{NS}_feed_messages_total", "WS messages received", ["feed"])
feed_connected = Gauge(f"{NS}_feed_connected", "1 if feed WS connected", ["feed"])
feed_reconnects = Counter(f"{NS}_feed_reconnects_total", "WS reconnects", ["feed"])
feed_stale = Gauge(f"{NS}_feed_stale", "1 if feed has not delivered for stale_after_s", ["feed"])

l2_updates = Counter(f"{NS}_l2_updates_total", "L2 snapshots", ["coin"])
trades_seen = Counter(f"{NS}_trades_total", "Trades seen", ["coin", "side"])
ref_ticks = Counter(f"{NS}_ref_ticks_total", "Reference ticks", ["source", "coin"])
recorder_rows = Gauge(f"{NS}_recorder_rows_written", "Rows written to Parquet", ["table"])

mid_px = Gauge(f"{NS}_mid_px", "HL mid price", ["coin"])
spread_bps = Gauge(f"{NS}_spread_bps", "HL spread in bps", ["coin"])
feature_gauge = Gauge(f"{NS}_feature", "Latest feature value", ["coin", "name"])
zscore_gauge = Gauge(f"{NS}_feature_z", "Latest feature z-score", ["coin", "name"])
score_gauge = Gauge(f"{NS}_score", "Strategy score", ["coin"])
leadlag_bps = Gauge(f"{NS}_leadlag_bps", "(ref mid - hl mid)/hl mid in bps", ["coin", "source"])

universe_size = Gauge(f"{NS}_universe_size", "Coins in active universe")
event_loop_lag = Histogram(
    f"{NS}_event_loop_lag_seconds", "asyncio loop lag", buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1)
)

# trading
position_size = Gauge(f"{NS}_position_size", "Signed position size", ["coin"])
position_notional = Gauge(f"{NS}_position_notional_usd", "Abs position notional", ["coin"])
unrealized_pnl = Gauge(f"{NS}_unrealized_pnl_usd", "Unrealized PnL", ["coin"])
realized_pnl_total = Gauge(f"{NS}_realized_pnl_usd_total", "Realized PnL since start (incl fees)")
daily_pnl = Gauge(f"{NS}_daily_pnl_usd", "Realized+unrealized PnL today")
account_value = Gauge(f"{NS}_account_value_usd", "Account value")
orders_sent = Counter(f"{NS}_orders_sent_total", "Orders sent", ["coin", "kind"])
orders_filled = Counter(f"{NS}_orders_filled_total", "Fills", ["coin", "maker"])
orders_rejected = Counter(f"{NS}_orders_rejected_total", "Rejected orders", ["coin", "reason"])
actions_budget_left = Gauge(f"{NS}_actions_budget_left", "Remaining action budget this minute")
kill_switch = Gauge(f"{NS}_kill_switch", "1 if kill switch tripped")
paper_trades = Counter(f"{NS}_paper_trades_total", "Paper trades", ["coin", "side"])
paper_pnl = Gauge(f"{NS}_paper_pnl_usd_total", "Paper realized PnL after fees")


def serve(port: int) -> None:
    start_http_server(port)
