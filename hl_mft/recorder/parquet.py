from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..bus import EventBus
from ..config import RecorderConfig
from ..events import AssetCtx, Bbo, FeatureVector, L2Update, RefTick, Trade
from ..logging_setup import get_logger

log = get_logger(__name__)

L2_DEPTH = 20


def _l2_schema() -> pa.Schema:
    fields = [("coin", pa.string()), ("ts_ms", pa.int64()), ("recv_ns", pa.int64())]
    for side in ("bid", "ask"):
        for i in range(L2_DEPTH):
            fields += [
                (f"{side}_px_{i}", pa.float64()),
                (f"{side}_sz_{i}", pa.float64()),
                (f"{side}_n_{i}", pa.int32()),
            ]
    return pa.schema(fields)


SCHEMAS: dict[str, pa.Schema] = {
    "l2": _l2_schema(),
    "trades": pa.schema(
        [
            ("coin", pa.string()),
            ("ts_ms", pa.int64()),
            ("recv_ns", pa.int64()),
            ("px", pa.float64()),
            ("sz", pa.float64()),
            ("is_buy", pa.bool_()),
            ("tid", pa.int64()),
            ("buyer", pa.string()),
            ("seller", pa.string()),
        ]
    ),
    "bbo": pa.schema(
        [
            ("coin", pa.string()),
            ("ts_ms", pa.int64()),
            ("recv_ns", pa.int64()),
            ("bid_px", pa.float64()),
            ("bid_sz", pa.float64()),
            ("ask_px", pa.float64()),
            ("ask_sz", pa.float64()),
        ]
    ),
    "ref": pa.schema(
        [
            ("source", pa.string()),
            ("coin", pa.string()),
            ("ts_ms", pa.int64()),
            ("recv_ns", pa.int64()),
            ("bid", pa.float64()),
            ("ask", pa.float64()),
        ]
    ),
    "ctx": pa.schema(
        [
            ("coin", pa.string()),
            ("recv_ns", pa.int64()),
            ("funding", pa.float64()),
            ("open_interest", pa.float64()),
            ("day_ntl_vlm", pa.float64()),
            ("mark_px", pa.float64()),
            ("oracle_px", pa.float64()),
            ("mid_px", pa.float64()),
            ("premium", pa.float64()),
        ]
    ),
}


class _Table:
    def __init__(self, name: str, schema: pa.Schema | None) -> None:
        self.name = name
        self.schema = schema
        self.rows: list[dict[str, Any]] = []
        self.written = 0


class ParquetRecorder:
    """Buffers events per table and flushes to hour-partitioned Parquet files (zstd)."""

    def __init__(self, cfg: RecorderConfig, bus: EventBus) -> None:
        self.cfg = cfg
        self.root = Path(cfg.root)
        self.tables: dict[str, _Table] = {n: _Table(n, s) for n, s in SCHEMAS.items()}
        self.tables["features"] = _Table("features", None)  # dynamic columns
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        if cfg.record_l2:
            bus.subscribe(L2Update, self._on_l2)
            bus.subscribe(Bbo, self._on_bbo)
        if cfg.record_trades:
            bus.subscribe(Trade, self._on_trade)
        if cfg.record_ref:
            bus.subscribe(RefTick, self._on_ref)
        bus.subscribe(AssetCtx, self._on_ctx)
        if cfg.record_features:
            bus.subscribe(FeatureVector, self._on_feature)

    # -- handlers ------------------------------------------------------------
    def _on_l2(self, e: L2Update) -> None:
        row: dict[str, Any] = {"coin": e.coin, "ts_ms": e.ts_ms, "recv_ns": e.recv_ns}
        for side, levels in (("bid", e.bids), ("ask", e.asks)):
            for i in range(L2_DEPTH):
                if i < len(levels):
                    lv = levels[i]
                    row[f"{side}_px_{i}"], row[f"{side}_sz_{i}"], row[f"{side}_n_{i}"] = lv.px, lv.sz, lv.n
                else:
                    row[f"{side}_px_{i}"], row[f"{side}_sz_{i}"], row[f"{side}_n_{i}"] = None, None, None
        self._add("l2", row)

    def _on_bbo(self, e: Bbo) -> None:
        self._add(
            "bbo",
            {
                "coin": e.coin,
                "ts_ms": e.ts_ms,
                "recv_ns": e.recv_ns,
                "bid_px": e.bid_px,
                "bid_sz": e.bid_sz,
                "ask_px": e.ask_px,
                "ask_sz": e.ask_sz,
            },
        )

    def _on_trade(self, e: Trade) -> None:
        self._add(
            "trades",
            {
                "coin": e.coin,
                "ts_ms": e.ts_ms,
                "recv_ns": e.recv_ns,
                "px": e.px,
                "sz": e.sz,
                "is_buy": e.is_buy,
                "tid": e.tid,
                "buyer": e.buyer,
                "seller": e.seller,
            },
        )

    def _on_ref(self, e: RefTick) -> None:
        self._add(
            "ref",
            {
                "source": e.source,
                "coin": e.coin,
                "ts_ms": e.ts_ms,
                "recv_ns": e.recv_ns,
                "bid": e.bid,
                "ask": e.ask,
            },
        )

    def _on_ctx(self, e: AssetCtx) -> None:
        self._add(
            "ctx",
            {
                "coin": e.coin,
                "recv_ns": e.recv_ns,
                "funding": e.funding,
                "open_interest": e.open_interest,
                "day_ntl_vlm": e.day_ntl_vlm,
                "mark_px": e.mark_px,
                "oracle_px": e.oracle_px,
                "mid_px": e.mid_px,
                "premium": e.premium,
            },
        )

    def _on_feature(self, e: FeatureVector) -> None:
        row: dict[str, Any] = {
            "coin": e.coin,
            "recv_ns": e.recv_ns,
            "mid": e.mid,
            "spread_bps": e.spread_bps,
            "score": e.score,
        }
        for k, v in e.values.items():
            row[f"f_{k}"] = v
        for k, v in e.zscores.items():
            row[f"z_{k}"] = v
        self._add("features", row)

    def _add(self, table: str, row: dict[str, Any]) -> None:
        t = self.tables[table]
        t.rows.append(row)
        if len(t.rows) >= self.cfg.flush_rows:
            self._flush_table(t)

    # -- flushing ------------------------------------------------------------
    def _flush_table(self, t: _Table) -> None:
        if not t.rows:
            return
        rows, t.rows = t.rows, []
        now = datetime.now(UTC)
        d = self.root / t.name / f"date={now:%Y-%m-%d}" / f"hour={now:%H}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{int(time.time() * 1000)}.parquet"
        try:
            tbl = pa.Table.from_pylist(rows, schema=t.schema) if t.schema else pa.Table.from_pylist(rows)
            pq.write_table(tbl, path, compression="zstd")
            t.written += len(rows)
        except Exception:  # noqa: BLE001
            log.exception("parquet_flush_failed", table=t.name, rows=len(rows))

    def flush_all(self) -> None:
        for t in self.tables.values():
            self._flush_table(t)

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.flush_interval_s)
            except TimeoutError:
                pass
            await asyncio.to_thread(self.flush_all)

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="recorder")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
        self.flush_all()

    def stats(self) -> dict[str, int]:
        return {n: t.written for n, t in self.tables.items()}
