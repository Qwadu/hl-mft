from __future__ import annotations

import csv
import heapq
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .. import clock
from ..bus import EventBus
from ..config import AppConfig
from ..events import Bbo, Fill, L2Update, Level, RefTick, Trade
from ..execution.paper import PaperBroker
from ..features.engine import FeatureEngine
from ..feeds.hl_info import HLInfo, PerpMeta
from ..portfolio import Portfolio
from ..recorder.parquet import L2_DEPTH
from ..risk import RiskManager
from ..strategy.flow import FlowStrategy


def _files(root: Path, table: str, date: str, hours: list[int] | None) -> list[Path]:
    d = root / table / f"date={date}"
    if not d.exists():
        return []
    out: list[Path] = []
    for hd in sorted(d.iterdir()):
        h = int(hd.name.split("=")[1])
        if hours is not None and h not in hours:
            continue
        out += sorted(hd.glob("*.parquet"))
    return out


def _iter_l2(files: list[Path], coins: set[str] | None) -> Iterator[tuple[int, int, Any]]:
    for f in files:
        t = pq.read_table(f)
        if coins:
            t = t.filter(pc.is_in(t["coin"], value_set=pa.array(sorted(coins))))
        rows = t.to_pylist()
        rows.sort(key=lambda r: r["recv_ns"])
        for r in rows:
            bids = [
                Level(r[f"bid_px_{i}"], r[f"bid_sz_{i}"], r[f"bid_n_{i}"] or 0)
                for i in range(L2_DEPTH)
                if r[f"bid_px_{i}"] is not None
            ]
            asks = [
                Level(r[f"ask_px_{i}"], r[f"ask_sz_{i}"], r[f"ask_n_{i}"] or 0)
                for i in range(L2_DEPTH)
                if r[f"ask_px_{i}"] is not None
            ]
            yield r["recv_ns"], 0, L2Update(r["coin"], r["ts_ms"], r["recv_ns"], bids, asks)


def _iter_simple(files: list[Path], coins: set[str] | None, kind: str) -> Iterator[tuple[int, int, Any]]:
    for f in files:
        rows = pq.read_table(f).to_pylist()
        rows.sort(key=lambda r: r["recv_ns"])
        for r in rows:
            if coins and r["coin"] not in coins:
                continue
            if kind == "trades":
                # trades printed before the book snapshot that reflects them: order trades first on ties
                yield (
                    r["recv_ns"],
                    -1,
                    Trade(
                        r["coin"],
                        r["ts_ms"],
                        r["recv_ns"],
                        r["px"],
                        r["sz"],
                        r["is_buy"],
                        r["tid"],
                        r["buyer"],
                        r["seller"],
                    ),
                )
            elif kind == "bbo":
                yield (
                    r["recv_ns"],
                    0,
                    Bbo(
                        r["coin"],
                        r["ts_ms"],
                        r["recv_ns"],
                        r["bid_px"],
                        r["bid_sz"],
                        r["ask_px"],
                        r["ask_sz"],
                    ),
                )
            else:
                yield (
                    r["recv_ns"],
                    1,
                    RefTick(r["source"], r["coin"], r["recv_ns"], r["bid"], r["ask"], r["ts_ms"]),
                )


def merged(root: Path, date: str, hours: list[int] | None, coins: set[str] | None) -> Iterator[Any]:
    its = [
        _iter_l2(_files(root, "l2", date, hours), coins),
        _iter_simple(_files(root, "trades", date, hours), coins, "trades"),
        _iter_simple(_files(root, "bbo", date, hours), coins, "bbo"),
        _iter_simple(_files(root, "ref", date, hours), coins, "ref"),
    ]
    for _, _, ev in heapq.merge(*its, key=lambda x: (x[0], x[1])):
        yield ev


async def run_backtest(
    cfg: AppConfig,
    data: Path,
    date: str,
    hours: list[int] | None,
    coins: list[str] | None,
    report: Path | None,
) -> dict[str, Any]:
    info = HLInfo(cfg.feeds.hl_info_url)
    try:
        metas: dict[str, PerpMeta] = {m.coin: m for m in await info.meta_and_ctxs()}
    finally:
        await info.close()

    bus = EventBus()
    engine = FeatureEngine(cfg.features, bus, coins or [])
    pf = Portfolio(equity_start=cfg.paper.equity_start)
    broker = PaperBroker(cfg.paper, cfg.fees, bus)
    cfg.risk.max_actions_per_minute = 10_000
    risk = RiskManager(cfg.risk, pf)
    strat = FlowStrategy(cfg.strategy, bus, broker, pf, risk, metas)
    strat.max_events = 10_000_000
    fills: list[Fill] = []
    bus.subscribe(Fill, lambda f: fills.append(f))

    coin_set = set(coins) if coins else None
    n = 0
    t0 = time.time()
    first_ns = last_ns = 0
    for ev in merged(data, date, hours, coin_set):
        ns = ev.recv_ns
        if not first_ns:
            first_ns = ns
            pf.roll_day(ns / 1e9)
            pf.peak_equity = pf.equity
        last_ns = ns
        clock.set_sim_time(ns)
        if isinstance(ev, L2Update) and ev.coin not in engine.coins:
            engine.ensure(ev.coin)
        await bus.publish(ev)
        n += 1
    clock.set_sim_time(None)

    span_h = (last_ns - first_ns) / 3.6e12 if last_ns else 0.0
    snap = pf.snapshot()
    per_coin: dict[str, dict[str, float]] = {}
    for p in pf.positions.values():
        per_coin[p.coin] = {
            "realized": round(p.realized - p.fees, 4),
            "fees": round(p.fees, 4),
            "fills": p.n_fills,
        }
    maker_fills = sum(1 for f in fills if f.maker)
    summary = {
        "date": date,
        "hours": hours,
        "events": n,
        "span_h": round(span_h, 2),
        "wall_s": round(time.time() - t0, 1),
        "fills": len(fills),
        "maker_share": round(maker_fills / len(fills), 3) if fills else None,
        "trades_closed": pf.trades_closed,
        "win_rate": snap["win_rate"],
        "pnl_net": round(pf.realized_total - pf.fees_total, 4),
        "fees": round(pf.fees_total, 4),
        "pnl_per_trade_bps": round(
            (pf.realized_total - pf.fees_total)
            / max(pf.trades_closed, 1)
            / cfg.risk.max_notional_per_position_usd
            * 1e4,
            2,
        ),
        "max_drawdown_pct": round(pf.drawdown_pct, 3),
        "open_at_end": len(pf.open_positions()),
        "per_coin": per_coin,
    }
    for k, v in summary.items():
        if k != "per_coin":
            print(f"{k:18s} {v}")
    print("per_coin:")
    for c, d in sorted(per_coin.items(), key=lambda x: x[1]["realized"]):
        print(f"  {c:8s} {d}")
    if report:
        with report.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["recv_ns", "coin", "cid", "px", "sz", "fee", "maker", "tag"])
            for f in fills:
                w.writerow([f.recv_ns, f.coin, f.cid, f.px, f.sz, f.fee, f.maker, f.tag])
        ev_path = report.with_suffix(".events.csv")
        with ev_path.open("w", newline="") as fh:
            w = csv.writer(fh)
            keys = sorted({k for e in strat.events for k in e})
            w.writerow(keys)
            for e in strat.events:
                w.writerow([e.get(k, "") for k in keys])
        print(f"report: {report}  events: {ev_path}")
    return summary
