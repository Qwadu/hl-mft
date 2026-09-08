from pathlib import Path

import pyarrow.parquet as pq
import pytest

from hl_mft.app import TESTNET_INFO_URL, TESTNET_WS_URL, App
from hl_mft.bus import EventBus
from hl_mft.config import AppConfig, RecorderConfig, RiskConfig, Secrets
from hl_mft.portfolio import Portfolio
from hl_mft.recorder.parquet import ParquetRecorder
from hl_mft.risk import RiskManager


def _row(i: int) -> dict[str, object]:
    return {"coin": "A", "ts_ms": i, "recv_ns": i, "bid_px": 1.0, "bid_sz": 1.0, "ask_px": 2.0, "ask_sz": 1.0}


def test_recorder_keeps_rows_when_write_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rec = ParquetRecorder(RecorderConfig(root=tmp_path, flush_rows=3, max_pending_batches=2), EventBus())
    t = rec.tables["bbo"]

    def fail(*a: object, **k: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("hl_mft.recorder.parquet.pq.write_table", fail)
    for i in range(3):
        rec._add("bbo", _row(i))  # third row triggers a flush that fails
    assert [r["ts_ms"] for r in t.rows] == [0, 1, 2] and t.written == 0
    rec._add("bbo", _row(3))
    rec._add("bbo", _row(4))
    rec._add("bbo", _row(5))  # 6 rows == cap (3 * 2): nothing dropped yet
    assert len(t.rows) == 6 and t.dropped == 0
    rec._add("bbo", _row(6))  # over cap: the oldest row goes
    assert t.dropped == 1 and [r["ts_ms"] for r in t.rows][:2] == [1, 2]

    monkeypatch.undo()
    rec._flush_table(t)
    assert t.rows == [] and t.written == 6
    files = list((tmp_path / "bbo").rglob("*.parquet"))
    assert len(files) == 1 and pq.read_table(files[0]).num_rows == 6


def test_pending_entries_reserve_slots_and_gross() -> None:
    pf = Portfolio(equity_start=500.0)
    rm = RiskManager(
        RiskConfig(max_positions=2, max_gross_leverage=1.0, max_notional_per_position_usd=200.0), pf
    )
    rm.reserve("A", 200.0)
    assert rm.can_open("B") == (True, "")
    rm.reserve("B", 200.0)
    assert rm.can_open("C") == (False, "max_positions")
    assert rm.can_open("B") == (True, "")  # re-checking the coin that holds the reservation is fine
    rm.release("B")
    rm.cfg.max_positions = 3
    pf.on_fill("B", 2.0, 100.0, 0.0, True)  # $200 held + $200 pending + $200 new > $500 gross cap
    assert rm.can_open("C") == (False, "gross_leverage")
    rm.release("A")
    assert rm.can_open("C") == (True, "")


def test_testnet_flag_moves_every_endpoint() -> None:
    app = App(AppConfig(), Secrets(hl_testnet=True))
    assert app.cfg.feeds.hl_ws_url == TESTNET_WS_URL
    assert app.cfg.feeds.hl_info_url == TESTNET_INFO_URL
    assert app.info.url == TESTNET_INFO_URL


def test_live_refuses_testnet_urls_without_flag() -> None:
    cfg = AppConfig(mode="live")
    cfg.feeds.hl_ws_url = TESTNET_WS_URL
    with pytest.raises(SystemExit):
        App(cfg, Secrets(hl_testnet=False))
