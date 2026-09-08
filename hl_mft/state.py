from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .logging_setup import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class RiskState:
    day_key: str = ""
    day_start_equity: float = 0.0
    peak_equity: float = 0.0
    killed: bool = False
    kill_reason: str = ""


class StateStore:
    """Durable risk baselines (daily anchor, high-water mark, kill switch) so a restart cannot reset them.

    One row per `namespace` (account + network), so paper/testnet/mainnet state never mixes.
    """

    def __init__(self, path: Path, namespace: str) -> None:
        self.path = path
        self.ns = namespace
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path, isolation_level=None)  # autocommit
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS risk_state ("
            "ns TEXT PRIMARY KEY, day_key TEXT, day_start_equity REAL, peak_equity REAL, "
            "killed INTEGER, kill_reason TEXT, updated REAL)"
        )

    def load(self) -> RiskState | None:
        row = self._db.execute(
            "SELECT day_key, day_start_equity, peak_equity, killed, kill_reason FROM risk_state WHERE ns=?",
            (self.ns,),
        ).fetchone()
        if row is None:
            return None
        return RiskState(str(row[0]), float(row[1]), float(row[2]), bool(row[3]), str(row[4]))

    def save(self, s: RiskState) -> None:
        self._db.execute(
            "INSERT INTO risk_state "
            "(ns, day_key, day_start_equity, peak_equity, killed, kill_reason, updated) "
            "VALUES (?,?,?,?,?,?,strftime('%s','now')) ON CONFLICT(ns) DO UPDATE SET "
            "day_key=excluded.day_key, day_start_equity=excluded.day_start_equity, "
            "peak_equity=excluded.peak_equity, killed=excluded.killed, kill_reason=excluded.kill_reason, "
            "updated=excluded.updated",
            (self.ns, s.day_key, s.day_start_equity, s.peak_equity, int(s.killed), s.kill_reason),
        )

    def close(self) -> None:
        self._db.close()
