from __future__ import annotations

import time

_sim_ns: int | None = None


def now_ns() -> int:
    return _sim_ns if _sim_ns is not None else time.time_ns()


def now_s() -> float:
    return now_ns() / 1e9


def set_sim_time(ns: int | None) -> None:
    global _sim_ns
    _sim_ns = ns
