from __future__ import annotations

import math
from collections import deque


class TimeWindowSum:
    """Sum of values over a trailing time window (ns timestamps)."""

    __slots__ = ("window_ns", "_q", "total")

    def __init__(self, window_s: float) -> None:
        self.window_ns = int(window_s * 1e9)
        self._q: deque[tuple[int, float]] = deque()
        self.total = 0.0

    def add(self, ts_ns: int, v: float) -> None:
        self._q.append((ts_ns, v))
        self.total += v
        self._evict(ts_ns)

    def value(self, now_ns: int) -> float:
        self._evict(now_ns)
        return self.total

    def _evict(self, now_ns: int) -> None:
        cutoff = now_ns - self.window_ns
        q = self._q
        while q and q[0][0] < cutoff:
            _, v = q.popleft()
            self.total -= v
        if not q:
            self.total = 0.0


class RollingStats:
    """Time-windowed mean/std for z-scoring, sampled at most every `min_dt_s`."""

    __slots__ = ("window_ns", "min_dt_ns", "_q", "_s", "_s2", "_last_ns")

    def __init__(self, window_s: float, min_dt_s: float = 0.2) -> None:
        self.window_ns = int(window_s * 1e9)
        self.min_dt_ns = int(min_dt_s * 1e9)
        self._q: deque[tuple[int, float]] = deque()
        self._s = 0.0
        self._s2 = 0.0
        self._last_ns = 0

    def add(self, ts_ns: int, v: float) -> None:
        if ts_ns - self._last_ns < self.min_dt_ns:
            return
        self._last_ns = ts_ns
        self._q.append((ts_ns, v))
        self._s += v
        self._s2 += v * v
        cutoff = ts_ns - self.window_ns
        q = self._q
        while q and q[0][0] < cutoff:
            _, o = q.popleft()
            self._s -= o
            self._s2 -= o * o

    @property
    def n(self) -> int:
        return len(self._q)

    @property
    def mean(self) -> float:
        return self._s / len(self._q) if self._q else 0.0

    @property
    def std(self) -> float:
        n = len(self._q)
        if n < 2:
            return 0.0
        var = self._s2 / n - (self._s / n) ** 2
        return math.sqrt(var) if var > 0 else 0.0

    def z(self, v: float, min_n: int = 30, floor_std: float = 1e-9) -> float:
        if self.n < min_n:
            return 0.0
        sd = self.std
        if sd < floor_std:
            return 0.0
        return (v - self.mean) / sd
