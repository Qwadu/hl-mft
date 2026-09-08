from hl_mft.book.l2 import OrderBook
from hl_mft.events import Bbo, L2Update, Level
from hl_mft.features.rolling import RollingStats, TimeWindowSum


def mk_book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> OrderBook:
    b = OrderBook("X")
    b.apply(L2Update("X", 0, 1, [Level(px, sz, 1) for px, sz in bids], [Level(px, sz, 1) for px, sz in asks]))
    return b


def test_book_basics() -> None:
    b = mk_book([(99.0, 2.0), (98.0, 1.0)], [(101.0, 1.0), (102.0, 3.0)])
    assert b.ready
    assert b.mid == 100.0
    assert abs(b.spread_bps - 200.0) < 1e-9
    # microprice tilts toward the ask when bid size dominates
    assert b.microprice() > b.mid
    assert abs(b.imbalance(1) - (2.0 - 1.0) / 3.0) < 1e-12
    assert abs(b.imbalance(2) - (3.0 - 4.0) / 7.0) < 1e-12


def test_apply_bbo_merges_top() -> None:
    b = mk_book([(99.0, 2.0), (98.0, 1.0), (97.0, 1.0)], [(101.0, 1.0), (102.0, 3.0)])
    # bid improves: new level inserted in front
    b.apply_bbo(Bbo("X", 0, 2, 99.5, 0.5, 101.0, 4.0))
    assert [lv.px for lv in b.bids] == [99.5, 99.0, 98.0, 97.0]
    assert b.asks[0].sz == 4.0 and len(b.asks) == 2  # same px -> size replaced
    # bid worsens to 98: 99.5 and 99 are gone, 98 replaced by the bbo size
    b.apply_bbo(Bbo("X", 0, 3, 98.0, 7.0, 103.0, 1.0))
    assert [(lv.px, lv.sz) for lv in b.bids] == [(98.0, 7.0), (97.0, 1.0)]
    assert [lv.px for lv in b.asks] == [103.0]


def test_time_window_sum_evicts() -> None:
    w = TimeWindowSum(1.0)
    w.add(0, 1.0)
    w.add(int(0.5e9), 2.0)
    assert w.value(int(0.9e9)) == 3.0
    assert w.value(int(1.2e9)) == 2.0
    assert w.value(int(3e9)) == 0.0


def test_rolling_z() -> None:
    r = RollingStats(window_s=100.0, min_dt_s=0.0)
    for i in range(1, 41):
        r.add(i * 10**9, float(i % 2))  # alternating 0/1 -> mean .5, std .5
    assert r.n == 40
    assert abs(r.mean - 0.5) < 1e-12
    assert abs(r.z(1.0) - 1.0) < 1e-9
    assert r.z(1.0, min_n=100) == 0.0


def test_rolling_z_after_gap_longer_than_window() -> None:
    r = RollingStats(window_s=100.0, min_dt_s=0.0)
    for i in range(1, 41):
        r.add(i * 10**9, float(i % 2))
    assert r.z(1.0) != 0.0
    # feed outage: next observation arrives long after every sample has expired
    r.evict(1000 * 10**9)
    assert r.n == 0 and r.mean == 0.0 and r.std == 0.0
    assert r.z(1.0) == 0.0  # no baseline -> neutral, cannot trigger an entry
