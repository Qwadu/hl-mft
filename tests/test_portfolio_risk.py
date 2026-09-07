from hl_mft.config import RiskConfig
from hl_mft.execution.rounding import round_px, round_sz
from hl_mft.portfolio import Portfolio
from hl_mft.risk import RiskManager


def test_position_pnl_and_flip() -> None:
    pf = Portfolio(equity_start=1000.0)
    pf.on_fill("X", 100.0, 2.0, 0.02, True)
    pf.mark("X", 110.0)
    assert abs(pf.unrealized - 20.0) < 1e-9
    assert abs(pf.equity - (1000.0 + 20.0 - 0.02)) < 1e-9
    # partial close
    pf.on_fill("X", 110.0, -1.0, 0.0, True)
    assert abs(pf.realized_total - 10.0) < 1e-9 and pf.trades_closed == 0
    # close and flip short 1
    pf.on_fill("X", 105.0, -2.0, 0.0, False)
    p = pf.pos("X")
    assert p.size == -1.0 and p.entry_px == 105.0
    assert abs(pf.realized_total - 15.0) < 1e-9
    # close short at 100 -> +5 ; counts as a closed winning trade
    pf.on_fill("X", 100.0, 1.0, 0.0, False)
    assert pf.pos("X").size == 0.0 and pf.trades_closed == 1 and pf.wins == 1
    assert abs(pf.realized_total - 20.0) < 1e-9


def test_sync_equity() -> None:
    pf = Portfolio(equity_start=1000.0)
    pf.on_fill("X", 100.0, 1.0, 0.1, True)
    pf.sync_equity(990.0)
    assert abs(pf.equity - 990.0) < 1e-9


def test_kill_switches() -> None:
    pf = Portfolio(equity_start=1000.0)
    pf.roll_day(0.0)
    pf.peak_equity = pf.equity
    rm = RiskManager(RiskConfig(daily_loss_stop_pct=10, max_drawdown_stop_pct=25), pf)
    pf.on_fill("X", 100.0, 10.0, 0.0, True)
    pf.mark("X", 90.0)  # -100 = -10%
    rm.check_limits()
    assert rm.killed and "daily loss" in rm.kill_reason
    assert rm.can_open("Y") == (False, "killed")
    rm.reset()
    assert not rm.killed
    # drawdown check independent of the daily stop
    pf2 = Portfolio(equity_start=1000.0)
    pf2.roll_day(0.0)
    pf2.peak_equity = 1000.0
    rm2 = RiskManager(RiskConfig(daily_loss_stop_pct=50, max_drawdown_stop_pct=25), pf2)
    pf2.on_fill("X", 100.0, 10.0, 0.0, True)
    pf2.mark("X", 74.0)  # -260 -> dd 26%
    rm2.check_limits()
    assert rm2.killed and "drawdown" in rm2.kill_reason


def test_sizing_and_gross_cap() -> None:
    pf = Portfolio(equity_start=500.0)
    cfg = RiskConfig(
        risk_per_trade_pct=2.0,
        max_notional_per_position_usd=50.0,
        min_notional_usd=10.5,
        leverage=5,
        max_gross_leverage=8.0,
        max_positions=2,
    )
    rm = RiskManager(cfg, pf)
    # 2% of 500 = $10 risk; stop 10bps -> $10k notional, capped at $50
    assert rm.size_notional(100.0, 10.0) == 50.0
    # huge stop -> below min notional -> 0
    assert rm.size_notional(100.0, 20000.0) == 0.0
    pf.on_fill("A", 1.0, 10.0, 0.0, True)
    pf.on_fill("B", 1.0, 10.0, 0.0, True)
    assert rm.can_open("C") == (False, "max_positions")


def test_rounding_rules() -> None:
    assert round_px(12345.678, 5) == 12346.0  # 5 sig figs
    assert round_px(1.234567, 2) == 1.2346  # 6-2=4 decimals cap
    assert round_px(0.0012345678, 0) == 0.001235  # 6 decimals cap beats sig figs
    assert round_sz(1.239, 2) == 1.23  # size rounds down
