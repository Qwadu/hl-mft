from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .config import AppConfig, Secrets
from .logging_setup import setup_logging


def main() -> None:
    ap = argparse.ArgumentParser(prog="hl-mft", description="Hyperliquid mid-frequency flow-following bot")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run the bot (record / paper / live)")
    run.add_argument("-c", "--config", type=Path, default=Path("config.yaml"))
    run.add_argument("--mode", choices=["record", "paper", "live"], default=None)
    run.add_argument("--log-level", default="INFO")
    run.add_argument("--text-logs", action="store_true")

    sub.add_parser("dump-config", help="print default config as YAML")

    uni = sub.add_parser("universe", help="print the currently selected universe")
    uni.add_argument("-c", "--config", type=Path, default=Path("config.yaml"))

    bt = sub.add_parser("backtest", help="replay recorded parquet through features+strategy")
    bt.add_argument("-c", "--config", type=Path, default=Path("config.yaml"))
    bt.add_argument("--data", type=Path, default=Path("data"))
    bt.add_argument("--date", required=True, help="YYYY-MM-DD (UTC) partition to replay")
    bt.add_argument("--hours", default=None, help="comma-separated hours e.g. 10,11,12")
    bt.add_argument("--coins", default=None, help="comma-separated coin filter")
    bt.add_argument("--report", type=Path, default=None, help="write trades CSV here")
    bt.add_argument(
        "--set", action="append", default=[], help="override strategy param, e.g. theta_enter=2.0"
    )

    args = ap.parse_args()
    if args.cmd == "dump-config":
        import yaml

        print(yaml.safe_dump(AppConfig().model_dump(mode="json"), sort_keys=False))
        return
    if args.cmd == "universe":
        from .feeds.hl_info import HLInfo
        from .universe import fetch_universe

        cfg = AppConfig.load(args.config)

        async def _u() -> None:
            info = HLInfo(cfg.feeds.hl_info_url)
            coins, metas = await fetch_universe(info, cfg.universe)
            for c in coins:
                m = metas[c]
                oi_m = m.open_interest * m.mark_px / 1e6
                print(f"{c:10s} vol24h=${m.day_ntl_vlm / 1e6:9.1f}M  OI={oi_m:8.1f}M  px={m.mark_px}")
            await info.close()

        asyncio.run(_u())
        return
    if args.cmd == "backtest":
        from .backtest.replay import run_backtest

        cfg = AppConfig.load(args.config)
        for kv in args.set:
            k, v = kv.split("=", 1)
            setattr(cfg.strategy, k, type(getattr(cfg.strategy, k))(v))
        setup_logging("WARNING", json_logs=False)
        hours = [int(h) for h in args.hours.split(",")] if args.hours else None
        coins = args.coins.split(",") if args.coins else None
        asyncio.run(run_backtest(cfg, args.data, args.date, hours, coins, args.report))
        return

    cfg = AppConfig.load(args.config)
    if args.mode:
        cfg.mode = args.mode
    setup_logging(args.log_level, json_logs=not args.text_logs)
    secrets = Secrets()
    from .app import App

    asyncio.run(App(cfg, secrets, args.config).run())


if __name__ == "__main__":
    main()
