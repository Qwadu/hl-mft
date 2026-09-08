from __future__ import annotations

import secrets as pysecrets
from pathlib import Path
from typing import TYPE_CHECKING, Any

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ValidationError

from ..logging_setup import get_logger

if TYPE_CHECKING:
    from ..app import App

log = get_logger(__name__)
HTML = (Path(__file__).parent / "index.html").read_text()


class ParamUpdate(BaseModel):
    strategy: dict[str, Any] = {}
    risk: dict[str, Any] = {}
    persist: bool = False


def build_app(app: App) -> FastAPI:
    token = app.secrets.dashboard_token
    if token in ("", "change-me") and app.cfg.mode == "live":
        log.error("dashboard_token_default_in_live", token=token)
        token = ""

    def auth(request: Request) -> None:
        supplied = (
            request.headers.get("x-token")
            or request.query_params.get("token")
            or request.cookies.get("token")
        )
        if not supplied or not token or not pysecrets.compare_digest(supplied, token):
            raise HTTPException(status_code=401, detail="bad token")

    api = FastAPI(title="hl-mft", docs_url=None, redoc_url=None)

    @api.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> Any:
        t = request.query_params.get("token")
        resp = HTMLResponse(HTML)
        if t:
            resp.set_cookie("token", t, httponly=True, samesite="strict")
        return resp

    @api.get("/api/status", dependencies=[Depends(auth)])
    async def status() -> Any:
        return app.status()

    @api.get("/api/portfolio", dependencies=[Depends(auth)])
    async def portfolio() -> Any:
        return app.pf.snapshot() if app.pf else {}

    @api.get("/api/strategy", dependencies=[Depends(auth)])
    async def strategy() -> Any:
        return app.strategy.snapshot() if app.strategy else {}

    @api.get("/api/events", dependencies=[Depends(auth)])
    async def events(n: int = 100) -> Any:
        n = max(0, min(n, 5000))
        return app.strategy.events[-n:] if app.strategy else []

    @api.get("/api/orders", dependencies=[Depends(auth)])
    async def orders() -> Any:
        if not app.broker:
            return []
        out = []
        for c in app.coins:
            for o in app.broker.open_orders(c):
                out.append(
                    {
                        "coin": c,
                        "cid": o.req.cid,
                        "side": o.req.side,
                        "px": o.req.px,
                        "sz": o.req.sz,
                        "kind": o.req.kind,
                        "filled": o.filled_sz,
                        "tag": o.req.tag,
                        "oid": o.oid,
                    }
                )
        return out

    @api.get("/api/params", dependencies=[Depends(auth)])
    async def params() -> Any:
        return {
            "strategy": app.cfg.strategy.model_dump(),
            "risk": app.cfg.risk.model_dump(),
            "mode": app.cfg.mode,
        }

    @api.post("/api/params", dependencies=[Depends(auth)])
    async def set_params(upd: ParamUpdate) -> Any:
        unknown = [k for k in upd.strategy if k not in type(app.cfg.strategy).model_fields] + [
            k for k in upd.risk if k not in type(app.cfg.risk).model_fields
        ]
        if unknown:
            raise HTTPException(status_code=422, detail=f"unknown params: {unknown}")
        try:
            # full re-validation: Field bounds + cross-field validators apply to runtime edits too
            new_strat = type(app.cfg.strategy).model_validate(
                {**app.cfg.strategy.model_dump(), **upd.strategy}
            )
            new_risk = type(app.cfg.risk).model_validate({**app.cfg.risk.model_dump(), **upd.risk})
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e.errors(include_url=False)) from None
        app.cfg.strategy = new_strat
        app.cfg.risk = new_risk
        if app.strategy:
            app.strategy.cfg = new_strat
        if app.risk:
            app.risk.cfg = new_risk
            app.risk.budget.per_minute = new_risk.max_actions_per_minute
            app.risk.budget.reserve = new_risk.emergency_actions_reserve
        if upd.persist and app.config_path:
            app.cfg.dump(app.config_path)
        log.info("params_updated", strategy=upd.strategy, risk=upd.risk, persist=upd.persist)
        return {"ok": True}

    @api.post("/api/pause", dependencies=[Depends(auth)])
    async def pause() -> Any:
        if app.risk:
            app.risk.paused = True
        return {"ok": True}

    @api.post("/api/resume", dependencies=[Depends(auth)])
    async def resume() -> Any:
        if app.risk:
            app.risk.paused = False
        return {"ok": True}

    @api.post("/api/kill", dependencies=[Depends(auth)])
    async def kill() -> Any:
        if app.risk:
            app.risk.trip("manual")
        failed = await app.strategy.flatten_all("manual_kill") if app.strategy else []
        return {"ok": not failed, "failed": failed}

    @api.post("/api/reset-kill", dependencies=[Depends(auth)])
    async def reset_kill() -> Any:
        if app.risk:
            app.risk.reset()
        return {"ok": True}

    @api.post("/api/flatten", dependencies=[Depends(auth)])
    async def flatten() -> Any:
        failed = await app.strategy.flatten_all("manual_flatten") if app.strategy else []
        return {"ok": not failed, "failed": failed}

    @api.post("/api/strategy/{state}", dependencies=[Depends(auth)])
    async def toggle(state: str) -> Any:
        if app.strategy:
            app.strategy.enabled = state == "on"
        return {"ok": True, "enabled": app.strategy.enabled if app.strategy else None}

    @api.exception_handler(Exception)
    async def _err(_: Request, exc: Exception) -> JSONResponse:
        log.exception("dashboard_error")
        return JSONResponse(status_code=500, content={"error": repr(exc)})

    return api


async def serve_dashboard(app: App) -> None:
    api = build_app(app)
    config = uvicorn.Config(
        api, host=app.cfg.dashboard.host, port=app.cfg.dashboard.port, log_level="warning", loop="none"
    )
    server = uvicorn.Server(config)
    await server.serve()
