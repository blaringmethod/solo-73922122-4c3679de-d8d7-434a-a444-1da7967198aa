"""Web application for the institutional-repository harvester.

Pages
-----
``/``                          catalogue browser, watermark, run controls
``/runs``                      round history (windows, attempts, restarts, errors)
``/status``                    statically rendered status JSON
``GET  /api/status``           polling endpoint used by the page
``GET  /api/catalog``          catalogue JSON (search + pagination)
``POST /api/harvest``          start a round
``POST /api/harvest/cancel``   cancel the active round without waiting for it
``POST /api/harvest/restart``  cancel (if any) and immediately start a new round
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .harvest import HarvestConfig, Harvester
from .store import Store

BASE_DIR = Path(__file__).resolve().parent

# How long "取消并立即重开" waits for the previous round's finalisation to
# become durable before giving up with a 409 (the user retries shortly).
RESTART_FINALIZE_WAIT_SECONDS = float(
    os.environ.get("HARVEST_RESTART_WAIT_SECONDS", "5")
)


def _configure_logging() -> None:
    """Make the harvester's operational messages visible under uvicorn.

    Lock-contention retries and finalisation outcomes are operationally
    important; uvicorn's default logging config would drop them.
    """
    logger = logging.getLogger("harvester")
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data"))


def _oai_url() -> str:
    return os.environ.get("OAI_URL", "http://demo-oai:8000/oai")


def create_app(
    *,
    db_path: str | Path | None = None,
    oai_url: str | None = None,
    read_timeout: float | None = None,
    max_attempts: int | None = None,
    busy_timeout_ms: int | None = None,
    client_factory: Any = None,
) -> FastAPI:
    data_dir = _data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    _configure_logging()
    store = Store(
        db_path or (data_dir / "harvester.sqlite3"),
        busy_timeout_ms=int(
            busy_timeout_ms
            if busy_timeout_ms is not None
            else os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "250")
        ),
    )
    config = HarvestConfig(
        oai_url=oai_url or _oai_url(),
        read_timeout=float(
            read_timeout
            if read_timeout is not None
            else os.environ.get("OAI_READ_TIMEOUT", "20")
        ),
        connect_timeout=float(os.environ.get("OAI_CONNECT_TIMEOUT", "10")),
        max_attempts=int(
            max_attempts
            if max_attempts is not None
            else os.environ.get("HARVEST_MAX_ATTEMPTS", "3")
        ),
        retry_backoff_ms=int(os.environ.get("HARVEST_RETRY_BACKOFF_MS", "300")),
    )
    harvester = Harvester(store, config, client_factory=client_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # A fresh process owns no in-flight rounds; Store already marked any
        # 'running' rows from a previous process as failed without publishing.
        yield
        await harvester.shutdown()
        store.close()

    app = FastAPI(title="机构库采集控制台", lifespan=lifespan)
    app.state.store = store
    app.state.harvester = harvester
    app.state.config = config
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def status_payload() -> dict[str, Any]:
        active = store.get_active_run()
        if active is not None and active["status"] == "running":
            # Overlay in-memory control state the database cannot show yet:
            # a cancel that was accepted but is still waiting for the write
            # lock, or a finished round whose final state is being persisted.
            control = harvester.control(active["id"])
            if control is not None:
                if control.cancel_requested:
                    active["phase"] = "cancelling"
                    active["cancel_pending"] = True
                    active["cancel_reason"] = control.cancel_reason
                elif (
                    control.task is not None
                    and control.task.done()
                    and not control.done.is_set()
                ):
                    active["phase"] = "finalizing"
                    active["finalize_pending"] = True
        published = store.published_state()
        last = store.list_runs(limit=1)
        return {
            "active": active,
            "published": published,
            "last_run": last[0] if last else None,
            "config": {
                "oai_url": config.oai_url,
                "max_attempts": config.max_attempts,
                "read_timeout": config.read_timeout,
            },
        }

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        catalogue = store.catalog_page(q, page, page_size)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "catalogue": catalogue,
                "published": store.published_state(),
                "status": status_payload(),
                "runs": store.list_runs(limit=10),
                "message": message,
            },
        )

    @app.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": store.list_runs(limit=50),
                "published": store.published_state(),
                "status": status_payload(),
            },
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        return JSONResponse(status_payload())

    @app.get("/status")
    async def status_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "status.html", {"status": status_payload()}
        )

    @app.post("/api/harvest")
    async def start_harvest() -> JSONResponse:
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except sqlite3.OperationalError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"数据库暂时不可写（可能被其他连接锁定）: {exc}",
            ) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/cancel")
    async def cancel_harvest() -> JSONResponse:
        active = store.get_active_run()
        if active is None:
            raise HTTPException(status_code=409, detail="当前没有正在运行的采集轮次")
        outcome = await harvester.cancel_run(active["id"], "用户在网页取消", "user")
        # "cancelled"  = 取消已落库，本轮彻底结束；
        # "cancelling" = 取消已受理（在途写入与发布已被禁止），数据库正被
        #                外部写锁占用，后台收尾会在锁释放后自动落库。
        return JSONResponse({"run_id": active["id"], "status": outcome})

    @app.post("/api/harvest/restart")
    async def restart_harvest() -> JSONResponse:
        """Cancel the active round (if any) and immediately start a new one.

        With no external lock this is instant: the cancellation is durable
        by the time ``cancel_run`` returns.  While another connection holds
        the write lock, the cancel is still accepted at once and finalised
        in the background; this endpoint waits briefly for that finalisation
        (a new round cannot start while the old row is still ``running``)
        and answers 409 if the lock outlasts the wait — retry shortly, no
        process restart needed.
        """
        active = store.get_active_run()
        if active is not None:
            await harvester.cancel_run(
                active["id"], "用户取消了本轮以开始新一轮", "superseded"
            )
            control = harvester.control(active["id"])
            if control is not None:
                try:
                    await asyncio.wait_for(
                        control.done.wait(), RESTART_FINALIZE_WAIT_SECONDS
                    )
                except asyncio.TimeoutError:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "上一轮取消尚未落库（数据库可能被外部写锁占用），"
                            "后台仍在收尾，请稍候重试"
                        ),
                    ) from None
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except sqlite3.OperationalError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"数据库暂时不可写（可能被其他连接锁定）: {exc}",
            ) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.get("/api/catalog")
    async def api_catalog(q: str = "", page: int = 1, page_size: int = 20) -> JSONResponse:
        return JSONResponse(store.catalog_page(q, page, page_size))

    return app


app = create_app()
