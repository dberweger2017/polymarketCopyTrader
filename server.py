from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from bot_core import BotConfig, PolymarketCopyBot
from options_dashboard import load_options_dashboard


bot = PolymarketCopyBot(BotConfig.from_env())
frontend_dist = Path(__file__).resolve().parent / "frontend" / "dist"
project_root = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(_app: FastAPI):
    bot.start()
    try:
        yield
    finally:
        bot.stop()


app = FastAPI(title="Polymarket Copy Bot", lifespan=lifespan)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/state")
def state() -> dict:
    return bot.snapshot()


@app.get("/api/chart")
def chart(series: str = "all", window: str = "since_start") -> dict:
    try:
        return bot.chart_snapshot(series=series, window=window)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/options/state")
def options_state() -> dict:
    return load_options_dashboard(project_root)


if frontend_dist.exists():
    assets_dir = frontend_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{path:path}")
    def spa(path: str) -> FileResponse:
        if path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        return FileResponse(frontend_dist / "index.html")
