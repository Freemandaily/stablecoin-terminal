"""FastAPI entrypoint: Frontend -> FastAPI -> Trino -> GCS Iceberg marts."""
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

from app.routers import api, health, query, sql
from app.logo_proxy import router as logo_router

app = FastAPI(
    title="StableFlow Lakehouse API",
    version="0.1.0",
    description=(
        "Read-only API over Trino: lake.marts.* Iceberg tables on "
        "gs://term-flow/warehouse. No mart data in Postgres."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class NoCacheMiddleware(BaseHTTPMiddleware):
    """Always revalidate UI/API responses; images keep their own cache rules."""

    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        if not response.headers.get("content-type", "").startswith("image/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        return response


app.add_middleware(NoCacheMiddleware)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(api.router)          # /api/* — exact prototype JSON contract
app.include_router(logo_router)         # /api/tw/*, /api/logo/* — same-origin logos
app.include_router(query.router)        # /query/* — flexible mart explorer
app.include_router(sql.router)          # /sql — gated Trino SELECT

# Mount last: the catch-all "/" must not swallow /api, /query, /sql or /docs.
_default_frontend = Path(__file__).resolve().parents[2] / "frontend"
_frontend = Path(os.environ.get("FRONTEND_DIR") or _default_frontend)
if not _frontend.is_dir():
    _frontend = Path("/app/frontend")  # container path (Dockerfile COPY)
app.mount("/", StaticFiles(directory=_frontend, html=True), name="ui")
