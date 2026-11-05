# StableFlow backend — API + terminal UI in one process (B-D18)
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PGHOST=postgres

WORKDIR /app

# uv — same lockfile as the host (D8: Python 3.11+ / uv)
RUN pip install --no-cache-dir uv

# deps first (layer cache): frozen lock, no dev/etl extras, no project (package=false)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# app + UI (raw_* tables are reached via PGHOST=postgres inside compose)
COPY backend/ ./backend/
COPY ui/ ./ui/

EXPOSE 8080
CMD ["uv", "run", "--no-sync", "uvicorn", "backend.api.main:app", \
     "--host", "0.0.0.0", "--port", "8080"]
