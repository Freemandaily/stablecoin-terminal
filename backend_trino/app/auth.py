"""API key auth. Open when API_KEY env is empty (local dev)."""
from fastapi import Header, HTTPException

from app import config


async def require_key(x_api_key: str | None = Header(default=None)):
    if not config.API_KEY:
        return True
    if x_api_key != config.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True
