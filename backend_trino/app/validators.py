"""Shared query validators. Addresses are chain-agnostic (EVM or base58)."""
from datetime import date

from fastapi import HTTPException

from app import config


def check_limit(limit: int) -> int:
    if limit < 1 or limit > config.MAX_LIMIT:
        raise HTTPException(status_code=422, detail=f"limit must be 1..{config.MAX_LIMIT}")
    return limit


def check_offset(offset: int) -> int:
    if offset < 0:
        raise HTTPException(status_code=422, detail="offset must be >= 0")
    return offset


def check_dates(start: date | None, end: date | None):
    if start and end and start > end:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")
    if start and end and (end - start).days > 90:
        raise HTTPException(status_code=422, detail="date range max 90 days")
    return start, end


def check_address(value: str, label: str = "address") -> str:
    """Only reject empty/oversized values; token and holder ids vary by chain."""
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise HTTPException(status_code=422, detail=f"invalid {label}")
    return value
