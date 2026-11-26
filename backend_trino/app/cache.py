"""Tiny in-memory TTL cache. Same interface a Redis swap can keep."""
import time

_store: dict = {}


def make_key(*parts) -> str:
    return "|".join(str(p) for p in parts)


def get(key: str):
    entry = _store.get(key)
    if not entry:
        return None
    value, expires = entry
    if time.time() > expires:
        _store.pop(key, None)
        return None
    return value


def set(key: str, value, ttl_s: int):
    _store[key] = (value, time.time() + ttl_s)


def clear():
    _store.clear()
