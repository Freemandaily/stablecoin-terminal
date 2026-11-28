"""Same-origin logo proxy (B-D37).

The verified logo URLs in the marts point at TrustWallet/CoinGecko hosts. Some
client networks block those hosts, so the API rewrites them to local paths and
fetches the image server-side. Unknown failures are 404 — the UI falls back to
a letter chip instead of showing a fabricated image (§9).
"""

from __future__ import annotations

import urllib.request

from fastapi import APIRouter, Response

router = APIRouter(prefix="/api", tags=["logo-proxy"])


def _safe_path(path: str, *, prefix: str, suffix: str = "") -> bool:
    return (
        bool(path)
        and not path.startswith("/")
        and ".." not in path.split("/")
        and "?" not in path
        and "#" not in path
        and path.startswith(prefix)
        and (not suffix or path.endswith(suffix))
    )


def proxy_logo_url(url: str | None) -> str | None:
    """Rewrite known upstream logo URLs to same-origin proxy paths."""
    if not url:
        return url
    tw = "raw.githubusercontent.com/trustwallet/assets/master/"
    if tw in url:
        path = url.split(tw, 1)[1]
        return f"/api/tw/{path}" if _safe_path(path, prefix="blockchains/", suffix="/logo.png") else url
    cdn = "coin-images.coingecko.com/"
    if cdn in url:
        path = url.split(cdn, 1)[1]
        return f"/api/logo/{path}" if _safe_path(path, prefix="coins/images/") else url
    return url


def _fetch_logo(url: str) -> Response:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "StableFlow/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            return Response(
                content=resp.read(),
                media_type=resp.headers.get("Content-Type", "image/png"),
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except Exception:
        return Response(status_code=404)


@router.get("/logo/{path:path}")
def proxy_logo(path: str) -> Response:
    """CoinGecko logo proxy (legacy URLs)."""
    if not _safe_path(path, prefix="coins/images/"):
        return Response(status_code=404)
    return _fetch_logo(f"https://coin-images.coingecko.com/{path}")


@router.get("/tw/{path:path}")
def proxy_tw_logo(path: str) -> Response:
    """TrustWallet logo proxy — only blockchains/…/logo.png paths."""
    if not _safe_path(path, prefix="blockchains/", suffix="/logo.png"):
        return Response(status_code=404)
    return _fetch_logo(f"https://raw.githubusercontent.com/trustwallet/assets/master/{path}")
