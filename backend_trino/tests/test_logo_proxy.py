from fastapi.testclient import TestClient

from app import logo_proxy
from app.main import app


TW = (
    "https://raw.githubusercontent.com/trustwallet/assets/master/"
    "blockchains/ethereum/assets/0xABC/logo.png"
)
CG = "https://coin-images.coingecko.com/coins/images/1/large/tether.png"


class _Resp:
    headers = {"Content-Type": "image/png"}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return b"\x89PNG\r\n\x1a\n"


def test_rewrite_known_logo_hosts():
    assert logo_proxy.proxy_logo_url(TW) == (
        "/api/tw/blockchains/ethereum/assets/0xABC/logo.png"
    )
    assert logo_proxy.proxy_logo_url(CG) == "/api/logo/coins/images/1/large/tether.png"
    assert logo_proxy.proxy_logo_url(None) is None
    assert logo_proxy.proxy_logo_url("https://example.com/logo.png") == (
        "https://example.com/logo.png"
    )


def test_tw_proxy_serves_image(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout):
        seen["url"] = req.full_url
        return _Resp()

    monkeypatch.setattr(logo_proxy.urllib.request, "urlopen", fake_urlopen)
    r = TestClient(app).get("/api/tw/blockchains/ethereum/assets/0xABC/logo.png")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/png"
    assert r.headers["cache-control"] == "public, max-age=86400"
    assert seen["url"] == TW


def test_tw_proxy_rejects_non_logo_paths():
    r = TestClient(app).get("/api/tw/README.md")
    assert r.status_code == 404
