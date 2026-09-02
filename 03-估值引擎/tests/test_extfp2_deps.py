"""EXTFP2-A 新增下载依赖审计与锁定（离线测试）。

验证 httpx + Pillow 已加入本地依赖并锁定，且在本环境可用；全程不触发实时网络
（Pillow 用合成图，httpx 用 MockTransport）。退出证据 = 依赖锁定证据 + 离线测试绿。
"""

from __future__ import annotations

import io
from pathlib import Path

import httpx
from PIL import Image, __version__

# 本项目约定的下载依赖版本下限（技术方案 §8.5 候选依赖）
HTTPX_MIN_VERSION = (0, 28)
PILLOW_MIN_VERSION = (12, 3)

PYPROJECT_PATH = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _parse_version(v: str) -> tuple[int, ...]:
    parts = []
    for seg in v.split("."):
        digits = ""
        for ch in seg:
            if ch.isdigit():
                digits += ch
            else:
                break
        if digits:
            parts.append(int(digits))
    return tuple(parts)


def test_httpx_importable_and_version() -> None:
    ver = _parse_version(httpx.__version__)
    assert ver >= HTTPX_MIN_VERSION, f"httpx {httpx.__version__} < {HTTPX_MIN_VERSION}"


def test_pillow_importable_and_version() -> None:
    ver = _parse_version(__version__)
    assert ver >= PILLOW_MIN_VERSION, f"pillow {__version__} < {PILLOW_MIN_VERSION}"


def test_pillow_generates_and_reads_image() -> None:
    """合成 2x2 图往返，验证 Pillow 图像创建/打开/尺寸/格式可用（不触网）。"""
    img = Image.new("RGB", (2, 2), color=(255, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    reopened = Image.open(buf)
    assert reopened.format == "PNG"
    assert reopened.size == (2, 2)


def test_httpx_mock_transport_no_network() -> None:
    """httpx 客户端经 MockTransport 返回固定响应，证明网络可由 mock 替代（离线）。"""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    resp = client.get("https://example.invalid/")  # 域名无效也不触网（MockTransport）
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_httpx_exception_types_available() -> None:
    """下载器重试与停止逻辑依赖的错误类型已可导入（技术方案 §8.2）。"""
    assert issubclass(httpx.ConnectError, httpx.HTTPError)
    assert issubclass(httpx.ReadTimeout, httpx.HTTPError)
    assert issubclass(httpx.HTTPStatusError, httpx.HTTPError)


def test_pyproject_declares_download_deps() -> None:
    """pyproject.toml 已声明 httpx >=0.28 与 pillow >=12.3（锁定契约证据）。"""
    text = PYPROJECT_PATH.read_text(encoding="utf-8")
    assert "httpx>=0.28" in text
    assert "pillow>=12.3" in text
