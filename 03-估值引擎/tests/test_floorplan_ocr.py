"""EXTFP3-B 离线测试：OCR 请求器与运行记录（floorplan_ocr）。

全部用例用 ``httpx.MockTransport`` 离线应答 + 脱敏 API 响应 fixture，绝不触网、不调用
真实 Qwen、不读写真实密钥。覆盖：幂等键/请求哈希/Data URL、响应元数据提取、状态机
迁移白名单、重试/退避分类、单图请求（成功/部分/需复核/重试/失败/敏感响应/成本门禁）、
批运行（成功/断点续跑/force-new-run/成本门禁停止/非 DOWNLOADED 跳过）。
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image

from compsval.ingest.floorplan_asset import (
    ASSET_MANIFEST_FILENAME,
    AssetStatus,
    FloorplanAsset,
    FloorplanAssetRun,
)
from compsval.ingest.floorplan_ocr import (
    OCR_PARSER_VERSION,
    OCR_REQUESTER_VERSION,
    STATE_FILENAME,
    OcrRunRecord,
    OcrState,
    OcrTaskRecord,
    build_image_data_url,
    call_ocr_one,
    compute_ocr_task_id,
    compute_request_hash,
    deterministic_ocr_run_id,
    extract_response_metadata,
    load_asset_manifest,
    raw_response_filename,
    run_ocr_batch,
    transition,
)
from compsval.ingest.floorplan_ocr_contract import (
    OCR_MODEL_ID,
    OcrCostConfig,
    OcrCostGate,
    OcrRunConfig,
    utc_now_iso,
)

FAKE_KEY = "sk-EXTFP3-B-test-dummy-0123456789abcdef"
MANIFEST_HASH = "recids-hash-EXTFP3B-0123456789abcdef"
DOMAIN = "ke-image.ljcdn.com"


def _png_bytes(width: int = 8, height: int = 6) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), (30, 200, 30)).save(buf, format="PNG")
    return buf.getvalue()


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ---------------------------------------------------------------------------
# 资产 manifest fixture：真实原图文件 + 不可变 manifest JSON（离线）
# ---------------------------------------------------------------------------


def _write_asset_manifest(
    tmp_path: Path, specs: list[tuple[str, bytes]], *, status: AssetStatus = AssetStatus.DOWNLOADED
) -> Path:
    """把 specs[(asset_id, 图片字节)...] 写成资产 manifest + 原图，返回 manifest 路径。"""
    batch = tmp_path / "batch"
    batch.mkdir(parents=True, exist_ok=True)
    assets: list[FloorplanAsset] = []
    for asset_id, img in specs:
        fname = f"{asset_id}.png"
        (batch / fname).write_bytes(img)
        assets.append(
            FloorplanAsset(
                asset_id=asset_id,
                download_task_id=f"dt-{asset_id}",
                source_record_id=f"R-{asset_id}",
                source_row_number=1,
                url_ordinal=1,
                source_url_raw=f"https://{DOMAIN}/{asset_id}.png",
                download_url=f"https://{DOMAIN}/{asset_id}.png",
                downloader_version="EXTFP2-C-DL-1.0",
                asset_status=status,
                mime_type="image/png",
                file_extension=".png",
                width=8,
                height=6,
                byte_size=len(img),
                sha256=_sha(img),
                storage_path=fname,
            )
        )
    run = FloorplanAssetRun(
        batch_id="batch-1",
        download_run_id="dl-run-1",
        download_run_dir=".",
        manifest_ref=MANIFEST_HASH,
        sourced=True,
        created_at="2026-08-25T00:00:00Z",
        assets=assets,
        counts={"DOWNLOADED": len(assets)},
    )
    manifest_path = batch / ASSET_MANIFEST_FILENAME
    manifest_path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
    return manifest_path


# ---------------------------------------------------------------------------
# OCR 响应 fixture 与 MockTransport（脱敏，不触网）
# ---------------------------------------------------------------------------


def _ocr_ok_body(
    *, finish_reason: str = "stop", model: str | None = None, extra: dict | None = None
) -> dict:
    body = {
        "output": {"choices": [{"finish_reason": finish_reason}]},
        "usage": {"input_tokens": 1520, "output_tokens": 30},
        "request_id": "req-test-001",
        "model": model or OCR_MODEL_ID,
    }
    if extra:
        body.update(extra)
    return body


def _json_transport(counter: dict[str, int], body: dict, status: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        counter["calls"] = counter.get("calls", 0) + 1
        return httpx.Response(status, json=body, request=request)

    return httpx.MockTransport(handler)


def _retry_then_ok_transport(
    counter: dict[str, int], fail_status: int = 429
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        n = counter.get("calls", 0) + 1
        counter["calls"] = n
        if n == 1:
            return httpx.Response(fail_status, json={}, request=request)
        return httpx.Response(200, json=_ocr_ok_body(), request=request)

    return httpx.MockTransport(handler)


def _mock_transport(
    handler: Callable[[httpx.Request], httpx.Response | httpx.HTTPError],
) -> httpx.MockTransport:
    def _handle(request: httpx.Request) -> httpx.Response:
        result = handler(request)
        if isinstance(result, httpx.HTTPError):
            raise result
        return result

    return httpx.MockTransport(_handle)


def _fast_config(**cost_kw: Any) -> OcrRunConfig:
    return OcrRunConfig(cost=OcrCostConfig(base_backoff=0.0, max_backoff=0.0, **cost_kw))


def _task(img: bytes, *, asset_id: str = "A1") -> OcrTaskRecord:
    return OcrTaskRecord(
        ocr_task_id=compute_ocr_task_id(_sha(img)),
        ocr_run_id="run-test",
        asset_id=asset_id,
        image_sha256=_sha(img),
        width=8,
        height=6,
        mime_type="image/png",
        request_hash=compute_request_hash(
            image_sha256=_sha(img),
            min_pixels=3072,
            max_pixels=8388608,
            enable_rotate=False,
            stream=False,
        ),
    )


# ---------------------------------------------------------------------------
# 1) 幂等键 / 请求哈希 / Data URL
# ---------------------------------------------------------------------------


def test_compute_ocr_task_id_idempotent() -> None:
    """同输入（image_sha256 + 冻结合同 params + parser_version）→ 同幂等键。"""
    img_a = _png_bytes()
    params = {"min_pixels": 3072, "max_pixels": 8388608, "enable_rotate": False, "stream": False}
    k1 = compute_ocr_task_id(_sha(img_a), params=params)
    k2 = compute_ocr_task_id(_sha(img_a), params=params)
    assert k1 == k2
    # 图片不同 → 键不同
    img_b = _png_bytes(width=9, height=7)
    assert compute_ocr_task_id(_sha(img_b), params=params) != k1
    # 参数不同 → 键不同
    assert compute_ocr_task_id(_sha(img_a), params={**params, "stream": True}) != k1
    # parser 版本不同 → 键不同
    assert compute_ocr_task_id(_sha(img_a), params=params, parser_version="x1") != k1
    # 键是 64 位 hex
    assert len(k1) == 64


def test_compute_request_hash_no_secrets_and_stable() -> None:
    """请求哈希不含密钥/Base64，且同参稳定。"""
    img = _png_bytes()
    h1 = compute_request_hash(
        image_sha256=_sha(img),
        min_pixels=3072,
        max_pixels=8388608,
        enable_rotate=False,
        stream=False,
    )
    h2 = compute_request_hash(
        image_sha256=_sha(img),
        min_pixels=3072,
        max_pixels=8388608,
        enable_rotate=False,
        stream=False,
    )
    assert h1 == h2
    assert FAKE_KEY not in h1
    assert "base64" not in h1
    assert (
        compute_request_hash(
            image_sha256=_sha(img),
            min_pixels=3072,
            max_pixels=8388608,
            enable_rotate=True,
            stream=False,
        )
        != h1
    )


def test_build_image_data_url_explicit_mime() -> None:
    """显式 MIME 时直接用（不嗅探），前缀正确。"""
    url = build_image_data_url(b"\x00\x01", mime_type="image/png")
    assert url.startswith("data:image/png;base64,")


def test_build_image_data_url_sniffs_real_bytes() -> None:
    """MIME 缺失/非 image 时回退真实字节嗅探。"""
    png = _png_bytes()
    url = build_image_data_url(png)
    assert url.startswith("data:image/png;base64,")
    url2 = build_image_data_url(png, mime_type="application/octet-stream")
    assert url2.startswith("data:image/png;base64,")


def test_build_image_data_url_invalid_bytes_fails_closed() -> None:
    """字节无法识别 MIME 时抛错（fail-closed，不把 MIME 不明图片发给 Qwen）。"""
    with pytest.raises(ValueError):
        build_image_data_url(b"not an image at all")


# ---------------------------------------------------------------------------
# 2) 响应元数据提取（原生 / OpenAI 兼容 / 缺失）
# ---------------------------------------------------------------------------


def test_extract_response_metadata_native() -> None:
    """DashScope 原生接口（output.choices / usage.input_tokens / request_id / model）。"""
    meta = extract_response_metadata(
        {
            "output": {"choices": [{"finish_reason": "stop"}]},
            "usage": {"input_tokens": 1520, "output_tokens": 30},
            "request_id": "req-1",
            "model": OCR_MODEL_ID,
        }
    )
    assert meta["finish_reason"] == "stop"
    assert meta["prompt_tokens"] == 1520
    assert meta["completion_tokens"] == 30
    assert meta["provider_request_id"] == "req-1"
    assert meta["model_returned"] == OCR_MODEL_ID


def test_extract_response_metadata_openai_compat() -> None:
    """OpenAI 兼容格式（choices / usage.prompt_tokens / id / model）。"""
    meta = extract_response_metadata(
        {
            "choices": [{"finish_reason": "length"}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 200},
            "id": "chatcmpl-1",
            "model": OCR_MODEL_ID,
        }
    )
    assert meta["finish_reason"] == "length"
    assert meta["prompt_tokens"] == 100
    assert meta["completion_tokens"] == 200
    assert meta["provider_request_id"] == "chatcmpl-1"


def test_extract_response_metadata_missing_fields() -> None:
    """结构不匹配时字段为 None，不静默伪造。"""
    meta = extract_response_metadata({"unexpected": True})
    assert meta["finish_reason"] is None
    assert meta["prompt_tokens"] is None
    assert meta["completion_tokens"] is None
    assert meta["model_returned"] is None
    assert meta["provider_request_id"] is None


# ---------------------------------------------------------------------------
# 3) 状态机迁移白名单
# ---------------------------------------------------------------------------


def test_transition_valid_paths() -> None:
    """正常路径：PENDING→RUNNING→{SUCCEEDED/PARTIAL/FAILED/NEEDS_REVIEW}，终态不回退。"""
    # 合法：PENDING → RUNNING → SUCCEEDED（终态）
    t = _task(_png_bytes())
    transition(t, OcrState.OCR_RUNNING)
    transition(t, OcrState.OCR_SUCCEEDED)
    assert t.state is OcrState.OCR_SUCCEEDED
    with pytest.raises(ValueError):
        transition(t, OcrState.OCR_RUNNING)  # 终态不可回退
    with pytest.raises(ValueError):
        transition(t, OcrState.OCR_PENDING)

    # RUNNING 状态可被显式观测（PENDING 必须先经过 RUNNING）
    r = _task(_png_bytes(width=9, height=7))
    transition(r, OcrState.OCR_RUNNING)
    assert r.state is OcrState.OCR_RUNNING


def test_transition_invalid_from_pending() -> None:
    """PENDING 不能直接跳到成功/部分/需复核（必须先 RUNNING）。"""
    t = _task(_png_bytes())
    with pytest.raises(ValueError):
        transition(t, OcrState.OCR_SUCCEEDED)
    with pytest.raises(ValueError):
        transition(t, OcrState.OCR_PARTIAL)
    with pytest.raises(ValueError):
        transition(t, OcrState.NEEDS_REVIEW)


# ---------------------------------------------------------------------------
# 4) 单图请求 call_ocr_one
# ---------------------------------------------------------------------------


def test_call_ocr_one_success(tmp_path: Path) -> None:
    """200 + stop → OCR_SUCCEEDED：元数据/原始响应落盘/哈希/成本累计。"""
    img = _png_bytes()
    task = _task(img)
    counter: dict[str, int] = {}
    gate = OcrCostGate()
    with httpx.Client(transport=_json_transport(counter, _ocr_ok_body())) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.OCR_SUCCEEDED
    assert result.model_returned == OCR_MODEL_ID
    assert result.finish_reason == "stop"
    assert result.prompt_tokens == 1520
    assert result.completion_tokens == 30
    assert result.attempts == 1
    assert result.response_status == "http-200"
    assert result.error_code is None
    raw_path = tmp_path / raw_response_filename(result.ocr_task_id)
    assert raw_path.is_file()
    assert result.raw_response_sha256 == _sha(raw_path.read_bytes())  # 哈希与落盘字节一致
    assert len(result.raw_response_sha256) == 64
    assert gate.total_images == 1
    assert gate.total_prompt_tokens == 1520
    assert gate.total_completion_tokens == 30
    assert counter["calls"] == 1


def test_call_ocr_one_dual_caliber_hashes(tmp_path: Path) -> None:
    """双口径登记：raw_response_sha256=原始字节；raw_response_file_sha256=落盘实际字节。

    响应体含换行时 Windows 文本模式会把 ``\\n`` 翻译为 ``\\r\\n``，落盘字节 ≠ 原始字节；
    两字段分别按各自口径登记，完整性/一致性门禁以落盘口径为主核对（SUGGESTION ①）。
    """
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    raw = json.dumps(_ocr_ok_body(), ensure_ascii=False, indent=2).encode("utf-8")  # 含 \n

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=raw, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.OCR_SUCCEEDED
    raw_path = tmp_path / raw_response_filename(result.ocr_task_id)
    assert raw_path.is_file()
    # 原始字节口径（历史可比）与落盘实际字节口径分别登记
    assert result.raw_response_sha256 == _sha(raw)
    assert result.raw_response_file_sha256 == _sha(raw_path.read_bytes())
    assert len(result.raw_response_file_sha256) == 64


def test_ocr_task_record_legacy_without_file_sha_defaults_none() -> None:
    """历史运行记录 JSON 无 raw_response_file_sha256 字段时反序列化默认 None（不回填）。"""
    record = OcrTaskRecord(
        ocr_task_id="t",
        ocr_run_id="r",
        asset_id="a",
        image_sha256="img",
        request_hash="rh",
        raw_response_sha256="x",
    )
    loaded = OcrTaskRecord.model_validate(record.model_dump(exclude={"raw_response_file_sha256"}))
    assert loaded.raw_response_sha256 == "x"
    assert loaded.raw_response_file_sha256 is None


def test_call_ocr_one_finish_reason_length_partial(tmp_path: Path) -> None:
    """finish_reason=length 视为部分成功（OCR_PARTIAL），不当作成功（§9.2）。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    body = _ocr_ok_body(finish_reason="length")
    with httpx.Client(transport=_json_transport({}, body)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.OCR_PARTIAL
    assert result.error_code == "finish_reason_length"
    # 部分成功的原始响应仍落盘（供 EXTFP3-C 解析诊断）
    assert result.raw_response_path is not None


def test_call_ocr_one_model_mismatch_needs_review(tmp_path: Path) -> None:
    """模型不一致 → NEEDS_REVIEW（证据保留，不当作成功）。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    body = _ocr_ok_body(model="qwen-vl-ocr-2999-01-01")
    with httpx.Client(transport=_json_transport({}, body)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.NEEDS_REVIEW
    assert result.error_code == "model_mismatch"
    assert result.model_returned == "qwen-vl-ocr-2999-01-01"


def test_call_ocr_one_retry_429_then_success(tmp_path: Path) -> None:
    """429 → 退避重试 → 200：attempts=2，请求数 2。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    counter: dict[str, int] = {}
    with httpx.Client(transport=_retry_then_ok_transport(counter)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.OCR_SUCCEEDED
    assert result.attempts == 2
    assert counter["calls"] == 2


def test_call_ocr_one_5xx_exhausts_retries(tmp_path: Path) -> None:
    """连续 5xx → 重试耗尽 → OCR_FAILED（http_error）。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    transport = _json_transport({}, {}, status=500)
    with httpx.Client(transport=transport) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
            max_attempts=3,
        )
    assert result.state is OcrState.OCR_FAILED
    assert result.attempts == 3
    assert result.response_status == "http-500"
    assert result.error_code == "http_error"


def test_call_ocr_one_404_no_retry(tmp_path: Path) -> None:
    """404 不重试：attempts=1 直接失败（§8.2 其余 4xx 不重试）。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    with httpx.Client(transport=_json_transport({}, {}, status=404)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
            max_attempts=3,
        )
    assert result.state is OcrState.OCR_FAILED
    assert result.attempts == 1
    assert result.response_status == "http-404"


def test_call_ocr_one_network_error(tmp_path: Path) -> None:
    """连接类异常 → 重试耗尽 → OCR_FAILED（network_error）。"""
    img = _png_bytes()
    task = _task(img)

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    gate = OcrCostGate()
    with httpx.Client(transport=_mock_transport(boom)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
            max_attempts=2,
        )
    assert result.state is OcrState.OCR_FAILED
    assert result.response_status == "network-ConnectError"
    assert result.error_code == "network_error"
    assert result.attempts == 2


def test_call_ocr_one_sensitive_response_blocked(tmp_path: Path) -> None:
    """原始响应含 Base64 Data URL → NEEDS_REVIEW 且不落盘（§15 fail-closed）。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate()
    body = _ocr_ok_body(extra={"text": "data:image/png;base64,AAAA=="})
    with httpx.Client(transport=_json_transport({}, body)) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.NEEDS_REVIEW
    assert result.error_code == "sensitive_content_in_response"
    assert result.raw_response_path is None
    assert result.raw_response_sha256 is None


def test_call_ocr_one_cost_gate_blocked(tmp_path: Path) -> None:
    """请求前成本门禁已触发 → fail-closed：OCR_FAILED（cost_gate_hit），零请求。"""
    img = _png_bytes()
    task = _task(img)
    gate = OcrCostGate(OcrCostConfig(max_images=1))
    gate.total_images = 1  # 已满
    counter: dict[str, int] = {}
    with httpx.Client(transport=_json_transport(counter, _ocr_ok_body())) as client:
        result = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=tmp_path,
            client=client,
            cost_gate=gate,
        )
    assert result.state is OcrState.OCR_FAILED
    assert result.error_code == "cost_gate_hit"
    assert counter.get("calls", 0) == 0
    assert gate.limit_hit == "max_images"


# ---------------------------------------------------------------------------
# 5) 批运行 run_ocr_batch
# ---------------------------------------------------------------------------


def test_run_ocr_batch_success(tmp_path: Path) -> None:
    """2 张 DOWNLOADED 资产 → 全部 OCR_SUCCEEDED，请求体/头符合冻结合同。"""
    img_a, img_b = _png_bytes(), _png_bytes(width=9, height=7)
    manifest_path = _write_asset_manifest(tmp_path, [("A1", img_a), ("B2", img_b)])
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        captured["auth"] = request.headers.get("authorization")
        captured["inspect"] = request.headers.get("x-dashscope-datainspection")
        return httpx.Response(200, json=_ocr_ok_body(), request=request)

    transport = _mock_transport(handler)
    record = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=transport,
    )
    assert isinstance(record, OcrRunRecord)
    assert record.state_counts == {OcrState.OCR_SUCCEEDED.value: 2}
    assert record.cost["total_images"] == 2
    assert record.cost["total_prompt_tokens"] == 3040
    assert all(t.state is OcrState.OCR_SUCCEEDED for t in record.tasks)
    assert all(t.raw_response_path for t in record.tasks)
    assert (tmp_path / "out" / f"run_{record.ocr_run_id}" / "ocr_run.json").is_file()
    body = captured["body"]
    assert body["model"] == OCR_MODEL_ID
    assert body["stream"] is False
    assert captured["auth"] == f"Bearer {FAKE_KEY}"
    assert captured["inspect"] == "enable"


def test_run_ocr_batch_resume_skips_completed(tmp_path: Path) -> None:
    """断点续跑：同 manifest 二次运行按幂等键跳过已完成任务，零新增请求。"""
    img_a, img_b = _png_bytes(), _png_bytes(width=9, height=7)
    manifest_path = _write_asset_manifest(tmp_path, [("A1", img_a), ("B2", img_b)])
    counter: dict[str, int] = {}
    transport = _json_transport(counter, _ocr_ok_body())

    r1 = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=transport,
    )
    assert counter["calls"] == 2
    r2 = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=transport,
    )
    assert r2.ocr_run_id == r1.ocr_run_id
    assert r2.state_counts == {OcrState.OCR_SUCCEEDED.value: 2}
    assert counter["calls"] == 2  # 无新增请求
    # 双口径字段随续跑复用一并拷贝：复用任务的落盘口径与 r1 一致且与盘上字节相符
    run_dir = tmp_path / "out" / f"run_{r2.ocr_run_id}"
    for task in r2.tasks:
        assert task.raw_response_file_sha256 is not None
        assert task.raw_response_file_sha256 == _sha(
            (run_dir / Path(task.raw_response_path).name).read_bytes()
        )


def test_run_ocr_batch_force_new_run(tmp_path: Path) -> None:
    """force_new_run → 新 run_id 与新目录，不覆盖旧运行证据（重发请求）。"""
    img = _png_bytes()
    manifest_path = _write_asset_manifest(tmp_path, [("A1", img)])
    counter: dict[str, int] = {}
    transport = _json_transport(counter, _ocr_ok_body())
    r1 = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=transport,
    )
    r2 = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=transport,
        force_new_run=True,
    )
    assert r2.ocr_run_id != r1.ocr_run_id
    assert r2.run_dir != r1.run_dir
    assert counter["calls"] == 2  # 二次运行重新请求


def test_raw_response_filename_stays_within_max_path() -> None:
    """EXTFP3-H#MAX_PATH 回归：force-new-run 长 run_id 下原始响应路径须短于 260。

    实测基线（2026-08-27）：OCR 输出根目录 117 字符 + force-new-run run 目录
    ``run_floorplan-ocr-data_selection_l-20260827T092014Z``（51 字符）+ 旧文件名
    ``raw_response_<64hex>.json``（82 字符）+ ``.incomplete`` 中间后缀（11 字符）
    = 263 > Windows MAX_PATH（260），H9 复跑落盘 FileNotFoundError。
    修复：文件名内 task_id 截断为 24 hex（42 字符），全路径 + .incomplete ≤ 223。
    """
    task_id = "a" * 64
    fname = raw_response_filename(task_id)
    assert len(fname) == len("raw_response_") + 24 + len(".json")
    assert fname == f"raw_response_{task_id[:24]}.json"
    # 以 H9 同构的真实最坏基线断言：+ .incomplete 中间文件也必须 < 260。
    worst_incomplete_len = 117 + 51 + len(fname) + len(".incomplete")
    assert worst_incomplete_len < 260


def test_run_ocr_batch_cost_gate_stops_batch(tmp_path: Path) -> None:
    """max_images=1 时第 2 张 fail-closed：1 成功 + 1 失败（cost_gate_hit），不透支。"""
    img_a, img_b = _png_bytes(), _png_bytes(width=9, height=7)
    manifest_path = _write_asset_manifest(tmp_path, [("A1", img_a), ("B2", img_b)])
    counter: dict[str, int] = {}
    record = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(max_images=1),
        api_key=FAKE_KEY,
        transport=_json_transport(counter, _ocr_ok_body()),
    )
    assert record.state_counts == {
        OcrState.OCR_SUCCEEDED.value: 1,
        OcrState.OCR_FAILED.value: 1,
    }
    assert record.cost["limit_hit"] == "max_images"
    assert counter["calls"] == 1


def test_run_ocr_batch_skips_non_downloaded(tmp_path: Path) -> None:
    """非 DOWNLOADED 资产不进 OCR（IMAGE_INVALID 跳过，零请求）。"""
    img = _png_bytes()
    manifest_path = _write_asset_manifest(tmp_path, [("A1", img)], status=AssetStatus.IMAGE_INVALID)
    counter: dict[str, int] = {}
    record = run_ocr_batch(
        manifest_path,
        tmp_path / "out",
        config=_fast_config(),
        api_key=FAKE_KEY,
        transport=_json_transport(counter, _ocr_ok_body()),
    )
    assert record.tasks == []
    assert record.state_counts == {}
    assert counter.get("calls", 0) == 0


def test_run_ocr_batch_deterministic_run_id() -> None:
    """确定性 run_id 基于 manifest_ref；manifest 缺 ref 时回退 unknown 前缀。"""
    assert (
        deterministic_ocr_run_id(
            FloorplanAssetRun(
                batch_id="b",
                download_run_id="d",
                download_run_dir=".",
                manifest_ref=MANIFEST_HASH,
                sourced=True,
                created_at="2026-08-25T00:00:00Z",
            )
        )
        == f"floorplan-ocr-{MANIFEST_HASH[:16]}"
    )


def test_deterministic_run_id_sanitizes_path_separators() -> None:
    """EXTFP3-B#F6：manifest_ref 含路径分隔符/冒号（Windows 绝对路径）时，run_id 只含
    字母/数字/.//_，可作为目录名（EXTFP3-F 真实联调发现含 ':' 的 run_id 曾致 mkdir 失败）。"""
    run_id = deterministic_ocr_run_id(
        FloorplanAssetRun(
            batch_id="b",
            download_run_id="d",
            download_run_dir=".",
            manifest_ref="C:/Users/某用户/样本来源清单.md",
            sourced=False,
            created_at="2026-08-25T00:00:00Z",
        )
    )
    assert run_id.startswith("floorplan-ocr-")
    assert re.fullmatch(r"[A-Za-z0-9._-]+", run_id.removeprefix("floorplan-ocr-"))
    assert run_id == deterministic_ocr_run_id(
        FloorplanAssetRun(
            batch_id="b",
            download_run_id="d",
            download_run_dir=".",
            manifest_ref="C:/Users/某用户/样本来源清单.md",
            sourced=False,
            created_at="2026-08-25T00:00:00Z",
        )
    )


def test_load_asset_manifest_missing_fails() -> None:
    """缺失 manifest → FileNotFoundError（fail-closed）。"""
    with pytest.raises(FileNotFoundError):
        load_asset_manifest(Path("does-not-exist.json"))


def test_requester_version_frozen() -> None:
    """请求器版本与 parser 占位版本冻结（幂等键依赖 parser_version）。

    OCRNEXT-B：请求器版本升级为 OCRNEXT-B-REQ-1.0（并发调度与性能埋点，方案 §5.3/§5.4），
    parser_version 保持不变——ocr_task_id 跨运行身份可比（OCRNEXT-WP0 合同 §4）。
    """
    assert OCR_REQUESTER_VERSION == "OCRNEXT-B-REQ-1.0"
    assert OCR_PARSER_VERSION == "EXTFP3-B-NO-PARSER"


# ---------------------------------------------------------------------------
# OCRNEXT-B：有界并发调度 / 单写入者 / 性能埋点（离线 fixture，绝不触网）
# 方案 §7.3.1 场景：投放、乱序完成、超时、429、重试、成本停止、断点续跑、
# 原子持久化、峰值并发不越界、稳定任务键排序。
# ---------------------------------------------------------------------------

_DATA_URL_TAIL_RE = re.compile(r"data:image/[A-Za-z0-9.+-]+;base64,([A-Za-z0-9+/=]+)")


def _spec_tail(img: bytes) -> str:
    """图片在其请求 Data URL 中的可识别尾部（同一图片恒定、不同图片互异）。"""
    return base64.b64encode(img).decode("ascii")[-24:]


def _request_image_key(request: httpx.Request) -> str:
    match = _DATA_URL_TAIL_RE.search(request.read().decode("utf-8"))
    return match.group(1)[-24:] if match else "unknown"


class _ConcurrencyProbe:
    """线程安全在飞计数探针（MockTransport handler 内调用）。

    ``enter`` 返回本次调用的全局序号（进入时捕获，避免并发下序号漂移）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.calls = 0
        self.completion_order: list[str] = []

    def enter(self, key: str) -> int:
        with self._lock:
            self.active += 1
            self.calls += 1
            self.peak = max(self.peak, self.active)
            return self.calls

    def exit(self, key: str) -> None:
        with self._lock:
            self.active -= 1
            self.completion_order.append(key)


def _probe_transport(
    probe: _ConcurrencyProbe,
    *,
    hold_seconds: float = 0.0,
    slow_key: str | None = None,
    slow_seconds: float = 0.0,
    fail_first_call: bool = False,
    raise_timeout_keys: frozenset[str] = frozenset(),
) -> httpx.MockTransport:
    """按图片 key 注入延迟/首调失败/超时的并发探针 transport（离线）。"""

    def handler(request: httpx.Request) -> httpx.Response:
        key = _request_image_key(request)
        call_no = probe.enter(key)
        try:
            if key in raise_timeout_keys:
                raise httpx.ReadTimeout("fixture-timeout", request=request)
            hold = slow_seconds if key == slow_key else hold_seconds
            if hold:
                time.sleep(hold)
            if fail_first_call and call_no == 1:
                return httpx.Response(429, json={}, request=request)
            return httpx.Response(200, json=_ocr_ok_body(), request=request)
        finally:
            probe.exit(key)

    return httpx.MockTransport(handler)


def test_run_ocr_batch_serial_default_records_performance(tmp_path: Path) -> None:
    """concurrency=1（默认）：串行语义不变，新运行补性能块与逐图埋点字段。"""
    specs = [
        ("A1", _png_bytes()),
        ("A2", _png_bytes(width=9, height=6)),
        ("A3", _png_bytes(width=8, height=7)),
    ]
    manifest = _write_asset_manifest(tmp_path, specs)
    counter: dict[str, int] = {}
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport(counter, _ocr_ok_body()),
        run_id="ocrnext-serial",
    )
    assert counter["calls"] == 3
    assert record.state_counts == {OcrState.OCR_SUCCEEDED.value: 3}
    perf = record.performance
    assert perf is not None
    assert perf["concurrency"] == 1 and perf["peak_in_flight"] == 1
    assert perf["dispatched"] == 3 and perf["reused"] == 0
    assert perf["retries"] == 0 and perf["http_429"] == 0 and perf["failed"] == 0
    assert [t.asset_id for t in record.tasks] == ["A1", "A2", "A3"]  # 串行保持 manifest 顺序
    for t in record.tasks:
        assert t.queued_at is not None
        assert t.in_flight_at_dispatch == 1
        assert t.attempt_log is not None and t.attempt_log[0]["outcome"] == "http-200"
        assert t.persisted_at is not None and t.total_ms is not None


def test_run_ocr_batch_concurrent8_bounded_out_of_order_sorted_output(tmp_path: Path) -> None:
    """并发 8：峰值不越界、慢图乱序最后完成、输出按稳定任务键排序、原始响应齐全。"""
    specs = [(f"A{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(16)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    slow_tail = _spec_tail(specs[0][1])
    transport = _probe_transport(probe, hold_seconds=0.02, slow_key=slow_tail, slow_seconds=0.3)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        run_id="ocrnext-c8",
        concurrency=8,
    )
    assert len(record.tasks) == 16
    assert all(t.state is OcrState.OCR_SUCCEEDED for t in record.tasks)
    perf = record.performance
    assert perf is not None and perf["concurrency"] == 8
    assert 2 <= perf["peak_in_flight"] <= 8
    assert probe.peak <= 8
    assert probe.completion_order[-1] == slow_tail  # 完成顺序 ≠ 投放顺序
    task_ids = [t.ocr_task_id for t in record.tasks]
    assert task_ids == sorted(task_ids)  # 稳定任务键排序，与完成顺序无关
    run_dir = Path(record.run_dir)
    for t in record.tasks:
        assert t.raw_response_path is not None
        assert (run_dir / Path(t.raw_response_path).name).is_file()


def test_run_ocr_batch_concurrency16_all_succeed(tmp_path: Path) -> None:
    """并发 16：16 张全部成功，峰值观测不超过 16。"""
    specs = [(f"B{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(16)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    transport = _probe_transport(probe, hold_seconds=0.015)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        run_id="ocrnext-c16",
        concurrency=16,
    )
    perf = record.performance
    assert perf is not None and perf["concurrency"] == 16
    assert perf["peak_in_flight"] <= 16 and probe.peak <= 16
    assert probe.peak >= 2
    assert record.state_counts == {OcrState.OCR_SUCCEEDED.value: 16}


def test_run_ocr_batch_concurrent_429_retry_isolated(tmp_path: Path) -> None:
    """并发下 429 只占用该任务自己的重试槽，其余任务继续；重试被埋点计数。"""
    specs = [(f"C{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(8)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    transport = _probe_transport(probe, hold_seconds=0.01, fail_first_call=True)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        config=_fast_config(),
        run_id="ocrnext-429",
        concurrency=8,
    )
    assert record.state_counts == {OcrState.OCR_SUCCEEDED.value: 8}
    perf = record.performance
    assert perf is not None
    assert perf["http_429"] == 1 and perf["retries"] == 1
    assert probe.peak >= 2


def test_run_ocr_batch_concurrent_timeout_failure_isolated(tmp_path: Path) -> None:
    """单图持续超时 → 该图 OCR_FAILED(network_error)，其余任务不受阻塞。"""
    specs = [(f"D{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(8)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    victim_tail = _spec_tail(specs[3][1])
    transport = _probe_transport(
        probe, hold_seconds=0.01, raise_timeout_keys=frozenset({victim_tail})
    )
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        config=_fast_config(),
        run_id="ocrnext-timeout",
        concurrency=4,
    )
    victim = next(t for t in record.tasks if t.asset_id == "D03")
    assert victim.state is OcrState.OCR_FAILED
    assert victim.error_code == "network_error"
    assert victim.attempts == 3
    assert victim.attempt_log is not None
    assert [a["outcome"] for a in victim.attempt_log] == ["ReadTimeout"] * 3
    others = [t for t in record.tasks if t.asset_id != "D03"]
    assert all(t.state is OcrState.OCR_SUCCEEDED for t in others)
    perf = record.performance
    assert perf is not None and perf["http_timeout"] == 3 and perf["failed"] == 1


def test_run_ocr_batch_concurrent_cost_gate_stop_midway(tmp_path: Path) -> None:
    """门禁中途触发：停止投放、保留在飞结果、剩余任务保持 PENDING、检查点原子落盘。"""
    specs = [(f"E{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(12)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    transport = _probe_transport(probe, hold_seconds=0.005)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        config=_fast_config(max_images=5),
        run_id="ocrnext-gate",
        concurrency=4,
    )
    perf = record.performance
    assert perf is not None
    assert perf["limit_hit"] == "max_images" and perf["stop_reason"] == "dispatch"
    # 门禁在完成时计数，FIRST_COMPLETED 粒度下越闸完成量 ∈ [max_images, max_images+并发-1]；
    # 补位提交的任务可能在 worker 入口被门禁重查拦截 → 至多 1 张 OCR_FAILED(cost_gate_hit)，
    # 与串行路径「门禁触发时当前任务标 FAILED」语义一致；剩余全部保持 PENDING。
    succeeded = record.state_counts.get(OcrState.OCR_SUCCEEDED.value, 0)
    failed = record.state_counts.get(OcrState.OCR_FAILED.value, 0)
    assert 5 <= succeeded <= 5 + 3
    assert failed in (0, 1)
    gate_hit = [t for t in record.tasks if t.error_code == "cost_gate_hit"]
    assert len(gate_hit) == failed and all(t.state is OcrState.OCR_FAILED for t in gate_hit)
    assert record.state_counts.get(OcrState.OCR_PENDING.value, 0) == 12 - succeeded - failed
    assert record.cost["total_images"] == succeeded
    assert record.cost["limit_hit"] == "max_images"
    state_path = Path(record.run_dir) / STATE_FILENAME
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(persisted["tasks"]) == 12


def test_run_ocr_batch_concurrent_resume_reuses_completed(tmp_path: Path) -> None:
    """断点续跑（并发调度）：同 run 重跑复用已完成任务，不重复请求。"""
    specs = [(f"F{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(6)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe1 = _ConcurrencyProbe()
    record1 = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_probe_transport(probe1, hold_seconds=0.005),
        run_id="ocrnext-resume",
        concurrency=4,
    )
    assert record1.state_counts == {OcrState.OCR_SUCCEEDED.value: 6}
    counter2: dict[str, int] = {}
    record2 = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport(counter2, _ocr_ok_body()),
        run_id="ocrnext-resume",
        concurrency=4,
    )
    assert counter2.get("calls", 0) == 0  # 全部命中幂等复用，零新请求
    perf2 = record2.performance
    assert perf2 is not None
    assert perf2["dispatched"] == 0 and perf2["reused"] == 6
    assert record2.state_counts == {OcrState.OCR_SUCCEEDED.value: 6}


def test_run_ocr_batch_rejects_invalid_concurrency(tmp_path: Path) -> None:
    """并发档位 fail-closed：仅接受 1/4/8/16（4 为诊断档位）。"""
    manifest = _write_asset_manifest(tmp_path, [("A1", _png_bytes())])
    for bad in (0, 2, 3, 32):
        with pytest.raises(ValueError, match="invalid concurrency"):
            run_ocr_batch(manifest, tmp_path / "out", api_key=FAKE_KEY, concurrency=bad)


def test_call_ocr_one_records_attempt_log_and_perf_fields(tmp_path: Path) -> None:
    """单图埋点：429→200 重试时间线、排队/落盘/总耗时齐全。"""
    img = _png_bytes()
    task = _task(img)
    task.queued_at = utc_now_iso()
    counter: dict[str, int] = {}
    transport = _retry_then_ok_transport(counter, fail_status=429)
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    with httpx.Client(transport=transport) as client:
        returned = call_ocr_one(
            task,
            image_bytes=img,
            config=_fast_config(),
            api_key=FAKE_KEY,
            out_dir=out_dir,
            client=client,
            cost_gate=OcrCostGate(),
        )
    assert returned.attempts == 2
    assert returned.attempt_log is not None
    assert [a["outcome"] for a in returned.attempt_log] == ["http-429", "http-200"]
    assert returned.queue_wait_ms is not None and returned.queue_wait_ms >= 0
    assert returned.request_started_at is not None
    assert returned.response_completed_at is not None
    assert returned.persisted_at is not None and returned.persist_ms is not None
    assert returned.total_ms is not None


def test_run_ocr_batch_abort_preserves_prior_checkpoint_and_resumes(tmp_path: Path) -> None:
    """取消/中止（方案 §7.3.1）：批内异常浮出中止批次——此前批次检查点保留、raw 证据
    在盘，断点续跑幂等重投（重复规模 ≤1 批），已检查点任务零重复请求。

    时序设计：并发 4，首批 G00—G03（G03 延迟 0.15s 后中止异常，确保先有完整检查点
    批次），补位 G04 完成并持久化后，G03 的异常浮出中止批次。
    """
    specs = [
        ("G00", _png_bytes(width=8, height=6)),
        ("G01", _png_bytes(width=8, height=7)),
        ("G02", _png_bytes(width=8, height=8)),
        ("G03", _png_bytes(width=9, height=8)),
        ("G04", _png_bytes(width=8, height=9)),
    ]
    manifest = _write_asset_manifest(tmp_path, specs)
    abort_tail = _spec_tail(specs[3][1])
    probe = _ConcurrencyProbe()

    def abort_handler(request: httpx.Request) -> httpx.Response:
        key = _request_image_key(request)
        probe.enter(key)
        try:
            if key == abort_tail:
                time.sleep(0.15)
                raise RuntimeError("fixture-abort")
            return httpx.Response(200, json=_ocr_ok_body(), request=request)
        finally:
            probe.exit(key)

    with pytest.raises(RuntimeError, match="fixture-abort"):
        run_ocr_batch(
            manifest,
            tmp_path / "out",
            api_key=FAKE_KEY,
            transport=httpx.MockTransport(abort_handler),
            run_id="ocrnext-abort",
            concurrency=4,
        )
    # 此前批次检查点保留：G00—G02、G04 均已持久化为 SUCCEEDED（G03 仍 RUNNING）
    run_dir = tmp_path / "out" / "run_ocrnext-abort"
    checkpoint = json.loads((run_dir / STATE_FILENAME).read_text(encoding="utf-8"))
    persisted_states = {t["asset_id"]: t["state"] for t in checkpoint["tasks"]}
    assert persisted_states.get("G00") == OcrState.OCR_SUCCEEDED.value
    assert persisted_states.get("G04") == OcrState.OCR_SUCCEEDED.value
    # raw 证据在盘
    assert len(list(run_dir.glob("raw_response_*.json"))) == 4
    # 断点续跑：已检查点任务复用零重复，仅中止任务幂等重投
    counter2: dict[str, int] = {}
    record2 = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport(counter2, _ocr_ok_body()),
        run_id="ocrnext-abort",
        concurrency=4,
    )
    assert record2.state_counts == {OcrState.OCR_SUCCEEDED.value: 5}
    assert counter2.get("calls", 0) == 1  # 仅 G03 重投；G00—G02、G04 复用
    perf2 = record2.performance
    assert perf2 is not None and perf2["reused"] == 4


# ---------------------------------------------------------------------------
# change ocr-concurrency-optimization 任务 2.1—2.4 回归（2026-08-30）
# ---------------------------------------------------------------------------


def test_run_ocr_batch_connection_pool_limits_match_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """任务 2.1：HTTP 连接池按并发档位显式配置（max/keepalive 与并发数对齐）。"""
    import compsval.ingest.floorplan_ocr as ocr_mod

    captured: dict[str, httpx.Limits] = {}
    original_client = ocr_mod.httpx.Client

    class SpyClient(original_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["limits"] = kwargs.get("limits")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(ocr_mod.httpx, "Client", SpyClient)
    manifest = _write_asset_manifest(tmp_path, [("H1", _png_bytes())])
    for concurrency in (1, 4, 8, 16):
        run_ocr_batch(
            manifest,
            tmp_path / f"out-{concurrency}",
            api_key=FAKE_KEY,
            transport=_json_transport({}, _ocr_ok_body()),
            run_id=f"pool-{concurrency}",
            concurrency=concurrency,
        )
        limits = captured["limits"]
        assert limits is not None
        assert limits.max_connections == concurrency
        assert limits.max_keepalive_connections == concurrency


def test_run_ocr_batch_resume_archives_previous_run_json(tmp_path: Path) -> None:
    """任务 2.2：断点续跑前归档旧 ocr_run.json，旧时间线证据字节保留、新记录另写。"""
    specs = [(f"I{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(4)]
    manifest = _write_asset_manifest(tmp_path, specs)
    record1 = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport({}, _ocr_ok_body()),
        run_id="ocrnext-archive",
        concurrency=4,
    )
    run_dir = Path(record1.run_dir)
    first_run_json = (run_dir / "ocr_run.json").read_text(encoding="utf-8")
    record2 = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport({}, _ocr_ok_body()),
        run_id="ocrnext-archive",
        concurrency=4,
    )
    run_dir2 = Path(record2.run_dir)
    archives = sorted(run_dir2.glob("ocr_run.previous_*.json"))
    assert len(archives) == 1
    assert archives[0].read_text(encoding="utf-8") == first_run_json  # 旧记录字节不变
    assert (run_dir2 / "ocr_run.json").read_text(encoding="utf-8") != first_run_json
    perf2 = record2.performance
    assert perf2 is not None and perf2["reused"] == 4 and perf2["dispatched"] == 0


def test_run_ocr_batch_cost_derived_from_tasks_matches_snapshot(tmp_path: Path) -> None:
    """任务 2.3：cost 附逐任务求和派生块，与门禁快照一致、Token 可回指任务。"""
    specs = [(f"J{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(6)]
    manifest = _write_asset_manifest(tmp_path, specs)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=_json_transport({}, _ocr_ok_body()),
        run_id="ocrnext-derive",
        concurrency=8,
    )
    derived = record.cost["derived_from_tasks"]
    assert derived["total_images"] == 6
    assert derived["total_prompt_tokens"] == sum(t.prompt_tokens or 0 for t in record.tasks)
    assert derived["total_completion_tokens"] == sum(t.completion_tokens or 0 for t in record.tasks)
    # 派生口径与门禁快照（同一批成功任务）一致
    assert derived["total_cost_yuan"] == record.cost["total_cost_yuan"]
    assert derived["total_images"] == record.cost["total_images"]


def test_run_ocr_batch_performance_attempt_outcomes_distribution(tmp_path: Path) -> None:
    """任务 2.4：性能块含 attempt 级 HTTP 状态/异常类别分布与落盘分位。"""
    specs = [(f"K{i:02d}", _png_bytes(width=8, height=6 + i)) for i in range(8)]
    manifest = _write_asset_manifest(tmp_path, specs)
    probe = _ConcurrencyProbe()
    transport = _probe_transport(probe, hold_seconds=0.01, fail_first_call=True)
    record = run_ocr_batch(
        manifest,
        tmp_path / "out",
        api_key=FAKE_KEY,
        transport=transport,
        config=_fast_config(),
        run_id="ocrnext-outcomes",
        concurrency=8,
    )
    perf = record.performance
    assert perf is not None
    outcomes = perf["attempt_outcomes"]
    assert outcomes.get("http-429", 0) == 1
    assert outcomes.get("http-200", 0) == 8
    assert perf["http_429"] == 1
    assert "p50_persist_ms" in perf and "p95_persist_ms" in perf
    assert perf["peak_in_flight"] >= 2
