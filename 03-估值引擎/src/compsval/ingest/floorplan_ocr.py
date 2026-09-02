"""Qwen OCR 请求器与运行记录（EXTFP3-B，技术方案 §8.3/§9.2/§11.3/§12/§15/§7.2）。

本模块实现 ``compsval floorplan ocr`` 的请求器与运行记录层，冻结以下行为：

1. **幂等键**（§8.3）：``ocr_task_id = SHA256(image_sha256 + model_id + task + params
   + parser_version)``；同 run 已产出完整成功产物的任务按幂等键命中直接跳过（断点续跑）。
2. **Base64 Data URL**（§9.2）：MIME 与真实文件格式一致（由字节嗅探或资产 manifest 的
   ``mime_type`` 决定），不直接把链家 URL 交给 Qwen；``image_sha256`` 写入运行记录。
3. **请求哈希**（§7.2/§15）：``request_hash`` 为不含密钥与 Base64 的规范化请求哈希。
4. **非流式 + finish_reason=length → OCR_PARTIAL**（§9.2），不得当作成功。
5. **有界重试/退避**（§11.3）：429/408/5xx 与超时/连接类指数退避重试；其余 4xx 不重试；
   单图有界（``OCR_SINGLE_IMAGE_MAX_ATTEMPTS`` 含首次），全局重试门禁由 ``OcrCostGate`` 强制。
6. **运行时成本与数量门禁**（§11.3）：每张图请求前 ``OcrCostGate.check_before_request``，
   每张图结束后 ``record_usage`` 累计；任一上限触发即 fail-closed 停止，不透支。
7. **运行记录**（§7.2 ``floorplan_ocr_run``）：redacted ``request_hash``、``provider_request_id``、
   Token 用量、``response_status``/``error_code``；原始响应落盘 + SHA256。
8. **状态机**（§7.5）：``OCR_PENDING → OCR_RUNNING → {OCR_SUCCEEDED, OCR_PARTIAL,
   OCR_FAILED, NEEDS_REVIEW}``；终态不可回退；原始响应含密钥/Base64 回显时按 §15
   fail-closed 标记 ``NEEDS_REVIEW`` 且不落盘。

范围边界（EXTFP3-B 合同）：本模块只做**请求器 + 运行记录 + 离线 mock 测试**，默认测试全部离线，
绝不发出真实 HTTP；真实付费调用属 EXTFP3-F（10 张调试）与 EXTFP3-H（300 张验收）。解析原始响应
到逐词表属 EXTFP3-C，本模块只保存原始响应与元字段。
"""

from __future__ import annotations

import base64
import hashlib
import json
import random
import re
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field

from compsval.ingest.floorplan_asset import (
    AssetStatus,
    FloorplanAssetRun,
    sniff_image,
)
from compsval.ingest.floorplan_ocr_contract import (
    DASHSCOPE_API_KEY_ENV,
    OCR_MODEL_ID,
    OCR_REGION,
    OCR_RUN_CONFIG_SCHEMA_VERSION,
    OCR_TASK,
    REQUEST_CONTRACT_V1,
    REQUEST_CONTRACT_VERSION,
    OcrCostGate,
    OcrRunConfig,
    OcrUsageRecord,
    audit_no_secrets,
    estimate_image_tokens,
    read_dashscope_api_key,
    utc_now_iso,
)

# 请求器版本：写入每条运行记录。OCRNEXT-B：新增有界并发调度、单写入者聚合与性能埋点；
# 版本升级只标识请求器实现，不参与 ocr_task_id / request_hash（合同 §4）。
OCR_REQUESTER_VERSION = "OCRNEXT-B-REQ-1.0"

# 并发档位（OCRNEXT-WP0 合同 §2）：1 为串行兼容基线，4 仅诊断档位；验证与验收只走 1/8/16。
OCR_CONCURRENCY_CHOICES: tuple[int, ...] = (1, 4, 8, 16)

# 解析器版本：ocr_task_id 公式的一部分（技术方案 §8.3）。
# EXTFP3-B 阶段无解析器，先置占位；EXTFP3-D 冻结确定性转录解析器后更新为最终版本。
OCR_PARSER_VERSION = "EXTFP3-B-NO-PARSER"

# 幂等键分隔符（保证拼接无歧义；幂等键源于 SHA256，分隔符只影响确定性输入）
_KEY_SEP = "|"

# 运行记录/状态文件名（断点续跑锚点，位于输出目录）
RUN_FILENAME = "ocr_run.json"
STATE_FILENAME = "ocr_state.json"

# 原始响应文件名前缀（<prefix>_<ocr_task_id 截断>.json）
RAW_RESPONSE_PREFIX = "raw_response"

# 原始响应文件名中保留的 ocr_task_id 前缀长度。ocr_task_id 为 64 位 SHA256 hex，截断到 24 hex
# （96 bit）在 300 张规模下碰撞概率可忽略；文件名仅作 run 目录内唯一键，完整性由任务记录的
# ocr_task_id（全 64 hex）/ raw_response_path / raw_response_sha256 承担。
# EXTFP3-H#MAX_PATH：完整 `raw_response_<64hex>.json` 在 force-new-run 长 run_id 下使
# `.incomplete` 中间文件路径达 263 字符，超过 Windows MAX_PATH（260），H9 复跑落盘失败。
RAW_RESPONSE_TASK_ID_TRUNC = 24


def raw_response_filename(task_id: str) -> str:
    """原始响应文件名：run 目录内唯一且把全路径压回 MAX_PATH 约束内。"""
    return f"{RAW_RESPONSE_PREFIX}_{task_id[:RAW_RESPONSE_TASK_ID_TRUNC]}.json"


# 单图最大请求次数（含首次，即最多重试 2 次；与 RV-EXTFP3-A-01#F2 对齐：单图有界重试 +
# 全局重试门禁由 OcrCostGate 强制，两条防线共同防「不因 API 失败无限重试」§11.3）
OCR_SINGLE_IMAGE_MAX_ATTEMPTS = 3

# 单任务指数退避默认参数（§11.3，与下载器一致）
DEFAULT_BASE_BACKOFF = 1.0
DEFAULT_MAX_BACKOFF = 30.0

# provider 标识（§7.2）
PROVIDER = "aliyun_model_studio"

# 请求/响应超时（秒）
DEFAULT_OCR_TIMEOUT = 60.0


class OcrState(StrEnum):
    """OCR 阶段状态机（技术方案 §7.5）：显式状态，不用布尔掩盖失败原因。

    状态迁移白名单（不可回退）：
        OCR_PENDING   -> {OCR_RUNNING, OCR_FAILED}
        OCR_RUNNING   -> {OCR_SUCCEEDED, OCR_PARTIAL, OCR_FAILED, NEEDS_REVIEW}
        OCR_SUCCEEDED / OCR_PARTIAL / OCR_FAILED / NEEDS_REVIEW -> {}  # 终态
    """

    OCR_PENDING = "OCR_PENDING"
    OCR_RUNNING = "OCR_RUNNING"
    OCR_SUCCEEDED = "OCR_SUCCEEDED"
    OCR_PARTIAL = "OCR_PARTIAL"
    OCR_FAILED = "OCR_FAILED"
    NEEDS_REVIEW = "NEEDS_REVIEW"


_STATE_TRANSITIONS: dict[OcrState, frozenset[OcrState]] = {
    OcrState.OCR_PENDING: frozenset({OcrState.OCR_RUNNING, OcrState.OCR_FAILED}),
    OcrState.OCR_RUNNING: frozenset(
        {
            OcrState.OCR_SUCCEEDED,
            OcrState.OCR_PARTIAL,
            OcrState.OCR_FAILED,
            OcrState.NEEDS_REVIEW,
        }
    ),
    OcrState.OCR_SUCCEEDED: frozenset(),
    OcrState.OCR_PARTIAL: frozenset(),
    OcrState.OCR_FAILED: frozenset(),
    OcrState.NEEDS_REVIEW: frozenset(),
}

TERMINAL_OCR_STATES: frozenset[OcrState] = frozenset(
    {
        OcrState.OCR_SUCCEEDED,
        OcrState.OCR_PARTIAL,
        OcrState.OCR_FAILED,
        OcrState.NEEDS_REVIEW,
    }
)


def transition(task: OcrTaskRecord, to: OcrState) -> None:
    """状态按白名单迁移；任务进终态后不可回退。非法迁移抛出 ValueError。"""
    if to not in _STATE_TRANSITIONS[task.state]:
        raise ValueError(
            f"invalid state transition {task.state.value} -> {to.value} "
            f"(ocr_task_id={task.ocr_task_id!r})"
        )
    task.state = to


# ---------------------------------------------------------------------------
# 幂等键 / 请求哈希 / Data URL（技术方案 §8.3 / §9.2 / §7.2 / §15）
# ---------------------------------------------------------------------------


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_ocr_task_id(
    image_sha256: str,
    *,
    model: str = OCR_MODEL_ID,
    task: str = OCR_TASK,
    params: dict[str, Any] | None = None,
    parser_version: str = OCR_PARSER_VERSION,
) -> str:
    """ocr_task_id = SHA256(image_sha256 + model_id + task + params + parser_version)（§8.3）。

    ``params`` 用排序后 JSON 规范化，保证相同参数产生相同键。不含密钥与 Base64。
    """
    params_json = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
    content = _KEY_SEP.join([image_sha256, model, task, params_json, parser_version])
    return _sha256_text(content)


def compute_request_hash(
    *,
    image_sha256: str,
    model: str = OCR_MODEL_ID,
    task: str = OCR_TASK,
    min_pixels: int,
    max_pixels: int,
    enable_rotate: bool,
    stream: bool,
    parser_version: str = OCR_PARSER_VERSION,
) -> str:
    """规范化请求哈希（§7.2/§15）：不含密钥与 Base64 的确定性哈希。"""
    canonical = json.dumps(
        {
            "image_sha256": image_sha256,
            "model": model,
            "task": task,
            "min_pixels": min_pixels,
            "max_pixels": max_pixels,
            "enable_rotate": enable_rotate,
            "stream": stream,
            "parser_version": parser_version,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return _sha256_text(canonical)


def build_image_data_url(data: bytes, mime_type: str | None = None) -> str:
    """构造 Base64 Data URL（§9.2）；MIME 必须为 image/*，缺失时回退字节嗅探。

    ``mime_type`` 为空或非 image/* 时，用 ``sniff_image`` 由真实字节决定；仍无法决定则抛错
    fail-closed（不把 MIME 不明确的图片发给 Qwen）。
    """
    mime = mime_type
    if not mime or not mime.startswith("image/"):
        detected = sniff_image(data)
        if detected is None:
            raise ValueError("图片字节无法识别 MIME；拒绝构造 Data URL")
        mime = detected.mime_type
    return f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"


def _canonical_ocr_params() -> dict[str, Any]:
    """冻结合同的请求参数子集（用于幂等键，技术方案 §8.3 的 params）。"""
    c = REQUEST_CONTRACT_V1
    return {
        "min_pixels": c.min_pixels,
        "max_pixels": c.max_pixels,
        "enable_rotate": c.enable_rotate,
        "stream": c.stream,
    }


# ---------------------------------------------------------------------------
# 响应元数据提取（原生 DashScope multimodal-generation 接口；容错解析）
# ---------------------------------------------------------------------------


def extract_response_metadata(body: dict[str, Any]) -> dict[str, Any]:
    """从原始响应 JSON 提取运行记录元字段（§7.2）。

    兼容原生接口（``output.choices[]`` / ``usage.input_tokens/output_tokens`` /
    ``request_id``）与 OpenAI 兼容格式（``choices[]`` / ``model`` / ``id``）。
    无法定位的字段返回 None（真实运行 EXTFP3-F 会揭示实际结构；缺失不静默伪造）。
    """
    choices: list[Any] = []
    output = body.get("output")
    if isinstance(output, dict) and isinstance(output.get("choices"), list):
        choices = output["choices"]
    elif isinstance(body.get("choices"), list):
        choices = body["choices"]

    finish_reason: str | None = None
    if choices:
        first = choices[0]
        if isinstance(first, dict):
            finish_reason = first.get("finish_reason")
            if not isinstance(finish_reason, str):
                finish_reason = None

    usage = body.get("usage")
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    if isinstance(usage, dict):
        prompt_tokens = usage.get("input_tokens") or usage.get("prompt_tokens")
        completion_tokens = usage.get("output_tokens") or usage.get("completion_tokens")
    if not isinstance(prompt_tokens, int):
        prompt_tokens = None
    if not isinstance(completion_tokens, int):
        completion_tokens = None

    model_returned: str | None = None
    for candidate in (
        body.get("model"),
        output.get("model") if isinstance(output, dict) else None,
        choices[0].get("model") if choices and isinstance(choices[0], dict) else None,
    ):
        if isinstance(candidate, str) and candidate:
            model_returned = candidate
            break

    request_id: str | None = None
    for candidate in (
        body.get("request_id"),
        body.get("id"),
        output.get("request_id") if isinstance(output, dict) else None,
    ):
        if isinstance(candidate, str) and candidate:
            request_id = candidate
            break

    return {
        "model_returned": model_returned,
        "provider_request_id": request_id,
        "finish_reason": finish_reason,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
    }


# ---------------------------------------------------------------------------
# 重试 / 退避（技术方案 §11.3 / §8.2 语义复用）
# ---------------------------------------------------------------------------


def _is_retryable_status(status: int) -> bool:
    """429/408 与 5xx 重试；其余 4xx 不重试（§8.2）。"""
    return status in (408, 429) or 500 <= status < 600


def _is_retryable_exception(exc: Exception) -> bool:
    """超时与连接类错误重试（§8.2）。"""
    return isinstance(exc, (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError))


def _backoff_seconds(base: float, attempt: int, max_backoff: float) -> float:
    """指数退避 + 随机抖动：base * 2^(attempt-1)，封顶 max_backoff，±25% 抖动。"""
    pow_n: int = max(0, attempt - 1)
    raw: float = base * float(2**pow_n)
    capped: float = min(raw, max_backoff)
    jitter: float = capped * 0.25
    return float(capped + random.uniform(-jitter, jitter))


# ---------------------------------------------------------------------------
# 数据模型（技术方案 §7.2 floorplan_ocr_run）
# ---------------------------------------------------------------------------


class OcrTaskRecord(BaseModel):
    """一条图片的 OCR 任务记录（§7.2 字段，EXTFP3-B-REQ-1.0）。

    只保存非敏感配置与请求哈希；绝不保存密钥、Base64 或完整敏感响应体。
    """

    ocr_task_id: str
    ocr_run_id: str
    asset_id: str
    image_sha256: str
    image_path: str | None = Field(default=None, description="本地原图相对路径（不落敏感信息）")
    width: int | None = None
    height: int | None = None
    mime_type: str | None = None
    provider: str = PROVIDER
    region: str = OCR_REGION
    model_requested: str = OCR_MODEL_ID
    model_returned: str | None = None
    task: str = OCR_TASK
    request_contract_version: str = REQUEST_CONTRACT_VERSION
    parser_version: str = OCR_PARSER_VERSION
    request_hash: str
    provider_request_id: str | None = None
    state: OcrState = OcrState.OCR_PENDING
    attempts: int = 0
    started_at: str | None = None
    completed_at: str | None = None
    finish_reason: str | None = None
    prompt_tokens: int | None = None
    image_tokens: int | None = None
    completion_tokens: int | None = None
    response_status: str | None = Field(default=None, description="如 http-200 / network-timeout")
    error_code: str | None = Field(default=None, description="业务错误码，如 finish_reason_length")
    raw_response_path: str | None = None
    raw_response_sha256: str | None = None
    # 双口径登记（change extfp4-verify-followups）：raw_response_sha256 保持原始字节口径
    # （历史可比）；raw_response_file_sha256 登记落盘实际字节（净化文本 + Windows CRLF 翻译），
    # 完整性/一致性门禁以落盘口径为主核对、与原始口径并列披露。历史运行记录该字段为 None。
    raw_response_file_sha256: str | None = None
    latency_ms: int | None = None
    # ---- OCRNEXT-B 性能埋点（方案 §5.4）：只由 OCRNEXT-B-REQ-1.0 起的新运行填写；
    # 旧运行记录按只读边界不改写，续跑复用的旧任务这些字段保持 None。 ----
    queued_at: str | None = Field(default=None, description="调度器投放时刻")
    in_flight_at_dispatch: int | None = Field(default=None, description="投放时的在飞请求数")
    request_started_at: str | None = Field(default=None, description="首次请求开始时刻")
    response_completed_at: str | None = Field(default=None, description="末次响应完成时刻")
    persisted_at: str | None = Field(default=None, description="原始响应落盘完成时刻")
    queue_wait_ms: int | None = Field(default=None, description="投放→首次请求开始")
    persist_ms: int | None = Field(default=None, description="原始响应落盘耗时")
    total_ms: int | None = Field(default=None, description="投放→落盘完成总耗时")
    attempt_log: list[dict[str, Any]] | None = Field(
        default=None, description="逐次尝试时间线（attempt/started_at/completed_at/outcome）"
    )


class OcrRunRecord(BaseModel):
    """一次 OCR 运行的总记录（断点续跑与复核证据，EXTFP3-B-REQ-1.0）。"""

    ocr_run_id: str
    requester_version: str = OCR_REQUESTER_VERSION
    config_schema_version: str = OCR_RUN_CONFIG_SCHEMA_VERSION
    contract_version: str = REQUEST_CONTRACT_VERSION
    asset_manifest_ref: str = Field(description="资产 manifest 的 manifest_ref（或路径/哈希）")
    sourced: bool = Field(description="来源快照是否可追溯")
    state_counts: dict[str, int] = Field(default_factory=dict)
    cost: dict[str, Any] = Field(default_factory=dict)
    performance: dict[str, Any] | None = Field(
        default=None, description="OCRNEXT-B 批次性能块（并发/峰值/P50/P95/429/超时/重试等）"
    )
    tasks: list[OcrTaskRecord] = Field(default_factory=list)
    created_at: str
    updated_at: str
    run_dir: str = Field(description="本运行实际输出目录")


# ---------------------------------------------------------------------------
# 单任务请求（重试 / 状态机 / 原始响应落盘 / 成本回填）
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, text: str) -> None:
    """原子写盘：先写 .incomplete 再 replace。"""
    work = path.with_name(path.name + ".incomplete")
    work.write_text(text, encoding="utf-8")
    work.replace(path)


def _persist(record: OcrRunRecord) -> None:
    """把运行记录原子写盘到 ocr_run.json 与 ocr_state.json（断点续跑锚点）。"""
    run_dir = Path(record.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    record.updated_at = datetime.now(UTC).isoformat()
    text = record.model_dump_json(indent=2)
    _atomic_write(run_dir / RUN_FILENAME, text)
    _atomic_write(run_dir / STATE_FILENAME, text)


def _load_previous(state_path: Path) -> dict[str, OcrTaskRecord]:
    """读取同 run 既有 ocr_state.json，供断点续跑按幂等键复用已完成任务。"""
    resume: dict[str, OcrTaskRecord] = {}
    if not state_path.is_file():
        return resume
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return resume
    if not isinstance(data, dict):
        return resume
    for raw in data.get("tasks", []) or []:
        try:
            t = OcrTaskRecord.model_validate(raw)
        except Exception:  # noqa: BLE001 - 历史损坏记录跳过续跑
            continue
        resume[t.ocr_task_id] = t
    return resume


def _archive_previous_run(run_dir: Path) -> str | None:
    """断点续跑覆盖前归档既有 ocr_run.json（任务 2.2，change ocr-concurrency-optimization）。

    同 run_id 续跑会重写 ``ocr_run.json``；重写前先把旧文件移为带 UTC 时间戳的归档
    ``ocr_run.previous_<ts>.json``，保留旧时间线证据（旧记录字节不被改写，仅改名），
    断点恢复仍从 ``ocr_state.json`` 读取不受影响。不存在旧文件时返回 None。
    """
    src = run_dir / RUN_FILENAME
    if not src.is_file():
        return None
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dst = run_dir / f"ocr_run.previous_{ts}.json"
    if dst.exists():  # 同秒续跑两次的极小概率碰撞：追加序号避免覆盖
        dst = run_dir / f"ocr_run.previous_{ts}_{random.randrange(1000)}.json"
    src.replace(dst)
    return dst.as_posix()


def call_ocr_one(
    task: OcrTaskRecord,
    *,
    image_bytes: bytes,
    config: OcrRunConfig,
    api_key: str,
    out_dir: Path,
    client: httpx.Client,
    cost_gate: OcrCostGate,
    max_attempts: int = OCR_SINGLE_IMAGE_MAX_ATTEMPTS,
    timeout: float = DEFAULT_OCR_TIMEOUT,
) -> OcrTaskRecord:
    """单张图片的完整 OCR 请求（重试 + 状态机 + 原始响应落盘 + 成本回填）。

    请求前由调用方（``run_ocr_batch``）已保证 ``cost_gate.check_before_request()`` 通过；
    本函数结束时无论成败都 ``record_usage`` 一次（成功按实际 usage，失败按 0 Token），
    使全局图片数 / Token / 重试 / 金额门禁全部累计。不抛网络异常（内部已归类失败）。
    """
    if not cost_gate.check_before_request():
        task.error_code = "cost_gate_hit"
        if task.state is OcrState.OCR_PENDING:
            transition(task, OcrState.OCR_FAILED)
        return task

    transition(task, OcrState.OCR_RUNNING)
    task.started_at = utc_now_iso()
    task.request_started_at = task.started_at
    if task.queued_at:
        task.queue_wait_ms = _iso_delta_ms(task.queued_at, task.started_at)
    started_ns = time.perf_counter_ns()
    attempt_log: list[dict[str, Any]] = []

    body = REQUEST_CONTRACT_V1.request_body(
        image_data_url=build_image_data_url(image_bytes, task.mime_type)
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "X-DashScope-DataInspection": "enable",
    }

    last_error: str | None = None
    success_response: httpx.Response | None = None

    for attempt in range(1, max_attempts + 1):  # 含首次共 max_attempts 次请求
        task.attempts = attempt
        attempt_started = utc_now_iso()
        try:
            response = client.post(
                REQUEST_CONTRACT_V1.endpoint,
                json=body,
                headers=headers,
                timeout=timeout,
            )
        except httpx.HTTPError as exc:
            attempt_log.append(
                {
                    "attempt": attempt,
                    "started_at": attempt_started,
                    "completed_at": utc_now_iso(),
                    "outcome": type(exc).__name__,
                }
            )
            _stamp_attempt_log(task, attempt_log)
            last_error = type(exc).__name__
            if _is_retryable_exception(exc) and attempt < max_attempts:
                time.sleep(
                    _backoff_seconds(config.cost.base_backoff, attempt, config.cost.max_backoff)
                )
                continue
            task.response_status = f"network-{last_error}"
            task.error_code = "network_error"
            task.latency_ms = _elapsed_ms(started_ns)
            transition(task, OcrState.OCR_FAILED)
            _record_usage_for(cost_gate, task)
            return task

        attempt_log.append(
            {
                "attempt": attempt,
                "started_at": attempt_started,
                "completed_at": utc_now_iso(),
                "outcome": f"http-{response.status_code}",
            }
        )
        _stamp_attempt_log(task, attempt_log)

        if 200 <= response.status_code < 300:
            success_response = response
            break

        last_error = f"http-{response.status_code}"
        if _is_retryable_status(response.status_code) and attempt < max_attempts:
            time.sleep(_backoff_seconds(config.cost.base_backoff, attempt, config.cost.max_backoff))
            continue
        task.response_status = last_error
        task.error_code = "http_error"
        task.latency_ms = _elapsed_ms(started_ns)
        transition(task, OcrState.OCR_FAILED)
        _record_usage_for(cost_gate, task)
        return task

    assert success_response is not None  # 循环内已 return 或 break 到此处必有成功响应
    task.latency_ms = _elapsed_ms(started_ns)

    raw_bytes = success_response.content
    try:
        body_json = json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        task.response_status = "http-200"
        task.error_code = "unparseable_response"
        transition(task, OcrState.NEEDS_REVIEW)
        _record_usage_for(cost_gate, task)
        return task

    # §15：原始响应落盘前检查是否回显密钥或 Base64 图片；泄露则 fail-closed 不落盘
    text_view = raw_bytes.decode("utf-8", errors="replace")
    if not audit_no_secrets(text_view, api_key=api_key):
        task.response_status = "http-200"
        task.error_code = "sensitive_content_in_response"
        transition(task, OcrState.NEEDS_REVIEW)
        _record_usage_for(cost_gate, task)
        return task

    meta = extract_response_metadata(body_json)
    task.model_returned = meta["model_returned"]
    task.provider_request_id = meta["provider_request_id"]
    task.finish_reason = meta["finish_reason"]
    task.prompt_tokens = meta["prompt_tokens"]
    task.completion_tokens = meta["completion_tokens"]
    if task.width and task.height:
        task.image_tokens = estimate_image_tokens(task.width, task.height)
    task.response_status = "http-200"

    # 原始响应落盘（完整证据 + 哈希；文件名用截断 task_id 保持路径短于 MAX_PATH）
    raw_path = out_dir / raw_response_filename(task.ocr_task_id)
    persist_started_ns = time.perf_counter_ns()
    _atomic_write(raw_path, raw_bytes.decode("utf-8", errors="replace"))
    task.raw_response_path = raw_path.as_posix()
    task.raw_response_sha256 = _sha256_bytes(raw_bytes)
    # 双口径登记（change extfp4-verify-followups）：raw_response_sha256 为原始字节口径
    # （历史可比）；raw_response_file_sha256 为落盘实际字节（净化文本 + CRLF 翻译后），
    # 原子写完成后读取，供完整性/一致性门禁以落盘口径为主核对。
    task.raw_response_file_sha256 = _sha256_bytes(raw_path.read_bytes())
    task.persisted_at = utc_now_iso()
    task.persist_ms = _elapsed_ms(persist_started_ns)
    if task.queued_at and task.persisted_at:
        task.total_ms = _iso_delta_ms(task.queued_at, task.persisted_at)

    # 状态迁移：finish_reason=length 视为部分成功；模型不一致则需复核（§19.2 整批停止由编排层执行）
    if task.finish_reason == "length":
        task.error_code = "finish_reason_length"
        transition(task, OcrState.OCR_PARTIAL)
    elif task.model_returned and task.model_returned != task.model_requested:
        task.error_code = "model_mismatch"
        transition(task, OcrState.NEEDS_REVIEW)
    else:
        transition(task, OcrState.OCR_SUCCEEDED)

    task.completed_at = utc_now_iso()
    _record_usage_for(cost_gate, task)
    return task


def _elapsed_ms(started_ns: int) -> int:
    return int((time.perf_counter_ns() - started_ns) // 1_000_000)


def _stamp_attempt_log(task: OcrTaskRecord, attempt_log: list[dict[str, Any]]) -> None:
    """把逐次尝试时间线回填到任务记录；末次完成时刻即末次响应/异常完成时刻。"""
    task.attempt_log = attempt_log
    if attempt_log:
        task.response_completed_at = attempt_log[-1]["completed_at"]


def _iso_delta_ms(start_iso: str, end_iso: str) -> int | None:
    """两个 ISO 时刻的毫秒差（解析失败返回 None，不伪造）。"""
    try:
        start = datetime.fromisoformat(start_iso)
        end = datetime.fromisoformat(end_iso)
    except ValueError:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def _percentile(sorted_values: list[int], percentile: int) -> int | None:
    """最近序统计位分位数；空集返回 None（不伪造 0）。输入必须已升序。"""
    if not sorted_values:
        return None
    rank = max(1, -(-percentile * len(sorted_values) // 100))
    return sorted_values[min(rank, len(sorted_values)) - 1]


def _count_states(tasks: list[OcrTaskRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in tasks:
        counts[t.state.value] = counts.get(t.state.value, 0) + 1
    return counts


def _aggregate_and_persist(
    record: OcrRunRecord, tasks: list[OcrTaskRecord], cost_gate: OcrCostGate
) -> None:
    """聚合状态计数与成本快照并原子持久化（并发路径的批次检查点）。"""
    record.tasks = tasks
    record.state_counts = _count_states(tasks)
    # 任务 2.3：cost 保留门禁快照，并附 tasks 逐条求和派生块供报告交叉核对（不改历史）
    record.cost = {
        **cost_gate.snapshot(),
        "derived_from_tasks": _derive_cost_from_tasks(tasks),
    }
    _persist(record)


def _derive_cost_from_tasks(tasks: list[OcrTaskRecord]) -> dict[str, Any]:
    """从任务记录逐条求和的成本/用量派生块（任务 2.3，change ocr-concurrency-optimization）。

    OcrCostGate.snapshot() 为门禁口径；本派生块按 tasks 逐条重算（提示/完成 Token、
    图片数、重试数、按官方单价的费用），供报告交叉核对。只写入新运行记录，不改写
    任何历史 ocr_run.json 聚合块。
    """
    prompt = sum(t.prompt_tokens or 0 for t in tasks)
    completion = sum(t.completion_tokens or 0 for t in tasks)
    images = sum(1 for t in tasks if (t.prompt_tokens or 0) > 0 or t.attempts > 0)
    retries = sum(max(0, (t.attempts or 0) - 1) for t in tasks)
    cost = round(
        prompt * 0.3 / 1_000_000 + completion * 0.5 / 1_000_000, 6
    )  # 官方单价：输入 0.3 / 输出 0.5 元每百万 Token
    return {
        "total_images": images,
        "total_prompt_tokens": prompt,
        "total_completion_tokens": completion,
        "total_tokens": prompt + completion,
        "total_retries": retries,
        "total_cost_yuan": cost,
    }


def _build_performance(
    *,
    concurrency: int,
    peak_in_flight: int,
    reused: int,
    dispatched: list[OcrTaskRecord],
    state_counts: dict[str, int],
    cost_gate: OcrCostGate,
    wall_ms: int,
    stop_reason: str | None,
) -> dict[str, Any]:
    """批次性能块（方案 §5.4）：并发/峰值、分位数、429/超时/重试/部分/失败与门禁结果。

    任务 2.4（change ocr-concurrency-optimization）：补 attempt 级 HTTP 状态与异常类别
    分布（attempt_outcomes）与落盘耗时（persist）分位，完善延迟分解口径。
    """
    total_values = sorted(t.total_ms for t in dispatched if t.total_ms is not None)
    request_values = sorted(t.latency_ms for t in dispatched if t.latency_ms is not None)
    persist_values = sorted(t.persist_ms for t in dispatched if t.persist_ms is not None)
    attempt_outcomes: dict[str, int] = {}
    for t in dispatched:
        for entry in t.attempt_log or []:
            outcome = entry.get("outcome")
            if outcome is not None:
                attempt_outcomes[str(outcome)] = attempt_outcomes.get(str(outcome), 0) + 1
    outcomes = [str(entry.get("outcome")) for t in dispatched for entry in (t.attempt_log or [])]
    return {
        "concurrency": concurrency,
        "peak_in_flight": peak_in_flight,
        "dispatched": len(dispatched),
        "reused": reused,
        "wall_ms": wall_ms,
        "p50_total_ms": _percentile(total_values, 50),
        "p95_total_ms": _percentile(total_values, 95),
        "p50_request_ms": _percentile(request_values, 50),
        "p95_request_ms": _percentile(request_values, 95),
        "p50_persist_ms": _percentile(persist_values, 50),
        "p95_persist_ms": _percentile(persist_values, 95),
        "http_429": outcomes.count("http-429"),
        "http_timeout": sum(
            1 for o in outcomes if o in ("http-408", "ReadTimeout", "ConnectTimeout")
        ),
        "attempt_outcomes": dict(sorted(attempt_outcomes.items())),
        "retries": sum(max(0, (t.attempts or 0) - 1) for t in dispatched),
        "ocr_partial": state_counts.get(OcrState.OCR_PARTIAL.value, 0),
        "failed": state_counts.get(OcrState.OCR_FAILED.value, 0),
        "needs_review": state_counts.get(OcrState.NEEDS_REVIEW.value, 0),
        "limit_hit": cost_gate.limit_hit,
        "stop_reason": stop_reason,
    }


def _record_usage_for(cost_gate: OcrCostGate, task: OcrTaskRecord) -> None:
    """每张图结束时累计一次用量（成功按实际 Token，失败按 0），重试次数经 attempts 计入。"""
    cost_gate.record_usage(
        OcrUsageRecord(
            ocr_task_id=task.ocr_task_id,
            prompt_tokens=task.prompt_tokens or 0,
            completion_tokens=task.completion_tokens or 0,
            attempts=task.attempts,
        )
    )


# ---------------------------------------------------------------------------
# 运行编排（资产 manifest 读取 / 幂等 / 断点续跑 / force-new-run）
# ---------------------------------------------------------------------------


def load_asset_manifest(manifest_path: Path) -> FloorplanAssetRun:
    """读取不可变资产 manifest（floorplan_asset_manifest.json）。"""
    if not manifest_path.is_file():
        raise FileNotFoundError(f"资产 manifest 不存在: {manifest_path}")
    return FloorplanAssetRun.model_validate(json.loads(manifest_path.read_text(encoding="utf-8")))


def deterministic_ocr_run_id(manifest: FloorplanAssetRun) -> str:
    """非 force 的确定性 run_id（基于 manifest_ref，保证相同输入相同 run_id 幂等）。

    ``manifest_ref`` 可能是相对路径/绝对路径/哈希，run_id 会被用作目录名；前缀只保留
    字母/数字/``.``/``-``/``_``，路径分隔符与冒号（如 Windows 绝对路径 ``C:/...``）替换为
    ``_``，避免 mkdir 因非法字符失败（EXTFP3-F 真实联调发现，EXTFP3-B#F6）。
    """
    prefix = re.sub(r"[^A-Za-z0-9._-]", "_", manifest.manifest_ref or "unknown")[:16]
    return f"floorplan-ocr-{prefix}"


def _dispatch_concurrent(
    to_ocr: list[OcrTaskRecord],
    tasks: list[OcrTaskRecord],
    record: OcrRunRecord,
    *,
    batch_dir: Path,
    run_dir: Path,
    cfg: OcrRunConfig,
    key: str,
    client: httpx.Client,
    cost_gate: OcrCostGate,
    max_attempts: int,
    timeout: float,
    concurrency: int,
) -> tuple[int, str | None]:
    """有界并发调度（OCRNEXT-B 方案 §5.3：单写入者）。

    - worker（线程池）只执行单图请求并返回不可变结果；每张原始响应写独立文件；
    - 唯一被并发读写的共享状态是锁保护的 ``OcrCostGate``；任务记录只被其所属 worker
      修改；运行账本（状态计数/成本/持久化）仅由本调度循环（主线程）聚合写入；
    - 门禁语义：投放前 ``check_before_request()``，触发即停止投放、保留在飞结果，
      剩余任务保持 ``OCR_PENDING``（断点续跑与串行一致）。由于门禁在完成时计数，
      在飞越过上限最多 ``concurrency - 1`` 张，金额越过量级由金额双门禁兜底；
    - 并发路径在每批完成后原子持久化一次检查点（进程中断可续跑）；串行路径保持
      EXTFP3-B 原行为（结束一次持久化）。

    返回 ``(peak_in_flight, stop_reason)``；``stop_reason`` 为 ``"dispatch"`` 表示
    因门禁停止投放。意外异常（call_ocr_one 不抛网络异常）经 ``future.result()``
    在此浮出并中止批次：**此前批次**的检查点已持久化；异常浮出的当前批次不落
    检查点，其中已完成任务的 raw 证据仍在盘上、由断点续跑幂等重投（重复规模
    ≤1 批）。
    """
    peak = 0
    stop: str | None = None
    index = 0
    futures: dict[Future[OcrTaskRecord], None] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        while index < len(to_ocr) or futures:
            while (
                index < len(to_ocr)
                and len(futures) < concurrency
                and cost_gate.check_before_request()
            ):
                task = to_ocr[index]
                index += 1
                assert task.image_path is not None  # 仅 DOWNLOADED 且 storage_path 齐全者入列
                task.queued_at = utc_now_iso()
                task.in_flight_at_dispatch = len(futures) + 1
                image_bytes = (batch_dir / task.image_path).read_bytes()
                future = pool.submit(
                    call_ocr_one,
                    task,
                    image_bytes=image_bytes,
                    config=cfg,
                    api_key=key,
                    out_dir=run_dir,
                    client=client,
                    cost_gate=cost_gate,
                    max_attempts=max_attempts,
                    timeout=timeout,
                )
                futures[future] = None
                peak = max(peak, len(futures))
            if not futures:
                break
            done, _ = wait(set(futures), return_when=FIRST_COMPLETED)
            for future in done:
                futures.pop(future)
                future.result()
            if cost_gate.limit_hit is not None and index < len(to_ocr):
                stop = "dispatch"
                print(
                    f"[floorplan ocr] 成本门禁触发停止投放: "
                    f"limit_hit={cost_gate.limit_hit} 已处理={cost_gate.total_images} "
                    f"剩余待投放={len(to_ocr) - index}"
                )
            # 每完成一批即原子持久化检查点（在飞与未投放任务保持各自状态）
            _aggregate_and_persist(record, tasks, cost_gate)
    return peak, stop


def run_ocr_batch(
    asset_manifest_path: Path,
    out_raw_dir: Path,
    *,
    config: OcrRunConfig | None = None,
    api_key: str | None = None,
    transport: httpx.BaseTransport | None = None,
    force_new_run: bool = False,
    run_id: str | None = None,
    timeout: float = DEFAULT_OCR_TIMEOUT,
    max_attempts: int = OCR_SINGLE_IMAGE_MAX_ATTEMPTS,
    concurrency: int = 1,
) -> OcrRunRecord:
    """执行一次 OCR 运行，返回完整运行记录并持久化状态（EXTFP3-B / OCRNEXT-B）。

    - 从资产 manifest 读取 ``DOWNLOADED`` 且 ``sha256``/``storage_path`` 齐全的资产；
    - 逐张计算幂等键；同 run 已 ``OCR_SUCCEEDED``/``OCR_PARTIAL`` 且原始响应存在的跳过；
    - 每张图请求前 ``cost_gate.check_before_request``，不通过即 fail-closed 停止批次；
    - 运行时读取 ``DASHSCOPE_API_KEY`` 环境变量（可注入用于离线测试）；
    - ``force_new_run`` 产生新 run_id 与新输出目录，不覆盖旧运行证据；
    - ``concurrency``（OCRNEXT-B）：1 为串行兼容路径（行为与 EXTFP3-B 一致）；4/8/16
      走有界并发调度（单写入者聚合，性能块与稳定任务键排序，方案 §5.3/§5.4）。

    日志只输出 run_id、计数、状态统计，不输出密钥 / Base64 / 响应体 / 敏感信息。
    """
    if concurrency not in OCR_CONCURRENCY_CHOICES:
        raise ValueError(
            f"invalid concurrency={concurrency}（仅支持 {OCR_CONCURRENCY_CHOICES}；"
            "4 为诊断档位，验证与验收只走 1/8/16）"
        )
    manifest = load_asset_manifest(asset_manifest_path)
    cfg = config or OcrRunConfig()
    key = api_key if api_key is not None else read_dashscope_api_key()

    resolved_run_id = run_id or deterministic_ocr_run_id(manifest)
    if force_new_run:
        unique = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        resolved_run_id = f"{resolved_run_id}-{unique}"
        run_dir = out_raw_dir / f"run_{resolved_run_id}"
    else:
        run_dir = out_raw_dir / f"run_{resolved_run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    state_path = run_dir / STATE_FILENAME

    batch_dir = asset_manifest_path.parent
    assets = [
        a
        for a in manifest.assets
        if a.asset_status is AssetStatus.DOWNLOADED and a.sha256 and a.storage_path
    ]

    record = OcrRunRecord(
        ocr_run_id=resolved_run_id,
        asset_manifest_ref=manifest.manifest_ref or asset_manifest_path.as_posix(),
        sourced=manifest.sourced,
        tasks=[],
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
        run_dir=run_dir.as_posix(),
    )

    cost_gate = OcrCostGate(cfg.cost)
    tasks: list[OcrTaskRecord] = []
    for asset in assets:
        assert asset.sha256 is not None and asset.storage_path is not None  # 上方过滤保证
        task = OcrTaskRecord(
            ocr_task_id=compute_ocr_task_id(
                asset.sha256, params=_canonical_ocr_params(), parser_version=OCR_PARSER_VERSION
            ),
            ocr_run_id=resolved_run_id,
            asset_id=asset.asset_id,
            image_sha256=asset.sha256,
            image_path=asset.storage_path,
            width=asset.width,
            height=asset.height,
            mime_type=asset.mime_type,
            request_hash=compute_request_hash(
                image_sha256=asset.sha256,
                min_pixels=cfg.request.min_pixels,
                max_pixels=cfg.request.max_pixels,
                enable_rotate=cfg.request.enable_rotate,
                stream=cfg.request.stream,
                parser_version=OCR_PARSER_VERSION,
            ),
            state=OcrState.OCR_PENDING,
        )
        tasks.append(task)

    # 断点续跑：非 force 时复用同 run 已完成任务（幂等键命中且原始响应已落盘即跳过）。
    # 任务 2.2（change ocr-concurrency-optimization）：重写前归档旧 ocr_run.json
    # （保留旧时间线证据；复用任务不迁移时间线字段，dispatched 口径只含本次实际投放，
    # 与 OCRNEXT-B-01#F8 原设计一致——旧时间线由归档文件承载）。
    previous: dict[str, OcrTaskRecord] = {}
    if not force_new_run:
        _archive_previous_run(run_dir)
        previous = _load_previous(state_path)
        for task in tasks:
            old = previous.get(task.ocr_task_id)
            if (
                old
                and old.state in (OcrState.OCR_SUCCEEDED, OcrState.OCR_PARTIAL)
                and old.raw_response_path
                and (run_dir / Path(old.raw_response_path).name).is_file()
            ):
                task.state = old.state
                task.raw_response_path = old.raw_response_path
                task.raw_response_sha256 = old.raw_response_sha256
                task.raw_response_file_sha256 = old.raw_response_file_sha256
                task.model_returned = old.model_returned
                task.provider_request_id = old.provider_request_id
                task.finish_reason = old.finish_reason
                task.prompt_tokens = old.prompt_tokens
                task.completion_tokens = old.completion_tokens
                task.attempts = old.attempts
                task.completed_at = old.completed_at

    to_ocr = [t for t in tasks if t.state is OcrState.OCR_PENDING]
    reused = len(tasks) - len(to_ocr)
    print(
        f"[floorplan ocr] run={resolved_run_id} 资产={len(tasks)} "
        f"待 OCR={len(to_ocr)} 复用完成={reused} 并发={concurrency}"
    )

    batch_started_ns = time.perf_counter_ns()
    stop_reason: str | None = None
    peak_in_flight = 1 if to_ocr else 0

    # 任务 2.1（change ocr-concurrency-optimization）：HTTP 连接池按并发档位显式配置，
    # 避免每图重建 TLS 建连；并发 1 时与既有串行语义一致（连接数=1）。
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )
    with httpx.Client(transport=transport, timeout=timeout, limits=limits) as client:
        if concurrency == 1:
            # 串行兼容路径：语句序列与 EXTFP3-B 一致（仅新增 queued_at/in_flight 埋点字段）
            for task in to_ocr:
                if not cost_gate.check_before_request():
                    task.error_code = "cost_gate_hit"
                    if task.state is OcrState.OCR_PENDING:
                        transition(task, OcrState.OCR_FAILED)
                    stop_reason = "dispatch"
                    print(
                        f"[floorplan ocr] 成本门禁触发停止: "
                        f"limit_hit={cost_gate.limit_hit} 已处理={cost_gate.total_images}"
                    )
                    break
                assert task.image_path is not None  # 仅 DOWNLOADED 且 storage_path 齐全的资产入 OCR
                task.queued_at = utc_now_iso()
                task.in_flight_at_dispatch = 1
                image_bytes = (batch_dir / task.image_path).read_bytes()
                call_ocr_one(
                    task,
                    image_bytes=image_bytes,
                    config=cfg,
                    api_key=key,
                    out_dir=run_dir,
                    client=client,
                    cost_gate=cost_gate,
                    max_attempts=max_attempts,
                    timeout=timeout,
                )
        else:
            peak_in_flight, stop_reason = _dispatch_concurrent(
                to_ocr,
                tasks,
                record,
                batch_dir=batch_dir,
                run_dir=run_dir,
                cfg=cfg,
                key=key,
                client=client,
                cost_gate=cost_gate,
                max_attempts=max_attempts,
                timeout=timeout,
                concurrency=concurrency,
            )

    record.tasks = tasks
    record.state_counts = _count_states(tasks)
    # 任务 2.3：cost 保留门禁快照，并附 tasks 逐条求和派生块供报告交叉核对（不改历史）
    record.cost = {
        **cost_gate.snapshot(),
        "derived_from_tasks": _derive_cost_from_tasks(tasks),
    }
    wall_ms = _elapsed_ms(batch_started_ns)
    record.performance = _build_performance(
        concurrency=concurrency,
        peak_in_flight=peak_in_flight,
        reused=reused,
        dispatched=[t for t in tasks if t.queued_at is not None],
        state_counts=record.state_counts,
        cost_gate=cost_gate,
        wall_ms=wall_ms,
        stop_reason=stop_reason,
    )
    if concurrency > 1:
        # 并发完成顺序不得影响下游输入顺序：按稳定任务键排序后输出（方案 §5.3）
        record.tasks = sorted(record.tasks, key=lambda t: t.ocr_task_id)
    _persist(record)

    summary = ", ".join(f"{k}={v}" for k, v in record.state_counts.items())
    print(
        f"[floorplan ocr] run={resolved_run_id} "
        f"成功={record.state_counts.get(OcrState.OCR_SUCCEEDED.value, 0)} "
        f"部分={record.state_counts.get(OcrState.OCR_PARTIAL.value, 0)} "
        f"失败={record.state_counts.get(OcrState.OCR_FAILED.value, 0)} "
        f"需复核={record.state_counts.get(OcrState.NEEDS_REVIEW.value, 0)} "
        f"成本元={record.cost.get('total_cost_yuan')} "
        f"状态={{ {summary} }} 记录目录={record.run_dir}"
    )
    return record


__all__ = [
    "DASHSCOPE_API_KEY_ENV",
    "DEFAULT_BASE_BACKOFF",
    "DEFAULT_MAX_BACKOFF",
    "DEFAULT_OCR_TIMEOUT",
    "OCR_CONCURRENCY_CHOICES",
    "OCR_PARSER_VERSION",
    "OCR_REQUESTER_VERSION",
    "OCR_SINGLE_IMAGE_MAX_ATTEMPTS",
    "PROVIDER",
    "RAW_RESPONSE_PREFIX",
    "RUN_FILENAME",
    "STATE_FILENAME",
    "TERMINAL_OCR_STATES",
    "OcrRunRecord",
    "OcrState",
    "OcrTaskRecord",
    "build_image_data_url",
    "call_ocr_one",
    "compute_ocr_task_id",
    "compute_request_hash",
    "deterministic_ocr_run_id",
    "extract_response_metadata",
    "load_asset_manifest",
    "raw_response_filename",
    "run_ocr_batch",
    "transition",
]
