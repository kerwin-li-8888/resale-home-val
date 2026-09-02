"""户型图下载器与状态机（EXTFP2-C，技术方案 §7.5 / §8.1-8.4）。

从 ``selection_manifest`` 读取待下载资产清单，按域名白名单、幂等键、有界并发、
指数退避重试与断点续跑把每张户型图**原始二进制字节**落盘到
``{out_dir}/{download_task_id}.img``，并维护 ``download_state.json`` 运行状态。

范围边界（EXTFP2-C 合同）：本模块只做**下载器 + 状态机 + 离线 mock 测试**，
不执行任何真实网络下载（真实 10 张试跑属后续 EXTFP2-E）。默认测试全部离线，
绝不发出真实 HTTP。字节级 ``IMAGE_INVALID`` 校验属后续 EXTFP2-D，本模块不产出。

状态机（§7.5）：显式 ``DownloadState`` 状态，不用布尔掩盖失败原因；状态只能按
白名单迁移且终态（DOWNLOADED / DOWNLOAD_FAILED）不可回退。图片与 OCR 全状态机中
的 NOT_SELECTED/NO_URL/NON_FLOORPLAN_PLACEHOLDER/URL_PARSE_FAILURE 以及
OCR_*/NEEDS_REVIEW/VERIFIED 均不在本下载器产出范围内。

幂等键（§8.3）：
    sale_record_key  = SHA256(source_snapshot_id + source_row_number + source_record_id)
    asset_id         = SHA256(sale_record_key + url_ordinal)
    download_task_id = SHA256(asset_id + canonical_url + downloader_version)

断点续跑（§8.2/§8.3）：同一 run 重跑时已 DOWNLOADED 且 content_sha256 已落盘的
任务按幂等键命中直接跳过；仅对未完成或失败的任务增量续跑；``force_new_run``
产生新 run_id 与新输出目录，不覆盖旧运行的证据。

纪律（§8.2）：域名白名单外的任务直接 DOWNLOAD_FAILED 且零请求；只尊重公开图片
URL 并记录 final_url；对 429/408、超时与 5xx 指数退避 + 随机抖动重试；其余 4xx
不重试；日志只输出 run_id、计数、状态转移与统计，绝不写 Cookie/Authorization/
响应体/个人敏感字段。
"""

from __future__ import annotations

import hashlib
import json
import random
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import BaseModel, Field

from compsval.ingest.floorplan_selection import (
    DOMAIN_WHITELIST,
    SelectionEntry,
    SelectionManifest,
)

# 下载器版本：写入每条任务记录（EXTFP2-C-DL-1.0）
DOWNLOADER_VERSION = "EXTFP2-C-DL-1.0"

# 幂等键分隔符（保证拼接无歧义；幂等键源于 SHA256，分隔符只影响确定性输入）
_KEY_SEP = "|"

# 状态文件 / 运行记录文件名（断点续跑锚点，位于输出目录）
STATE_FILENAME = "download_state.json"
RUN_FILENAME = "download_run.json"

# 单任务指数退避默认参数（§8.2）
DEFAULT_BASE_BACKOFF = 1.0
DEFAULT_MAX_BACKOFF = 30.0

# 默认并发 / 单任务最大请求次数
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_MAX_ATTEMPTS = 3

# 幂等键中 source_snapshot_id 缺省（manifest.snapshot_ref 为空）
FALLBACK_SNAPSHOT_ID = "no-snapshot-ref"


class DownloadState(StrEnum):
    """下载阶段状态机（技术方案 §7.5）：显式状态，不用布尔掩盖失败原因。

    状态迁移白名单（不可回退）：
        READY_TO_DOWNLOAD -> {DOWNLOADING, DOWNLOAD_FAILED}
        DOWNLOADING       -> {DOWNLOADED, DOWNLOAD_FAILED}
        DOWNLOADED        -> {}   # 终态
        DOWNLOAD_FAILED   -> {}   # 终态

    IMAGE_INVALID 属后续 EXTFP2-D 的字节校验状态，C 不产出。
    """

    READY_TO_DOWNLOAD = "READY_TO_DOWNLOAD"
    DOWNLOADING = "DOWNLOADING"
    DOWNLOADED = "DOWNLOADED"
    DOWNLOAD_FAILED = "DOWNLOAD_FAILED"


# 状态迁移白名单：终态（DOWNLOADED/DOWNLOAD_FAILED）不再可迁移，也不可回退
_STATE_TRANSITIONS: dict[DownloadState, frozenset[DownloadState]] = {
    DownloadState.READY_TO_DOWNLOAD: frozenset(
        {DownloadState.DOWNLOADING, DownloadState.DOWNLOAD_FAILED}
    ),
    DownloadState.DOWNLOADING: frozenset({DownloadState.DOWNLOADED, DownloadState.DOWNLOAD_FAILED}),
    DownloadState.DOWNLOADED: frozenset(),
    DownloadState.DOWNLOAD_FAILED: frozenset(),
}

TERMINAL_STATES: frozenset[DownloadState] = frozenset(
    {DownloadState.DOWNLOADED, DownloadState.DOWNLOAD_FAILED}
)


def transition(task: DownloadTask, to: DownloadState) -> None:
    """状态按白名单迁移；任务进终态后不可回退。非法迁移抛出 ValueError。"""
    if to not in _STATE_TRANSITIONS[task.state]:
        raise ValueError(
            f"invalid state transition {task.state.value} -> {to.value} "
            f"(download_task_id={task.download_task_id!r})"
        )
    task.state = to


# ---------------------------------------------------------------------------
# 幂等键（§8.3）
# ---------------------------------------------------------------------------


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sale_record_key(source_snapshot_id: str, source_row_number: int, source_record_id: str) -> str:
    """sale_record_key = SHA256(source_snapshot_id + source_row_number + source_record_id)。"""
    content = _KEY_SEP.join(
        [source_snapshot_id or FALLBACK_SNAPSHOT_ID, str(source_row_number), source_record_id]
    )
    return _sha256_hex(content)


def compute_asset_id(sale_key: str, url_ordinal: int) -> str:
    """asset_id = SHA256(sale_record_key + url_ordinal)，url_ordinal 即记录内 URL 序号。"""
    return _sha256_hex(_KEY_SEP.join([sale_key, str(url_ordinal)]))


def compute_download_task_id(
    asset: str, canonical_url: str, downloader_version: str = DOWNLOADER_VERSION
) -> str:
    """download_task_id = SHA256(asset_id + canonical_url + downloader_version)。"""
    return _sha256_hex(_KEY_SEP.join([asset, canonical_url, downloader_version]))


def _normalize_https(url: str) -> str:
    """URL 规范化：原 scheme 为 http 时改 https，其余原样（与 EXTFP2-B 一致）。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if parts.scheme.lower() != "http":
        return url
    return urlunsplit(("https", parts.netloc, parts.path, parts.query, parts.fragment))


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


class DownloadTask(BaseModel):
    """一条资产的下载任务（每资产一条，技术方案 §8.3/§7.5）。

    记录全部幂等键与元数据；``last_error`` 只存异常类名或 ``http-<状态码>``，
    不写 URL 细节 / Cookie / Authorization / 响应体等敏感或非必要信息。
    """

    download_task_id: str
    asset_id: str
    sale_record_key: str
    source_record_id: str
    row_number: int
    url_ordinal: int = Field(description="记录内 URL 序号（1 起，即幂等键中的 url_ordinal）")
    url: str = Field(description="原始候选 URL，不改写")
    canonical_url: str = Field(description="规范化 URL（HTTPS 优先）")
    domain: str
    state: DownloadState = DownloadState.READY_TO_DOWNLOAD
    attempts: int = 0
    last_error: str | None = None
    downloaded_at: str | None = None
    size_bytes: int | None = None
    content_sha256: str | None = None
    final_url: str | None = Field(default=None, description="遵从重定向后的最终 URL")


class DownloadRunRecord(BaseModel):
    """一次下载运行的总记录（断点续跑与复核证据）。"""

    run_id: str
    downloader_version: str = DOWNLOADER_VERSION
    manifest_ref: str = Field(description="selection_manifest 的 record_ids_hash（或清单路径）")
    sourced: bool = Field(description="清单来源快照是否可追溯（snapshot_ref 非空）")
    state_counts: dict[str, int] = Field(default_factory=dict)
    tasks: list[DownloadTask] = Field(default_factory=list)
    created_at: str
    updated_at: str
    run_dir: str = Field(description="本运行实际输出目录（force-new-run 时与基础 out 不同）")
    stop_reason: str | None = Field(
        default=None,
        description="运行中自动停止原因（§19.2 门禁回调触发时写入；None=未触发）",
    )


# ---------------------------------------------------------------------------
# 单任务下载与重试
# ---------------------------------------------------------------------------


def _is_retryable_status(status: int) -> bool:
    """§8.2：429/408 与 5xx 重试；其余 4xx 不重试。"""
    return status in (408, 429) or 500 <= status < 600


def _is_retryable_exception(exc: Exception) -> bool:
    """超时与连接类错误重试（§8.2）。"""
    return isinstance(exc, (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError))


def _backoff_seconds(base: float, attempt: int, max_backoff: float) -> float:
    """指数退避 + 随机抖动（§8.2）：base * 2^(attempt-1)，封顶 max_backoff，±25% 抖动。"""
    pow_n: int = max(0, attempt - 1)
    raw: float = base * float(2**pow_n)
    capped: float = min(raw, max_backoff)
    jitter: float = capped * 0.25
    return float(capped + random.uniform(-jitter, jitter))


def _download_one(
    task: DownloadTask,
    *,
    out_dir: Path,
    client: httpx.Client,
    max_attempts: int,
    base_backoff: float,
    max_backoff: float,
) -> None:
    """单任务下载：成功落盘原始字节，失败则按规则重试，超限进入 DOWNLOAD_FAILED。"""
    transition(task, DownloadState.DOWNLOADING)

    for attempt in range(1, max_attempts + 1):  # 含首次共 max_attempts 次请求
        task.attempts = attempt
        try:
            response = client.get(task.canonical_url)
        except httpx.HTTPError as exc:
            if _is_retryable_exception(exc) and attempt < max_attempts:
                time.sleep(_backoff_seconds(base_backoff, attempt, max_backoff))
                continue
            task.last_error = type(exc).__name__
            transition(task, DownloadState.DOWNLOAD_FAILED)
            return

        if 200 <= response.status_code < 300:
            payload = response.content
            path = out_dir / f"{task.download_task_id}.img"
            path.write_bytes(payload)  # 原始字节落盘，不转码不重压缩
            task.content_sha256 = hashlib.sha256(payload).hexdigest()
            task.size_bytes = len(payload)
            task.downloaded_at = datetime.now(UTC).isoformat()
            task.final_url = str(response.url)
            task.last_error = None  # 成功后清空此前重试留下的错误标记
            transition(task, DownloadState.DOWNLOADED)
            return

        task.last_error = f"http-{response.status_code}"
        if _is_retryable_status(response.status_code) and attempt < max_attempts:
            time.sleep(_backoff_seconds(base_backoff, attempt, max_backoff))
            continue
        transition(task, DownloadState.DOWNLOAD_FAILED)
        return


# ---------------------------------------------------------------------------
# 运行编排（幂等键 / 断点续跑 / force-new-run / 并发）
# ---------------------------------------------------------------------------


def _make_task(entry: SelectionEntry, snapshot_id: str) -> DownloadTask:
    """由一条 SelectionEntry 构建任务并计算各层幂等键（§8.3）。"""
    sale_key = sale_record_key(snapshot_id, entry.row_number, entry.source_record_id)
    asset = compute_asset_id(sale_key, entry.url_seq)
    canonical = entry.normalized_url or _normalize_https(entry.url)
    dtask = compute_download_task_id(asset, canonical)
    return DownloadTask(
        download_task_id=dtask,
        asset_id=asset,
        sale_record_key=sale_key,
        source_record_id=entry.source_record_id,
        row_number=entry.row_number,
        url_ordinal=entry.url_seq,
        url=entry.url,
        canonical_url=canonical,
        domain=entry.domain,
        state=DownloadState.READY_TO_DOWNLOAD,
    )


def _atomic_write(path: Path, text: str) -> None:
    """原子写盘：先写 .incomplete 再 replace。"""
    work = path.with_name(path.name + ".incomplete")
    work.write_text(text, encoding="utf-8")
    work.replace(path)


def _persist(record: DownloadRunRecord) -> None:
    """把运行记录原子写盘到 download_state.json 与 download_run.json。"""
    run_dir = Path(record.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    record.updated_at = datetime.now(UTC).isoformat()
    text = record.model_dump_json(indent=2)
    _atomic_write(run_dir / STATE_FILENAME, text)
    _atomic_write(run_dir / RUN_FILENAME, text)


def _load_previous(state_path: Path) -> dict[str, DownloadTask]:
    """读取同 run 既有 download_state.json，供断点续跑按幂等键复用已完成任务。"""
    resume: dict[str, DownloadTask] = {}
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
            t = DownloadTask.model_validate(raw)
        except Exception:  # noqa: BLE001 - 历史损坏记录跳过续跑
            continue
        resume[t.download_task_id] = t
    return resume


def deterministic_run_id(manifest: SelectionManifest) -> str:
    """非 force 的确定性 run_id（基于 record_ids_hash，保证相同输入相同 run_id 幂等）。"""
    prefix = (manifest.record_ids_hash or manifest.run_id or "unknown")[:16]
    return f"floorplan-dl-{prefix}"


def run_download(
    manifest: SelectionManifest,
    out_dir: Path,
    *,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    timeout: float = 30.0,
    transport: httpx.BaseTransport | None = None,
    force_new_run: bool = False,
    manifest_path: Path | None = None,
    run_id: str | None = None,
    base_backoff: float = DEFAULT_BASE_BACKOFF,
    max_backoff: float = DEFAULT_MAX_BACKOFF,
    stop_check: Callable[[DownloadTask], str | None] | None = None,
) -> DownloadRunRecord:
    """执行一次户型图下载运行，返回完整运行记录并持久化状态。

    - 从 manifest.records（缺省回退 record_sample）读取待下载资产清单；
    - 计算各层幂等键；域名白名单外任务直接 DOWNLOAD_FAILED 且零请求；
    - 有界并发下载（默认 4），httpx client 可注入 transport 以便离线 mock；
    - 断点续跑：同 run 已 DOWNLOADED 且 content_sha256 已落盘的任务按幂等键命中跳过；
    - force_new_run 产生新 run_id 与新输出目录，不覆盖旧运行证据；
    - stop_check（EXTFP4）：每个任务完成后回调；返回非 None 即停止投放剩余任务
      （§19.2 触发即停，保留在飞与已完成证据），原因写入 run 记录 stop_reason。

    日志只输出 run_id、计数、状态统计，不输出 URL/响应体/敏感信息。
    """
    if max_concurrency < 1 or max_attempts < 1 or timeout <= 0:
        raise ValueError("max_concurrency/max_attempts 必须 >=1，timeout 必须 >0")

    snapshot_id = manifest.snapshot_ref or FALLBACK_SNAPSHOT_ID
    # 域名白名单：模块级 DOMAIN_WHITELIST 为权威底线，绝不因 manifest 而放宽；
    # manifest 显式声明 domain_whitelist 时取二者交集（fail-closed，防口径漂移）。
    manifest_whitelist = set(manifest.domain_whitelist or ())
    effective_whitelist: frozenset[str] = (
        DOMAIN_WHITELIST & frozenset(manifest_whitelist) if manifest_whitelist else DOMAIN_WHITELIST
    )
    resolved_run_id = run_id or deterministic_run_id(manifest)
    if force_new_run:
        unique = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        resolved_run_id = f"{resolved_run_id}-{unique}"
        run_dir = out_dir / f"run_{resolved_run_id}"
    else:
        run_dir = out_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    state_path = run_dir / STATE_FILENAME

    # 待下载资产：records 为空时回退 record_sample（§4.3）
    entries = manifest.records or manifest.record_sample or []
    record = DownloadRunRecord(
        run_id=resolved_run_id,
        downloader_version=DOWNLOADER_VERSION,
        manifest_ref=(
            manifest_path.as_posix() if manifest_path is not None else manifest.record_ids_hash
        ),
        sourced=bool(manifest.snapshot_ref),
        tasks=[],
        created_at=datetime.now(UTC).isoformat(),
        updated_at=datetime.now(UTC).isoformat(),
        run_dir=run_dir.as_posix(),
    )

    tasks: list[DownloadTask] = []
    for entry in entries:
        task = _make_task(entry, snapshot_id)
        if entry.domain not in effective_whitelist:
            # 白名单外零请求，直接失败且不计入重试
            task.last_error = "domain-not-allowed"
            transition(task, DownloadState.DOWNLOAD_FAILED)
        tasks.append(task)

    # 断点续跑：非 force 时复用同 run 已完成任务（幂等键命中即跳过下载）
    previous: dict[str, DownloadTask] = {}
    if not force_new_run:
        previous = _load_previous(state_path)
        for task in tasks:
            old = previous.get(task.download_task_id)
            if (
                old
                and old.state is DownloadState.DOWNLOADED
                and old.content_sha256
                and (run_dir / f"{task.download_task_id}.img").is_file()
            ):
                # 已完整落盘的成果直接复用，不再发起请求
                task.state = old.state
                task.content_sha256 = old.content_sha256
                task.size_bytes = old.size_bytes
                task.downloaded_at = old.downloaded_at
                task.final_url = old.final_url
                task.attempts = old.attempts

    to_download = [t for t in tasks if t.state is DownloadState.READY_TO_DOWNLOAD]
    print(
        f"[floorplan download] run={record.run_id} 资产={len(tasks)} "
        f"待下载={len(to_download)} 复用完成={len(tasks) - len(to_download)}"
    )

    # 有界并发下载（任务状态字段由 GIL 保证线程安全更新）
    def _process(task: DownloadTask) -> None:
        with httpx.Client(transport=transport, follow_redirects=True, timeout=timeout) as client:
            _download_one(
                task,
                out_dir=run_dir,
                client=client,
                max_attempts=max_attempts,
                base_backoff=base_backoff,
                max_backoff=max_backoff,
            )

    if to_download:
        # EXTFP4 stop_check：future -> task 映射，任务完成后回调判定是否停止投放；
        # 触发即取消未开始任务（在飞任务自然完成），stop_reason 写入运行记录。
        with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            futures = {pool.submit(_process, t): t for t in to_download}
            for fut in as_completed(list(futures)):
                task_done = futures[fut]
                fut.result()  # _download_one 内部已捕获网络错误，无预期异常外泄
                if stop_check is not None:
                    reason = stop_check(task_done)
                    if reason is not None:
                        record.stop_reason = reason
                        print(
                            f"[floorplan download] §19.2 自动停止投放: "
                            f"reason={reason} 剩余未投放={len(futures) - 1}"
                        )
                        for pending in futures:
                            pending.cancel()
                        break

    # 收尾：装载全量任务 + 状态聚合计数并原子持久化
    record.tasks = tasks
    counts: dict[str, int] = {}
    for t in tasks:
        counts[t.state.value] = counts.get(t.state.value, 0) + 1
    record.state_counts = counts
    _persist(record)

    summary = ", ".join(f"{k}={v}" for k, v in counts.items())
    print(
        f"[floorplan download] run={record.run_id} "
        f"成功下载={counts.get(DownloadState.DOWNLOADED.value, 0)} "
        f"失败={counts.get(DownloadState.DOWNLOAD_FAILED.value, 0)} "
        f"状态={{ {summary} }} 记录目录={record.run_dir}"
    )
    return record


__all__ = [
    "DEFAULT_BASE_BACKOFF",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_BACKOFF",
    "DEFAULT_MAX_CONCURRENCY",
    "DOMAIN_WHITELIST",
    "DOWNLOADER_VERSION",
    "RUN_FILENAME",
    "STATE_FILENAME",
    "TERMINAL_STATES",
    "DownloadRunRecord",
    "DownloadState",
    "DownloadTask",
    "compute_asset_id",
    "compute_download_task_id",
    "deterministic_run_id",
    "run_download",
    "sale_record_key",
    "transition",
]
