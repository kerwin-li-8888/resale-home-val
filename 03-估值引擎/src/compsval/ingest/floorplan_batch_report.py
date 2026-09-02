"""EXTFP4 批次报告层：范围外标注隔离、一致性差异清单、完整性门禁与质量报告。

change extfp4-production-batch（技术方案 §14 / specs ocr-production-batch）：

- **范围外标注隔离（任务 5.1）**：范围外样本经 :class:`OutOfScopeRegistry`
  登记（人工/规则判定来源可追溯），转录产物显式标 ``OUT_OF_SCOPE``：
  不自动接受、不入有效分母与有效字段统计、保留 ``isolation_reason`` 审计痕迹；
  最终消费策略归 EXTFP5 数据冻结另案，本层不预先决定。
- **一致性与差异清单（任务 5.2）**：对运行记录逐样本核对原始响应存在性/哈希/
  可解析性/模型一致性，差异逐样本列入机器生成的可解释清单，可追溯到原始响应。
- **完整性门禁（任务 6.2）**：资产、原始响应、转录与运行记录的哈希与数量
  一致性核对；未通过即批次保持未完成并输出缺口清单。
- **质量报告（任务 6.1）**：按 §14 口径从机器产物装配，动态数字不由手工维护。
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_asset import AssetStatus
from compsval.ingest.floorplan_ocr import OcrState
from compsval.ingest.floorplan_transcribe import AnnotationState

_BATCH_REPORT_VERSION = "EXTFP4-BATCH-REPORT-1.0"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _raw_response_primary_ok(task: Any, data: bytes) -> bool:
    """原始响应哈希主口径核对（change extfp4-verify-followups）。

    新运行（登记了 ``raw_response_file_sha256``）：主口径 = 落盘实际字节直比；
    历史运行（无该字段）：保持既有「字节直比 OR 换行归一化净化文本」双口径兼容。
    """
    file_sha = getattr(task, "raw_response_file_sha256", None)
    if file_sha:
        return bool(_sha256_bytes(data) == file_sha)
    direct_ok = _sha256_bytes(data) == task.raw_response_sha256
    normalized = data.replace(b"\r\n", b"\n")
    sanitized_ok = (
        _sha256_bytes(normalized.decode("utf-8", errors="replace").encode("utf-8"))
        == task.raw_response_sha256
    )
    return bool(direct_ok or sanitized_ok)


def _raw_caliber_disclosed_mismatch(task: Any, data: bytes) -> bool:
    """原始字节口径并列披露：换行归一化净化文本哈希与 ``raw_response_sha256`` 不一致。

    仅对登记了 ``raw_response_file_sha256`` 的新运行统计——差异源于净化文本 + CRLF
    翻译，属已知形态，不计入一致性差异清单，只登记计数披露。
    """
    file_sha = getattr(task, "raw_response_file_sha256", None)
    if not file_sha or not task.raw_response_sha256:
        return False
    normalized = data.replace(b"\r\n", b"\n")
    return bool(
        _sha256_bytes(normalized.decode("utf-8", errors="replace").encode("utf-8"))
        != task.raw_response_sha256
    )


# ---------------------------------------------------------------------------
# 范围外样本登记与标注隔离（任务 5.1）
# ---------------------------------------------------------------------------


class OutOfScopeEntry(BaseModel):
    """一条范围外判定（判定来源、理由与时刻留痕，可审计）。"""

    source_record_id: str
    reason: str = Field(description="判定理由（如 多层户型/商住混入）")
    judged_by: str = Field(description="判定来源（user/review/规则版本）")
    judged_at: str


class OutOfScopeRegistry(BaseModel):
    """范围外样本登记表（JSON 落盘；不改写既有条目，只追加）。"""

    change_ref: str = "extfp4-production-batch"
    entries: list[OutOfScopeEntry] = Field(default_factory=list)

    def add(self, entry: OutOfScopeEntry) -> None:
        for existing in self.entries:
            if existing.source_record_id == entry.source_record_id:
                if (existing.reason, existing.judged_by) != (entry.reason, entry.judged_by):
                    raise ValueError(
                        f"范围外判定冲突: {entry.source_record_id} 已登记为 "
                        f"{existing.reason!r}/{existing.judged_by!r}"
                    )
                return
        self.entries.append(entry)

    def is_out_of_scope(self, source_record_id: str) -> OutOfScopeEntry | None:
        for entry in self.entries:
            if entry.source_record_id == source_record_id:
                return entry
        return None

    @classmethod
    def load(cls, path: Path) -> OutOfScopeRegistry:
        if not path.is_file():
            return cls()
        return cls.model_validate(json.loads(path.read_text(encoding="utf-8")))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        work = path.with_name(path.name + ".incomplete")
        work.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        work.replace(path)


def apply_out_of_scope_marks(
    annotations: list[Any],
    *,
    ocr_tasks: list[Any],
    assets_by_asset_id: dict[str, Any],
    registry: OutOfScopeRegistry,
) -> tuple[list[Any], dict[str, int]]:
    """把登记表中的范围外判定应用到转录标注（返回新列表，不改写入参）。

    映射链：annotation.ocr_task_id → ocr_task.asset_id → asset.source_record_id
    → registry 判定。命中样本的标注 ``parse_state`` 置 ``OUT_OF_SCOPE``，
    ``isolation_reason`` 记录理由与判定来源（审计痕迹）；不自动接受、不入有效
    分母。返回 (标注列表, 状态计数)。
    """
    task_to_asset = {t.ocr_task_id: t.asset_id for t in ocr_tasks}
    asset_to_source = {
        asset_id: getattr(asset, "source_record_id", "")
        for asset_id, asset in assets_by_asset_id.items()
    }
    marked: list[Any] = []
    counts: dict[str, int] = {}
    for ann in annotations:
        asset_id = task_to_asset.get(ann.ocr_task_id)
        source_id = asset_to_source.get(asset_id or "", "")
        entry = registry.is_out_of_scope(source_id) if source_id else None
        if entry is not None:
            ann = ann.model_copy(
                update={
                    "parse_state": AnnotationState.OUT_OF_SCOPE.value,
                    "isolation_reason": (
                        f"out_of_scope:{entry.reason}（判定来源 {entry.judged_by} "
                        f"@ {entry.judged_at}）"
                    ),
                }
            )
        marked.append(ann)
        counts[ann.parse_state] = counts.get(ann.parse_state, 0) + 1
    return marked, counts


def valid_denominator_annotations(annotations: list[Any]) -> list[Any]:
    """有效分母：排除 ``OUT_OF_SCOPE`` 标注（范围外不入分母，#N5 口径）。"""
    return [a for a in annotations if a.parse_state != AnnotationState.OUT_OF_SCOPE.value]


# ---------------------------------------------------------------------------
# 一致性与差异清单（任务 5.2）
# ---------------------------------------------------------------------------


class ConsistencyEntry(BaseModel):
    """逐样本一致性/差异条目（形态与证据路径可追溯）。"""

    ocr_task_id: str
    kind: str = Field(
        description="差异形态：missing_raw/sha_mismatch/unparseable/model_mismatch/partial/failed/needs_review"
    )
    detail: str
    raw_response_path: str | None = None


def build_consistency_report(ocr_record: Any, run_dir: Path) -> list[ConsistencyEntry]:
    """逐样本核对原始响应与运行记录的一致性，返回机器生成的差异清单。

    核对项：原始响应文件存在、SHA256 与记录一致、JSON 可解析、返回模型与请求
    模型一致、finish_reason=length（PARTIAL）与失败/需复核形态。全部条目可沿
    ``raw_response_path`` 追溯到原始响应。
    """
    entries: list[ConsistencyEntry] = []
    for task in ocr_record.tasks:
        if task.state in (OcrState.OCR_SUCCEEDED, OcrState.OCR_PARTIAL):
            raw_name = Path(task.raw_response_path).name if task.raw_response_path else None
            raw_path = run_dir / raw_name if raw_name else None
            if raw_path is None or not raw_path.is_file():
                entries.append(
                    ConsistencyEntry(
                        ocr_task_id=task.ocr_task_id,
                        kind="missing_raw",
                        detail="状态为成功/部分但原始响应文件缺失",
                        raw_response_path=task.raw_response_path,
                    )
                )
                continue
            data = raw_path.read_bytes()
            # 哈希主口径（change extfp4-verify-followups）：新运行以落盘实际字节
            # （raw_response_file_sha256）为主核对；历史运行按既有换行归一化双口径兼容。
            if not _raw_response_primary_ok(task, data):
                entries.append(
                    ConsistencyEntry(
                        ocr_task_id=task.ocr_task_id,
                        kind="sha_mismatch",
                        detail="原始响应 SHA256 与运行记录不一致（主口径核对失败）",
                        raw_response_path=task.raw_response_path,
                    )
                )
                continue
            try:
                json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                entries.append(
                    ConsistencyEntry(
                        ocr_task_id=task.ocr_task_id,
                        kind="unparseable",
                        detail="原始响应 JSON 不可解析",
                        raw_response_path=task.raw_response_path,
                    )
                )
                continue
            if task.model_returned and task.model_returned != task.model_requested:
                entries.append(
                    ConsistencyEntry(
                        ocr_task_id=task.ocr_task_id,
                        kind="model_mismatch",
                        detail=(
                            f"返回模型 {task.model_returned} 与请求模型 "
                            f"{task.model_requested} 不一致"
                        ),
                        raw_response_path=task.raw_response_path,
                    )
                )
            if task.state is OcrState.OCR_PARTIAL:
                entries.append(
                    ConsistencyEntry(
                        ocr_task_id=task.ocr_task_id,
                        kind="partial",
                        detail=f"部分成功（finish_reason={task.finish_reason}），列入需复核差异",
                        raw_response_path=task.raw_response_path,
                    )
                )
        else:
            entries.append(
                ConsistencyEntry(
                    ocr_task_id=task.ocr_task_id,
                    kind=task.state.value.lower(),
                    detail=f"失败/需复核形态（error_code={task.error_code}）",
                    raw_response_path=task.raw_response_path,
                )
            )
    return entries


# ---------------------------------------------------------------------------
# 完整性门禁（任务 6.2）
# ---------------------------------------------------------------------------


class IntegrityReport(BaseModel):
    """批次完整性门禁结果（通过与否 + 缺口清单）。"""

    passed: bool
    checked_at: str = Field(default_factory=_now)
    checks: list[dict[str, Any]] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)


def _task_field(task: Any, name: str) -> Any:
    """兼容 dict 与 pydantic 对象两种任务记录形态。"""
    if isinstance(task, dict):
        return task.get(name)
    return getattr(task, name, None)


def check_batch_integrity(
    *,
    selection_manifest: Any,
    download_record: Any,
    ocr_record: Any,
    annotations: list[Any],
    run_dir: Path,
) -> IntegrityReport:
    """核对资产、原始响应、转录与运行记录的哈希与数量一致性。

    - 数量：selection.asset_count == 下载任务数 == OCR 任务数；
    - 哈希：下载任务 content_sha256 与运行记录一致（跨记录核对，不重读原图字节）；
      成功/部分任务的原始响应 SHA256 逐一重算核对（字节与净化文本双口径）；
    - 转录：annotation.ocr_task_id 必须能对应到 OCR 任务。
    未通过时 ``passed=False`` 并给出缺口清单。
    """
    checks: list[dict[str, Any]] = []
    gaps: list[str] = []

    dl_count = len(getattr(download_record, "tasks", []) or [])
    ocr_count = len(ocr_record.tasks or [])
    asset_expected = selection_manifest.asset_count
    checks.append(
        {
            "name": "count_alignment",
            "selection_assets": asset_expected,
            "download_tasks": dl_count,
            "ocr_tasks": ocr_count,
        }
    )
    if not (dl_count == ocr_count == asset_expected):
        gaps.append(f"数量不一致: selection={asset_expected} download={dl_count} ocr={ocr_count}")

    dl_tasks = getattr(download_record, "tasks", []) or []
    dl_by_asset = {_task_field(t, "asset_id"): t for t in dl_tasks}
    hash_mismatch = 0
    for task in ocr_record.tasks or []:
        dl = dl_by_asset.get(task.asset_id)
        if dl is None:
            gaps.append(f"OCR 任务 {task.ocr_task_id} 的 asset_id 在下载记录中不存在")
            continue
        dl_sha = _task_field(dl, "content_sha256")
        if dl_sha and task.image_sha256 and dl_sha != task.image_sha256:
            hash_mismatch += 1
            gaps.append(f"asset {task.asset_id} 下载/OCR 记录的图片 SHA256 不一致")
    checks.append({"name": "asset_hash_alignment", "mismatches": hash_mismatch})

    raw_missing = 0
    file_caliber_mismatch = 0
    raw_disclosed_mismatch = 0
    legacy_checked = 0
    for task in ocr_record.tasks:
        if task.state in (OcrState.OCR_SUCCEEDED, OcrState.OCR_PARTIAL):
            raw_name = Path(task.raw_response_path).name if task.raw_response_path else None
            raw_path = run_dir / raw_name if raw_name else None
            if raw_path is None or not raw_path.is_file():
                raw_missing += 1
                gaps.append(f"任务 {task.ocr_task_id} 原始响应缺失")
                continue
            file_data = raw_path.read_bytes()
            # 主口径（change extfp4-verify-followups）：新运行以落盘实际字节
            # （raw_response_file_sha256）为准；历史运行按既有双口径兼容核对。
            if not _raw_response_primary_ok(task, file_data):
                file_caliber_mismatch += 1
                gaps.append(f"任务 {task.ocr_task_id} 原始响应 SHA256 不符（落盘口径）")
                continue
            if _raw_caliber_disclosed_mismatch(task, file_data):
                raw_disclosed_mismatch += 1
            if getattr(task, "raw_response_file_sha256", None) is None:
                legacy_checked += 1
    checks.append(
        {
            "name": "raw_response_integrity",
            "missing_or_bad": raw_missing,
            "file_caliber_mismatch": file_caliber_mismatch,
            "raw_caliber_disclosed_mismatch": raw_disclosed_mismatch,
            "legacy_records_checked": legacy_checked,
        }
    )

    valid_task_ids = {t.ocr_task_id for t in (ocr_record.tasks or [])}
    orphan = sum(1 for a in annotations if a.ocr_task_id not in valid_task_ids)
    checks.append(
        {"name": "annotation_lineage", "annotations": len(annotations), "orphans": orphan}
    )
    if orphan:
        gaps.append(f"{orphan} 条标注引用了不存在的 OCR 任务")

    invalid_assets = sum(
        1
        for t in dl_tasks
        if _task_field(t, "last_error") is not None and _task_field(t, "state") == "DOWNLOAD_FAILED"
    )
    checks.append({"name": "download_failed_tasks", "count": invalid_assets})

    return IntegrityReport(passed=not gaps, checks=checks, gaps=gaps)


def load_assets_by_id(asset_manifest_path: Path) -> dict[str, Any]:
    """读取资产 manifest 并返回 asset_id -> asset 映射（供范围外映射链）。"""
    from compsval.ingest.floorplan_ocr import load_asset_manifest

    manifest = load_asset_manifest(asset_manifest_path)
    return {a.asset_id: a for a in manifest.assets if a.asset_status is AssetStatus.DOWNLOADED}


# ---------------------------------------------------------------------------
# §14 批次质量报告（任务 6.1/6.3）
# ---------------------------------------------------------------------------


class BatchQualityReport(BaseModel):
    """EXTFP4 批次质量报告（§14 口径；动态数字全部由机器产物生成）。"""

    report_version: str = _BATCH_REPORT_VERSION
    change_ref: str = "extfp4-production-batch"
    batch_id: str
    generated_at: str = Field(default_factory=_now)
    selection: dict[str, Any] = Field(default_factory=dict)
    download: dict[str, Any] = Field(default_factory=dict)
    ocr: dict[str, Any] = Field(default_factory=dict)
    transcription: dict[str, Any] = Field(default_factory=dict)
    consistency: dict[str, Any] = Field(default_factory=dict)
    integrity: dict[str, Any] = Field(default_factory=dict)
    stop_conditions_triggered: list[dict[str, Any]] = Field(default_factory=list)
    gate_failures: list[dict[str, str]] = Field(
        default_factory=list, description="未通过门禁及最小修复建议"
    )
    known_limitations: list[dict[str, str]] = Field(
        default_factory=list, description="已知限制四项（结论强制引用）"
    )


def build_batch_quality_report(
    *,
    batch_id: str,
    selection_manifest: Any,
    exclusion_report: dict[str, Any] | None,
    download_record: Any,
    ocr_record: Any,
    annotation_state_counts: dict[str, int],
    annotations_total: int,
    consistency_entries: list[ConsistencyEntry],
    integrity: IntegrityReport,
    stop_triggers: list[dict[str, Any]],
    known_limitations: list[dict[str, str]],
) -> BatchQualityReport:
    """从机器产物装配 §14 质量报告；不接收任何手填动态数字。"""
    dl_tasks = list(getattr(download_record, "tasks", []) or [])
    sizes = sorted(
        int(s) for s in (_task_field(t, "size_bytes") for t in dl_tasks) if s is not None
    )

    def _pct(values: list[int], p: int) -> int | None:
        if not values:
            return None
        rank = max(1, -(-p * len(values) // 100))
        return values[min(rank, len(values)) - 1]

    http_states: dict[str, int] = {}
    retries = 0
    for t in dl_tasks:
        err = _task_field(t, "last_error")
        if err is not None:
            http_states[err] = http_states.get(err, 0) + 1
        retries += max(0, (_task_field(t, "attempts") or 0) - 1)

    gate_failures: list[dict[str, str]] = []
    if not integrity.passed:
        gate_failures.append(
            {
                "gate": "integrity",
                "suggestion": "按缺口清单补齐缺失产物或排查哈希不一致后重跑完整性门禁",
            }
        )
    for entry in consistency_entries:
        if entry.kind in {"missing_raw", "sha_mismatch", "unparseable", "model_mismatch"}:
            gate_failures.append(
                {
                    "gate": f"consistency:{entry.kind}",
                    "suggestion": f"核对任务 {entry.ocr_task_id} 的原始响应与运行记录",
                }
            )
            break

    report = BatchQualityReport(
        batch_id=batch_id,
        selection={
            "rule_version": selection_manifest.selection_rule_version,
            "geoscope": selection_manifest.geoscope,
            "date_window": [
                getattr(selection_manifest, "date_window_min", None),
                getattr(selection_manifest, "date_window_max", None),
            ],
            "record_count": selection_manifest.record_count,
            "asset_count": selection_manifest.asset_count,
            "forbidden_domain_count": selection_manifest.forbidden_domain_count,
            "record_ids_hash": selection_manifest.record_ids_hash,
            "exclusion": exclusion_report,
        },
        download={
            "state_counts": getattr(download_record, "state_counts", {}),
            "run_id": getattr(download_record, "run_id", None),
            "stop_reason": getattr(download_record, "stop_reason", None),
            "bytes_p50": _pct(sizes, 50),
            "bytes_p95": _pct(sizes, 95),
            "bytes_min": sizes[0] if sizes else None,
            "bytes_max": sizes[-1] if sizes else None,
            "bytes_avg": round(sum(sizes) / len(sizes)) if sizes else None,
            "last_error_distribution": http_states,
            "download_retries": retries,
        },
        ocr={
            "run_id": getattr(ocr_record, "ocr_run_id", None),
            "state_counts": getattr(ocr_record, "state_counts", {}),
            "cost": getattr(ocr_record, "cost", {}),
            "performance": getattr(ocr_record, "performance", None),
        },
        transcription={
            "annotations_total": annotations_total,
            "state_counts": annotation_state_counts,
            "valid_denominator_total": (
                annotation_state_counts.get("annotations_total", annotations_total)
                - annotation_state_counts.get(AnnotationState.OUT_OF_SCOPE.value, 0)
            ),
        },
        consistency={
            "entries_total": len(consistency_entries),
            "by_kind": _count_by_kind(consistency_entries),
            "entries": [e.model_dump() for e in consistency_entries],
        },
        integrity=integrity.model_dump(),
        stop_conditions_triggered=stop_triggers,
        gate_failures=gate_failures,
        known_limitations=known_limitations,
    )
    return report


def _count_by_kind(entries: list[ConsistencyEntry]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for e in entries:
        counts[e.kind] = counts.get(e.kind, 0) + 1
    return counts


__all__ = [
    "BatchQualityReport",
    "ConsistencyEntry",
    "IntegrityReport",
    "OutOfScopeEntry",
    "OutOfScopeRegistry",
    "apply_out_of_scope_marks",
    "build_batch_quality_report",
    "build_consistency_report",
    "check_batch_integrity",
    "load_assets_by_id",
    "valid_denominator_annotations",
]
