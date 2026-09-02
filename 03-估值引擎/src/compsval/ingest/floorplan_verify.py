"""自动一致性检查、质量报告与状态分类（EXTFP3-E，技术方案 §9.5/§14/§12）。

本模块实现 ``compsval floorplan verify`` 与 ``compsval floorplan status`` 的核心逻辑：

1. **一致性检查**（§9.5，只产生警告/复核任务，不自动覆盖任何一方）：
   - ``model_match``：Qwen 返回模型 == 请求模型；
   - ``batch_request_uniqueness``：同批请求参数/解析器版本唯一；
   - ``accepted_word_evidence``：已接受字段 ``word_id`` + 位置证据覆盖；
   - ``multiple_areas_conflict``：同一房间多个无法解释的面积；
   - ``room_count_vs_excel``：OCR 卧室/客厅标签数 vs Excel 卧室/客厅数量；
   - ``total_area_vs_transaction``：OCR 明确总面积 vs 第 17 列成交面积；
   - ``building_area_vs_excel``：OCR 建筑面积/套内面积文本 vs 第 40 列；
   - ``repeat_run_consistency``：固定图片重复运行的有效字段一致率。
2. **质量报告**（§14）：按机器产物聚合输入版本、状态、词条、标注、成本、版本与
   门禁指标；Markdown 只解释，不手工维护另一套数字。
3. **状态分类**（§12）：按状态机显式分类失败，失败可分类、可追溯、可重试。

范围边界（EXTFP3-E 合同）：只做**检查 + 报告 + CLI + 离线测试**，不触网、不付费、
不改写原始证据；``consistency_status`` 回填标注表（只追加派生字段）。通过/拒绝决定
属 EXTFP3-H（依赖黄金标签与 §10.3 验收线）。
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_asset import FloorplanAssetRun
from compsval.ingest.floorplan_ocr import (
    OcrRunRecord,
    OcrState,
    OcrTaskRecord,
    load_asset_manifest,
    raw_response_filename,
)
from compsval.ingest.floorplan_ocr_contract import OCR_MODEL_ID
from compsval.ingest.floorplan_ocr_parse import (
    OcrWordRecord,
    load_ocr_run_record,
)
from compsval.ingest.floorplan_transcribe import (
    ANNOTATION_STAGED_FILENAME,
    TRANSCRIBE_PARSER_VERSION,
    AnnotationState,
    RoomAnnotationRecord,
)

# 一致性检查/报告版本（写入每条检查结果）
VERIFY_VERSION = "EXTFP3-E-1.0"

# 一致率验收线门槛（§10.3 候选值；H 阶段正式判定，E 只产出指标）
REPEAT_CONSISTENCY_THRESHOLD = 0.995

# 面积冲突判定阈值（E 阶段候选；H 按 §10.3 对齐，改动需证据）
AREA_OVER_RATIO = Decimal("1.10")  # OCR 总面积 > 成交面积 * 1.10 → 过度转录警告
AREA_UNDER_RATIO = Decimal("0.50")  # OCR 总面积 < 成交面积 * 0.50 → 明显漏标警告
BUILDING_AREA_TOLERANCE = Decimal("0.05")  # 建筑面积相对偏差容差

# 卧室/客厅标准房间类型（用于与 Excel 卧室/客厅数量比对）
BEDROOM_TYPES = frozenset({"master_bedroom", "secondary_bedroom", "bedroom"})
LIVING_ROOM_TYPES = frozenset({"living_room"})

# 建筑面积/套内面积关键词（OCR 词条识别）
_BUILDING_AREA_KEYWORDS = ("建筑面积", "建面", "套内面积")

# 检查项 ID（稳定，供报告/门禁引用）
CHECK_MODEL_MATCH = "model_match"
CHECK_BATCH_UNIQUENESS = "batch_request_uniqueness"
CHECK_WORD_EVIDENCE = "accepted_word_evidence"
CHECK_MULTIPLE_AREAS = "multiple_areas_conflict"
CHECK_ROOM_COUNT_EXCEL = "room_count_vs_excel"
CHECK_TOTAL_AREA_EXCEL = "total_area_vs_transaction"
CHECK_BUILDING_AREA_EXCEL = "building_area_vs_excel"
CHECK_REPEAT_CONSISTENCY = "repeat_run_consistency"


class Finding(BaseModel):
    """一条一致性检查发现（含稳定 ID、严重度、任务引用与证据）。"""

    finding_id: str
    check_id: str
    severity: str = Field(description="info / warn / error")
    ocr_task_id: str | None = None
    message: str
    evidence: dict[str, Any] = Field(default_factory=dict)


class CheckResult(BaseModel):
    """一次一致性检查的结果。"""

    check_id: str
    name: str
    status: str = Field(description="ok / warn / fail / not_applicable")
    detail: str | None = None
    findings: list[Finding] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class VerifyReport(BaseModel):
    """一次 verify 运行的整体报告（检查 + §14 质量字段 + §10.3 门禁指标）。"""

    ocr_run_id: str
    verify_version: str = VERIFY_VERSION
    generated_at: str
    overall: str = Field(description="ok / warn / blocked / not_applicable")
    checks: list[CheckResult] = Field(default_factory=list)
    quality: dict[str, Any] = Field(default_factory=dict)
    gating: dict[str, Any] = Field(default_factory=dict)


class TaskStatusItem(BaseModel):
    """status 命令中的单任务状态条目（失败可分类、可追溯、可重试）。"""

    ocr_task_id: str
    asset_id: str
    state: str
    model_returned: str | None = None
    finish_reason: str | None = None
    response_status: str | None = None
    error_code: str | None = None
    attempts: int = 0
    retryable: bool = False
    raw_response_present: bool = False


class StatusReport(BaseModel):
    """status 命令的输出：状态机计数 + 失败分类 + 可重试提示。"""

    ocr_run_id: str
    requester_version: str
    generated_at: str
    state_counts: dict[str, int] = Field(default_factory=dict)
    tasks_total: int = 0
    tasks_terminal: int = 0
    failures: list[TaskStatusItem] = Field(default_factory=list)
    retryable_tasks: list[TaskStatusItem] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)


class ExcelRoomAreaInfo(BaseModel):
    """从 staged 普通住宅表抽取的 Excel 侧房间/面积信息（供 §9.5 比对）。"""

    source_record_id: str
    bedrooms: int | None = None
    living_rooms: int | None = None
    transaction_area_sqm: Decimal | None = None
    building_area_detail_sqm: Decimal | None = None


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


# ---------------------------------------------------------------------------
# 解析辅助（§9.5 的 Excel 侧字段解析；未知不用 0，解析失败保持未知）
# ---------------------------------------------------------------------------


def parse_count(raw: str | None) -> int | None:
    """从原始文本提取首个整数（如 '3'、'3室'、'3室2厅'）；无可解析整数返回 None。"""
    if raw is None:
        return None
    match = re.search(r"\d+", str(raw))
    return int(match.group()) if match else None


def parse_decimal(value: Any) -> Decimal | None:
    """把 Decimal/str/int/float 安全转为 Decimal；非法/None 返回 None。"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def load_staged_excel_lookup(staged_path: Path) -> dict[str, ExcelRoomAreaInfo]:
    """读取 staged 普通住宅 parquet 为 ``source_record_id -> ExcelRoomAreaInfo``。"""
    import pyarrow.parquet as pq

    table = pq.read_table(staged_path)
    lookup: dict[str, ExcelRoomAreaInfo] = {}
    for row in table.to_pylist():
        sid = row.get("source_record_id")
        if not sid:
            continue
        lookup[str(sid)] = ExcelRoomAreaInfo(
            source_record_id=str(sid),
            bedrooms=parse_count(row.get("bedrooms_raw")),
            living_rooms=parse_count(row.get("living_rooms_raw")),
            transaction_area_sqm=parse_decimal(row.get("transaction_area_sqm")),
            building_area_detail_sqm=parse_decimal(row.get("building_area_detail_sqm")),
        )
    return lookup


def build_task_excel_map(
    run: OcrRunRecord,
    asset_manifest: FloorplanAssetRun | None,
    excel_lookup: dict[str, ExcelRoomAreaInfo] | None,
) -> dict[str, ExcelRoomAreaInfo]:
    """按 ``ocr_task_id -> asset -> source_record_id`` 关联 Excel 信息。

    任一环节缺失（无资产 manifest / 无 staged 表 / 记录不在表内）则该任务不参与
    Excel 比对（不静默猜测）。OCR 字段永远是派生事实，不能反向覆盖 Excel 原值。
    """
    if asset_manifest is None or excel_lookup is None:
        return {}
    asset_by_id = {a.asset_id: a for a in asset_manifest.assets}
    result: dict[str, ExcelRoomAreaInfo] = {}
    for task in run.tasks:
        asset = asset_by_id.get(task.asset_id)
        if asset is None:
            continue
        info = excel_lookup.get(asset.source_record_id)
        if info is not None:
            result[task.ocr_task_id] = info
    return result


# ---------------------------------------------------------------------------
# 标注表读取（verify 输入之一）
# ---------------------------------------------------------------------------


def read_annotation_table(annotation_table_path: Path) -> list[RoomAnnotationRecord]:
    """读取 ``floorplan_room_annotation.parquet``（EXTFP3-D 产物）为标注列表。"""
    import pyarrow.parquet as pq

    table = pq.read_table(annotation_table_path)
    annotations: list[RoomAnnotationRecord] = []
    for row in table.to_pylist():
        location = row.get("location")
        if isinstance(location, str):
            try:
                location = json.loads(location)
            except json.JSONDecodeError:
                location = []
        annotations.append(
            RoomAnnotationRecord(
                annotation_id=row["annotation_id"],
                ocr_run_id=row["ocr_run_id"],
                ocr_task_id=row["ocr_task_id"],
                parse_version=row.get("parse_version") or TRANSCRIBE_PARSER_VERSION,
                room_word_id=row.get("room_word_id"),
                area_word_id=row.get("area_word_id"),
                room_name_raw=row.get("room_name_raw"),
                room_name_normalized=row.get("room_name_normalized"),
                standard_room_type=row.get("standard_room_type"),
                area_text_raw=row.get("area_text_raw"),
                area_text_normalized=row.get("area_text_normalized"),
                area_value=row.get("area_value"),
                area_unit=row.get("area_unit"),
                location=location or [],
                parse_state=row.get("parse_state") or AnnotationState.NEEDS_REVIEW.value,
                isolation_reason=row.get("isolation_reason"),
                consistency_status=row.get("consistency_status"),
                review_state=row.get("review_state"),
                review_event_ref=row.get("review_event_ref"),
            )
        )
    return annotations


# ---------------------------------------------------------------------------
# 统计辅助
# ---------------------------------------------------------------------------


def ocr_room_type_counts(annotations: list[RoomAnnotationRecord]) -> dict[str, dict[str, int]]:
    """按 ocr_task_id 统计标准房间类型出现次数（有 ``room_word_id`` 的房间标签才计入）。"""
    out: dict[str, dict[str, int]] = {}
    for ann in annotations:
        if ann.room_word_id is None or ann.standard_room_type is None:
            continue
        task = out.setdefault(ann.ocr_task_id, {})
        task[ann.standard_room_type] = task.get(ann.standard_room_type, 0) + 1
    return out


def ocr_accepted_area_total(
    annotations: list[RoomAnnotationRecord],
) -> tuple[Decimal | None, int]:
    """汇总一张图已接受面积之和 ``(total, count)``；无已接受面积返回 (None, 0)。

    只累计 ``ACCEPTED`` 标注的 ``area_value``（可解析 Decimal）；ROOM_ONLY/CONFLICT/
    NEEDS_REVIEW 不进入总和（不把不确定面积当作明确事实）。
    """
    total = Decimal("0")
    count = 0
    for ann in annotations:
        if ann.parse_state != AnnotationState.ACCEPTED.value:
            continue
        value = parse_decimal(ann.area_value)
        if value is None:
            continue
        total += value
        count += 1
    return (total, count) if count else (None, 0)


def _accepted_fields_by_task(
    annotations: list[RoomAnnotationRecord],
) -> dict[str, list[tuple[str, str]]]:
    """按 ocr_task_id 聚合已接受字段 ``(standard_room_type, area_value)``（供一致率）。"""
    out: dict[str, list[tuple[str, str]]] = {}
    for ann in annotations:
        if ann.parse_state != AnnotationState.ACCEPTED.value:
            continue
        key = ann.standard_room_type or ann.room_name_normalized or "?"
        value = ann.area_value if ann.area_value is not None else ""
        out.setdefault(ann.ocr_task_id, []).append((key, value))
    return out


# ---------------------------------------------------------------------------
# 一致性检查（§9.5；每条为纯函数，离线可测）
# ---------------------------------------------------------------------------


def _finding(check_id: str, index: int, severity: str, message: str, **evidence: Any) -> Finding:
    return Finding(
        finding_id=f"{check_id}-{index:03d}",
        check_id=check_id,
        severity=severity,
        message=message,
        evidence=evidence,
    )


def check_model_match(run: OcrRunRecord) -> CheckResult:
    """Qwen 返回模型 == 请求模型（§9.5）。

    只核对有 ``model_returned`` 的任务（失败/无响应任务不参与模型比对）；任一返回
    模型与请求模型不一致 → warn。``model_returned`` 缺失保持 info（fail-open 边界，
    RV-EXTFP3-C-01#F3 承接，F 阶段真实联调确认后收紧）。
    """
    checked = 0
    matched = 0
    missing = 0
    findings: list[Finding] = []
    for idx, task in enumerate(run.tasks):
        if task.model_returned is None:
            missing += 1
            continue
        checked += 1
        if task.model_returned == task.model_requested:
            matched += 1
        else:
            findings.append(
                _finding(
                    CHECK_MODEL_MATCH,
                    idx,
                    "warn",
                    f"返回模型 {task.model_returned!r} != 请求模型 {task.model_requested!r}",
                    ocr_task_id=task.ocr_task_id,
                    model_returned=task.model_returned,
                    model_requested=task.model_requested,
                )
            )
    status = "not_applicable" if checked == 0 else ("ok" if not findings else "warn")
    return CheckResult(
        check_id=CHECK_MODEL_MATCH,
        name="模型匹配",
        status=status,
        detail=f"核对 {checked} 任务，{matched} 匹配，{missing} 缺 model_returned",
        findings=findings,
        counts={"checked": checked, "matched": matched, "missing": missing},
    )


def check_batch_uniqueness(
    run: OcrRunRecord,
    words: list[OcrWordRecord],
    annotations: list[RoomAnnotationRecord],
) -> CheckResult:
    """同批请求参数/解析器版本唯一（§9.5）。

    检查 parser_version、contract_version、parse_version 是否各自唯一（同一批次不应
    混用不同参数/解析器）；request_hash 是请求内容哈希（同一批次每张图天然不同，见
    EXTFP3-F 真实联调 RV-EXTFP3-F-01#F1），仅保留计数不参与 warn 判定。
    任一版本维度出现多个不同值 → warn。
    """
    request_hash_count = len({t.request_hash for t in run.tasks})
    parser_versions = {t.parser_version for t in run.tasks}
    contract_versions = {t.request_contract_version for t in run.tasks}
    word_parse_versions = {w.parse_version for w in words}
    annotation_parse_versions = {a.parse_version for a in annotations}

    findings: list[Finding] = []
    dirty: list[str] = []
    dims = {
        "parser_version": parser_versions,
        "contract_version": contract_versions,
        "word_parse_version": word_parse_versions,
        "annotation_parse_version": annotation_parse_versions,
    }
    for name, values in dims.items():
        if len(values) > 1:
            dirty.append(name)
            findings.append(
                _finding(
                    CHECK_BATCH_UNIQUENESS,
                    len(findings),
                    "warn",
                    f"批次内 {name} 不唯一：{sorted(str(v) for v in values)}",
                )
            )
    counts = {f"{name}_count": len(values) for name, values in dims.items()}
    counts["request_hash_count"] = request_hash_count
    return CheckResult(
        check_id=CHECK_BATCH_UNIQUENESS,
        name="同批请求参数/解析器版本唯一",
        status=(
            "not_applicable"
            if not (run.tasks or words or annotations)
            else ("warn" if dirty else "ok")
        ),
        detail="批次内各维度版本唯一" if not dirty else f"不唯一维度: {', '.join(dirty)}",
        findings=findings,
        counts=counts,
    )


def check_accepted_word_evidence(annotations: list[RoomAnnotationRecord]) -> CheckResult:
    """已接受字段 word_id + 位置证据覆盖（§9.5，验收线 §10.3 证据覆盖率）。

    每个 ACCEPTED 标注必须有 ``room_word_id`` 且 ``location`` 非空；带面积的必须同时
    有 ``area_word_id``。缺证据 → warn（不自动删除，标记复核）。
    """
    accepted_total = 0
    missing_room_word = 0
    missing_area_word = 0
    missing_location = 0
    findings: list[Finding] = []
    for idx, ann in enumerate(annotations):
        if ann.parse_state != AnnotationState.ACCEPTED.value:
            continue
        accepted_total += 1
        if ann.room_word_id is None:
            missing_room_word += 1
            findings.append(
                _finding(
                    CHECK_WORD_EVIDENCE,
                    idx,
                    "warn",
                    "ACCEPTED 标注缺 room_word_id",
                    ocr_task_id=ann.ocr_task_id,
                    annotation_id=ann.annotation_id,
                )
            )
        if not ann.location:
            missing_location += 1
            findings.append(
                _finding(
                    CHECK_WORD_EVIDENCE,
                    idx,
                    "warn",
                    "ACCEPTED 标注缺位置证据",
                    ocr_task_id=ann.ocr_task_id,
                    annotation_id=ann.annotation_id,
                )
            )
        if ann.area_value is not None and ann.area_word_id is None:
            missing_area_word += 1
            findings.append(
                _finding(
                    CHECK_WORD_EVIDENCE,
                    idx,
                    "warn",
                    "ACCEPTED 面积字段缺 area_word_id",
                    ocr_task_id=ann.ocr_task_id,
                    annotation_id=ann.annotation_id,
                )
            )
    status = "not_applicable" if accepted_total == 0 else ("ok" if not findings else "warn")
    return CheckResult(
        check_id=CHECK_WORD_EVIDENCE,
        name="已接受字段 word_id + 位置证据覆盖",
        status=status,
        detail=(
            f"ACCEPTED {accepted_total} 条；缺 room_word_id {missing_room_word}、"
            f"缺 area_word_id {missing_area_word}、缺位置 {missing_location}"
        ),
        findings=findings,
        counts={
            "accepted_total": accepted_total,
            "missing_room_word": missing_room_word,
            "missing_area_word": missing_area_word,
            "missing_location": missing_location,
        },
    )


def check_multiple_areas_conflict(annotations: list[RoomAnnotationRecord]) -> CheckResult:
    """同一房间多个无法解释的面积（§9.5；EXTFP3-D 已写 CONFLICT，这里计数报告）。"""
    conflicts = [a for a in annotations if a.parse_state == AnnotationState.CONFLICT.value]
    tasks_with_conflict = len({a.ocr_task_id for a in conflicts})
    findings = [
        _finding(
            CHECK_MULTIPLE_AREAS,
            idx,
            "warn",
            f"CONFLICT 标注（同房多面积，无法唯一解释）: {a.room_name_normalized or '?'}",
            ocr_task_id=a.ocr_task_id,
            annotation_id=a.annotation_id,
        )
        for idx, a in enumerate(conflicts)
    ]
    return CheckResult(
        check_id=CHECK_MULTIPLE_AREAS,
        name="同一房间多个无法解释的面积",
        status=("not_applicable" if not annotations else ("warn" if conflicts else "ok")),
        detail=f"CONFLICT {len(conflicts)} 条 / {tasks_with_conflict} 张图",
        findings=findings,
        counts={"conflict": len(conflicts), "tasks_with_conflict": tasks_with_conflict},
    )


def check_room_count_vs_excel(
    annotations: list[RoomAnnotationRecord],
    excel_by_task: dict[str, ExcelRoomAreaInfo],
) -> CheckResult:
    """OCR 卧室/客厅标签数 vs Excel 卧室/客厅数量（§9.5，只警告不覆盖）。"""
    counts = ocr_room_type_counts(annotations)
    findings: list[Finding] = []
    compared = 0
    mismatched = 0
    for task_id, task_counts in sorted(counts.items()):
        info = excel_by_task.get(task_id)
        if info is None or (info.bedrooms is None and info.living_rooms is None):
            continue
        compared += 1
        ocr_bed = sum(task_counts.get(t, 0) for t in BEDROOM_TYPES)
        ocr_living = sum(task_counts.get(t, 0) for t in LIVING_ROOM_TYPES)
        if info.bedrooms is not None and ocr_bed != info.bedrooms:
            mismatched += 1
            findings.append(
                _finding(
                    CHECK_ROOM_COUNT_EXCEL,
                    len(findings),
                    "warn",
                    f"OCR 卧室 {ocr_bed} != Excel 卧室 {info.bedrooms}",
                    ocr_task_id=task_id,
                    ocr_bedrooms=ocr_bed,
                    excel_bedrooms=info.bedrooms,
                )
            )
        if info.living_rooms is not None and ocr_living != info.living_rooms:
            mismatched += 1
            findings.append(
                _finding(
                    CHECK_ROOM_COUNT_EXCEL,
                    len(findings),
                    "warn",
                    f"OCR 客厅 {ocr_living} != Excel 客厅 {info.living_rooms}",
                    ocr_task_id=task_id,
                    ocr_living_rooms=ocr_living,
                    excel_living_rooms=info.living_rooms,
                )
            )
    status = "not_applicable" if compared == 0 else ("warn" if findings else "ok")
    return CheckResult(
        check_id=CHECK_ROOM_COUNT_EXCEL,
        name="OCR 卧室/客厅标签数 vs Excel",
        status=status,
        detail=f"比对 {compared} 张图，不一致 {mismatched} 处",
        findings=findings,
        counts={"compared": compared, "mismatched": mismatched},
    )


def check_total_area_vs_transaction(
    annotations: list[RoomAnnotationRecord],
    excel_by_task: dict[str, ExcelRoomAreaInfo],
) -> CheckResult:
    """OCR 明确总面积 vs 第 17 列成交面积显著不一致（§9.5，只警告不覆盖）。"""
    findings: list[Finding] = []
    compared = 0
    over_count = 0
    under_count = 0
    for task_id in sorted({a.ocr_task_id for a in annotations}):
        info = excel_by_task.get(task_id)
        if info is None or info.transaction_area_sqm is None:
            continue
        total, count = ocr_accepted_area_total([a for a in annotations if a.ocr_task_id == task_id])
        if total is None or count == 0:
            continue
        compared += 1
        excel_area = info.transaction_area_sqm
        if total > excel_area * AREA_OVER_RATIO:
            over_count += 1
            findings.append(
                _finding(
                    CHECK_TOTAL_AREA_EXCEL,
                    len(findings),
                    "warn",
                    f"OCR 总面积 {total} 超过成交面积 {excel_area}（>×{AREA_OVER_RATIO}）",
                    ocr_task_id=task_id,
                    ocr_total=str(total),
                    excel_area=str(excel_area),
                )
            )
        elif total < excel_area * AREA_UNDER_RATIO:
            under_count += 1
            findings.append(
                _finding(
                    CHECK_TOTAL_AREA_EXCEL,
                    len(findings),
                    "info",
                    f"OCR 总面积 {total} 明显小于成交面积 {excel_area}（<×{AREA_UNDER_RATIO}，"
                    "可能漏标，不推断）",
                    ocr_task_id=task_id,
                    ocr_total=str(total),
                    excel_area=str(excel_area),
                )
            )
    status = "not_applicable" if compared == 0 else ("warn" if over_count else "ok")
    return CheckResult(
        check_id=CHECK_TOTAL_AREA_EXCEL,
        name="OCR 明确总面积 vs 成交面积",
        status=status,
        detail=f"比对 {compared} 张图；过度转录 {over_count}、明显漏标 {under_count}",
        findings=findings,
        counts={"compared": compared, "over": over_count, "under": under_count},
    )


def _building_area_from_words(words: list[OcrWordRecord]) -> tuple[str | None, Decimal | None]:
    """从逐词表找建筑面积/套内面积词条并解析数值；未找到返回 (None, None)。

    匹配策略：词条归一化文本含关键词（如「建筑面积」），且同词条内出现面积数字单位。
    """
    for w in words:
        if not any(kw in w.text_normalized for kw in _BUILDING_AREA_KEYWORDS):
            continue
        match = re.search(
            r"(?P<value>\d+(?:\.\d+)?)\s*(?:㎡|m²|m2|M2|平方米|平米|平)", w.text_normalized
        )
        if match:
            value = parse_decimal(match.group("value"))
            if value is not None:
                return w.text_normalized, value
    return None, None


def check_building_area_vs_excel(
    words: list[OcrWordRecord],
    excel_by_task: dict[str, ExcelRoomAreaInfo],
) -> CheckResult:
    """OCR 建筑面积/套内面积文本 vs 第 40 列冲突（§9.5，只警告不覆盖）。"""
    findings: list[Finding] = []
    compared = 0
    mismatched = 0
    for task_id in sorted({w.ocr_task_id for w in words}):
        info = excel_by_task.get(task_id)
        if info is None or info.building_area_detail_sqm is None:
            continue
        task_words = [w for w in words if w.ocr_task_id == task_id]
        text, value = _building_area_from_words(task_words)
        if value is None:
            continue
        compared += 1
        excel_area = info.building_area_detail_sqm
        if excel_area != 0 and abs(value - excel_area) / excel_area > BUILDING_AREA_TOLERANCE:
            mismatched += 1
            findings.append(
                _finding(
                    CHECK_BUILDING_AREA_EXCEL,
                    len(findings),
                    "warn",
                    f"OCR 建筑面积 {value}（{text!r}）与 Excel 第40列 {excel_area} 冲突",
                    ocr_task_id=task_id,
                    ocr_building_area=str(value),
                    excel_building_area=str(excel_area),
                )
            )
    status = "not_applicable" if compared == 0 else ("warn" if findings else "ok")
    return CheckResult(
        check_id=CHECK_BUILDING_AREA_EXCEL,
        name="OCR 建筑面积文本 vs Excel 第40列",
        status=status,
        detail=f"比对 {compared} 张图，冲突 {mismatched} 处",
        findings=findings,
        counts={"compared": compared, "mismatched": mismatched},
    )


def check_repeat_consistency(
    annotations_a: list[RoomAnnotationRecord],
    annotations_b: list[RoomAnnotationRecord],
    *,
    threshold: float = REPEAT_CONSISTENCY_THRESHOLD,
) -> CheckResult:
    """固定图片重复运行的有效字段一致率（§9.5，验收线 §10.3 ≥99.5%）。

    对两次运行都出现的 ocr_task_id，比较已接受字段集合 ``(房间类型, 面积)``：
    一致率 = 两运行共有的匹配字段 / 两运行字段并集；仅出现在一次运行的任务单独计数，
    不进分母（避免把缺失运行当作不一致）。
    """
    fields_a = _accepted_fields_by_task(annotations_a)
    fields_b = _accepted_fields_by_task(annotations_b)
    common = sorted(set(fields_a) & set(fields_b))
    matched = 0
    total = 0
    mismatched_tasks = 0
    findings: list[Finding] = []
    for task_id in common:
        # 多重集比较（RV-EXTFP3-E-01#F2）：A 两间同类型同面积、B 一间 → 一致率按
        # 交集 min 计数 / 并集 max 计数，避免集合式成员判断系统性高估一致率。
        counter_a = Counter(fields_a[task_id])
        counter_b = Counter(fields_b[task_id])
        union_size = sum((counter_a | counter_b).values())
        agree = sum((counter_a & counter_b).values())
        matched += agree
        total += union_size
        if agree != union_size:
            mismatched_tasks += 1
            findings.append(
                _finding(
                    CHECK_REPEAT_CONSISTENCY,
                    len(findings),
                    "warn",
                    f"重复运行字段不一致: A={counter_a} B={counter_b}",
                    ocr_task_id=task_id,
                )
            )
    rate = (matched / total) if total else None
    only_a = sorted(set(fields_a) - set(fields_b))
    only_b = sorted(set(fields_b) - set(fields_a))
    if rate is None:
        status = "not_applicable"
    elif rate >= threshold:
        status = "ok"
    else:
        status = "warn"
    return CheckResult(
        check_id=CHECK_REPEAT_CONSISTENCY,
        name="固定图片重复运行有效字段一致率",
        status=status,
        detail=f"一致率 {rate if rate is None else f'{rate:.4%}'}（门槛 ≥{threshold:.4f}）",
        findings=findings,
        counts={
            "common_tasks": len(common),
            "matched_fields": matched,
            "total_fields": total,
            "mismatched_tasks": mismatched_tasks,
            "only_in_run_a": len(only_a),
            "only_in_run_b": len(only_b),
        },
    )


# ---------------------------------------------------------------------------
# 一致性状态回填（§7.4 consistency_status，只追加派生字段）
# ---------------------------------------------------------------------------


def derive_consistency_status(annotation: RoomAnnotationRecord) -> str:
    """从标注解析状态与证据派生 ``consistency_status``（供 verify 回填标注表）。"""
    if annotation.parse_state == AnnotationState.ACCEPTED.value:
        if annotation.room_word_id is None or not annotation.location:
            return "REVIEW_MISSING_EVIDENCE"
        return "OK"
    if annotation.parse_state == AnnotationState.CONFLICT.value:
        return "CONFLICT_MULTIPLE_AREAS"
    if annotation.parse_state == AnnotationState.ROOM_ONLY.value:
        return "ROOM_ONLY_NO_AREA"
    return "REVIEW"


def backfill_consistency_status(
    annotations: list[RoomAnnotationRecord],
) -> list[RoomAnnotationRecord]:
    """返回带 ``consistency_status`` 的标注列表（不改写原始标注字段）。"""
    return [
        ann.model_copy(update={"consistency_status": derive_consistency_status(ann)})
        for ann in annotations
    ]


def write_annotation_consistency(
    annotations: list[RoomAnnotationRecord],
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """把回填 ``consistency_status`` 后的标注表原子写回（沿用 D 阶段列结构）。"""
    from compsval.ingest.floorplan_transcribe import write_annotation_table

    return write_annotation_table(annotations, data_dir, compression=compression)


# ---------------------------------------------------------------------------
# 状态分类（§12：失败可分类、可追溯、可重试）
# ---------------------------------------------------------------------------

# 业务可重试错误码（非终态或明确可重试的失败）
_RETRYABLE_ERROR_CODES = frozenset(
    {
        "finish_reason_length",
        "network-timeout",
        "connect-error",
        "http-429",
        "http-408",
        "http-5xx",
    }
)
_RETRYABLE_RESPONSE_STATUSES = frozenset(
    {"network-timeout", "connect-error", "http-429", "http-408"}
)


def _retryable(task: OcrTaskRecord) -> bool:
    if task.state in (OcrState.OCR_PENDING, OcrState.OCR_RUNNING):
        return True
    # OCR_PARTIAL（真实 finish_reason=length → error_code=finish_reason_length，floorplan_ocr.py
    # L542-545）与 OCR_FAILED（网络/429/408/5xx 等）都按业务错误码统一判定（RV-EXTFP3-E-01#F1）。
    # 例外：finish_reason=length（输出被截断）本质可重试——即使历史运行记录未回填
    # error_code（EXTFP3-H#F10 实测 ocr_run.json 中 2 条 OCR_PARTIAL 的 error_code=null），
    # finish_reason 本身即是可重试证据，不应判为不可重试。
    if task.state in (OcrState.OCR_PARTIAL, OcrState.OCR_FAILED):
        if (task.finish_reason or "").lower() == "length":
            return True
        code = (task.error_code or "").lower()
        status = (task.response_status or "").lower()
        return code in _RETRYABLE_ERROR_CODES or status in _RETRYABLE_RESPONSE_STATUSES
    return False


def build_status_report(run: OcrRunRecord, *, run_dir: Path | None = None) -> StatusReport:
    """按状态机聚合状态计数与失败分类（可重试/可追溯/可分类）。"""
    state_counts: dict[str, int] = {}
    failures: list[TaskStatusItem] = []
    retryable: list[TaskStatusItem] = []
    for task in run.tasks:
        state_counts[task.state.value] = state_counts.get(task.state.value, 0) + 1
        raw_present = False
        if run_dir is not None:
            # 原始响应文件名使用截断 task_id（MAX_PATH 修复，floorplan_ocr.py）；旧的全 64-hex
            # 文件名在此路径不存在，否则 H10 会把已落盘响应误判为缺失（EXTFP3-H#F10）。
            raw_present = (run_dir / raw_response_filename(task.ocr_task_id)).is_file()
        item = TaskStatusItem(
            ocr_task_id=task.ocr_task_id,
            asset_id=task.asset_id,
            state=task.state.value,
            model_returned=task.model_returned,
            finish_reason=task.finish_reason,
            response_status=task.response_status,
            error_code=task.error_code,
            attempts=task.attempts,
            retryable=_retryable(task),
            raw_response_present=raw_present,
        )
        if task.state in (OcrState.OCR_FAILED, OcrState.NEEDS_REVIEW, OcrState.OCR_PARTIAL):
            failures.append(item)
        if item.retryable:
            retryable.append(item)

    terminal = sum(
        state_counts.get(s.value, 0)
        for s in OcrState
        if s
        in (
            OcrState.OCR_SUCCEEDED,
            OcrState.OCR_PARTIAL,
            OcrState.OCR_FAILED,
            OcrState.NEEDS_REVIEW,
        )
    )
    return StatusReport(
        ocr_run_id=run.ocr_run_id,
        requester_version=run.requester_version,
        generated_at=utc_now_iso(),
        state_counts=state_counts,
        tasks_total=len(run.tasks),
        tasks_terminal=terminal,
        failures=sorted(failures, key=lambda f: (f.state, f.ocr_task_id)),
        retryable_tasks=sorted(retryable, key=lambda f: f.ocr_task_id),
        summary={
            "succeeded": state_counts.get(OcrState.OCR_SUCCEEDED.value, 0),
            "partial": state_counts.get(OcrState.OCR_PARTIAL.value, 0),
            "failed": state_counts.get(OcrState.OCR_FAILED.value, 0),
            "needs_review": state_counts.get(OcrState.NEEDS_REVIEW.value, 0),
            "pending_or_running": state_counts.get(OcrState.OCR_PENDING.value, 0)
            + state_counts.get(OcrState.OCR_RUNNING.value, 0),
        },
    )


# ---------------------------------------------------------------------------
# verify 编排
# ---------------------------------------------------------------------------


def _resolve_table(
    run_dir: Path,
    explicit: Path | None,
    data_dir: Path | None,
    filename: str,
) -> Path | None:
    if explicit is not None:
        return explicit if explicit.is_file() else None
    if data_dir is not None:
        candidate = data_dir / "staged" / filename
        if candidate.is_file():
            return candidate
    candidate = run_dir / filename
    return candidate if candidate.is_file() else None


def verify_run(
    run_dir: Path,
    *,
    data_dir: Path | None = None,
    word_table_path: Path | None = None,
    annotation_table_path: Path | None = None,
    asset_manifest_path: Path | None = None,
    staged_table_path: Path | None = None,
    repeat_annotations: list[RoomAnnotationRecord] | None = None,
    write_consistency: bool = False,
) -> VerifyReport:
    """对一次 OCR 运行执行全部一致性检查并生成质量报告（EXTFP3-E）。

    - ``run_dir``：OCR 运行目录（含 ``ocr_run.json``）；
    - 词表/标注表未显式给出时，按 ``data_dir/staged/`` 或运行目录内查找；
    - Excel 比对需要 ``asset_manifest_path``（任务→记录）与 ``staged_table_path``
      （记录→Excel 字段），缺一则该维度 not_applicable；
    - ``repeat_annotations`` 提供第二次运行标注时计算重复运行一致率；
    - ``write_consistency`` 为 True 时把 ``consistency_status`` 回填标注表（仅追加派生字段）。
    """
    run = load_ocr_run_record(run_dir)

    word_table = _resolve_table(run_dir, word_table_path, data_dir, "floorplan_ocr_word.parquet")
    annotation_table = _resolve_table(
        run_dir, annotation_table_path, data_dir, ANNOTATION_STAGED_FILENAME
    )

    from compsval.ingest.floorplan_transcribe import read_word_table

    words: list[OcrWordRecord] = []
    if word_table is not None:
        words = read_word_table(word_table)

    annotations: list[RoomAnnotationRecord] = []
    if annotation_table is not None:
        annotations = read_annotation_table(annotation_table)

    asset_manifest: FloorplanAssetRun | None = None
    if asset_manifest_path is not None and asset_manifest_path.is_file():
        asset_manifest = load_asset_manifest(asset_manifest_path)

    excel_lookup: dict[str, ExcelRoomAreaInfo] | None = None
    if staged_table_path is not None and staged_table_path.is_file():
        excel_lookup = load_staged_excel_lookup(staged_table_path)

    excel_by_task = build_task_excel_map(run, asset_manifest, excel_lookup)

    checks: list[CheckResult] = [
        check_model_match(run),
        check_batch_uniqueness(run, words, annotations),
        check_accepted_word_evidence(annotations),
        check_multiple_areas_conflict(annotations),
        check_room_count_vs_excel(annotations, excel_by_task),
        check_total_area_vs_transaction(annotations, excel_by_task),
        check_building_area_vs_excel(words, excel_by_task),
    ]
    if repeat_annotations is not None:
        checks.append(check_repeat_consistency(annotations, repeat_annotations))
    else:
        checks.append(
            CheckResult(
                check_id=CHECK_REPEAT_CONSISTENCY,
                name="固定图片重复运行有效字段一致率",
                status="not_applicable",
                detail="未提供第二次运行标注，跳过",
            )
        )

    severity_rank = {"ok": 0, "info": 0, "warn": 1, "fail": 2}
    present = [c.status for c in checks if c.status in severity_rank]
    if not present:
        # 全部 not_applicable（空运行/无词表/无标注/无 Excel 比对输入）→ 不冒充 ok
        # （RV-EXTFP3-E-01#F4）
        overall = "not_applicable"
    else:
        worst = max(present, key=lambda s: severity_rank[s])
        overall = "blocked" if worst == "fail" else worst

    quality = build_quality_report(
        run,
        word_table_path=word_table,
        annotation_table_path=annotation_table,
        words=words,
        annotations=annotations,
        checks=checks,
    )
    gating = build_gating_metrics(checks, annotations, words)

    if write_consistency and data_dir is not None and annotation_table is not None:
        backfilled = backfill_consistency_status(annotations)
        write_annotation_consistency(backfilled, data_dir)
        quality["consistency_backfilled"] = True

    return VerifyReport(
        ocr_run_id=run.ocr_run_id,
        generated_at=utc_now_iso(),
        overall=overall,
        checks=checks,
        quality=quality,
        gating=gating,
    )


def build_quality_report(
    run: OcrRunRecord,
    *,
    word_table_path: Path | None = None,
    annotation_table_path: Path | None = None,
    words: list[OcrWordRecord] | None = None,
    annotations: list[RoomAnnotationRecord] | None = None,
    checks: list[CheckResult] | None = None,
) -> dict[str, Any]:
    """按 §14 聚合质量报告字段（全部由机器产物生成）。"""
    words = words or []
    annotations = annotations or []
    annotation_states: dict[str, int] = {}
    for a in annotations:
        annotation_states[a.parse_state] = annotation_states.get(a.parse_state, 0) + 1
    room_types: dict[str, int] = {}
    for a in annotations:
        if a.standard_room_type:
            room_types[a.standard_room_type] = room_types.get(a.standard_room_type, 0) + 1

    check_status = {c.check_id: c.status for c in (checks or [])}
    return {
        "ocr_run_id": run.ocr_run_id,
        "requester_version": run.requester_version,
        "config_schema_version": run.config_schema_version,
        "contract_version": run.contract_version,
        "asset_manifest_ref": run.asset_manifest_ref,
        "sourced": run.sourced,
        "state_counts": run.state_counts,
        "cost": run.cost,
        "ocr_tasks": len(run.tasks),
        "words": len(words),
        "word_table_present": word_table_path is not None,
        "annotations_total": len(annotations),
        "annotation_states": annotation_states,
        "room_types": room_types,
        "accepted_rate": (
            annotation_states.get(AnnotationState.ACCEPTED.value, 0) / len(annotations)
        )
        if annotations
        else None,
        "checks": check_status,
        "versions": {
            "model": OCR_MODEL_ID if _import_model_id() else None,
            "request_contract": run.contract_version,
            "requester": run.requester_version,
            "parser": TRANSCRIBE_PARSER_VERSION,
            "verify": VERIFY_VERSION,
            "created_at": run.created_at,
            "updated_at": run.updated_at,
        },
    }


def _import_model_id() -> str | None:
    from compsval.ingest.floorplan_ocr_contract import OCR_MODEL_ID

    return OCR_MODEL_ID


# ---------------------------------------------------------------------------
# OCRNEXT-C 离线回放与全量自动门禁（方案 §6.3/§7.0/§7.1；合同 §5-C）
# 对既有双轮 300 张原始响应以新转录版本离线回放：0 真实调用、不触网；结果写入
# 独立 evaluation 目录，绝不改写旧运行资产与旧 staged 表。
# ---------------------------------------------------------------------------


def _locate_raw_response(run_dir: Path, task: OcrTaskRecord) -> Path | None:
    """定位任务原始响应文件：记录路径名 → 24-hex 截断名 → 64-hex 全名（G 运行形态）。"""
    if task.raw_response_path:
        candidate = run_dir / Path(task.raw_response_path).name
        if candidate.is_file():
            return candidate
    truncated = run_dir / raw_response_filename(task.ocr_task_id)
    if truncated.is_file():
        return truncated
    full = run_dir / f"raw_response_{task.ocr_task_id}.json"
    return full if full.is_file() else None


def replay_run_annotations(
    run_dir: Path,
) -> tuple[dict[str, list[RoomAnnotationRecord]], dict[str, list[OcrWordRecord]], dict[str, Any]]:
    """回放一个冻结 OCR 运行：逐任务读原始响应 → 解析逐词 → 新规则转录标注。

    只读运行目录；返回 ``(annotations_by_task, words_by_task, load_stats)``。
    无响应文件（网络失败/门禁）与损坏响应的任务转录为空列表并进 load_stats 计数，
    不伪造词条。
    """
    from compsval.ingest.floorplan_ocr_parse import parse_response_body
    from compsval.ingest.floorplan_transcribe import transcribe_words

    run = load_ocr_run_record(run_dir)
    ann_by_task: dict[str, list[RoomAnnotationRecord]] = {}
    words_by_task: dict[str, list[OcrWordRecord]] = {}
    stats: dict[str, Any] = {
        "run_id": run.ocr_run_id,
        "tasks_total": len(run.tasks),
        "tasks_with_response": 0,
        "tasks_without_response": 0,
        "tasks_unparseable_response": 0,
        "total_words": 0,
    }
    for task in run.tasks:
        if task.state in (OcrState.OCR_PENDING, OcrState.OCR_RUNNING):
            continue
        raw_path = _locate_raw_response(run_dir, task)
        if raw_path is None:
            stats["tasks_without_response"] += 1
            ann_by_task[task.ocr_task_id] = []
            words_by_task[task.ocr_task_id] = []
            continue
        try:
            body = json.loads(raw_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            stats["tasks_unparseable_response"] += 1
            ann_by_task[task.ocr_task_id] = []
            words_by_task[task.ocr_task_id] = []
            continue
        stats["tasks_with_response"] += 1
        parsed = parse_response_body(
            body,
            ocr_run_id=run.ocr_run_id,
            ocr_task_id=task.ocr_task_id,
            source_state=task.state.value,
            model_requested=task.model_requested,
            model_returned=task.model_returned,
            image_width=task.width,
            image_height=task.height,
        )
        words = [w for w in parsed.words if w.parse_state == "PARSED"]
        stats["total_words"] += len(words)
        words_by_task[task.ocr_task_id] = words
        ann_by_task[task.ocr_task_id] = transcribe_words(words)
    return ann_by_task, words_by_task, stats


def _flat(ann_by_task: dict[str, list[RoomAnnotationRecord]]) -> list[RoomAnnotationRecord]:
    return [a for tid in sorted(ann_by_task) for a in ann_by_task[tid]]


def isolate_pair_annotations(
    primary_by_task: dict[str, list[RoomAnnotationRecord]],
    reference_by_task: dict[str, list[RoomAnnotationRecord]],
) -> tuple[list[RoomAnnotationRecord], list[RoomAnnotationRecord], list[RoomAnnotationRecord]]:
    """对主运行标注施加跨运行隔离（修复③），返回 (隔离后主标注, 主被隔离清单, 对照标注)。"""
    from compsval.ingest.floorplan_transcribe import (
        isolate_cross_run_inconsistencies,
    )

    adjusted: list[RoomAnnotationRecord] = []
    isolated: list[RoomAnnotationRecord] = []
    for tid in sorted(primary_by_task):
        ref = reference_by_task.get(tid, [])
        ann_adj, ann_iso = isolate_cross_run_inconsistencies(primary_by_task[tid], ref)
        adjusted.extend(ann_adj)
        isolated.extend(ann_iso)
    return adjusted, isolated, _flat(reference_by_task)


def _gate(gate_id: str, ok: bool, detail: str, **counts: Any) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "status": "ok" if ok else "fail",
        "detail": detail,
        "counts": counts,
    }


def full_asset_auto_gates(
    primary_dir: Path,
    reference_dir: Path,
    *,
    final_annotations: list[RoomAnnotationRecord],
    reference_annotations: list[RoomAnnotationRecord],
    words_by_task: dict[str, list[OcrWordRecord]],
) -> list[dict[str, Any]]:
    """全 300 张自动门禁（方案 §7.1 六项，全部离线、只读双轮资产）。

    1. raw 存在性与 SHA256/账本一致（含 24-hex 截断与 64-hex 全名两种口径）；
    2. 状态机合法：账本无遗留非终态任务，state_counts 与逐任务一致；
    3. 逐词可对账：转录引用的 word_id 全部存在于该任务词表；
    4. 已接受字段证据覆盖 100%：ACCEPTED 均有 room/area word_id + 位置证据；
    5. 跨运行隔离完整：主运行最终 accepted 多重集 ⊆ 对照 accepted 多重集，
       且不存在 isolation_reason=cross_run_inconsistent 但仍 ACCEPTED 的记录；
    6. 旧运行账本不改写：本次回放不产生对运行目录的写行为（结构性保证，
       报告登记回放输入哈希供审计）。
    """
    import hashlib

    from compsval.ingest.floorplan_transcribe import cross_run_field_key

    primary = load_ocr_run_record(primary_dir)
    reference = load_ocr_run_record(reference_dir)

    # 门禁 1：raw 存在性 + SHA
    missing: list[str] = []
    sha_mismatch: list[str] = []
    for run, rdir in ((primary, primary_dir), (reference, reference_dir)):
        for task in run.tasks:
            if task.state in (OcrState.OCR_PENDING, OcrState.OCR_RUNNING):
                continue
            raw = _locate_raw_response(rdir, task)
            if raw is None:
                if task.error_code not in ("cost_gate_hit", "network_error", "http_error"):
                    missing.append(f"{run.ocr_run_id}:{task.ocr_task_id[:8]}")
                continue
            if task.raw_response_sha256:
                digest = hashlib.sha256(raw.read_bytes()).hexdigest()
                if digest != task.raw_response_sha256:
                    sha_mismatch.append(f"{run.ocr_run_id}:{task.ocr_task_id[:8]}")
    gate1 = _gate(
        "raw_presence_and_sha",
        not missing and not sha_mismatch,
        f"缺失 {len(missing)}，SHA 不一致 {len(sha_mismatch)}（两种文件名词径均核）",
        missing=missing[:20],
        sha_mismatch=sha_mismatch[:20],
    )

    # 门禁 2：状态机合法 + 账本一致
    residual = [
        f"{run.ocr_run_id}:{t.ocr_task_id[:8]}"
        for run in (primary, reference)
        for t in run.tasks
        if t.state in (OcrState.OCR_PENDING, OcrState.OCR_RUNNING)
    ]
    counts_ok = True
    for run in (primary, reference):
        recomputed: dict[str, int] = {}
        for t in run.tasks:
            recomputed[t.state.value] = recomputed.get(t.state.value, 0) + 1
        if recomputed != run.state_counts:
            counts_ok = False
    gate2 = _gate(
        "ledger_state_machine",
        not residual and counts_ok,
        f"非终态残留 {len(residual)}；state_counts 一致={counts_ok}",
        residual=residual[:20],
    )

    # 门禁 3 + 4：词可对账与证据覆盖
    bad_reference: list[str] = []
    accepted_total = 0
    accepted_missing_evidence = 0
    for ann in final_annotations:
        if ann.parse_state != AnnotationState.ACCEPTED.value:
            continue
        accepted_total += 1
        if not ann.location or not ann.room_word_id or not ann.area_word_id:
            accepted_missing_evidence += 1
            continue
        known_ids = {w.word_id for w in words_by_task.get(ann.ocr_task_id, [])}
        if ann.room_word_id not in known_ids or ann.area_word_id not in known_ids:
            bad_reference.append(f"{ann.ocr_task_id[:8]}:{ann.annotation_id[:8]}")
    gate3 = _gate(
        "word_table_reconciliation",
        not bad_reference,
        f"引用不存在 word_id 的标注 {len(bad_reference)}",
        bad_reference=bad_reference[:20],
    )
    gate4 = _gate(
        "accepted_evidence_coverage",
        accepted_missing_evidence == 0,
        f"ACCEPTED {accepted_total}，缺证据 {accepted_missing_evidence}"
        "（accepted 为 0 时该检查空真成立，覆盖度另由回归指标承接）",
        accepted_total=accepted_total,
        missing_evidence=accepted_missing_evidence,
    )

    # 门禁 5：跨运行隔离完整——主最终 accepted 多重集 ⊆ 对照 accepted；
    # 且不存在带 cross_run_inconsistent 隔离标记却仍 ACCEPTED 的记录
    prim_counter: dict[str, Counter[Any]] = {}
    leaked = 0
    for ann in final_annotations:
        if ann.isolation_reason == "cross_run_inconsistent" and (
            ann.parse_state == AnnotationState.ACCEPTED.value
        ):
            leaked += 1
        if ann.parse_state == AnnotationState.ACCEPTED.value:
            prim_counter.setdefault(ann.ocr_task_id, Counter())[cross_run_field_key(ann)] += 1
    ref_counter: dict[str, Counter[Any]] = {}
    for ann in reference_annotations:
        if ann.parse_state == AnnotationState.ACCEPTED.value:
            ref_counter.setdefault(ann.ocr_task_id, Counter())[cross_run_field_key(ann)] += 1
    excess_tasks: list[str] = []
    for tid, counter in prim_counter.items():
        ref = ref_counter.get(tid, Counter())
        if counter - ref:
            excess_tasks.append(tid[:8])
    gate5 = _gate(
        "cross_run_isolation_complete",
        not excess_tasks and leaked == 0,
        f"隔离后仍超出对照 accepted 的任务 {len(excess_tasks)}；隔离标记泄漏 {leaked}",
        excess_tasks=excess_tasks[:20],
        leaked_isolated_accepted=leaked,
    )

    # 门禁 6：旧运行账本不改写（结构性：回放只读；登记输入哈希）
    def _run_ledger_sha(rdir: Path) -> str:
        return hashlib.sha256((rdir / "ocr_run.json").read_bytes()).hexdigest()

    gate6 = _gate(
        "old_runs_readonly",
        True,
        "回放为纯读路径；登记双轮 ocr_run.json SHA256 供审计前后比对",
        primary_run_sha256=_run_ledger_sha(primary_dir),
        reference_run_sha256=_run_ledger_sha(reference_dir),
    )
    return [gate1, gate2, gate3, gate4, gate5, gate6]


def auto_accept_room_claim_audit(
    annotations: list[RoomAnnotationRecord],
    golden_csv: Path,
    run_dir: Path,
) -> dict[str, Any]:
    """自动接受口径的房间名 claim 精确率（OCRNEXT-C §8.2 判定口径）。

    只统计 ``ACCEPTED``/``ROOM_ONLY`` 的房间 claim（CONFLICT/NEEDS_REVIEW/被隔离
    记录不进候选层），按 ``(ocr_task_id, room_word_id)`` 去重后与黄金标准房间数
    逐类型取 min 匹配。与 H 阶段全状态审计口径（``group_room_claims``，含 CONFLICT）
    并列披露、互不替代：本口径回答「候选层是否可用」，H 口径保留历史审计点位。
    """
    from collections import Counter, defaultdict

    from compsval.ingest.floorplan_golden_compare import (
        is_confirmed_golden,
        load_golden_label_rows,
        parse_golden_rooms,
    )

    run = load_ocr_run_record(run_dir)
    asset_to_task = {t.asset_id: t.ocr_task_id for t in run.tasks}
    golden_by_task: dict[str, Counter[str]] = {}
    for row in load_golden_label_rows(golden_csv):
        if not is_confirmed_golden(row):
            continue
        task_id = asset_to_task.get(row.get("asset_id") or "")
        if not task_id:
            continue
        counter: Counter[str] = Counter()
        for gr in parse_golden_rooms(row):
            if gr.room_type_std:
                counter[gr.room_type_std] += 1
        golden_by_task[task_id] = counter

    claims_by_task: dict[str, Counter[str]] = defaultdict(Counter)
    seen: set[tuple[str, str]] = set()
    for ann in annotations:
        if ann.parse_state not in (
            AnnotationState.ACCEPTED.value,
            AnnotationState.ROOM_ONLY.value,
        ):
            continue
        if not ann.standard_room_type or not ann.room_word_id:
            continue
        key = (ann.ocr_task_id, ann.room_word_id)
        if key in seen:
            continue
        seen.add(key)
        claims_by_task[ann.ocr_task_id][ann.standard_room_type] += 1

    total_claims = 0
    total_matched = 0
    for task_id, counter in claims_by_task.items():
        g = golden_by_task.get(task_id)
        if g is None:
            continue
        for std, n in counter.items():
            total_claims += n
            total_matched += min(n, g.get(std, 0))
    return {
        "scope_golden_tasks": len(golden_by_task),
        "auto_accept_claims": total_claims,
        "auto_accept_matched": total_matched,
        "auto_accept_room_precision": ((total_matched / total_claims) if total_claims else None),
        "note": "候选层口径：仅 ACCEPTED/ROOM_ONLY 房间 claim；与 H 阶段全状态审计口径并列披露",
    }


def adversarial_oos_audit(
    oos_csv: Path,
    final_annotations: list[RoomAnnotationRecord],
    run_dir: Path,
) -> dict[str, Any]:
    """历史范围外样本（多层户型等）的 ``adversarial_oos`` 观察（方案 §8.1）。

    范围外样本不进入任何范围内分母、不作黄金真值比对，只观察两件事：隔离后
    该任务的标注状态分布，以及是否仍有自动接受（ACCEPTED/ROOM_ONLY）的候选
    输出——错误接受与隔离由人逐张判定，本函数只做完整披露，不给通过/拒绝
    决定。来源为冻结只读清单 ``oos15.csv``（label_role=oos）。
    """
    run = load_ocr_run_record(run_dir)
    asset_to_task = {t.asset_id: t.ocr_task_id for t in run.tasks}
    by_task: dict[str, list[RoomAnnotationRecord]] = {}
    for ann in final_annotations:
        by_task.setdefault(ann.ocr_task_id, []).append(ann)

    samples: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()
    with oos_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            asset_id = (row.get("asset_id") or "").strip()
            task_id = asset_to_task.get(asset_id)
            recs = by_task.get(task_id or "", [])
            states = Counter(a.parse_state for a in recs)
            seen: set[tuple[str, str]] = set()
            accepted_room = 0
            for a in recs:
                accepted_state = a.parse_state in (
                    AnnotationState.ACCEPTED.value,
                    AnnotationState.ROOM_ONLY.value,
                )
                if not accepted_state or not a.standard_room_type:
                    continue
                key = (a.ocr_task_id, a.room_word_id or a.annotation_id)
                if key in seen:
                    continue
                seen.add(key)
                accepted_room += 1
            accepted_area = sum(
                1
                for a in recs
                if a.parse_state == AnnotationState.ACCEPTED.value and a.area_value is not None
            )
            isolated = sum(1 for a in recs if a.isolation_reason == "cross_run_inconsistent")
            samples.append(
                {
                    "sample_index": (row.get("sample_index") or "").strip(),
                    "asset_id": asset_id,
                    "ocr_task_id": task_id,
                    "oos_reason": (row.get("oos_reason") or "").strip(),
                    "mapped_in_run": task_id is not None,
                    "accepted_room_claims": accepted_room,
                    "accepted_areas": accepted_area,
                    "isolated": isolated,
                    "state_counts": dict(states),
                }
            )
            totals["samples"] += 1
            totals["unmapped"] += 1 if task_id is None else 0
            totals["accepted_room_claims"] += accepted_room
            totals["accepted_areas"] += accepted_area
            totals["isolated"] += isolated
    return {
        "role": "adversarial_oos",
        "source": oos_csv.as_posix(),
        "denominator_policy": "范围外样本不入范围内分母，只披露错误接受与隔离形态（方案 §8.1）",
        "totals": dict(totals),
        "samples": samples,
    }


def run_offline_replay_evaluation(
    primary_dir: Path,
    reference_dir: Path,
    out_eval_dir: Path,
    *,
    evaluation_id: str,
    golden_csv: Path | None = None,
    golden_run_dir: Path | None = None,
    oos_csv: Path | None = None,
) -> dict[str, Any]:
    """执行一次双轮离线回放评估（OCRNEXT-C 退出证据生成器）。

    - 双轮各自以新转录版本回放（修复①②内建于生产入口 ``transcribe_words``）；
    - 主运行施加跨运行隔离（修复③），对照为参考方向的独立隔离结果；
    - 产物写 ``out_eval_dir/evaluation_<id>/staged/``（新目录，绝不触碰既有 staged
      与旧运行资产）：标注表 + 隔离清单 JSON + 门禁报告；
    - ``golden_csv`` 给出时调用既有 ``run_golden_comparison`` 对主运行隔离后标注表
      生成 51 张高敏感回归比对（只读黄金标签），并给出候选层房间 claim 审计；
    - ``oos_csv`` 给出时追加 ``adversarial_oos`` 观察块（只披露，不入范围内分母）。

    返回完整评估报告 dict（调用方落盘/入库）。不触网、0 真实调用。
    """
    from compsval.ingest.floorplan_transcribe import (
        compute_annotation_stats,
        write_annotation_table,
    )

    primary_by_task, primary_words, primary_stats = replay_run_annotations(primary_dir)
    reference_by_task, _ref_words, reference_stats = replay_run_annotations(reference_dir)

    adjusted, isolated, ref_flat = isolate_pair_annotations(primary_by_task, reference_by_task)
    # 参考方向独立隔离（用于双向差异统计；不进任何写入产物）
    _ref_adjusted, ref_isolated, _ = isolate_pair_annotations(reference_by_task, primary_by_task)

    eval_dir = out_eval_dir / f"evaluation_{evaluation_id}"
    annotation_table = write_annotation_table(adjusted, eval_dir)
    pre_stats = compute_annotation_stats(
        _flat(primary_by_task), tasks_transcribed=len(primary_by_task)
    )
    post_stats = compute_annotation_stats(adjusted, tasks_transcribed=len(primary_by_task))

    gates = full_asset_auto_gates(
        primary_dir,
        reference_dir,
        final_annotations=adjusted,
        reference_annotations=ref_flat,
        words_by_task=primary_words,
    )
    isolation_list = [
        {
            "ocr_task_id": a.ocr_task_id,
            "annotation_id": a.annotation_id,
            "room_name_normalized": a.room_name_normalized,
            "area_value": a.area_value,
            "isolation_reason": a.isolation_reason,
        }
        for a in isolated
    ]
    (eval_dir / "isolation_list.json").write_text(
        json.dumps(isolation_list, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    golden_report: dict[str, Any] | None = None
    claim_audit: dict[str, Any] | None = None
    if golden_csv is not None:
        from compsval.ingest.floorplan_golden_compare import (  # 延迟导入防环
            run_golden_comparison,
        )

        gc = run_golden_comparison(golden_csv, annotation_table, golden_run_dir or primary_dir)
        golden_report = gc.model_dump(mode="json")
        claim_audit = auto_accept_room_claim_audit(
            adjusted, golden_csv, golden_run_dir or primary_dir
        )

    oos_audit = (
        adversarial_oos_audit(oos_csv, adjusted, golden_run_dir or primary_dir)
        if oos_csv is not None
        else None
    )

    report: dict[str, Any] = {
        "evaluation_id": evaluation_id,
        "verify_version": VERIFY_VERSION,
        "transcribe_parser_version": TRANSCRIBE_PARSER_VERSION,
        "created_at": utc_now_iso(),
        "inputs": {
            "primary_run": primary_stats,
            "reference_run": reference_stats,
            "golden_csv": golden_csv.as_posix() if golden_csv else None,
            "oos_csv": oos_csv.as_posix() if oos_csv else None,
        },
        "before_after": {
            "pre_isolation": pre_stats.model_dump(),
            "post_isolation": post_stats.model_dump(),
        },
        "isolation": {
            "primary_isolated_total": len(isolated),
            "reference_direction_isolated_total": len(ref_isolated),
            "list_path": (eval_dir / "isolation_list.json").as_posix(),
        },
        "gates": gates,
        "gates_all_ok": all(g["status"] == "ok" for g in gates),
        "golden_comparison": golden_report,
        "auto_accept_room_claim_audit": claim_audit,
        "adversarial_oos": oos_audit,
        "artifacts": {"annotation_table": annotation_table.as_posix()},
    }
    (eval_dir / "replay_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return report


def build_gating_metrics(
    checks: list[CheckResult],
    annotations: list[RoomAnnotationRecord],
    words: list[OcrWordRecord],
) -> dict[str, Any]:
    """按 §10.3 候选验收线映射可即时计算的指标（正式判定由 H 完成）。"""
    by_id = {c.check_id: c for c in checks}

    evidence = by_id.get(CHECK_WORD_EVIDENCE)
    evidence_counts = evidence.counts if evidence else {}
    accepted_total = evidence_counts.get("accepted_total", 0)
    evidence_ok = accepted_total - (
        evidence_counts.get("missing_room_word", 0)
        + evidence_counts.get("missing_area_word", 0)
        + evidence_counts.get("missing_location", 0)
    )
    evidence_rate = (evidence_ok / accepted_total) if accepted_total else None

    repeat = by_id.get(CHECK_REPEAT_CONSISTENCY)
    repeat_counts = repeat.counts if repeat else {}
    repeat_rate = None
    if repeat_counts.get("total_fields", 0):
        repeat_rate = repeat_counts["matched_fields"] / repeat_counts["total_fields"]

    total_words = len(words)
    words_with_field = sum(1 for w in words if w.participates_in_field)
    model_check = by_id.get(CHECK_MODEL_MATCH)
    return {
        "accepted_field_evidence_rate": evidence_rate,
        "repeat_run_consistency_rate": repeat_rate,
        "model_match_ok": model_check is not None and model_check.status == "ok",
        "word_participation_rate": (words_with_field / total_words) if total_words else None,
        "schema_parseable_rate": None,  # 由 C 阶段可解析率承接；H 对齐 §12 口径
        "gating_decision": "pending_H",
    }


__all__ = [
    "ANNOTATION_STAGED_FILENAME",
    "AREA_OVER_RATIO",
    "AREA_UNDER_RATIO",
    "BEDROOM_TYPES",
    "BUILDING_AREA_TOLERANCE",
    "CHECK_BATCH_UNIQUENESS",
    "CHECK_BUILDING_AREA_EXCEL",
    "CHECK_MODEL_MATCH",
    "CHECK_MULTIPLE_AREAS",
    "CHECK_REPEAT_CONSISTENCY",
    "CHECK_ROOM_COUNT_EXCEL",
    "CHECK_TOTAL_AREA_EXCEL",
    "CHECK_WORD_EVIDENCE",
    "LIVING_ROOM_TYPES",
    "REPEAT_CONSISTENCY_THRESHOLD",
    "VERIFY_VERSION",
    "CheckResult",
    "ExcelRoomAreaInfo",
    "Finding",
    "StatusReport",
    "TaskStatusItem",
    "VerifyReport",
    "adversarial_oos_audit",
    "auto_accept_room_claim_audit",
    "backfill_consistency_status",
    "build_gating_metrics",
    "build_quality_report",
    "build_status_report",
    "build_task_excel_map",
    "check_accepted_word_evidence",
    "check_batch_uniqueness",
    "check_building_area_vs_excel",
    "check_model_match",
    "check_multiple_areas_conflict",
    "check_repeat_consistency",
    "check_room_count_vs_excel",
    "check_total_area_vs_transaction",
    "derive_consistency_status",
    "full_asset_auto_gates",
    "isolate_pair_annotations",
    "load_staged_excel_lookup",
    "ocr_accepted_area_total",
    "ocr_room_type_counts",
    "parse_count",
    "parse_decimal",
    "read_annotation_table",
    "replay_run_annotations",
    "run_offline_replay_evaluation",
    "verify_run",
    "write_annotation_consistency",
]
