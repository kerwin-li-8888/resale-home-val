"""EXTFP3-H 验收聚合：十项指标、成本报告与通过/拒绝决定（技术方案 §10.3/§11/§17）。

把 EXTFP3-G 已产出的 300 张验收 OCR 运行、确定性转录标注表与黄金标签比对结果聚合为
§10.3 十项候选验收线指标，输出成本报告（按 300 张实际 usage 外推 208,075 张正式批次
预算参考）与「通过 / 拒绝」决定记录。

十项指标来源映射（§10.3）：

| # | 指标 | 来源 | 门槛 |
|---|---|---:|---:|
| H1 | 成功图片格式/哈希/尺寸/来源关联完整率 | OCR 运行记录 tasks | 100% |
| H2 | 已接受 OCR 数值字段有原文和位置证据 | 标注表 ACCEPTED 证据覆盖 | 100% |
| H3 | 面积数值精确率 | 黄金比对 aggregate.area_precision | 100% |
| H4 | 房间名称精确率 | 黄金比对 aggregate.room_precision | ≥99% |
| H5 | 房间名称召回率（标准词表口径） | 黄金比对 aggregate.room_recall_standard | ≥97% |
| H6 | 面积字段召回率（覆盖度口径，用户确认） | 黄金比对 aggregate.area_recall_coverage | ≥95% |
| H7 | 原图没有但系统生成的有效面积 | 黄金比对 aggregate.false_positive_areas | =0 |
| H8 | schema/响应可解析率 | OCR 运行响应重解析 WordRunStats | ≥99.5% |
| H9 | 固定图片重复运行有效字段一致率 | 第二次固定图片运行标注表 | ≥99.5% |
| H10 | 失败记录可重试、可分类、可追溯 | status 报告失败分类 | 100% |

决定规则（证据驱动，不静默放宽 §10.3）：

- 任一指标 `pass_ok=False` → **拒绝（REJECT）**；
- 全部达标但存在未测指标（`pass_ok=None`，如 H9 待复跑）→ **待定（PENDING）**；
- 全部达标且无未测项 → **通过（PASS）**。

范围边界（EXTFP3-H 合同）：本模块只做**只读聚合 + 指标计算 + 报告生成**，不触网、不付费、
不改写黄金标签/OCR 运行/标注表；300 张真实付费 OCR 已在 EXTFP3-G 完成（G#F3 记录），
H9 复跑属授权范围内的一次性固定图片重复运行，由 CLI/用户显式触发。通过/拒绝为 H 阶段
正式决定，写回任务状态文件。
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_golden_compare import (
    GOLDEN_COMPARE_VERSION,
    GoldenIndicatorAggregate,
    run_golden_comparison,
)
from compsval.ingest.floorplan_ocr import OcrRunRecord, OcrState
from compsval.ingest.floorplan_ocr_parse import (
    WordRunStats,
    load_ocr_run_record,
    parse_ocr_run_directory,
)
from compsval.ingest.floorplan_transcribe import AnnotationState
from compsval.ingest.floorplan_verify import (
    build_status_report,
    check_repeat_consistency,
    read_annotation_table,
)

# H 验收报告版本（写入报告）
ACCEPTANCE_H_VERSION = "EXTFP3-H-AGG-1.0"

# 正式批次外推图片数（技术方案 §11.2/§17；本包只给预算参考，不授权处理）
PROJECTION_IMAGE_COUNT = 208_075

# 官方价格基线（§11.1，2026-08-25 复核：输入 0.3 / 输出 0.5 元每百万 Token）
INPUT_PRICE_PER_MT_YUAN = Decimal("0.3")
OUTPUT_PRICE_PER_MT_YUAN = Decimal("0.5")

# H1 成功图片口径：OCR 处理成功的终态（SUCCEEDED + PARTIAL）
_H1_ELIGIBLE_STATES = frozenset({OcrState.OCR_SUCCEEDED, OcrState.OCR_PARTIAL})

# H10 失败记录口径：失败/部分/待复核任务
_H10_FAILURE_STATES = frozenset({OcrState.OCR_FAILED, OcrState.NEEDS_REVIEW, OcrState.OCR_PARTIAL})


class HIndicator(BaseModel):
    """一项验收指标（§10.3）。``pass_ok``：True 达标 / False 未达标 / None 未测或不可用。"""

    id: str
    name: str
    threshold: str
    value: float | int | None = None
    unit: str = ""
    pass_ok: bool | None = None
    detail: str = ""


class HCostReport(BaseModel):
    """成本报告：300 张实际 usage + 208,075 张正式批次外推预算参考（§11）。"""

    ocr_run_id: str
    images_actual: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_tokens: int = 0
    total_cost_yuan: Decimal = Decimal("0")
    cost_per_image_yuan_avg: Decimal | None = None
    hard_cap_yuan: Decimal = Decimal("0")
    projection_images: int = PROJECTION_IMAGE_COUNT
    projected_cost_yuan: Decimal | None = None
    projected_disk_gb: float | None = None
    note: str = ""


class HAcceptanceReport(BaseModel):
    """EXTFP3-H 验收报告（画像报告 JSON，机器可读；只含非敏感配置与统计）。"""

    workpackage: str = "EXTFP3-H"
    name: str = "300 张验收真实运行、指标计算与通过/拒绝决定"
    version: str = ACCEPTANCE_H_VERSION
    golden_compare_version: str = GOLDEN_COMPARE_VERSION
    created_at: str
    ocr_run_id: str
    golden_confirmed_samples: int = 0
    golden: GoldenIndicatorAggregate = Field(default_factory=GoldenIndicatorAggregate)
    indicators: list[HIndicator] = Field(default_factory=list)
    cost: HCostReport | None = None
    decision: str = Field(description="PASS / REJECT / PENDING")
    decision_reason: str = ""
    failures: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# H1 / H2 / H8 / H10：非黄金指标（直接由 OCR 运行与标注表计算）
# ---------------------------------------------------------------------------


def _indicator(
    id: str,
    name: str,
    threshold: str,
    value: float | int | None,
    pass_ok: bool | None,
    detail: str,
    unit: str = "",
) -> HIndicator:
    return HIndicator(
        id=id,
        name=name,
        threshold=threshold,
        value=value,
        unit=unit,
        pass_ok=pass_ok,
        detail=detail,
    )


def compute_h1_completeness(run: OcrRunRecord) -> HIndicator:
    """H1：成功图片的格式、哈希、尺寸和来源关联完整率（100%）。

    成功图片 = OCR_SUCCEEDED + OCR_PARTIAL 终态任务；完整性 = 同时具备
    image_sha256（哈希）、width/height（尺寸）、mime_type（格式）与 asset_id（来源关联）。
    """
    eligible = [t for t in run.tasks if t.state in _H1_ELIGIBLE_STATES]
    ok = [
        t
        for t in eligible
        if t.image_sha256
        and t.width is not None
        and t.height is not None
        and t.mime_type
        and t.asset_id
    ]
    total = len(eligible)
    rate = (len(ok) / total) if total else None
    missing = total - len(ok)
    detail = f"成功图片 {len(ok)}/{total}"
    if missing:
        detail += f"，缺证据 {missing} 张"
    return _indicator(
        "H1",
        "成功图片格式/哈希/尺寸/来源关联完整率",
        "100%",
        rate,
        (rate is not None and rate == 1.0),
        detail,
        unit="比例",
    )


def compute_h2_evidence(annotations: list[Any]) -> HIndicator:
    """H2：已接受 OCR 数值字段有原文和位置证据（100%）。

    每个 ACCEPTED 标注必须回指 room_word_id 且有 location；带面积的必须同时回指
    area_word_id（原文证据）。缺任一 → 该字段不计入证据完整。
    """
    accepted = [a for a in annotations if a.parse_state == AnnotationState.ACCEPTED.value]
    ok = [
        a
        for a in accepted
        if a.room_word_id and a.location and (a.area_value is None or a.area_word_id)
    ]
    total = len(accepted)
    rate = (len(ok) / total) if total else None
    detail = f"ACCEPTED {len(ok)}/{total} 条证据完整"
    return _indicator(
        "H2",
        "已接受 OCR 数值字段有原文和位置证据",
        "100%",
        rate,
        (rate is not None and rate == 1.0),
        detail,
        unit="比例",
    )


def compute_h8_parseability(stats: WordRunStats) -> HIndicator:
    """H8：schema/响应可解析率（≥99.5%）。

    复用 C 阶段 ``parse_ocr_run_directory`` 的重解析统计（只读原始响应，不触网）。
    """
    rate = stats.parseable_rate
    detail = f"有响应任务 {stats.tasks_with_response}，解析出单词 {stats.total_words} 个"
    return _indicator(
        "H8",
        "schema/响应可解析率",
        "≥99.5%",
        rate,
        (rate is not None and rate >= 0.995),
        detail,
        unit="比例",
    )


def compute_h9_repeat(
    annotations: list[Any],
    repeat_annotations: list[Any] | None,
) -> HIndicator:
    """H9：固定图片重复运行的有效字段一致率（≥99.5%）。

    需要第二次固定图片运行的标注表；未提供时标记 pending（待复跑），不静默视为达标。
    """
    if not repeat_annotations:
        return _indicator(
            "H9",
            "固定图片重复运行有效字段一致率",
            "≥99.5%",
            None,
            None,
            "待复跑（需第二次固定图片运行标注表；未提供不视为达标）",
        )
    check = check_repeat_consistency(annotations, repeat_annotations)
    counts = check.counts
    total = counts.get("total_fields", 0)
    matched = counts.get("matched_fields", 0)
    rate = (matched / total) if total else None
    detail = f"重复任务 {counts.get('common_tasks', 0)}，一致 {matched}/{total}"
    return _indicator(
        "H9",
        "固定图片重复运行有效字段一致率",
        "≥99.5%",
        rate,
        (rate is not None and rate >= 0.995),
        detail,
        unit="比例",
    )


def compute_h10_failures(status: Any) -> HIndicator:
    """H10：失败记录可重试、可分类、可追溯（100%）。

    失败口径 = OCR_FAILED + NEEDS_REVIEW + OCR_PARTIAL 任务；每条须同时满足：
    可分类（有 error_code/response_status/finish_reason）、可追溯（原始响应已落盘）、
    可重试（``_retryable`` 判定）。无失败任务时视为 100%（空真）。
    """
    failures = status.failures
    total = len(failures)
    ok = sum(
        1
        for f in failures
        if (f.error_code or f.response_status or f.finish_reason)
        and f.raw_response_present
        and f.retryable
    )
    rate = (ok / total) if total else None
    detail = f"失败记录 {ok}/{total} 条可重试/可分类/可追溯" if total else "无失败记录（空真 100%）"
    return _indicator(
        "H10",
        "失败记录可重试、可分类、可追溯",
        "100%",
        rate,
        (rate is None or rate == 1.0),
        detail,
        unit="比例",
    )


# ---------------------------------------------------------------------------
# H3—H7：黄金标签五项指标（直接引用黄金比对聚合）
# ---------------------------------------------------------------------------


def golden_indicators(agg: GoldenIndicatorAggregate) -> list[HIndicator]:
    """把黄金比对聚合映射为 H3—H7 五项指标。"""
    return [
        _indicator(
            "H3",
            "面积数值精确率",
            "100%（宁可返回未知，不接受错误数字）",
            agg.area_precision,
            agg.area_precision_pass,
            f"面积 claim {agg.area_claims}，精确匹配 {agg.area_claims_matched_exact}",
            unit="比例",
        ),
        _indicator(
            "H4",
            "房间名称精确率",
            "≥99%",
            agg.room_precision,
            agg.room_precision_pass,
            f"房间 claim {agg.room_claims}，命中黄金 {agg.room_claims_matched}",
            unit="比例",
        ),
        _indicator(
            "H5",
            "房间名称召回率（标准词表口径）",
            "≥97%",
            agg.room_recall_standard,
            agg.room_recall_pass,
            f"黄金标准词表房间 {agg.golden_std_rooms}，命中 {agg.room_claims_matched}",
            unit="比例",
        ),
        _indicator(
            "H6",
            "面积字段召回率（覆盖度口径）",
            "≥95%，黄金标准面积被任意解析状态转录捕获为分母（用户 2026-08-27 确认）",
            agg.area_recall_coverage,
            agg.area_recall_coverage_pass,
            f"黄金标准面积 {agg.golden_std_areas}，覆盖 {agg.golden_std_areas_covered}"
            f"（精确口径诊断 {agg.area_recall_standard:.4f}）",
            unit="比例",
        ),
        _indicator(
            "H7",
            "原图没有但系统生成的有效面积",
            "=0",
            agg.false_positive_areas,
            agg.fp_areas_pass,
            f"系统生成黄金外有效面积 {agg.false_positive_areas}",
            unit="个",
        ),
    ]


# ---------------------------------------------------------------------------
# 成本报告（§11）：实际 usage + 正式批次外推预算参考
# ---------------------------------------------------------------------------


def build_cost_report(run: OcrRunRecord, run_dir: Path) -> HCostReport:
    """按 300 张 tasks 逐条求和 Token 与费用（G#F1 口径：不用 ocr_run.json#cost 聚合块）。

    官方单价（§11.1）：输入 0.3 / 输出 0.5 元每百万 Token；磁盘外推按 G 容量实测
    （47.09MB/300 张，平均 161KB/张）→ 208,075 张。
    """
    prompt = 0
    completion = 0
    for t in run.tasks:
        prompt += t.prompt_tokens or 0
        completion += t.completion_tokens or 0
    total_tokens = prompt + completion
    cost = (
        Decimal(prompt) * INPUT_PRICE_PER_MT_YUAN + Decimal(completion) * OUTPUT_PRICE_PER_MT_YUAN
    ) / Decimal("1_000_000")
    images = len(run.tasks)
    per_image = (cost / Decimal(images)) if images else None

    # 磁盘外推：G#F2 实测 300 张 47.09MB（口径见 EXTFP3-G.json download.total_size_bytes）
    disk_actual_bytes = 49_373_485
    per_image_bytes = disk_actual_bytes / images if images else 0
    projected_disk_gb = (per_image_bytes * PROJECTION_IMAGE_COUNT) / (1024**3)

    return HCostReport(
        ocr_run_id=run.ocr_run_id,
        images_actual=images,
        total_prompt_tokens=prompt,
        total_completion_tokens=completion,
        total_tokens=total_tokens,
        total_cost_yuan=cost,
        cost_per_image_yuan_avg=per_image,
        hard_cap_yuan=Decimal("30"),
        projection_images=PROJECTION_IMAGE_COUNT,
        projected_cost_yuan=(
            per_image * Decimal(PROJECTION_IMAGE_COUNT) if per_image is not None else None
        ),
        projected_disk_gb=round(projected_disk_gb, 2),
        note=(
            "按 300 张实际 usage 逐任务求和 + 官方单价（输入 0.3/输出 0.5 元每百万 Token）"
            "重算（G#F1 口径，不改写 ocr_run.json#cost 聚合块）；正式批次外推仅预算参考，"
            "不授权处理（EXTFP4 范畴）"
        ),
    )


# ---------------------------------------------------------------------------
# 主入口：聚合全部指标 + 成本 + 决定
# ---------------------------------------------------------------------------


def assemble_indicators(
    run: OcrRunRecord,
    run_dir: Path,
    annotations: list[Any],
    stats: WordRunStats,
    golden_agg: GoldenIndicatorAggregate,
    repeat_annotations: list[Any] | None,
) -> list[HIndicator]:
    """组装十项指标（H1—H10）。"""
    status = build_status_report(run, run_dir=run_dir)
    indicators = [
        compute_h1_completeness(run),
        compute_h2_evidence(annotations),
        *golden_indicators(golden_agg),
        compute_h8_parseability(stats),
        compute_h9_repeat(annotations, repeat_annotations),
        compute_h10_failures(status),
    ]
    return indicators


def decide(indicators: list[HIndicator]) -> tuple[str, str]:
    """按 §10.3 决定规则输出通过/拒绝/待定。"""
    failed = [i for i in indicators if i.pass_ok is False]
    pending = [i for i in indicators if i.pass_ok is None]
    if failed:
        names = "、".join(i.id for i in failed)
        return (
            "REJECT",
            f"{names} 未达 §10.3 门槛，拒绝本次验收（详见各指标 detail）",
        )
    if pending:
        names = "、".join(i.id for i in pending)
        return (
            "PENDING",
            f"{names} 未测（待复跑/待数据），暂不能给出最终通过决定",
        )
    return "PASS", "十项指标全部达标"


def run_acceptance_h(
    golden_csv: Path,
    annotation_table: Path,
    ocr_run_dir: Path,
    *,
    repeat_annotation_table: Path | None = None,
    out_json: Path | None = None,
) -> HAcceptanceReport:
    """主入口：读黄金标签 + OCR 运行 + 标注表，聚合十项指标并生成 H 验收报告。

    - ``golden_csv``：golden_label_final.csv（只取备注含「已确认」且非范围外的行）；
    - ``ocr_run_dir``：300 张验收 OCR 运行目录；
    - ``annotation_table``：staged/floorplan_room_annotation.parquet；
    - ``repeat_annotation_table``：第二次固定图片运行标注表（H9 复跑；缺省则 H9 待测）。
    """
    golden_report = run_golden_comparison(golden_csv, annotation_table, ocr_run_dir)

    run = load_ocr_run_record(ocr_run_dir)
    annotations = read_annotation_table(annotation_table)
    stats = parse_ocr_run_directory(ocr_run_dir, data_dir=None)

    repeat_annotations: list[Any] | None = None
    if repeat_annotation_table is not None and repeat_annotation_table.is_file():
        repeat_annotations = read_annotation_table(repeat_annotation_table)

    indicators = assemble_indicators(
        run,
        ocr_run_dir,
        annotations,
        stats,
        golden_report.aggregate,
        repeat_annotations,
    )
    decision, reason = decide(indicators)
    failures = list(golden_report.failures)
    for i in indicators:
        if i.pass_ok is False:
            failures.append(f"{i.id} {i.name} 未达门槛（{i.detail}）")

    report = HAcceptanceReport(
        created_at=datetime.now(UTC).isoformat(),
        ocr_run_id=run.ocr_run_id,
        golden_confirmed_samples=golden_report.confirmed_count,
        golden=golden_report.aggregate,
        indicators=indicators,
        cost=build_cost_report(run, ocr_run_dir),
        decision=decision,
        decision_reason=reason,
        failures=failures,
    )
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(report.model_dump_json(indent=2, ensure_ascii=False), encoding="utf-8")
        work.replace(out_json)
    return report


__all__ = [
    "ACCEPTANCE_H_VERSION",
    "PROJECTION_IMAGE_COUNT",
    "HIndicator",
    "HCostReport",
    "HAcceptanceReport",
    "compute_h1_completeness",
    "compute_h2_evidence",
    "compute_h8_parseability",
    "compute_h9_repeat",
    "compute_h10_failures",
    "golden_indicators",
    "build_cost_report",
    "assemble_indicators",
    "decide",
    "run_acceptance_h",
]
