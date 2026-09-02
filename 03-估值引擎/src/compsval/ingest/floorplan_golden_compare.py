"""黄金标签比对与验收指标（EXTFP3-H，技术方案 §10.3）。

把「已确认」黄金标签（``golden_label_final.csv``，备注含「已确认」，用户对原图人工
标注，生产 OCR 不参与生成标准答案）与确定性转录标注表
（``floorplan_room_annotation.parquet``）按 ``asset_id → ocr_task_id`` 关联，计算
§10.3 候选验收线中依赖黄金标签的五项指标：

1. **面积数值精确率**（≥100%）：已接受面积字段中与黄金「同房型 + 同面积值」一致的比例；
2. **房间名称精确率**（≥99%）：系统产出房间名中出现在黄金房间清单的比例；
3. **房间名称召回率**（≥97%）：黄金标准词表房间中被系统产出的比例；
4. **面积字段召回率**（≥95%，以原图确有明确标注为分母，覆盖度口径，用户 2026-08-27
   确认）：黄金明确标注面积中被系统在任意解析状态转录捕获的比例；关联正确与否由
   面积精确率（第 1 项）与证据完整（H2）承担，避免与「宁可返回未知」政策冲突；
   （精确口径 ``area_recall_standard`` 保留为诊断数字）；
5. **原图没有但系统生成的有效面积**（=0）：系统产出但黄金没有的已接受面积数。

比对口径（确定性、可复现）：

- **关联键**：黄金 ``asset_id`` ↔ OCR 运行 tasks ``asset_id`` ↔ ``ocr_task_id`` ↔
  标注表 ``ocr_task_id``；
- **房间名 claim**：按 ``(ocr_task_id, room_word_id)`` 去重（同一房间多面积候选只算
  一个房间 claim）；含房间名的全部解析状态（ACCEPTED/ROOM_ONLY/CONFLICT/NEEDS_REVIEW）
  均算一个房间 claim；``room_word_id`` 缺失时按 ``annotation_id`` 独立计数；
- **面积 claim**：仅 ``ACCEPTED`` 且有可解析 ``area_value`` 的标注；同一房间多个面积
  （CONFLICT）不进入已接受面积（宁可返回未知，不接受错误数字 §10.3）；
- **黄金归一**：黄金房间先经 ``classify_room`` 归一到标准词表；无法归一的房间
  （过道/入户花园/门厅/洗衣房等，不在转录词表）属系统提取范围外：
  * **标准词表范围**（验收线口径）：召回率分母 = 黄金中 ``room_type_std`` 非 None 的房间；
  * **全量范围**（诊断口径）：召回率分母 = 黄金全部房间，透明展示范围外房间的覆盖缺口
    （G-R6 既有转录丢房；H 报告同时给两个数字）；
- **面积匹配**：Decimal 数值相等（15.30 == 15.3），房间类型须一致；另给 ±0.05 容忍
  匹配数作诊断（验收线用精确匹配）；
- **房型感知覆盖度**（H6 候选口径，诊断用）：黄金标准面积 ``(房型, 值)`` 是否被
  任意解析状态（ACCEPTED/ROOM_ONLY/CONFLICT/NEEDS_REVIEW）的转录捕获，给出
  ``area_recall_coverage``；该口径只判「面积是否被系统读到」，不判「关联是否正确」，
  不作为 §10.3 正式口径，除非用户依据证据确认后修订门槛。

范围边界（EXTFP3-H 合同）：本模块只做**只读比对 + 指标聚合**，不触网、不付费、不改写
黄金标签/OCR 运行/标注表；「已确认」判定依据黄金 CSV 备注。通过/拒绝决定与十项门槛
汇总由 EXTFP3-H 聚合模块/CLI 承接。
"""

from __future__ import annotations

import csv
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_acceptance import (
    GOLDEN_LABEL_CSV_COLUMNS,
    GoldenLabelRoom,
    _parse_room_list,
)
from compsval.ingest.floorplan_ocr_parse import load_ocr_run_record
from compsval.ingest.floorplan_transcribe import AnnotationState
from compsval.ingest.floorplan_verify import (
    parse_decimal,
    read_annotation_table,
)

# 黄金比对模块版本（写入报告）
GOLDEN_COMPARE_VERSION = "EXTFP3-H-GC-1.0"

# 面积匹配容忍（诊断用；验收线用精确匹配）
AREA_TOLERANCE_SQM = Decimal("0.05")

# §10.3 候选验收线（黄金标签相关五项；其余五项由 EXTFP3-H 聚合模块承接）
THRESHOLD_AREA_PRECISION: float | None = 1.0  # 面积数值精确率 ≥100%
THRESHOLD_ROOM_PRECISION: float | None = 0.99  # 房间名称精确率 ≥99%
THRESHOLD_ROOM_RECALL: float | None = 0.97  # 房间名称召回率 ≥97%
THRESHOLD_AREA_RECALL: float | None = 0.95  # 面积字段召回率 ≥95%（精确口径，诊断保留）
# 面积字段召回率 ≥95%（覆盖度口径，用户 2026-08-27 确认，EXTFP3-H 正式口径）：
# 黄金标准面积被任意解析状态转录捕获即算召回；关联质量由面积精确率 H3 与证据完整 H2 承担。
THRESHOLD_AREA_RECALL_COVERAGE: float | None = 0.95
THRESHOLD_FP_AREAS: int = 0  # 原图没有但系统生成的有效面积 = 0

# 备注中「已确认」标记子串（人工复核确认完成；未确认不作真值）
_CONFIRMED_MARK = "已确认"
_OOS_MARK = "范围外"


class SampleComparison(BaseModel):
    """一个已确认样本的黄金 ↔ 转录比对明细（只读聚合，无原始数据外泄）。"""

    sample_index: int
    ocr_task_id: str | None = None
    image_text_category: str = ""
    golden_rooms_total: int = 0
    golden_std_rooms: int = 0
    room_claims: int = 0
    room_claims_matched: int = 0
    golden_std_areas: int = 0
    golden_all_areas: int = 0
    golden_std_areas_covered: int = 0
    area_claims: int = 0
    area_claims_matched_exact: int = 0
    area_claims_matched_tol: int = 0
    false_positive_areas: int = 0


class GoldenIndicatorAggregate(BaseModel):
    """黄金标签相关五项指标的聚合（微平均，跨样本求和后取比）。"""

    samples_compared: int = 0
    samples_with_golden_rooms: int = 0
    room_claims: int = 0
    room_claims_matched: int = 0
    golden_std_rooms: int = 0
    golden_all_rooms: int = 0
    area_claims: int = 0
    area_claims_matched_exact: int = 0
    area_claims_matched_tol: int = 0
    golden_std_areas: int = 0
    golden_all_areas: int = 0
    golden_std_areas_covered: int = 0
    false_positive_areas: int = 0

    # 派生比率（None = 无数据，not_applicable）
    room_precision: float | None = None
    room_recall_standard: float | None = None
    room_recall_full: float | None = None
    area_precision: float | None = None
    area_recall_standard: float | None = None
    area_recall_full: float | None = None
    area_recall_coverage: float | None = None  # 房型感知覆盖度（H6 候选口径，诊断用）

    # §10.3 门槛判定（True 达标 / False 未达标 / None 不适用）
    area_precision_pass: bool | None = None
    room_precision_pass: bool | None = None
    room_recall_pass: bool | None = None
    area_recall_pass: bool | None = None
    area_recall_coverage_pass: bool | None = None
    fp_areas_pass: bool | None = None


class GoldenComparisonReport(BaseModel):
    """黄金比对报告（只读聚合，供 EXTFP3-H 十项汇总引用）。"""

    version: str = GOLDEN_COMPARE_VERSION
    generated_at: str
    golden_csv: str
    annotation_table: str
    ocr_run: str
    confirmed_count: int = 0
    aggregate: GoldenIndicatorAggregate = Field(default_factory=GoldenIndicatorAggregate)
    per_sample: list[SampleComparison] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)


def load_golden_label_rows(csv_path: Path) -> list[dict[str, str]]:
    """读取黄金标签 CSV 全部行（保留原始字段，不筛选）。"""
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            rows.append({k: (raw.get(k) or "").strip() for k in GOLDEN_LABEL_CSV_COLUMNS})
    return rows


def is_confirmed_golden(row: dict[str, str]) -> bool:
    """黄金行是否「已确认」（备注含已确认且非范围外；未确认不作真值）。"""
    note = row.get("备注") or ""
    category = row.get("图片文字类别") or ""
    return _CONFIRMED_MARK in note and _OOS_MARK not in note and _OOS_MARK not in category


def parse_golden_rooms(row: dict[str, str]) -> list[GoldenLabelRoom]:
    """解析黄金行「房间清单」为房间列表（含分类；分类失败 room_type_std=None）。"""
    rooms, _errors = _parse_room_list(row.get("房间清单") or "")
    return rooms


def group_room_claims(annotations: list[Any]) -> Counter[str]:
    """房间名 claim：按 (ocr_task_id, room_word_id) 去重（CONFLICT 多候选只算一次）。

    ``room_word_id`` 缺失时按 ``annotation_id`` 独立计数。含房间名的全部解析状态均计入。
    """
    claims: Counter[str] = Counter()
    seen: set[tuple[str, str]] = set()
    for ann in annotations:
        std = getattr(ann, "standard_room_type", None)
        if not std:
            continue
        task_id = str(getattr(ann, "ocr_task_id", "") or "")
        room_wid = str(getattr(ann, "room_word_id", None) or getattr(ann, "annotation_id", ""))
        key = (task_id, room_wid)
        if key in seen:
            continue
        seen.add(key)
        claims[std] += 1
    return claims


def group_area_claims(annotations: list[Any]) -> Counter[tuple[str, Decimal]]:
    """面积 claim：仅 ACCEPTED 且有可解析 area_value 的标注（同房型 + 面积值）。"""
    claims: Counter[tuple[str, Decimal]] = Counter()
    for ann in annotations:
        if getattr(ann, "parse_state", None) != AnnotationState.ACCEPTED.value:
            continue
        std = getattr(ann, "standard_room_type", None)
        value = parse_decimal(getattr(ann, "area_value", None))
        if not std or value is None:
            continue
        claims[(std, value)] += 1
    return claims


def group_all_area_claims(annotations: list[Any]) -> Counter[tuple[str, Decimal]]:
    """全状态面积 claim：任意解析状态（ACCEPTED/ROOM_ONLY/CONFLICT/NEEDS_REVIEW）。

    房型感知覆盖度（H6 候选口径，诊断用）的分母侧：只要某面积值在原图中被转录解析出来
    （无论最终判定为何种状态），即算「系统已捕获该面积」。不要求房间+面积唯一关联成功，
    故是比 group_area_claims 更宽的捕获口径；判定状态相关的质量另由面积精确率/已接受
    claim 承担。
    """
    claims: Counter[tuple[str, Decimal]] = Counter()
    for ann in annotations:
        std = getattr(ann, "standard_room_type", None)
        value = parse_decimal(getattr(ann, "area_value", None))
        if not std or value is None:
            continue
        claims[(std, value)] += 1
    return claims


def _match_counter_multi(
    claims: Counter[tuple[str, Decimal]],
    golden: Counter[tuple[str, Decimal]],
    *,
    tolerance: Decimal | None = None,
) -> int:
    """多集匹配：返回 claims 中被 golden 覆盖的最大个数（逐键取 min 交集）。

    ``tolerance`` 非 None 时按同房型面积 ±tolerance 匹配（贪心，诊断用）；
    精确匹配（tolerance=None）用 Counter 交集直接求 min。
    """
    if tolerance is None:
        return sum((claims & golden).values())
    matched = 0
    used: Counter[tuple[str, Decimal]] = Counter()
    for (rtype, value), cnt in claims.items():
        for (gtype, gval), gcnt in golden.items():
            if gtype != rtype or abs(value - gval) > tolerance:
                continue
            avail = gcnt - used[(gtype, gval)]
            take = min(cnt, avail)
            if take <= 0:
                continue
            matched += take
            used[(gtype, gval)] += take
            cnt -= take
            if cnt <= 0:
                break
    return matched


def compare_sample(
    sample_index: int,
    golden_rooms: list[GoldenLabelRoom],
    annotations: list[Any],
    *,
    ocr_task_id: str | None = None,
    image_text_category: str = "",
) -> SampleComparison:
    """比对一个样本：黄金房间/面积 vs 转录房间 claim/面积 claim。"""
    golden_std_rooms = Counter(r.room_type_std for r in golden_rooms if r.room_type_std is not None)

    golden_std_areas = Counter(
        (r.room_type_std, r.area_sqm)
        for r in golden_rooms
        if r.area_present and r.room_type_std is not None and r.area_sqm is not None
    )
    golden_all_areas = Counter(
        (r.room_type_std, r.area_sqm)
        for r in golden_rooms
        if r.area_present and r.area_sqm is not None
    )

    room_claims = group_room_claims(annotations)
    area_claims = group_area_claims(annotations)
    all_area_claims = group_all_area_claims(annotations)

    room_matched = sum((room_claims & golden_std_rooms).values())
    area_matched_exact = _match_counter_multi(area_claims, golden_std_areas)
    area_matched_tol = _match_counter_multi(
        area_claims, golden_std_areas, tolerance=AREA_TOLERANCE_SQM
    )
    # 房型感知覆盖度：黄金标准面积 (房型, 值) 是否被任意状态转录捕获（多集 min 交集）
    golden_std_areas_covered = _match_counter_multi(all_area_claims, golden_std_areas)

    return SampleComparison(
        sample_index=sample_index,
        ocr_task_id=ocr_task_id,
        image_text_category=image_text_category,
        golden_rooms_total=len(golden_rooms),
        golden_std_rooms=sum(golden_std_rooms.values()),
        room_claims=sum(room_claims.values()),
        room_claims_matched=room_matched,
        golden_std_areas=sum(golden_std_areas.values()),
        golden_all_areas=sum(golden_all_areas.values()),
        golden_std_areas_covered=golden_std_areas_covered,
        area_claims=sum(area_claims.values()),
        area_claims_matched_exact=area_matched_exact,
        area_claims_matched_tol=area_matched_tol,
        false_positive_areas=sum(area_claims.values()) - area_matched_exact,
    )


def _ratio(num: int, den: int) -> float | None:
    """除零安全比率。"""
    if den <= 0:
        return None
    return num / den


def aggregate_golden_comparison(
    samples: list[SampleComparison],
) -> GoldenIndicatorAggregate:
    """跨样本微平均聚合五项黄金指标 + §10.3 门槛判定。"""
    agg = GoldenIndicatorAggregate(
        samples_compared=len(samples),
    )
    for s in samples:
        agg.samples_with_golden_rooms += 1 if s.golden_rooms_total > 0 else 0
        agg.room_claims += s.room_claims
        agg.room_claims_matched += s.room_claims_matched
        agg.golden_std_rooms += s.golden_std_rooms
        agg.golden_all_rooms += s.golden_rooms_total
        agg.area_claims += s.area_claims
        agg.area_claims_matched_exact += s.area_claims_matched_exact
        agg.area_claims_matched_tol += s.area_claims_matched_tol
        agg.golden_std_areas += s.golden_std_areas
        agg.golden_all_areas += s.golden_all_areas
        agg.golden_std_areas_covered += s.golden_std_areas_covered
        agg.false_positive_areas += s.false_positive_areas

    agg.room_precision = _ratio(agg.room_claims_matched, agg.room_claims)
    agg.room_recall_standard = _ratio(agg.room_claims_matched, agg.golden_std_rooms)
    agg.room_recall_full = _ratio(agg.room_claims_matched, agg.golden_all_rooms)
    agg.area_precision = _ratio(agg.area_claims_matched_exact, agg.area_claims)
    agg.area_recall_standard = _ratio(agg.area_claims_matched_exact, agg.golden_std_areas)
    agg.area_recall_full = _ratio(agg.area_claims_matched_exact, agg.golden_all_areas)
    agg.area_recall_coverage = _ratio(agg.golden_std_areas_covered, agg.golden_std_areas)

    agg.area_precision_pass = _pass(agg.area_precision, THRESHOLD_AREA_PRECISION)
    agg.room_precision_pass = _pass(agg.room_precision, THRESHOLD_ROOM_PRECISION)
    agg.room_recall_pass = _pass(agg.room_recall_standard, THRESHOLD_ROOM_RECALL)
    agg.area_recall_pass = _pass(agg.area_recall_standard, THRESHOLD_AREA_RECALL)
    agg.area_recall_coverage_pass = _pass(agg.area_recall_coverage, THRESHOLD_AREA_RECALL_COVERAGE)
    if agg.area_claims == 0 and agg.golden_std_areas == 0:
        agg.fp_areas_pass = None
    else:
        agg.fp_areas_pass = agg.false_positive_areas <= THRESHOLD_FP_AREAS
    return agg


def _pass(value: float | None, threshold: float | None) -> bool | None:
    if value is None or threshold is None:
        return None
    return value >= threshold


def run_golden_comparison(
    golden_csv: Path,
    annotation_table: Path,
    ocr_run_dir: Path,
    *,
    out_json: Path | None = None,
) -> GoldenComparisonReport:
    """主入口：读黄金标签 + OCR 运行 + 标注表，生成黄金比对报告。

    - ``golden_csv``：golden_label_final.csv（只取备注含「已确认」且非范围外的行）；
    - ``ocr_run_dir``：OCR 运行目录（asset_id → ocr_task_id 关联）；
    - ``annotation_table``：staged/floorplan_room_annotation.parquet。
    """
    rows = load_golden_label_rows(golden_csv)
    confirmed = [r for r in rows if is_confirmed_golden(r)]

    run = load_ocr_run_record(ocr_run_dir)
    task_by_asset: dict[str, str] = {t.asset_id: t.ocr_task_id for t in run.tasks}

    annotations = read_annotation_table(annotation_table)
    by_task: dict[str, list[Any]] = {}
    for ann in annotations:
        by_task.setdefault(ann.ocr_task_id, []).append(ann)

    samples: list[SampleComparison] = []
    failures: list[str] = []
    for row in confirmed:
        try:
            sample_index = int(row["sample_index"])
        except (TypeError, ValueError):
            continue
        golden_rooms = parse_golden_rooms(row)
        ocr_task_id = task_by_asset.get(row.get("asset_id") or "")
        task_anns = by_task.get(ocr_task_id, []) if ocr_task_id else []
        sc = compare_sample(
            sample_index,
            golden_rooms,
            task_anns,
            ocr_task_id=ocr_task_id,
            image_text_category=row.get("图片文字类别") or "",
        )
        samples.append(sc)
        if ocr_task_id is None:
            failures.append(f"sample {sample_index}: 黄金 asset 无 OCR 任务关联")

    aggregate = aggregate_golden_comparison(samples)
    for flag_name, passed in (
        ("面积数值精确率", aggregate.area_precision_pass),
        ("房间名称精确率", aggregate.room_precision_pass),
        ("房间名称召回率(标准)", aggregate.room_recall_pass),
        ("面积字段召回率(覆盖度)", aggregate.area_recall_coverage_pass),
        ("无原图外生成面积", aggregate.fp_areas_pass),
    ):
        if passed is False:
            failures.append(f"{flag_name} 未达 §10.3 门槛")

    report = GoldenComparisonReport(
        generated_at=datetime.now(UTC).isoformat(),
        golden_csv=golden_csv.as_posix(),
        annotation_table=annotation_table.as_posix(),
        ocr_run=ocr_run_dir.as_posix(),
        confirmed_count=len(confirmed),
        aggregate=aggregate,
        per_sample=samples,
        failures=failures,
    )
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(report.model_dump_json(indent=2, ensure_ascii=False), encoding="utf-8")
        work.replace(out_json)
    return report


__all__ = [
    "GOLDEN_COMPARE_VERSION",
    "AREA_TOLERANCE_SQM",
    "THRESHOLD_AREA_PRECISION",
    "THRESHOLD_ROOM_PRECISION",
    "THRESHOLD_ROOM_RECALL",
    "THRESHOLD_AREA_RECALL",
    "THRESHOLD_AREA_RECALL_COVERAGE",
    "THRESHOLD_FP_AREAS",
    "SampleComparison",
    "GoldenIndicatorAggregate",
    "GoldenComparisonReport",
    "load_golden_label_rows",
    "is_confirmed_golden",
    "parse_golden_rooms",
    "group_room_claims",
    "group_area_claims",
    "group_all_area_claims",
    "compare_sample",
    "aggregate_golden_comparison",
    "run_golden_comparison",
]
