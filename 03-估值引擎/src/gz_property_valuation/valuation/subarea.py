"""WP6-S 家族层子区修正（OpenSpec change comparable-family-subarea-adjustment）。

家族归并后同家族内部存在统计上可分离的子区价差（冻结证据见 change proposal）。
本模块在 rule 1.2 下为同家族跨子区（tier 4）案例估计并应用子区位势修正：

- **基准选择（spec：目标子区自身样本优先，回退家族合并基准）**：
  - Mode A：目标自身池（tier 1–3 入选数 ≥ 现行最低有效案例数）→ 基准 =
    目标小区（``subject.community_id``）格内样本；
  - Mode B：自身池不足 → 基准 = 家族合并池格内样本（跨子区案例按成对判定
    应用数值修正或方向性因素；目标自身子区位差仅作方向性处理，不虚构数值）；
- **控制格子**：面积段（配置边界，初值 50/70/90/120）× 户型室数；修正仅在
  目标格内估计，构成不同的格子之间不外推（spec：构成效应受控）；
- **成对判定（每次只判「子区 S ↔ 基准」）**：
  - ``数值``：S 的 12 个月有效案例 ≥ 门槛 且 格内每侧样本 ≥ 下限 且 bootstrap
    置信区间分离（不含 1）且 |1/r−1| ≤ 上限（修正后幅度）→ 对 S 的同格
    tier 4 案例应用 1/r（折算因子=1/r，将源侧案例折算至基准水平）；
  - ``同质合并``：检验功效充足（格内样本达标）但 CI 含 1 → 合并计价，不修正、
    不扩张、不扣减（spec：同质家族合并计价）；
  - ``方向性``：案例数不足 / 格内样本不足 / 分离但超上限 → 仅记方向性因素 +
    区间扩张预算 + 可信度封顶（Q6 初值「中」），不虚构数值修正；
- **估计样本**：valid_sale 中 ``[估值时点-365, 估值时点]`` 内、目标格内、
  单价有效的成交行（与案例数门槛同窗口，无未来泄漏）；不回读任何时点后数据；
- **确定性**：bootstrap 固定种子（配置），同输入重跑逐位一致；
- **产物**：comp_adjustment 追加 ``adjustment_type="子区"`` 行（复用既有模式，
  数值行 factor=1/r（r=源/基准，折算方向修复见 change fix-subarea-ratio-direction），
  方向性行 factor=None、basis 记判定依据 JSON）；
  comp_candidate 回填 ``has_subarea_adjustment``；``subarea_judgment.parquet``
  与 ``subarea_stage_meta.json`` 留全部成对判定与同质判定供复核（README §6.9）。

配置缺失/损坏 → 整体不启用（回退 1.1 行为，见 subarea_config）。全程只读
raw/staged/marts/entities，不改写冻结估值。
"""

from __future__ import annotations

import json
import random
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from gz_property_valuation import __version__
from gz_property_valuation.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from gz_property_valuation.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
)
from gz_property_valuation.valuation.comparable import _room_count
from gz_property_valuation.valuation.subarea_config import (
    FAMILY_RULE_VERSION,
    SubareaConfig,
    load_subarea_config,
)
from gz_property_valuation.valuation.time_adjustment import (
    COMP_ADJUSTMENT_FILENAME,
    comp_adjustment_schema,
)

#: 子区判定清单表名/文件名（估值层新中间表，catalog 注册 ``sa_`` 前缀）。
SUBAREA_JUDGMENT_TABLE = "subarea_judgment"
SUBAREA_JUDGMENT_FILENAME = f"{SUBAREA_JUDGMENT_TABLE}.parquet"
SUBAREA_STAGE_META_FILENAME = "subarea_stage_meta.json"

#: 跨子区案例层级（ComparableTierPolicy 家族层）。
FAMILY_TIER = 4

#: Mode A 门槛 = 现行最低有效案例数（与 aggregation 同口径；Q5 冻结语义）。
MIN_EFFECTIVE_SAMPLES = 3

#: 估计窗口（天）：与案例数门槛一致的 12 个月。
_ESTIMATION_WINDOW_DAYS = 365

#: 判定结论（留痕与测试断言用）。
DECISION_NUMERIC = "数值"
DECISION_HOMOGENEOUS = "同质合并"
DECISION_DIRECTIONAL = "方向性"

_DIRECTIONAL_REASONS: Final = {
    "case_gate": "子区12个月案例数低于门槛",
    "cell_samples": "格内样本不足",
    "over_cap": "修正幅度超上限",
}


# ---------------------------------------------------------------------------
# 家族/子区索引与目标子区解析（估值链只读消费实体表，不重解析源名）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SubjectSubarea:
    """目标房源子区解析结果（歧义置空并留警告，不猜测）。"""

    sub_area: str | None
    family_id: str | None
    warning: str | None


def build_family_index(
    communities: pa.Table,
    community_subareas: pa.Table | None,
) -> tuple[dict[str, str | None], dict[str, str | None]]:
    """community 表 + 子区表 → (community_id → family_id, community_id → 子区名)。

    子区归属：注册子区表 ``community_id → sub_area_name`` 优先；merged 实体经
    ``redirect_subarea_name`` 归入承接子区（不覆盖已注册行）。UNKNOWN 不补造。
    """
    family_of: dict[str, str | None] = {}
    subarea_of: dict[str, str | None] = {}
    if "family_id" in communities.column_names:
        for cid, fid in zip(
            communities.column("community_id").to_pylist(),
            communities.column("family_id").to_pylist(),
            strict=True,
        ):
            if cid is not None:
                family_of[str(cid)] = str(fid) if fid is not None else None
    if community_subareas is not None:
        for cid, name in zip(
            community_subareas.column("community_id").to_pylist(),
            community_subareas.column("sub_area_name").to_pylist(),
            strict=True,
        ):
            if cid is not None and name is not None:
                subarea_of[str(cid)] = str(name)
    if "redirect_subarea_name" in communities.column_names:
        for cid, name in zip(
            communities.column("community_id").to_pylist(),
            communities.column("redirect_subarea_name").to_pylist(),
            strict=True,
        ):
            if cid is not None and name is not None and str(cid) not in subarea_of:
                subarea_of[str(cid)] = str(name)
    return family_of, subarea_of


def resolve_subject_subarea(
    subject: Any,
    communities: pa.Table,
    community_subareas: pa.Table | None,
) -> SubjectSubarea:
    """目标子区解析：显式 ``sub_area`` 字段优先，缺省按 community_id 唯一查表。

    - 显式值经子区表（``sub_area_name``/``match_names``）唯一解析；多义或无
      命中 → 置空 + 警告（不猜测）；解析命中但小区与 ``community_id`` 不一致
      → 置空 + 警告（矛盾输入不猜）；
    - 缺省：``community_id`` 在子区表（含 merged 重定向）中唯一命中 → 子区名；
      无命中 → None（无家族/裸名属正常，不告警）。
    """
    family_of, subarea_of = build_family_index(communities, community_subareas)
    family_id = family_of.get(subject.community_id)
    family_id = family_id if family_id not in (None, "", "UNKNOWN") else None

    explicit = getattr(subject, "sub_area", None)
    explicit = str(explicit).strip() if explicit else ""
    if explicit:
        hits: set[tuple[str, str]] = set()
        if community_subareas is not None:
            names_col = (
                community_subareas.column("sub_area_name").to_pylist()
                if "sub_area_name" in community_subareas.column_names
                else [None] * community_subareas.num_rows
            )
            match_col = (
                community_subareas.column("match_names").to_pylist()
                if "match_names" in community_subareas.column_names
                else [None] * community_subareas.num_rows
            )
            cids = community_subareas.column("community_id").to_pylist()
            for cid, name, matches in zip(cids, names_col, match_col, strict=True):
                if cid is None:
                    continue
                name_hit = name is not None and str(name) == explicit
                match_hit = any(
                    str(item) == explicit
                    for item in (matches if isinstance(matches, list) else [])
                )
                if (name_hit or match_hit) and name is not None:
                    hits.add((str(cid), str(name)))
        if len(hits) == 0:
            return SubjectSubarea(None, family_id, f"显式子区「{explicit}」无命中，置空")
        if len(hits) > 1:
            return SubjectSubarea(None, family_id, f"显式子区「{explicit}」多义命中，置空")
        resolved_cid, resolved_name = next(iter(hits))
        if resolved_cid != subject.community_id:
            return SubjectSubarea(
                None,
                family_id,
                f"显式子区「{explicit}」解析小区 {resolved_cid} 与目标小区不一致，置空",
            )
        return SubjectSubarea(resolved_name, family_id, None)

    default = subarea_of.get(subject.community_id)
    return SubjectSubarea(default, family_id, None)


# ---------------------------------------------------------------------------
# 格子与估计样本（valid_sale 只读；窗口 = 估值时点前 12 个月）
# ---------------------------------------------------------------------------


def area_segment_label(area: Decimal, segments: Sequence[float]) -> str:
    """面积段标签（配置边界升序；如 [50,70,90,120] → ≤50/50-70/70-90/90-120/>120）。"""
    value = float(area)
    labels = [f"≤{segments[0]:g}"]
    for i in range(len(segments) - 1):
        labels.append(f"{segments[i]:g}-{segments[i + 1]:g}")
    labels.append(f">{segments[-1]:g}")
    for i, bound in enumerate(segments):
        if value < bound:
            return labels[i]
    return labels[-1]


def cell_of(area: Decimal, layout: str, segments: Sequence[float]) -> tuple[str, str]:
    """控制格子 =（面积段，户型室数）；室数未知 → "未知"（不猜测）。"""
    room = _room_count(layout)
    return area_segment_label(area, segments), str(room) if room is not None else "未知"


def _cell_matches(
    area: Decimal, layout: str, cell: tuple[str, str], segments: Sequence[float]
) -> bool:
    return cell_of(area, layout, segments) == cell


def cases_in_window(
    valid_sale: pa.Table, community_id: str, valuation_date: date
) -> int:
    """小区 12 个月有效案例数（门槛口径：截点前一年、单价有效、非车位）。"""
    count = 0
    lower = valuation_date.toordinal() - _ESTIMATION_WINDOW_DAYS
    for cid, sale_date, price, layout in zip(
        valid_sale.column("community_id").to_pylist(),
        valid_sale.column("sale_date").to_pylist(),
        valid_sale.column("unit_price").to_pylist(),
        valid_sale.column("layout").to_pylist(),
        strict=True,
    ):
        if cid is None or str(cid) != community_id:
            continue
        if not isinstance(sale_date, date) or sale_date.toordinal() < lower:
            continue
        if sale_date > valuation_date:
            continue
        if price is None or price <= 0:
            continue
        if str(layout) == "车位":
            continue
        count += 1
    return count


def cell_samples(
    valid_sale: pa.Table,
    community_ids: set[str],
    cell: tuple[str, str],
    segments: Sequence[float],
    valuation_date: date,
) -> list[Decimal]:
    """估计样本：目标格内、窗口内、单价有效的成交单价（无未来泄漏）。"""
    lower = valuation_date.toordinal() - _ESTIMATION_WINDOW_DAYS
    values: list[Decimal] = []
    columns = (
        valid_sale.column("community_id").to_pylist(),
        valid_sale.column("sale_date").to_pylist(),
        valid_sale.column("unit_price").to_pylist(),
        valid_sale.column("area_sqm").to_pylist(),
        valid_sale.column("layout").to_pylist(),
    )
    for cid, sale_date, price, area, layout in zip(*columns, strict=True):
        if cid is None or str(cid) not in community_ids:
            continue
        if not isinstance(sale_date, date):
            continue
        if sale_date > valuation_date or sale_date.toordinal() < lower:
            continue
        if price is None or price <= 0 or area is None:
            continue
        if str(layout) == "车位":
            continue
        try:
            area_decimal = Decimal(str(area))
            price_decimal = Decimal(str(price))
        except (TypeError, ValueError, InvalidOperation):
            continue
        if area_decimal <= 0 or price_decimal <= 0:
            continue
        if not _cell_matches(area_decimal, str(layout), cell, segments):
            continue
        values.append(price_decimal)
    return values


# ---------------------------------------------------------------------------
# bootstrap 中位比率与成对判定
# ---------------------------------------------------------------------------


def bootstrap_median_ratio(
    samples_sub: Sequence[Decimal],
    samples_base: Sequence[Decimal],
    *,
    iterations: int,
    confidence_level: float,
    seed: int,
) -> tuple[Decimal, Decimal, Decimal]:
    """格内中位比率 r 与 bootstrap 百分位置信区间（固定种子，确定性）。

    重采样两侧独立、各回到自身样本量；r = median(子区)/median(基准)。
    返回 (r, ci_low, ci_high)。
    """
    sub = [float(v) for v in samples_sub]
    base = [float(v) for v in samples_base]
    rng = random.Random(seed)
    ratios: list[float] = []
    for _ in range(iterations):
        rs = statistics.median(rng.choices(sub, k=len(sub)))
        rb = statistics.median(rng.choices(base, k=len(base)))
        if rb > 0:
            ratios.append(rs / rb)
    ratios.sort()
    point = statistics.median(sub) / statistics.median(base)
    if not ratios:
        return Decimal(str(point)), Decimal(str(point)), Decimal(str(point))
    alpha = (1.0 - confidence_level) / 2.0
    low_idx = min(len(ratios) - 1, max(0, int(round((len(ratios) - 1) * alpha))))
    high_idx = min(len(ratios) - 1, max(0, int(round((len(ratios) - 1) * (1 - alpha)))))
    return (
        Decimal(str(point)),
        Decimal(str(ratios[low_idx])),
        Decimal(str(ratios[high_idx])),
    )


@dataclass(frozen=True)
class PairJudgment:
    """一次「子区 S ↔ 基准」在目标格内的成对判定（全量留痕）。"""

    family_id: str
    sub_area: str
    baseline_label: str
    area_segment: str
    room_count: str
    cases_12m_sub: int
    n_cell_sub: int
    n_cell_base: int
    ratio: Decimal | None
    ci_low: Decimal | None
    ci_high: Decimal | None
    separated: bool | None
    within_cap: bool | None
    decision: str
    reason: str


def judge_pair(
    *,
    family_id: str,
    sub_area: str,
    baseline_label: str,
    cell: tuple[str, str],
    cases_12m_sub: int,
    samples_sub: Sequence[Decimal],
    samples_base: Sequence[Decimal],
    config: SubareaConfig,
) -> PairJudgment:
    """单对单格判定：数值 / 同质合并 / 方向性（决策树见模块 docstring）。"""
    segment, room = cell
    n_sub = len(samples_sub)
    n_base = len(samples_base)

    def _directional(reason: str) -> PairJudgment:
        return PairJudgment(
            family_id=family_id,
            sub_area=sub_area,
            baseline_label=baseline_label,
            area_segment=segment,
            room_count=room,
            cases_12m_sub=cases_12m_sub,
            n_cell_sub=n_sub,
            n_cell_base=n_base,
            ratio=None,
            ci_low=None,
            ci_high=None,
            separated=None,
            within_cap=None,
            decision=DECISION_DIRECTIONAL,
            reason=_DIRECTIONAL_REASONS[reason],
        )

    if cases_12m_sub < config.min_subarea_cases_12m:
        return _directional("case_gate")
    if n_sub < config.min_cell_samples_per_side or n_base < config.min_cell_samples_per_side:
        return _directional("cell_samples")

    ratio, ci_low, ci_high = bootstrap_median_ratio(
        samples_sub,
        samples_base,
        iterations=config.bootstrap_iterations,
        confidence_level=config.confidence_level,
        seed=config.random_seed,
    )
    separated = not (ci_low <= Decimal("1") <= ci_high)
    one = Decimal("1")
    within_cap = abs(one / ratio - one) <= Decimal(str(config.cap_ratio))
    if separated and within_cap:
        return PairJudgment(
            family_id=family_id,
            sub_area=sub_area,
            baseline_label=baseline_label,
            area_segment=segment,
            room_count=room,
            cases_12m_sub=cases_12m_sub,
            n_cell_sub=n_sub,
            n_cell_base=n_base,
            ratio=ratio,
            ci_low=ci_low,
            ci_high=ci_high,
            separated=True,
            within_cap=True,
            decision=DECISION_NUMERIC,
            reason="门槛满足且 CI 分离、幅度在上限内",
        )
    if not separated:
        return PairJudgment(
            family_id=family_id,
            sub_area=sub_area,
            baseline_label=baseline_label,
            area_segment=segment,
            room_count=room,
            cases_12m_sub=cases_12m_sub,
            n_cell_sub=n_sub,
            n_cell_base=n_base,
            ratio=ratio,
            ci_low=ci_low,
            ci_high=ci_high,
            separated=False,
            within_cap=within_cap,
            decision=DECISION_HOMOGENEOUS,
            reason="格内样本充足但 CI 含 1（无可分离价差），合并计价",
        )
    return _directional("over_cap")


# ---------------------------------------------------------------------------
# 主阶段入口
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SubareaResult:
    """一次子区修正阶段的结果（供 CLI/回放诊断与测试断言）。"""

    applied: bool
    note: str
    mode: str | None = None
    baseline_label: str | None = None
    n_numeric: int = 0
    n_directional: int = 0
    n_homogeneous: int = 0
    judgment_path: Path | None = None
    meta_path: Path | None = None
    warnings: tuple[str, ...] = ()


def _align_to_schema(table: pa.Table) -> pa.Table:
    """把既有 comp_adjustment 对齐到当前模式（缺列补空，向后兼容）。"""
    schema = comp_adjustment_schema()
    for name in schema.names:
        if name not in table.column_names:
            field = schema.field(name)
            table = table.append_column(field, pa.nulls(table.num_rows, type=field.type))
    return table.cast(schema)


def _basis_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def apply_subarea_adjustments(
    *,
    data_dir: Path,
    subject: Any,
    valid_sale: pa.Table,
    communities: pa.Table,
    community_subareas: pa.Table | None,
    input_refs: Sequence[InputRef],
    rule_version: str,
) -> SubareaResult:
    """WP6-S 主入口：tier 4 跨子区案例的子区修正判定与应用。

    - 配置缺失或 ``rule_version != 1.2`` → 不启用（返回 applied=False，零改写）；
    - 无 tier 4 入选案例 → 不启用（零改写）；
    - 成对判定 → 数值行写入 comp_adjustment（factor=r）、方向性行（factor=None）
      一并留痕；comp_candidate 回填 has_subarea_adjustment；
    - 写 subarea_judgment.parquet + subarea_stage_meta.json（复核可见）。
    """
    if rule_version != FAMILY_RULE_VERSION:
        return SubareaResult(False, f"规则版本 {rule_version} 非家族层版本，不启用")
    config = load_subarea_config(data_dir)
    if config is None:
        return SubareaResult(False, "子区修正配置缺失或非法，整体回退（家族层不启用）")

    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    if not candidate_path.is_file():
        return SubareaResult(False, "comp_candidate 缺失（未运行候选/层级阶段），不启用")
    source = pq.read_table(candidate_path)
    rows = source.to_pylist()
    family_of, subarea_of = build_family_index(communities, community_subareas)
    subject_sub = resolve_subject_subarea(subject, communities, community_subareas)

    def _row_family(row: dict[str, Any]) -> str | None:
        value = row.get("family_id")
        if value is None:
            value = family_of.get(str(row.get("community_id")))
        return str(value) if value not in (None, "", "UNKNOWN") else None

    def _row_subarea(row: dict[str, Any]) -> str | None:
        value = row.get("sub_area")
        if value is None:
            value = subarea_of.get(str(row.get("community_id")))
        return str(value) if value else None

    selected = [row for row in rows if row.get("selected")]
    own_pool = [
        row for row in selected if str(row.get("community_id")) == subject.community_id
    ]
    family_rows = [
        row
        for row in selected
        if row.get("tier") is not None and int(row["tier"]) == FAMILY_TIER
    ]
    if not family_rows:
        return SubareaResult(False, "无 tier 4 跨子区入选案例，不启用")

    mode = "A" if len(own_pool) >= MIN_EFFECTIVE_SAMPLES else "B"
    baseline_label = (
        f"自身池({subject.community_id})" if mode == "A" else "家族合并池"
    )
    subject_cell = cell_of(
        Decimal(str(subject.area_sqm)), subject.layout, config.area_segments
    )

    # 基准侧样本：Mode A = 目标小区；Mode B = 家族合并池（全部成员小区）
    if mode == "A":
        baseline_ids = {subject.community_id}
    else:
        baseline_ids = {
            str(row.get("community_id"))
            for row in selected
            if _row_family(row) is not None
        }
        baseline_ids.add(subject.community_id)

    # 子区分组（按家族；跨家族的 tier4 理论不存在，防御性归其家族）
    warnings: list[str] = []
    judgments: list[PairJudgment] = []
    decisions_by_candidate: dict[str, tuple[str, PairJudgment | None]] = {}
    # 案例属性溯源：comp_candidate 不携带面积/户型，从 valid_sale 取（格内判定用）
    attrs_by_event: dict[str, tuple[object, object]] = {}
    for eid, area_raw, layout_raw in zip(
        valid_sale.column("sale_event_id").to_pylist(),
        valid_sale.column("area_sqm").to_pylist(),
        valid_sale.column("layout").to_pylist(),
        strict=True,
    ):
        attrs_by_event[str(eid)] = (area_raw, layout_raw)
    if subject_sub.family_id is None:
        warnings.append(
            "目标小区 family_id 为 UNKNOWN/缺失，tier 4 案例按方向性因素处理（不入数值修正）"
        )

    subareas_present = sorted({_row_subarea(row) or "未知子区" for row in family_rows})
    for sub_name in subareas_present:
        comp_rows = [row for row in family_rows if (_row_subarea(row) or "未知子区") == sub_name]
        family_id = _row_family(comp_rows[0]) or "UNKNOWN"
        sub_community_ids = {str(row.get("community_id")) for row in comp_rows}
        cases = max(
            cases_in_window(valid_sale, cid, subject.valuation_date)
            for cid in sub_community_ids
        )
        samples_sub = cell_samples(
            valid_sale,
            sub_community_ids,
            subject_cell,
            config.area_segments,
            subject.valuation_date,
        )
        samples_base = cell_samples(
            valid_sale, baseline_ids, subject_cell, config.area_segments, subject.valuation_date
        )
        judgment = judge_pair(
            family_id=family_id,
            sub_area=sub_name,
            baseline_label=baseline_label,
            cell=subject_cell,
            cases_12m_sub=cases,
            samples_sub=samples_sub,
            samples_base=samples_base,
            config=config,
        )
        judgments.append(judgment)
        for row in comp_rows:
            area_raw, layout_raw = attrs_by_event.get(
                str(row.get("sale_event_id")), (None, None)
            )
            in_cell = False
            if area_raw is not None and layout_raw is not None:
                try:
                    comp_area = Decimal(str(area_raw))
                    comp_layout = str(layout_raw)
                except (TypeError, ValueError, InvalidOperation):
                    comp_area = Decimal("0")
                    comp_layout = ""
                if comp_area > 0:
                    in_cell = _cell_matches(
                        comp_area,
                        comp_layout,
                        subject_cell,
                        config.area_segments,
                    )
            if judgment.decision == DECISION_NUMERIC and in_cell:
                decisions_by_candidate[str(row["candidate_id"])] = (DECISION_NUMERIC, judgment)
            elif judgment.decision == DECISION_NUMERIC and not in_cell:
                decisions_by_candidate[str(row["candidate_id"])] = (
                    DECISION_DIRECTIONAL,
                    judgment,
                )
            elif judgment.decision == DECISION_HOMOGENEOUS:
                decisions_by_candidate[str(row["candidate_id"])] = (DECISION_HOMOGENEOUS, judgment)
            else:
                decisions_by_candidate[str(row["candidate_id"])] = (DECISION_DIRECTIONAL, judgment)

    n_numeric = sum(1 for d, _ in decisions_by_candidate.values() if d == DECISION_NUMERIC)
    n_directional = sum(1 for d, _ in decisions_by_candidate.values() if d == DECISION_DIRECTIONAL)
    n_homogeneous = sum(1 for d, _ in decisions_by_candidate.values() if d == DECISION_HOMOGENEOUS)

    # comp_adjustment 追加子区行（保留时间/差异行，幂等重建子区部分）
    adjustment_path = data_dir / VALUATION_LAYER / COMP_ADJUSTMENT_FILENAME
    if adjustment_path.is_file():
        merged = _align_to_schema(pq.read_table(adjustment_path))
        merged = merged.filter(
            pa.array(
                [str(t) != "子区" for t in merged.column("adjustment_type").to_pylist()]
            )
        )
    else:
        merged = pa.table(
            {name: [] for name in comp_adjustment_schema().names},
            schema=comp_adjustment_schema(),
        )
    extra_rows: list[dict[str, Any]] = []
    for row in family_rows:
        cid = str(row["candidate_id"])
        got = decisions_by_candidate.get(cid)
        decision, pair = got if got is not None else (DECISION_DIRECTIONAL, None)
        factor: Decimal | None = None
        basis: dict[str, Any]
        if decision == DECISION_NUMERIC and pair is not None and pair.ratio is not None:
            factor = (Decimal("1") / pair.ratio).quantize(Decimal("0.0001"))
            ci_low = pair.ci_low if pair.ci_low is not None else pair.ratio
            ci_high = pair.ci_high if pair.ci_high is not None else pair.ratio
            factor_ci_low = (Decimal("1") / ci_high).quantize(Decimal("0.0001"))
            factor_ci_high = (Decimal("1") / ci_low).quantize(Decimal("0.0001"))
            basis = {
                "判定": DECISION_NUMERIC,
                "基准": pair.baseline_label,
                "格子": f"{pair.area_segment}㎡×{pair.room_count}室",
                "样本量": {"子区": pair.n_cell_sub, "基准": pair.n_cell_base},
                "cases_12m": pair.cases_12m_sub,
                "ratio": float(pair.ratio),
                "ci": [float(ci_low), float(ci_high)],
                "字段口径": {
                    "ratio/ci": "源/基准（r=median(源池)/median(基准池)），留痕口径",
                    "折算因子/折算因子_ci": "实际应用乘数（=1/r，源侧案例折算至基准水平）",
                },
                "折算因子": float(factor),
                "折算因子_ci": [float(factor_ci_low), float(factor_ci_high)],
                "上限": config.cap_ratio,
                "bootstrap": {
                    "次数": config.bootstrap_iterations,
                    "水平": config.confidence_level,
                    "种子": config.random_seed,
                },
            }
        elif decision == DECISION_HOMOGENEOUS and pair is not None:
            basis = {
                "判定": DECISION_HOMOGENEOUS,
                "基准": pair.baseline_label,
                "格子": f"{pair.area_segment}㎡×{pair.room_count}室",
                "样本量": {"子区": pair.n_cell_sub, "基准": pair.n_cell_base},
                "ci": [
                    float(pair.ci_low) if pair.ci_low is not None else None,
                    float(pair.ci_high) if pair.ci_high is not None else None,
                ],
                "说明": "同质合并计价，不修正、不扩张、不扣减",
            }
        else:
            basis = {
                "判定": DECISION_DIRECTIONAL,
                "基准": baseline_label,
                "格子": f"{subject_cell[0]}㎡×{subject_cell[1]}室",
                "原因": pair.reason if pair is not None else "无判定",
                "区间扩张预算": config.directional_widening_budget,
                "可信度封顶": config.confidence_cap_directional,
            }
        extra_rows.append(
            {
                "adjustment_id": f"{cid}-SUB",
                "candidate_id": cid,
                "adjustment_type": "子区",
                "amount": None,
                "sale_date": None,
                "valuation_date": subject.valuation_date,
                "basis": _basis_json(basis),
                "evidence_strength": "强" if decision == DECISION_NUMERIC else "弱",
                "source_series": "无（家族/子区位势修正，格内市场样本）",
                "warning": (
                    None
                    if decision != DECISION_DIRECTIONAL
                    else pair.reason if pair is not None else None
                ),
                "direction": (
                    "上"
                    if factor is not None and factor > 1
                    else "下" if factor is not None and factor < 1 else None
                ),
                "factor": factor,
                "feature": "子区位势",
                "formula": (
                    "调整后单价=单价×时间系数×(1/r)"
                    if factor is not None
                    else "方向性因素（无数值修正）"
                ),
                "subject_side": subject_sub.sub_area or subject.community_id,
                "comparable_side": _row_subarea(row) or "未知子区",
                "rule_version": rule_version,
            }
        )
    if extra_rows:
        schema = comp_adjustment_schema()
        extra_cols: dict[str, list[Any]] = {name: [] for name in schema.names}
        for payload in extra_rows:
            for name in schema.names:
                extra_cols[name].append(payload[name])
        merged = pa.concat_tables([merged, pa.table(extra_cols, schema=schema)])

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    work = valuation_dir / (COMP_ADJUSTMENT_FILENAME + ".incomplete")
    pq.write_table(merged, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table="comp_adjustment",
            built_at=datetime.now(UTC),
            row_count=merged.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-S: 追加子区修正行(adjustment_type=子区)",
        ),
        adjustment_path,
    )
    work.replace(adjustment_path)

    # comp_candidate 回填 has_subarea_adjustment（additive，不改既有列）
    marker_by_candidate = {
        str(row["candidate_id"]): (
            decisions_by_candidate.get(str(row["candidate_id"]), (None, None))[0]
            == DECISION_NUMERIC
        )
        for row in family_rows
    }
    marker_values = pa.array(
        [marker_by_candidate.get(str(row.get("candidate_id"))) for row in rows],
        type=pa.bool_(),
    )
    if "has_subarea_adjustment" in source.column_names:
        updated = source.set_column(
            source.column_names.index("has_subarea_adjustment"),
            pa.field("has_subarea_adjustment", pa.bool_()),
            marker_values,
        )
    else:
        updated = source.append_column(
            pa.field("has_subarea_adjustment", pa.bool_()), marker_values
        )
    candidate_work = valuation_dir / (COMP_CANDIDATE_FILENAME + ".incomplete")
    pq.write_table(updated, candidate_work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table="comp_candidate",
            built_at=datetime.now(UTC),
            row_count=updated.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-S: 回填 has_subarea_adjustment",
        ),
        candidate_path,
    )
    candidate_work.replace(candidate_path)

    # 判定清单 + 元数据（复核可见，README §6.9）
    judgment_table = _judgment_table(judgments, rule_version=rule_version)
    judgment_path = valuation_dir / SUBAREA_JUDGMENT_FILENAME
    jwork = valuation_dir / (SUBAREA_JUDGMENT_FILENAME + ".incomplete")
    pq.write_table(judgment_table, jwork, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=SUBAREA_JUDGMENT_TABLE,
            built_at=datetime.now(UTC),
            row_count=judgment_table.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-S: 子区修正成对判定清单（含同质判定）",
        ),
        judgment_path,
    )
    jwork.replace(judgment_path)

    homogeneous = all(j.decision == DECISION_HOMOGENEOUS for j in judgments) if judgments else False
    meta = {
        "subject_id": subject.subject_id,
        "community_id": subject.community_id,
        "sub_area": subject_sub.sub_area,
        "family_id": subject_sub.family_id,
        "subject_resolution_warning": subject_sub.warning,
        "mode": mode,
        "baseline_label": baseline_label,
        "cell": {"area_segment": subject_cell[0], "room_count": subject_cell[1]},
        "family_homogeneous": homogeneous,
        "n_numeric": n_numeric,
        "n_directional": n_directional,
        "n_homogeneous": n_homogeneous,
        "warnings": warnings,
        "rule_version": rule_version,
    }
    meta_path = valuation_dir / SUBAREA_STAGE_META_FILENAME
    mwork = meta_path.with_name(meta_path.name + ".incomplete")
    mwork.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    mwork.replace(meta_path)

    return SubareaResult(
        applied=True,
        note="子区修正判定完成",
        mode=mode,
        baseline_label=baseline_label,
        n_numeric=n_numeric,
        n_directional=n_directional,
        n_homogeneous=n_homogeneous,
        judgment_path=judgment_path,
        meta_path=meta_path,
        warnings=tuple(warnings),
    )


def _judgment_table(judgments: Sequence[PairJudgment], *, rule_version: str) -> pa.Table:
    """成对判定 → subarea_judgment 表（全量留痕，含同质判定）。"""
    schema = pa.schema(
        [
            pa.field("family_id", pa.string(), nullable=False),
            pa.field("sub_area", pa.string(), nullable=False),
            pa.field("baseline_label", pa.string(), nullable=False),
            pa.field("area_segment", pa.string(), nullable=False),
            pa.field("room_count", pa.string(), nullable=False),
            pa.field("cases_12m_sub", pa.int32(), nullable=False),
            pa.field("n_cell_sub", pa.int32(), nullable=False),
            pa.field("n_cell_base", pa.int32(), nullable=False),
            pa.field("ratio", pa.float64(), nullable=True),
            pa.field("ci_low", pa.float64(), nullable=True),
            pa.field("ci_high", pa.float64(), nullable=True),
            pa.field("separated", pa.bool_(), nullable=True),
            pa.field("within_cap", pa.bool_(), nullable=True),
            pa.field("decision", pa.string(), nullable=False),
            pa.field("reason", pa.string(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
        ]
    )
    cols: dict[str, list[Any]] = {name: [] for name in schema.names}
    for j in judgments:
        cols["family_id"].append(j.family_id)
        cols["sub_area"].append(j.sub_area)
        cols["baseline_label"].append(j.baseline_label)
        cols["area_segment"].append(j.area_segment)
        cols["room_count"].append(j.room_count)
        cols["cases_12m_sub"].append(j.cases_12m_sub)
        cols["n_cell_sub"].append(j.n_cell_sub)
        cols["n_cell_base"].append(j.n_cell_base)
        cols["ratio"].append(float(j.ratio) if j.ratio is not None else None)
        cols["ci_low"].append(float(j.ci_low) if j.ci_low is not None else None)
        cols["ci_high"].append(float(j.ci_high) if j.ci_high is not None else None)
        cols["separated"].append(j.separated)
        cols["within_cap"].append(j.within_cap)
        cols["decision"].append(j.decision)
        cols["reason"].append(j.reason)
        cols["rule_version"].append(rule_version)
    return pa.table(cols, schema=schema)


__all__ = [
    "DECISION_DIRECTIONAL",
    "DECISION_HOMOGENEOUS",
    "DECISION_NUMERIC",
    "FAMILY_TIER",
    "PairJudgment",
    "SubjectSubarea",
    "SubareaResult",
    "apply_subarea_adjustments",
    "bootstrap_median_ratio",
    "build_family_index",
    "cell_of",
    "cases_in_window",
    "cell_samples",
    "judge_pair",
    "resolve_subject_subarea",
]
