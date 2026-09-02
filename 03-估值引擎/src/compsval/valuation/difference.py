"""WP6-D 房源差异处理（VAL1-005）：DifferenceAdjustmentPolicy + comp_adjustment(差异)。

技术方案 §9.5 房源差异处理：面积先经单价口径处理（单价=总价/面积已归一，无需
书面面积调整）；楼层/电梯/朝向/年代**仅在本地成交证据支持时**才做数值调整
（验收①：每个数值修正必有市场证据 basis）；无市场证据时不得虚构比例，保持
``direction_only``/``unknown``（验收②），进区间与复核说明（下游 WP6-E）；现场
观察只属目标房源输入，不得反向修改历史案例。

每种差异记录含：方向（direction）、数值（factor/amount）、公式（formula）、
依据（basis）、证据强度、警告、规则版本（验收③）。

本模块只产出 ``comp_adjustment`` 的 ``adjustment_type=差异`` 行，保留既有时间行
（adjustment_type=时间）；不改写 raw/staged/marts/entities/comp_candidate；不做
回放校准（归 WP8）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import TypedDict

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import CompCandidate, SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    DEFAULT_RULE_VERSION,
    VALUATION_LAYER,
)
from compsval.valuation.time_adjustment import (
    COMP_ADJUSTMENT_FILENAME,
    COMP_ADJUSTMENT_TABLE,
    EvidenceStrength,
    comp_adjustment_schema,
)

ADJUSTMENT_TYPE_DIFFERENCE = "差异"
"""comp_adjustment 差异行类型（§3.12 enum '时间'/'差异'）。"""


# ---------------------------------------------------------------------------
# 枚举
# ---------------------------------------------------------------------------


class DifferenceDimension(StrEnum):
    """差异维度（§9.5 第一阶段可评估维度 + 不可量化预留）。"""

    AREA = "面积"
    FLOOR = "楼层"
    ELEVATOR = "电梯"
    ORIENTATION = "朝向"
    YEAR_BUILT = "年代"
    RENOVATION = "装修维护"


class DifferenceDirection(StrEnum):
    """差异方向：有市场证据时可量化；否则保持 direction_only/unknown。"""

    UP = "上调"
    DOWN = "下调"
    FLAT = "持平"
    NO_ADJUST = "无调整"
    UNKNOWN = "未知"


# ---------------------------------------------------------------------------
# 依据（§9.5 强制方向/数值/公式/依据/规则版本，basis 必填）
# ---------------------------------------------------------------------------

REASON_AREA_CALIBER = "面积由单价口径处理（单价=总价/面积），无需书面面积调整"
REASON_EQUAL = "可比与目标房源该维度一致，无差异"
REASON_NO_LOCAL_EVIDENCE = (
    "本地成交证据不足，无市场依据量化；保持 direction_only/unknown，进区间与复核说明，不虚构比例"
)
REASON_RENOVATION_UNQUANTIFIABLE = (
    "装修/电器/维护状态不可稳定量化，默认进区间与复核说明，不做数值调整"
)
REASON_ATTRIBUTE_UNKNOWN = "目标或可比房源该维度未知，不臆测方向与数值"


class LocalEvidenceRecord(TypedDict):
    """已批准的本地成交证据（针对某个维度）：有因子/方向/公式才做数值调整。"""

    factor: Decimal
    direction: str
    basis: str
    formula: str


# ---------------------------------------------------------------------------
# 数据对象
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ComparableAttributes:
    """可比房源属性（用于与目标房源比较差异）。"""

    area_sqm: Decimal | None
    layout: str | None
    floor: int | None
    total_floors: int | None
    has_elevator: bool | None
    orientation: str | None
    year_built: int | None


@dataclass(frozen=True)
class DifferenceAdjustment:
    """单条房源差异调整（一个维度，一条 comp_adjustment(差异) 行）。"""

    adjustment_id: str
    candidate_id: str
    adjustment_type: str
    feature: str
    direction: str
    factor: Decimal | None
    formula: str | None
    basis: str
    evidence_strength: EvidenceStrength
    subject_side: str | None
    comparable_side: str | None
    warning: str | None
    rule_version: str


# ---------------------------------------------------------------------------
# 朝向/年代的定性方向（仅可明确判定时给 direction_only，否则 unknown）
# ---------------------------------------------------------------------------

_ORIENTATION_RANK = {
    "南": 5,
    "东南": 4,
    "西南": 4,
    "东": 3,
    "南向": 5,
    "南北": 5,
    "西": 2,
    "北": 1,
    "东西": 3,
    "东北": 3,
    "西北": 2,
}


def _orientation_rank(o: str | None) -> int | None:
    if not o:
        return None
    return _ORIENTATION_RANK.get(str(o).strip())


def _side_label(value: object) -> str | None:
    """把属性取值转成直观文本（None/'UNKNOWN' → None）。"""
    if value is None:
        return None
    text = str(value)
    if text in {"UNKNOWN", "None", "NA", "nan", ""}:
        return None
    return text


# ---------------------------------------------------------------------------
# 差异策略
# ---------------------------------------------------------------------------


class DifferenceAdjustmentPolicy:
    """房源差异策略：无市场证据不虚构数值（验收①/②）。

    ``local_evidence`` 为调用方提供的**已批准市场证据**（本地同小区成交对某维度的
    价格影响），一期真实数据稀疏时为 ``{}``；仅当存在证据才输出数值 factor，
    否则保持 direction_only/unknown。
    """

    def __init__(self, rule_version: str = DEFAULT_RULE_VERSION) -> None:
        self.rule_version = rule_version

    def adjust(
        self,
        *,
        candidate: CompCandidate,
        subject: SubjectProperty,
        comparable: ComparableAttributes,
        local_evidence: dict[str, LocalEvidenceRecord] | None = None,
    ) -> list[DifferenceAdjustment]:
        """为目标可比解析各维度差异调整（每维度一条，basis 必填）。"""
        evidence = local_evidence or {}
        vid = candidate.candidate_id
        rows: list[DifferenceAdjustment] = []

        # 1) 面积：单价口径已归一，无需书面调整（既定处理，非虚构）
        rows.append(
            self._base(
                vid,
                feature=DifferenceDimension.AREA,
                direction=DifferenceDirection.NO_ADJUST,
                factor=Decimal("1.0000"),
                formula=None,
                basis=REASON_AREA_CALIBER,
                strength=EvidenceStrength.HIGH,
                subject_side=None,
                comparable_side=None,
                warning=None,
            )
        )

        # 2) 电梯
        rows.append(
            self._adjust_nominal(
                vid,
                feature=DifferenceDimension.ELEVATOR,
                subject_value=subject.has_elevator,
                comparable_value=comparable.has_elevator,
                evidence=evidence.get(DifferenceDimension.ELEVATOR.value),
                subject_side=_side_label(subject.has_elevator),
                comparable_side=_side_label(comparable.has_elevator),
                subject_better=bool(subject.has_elevator),
            )
        )

        # 3) 朝向
        rows.append(
            self._adjust_orient(
                vid,
                feature=DifferenceDimension.ORIENTATION,
                s_rank=_orientation_rank(subject.orientation),
                c_rank=_orientation_rank(comparable.orientation),
                evidence=evidence.get(DifferenceDimension.ORIENTATION.value),
                subject_side=_side_label(subject.orientation),
                comparable_side=_side_label(comparable.orientation),
            )
        )

        # 4) 楼层：可比单元楼层一期不可得 → unknown（不臆测）
        if comparable.floor is not None and subject.floor is not None:
            if subject.floor > comparable.floor:
                direction, factor, basis = (
                    DifferenceDirection.UP,
                    None,
                    REASON_NO_LOCAL_EVIDENCE,
                )
                warning: str | None = REASON_NO_LOCAL_EVIDENCE
            elif subject.floor < comparable.floor:
                direction, factor, basis = (
                    DifferenceDirection.DOWN,
                    None,
                    REASON_NO_LOCAL_EVIDENCE,
                )
                warning = REASON_NO_LOCAL_EVIDENCE
            else:
                direction, factor, basis = (
                    DifferenceDirection.FLAT,
                    Decimal("1.0000"),
                    REASON_EQUAL,
                )
                warning = None
            rows.append(
                self._base(
                    vid,
                    feature=DifferenceDimension.FLOOR,
                    direction=direction,
                    factor=factor,
                    formula=None,
                    basis=basis,
                    strength=(
                        EvidenceStrength.HIGH if factor is not None else EvidenceStrength.LOW
                    ),
                    subject_side=str(subject.floor),
                    comparable_side=str(comparable.floor),
                    warning=warning,
                )
            )
        else:
            rows.append(
                self._base(
                    vid,
                    feature=DifferenceDimension.FLOOR,
                    direction=DifferenceDirection.UNKNOWN,
                    factor=None,
                    formula=None,
                    basis=REASON_ATTRIBUTE_UNKNOWN,
                    strength=EvidenceStrength.LOW,
                    subject_side=_side_label(subject.floor),
                    comparable_side=_side_label(comparable.floor),
                    warning="可比房源单元楼层一期不可得，无法比较，未做楼层调整",
                )
            )

        # 5) 年代
        if subject.year_built is not None and comparable.year_built is not None:
            if subject.year_built == comparable.year_built:
                direction, factor, basis = (
                    DifferenceDirection.FLAT,
                    Decimal("1.0000"),
                    REASON_EQUAL,
                )
                strength = EvidenceStrength.HIGH
                warning = None
                formula = None
            else:
                ev = evidence.get(DifferenceDimension.YEAR_BUILT.value)
                if ev is not None:
                    direction, factor, basis = (
                        DifferenceDirection(ev["direction"]),
                        ev["factor"],
                        ev["basis"],
                    )
                    strength = EvidenceStrength.MEDIUM
                    warning = None
                    formula = ev["formula"]
                else:
                    direction = (
                        DifferenceDirection.UP
                        if subject.year_built > comparable.year_built
                        else DifferenceDirection.DOWN
                    )
                    factor = None
                    basis = REASON_NO_LOCAL_EVIDENCE
                    strength = EvidenceStrength.LOW
                    warning = REASON_NO_LOCAL_EVIDENCE
                    formula = None
            rows.append(
                self._base(
                    vid,
                    feature=DifferenceDimension.YEAR_BUILT,
                    direction=direction,
                    factor=factor,
                    formula=formula,
                    basis=basis,
                    strength=strength,
                    subject_side=str(subject.year_built),
                    comparable_side=str(comparable.year_built),
                    warning=warning,
                )
            )
        else:
            rows.append(
                self._base(
                    vid,
                    feature=DifferenceDimension.YEAR_BUILT,
                    direction=DifferenceDirection.UNKNOWN,
                    factor=None,
                    formula=None,
                    basis=REASON_ATTRIBUTE_UNKNOWN,
                    strength=EvidenceStrength.LOW,
                    subject_side=_side_label(subject.year_built),
                    comparable_side=_side_label(comparable.year_built),
                    warning="年代信息缺失，无法比较，未做年代调整",
                )
            )

        # 6) 装修/维护：不可稳定量化 → unknown，进区间与复核说明
        rows.append(
            self._base(
                vid,
                feature=DifferenceDimension.RENOVATION,
                direction=DifferenceDirection.UNKNOWN,
                factor=None,
                formula=None,
                basis=REASON_RENOVATION_UNQUANTIFIABLE,
                strength=EvidenceStrength.INSUFFICIENT,
                subject_side=None,
                comparable_side=None,
                warning=REASON_RENOVATION_UNQUANTIFIABLE,
            )
        )

        return rows

    def _adjust_nominal(
        self,
        vid: str,
        *,
        feature: DifferenceDimension,
        subject_value: bool | None,
        comparable_value: bool | None,
        evidence: LocalEvidenceRecord | None,
        subject_side: str | None,
        comparable_side: str | None,
        subject_better: bool,
    ) -> DifferenceAdjustment:
        """电梯等布尔维度：等→1.0；异→有证据数值，否则 direction_only/unknown。"""
        if subject_value is None or comparable_value is None:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection.UNKNOWN,
                factor=None,
                formula=None,
                basis=REASON_ATTRIBUTE_UNKNOWN,
                strength=EvidenceStrength.LOW,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning="电梯信息缺失，无法比较，未做调整",
            )
        if subject_value == comparable_value:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection.FLAT,
                factor=Decimal("1.0000"),
                formula=None,
                basis=REASON_EQUAL,
                strength=EvidenceStrength.HIGH,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning=None,
            )
        if evidence is not None:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection(evidence["direction"]),
                factor=evidence["factor"],
                formula=evidence["formula"],
                basis=evidence["basis"],
                strength=EvidenceStrength.MEDIUM,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning=None,
            )
        direction = DifferenceDirection.UP if subject_better else DifferenceDirection.DOWN
        return self._base(
            vid,
            feature=feature,
            direction=direction,
            factor=None,
            formula=None,
            basis=REASON_NO_LOCAL_EVIDENCE,
            strength=EvidenceStrength.LOW,
            subject_side=subject_side,
            comparable_side=comparable_side,
            warning=REASON_NO_LOCAL_EVIDENCE,
        )

    def _adjust_orient(
        self,
        vid: str,
        *,
        feature: DifferenceDimension,
        s_rank: int | None,
        c_rank: int | None,
        evidence: LocalEvidenceRecord | None,
        subject_side: str | None,
        comparable_side: str | None,
    ) -> DifferenceAdjustment:
        if s_rank is None or c_rank is None:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection.UNKNOWN,
                factor=None,
                formula=None,
                basis=REASON_ATTRIBUTE_UNKNOWN,
                strength=EvidenceStrength.LOW,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning="朝向缺失或无法评级，无法比较，未做调整",
            )
        if s_rank == c_rank:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection.FLAT,
                factor=Decimal("1.0000"),
                formula=None,
                basis=REASON_EQUAL,
                strength=EvidenceStrength.HIGH,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning=None,
            )
        if evidence is not None:
            return self._base(
                vid,
                feature=feature,
                direction=DifferenceDirection(evidence["direction"]),
                factor=evidence["factor"],
                formula=evidence["formula"],
                basis=evidence["basis"],
                strength=EvidenceStrength.MEDIUM,
                subject_side=subject_side,
                comparable_side=comparable_side,
                warning=None,
            )
        direction = DifferenceDirection.UP if s_rank > c_rank else DifferenceDirection.DOWN
        return self._base(
            vid,
            feature=feature,
            direction=direction,
            factor=None,
            formula=None,
            basis=REASON_NO_LOCAL_EVIDENCE,
            strength=EvidenceStrength.LOW,
            subject_side=subject_side,
            comparable_side=comparable_side,
            warning=REASON_NO_LOCAL_EVIDENCE,
        )

    def _base(
        self,
        vid: str,
        *,
        feature: DifferenceDimension,
        direction: DifferenceDirection,
        factor: Decimal | None,
        formula: str | None,
        basis: str,
        strength: EvidenceStrength,
        subject_side: str | None,
        comparable_side: str | None,
        warning: str | None,
    ) -> DifferenceAdjustment:
        return DifferenceAdjustment(
            adjustment_id=f"ADJ-{vid}-D-{feature.value}",
            candidate_id=vid,
            adjustment_type=ADJUSTMENT_TYPE_DIFFERENCE,
            feature=feature.value,
            direction=direction.value,
            factor=factor,
            formula=formula,
            basis=basis,
            evidence_strength=strength,
            subject_side=subject_side,
            comparable_side=comparable_side,
            warning=warning,
            rule_version=self.rule_version,
        )


# ---------------------------------------------------------------------------
# 写盘：对齐既有时间行 + 追加差异行；原子写，差异部分幂等重建
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DifferenceAdjustResult:
    """一次 ``compsval valuation diff`` 的结果（供 CLI 打印与测试断言）。"""

    adjustment_path: Path
    adjustments: Sequence[DifferenceAdjustment] = ()
    """本次写入的差异行（adjustment_type=差异）。"""


def _align_to_schema(table: pa.Table) -> pa.Table:
    """把既有（可能旧 11 列）comp_adjustment 对齐到扩展模式（补新列为空）。"""
    schema = comp_adjustment_schema()
    for name in schema.names:
        if name not in table.column_names:
            field = schema.field(name)
            col = pa.nulls(table.num_rows, type=field.type)
            table = table.append_column(field, col)
    return table.cast(schema)


def comp_difference_table(
    adjustments: Sequence[DifferenceAdjustment],
    *,
    valuation_date: date,
    rule_version: str,
) -> pa.Table:
    """差异调整序列 → 扩展模式表（含 direction/factor/feature/formula/side）。

    ``valuation_date`` 为该次估值时点，写入每条差异行（valuation_date 非空）。
    """
    schema = comp_adjustment_schema()
    rows: dict[str, list[object]] = {name: [] for name in schema.names}
    for adj in adjustments:
        rows["adjustment_id"].append(adj.adjustment_id)
        rows["candidate_id"].append(adj.candidate_id)
        rows["adjustment_type"].append(adj.adjustment_type)
        rows["amount"].append(adj.factor)
        rows["sale_date"].append(None)
        rows["valuation_date"].append(valuation_date)
        rows["basis"].append(adj.basis)
        rows["evidence_strength"].append(adj.evidence_strength.value)
        rows["source_series"].append("无（房源差异不依赖时间序列）")
        rows["warning"].append(adj.warning)
        rows["rule_version"].append(rule_version)
        rows["direction"].append(adj.direction)
        rows["factor"].append(adj.factor)
        rows["feature"].append(adj.feature)
        rows["formula"].append(adj.formula)
        rows["subject_side"].append(adj.subject_side)
        rows["comparable_side"].append(adj.comparable_side)
    return pa.table(rows, schema=schema)


def _sale_attrs(valid_sale: pa.Table, sale_event_id: str) -> ComparableAttributes:
    """从 valid_sale 溯源可比房源属性。"""
    eids = valid_sale.column("sale_event_id").to_pylist()
    for i, eid in enumerate(eids):
        if str(eid) == sale_event_id:
            area = valid_sale.column("area_sqm")[i].as_py()
            layout = valid_sale.column("layout")[i].as_py()
            orient = valid_sale.column("orientation")[i].as_py()
            return ComparableAttributes(
                area_sqm=Decimal(str(area)) if area is not None else None,
                layout=str(layout) if layout is not None else None,
                floor=None,  # 单元级楼层一期不可得
                total_floors=None,
                has_elevator=None,
                orientation=str(orient) if orient else None,
                year_built=None,
            )
    return ComparableAttributes(None, None, None, None, None, None, None)


def _building_attrs(buildings: pa.Table, community_id: str) -> ComparableAttributes:
    """从 building 实体表按小区取其楼栋级属性（年代/总层/电梯，楼栋1）。"""
    cids = buildings.column("community_id").to_pylist()
    for i, cid in enumerate(cids):
        if cid is not None and str(cid) == community_id:
            return ComparableAttributes(
                area_sqm=None,
                layout=None,
                floor=None,
                total_floors=buildings.column("total_floors")[i].as_py(),
                has_elevator=buildings.column("has_elevator")[i].as_py(),
                orientation=None,
                year_built=buildings.column("year_built")[i].as_py(),
            )
    return ComparableAttributes(None, None, None, None, None, None, None)


def apply_difference_adjustments(
    *,
    data_dir: Path,
    subject: SubjectProperty,
    valid_sale: pa.Table,
    buildings: pa.Table,
    input_refs: Sequence[InputRef],
    rule_version: str = DEFAULT_RULE_VERSION,
) -> DifferenceAdjustResult:
    """WP6-D 主入口：读 WP6-B 选中可比 → 逐维差异 → 写 comp_adjustment 差异行。

    - 读 ``data/valuation/comp_candidate.parquet`` 选定可比；
    - 从 valid_sale 溯源可比属性、从 building 表（community_id）补楼栋级
      年代/总层/电梯；
    - 一期真实数据稀疏，本地成交证据 ``local_evidence={}`` → 楼层不可得以外
      的属性已知时给 direction_only，缺失 give unknown；不虚构比例（验收②）；
    - 保留既有时间行，原子写 comp_adjustment + DerivedManifest。
    """
    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    source = pq.read_table(candidate_path)

    adjustments: list[DifferenceAdjustment] = []
    policy = DifferenceAdjustmentPolicy(rule_version=rule_version)

    for row in source.to_pylist():
        if not row.get("selected"):
            continue
        candidate = CompCandidate(
            candidate_id=str(row["candidate_id"]),
            run_id=str(row["run_id"]),
            sale_event_id=str(row["sale_event_id"]),
            community_id=str(row["community_id"]),
            selected=bool(row["selected"]),
            tier=int(row["tier"]) if row.get("tier") is not None else None,
            similarity=(
                Decimal(str(row["similarity"]))
                if row.get("similarity") is not None
                else None
            ),
            reason=str(row["reason"]),
        )
        attrs = _sale_attrs(valid_sale, candidate.sale_event_id)
        bld = _building_attrs(buildings, candidate.community_id)
        merged = ComparableAttributes(
            area_sqm=attrs.area_sqm,
            layout=attrs.layout,
            floor=attrs.floor,
            total_floors=bld.total_floors,
            has_elevator=bld.has_elevator,
            orientation=attrs.orientation,
            year_built=bld.year_built,
        )
        adjustments.extend(
            policy.adjust(
                candidate=candidate,
                subject=subject,
                comparable=merged,
                local_evidence={},  # 一期真实数据稀疏：无本地证据 → 方向性/未知
            )
        )

    diff_table = comp_difference_table(
        adjustments, valuation_date=subject.valuation_date, rule_version=rule_version
    )

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    adjustment_path = valuation_dir / COMP_ADJUSTMENT_FILENAME
    work = valuation_dir / (COMP_ADJUSTMENT_FILENAME + ".incomplete")

    if adjustment_path.exists():
        existing = pq.read_table(adjustment_path)
        existing_aligned = _align_to_schema(existing)
        # 差异行整体重建（幂等）：只保留时间行，再追加本次差异行；
        # 避免重复运行 `compsval valuation diff` 时差异行翻倍。
        types = existing_aligned.column("adjustment_type").to_pylist()
        keep_mask = pa.array([t != ADJUSTMENT_TYPE_DIFFERENCE for t in types])
        time_rows = existing_aligned.filter(keep_mask)
        table = pa.concat_tables([time_rows, diff_table])
    else:
        table = diff_table

    pq.write_table(table, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=COMP_ADJUSTMENT_TABLE,
            built_at=datetime.now(UTC),
            row_count=table.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-D: 房源差异(adjustment_type=差异)，保留既有时间行",
        ),
        adjustment_path,
    )
    work.replace(adjustment_path)

    return DifferenceAdjustResult(adjustment_path=adjustment_path, adjustments=adjustments)