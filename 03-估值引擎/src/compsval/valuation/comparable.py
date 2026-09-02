"""WP6-B 可比层级与相似度（VAL1-003）：ComparableTierPolicy + SimilarityPolicy + 竞争小区关系。

技术方案 §9.3 逐级放宽：可比层级 A/B/C/D/E，A 层=1（最近似）逐级放宽；原则
是**一次只放宽一个主要条件**，层级按最近似优先择优归类（不可放宽一个条件达到
两个层级时取更近似的一个）。每个选中案例保留放宽轨迹（tier + reason，验收①/④）。

维度口径（技术方案 §9.3 表）：

- 空间：A/B/C = 同小区（``community_id == subject.community_id``）；D/E =
  **已确认**竞争小区（验收③：候选竞争小区关系未经人工确认 → 不自动用于 D/E）；
- 产品：A/C/D/E = 同类面积/户型；B = 适度放宽面积或户型（**仅放宽面积或仅放宽
  户型之一**，同时放宽面积与户型属于一次放宽两个主要条件 → 反例，不得归 B）；
- 时间：A/B/D = 优先近期（≤ recent_days）；C/E = 一年以内（≤ one_year_days）；
  同时放宽产品与时间（如 B 的产品放宽 + 超过近期窗口）→ 反例，无层级。

相似度分项（SimilarityPolicy，技术方案 §9.3）：面积/户型/楼层/电梯/朝向/年代，
每项带权重；**未知项不进数值加权**（验收②），仅对已知分项归一化；全未知则
``similarity=None``（未知用 None 不用 0）。

竞争小区关系（用户授权 2026-08-22 内建于 WP6-B）：同板块 + 无坐标（名录无坐标，
地理近似无法计算）→ 机器生成候选关系，置信 LOW、``confirmed=False``，构成
**待人工确认清单**；未确认前不用于 D/E；用户确认后置 ``confirmed=True`` 冻结为
正式竞争关系。

本模块产出：可比层级判定、相似度计算、竞争小区关系清单、更新后的 comp_candidate
列表（tier/similarity/selected/reason 全量留痕）。不改写 raw/staged/marts/entities
既有表。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import (
    BoundaryStatus,
    CompCandidate,
    SubjectProperty,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    DEFAULT_RULE_VERSION,
    VALUATION_LAYER,
    comp_candidate_table,
)

# ---------------------------------------------------------------------------
# 排除/分层理由（每行可溯源，验收①/④）
# ---------------------------------------------------------------------------

REASON_TIER_A = "层级A：同小区·同类面积/户型·近期成交（最相似）"
REASON_TIER_B = "层级B：同小区·适度放宽面积或户型·近期成交"
REASON_TIER_C = "层级C：同小区·同类面积/户型·一年以内成交"
REASON_TIER_D = "层级D：已确认竞争小区·同类面积/户型·近期成交"
REASON_TIER_E = "层级E：已确认竞争小区·同类面积/户型·一年以内成交"
REASON_NO_TIER = (
    "超出可比层级放宽范围（需一次放宽多个主要条件，如同时放宽面积与户型，"
    "或产品放宽叠加时间放宽），无法归入A-E，排除为可比"
)
REASON_NOT_COMPETITIVE = "小区既非同小区也非竞争小区，不作可比，排除"
REASON_COMP_UNCONFIRMED = (
    "竞争小区关系为机器生成候选、未经人工确认，不得自动用于D/E层级，排除"
)
REASON_NO_SALE_DATE = "成交日期缺失，无法判定时间层级，排除"

#: 竞争小区关系清单表名与模式名。
COMPETITIVE_RELATIONS_TABLE = "competitive_relations"
COMPETITIVE_RELATIONS_FILENAME = f"{COMPETITIVE_RELATIONS_TABLE}.parquet"


class CompetitiveConfidence(StrEnum):
    """竞争小区关系置信（无坐标核实 → 仅同板块为 LOW；人工确认后提升）。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"


@dataclass(frozen=True)
class CompetitiveRelation:
    """一条竞争小区候选关系（机器生成，待人工确认或已确认冻结）。

    relation_id 稳定、可溯源；``confirmed`` 区分机器生成（False）与人工确认
    （True）；D/E 层级只使用 ``confirmed=True`` 的关系（验收③）。
    """

    relation_id: str
    community_id: str
    competitor_id: str
    block: str
    basis: str
    confidence: CompetitiveConfidence
    confirmed: bool
    source_ref: str


# ---------------------------------------------------------------------------
# 相似度分项（验收②：未知项不进数值加权）
# ---------------------------------------------------------------------------


def _room_count(layout: str) -> int | None:
    """从户型文本解析室数（如 "2室1厅" → 2）；无 → None。"""
    marker = "室"
    idx = layout.find(marker)
    if idx <= 0:
        return None
    digits = layout[:idx].strip()
    return int(digits) if digits.isdigit() else None


def orientation_group(orientation: str) -> str:
    """朝向归组（南/北/东/西/其他），用于相似度而非虚构数值。"""
    if not orientation or orientation == "UNKNOWN":
        return "UNKNOWN"
    for key in ("南", "北", "东", "西"):
        if key in orientation:
            return key
    return "其他"


@dataclass(frozen=True)
class SimilarityPolicy:
    """相似度分项策略（技术方案 §9.3）；带 rule_version。

    factor_weights：((分项名, 权重), ...)；每项子相似度 ∈ [0,1]。分项值缺失
    （None/UNKNOWN）时该分项不计权；最终相似度 = Σ(子分×权重) / Σ(已知权重)；
    全未知 → None（未知不用 0，验收②）。
    """

    rule_version: str = DEFAULT_RULE_VERSION
    factor_weights: tuple[tuple[str, Decimal], ...] = (
        ("area", Decimal("0.35")),
        ("layout", Decimal("0.25")),
        ("floor", Decimal("0.10")),
        ("elevator", Decimal("0.05")),
        ("orientation", Decimal("0.15")),
        ("year_built", Decimal("0.10")),
    )
    area_scale: Decimal = Decimal("0.35")  # 面积相似度随相对差线性下降的带宽

    def _area(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None:
            return None
        sub = Decimal(str(subject.area_sqm))
        cand = Decimal(str(value))
        if sub == 0:
            return Decimal("1") if cand == 0 else Decimal("0")
        rel = abs(cand - sub) / sub
        if self.area_scale == 0:
            return Decimal("1") if rel == 0 else Decimal("0")
        return Decimal("1") - min(rel / self.area_scale, Decimal("1"))

    def _layout(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None or str(value) in ("", "UNKNOWN"):
            return None
        cand = str(value)
        if cand == subject.layout:
            return Decimal("1")
        sc = _room_count(subject.layout)
        cc = _room_count(cand)
        if sc and cc and sc == cc:  # 同室数不同厅 → 相似
            return Decimal("0.5")
        return Decimal("0")

    def _floor(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None or subject.floor is None or not isinstance(value, int):
            return None  # 任一侧未知或非整数 → 不计权
        return Decimal("1") if value == subject.floor else Decimal("0.4")

    def _elevator(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None or subject.has_elevator is None:
            return None
        return Decimal("1") if bool(value) == subject.has_elevator else Decimal("0")

    def _orientation(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None or str(value) in ("", "UNKNOWN"):
            return None
        sg = orientation_group(subject.orientation)
        cg = orientation_group(str(value))
        if sg == cg:
            return Decimal("1")
        return Decimal("0.4")

    def _year_built(self, subject: SubjectProperty, value: object) -> Decimal | None:
        if value is None or subject.year_built is None or not isinstance(value, int):
            return None
        diff = abs(value - subject.year_built)
        if diff <= 2:
            return Decimal("1")
        if diff <= 5:
            return Decimal("0.6")
        return Decimal("0.3")

    def similarity(
        self,
        subject: SubjectProperty,
        attrs: Mapping[str, object],
    ) -> Decimal | None:
        """目标房源 + 案例属性 → 加权相似度（未知分项不计权，验收②）。

        attrs 键：area/layout/floor/elevator/orientation/year_built；缺失或
        None 代表该属性对案例未知 → 该分项不进加权。
        """
        factors = {
            "area": self._area,
            "layout": self._layout,
            "floor": self._floor,
            "elevator": self._elevator,
            "orientation": self._orientation,
            "year_built": self._year_built,
        }
        total_weight = Decimal("0")
        weighted = Decimal("0")
        for name, weight in self.factor_weights:
            score = factors[name](subject, attrs.get(name))
            if score is not None:
                total_weight += weight
                weighted += score * weight
        if total_weight == 0:
            return None  # 全未知 → None（未知不用 0）
        return weighted / total_weight


# ---------------------------------------------------------------------------
# 可比层级（一次只放宽一个主要条件，验收①）
# ---------------------------------------------------------------------------


def area_level(
    subject_area: Decimal,
    cand_area: object,
    tight_band: Decimal,
    wide_band: Decimal,
) -> str:
    """面积相对差分层级：same（同类）/ relaxed（适度放宽）/ out（超出）。"""
    if cand_area is None:
        return "out"
    cand = Decimal(str(cand_area))
    if subject_area == 0:
        return "same" if cand == 0 else "out"
    rel = abs(cand - subject_area) / subject_area
    if rel <= tight_band:
        return "same"
    if rel <= wide_band:
        return "relaxed"
    return "out"


def layout_level(subject_layout: str, cand_layout: object) -> str:
    """户型层级：same（同类）/ relaxed（同室数）/ out（不同/未知）。"""
    if cand_layout is None or str(cand_layout) in ("", "UNKNOWN"):
        return "out"
    cand = str(cand_layout)
    if cand == subject_layout:
        return "same"
    sc = _room_count(subject_layout)
    cc = _room_count(cand)
    return "relaxed" if (sc and cc and sc == cc) else "out"


def product_level(
    *,
    subject_area: Decimal,
    subject_layout: str,
    cand_area: object,
    cand_layout: object,
    tight_band: Decimal,
    wide_band: Decimal,
) -> str:
    """产品维度：same（同类面积且同类户型）/ relaxed（仅放宽面积或仅放宽户型
    之一）/ out（同时放宽面积与户型，或任一超出放宽带宽 → 反例，不得归 B）。

    一次只放宽一个主要条件：relaxed 仅在面积与户型**恰有一个**被放宽、另一
    个保持同类时成立；两者同时放宽 → out（验收①反例）。
    """
    al = area_level(subject_area, cand_area, tight_band, wide_band)
    ll = layout_level(subject_layout, cand_layout)
    if al == "same" and ll == "same":
        return "same"
    if (al == "relaxed" and ll == "same") or (al == "same" and ll == "relaxed"):
        return "relaxed"
    return "out"


@dataclass(frozen=True)
class ComparableTierPolicy:
    """逐级放宽策略（技术方案 §9.3）。

    配置项（面积带/近期天数）为 WP6 待定参数的候选默认值，仅供候选参数实验，
    不得当正式规则（技术方案 §9.3 末）。规则改动 → 新 rule_version 输出。
    """

    rule_version: str = DEFAULT_RULE_VERSION
    area_tight_band: Decimal = Decimal("0.20")  # 同类面积：±20%
    area_wide_band: Decimal = Decimal("0.35")  # 适度放宽面积：±35%
    recent_days: int = 180  # “优先近期”默认 180 天
    one_year_days: int = 365

    def tier_of(
        self,
        *,
        subject: SubjectProperty,
        cand_sale_date: date | None,
        cand_community: str,
        cand_area: object,
        cand_layout: object,
        confirmed_competitors: set[str],
        candidate_competitors: set[str],
    ) -> tuple[int | None, str]:
        """按最近似优先为单个案例判定层级与理由（全量留痕，验收①/④）。

        返回 ``(tier, reason)``；``tier=None`` 表示不当作可比（非竞争小区、
        竞争未确认、超出放宽范围反例、缺成交日期）。
        """
        if cand_sale_date is None:
            return None, REASON_NO_SALE_DATE
        same_space = cand_community == subject.community_id
        days = max((subject.valuation_date - cand_sale_date).days, 0)
        recent = days <= self.recent_days
        within_year = days <= self.one_year_days
        prd = product_level(
            subject_area=Decimal(str(subject.area_sqm)),
            subject_layout=subject.layout,
            cand_area=cand_area,
            cand_layout=cand_layout,
            tight_band=self.area_tight_band,
            wide_band=self.area_wide_band,
        )

        if same_space:
            if prd == "same" and recent:
                return 1, REASON_TIER_A
            if prd == "relaxed" and recent:
                return 2, REASON_TIER_B
            if prd == "same" and within_year:
                return 3, REASON_TIER_C
            return None, REASON_NO_TIER  # 放宽叠加或超带宽 → 反例

        if cand_community in candidate_competitors:
            if cand_community not in confirmed_competitors:
                return None, REASON_COMP_UNCONFIRMED  # 验收③
            if prd == "same" and recent:
                return 4, REASON_TIER_D
            if prd == "same" and within_year:
                return 5, REASON_TIER_E
            return None, REASON_NO_TIER
        return None, REASON_NOT_COMPETITIVE


# ---------------------------------------------------------------------------
# 竞争小区关系发现（同板块 + 无坐标 → 机器生成候选，置信 LOW）
# ---------------------------------------------------------------------------


def competitive_relations_of(
    subject: SubjectProperty, communities: pa.Table
) -> list[CompetitiveRelation]:
    """从 community 权威表为目标小区发现同板块候选竞争关系（验收③）。

    规则（用户授权 2026-08-22）：候选竞争小区 = 与目标小区**同板块**且
    ``boundary_status == 机器确认`` 的其他小区。名录无坐标 → 地理近似无法计算，
    关系置信一律 LOW、``confirmed=False``，构成待人工确认清单；未确认不得用于
    D/E。逐条可溯源（source_ref 指向 community 两行的来源定位）。
    """
    if "community_id" not in communities.column_names or "block" not in communities.column_names:
        return []
    ids = communities.column("community_id").to_pylist()
    blocks = communities.column("block").to_pylist()
    refs = (
        communities.column("source_ref").to_pylist()
        if "source_ref" in communities.column_names
        else ["UNKNOWN"] * communities.num_rows
    )
    boundary = (
        communities.column("boundary_status").to_pylist()
        if "boundary_status" in communities.column_names
        else ["机器确认"] * communities.num_rows
    )

    sub_block: str | None = None
    sub_ref: str = "UNKNOWN"
    for i in range(communities.num_rows):
        if str(ids[i]) == subject.community_id:
            sub_block = str(blocks[i])
            sub_ref = str(refs[i])

    relations: list[CompetitiveRelation] = []
    if sub_block is None:
        return relations
    for i in range(communities.num_rows):
        cid = str(ids[i])
        if cid == subject.community_id:
            continue
        if str(blocks[i]) != sub_block:
            continue
        if str(boundary[i]) != BoundaryStatus.MACHINE_CONFIRMED.value:
            continue
        relations.append(
            CompetitiveRelation(
                relation_id=f"COMP-{subject.community_id}-{cid}",
                community_id=subject.community_id,
                competitor_id=cid,
                block=sub_block,
                basis=f"同板块(房天下)：{sub_block}；无坐标，地理近似未核实",
                confidence=CompetitiveConfidence.LOW,
                confirmed=False,
                source_ref=f"community:{sub_ref}+community:{str(refs[i])}",
            )
        )
    relations.sort(key=lambda r: r.competitor_id)
    return relations


# ---------------------------------------------------------------------------
# 分层入口：更新 comp_candidate（tier/similarity/selected/reason）
# ---------------------------------------------------------------------------


def _candidate_attrs(
    valid_sale: pa.Table,
    sale_event_id: str,
) -> dict[str, object]:
    """从 valid_sale 取案例属性（缺失列 → None，随确认进/不进权，验收②）。"""
    by_event = valid_sale.column("sale_event_id").to_pylist()
    idx = None
    for i, eid in enumerate(by_event):
        if str(eid) == sale_event_id:
            idx = i
            break
    if idx is None:
        return {}
    attrs: dict[str, object] = {
        "area": _column_at(valid_sale, "area_sqm", idx),
        "layout": _column_at(valid_sale, "layout", idx),
        "orientation": _column_at(valid_sale, "orientation", idx),
        "floor": None,
        "elevator": None,
        "year_built": None,
    }
    # valid_sale（mart）未携带楼层/电梯/年代；若扩列版补了列则读取，否则保持 None
    # （excel-attribute-enrichment：year_built/has_elevator 由身份键回填；缺列即旧行为）
    for name, key in (
        ("floor", "floor"),
        ("has_elevator", "elevator"),
        ("year_built", "year_built"),
    ):
        if name in valid_sale.column_names:
            attrs[key] = _column_at(valid_sale, name, idx)
    return attrs


def _column_at(table: pa.Table, name: str, idx: int) -> object:
    if name not in table.column_names:
        return None
    value = table.column(name)[idx].as_py()
    return value


def _sale_date_at(valid_sale: pa.Table, sale_event_id: str) -> date | None:
    by_event = valid_sale.column("sale_event_id").to_pylist()
    for i, eid in enumerate(by_event):
        if str(eid) == sale_event_id:
            value = valid_sale.column("sale_date")[i].as_py()
            return value if isinstance(value, date) else None
    return None


def select_comparables(
    candidates: Sequence[CompCandidate],
    valid_sale: pa.Table,
    subject: SubjectProperty,
    *,
    relations: Sequence[CompetitiveRelation] = (),
    tier_policy: ComparableTierPolicy | None = None,
    similarity_policy: SimilarityPolicy | None = None,
) -> list[CompCandidate]:
    """对 WP6-A 候选池分层/算相似度 → 新 comp_candidate 列表（全量留痕）。

    - 候选池排除行（selected=False）保留原 reason，tier/similarity 仍为 None；
    - 候选池入选行按 ComparableTierPolicy 判定层级与理由；归入 A-E 者计算
      similarity 并保持 selected=True；无法归入（非竞争/未确认/放宽反例/缺日期）
      者 selected 置 False 并写出原因，tier/similarity=None；
    - 竞争关系通过 relations 提供：``confirmed=True`` 者进 D/E，未确认（候选）
      者按验收③排除并将待确认清单输出（本函数返回的 relations 即清单）。
    """
    tp = tier_policy or ComparableTierPolicy()
    sp = similarity_policy or SimilarityPolicy()
    confirmed = {r.competitor_id for r in relations if r.confirmed}
    candidates_all = {r.competitor_id for r in relations}

    result: list[CompCandidate] = []
    for cand in candidates:
        if not cand.selected:
            result.append(cand)  # 候选池排除行保留
            continue
        sale_date = _sale_date_at(valid_sale, cand.sale_event_id)
        attrs = _candidate_attrs(valid_sale, cand.sale_event_id)
        tier, reason = tp.tier_of(
            subject=subject,
            cand_sale_date=sale_date,
            cand_community=cand.community_id,
            cand_area=attrs.get("area"),
            cand_layout=attrs.get("layout"),
            confirmed_competitors=confirmed,
            candidate_competitors=candidates_all,
        )
        if tier is None:
            result.append(
                CompCandidate(
                    candidate_id=cand.candidate_id,
                    run_id=cand.run_id,
                    sale_event_id=cand.sale_event_id,
                    community_id=cand.community_id,
                    selected=False,
                    tier=None,
                    similarity=None,
                    reason=reason,
                )
            )
            continue
        sim = sp.similarity(subject, attrs)
        result.append(
            CompCandidate(
                candidate_id=cand.candidate_id,
                run_id=cand.run_id,
                sale_event_id=cand.sale_event_id,
                community_id=cand.community_id,
                selected=True,
                tier=tier,
                similarity=sim,
                reason=reason,
            )
        )
    result.sort(key=lambda c: c.candidate_id)
    return result


# ---------------------------------------------------------------------------
# 竞争小区关系清单表模式与写盘
# ---------------------------------------------------------------------------


def competitive_relations_schema() -> pa.Schema:
    """``competitive_relations`` 清单 PyArrow 模式（待人工确认/已确认）。"""
    return pa.schema(
        [
            pa.field("relation_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("competitor_id", pa.string(), nullable=False),
            pa.field("block", pa.string(), nullable=False),
            pa.field("basis", pa.string(), nullable=False),
            pa.field("confidence", pa.string(), nullable=False),
            pa.field("confirmed", pa.bool_(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
        ]
    )


def competitive_relations_table(
    relations: Sequence[CompetitiveRelation],
    *,
    rule_version: str,
) -> pa.Table:
    """竞争关系序列 → 清单表（rule_version 列便于追溯）。"""
    rows: dict[str, list[object]] = {name: [] for name in competitive_relations_schema().names}
    for rel in relations:
        rows["relation_id"].append(rel.relation_id)
        rows["community_id"].append(rel.community_id)
        rows["competitor_id"].append(rel.competitor_id)
        rows["block"].append(rel.block)
        rows["basis"].append(rel.basis)
        rows["confidence"].append(rel.confidence.value)
        rows["confirmed"].append(rel.confirmed)
        rows["source_ref"].append(rel.source_ref)
        rows["rule_version"].append(rule_version)
    return pa.table(rows, schema=competitive_relations_schema())


@dataclass(frozen=True)
class TierResult:
    """一次 ``compsval valuation tier`` 的结果（供 CLI 打印与测试断言）。"""

    candidate_path: Path
    relations_path: Path
    candidates: list[CompCandidate]
    relations: list[CompetitiveRelation]


def apply_tiers_to_candidate_table(
    *,
    data_dir: Path,
    subject: SubjectProperty,
    valid_sale: pa.Table,
    communities: pa.Table,
    input_refs: Sequence[InputRef],
    rule_version: str = DEFAULT_RULE_VERSION,
) -> TierResult:
    """WP6-B 主入口：读者 WP6-A comp_candidate → 分层/相似度 → 写回 + 关系清单。

    - 从 ``data/valuation/comp_candidate.parquet``（WP6-A 产出）读候选池；
    - 从 community 权威表发现候选竞争小区关系（待人工确认清单）；
    - ``select_comparables`` 更新 tier/similarity/selected/reason；
    - 原子写回 comp_candidate.parquet（同一 run_id 再生，不追加）并写
      competitive_relations.parquet + 各自 DerivedManifest。
    """
    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    source = pq.read_table(candidate_path)

    source_candidates: list[CompCandidate] = []
    for row in zip(
        *[source.column(name).to_pylist() for name in (
            "candidate_id", "run_id", "sale_event_id", "community_id",
            "selected", "tier", "similarity", "reason",
        )],
        strict=True,
    ):
        source_candidates.append(
            CompCandidate(
                candidate_id=str(row[0]),
                run_id=str(row[1]),
                sale_event_id=str(row[2]),
                community_id=str(row[3]),
                selected=bool(row[4]),
                tier=int(row[5]) if row[5] is not None else None,
                similarity=Decimal(str(row[6])) if row[6] is not None else None,
                reason=str(row[7]),
            )
        )

    relations = competitive_relations_of(subject, communities)
    updated = select_comparables(
        source_candidates, valid_sale, subject, relations=relations
    )
    result_table = comp_candidate_table(updated, valid_sale, rule_version=rule_version)
    relations_table = competitive_relations_table(relations, rule_version=rule_version)

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    work = valuation_dir / (COMP_CANDIDATE_FILENAME + ".incomplete")
    pq.write_table(result_table, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table="comp_candidate",
            built_at=datetime.now(UTC),
            row_count=result_table.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-B: 可比层级+相似度重写",
        ),
        candidate_path,
    )
    work.replace(candidate_path)

    relations_path = valuation_dir / COMPETITIVE_RELATIONS_FILENAME
    rel_work = valuation_dir / (COMPETITIVE_RELATIONS_FILENAME + ".incomplete")
    pq.write_table(relations_table, rel_work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=COMPETITIVE_RELATIONS_TABLE,
            built_at=datetime.now(UTC),
            row_count=relations_table.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-B: 候选竞争小区关系（待人工确认）",
        ),
        relations_path,
    )
    rel_work.replace(relations_path)

    return TierResult(
        candidate_path=candidate_path,
        relations_path=relations_path,
        candidates=updated,
        relations=relations,
    )