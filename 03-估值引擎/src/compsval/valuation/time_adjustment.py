"""WP6-C 时间修正（VAL1-004）：TimeAdjustmentPolicy + comp_adjustment(时间)。

技术方案 §9.4 时间修正策略，为 WP6-B 选定的每个可比案例把成交价修订到估值
时点。**只用估值时点之前（含当日）可得的数据**（验收①，反例：未来数据不得
进入）；无可靠序列绝不默认“市场不变”，进入降级路径（验收②）。每次修正输出来源
序列、观察窗口、原日期、估值时点、调整系数、证据强度与警告（验收③）；每条
comp_adjustment 带 basis（验收④）。

证据序列自下而上（§9.4 顺序）：
- 同小区滚动成交序列：由 valid_sale 同小区单价按月取中位数构建；
- 同小区更宽产品口径 / 竞争板块序列：market_series 板块月度均价（可用性按
  ``month_end <= valuation_date`` 判定，未来月不计）；
- 目标区/示例城市聚合指数：第一阶段未建，一律不可用；
- 无可靠序列 → 降级：不输出虚构系数（amount=None），把证据缺失传给下游
  （WP6-E 扩大区间/降低可信度/排除过旧案例）。

调整系数为**乘法系数**（§9.4 “调整系数”）：修订价 = 原单价 × coefficient。
成交日与估值时点同日（零时间差）→ 无时间修正，coefficient=1.0（确定，非猜测）；
其他情况仅在存在可用序列且更新时间锚点得出可靠系数时输出数值，否则降级。

本模块只产出一条中间结果表 ``comp_adjustment``（adjustment_type=时间），不改写
raw/staged/marts/entities/comp_candidate 既有表；不改做回放校准（归 WP8）。
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path

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

#: comp_adjustment 中间结果表（adjustment_type=时间；WP6-D 追加差异行）。
COMP_ADJUSTMENT_TABLE = "comp_adjustment"
COMP_ADJUSTMENT_FILENAME = f"{COMP_ADJUSTMENT_TABLE}.parquet"

#: comp_adjustment 允许的调整类型之一（数据字典 §3.12）。
ADJUSTMENT_TYPE_TIME = "时间"


class EvidenceStrength(StrEnum):
    """时间修正证据强度（§9.4）：高/中/低/不足。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"
    INSUFFICIENT = "不足"


class TimeSourceSeries(StrEnum):
    """时间修正所依据的来源序列（可追溯）。"""

    SAME_COMMUNITY = "同小区滚动成交序列"
    COMPETITIVE_BLOCK = "竞争板块序列(market_series)"
    AGGREGATE_INDEX = "目标区/示例城市聚合指数"
    SAME_TIME = "同判定时点(零时间差)"
    NONE = "无可靠时间序列"


# ---------------------------------------------------------------------------
# 纳入/降级理由（每行可溯源，验收③/④）
# ---------------------------------------------------------------------------

REASON_NO_SALE_DATE = "成交日期缺失，无法判定时间差，不做时间修正"
REASON_FUTURE_SALE = (
    "成交日期晚于估值时点（未来成交），估值时点尚不可得，不得做时间修正，降级"
)
REASON_SAME_TIME = (
    "成交日与估值时点同日（零时间差），无时间修正需求，系数=1.0（确定非猜测）"
)
REASON_SAME_COMMUNITY = "同小区滚动成交序列（按月单价中位数）"
REASON_COMPETITIVE_BLOCK = "竞争板块序列(market_series)：{block}"
REASON_DEGRADE = (
    "无可靠时间序列（同小区序列不足或板块/聚合序列不可用），不得默认市场不变，"
    "不做时间修正；由下游(WP6-E)扩大区间/降低可信度/排除过旧案例"
)
REASON_FUTURE_SERIES = (
    "可用市场序列均为估值时点之后的未来数据，不得用于计算时间修正，降级"
)


# ---------------------------------------------------------------------------
# 时间序列辅助
# ---------------------------------------------------------------------------


def month_of(d: date) -> date:
    """日期 → 所在月的首日（序列按月聚合）。"""
    return d.replace(day=1)


def month_end(d: date) -> date:
    """``month_end(month 首日)`` → 该月最后一天（用于未来数据可用性判定）。"""
    if d.month == 12:
        return date(d.year + 1, 1, 1) - timedelta(days=1)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def same_community_index(
    valid_sale: pa.Table,
    community_id: str,
    *,
    valuation_date: date,
) -> list[tuple[date, Decimal]]:
    """同小区滚动成交序列：valid_sale 同小区单价按月取中位数。

    只纳入 ``sale_date <= valuation_date`` 的成交（验收①：截点之后不进入）。
    返回升序 ``(month首日, 月单价中位数)``；无有效数据返回空表。
    """
    required = ("sale_date", "community_id", "unit_price")
    missing = [c for c in required if c not in valid_sale.column_names]
    if missing:
        raise ValueError(f"valid_sale 缺少必要列: {', '.join(missing)}")

    by_month: dict[tuple[int, int], list[Decimal]] = {}
    for row in zip(
        *[valid_sale.column(c).to_pylist() for c in required],
        strict=True,
    ):
        sale_date = row[0]
        comm = row[1]
        unit_price = row[2]
        if not isinstance(sale_date, date):
            continue
        if sale_date > valuation_date:
            continue  # 未来数据不进入（验收①）
        if comm != community_id:
            continue
        if unit_price is None:
            continue
        try:
            price = Decimal(str(unit_price))
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        by_month.setdefault((sale_date.year, sale_date.month), []).append(price)

    index: list[tuple[date, Decimal]] = []
    for (year, month), prices in sorted(by_month.items()):
        index.append((date(year, month, 1), Decimal(str(statistics.median(prices)))))
    return index


def block_series(
    market_series: pa.Table,
    block: str,
    *,
    valuation_date: date,
) -> list[tuple[date, Decimal]]:
    """竞争板块序列：market_series 中该板块、且截点前可用的月度均价。

    ``month_end(month) <= valuation_date`` 才视为当时可得（验收①：未来月排除）。
    返回升序 ``(month首日, price)``；无可用数据返回空表。
    """
    required = ("region", "month", "price")
    missing = [c for c in required if c not in market_series.column_names]
    if missing:
        raise ValueError(f"market_series 缺少必要列: {', '.join(missing)}")

    rows: list[tuple[date, Decimal]] = []
    for region, month, price in zip(
        *[market_series.column(c).to_pylist() for c in required],
        strict=True,
    ):
        if region != block:
            continue
        if not isinstance(month, date):
            continue
        if month_end(month) > valuation_date:
            continue  # 未来月（当月统计月末才可知）
        if price is None:
            continue
        try:
            p = Decimal(str(price))
        except (TypeError, ValueError):
            continue
        if p <= 0:
            continue
        rows.append((month, p))
    rows.sort(key=lambda x: x[0])
    return rows


def _price_at_or_before(index: Sequence[tuple[date, Decimal]], month: date) -> Decimal | None:
    """升序序列中 ``<= month`` 的最近月价格；无返回 None。"""
    latest: Decimal | None = None
    for m, p in index:
        if m <= month:
            latest = p
        else:
            break
    return latest


def coefficient_from_index(
    index: Sequence[tuple[date, Decimal]],
    sale_month: date,
    *,
    min_series_points: int,
) -> Decimal | None:
    """由可用序列推出乘法调整系数（估值时点价 / 成交月价）。

    条件（皆为可靠趋势的必要条件）：
    - 序列月数 >= min_series_points；
    - 成交月有锚点价（<= 成交月最近月）；
    - 序列最新月晚于成交月（存在成交后的前瞻锚点，才能测量市场移动）；否则
      只有成交月同月数据 → 无市场移动证据 → 不输出系数。
    返回系数（4 位小数，ROUND_HALF_UP）；不满足任一条件 → None。
    """
    if len(index) < min_series_points:
        return None
    idx_sale = _price_at_or_before(index, sale_month)
    idx_val = index[-1][1]  # 序列全部 <= 估值时点（已过滤），取最新月为时点价
    last_month = index[-1][0]
    if idx_sale is None or idx_val is None:
        return None
    if last_month <= sale_month:
        return None  # 只有成交月及之前数据，无成交后锚点
    if idx_sale == 0:
        return None
    try:
        ratio = idx_val / idx_sale
    except (ZeroDivisionError, InvalidOperation):
        return None
    return ratio.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _series_available_in_future(
    market_series: pa.Table, valuation_date: date
) -> bool:
    """是否存在截点后的市场序列月（用于“全部为未来数据”的降级判定）。"""
    if "month" not in market_series.column_names:
        return False
    months = market_series.column("month").to_pylist()
    return any(isinstance(m, date) and month_end(m) > valuation_date for m in months)


# ---------------------------------------------------------------------------
# 时间修正策略
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TimeAdjustment:
    """一条可比案例的时间修正记录（comp_adjustment 时间行）。

    校正系数 ``coefficient``（乘法）在无可靠证据时为 None（未知不用 0，验收②），
    语义由 basis/evidence_strength/warning 说明，把降级交给下游。
    """

    adjustment_id: str
    candidate_id: str
    adjustment_type: str
    sale_date: date | None
    valuation_date: date
    amount: Decimal | None  # 乘法系数；降级=不足 → None
    basis: str
    evidence_strength: EvidenceStrength
    source_series: TimeSourceSeries
    warning: str | None
    rule_version: str


@dataclass(frozen=True)
class TimeAdjustmentPolicy:
    """时间修正策略集（技术方案 §9.4）；带 rule_version。

    候选参数（min_series_points 等）为 WP6 待定参数的候选默认值，仅供候选参数
    实验，不得当正式规则（§9.4 末：公式由 DATA-001 与历史回放决定）。
    """

    rule_version: str = DEFAULT_RULE_VERSION
    min_series_points: int = 2  # 同小区/板块序列最少月数

    def adjust(
        self,
        *,
        candidate: CompCandidate,
        subject: SubjectProperty,
        sale_date: date | None,
        valid_sale: pa.Table,
        market_series: pa.Table,
        community_block: Mapping[str, str],
    ) -> TimeAdjustment:
        """为单个可比候选解析时间修正（只用估值时点前数据，验收①）。

        ``candidate`` 为 WP6-B 选定的可比候选；``sale_date`` 为其成交日（由
        valid_sale 溯源）；``community_block`` 为 community_id → 板块映射
        （D/E 竞争小区据此取板块序列）。
        """
        vid = candidate.candidate_id
        cid = candidate.community_id
        if sale_date is None:
            return TimeAdjustment(
                adjustment_id=f"ADJ-{vid}-T",
                candidate_id=vid,
                adjustment_type=ADJUSTMENT_TYPE_TIME,
                sale_date=None,
                valuation_date=subject.valuation_date,
                amount=None,
                basis=REASON_NO_SALE_DATE,
                evidence_strength=EvidenceStrength.INSUFFICIENT,
                source_series=TimeSourceSeries.NONE,
                warning="成交日期缺失，无法判定时间差（WP6-B 已排除可比，理论分支）",
                rule_version=self.rule_version,
            )

        days_gap = (subject.valuation_date - sale_date).days
        if days_gap < 0:
            # 未来成交：估值时点尚不可得，不得做时间修正（验收①纵深守卫，反例）
            return TimeAdjustment(
                adjustment_id=f"ADJ-{vid}-T",
                candidate_id=vid,
                adjustment_type=ADJUSTMENT_TYPE_TIME,
                sale_date=sale_date,
                valuation_date=subject.valuation_date,
                amount=None,
                basis=REASON_FUTURE_SALE,
                evidence_strength=EvidenceStrength.INSUFFICIENT,
                source_series=TimeSourceSeries.NONE,
                warning="成交日期晚于估值时点，基于估值时点不可得的数据，未做时间修正",
                rule_version=self.rule_version,
            )
        if days_gap == 0:
            return TimeAdjustment(
                adjustment_id=f"ADJ-{vid}-T",
                candidate_id=vid,
                adjustment_type=ADJUSTMENT_TYPE_TIME,
                sale_date=sale_date,
                valuation_date=subject.valuation_date,
                amount=Decimal("1.0"),
                basis=REASON_SAME_TIME,
                evidence_strength=EvidenceStrength.HIGH,
                source_series=TimeSourceSeries.SAME_TIME,
                warning=None,
                rule_version=self.rule_version,
            )

        # 1) 同小区滚动序列
        index = same_community_index(
            valid_sale, cid, valuation_date=subject.valuation_date
        )
        coeff = coefficient_from_index(
            index, month_of(sale_date), min_series_points=self.min_series_points
        )
        if coeff is not None:
            lo = _price_at_or_before(index, month_of(sale_date))
            basis = (
                f"{REASON_SAME_COMMUNITY}（{cid}）依系列月"
                + (f"={lo}→={index[-1][1]}(元/㎡，截点前)" if lo is not None else "")
                + f"；原成交日{sale_date}→估值时点{subject.valuation_date}"
            )
            return TimeAdjustment(
                adjustment_id=f"ADJ-{vid}-T",
                candidate_id=vid,
                adjustment_type=ADJUSTMENT_TYPE_TIME,
                sale_date=sale_date,
                valuation_date=subject.valuation_date,
                amount=coeff,
                basis=basis,
                evidence_strength=EvidenceStrength.HIGH,
                source_series=TimeSourceSeries.SAME_COMMUNITY,
                warning=None,
                rule_version=self.rule_version,
            )

        # 2) 竞争板块序列（market_series）
        block = community_block.get(cid)
        if block:
            bindex = block_series(market_series, block, valuation_date=subject.valuation_date)
            bcoeff = coefficient_from_index(
                bindex, month_of(sale_date), min_series_points=self.min_series_points
            )
            if bcoeff is not None:
                return TimeAdjustment(
                    adjustment_id=f"ADJ-{vid}-T",
                    candidate_id=vid,
                    adjustment_type=ADJUSTMENT_TYPE_TIME,
                    sale_date=sale_date,
                    valuation_date=subject.valuation_date,
                    amount=bcoeff,
                    basis=f"{REASON_COMPETITIVE_BLOCK.format(block=block)}；"
                    f"原成交日{sale_date}→估值时点{subject.valuation_date}",
                    evidence_strength=EvidenceStrength.MEDIUM,
                    source_series=TimeSourceSeries.COMPETITIVE_BLOCK,
                    warning="板块序列来源为平台聚合，未经校准",
                    rule_version=self.rule_version,
                )

        # 3) 聚合指数（第一阶段未建）
        # 4) 无可靠序列 → 降级（验收②）：不输出虚构系数
        if _series_available_in_future(market_series, subject.valuation_date):
            basis = REASON_FUTURE_SERIES
        else:
            basis = REASON_DEGRADE
        return TimeAdjustment(
            adjustment_id=f"ADJ-{vid}-T",
            candidate_id=vid,
            adjustment_type=ADJUSTMENT_TYPE_TIME,
            sale_date=sale_date,
            valuation_date=subject.valuation_date,
            amount=None,
            basis=basis,
            evidence_strength=EvidenceStrength.INSUFFICIENT,
            source_series=TimeSourceSeries.NONE,
            warning=(
                "数据不足：同小区滚动序列不足/板块或聚合序列不可用，未做时间修正；"
                "不得默认市场不变，由下游扩大区间/降低可信度/排除过旧案例"
            ),
            rule_version=self.rule_version,
        )


# ---------------------------------------------------------------------------
# comp_adjustment 表模式与构造（数据字典 §3.12 + 时间修正扩展列）
# ---------------------------------------------------------------------------


def comp_adjustment_schema() -> pa.Schema:
    """``comp_adjustment`` 中间结果 PyArrow 模式（§3.12 + 证据列扩展）。

    ``direction/factor/feature/formula/subject_side/comparable_side`` 为房源差异
    （WP6-D）扩展列，时间修正行一律为空；差异行据此记录方向/数值/公式/依据。
    """
    return pa.schema(
        [
            pa.field("adjustment_id", pa.string(), nullable=False),
            pa.field("candidate_id", pa.string(), nullable=False),
            pa.field("adjustment_type", pa.string(), nullable=False),
            pa.field("amount", pa.decimal128(12, 4), nullable=True),
            pa.field("sale_date", pa.date32(), nullable=True),
            pa.field("valuation_date", pa.date32(), nullable=False),
            pa.field("basis", pa.string(), nullable=False),
            pa.field("evidence_strength", pa.string(), nullable=False),
            pa.field("source_series", pa.string(), nullable=False),
            pa.field("warning", pa.string(), nullable=True),
            pa.field("rule_version", pa.string(), nullable=False),
            pa.field("direction", pa.string(), nullable=True),
            pa.field("factor", pa.decimal128(12, 4), nullable=True),
            pa.field("feature", pa.string(), nullable=True),
            pa.field("formula", pa.string(), nullable=True),
            pa.field("subject_side", pa.string(), nullable=True),
            pa.field("comparable_side", pa.string(), nullable=True),
        ]
    )


def comp_adjustment_table(
    adjustments: Sequence[TimeAdjustment],
    *,
    rule_version: str,
) -> pa.Table:
    """时间修正序列 → 中间结果表（携带证据列，验收③/④）。"""
    rows: dict[str, list[object]] = {name: [] for name in comp_adjustment_schema().names}
    for adj in adjustments:
        rows["adjustment_id"].append(adj.adjustment_id)
        rows["candidate_id"].append(adj.candidate_id)
        rows["adjustment_type"].append(adj.adjustment_type)
        rows["amount"].append(adj.amount)
        rows["sale_date"].append(adj.sale_date)
        rows["valuation_date"].append(adj.valuation_date)
        rows["basis"].append(adj.basis)
        rows["evidence_strength"].append(adj.evidence_strength.value)
        rows["source_series"].append(adj.source_series.value)
        rows["warning"].append(adj.warning)
        rows["rule_version"].append(rule_version)
        rows["direction"].append(None)
        rows["factor"].append(None)
        rows["feature"].append(None)
        rows["formula"].append(None)
        rows["subject_side"].append(None)
        rows["comparable_side"].append(None)
    return pa.table(rows, schema=comp_adjustment_schema())


def _sale_date_of(valid_sale: pa.Table, sale_event_id: str) -> date | None:
    """从 valid_sale 溯源候选成交日（用于调整计算）。"""
    by_event = valid_sale.column("sale_event_id").to_pylist()
    for i, eid in enumerate(by_event):
        if str(eid) == sale_event_id:
            value = valid_sale.column("sale_date")[i].as_py()
            return value if isinstance(value, date) else None
    return None


@dataclass(frozen=True)
class TimeAdjustResult:
    """一次 ``compsval valuation time`` 的结果（供 CLI 打印与测试断言）。"""

    adjustment_path: Path
    adjustments: list[TimeAdjustment]


def apply_time_adjustments(
    *,
    data_dir: Path,
    subject: SubjectProperty,
    valid_sale: pa.Table,
    market_series: pa.Table,
    communities: pa.Table,
    input_refs: Sequence[InputRef],
    rule_version: str = DEFAULT_RULE_VERSION,
) -> TimeAdjustResult:
    """WP6-C 主入口：读 WP6-B comp_candidate → 时间修正 → 写 comp_adjustment。

    - 从 ``data/valuation/comp_candidate.parquet`` 读 WP6-B 选定可比（selected）；
    - 读 valid_sale 溯源成交日、community 建 ``community_id → block`` 映射；
    - 为每个可比解析时间修正（只用估值时点前数据，验收①；无证据降级，验收②）；
    - 原子写 ``comp_adjustment.parquet``（adjustment_type=时间）+ DerivedManifest。
    """
    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    source = pq.read_table(candidate_path)

    adjustments: list[TimeAdjustment] = []
    policy = TimeAdjustmentPolicy(rule_version=rule_version)

    block_map: dict[str, str] = {}
    if "community_id" in communities.column_names and "block" in communities.column_names:
        for cid, blk in zip(
            communities.column("community_id").to_pylist(),
            communities.column("block").to_pylist(),
            strict=True,
        ):
            if cid is not None and blk is not None:
                block_map[str(cid)] = str(blk)

    for row in source.to_pylist():
        if not row.get("selected"):
            continue  # 只对可比案例做时间修正
        candidate = CompCandidate(
            candidate_id=str(row["candidate_id"]),
            run_id=str(row["run_id"]),
            sale_event_id=str(row["sale_event_id"]),
            community_id=str(row["community_id"]),
            selected=bool(row["selected"]),
            tier=int(row["tier"]) if row.get("tier") is not None else None,
            similarity=(
                Decimal(str(row["similarity"])) if row.get("similarity") is not None else None
            ),
            reason=str(row["reason"]),
        )
        sale_date = _sale_date_of(valid_sale, candidate.sale_event_id)
        adjustment = policy.adjust(
            candidate=candidate,
            subject=subject,
            sale_date=sale_date,
            valid_sale=valid_sale,
            market_series=market_series,
            community_block=block_map,
        )
        adjustments.append(adjustment)

    table = comp_adjustment_table(adjustments, rule_version=rule_version)

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    adjustment_path = valuation_dir / COMP_ADJUSTMENT_FILENAME
    work = valuation_dir / (COMP_ADJUSTMENT_FILENAME + ".incomplete")
    pq.write_table(table, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=COMP_ADJUSTMENT_TABLE,
            built_at=datetime.now(UTC),
            row_count=table.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-C: 时间修正(adjustment_type=时间)",
        ),
        adjustment_path,
    )
    work.replace(adjustment_path)

    return TimeAdjustResult(
        adjustment_path=adjustment_path,
        adjustments=adjustments,
    )