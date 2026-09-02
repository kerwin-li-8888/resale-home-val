"""WP6-E 中心/区间/可信度/输出状态（VAL1-006）：Aggregation/Confidence/OutputStatus 三策略。

技术方案 §9.6/§9.7/§9.8：候选基线为调整后价格的稳健汇总：
- 中心候选 = 相似度加权中位数；
- 初始范围候选 = 加权分位范围；
- 权重集中检查 = 有效样本量（Kish），个别案例是否主导中心/范围（异常检查）；
- 可信度四级（高/中/低/不足）+ 分项证据 + 单调性（数据更差不得使范围更窄或可信度更高）；
- 输出状态（候选/参考/正式，数据字典 §3.13）：正式发布门槛（G5）通过前只输出参考。

本模块只产出 ``valuation_result`` 表（不可覆盖，数据字典 §3.13），不改写
raw/staged/marts/entities/comp_candidate/comp_adjustment 既有表；不做校准后
数值门槛与回放校准（归 WP8）。

中心构造可切换（add-aggregator-policy-experiment）：``AggregationPolicy`` 增加带
默认值的 ``aggregation_policy`` 字段，默认 ``c0_weighted_median`` 与现行为逐值等价
（V0.2 基线不动）；候选（截尾加权均值 / 加权分位 q=0.40 / median-of-means）只改变
中心值，区间构造与可信度合成不变；未登记策略标识以
``UnknownAggregationPolicyError`` 拒绝，不静默回退。

单调性保证：置信度从 HIGH 起步、各分项只做“封顶”降级（更差的数据只会导致
封顶更低/更宽的分位），因此数据更差绝不使范围更窄或可信度更高（验收①）。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import (
    ConfidenceLevel,
    OutputStatus,
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
)
from compsval.valuation.interval_calibration import (
    expand_interval,
    load_interval_calibration,
)
from compsval.valuation.time_adjustment import COMP_ADJUSTMENT_FILENAME

VALUATION_RESULT_TABLE = "valuation_result"
VALUATION_RESULT_FILENAME = f"{VALUATION_RESULT_TABLE}.parquet"

#: 可信度→加权分位（单调：可信度越低，区间越宽；不足不形成范围）。
QUANTILES_BY_CONFIDENCE: Mapping[ConfidenceLevel, tuple[float, float]] = {
    ConfidenceLevel.HIGH: (0.25, 0.75),
    ConfidenceLevel.MEDIUM: (0.15, 0.85),
    ConfidenceLevel.LOW: (0.05, 0.95),
    ConfidenceLevel.INSUFFICIENT: (0.0, 1.0),
}

#: 权重主导阈值：单案例权重占比超过该值视为主导（可检查，异常）。
DOMINANCE_THRESHOLD = Decimal("0.50")
#: 相似度缺失时的中性权重（SAT/§7.3：未知不进数值加权，但需中性占位不归零）。
WEIGHT_FALLBACK = Decimal("0.5")

# ---------------------------------------------------------------------------
# 中心构造策略（add-aggregator-policy-experiment）：默认 = 现行为，只切中心
# ---------------------------------------------------------------------------

#: 默认策略 = 加权中位数 q=0.50（现行为，V0.2 基线不动）。
DEFAULT_AGGREGATION_POLICY = "c0_weighted_median"
#: 已登记策略清单（稳定 id；同时用作回放配置/明细列/产物目录名/报告行键）。
AGGREGATION_POLICIES: tuple[str, ...] = (
    DEFAULT_AGGREGATION_POLICY,
    "c1_trimmed_weighted_mean",
    "c2_weighted_quantile_p40",
    "c3_median_of_means",
)
#: C1 截尾比例：按权重从价值低端、高端各累积截除总权重的 20%。
TRIM_FRACTION = Decimal("0.20")


class UnknownAggregationPolicyError(Exception):
    """未登记的汇总策略标识（输入不合法；不静默回退到默认策略）。"""


def _center_weighted_median(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> tuple[Decimal, bool]:
    """C0（默认）：加权中位数 q=0.50，与现行为逐值等价。"""
    return weighted_quantile(values, weights, 0.50), False


def _center_weighted_quantile_p40(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> tuple[Decimal, bool]:
    """C2：加权分位 q=0.40（同族最小改动，无边界分支）。"""
    return weighted_quantile(values, weights, 0.40), False


def _positive_items(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> list[tuple[Decimal, Decimal]]:
    """按价值升序的 (value, weight>0) 列表（与 weighted_quantile 同口径）。"""
    return sorted((v, w) for v, w in zip(values, weights, strict=True) if w > 0)


def _weighted_mean(items: Sequence[tuple[Decimal, Decimal]]) -> Decimal:
    """加权平均（items 非空且权重和 > 0）。"""
    total = sum((w for _, w in items), Decimal("0"))
    return sum((v * w for v, w in items), Decimal("0")) / total


def _center_trimmed_weighted_mean(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> tuple[Decimal, bool]:
    """C1：截尾加权均值——两侧各累积截除 20% 总权重（整例，跨界例归截除侧）。

    剩余案例 < 2 时退化为全体加权平均并返回退化标记（确定性，留痕）。
    """
    items = _positive_items(values, weights)
    total = sum((w for _, w in items), Decimal("0"))
    target = total * TRIM_FRACTION
    remaining = list(items)
    removed = Decimal("0")
    while remaining and removed < target:
        removed += remaining.pop(0)[1]
    removed = Decimal("0")
    while remaining and removed < target:
        removed += remaining.pop()[1]
    if len(remaining) < 2:
        return _weighted_mean(items), True
    return _weighted_mean(remaining), False


def _center_median_of_means(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> tuple[Decimal, bool]:
    """C3：median-of-means——升序连续切 k 块，块内加权均值后取中位数。

    n < 4 时退化为 C0 行为并返回退化标记；k = max(2, min(5, n//3))，前
    n mod k 块各多分 1 例（确定性切分）。
    """
    items = _positive_items(values, weights)
    n = len(items)
    if n < 4:
        return weighted_quantile(values, weights, 0.50), True
    k = max(2, min(5, n // 3))
    base, extra = divmod(n, k)
    blocks: list[list[tuple[Decimal, Decimal]]] = []
    start = 0
    for i in range(k):
        size = base + (1 if i < extra else 0)
        blocks.append(items[start : start + size])
        start += size
    means = [_weighted_mean(block) for block in blocks]
    return _decimal_median(means), False


#: 策略注册表：策略 id → 确定性中心构造（返回 (中心, 退化标记)）。
_CENTER_POLICIES: Mapping[
    str, Callable[[Sequence[Decimal], Sequence[Decimal]], tuple[Decimal, bool]]
] = {
    DEFAULT_AGGREGATION_POLICY: _center_weighted_median,
    "c1_trimmed_weighted_mean": _center_trimmed_weighted_mean,
    "c2_weighted_quantile_p40": _center_weighted_quantile_p40,
    "c3_median_of_means": _center_median_of_means,
}


def compute_center(
    values: Sequence[Decimal], weights: Sequence[Decimal], policy: str
) -> tuple[Decimal, bool]:
    """按策略标识计算中心值；未登记策略 → ``UnknownAggregationPolicyError``。"""
    if policy not in _CENTER_POLICIES:
        raise UnknownAggregationPolicyError(
            f"未登记的汇总策略：{policy!r}（合法策略：{', '.join(AGGREGATION_POLICIES)}）"
        )
    return _CENTER_POLICIES[policy](values, weights)

#: 置信度分项封顶阈值（数据更差→更低封顶 → 单调）。
MIN_EFFECTIVE_MEDIUM = 5
MIN_EFFECTIVE_LOW = 4
MEDIAN_SIMILARITY_MEDIUM = Decimal("0.50")
MEDIAN_SIMILARITY_LOW = Decimal("0.35")
OLDEST_DAYS_MEDIUM = 730
OLDEST_DAYS_LOW = 1095
DISPERSION_CV_MEDIUM = Decimal("0.30")
DISPERSION_CV_LOW = Decimal("0.50")
SUBJECT_MISSING_MEDIUM = 3
SUBJECT_MISSING_LOW = 5
NUMERIC_ADJ_FRAC_MEDIUM = Decimal("0.50")
MIN_SAMPLES = 3
MIN_EFFECTIVE_SAMPLES = 3


# ---------------------------------------------------------------------------
# 原因（每行可溯源，验收④；§3.13 状态原因/补数要求）
# ---------------------------------------------------------------------------

REASON_INSUFFICIENT = (
    "有效可比案例不足（最少{n_min}例/有效样本{e_min}），无法形成可靠范围；"
    "补数要求：补充目标小区近期成交或匹配community_id的成交"
)
REASON_NOT_APPLICABLE = "地域或房产类型不适用，不输出估值结果"
REASON_FORMAL = "正式发布门槛（G0-G5，§14）通过，输出正式估值范围"
REASON_REFERENCE = (
    "正式发布门槛未通过；单次数据可形成范围，但仅输出参考，不得自宣正式"
)
REASON_FORMAL_SCOPE_EXCLUDED = (
    "正式发布开关已开启，但目标小区不在适用范围（ScopePolicy 纳入名单）内；"
    "仅输出参考，不输出正式（README §3.3/§3.2 范围边界）"
)
REASON_NO_COMPS = "无入选可比案例，无法进行汇总（拒绝虚构估值）"

#: 目标房源完整度检查的字段清单（缺失→低估可信度）。
_SUBJECT_ORIENTATION_UNKNOWN = {"UNKNOWN", "None", ""}


class NotApplicable(StrEnum):
    """估值不适用类型（§9.8 四态之 not_applicable）。"""

    REGION = "地域"
    PROPERTY_TYPE = "房产类型"


# ---------------------------------------------------------------------------
# 统计辅助（加权分位 / 有效样本量 / 离散度）
# ---------------------------------------------------------------------------


def _weight_of(similarity: Decimal | None) -> Decimal:
    """可比相似度 → 权重：未知用中性占位，不归零（否则有效样本量失真为0）。"""
    if similarity is None:
        return WEIGHT_FALLBACK
    return similarity


def weighted_quantile(
    values: Sequence[Decimal], weights: Sequence[Decimal], q: float
) -> Decimal:
    """加权分位数（线性插值，q∈[0,1]）。values 须与 weights 同长且非空。"""
    if not values or len(values) != len(weights):
        raise ValueError("weighted_quantile: values 与 weights 需等长且非空")
    items = sorted(
        (v, w) for v, w in zip(values, weights, strict=True) if w > 0
    )
    if not items:
        raise ValueError("weighted_quantile: 所有权重为 0")
    total = sum((w for _, w in items), Decimal("0"))
    target = total * Decimal(str(q))
    acc = Decimal("0")
    for i, (v, w) in enumerate(items):
        prev = acc
        acc += w
        if acc >= target:
            if prev < target and i > 0:
                lo = items[i - 1][0]
                frac = (target - prev) / w
                return lo + frac * (v - lo)
            return v
    return items[-1][0]


def effective_sample_size(weights: Sequence[Decimal]) -> float:
    """Kish 有效样本量：(Σw)² / Σw²；用于权重集中检查（§9.6）。"""
    total = sum((w for w in weights if w > 0), Decimal("0"))
    if total <= 0:
        return 0.0
    sq = sum((w * w for w in weights if w > 0), Decimal("0"))
    if sq <= 0:
        return 0.0
    return float((total * total) / sq)


def coefficient_of_variation(
    values: Sequence[Decimal], weights: Sequence[Decimal]
) -> float | None:
    """加权变异系数（离散度，§9.7）；样本不足 2 或均值无效时返回 None。"""
    if len(values) < 2:
        return None
    total = sum((w for w in weights if w > 0), Decimal("0"))
    if total <= 0:
        return None
    mean = sum(
        (v * w for v, w in zip(values, weights, strict=True) if w > 0),
        Decimal("0"),
    ) / total
    if mean <= 0:
        return None
    var = sum(
        ((v - mean) * (v - mean) * w for v, w in zip(values, weights, strict=True) if w > 0),
        Decimal("0"),
    ) / total
    return float(math.sqrt(float(var)) / float(mean))


# ---------------------------------------------------------------------------
# 数据对象
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdjustableComp:
    """一个可比案例的调整后口径（供加权汇总与可信度分项评估）。"""

    candidate_id: str
    sale_event_id: str
    unit_price: Decimal
    adjusted_unit_price: Decimal
    similarity: Decimal | None
    days_gap: int | None  # 估值时点 - 成交日（天）；未知=None
    has_numeric_adjustment: bool  # 该案例是否存在数值调整证据（非纯降级）


@dataclass(frozen=True)
class WeightedStats:
    """稳健汇总结果（§9.6）：中心 + 范围 + 权重/异常检查。"""

    center: Decimal
    lower: Decimal | None
    upper: Decimal | None
    n: int
    effective_samples: float
    max_weight_share: Decimal
    dominant_candidate: str | None  # 权重主导案例（异常检查；无则 None）
    dispersion_cv: float | None  # 调整后价格离散度（§9.7 分项）
    values: tuple[Decimal, ...]
    weights: tuple[Decimal, ...]
    policy: str = DEFAULT_AGGREGATION_POLICY  # 中心构造策略标识
    center_fallback: bool = False  # 中心构造触发边界退化（确定性留痕）


@dataclass(frozen=True)
class AggregationPolicy:
    """稳健汇总（VAL1-006 §9.6）：加权中位数中心 + 加权分位范围 + 质量检查。

    ``aggregation_policy``（add-aggregator-policy-experiment）只切换中心构造；
    默认 ``c0_weighted_median`` 与现行为逐值等价，未登记标识在构造时即拒绝。
    """

    rule_version: str = DEFAULT_RULE_VERSION
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY

    def __post_init__(self) -> None:
        if self.aggregation_policy not in _CENTER_POLICIES:
            raise UnknownAggregationPolicyError(
                f"未登记的汇总策略：{self.aggregation_policy!r}"
                f"（合法策略：{', '.join(AGGREGATION_POLICIES)}）"
            )

    def compute_stats(
        self,
        comps: Sequence[AdjustableComp],
        confidence: ConfidenceLevel,
    ) -> WeightedStats:
        """由可比案例计算汇总统计；范围宽度随置信度（条件于单调性）。

        ``confidence`` 由 ConfidencePolicy 先行确定（不等式门槛），此处据其挑选
        加权分位宽度：置信度越高→区间越窄；不足（INSUFFICIENT）→不以数值形成范围
        （lower/upper=None，安全降级）。中心由 ``aggregation_policy`` 决定（默认
        = 现行为）；区间构造与可信度输入不随策略变化。
        """
        if not comps:
            raise ValueError("无可比案例，无法汇总")
        values = [c.adjusted_unit_price for c in comps]
        weights = [_weight_of(c.similarity) for c in comps]

        center, fallback = compute_center(values, weights, self.aggregation_policy)
        effective = effective_sample_size(weights)
        total_w = sum(weights)
        max_w = max(weights)
        max_share = max_w / total_w if total_w > 0 else Decimal("1.0")
        top_idx = max(range(len(weights)), key=lambda i: weights[i])
        dominant = comps[top_idx].candidate_id if max_share > DOMINANCE_THRESHOLD else None
        cv = coefficient_of_variation(values, weights)

        if confidence is ConfidenceLevel.INSUFFICIENT:
            lower = upper = None
        else:
            q_low, q_high = QUANTILES_BY_CONFIDENCE[confidence]
            lower = weighted_quantile(values, weights, q_low)
            upper = weighted_quantile(values, weights, q_high)

        return WeightedStats(
            center=center,
            lower=lower,
            upper=upper,
            n=len(comps),
            effective_samples=effective,
            max_weight_share=max_share,
            dominant_candidate=dominant,
            dispersion_cv=cv,
            values=tuple(values),
            weights=tuple(weights),
            policy=self.aggregation_policy,
            center_fallback=fallback,
        )


# ---------------------------------------------------------------------------
# 可信度四级 + 分项证据（§9.7）
# ---------------------------------------------------------------------------


class ConfidencePolicy:
    """可信度四级合成（§9.7）：HIGH 起步，各分项只做封顶降级 → 单调。

    分项（更差只降不升，保证单调性）：来源口径 / 有效样本量与权重集中 /
    相似度 / 时间新旧 / 调整证据强度 / 目标完整度 / 调整后离散度 / 回放表现。
    结果仅四级，不输出未经校准的分数/概率（§9.7）。
    """

    def __init__(
        self,
        *,
        min_samples: int = MIN_SAMPLES,
        min_effective_samples: int = MIN_EFFECTIVE_SAMPLES,
        source_ok: bool = True,
    ) -> None:
        self.min_samples = min_samples
        self.min_effective_samples = min_effective_samples
        self.source_ok = source_ok

    def evaluate(
        self,
        *,
        comps: Sequence[AdjustableComp],
        subject: SubjectProperty,
        stats: WeightedStats,
    ) -> tuple[ConfidenceLevel, dict[str, str]]:
        """合成可信度 + 分项证据（每项注明封顶理由，验收①单调）。"""
        evidence: dict[str, str] = {}

        # 不足（不形成可靠范围）：样本量 / 有效样本量不足
        if stats.n < self.min_samples or stats.effective_samples < self.min_effective_samples:
            evidence["案例数量与权重集中度"] = (
                f"可比 {stats.n} 例 / 有效样本 {stats.effective_samples:.1f} "
                f"< 下限（{self.min_samples}例/{self.min_effective_samples}），不足"
            )
            evidence["来源与价格口径"] = self._source_evidence()
            evidence["历史回放表现"] = "第一阶段无回放数据，不据此调整"
            return ConfidenceLevel.INSUFFICIENT, evidence

        level = ConfidenceLevel.HIGH  # 起始；以下只降不升
        level = self._cap(level, ConfidenceLevel.MEDIUM)

        # 1) 来源与价格口径
        if not self.source_ok:
            level = self._cap(level, ConfidenceLevel.LOW)
            evidence["来源与价格口径"] = "来源/价格口径不明，封顶低"
        else:
            evidence["来源与价格口径"] = "链家平台逐套成交（实际登记口径），充足"

        # 2) 案例数量与权重集中度
        if stats.effective_samples < self.min_effective_samples:
            level = self._cap(level, ConfidenceLevel.LOW)
            evidence["案例数量与权重集中度"] = (
                f"有效样本 {stats.effective_samples:.1f} < {self.min_effective_samples}，封顶低"
            )
        elif stats.effective_samples < MIN_EFFECTIVE_MEDIUM:
            level = self._cap(level, ConfidenceLevel.MEDIUM)
            evidence["案例数量与权重集中度"] = (
                f"有效样本 {stats.effective_samples:.1f} < {MIN_EFFECTIVE_MEDIUM}，封顶中"
            )
        elif stats.dominant_candidate is not None:
            level = self._cap(level, ConfidenceLevel.LOW)
            evidence["案例数量与权重集中度"] = (
                f"单案例权重占比 {float(stats.max_weight_share):.2f} 主导，封顶低"
            )
        else:
            evidence["案例数量与权重集中度"] = (
                f"可比 {stats.n} 例 / 有效样本 {stats.effective_samples:.1f}，充足"
            )

        # 3) 案例相似程度
        sims = [c.similarity for c in comps if c.similarity is not None]
        if not sims:
            level = self._cap(level, ConfidenceLevel.MEDIUM)
            evidence["案例相似程度"] = "可比相似度缺失，封顶中"
        else:
            med_sim = _decimal_median(sims)
            if med_sim < MEDIAN_SIMILARITY_LOW:
                level = self._cap(level, ConfidenceLevel.LOW)
                evidence["案例相似程度"] = f"可比相似度中位数 {med_sim} 过低，封顶低"
            elif med_sim < MEDIAN_SIMILARITY_MEDIUM:
                level = self._cap(level, ConfidenceLevel.MEDIUM)
                evidence["案例相似程度"] = f"可比相似度中位数 {med_sim} 一般，封顶中"
            else:
                evidence["案例相似程度"] = f"可比相似度中位数 {med_sim}，充足"

        # 4) 时间新旧
        gaps = [c.days_gap for c in comps if c.days_gap is not None]
        if not gaps:
            evidence["时间新旧"] = "成交时间未知（可比已排除缺失，理论分支）"
        else:
            med_gap = _int_median(gaps)
            if med_gap > OLDEST_DAYS_LOW:
                level = self._cap(level, ConfidenceLevel.LOW)
                evidence["时间新旧"] = f"可比与估值时点间隔中位数 {med_gap} 天过久，封顶低"
            elif med_gap > OLDEST_DAYS_MEDIUM:
                level = self._cap(level, ConfidenceLevel.MEDIUM)
                evidence["时间新旧"] = f"可比与估值时点间隔中位数 {med_gap} 天偏久，封顶中"
            else:
                evidence["时间新旧"] = f"可比与估值时点间隔中位数 {med_gap} 天，新"

        # 5) 调整证据强度
        numeric_frac = sum(1 for c in comps if c.has_numeric_adjustment) / len(comps)
        frac = Decimal(str(numeric_frac))
        if frac == Decimal("0"):
            level = self._cap(level, ConfidenceLevel.LOW)
            evidence["调整证据强度"] = "全部可比无数值调整证据（均降级），封顶低"
        elif frac < NUMERIC_ADJ_FRAC_MEDIUM:
            level = self._cap(level, ConfidenceLevel.MEDIUM)
            evidence["调整证据强度"] = (
                f"仅有 {frac} 比例可比有数值调整证据，封顶中"
            )
        else:
            evidence["调整证据强度"] = f"{frac} 比例可比有数值调整证据，充足"

        # 6) 目标房源完整度
        missing = _subject_missing_count(subject)
        if missing >= SUBJECT_MISSING_LOW:
            level = self._cap(level, ConfidenceLevel.LOW)
            evidence["目标房源完整度"] = f"目标关键属性缺失 {missing} 项，范围扩大且封顶低"
        elif missing >= SUBJECT_MISSING_MEDIUM:
            level = self._cap(level, ConfidenceLevel.MEDIUM)
            evidence["目标房源完整度"] = f"目标关键属性缺失 {missing} 项，封顶中"
        else:
            evidence["目标房源完整度"] = f"目标关键属性缺失 {missing} 项，充足"

        # 7) 调整后价格离散度
        if stats.dispersion_cv is not None:
            cv = Decimal(str(stats.dispersion_cv))
            if cv > DISPERSION_CV_LOW:
                level = self._cap(level, ConfidenceLevel.LOW)
                evidence["调整后价格离散度"] = f"调整后价格变异系数 {cv:.2f} 过高，封顶低"
            elif cv > DISPERSION_CV_MEDIUM:
                level = self._cap(level, ConfidenceLevel.MEDIUM)
                evidence["调整后价格离散度"] = f"调整后价格变异系数 {cv:.2f} 偏高，封顶中"
            else:
                evidence["调整后价格离散度"] = f"调整后价格变异系数 {cv:.2f}，低离散"
        else:
            evidence["调整后价格离散度"] = "样本不足无法测离散度"

        # 8) 回放表现（第一阶段无可回放数据 → 不据此调整，封顶中）
        level = self._cap(level, ConfidenceLevel.MEDIUM)
        evidence["历史回放表现"] = "第一阶段无历史回放(BT-001)数据，封顶中"

        return level, evidence

    def _source_evidence(self) -> str:
        return "来源/价格口径不明，不足"

    @staticmethod
    def _cap(current: ConfidenceLevel, upper: ConfidenceLevel) -> ConfidenceLevel:
        """把当前等级封顶到一个上限：只降不升（单调）。"""
        if _ORDER[current] > _ORDER[upper]:
            return upper
        return current


_ORDER = {
    ConfidenceLevel.HIGH: 3,
    ConfidenceLevel.MEDIUM: 2,
    ConfidenceLevel.LOW: 1,
    ConfidenceLevel.INSUFFICIENT: 0,
}


def _decimal_median(values: Sequence[Decimal]) -> Decimal:
    """Decimal 序列中位数。"""
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / Decimal("2")


def _int_median(values: Sequence[int]) -> int:
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) // 2


def _subject_missing_count(subject: SubjectProperty) -> int:
    """目标房源关键属性缺失计数（完整度分项，§9.7）。"""
    count = 0
    for value in (subject.floor, subject.total_floors, subject.has_elevator, subject.year_built):
        if value is None:
            count += 1
    if subject.orientation in _SUBJECT_ORIENTATION_UNKNOWN:
        count += 1
    if (
        not subject.site_observations
        or subject.site_observations.strip() in _SUBJECT_ORIENTATION_UNKNOWN
    ):
        count += 1
    return count


# ---------------------------------------------------------------------------
# 输出状态（§9.8 / 数据字典 §3.13 三态）
# ---------------------------------------------------------------------------


class OutputStatusPolicy:
    """输出状态决策：候选/参考/正式（数据字典 §3.13）。

    ``formal`` 由发布状态控制（formal_release_enabled），不由单次运行自宣
    （§9.8 反例：发布门槛通过前，即使单次数据充分也只输出 reference）。
    """

    def __init__(self, formal_release_enabled: bool = False) -> None:
        self.formal_release_enabled = formal_release_enabled

    def decide(
        self,
        *,
        confidence: ConfidenceLevel,
        not_applicable: NotApplicable | None = None,
        in_formal_scope: bool = True,
    ) -> tuple[OutputStatus, str]:
        if not_applicable is not None:
            return (
                OutputStatus.CANDIDATE,
                f"{not_applicable.value}不适用：{REASON_NOT_APPLICABLE}",
            )
        if confidence is ConfidenceLevel.INSUFFICIENT:
            return (
                OutputStatus.CANDIDATE,
                REASON_INSUFFICIENT.format(
                    n_min=MIN_SAMPLES, e_min=MIN_EFFECTIVE_SAMPLES
                ),
            )
        if self.formal_release_enabled and in_formal_scope:
            return OutputStatus.FORMAL, REASON_FORMAL
        if self.formal_release_enabled and not in_formal_scope:
            # 发布开关已开但目标在适用范围外：仍不输出正式（范围双闸）
            return OutputStatus.REFERENCE, REASON_FORMAL_SCOPE_EXCLUDED
        return OutputStatus.REFERENCE, REASON_REFERENCE


# ---------------------------------------------------------------------------
# valuation_result 表模式与写盘（不可覆盖，数据字典 §3.13）
# ---------------------------------------------------------------------------


def valuation_result_schema() -> pa.Schema:
    """``valuation_result`` 中间结果 PyArrow 模式（§3.13）。"""
    return pa.schema(
        [
            pa.field("result_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("subject_id", pa.string(), nullable=False),
            pa.field("center", pa.decimal128(14, 2), nullable=False),
            pa.field("range_lower", pa.decimal128(14, 2), nullable=True),
            pa.field("range_upper", pa.decimal128(14, 2), nullable=True),
            pa.field("confidence", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("valuation_date", pa.date32(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
            pa.field("evidence", pa.string(), nullable=False),
            pa.field("reason", pa.string(), nullable=False),
        ]
    )


def _dump_evidence(evidence: dict[str, str]) -> str:
    """分项证据 → JSON 字符串（落盘）；顺序稳定便于复现。"""
    return json.dumps(evidence, ensure_ascii=False, sort_keys=True)


def valuation_result_table(result: ValuationResultTabled) -> pa.Table:
    """估值结果 → §3.13 表。"""
    rows: dict[str, list[object]] = {name: [] for name in valuation_result_schema().names}
    rows["result_id"].append(result.result_id)
    rows["run_id"].append(result.run_id)
    rows["subject_id"].append(result.subject_id)
    rows["center"].append(result.center)
    rows["range_lower"].append(result.range_lower)
    rows["range_upper"].append(result.range_upper)
    rows["confidence"].append(result.confidence.value)
    rows["status"].append(result.status.value)
    rows["valuation_date"].append(result.valuation_date)
    rows["rule_version"].append(result.rule_version)
    rows["evidence"].append(result.evidence_json)
    rows["reason"].append(result.reason)
    return pa.table(rows, schema=valuation_result_schema())


@dataclass(frozen=True)
class ValuationResultTabled:
    """估值结果可落盘形态（中心/范围/可信度/状态 + 证据 JSON）。"""

    result_id: str
    run_id: str
    subject_id: str
    center: Decimal
    range_lower: Decimal | None
    range_upper: Decimal | None
    confidence: ConfidenceLevel
    status: OutputStatus
    valuation_date: date
    rule_version: str
    evidence_json: str
    reason: str


@dataclass(frozen=True)
class ValuationResultOutcome:
    """一次 ``compsval valuation aggregate`` 的结果（供 CLI 打印与测试断言）。"""

    result_path: Path
    result: ValuationResultTabled
    n_comps: int
    effective_samples: float
    # 中心构造诊断（add-aggregator-policy-experiment；默认值 = 现行为等价）
    policy: str = DEFAULT_AGGREGATION_POLICY
    max_weight_share: Decimal | None = None
    dominant_flag: bool = False
    center_fallback: bool = False


# ---------------------------------------------------------------------------
# 主入口：读 comp_candidate + comp_adjustment → 聚合 → 写 valuation_result
# ---------------------------------------------------------------------------


def _adjustment_multiplier(
    adjustment: pa.Table, candidate_id: str
) -> tuple[Decimal, bool]:
    """该可比的全部数值调整之积（乘法口径）；无数值调整→1.0。

    时间行（adjustment_type=时间）数值在 ``amount``，差异行数值在
    ``factor``；统一取“factor 优先，否则 amount”，把非量化(None)行排除
    （不得 0 乘导致价格为 0，§7.3）。返回 (multiplier, has_numeric)。
    """
    multiplier = Decimal("1.0")
    has_numeric = False
    for row in adjustment.to_pylist():
        if row.get("candidate_id") != candidate_id:
            continue
        value = row.get("factor")
        if value is None:
            value = row.get("amount")
        if value is None:
            continue
        try:
            factor = Decimal(str(value))
        except (TypeError, ValueError, InvalidOperation):
            continue
        if factor <= 0:
            continue
        has_numeric = True
        multiplier *= factor
    return multiplier, has_numeric


def _unit_price_of(valid_sale: pa.Table, sale_event_id: str) -> Decimal | None:
    """从 valid_sale 溯源可比原单价。"""
    eids = valid_sale.column("sale_event_id").to_pylist()
    ups = valid_sale.column("unit_price").to_pylist()
    for eid, up in zip(eids, ups, strict=False):
        if str(eid) == sale_event_id:
            if up is None:
                return None
            try:
                value = Decimal(str(up))
            except (TypeError, ValueError, InvalidOperation):
                return None
            return value if value > 0 else None
    return None


def _sale_date_of(valid_sale: pa.Table, sale_event_id: str) -> date | None:
    eids = valid_sale.column("sale_event_id").to_pylist()
    dates = valid_sale.column("sale_date").to_pylist()
    for eid, sd in zip(eids, dates, strict=False):
        if str(eid) == sale_event_id and isinstance(sd, date):
            return sd
    return None


def _build_comps(
    *,
    candidates: pa.Table,
    adjustment: pa.Table | None,
    valid_sale: pa.Table,
    valuation_date: date,
) -> tuple[list[AdjustableComp], str | None]:
    """把入选可比 → 调整后可比；返回 (comps, run_id)。"""
    comps: list[AdjustableComp] = []
    run_id: str | None = None
    for row in candidates.to_pylist():
        if not row.get("selected"):
            continue
        cid = str(row["candidate_id"])
        if run_id is None:
            run_id = str(row.get("run_id", ""))
        unit_price = _unit_price_of(valid_sale, str(row["sale_event_id"]))
        if unit_price is None:
            continue  # 缺原单价（应已被候选池排除，理论分支；不虚构）
        multiplier, has_numeric = (
            _adjustment_multiplier(adjustment, cid) if adjustment is not None else (
                Decimal("1.0"),
                False,
            )
        )
        sale_date = _sale_date_of(valid_sale, str(row["sale_event_id"]))
        days_gap = (valuation_date - sale_date).days if sale_date is not None else None
        similarity_raw = row.get("similarity")
        try:
            similarity = (
                Decimal(str(similarity_raw)) if similarity_raw is not None else None
            )
        except (TypeError, ValueError, InvalidOperation):
            similarity = None
        comps.append(
            AdjustableComp(
                candidate_id=cid,
                sale_event_id=str(row["sale_event_id"]),
                unit_price=unit_price,
                adjusted_unit_price=(unit_price * multiplier).quantize(Decimal("0.01")),
                similarity=similarity,
                days_gap=days_gap,
                has_numeric_adjustment=has_numeric,
            )
        )
    return comps, run_id


def apply_aggregation(
    *,
    data_dir: Path,
    subject: SubjectProperty,
    valid_sale: pa.Table,
    input_refs: Sequence[InputRef],
    rule_version: str = DEFAULT_RULE_VERSION,
    formal_release_enabled: bool = False,
    in_formal_scope: bool = True,
    not_applicable: NotApplicable | None = None,
    source_ok: bool = True,
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY,
) -> ValuationResultOutcome:
    """WP6-E 主入口：读 WP6-A/B 入选可比 + WP6-C/D 调整 → 聚合 → 写 valuation_result。

    - 读 ``data/valuation/comp_candidate.parquet`` 入选可比；
    - 读 ``comp_adjustment.parquet``（时间/差异）求各可比乘法调整系数；
    - 中心由 ``aggregation_policy`` 决定（默认 = 加权中位数，现行为）+ 加权分位
      范围（宽度随可信度，单调）；
      规则版本 1.1 起叠加时间外校准的分层宽度展开（interval_calibration，
      配置 ``<data_dir>/rules/``；1.0 保持旧行为，中心值一律不变）；
    - ConfidencePolicy 四级 + 分项证据；OutputStatusPolicy（正式受发布门禁）；
    - 原子写 valuation_result + DerivedManifest（不可覆盖）。
    """
    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    candidates = pq.read_table(candidate_path)
    adjustment_path = data_dir / VALUATION_LAYER / COMP_ADJUSTMENT_FILENAME
    adjustment = pq.read_table(adjustment_path) if adjustment_path.exists() else None

    calibration = load_interval_calibration(data_dir, rule_version)

    comps, run_id = _build_comps(
        candidates=candidates,
        adjustment=adjustment,
        valid_sale=valid_sale,
        valuation_date=subject.valuation_date,
    )
    if not comps:
        raise ValueError(REASON_NO_COMPS)

    run_id_out = run_id or f"RUN-{subject.subject_id}-{subject.valuation_date.isoformat()}"

    aggregation = AggregationPolicy(
        rule_version=rule_version, aggregation_policy=aggregation_policy
    )
    confidence_policy = ConfidencePolicy(source_ok=source_ok)
    status_policy = OutputStatusPolicy(
        formal_release_enabled=formal_release_enabled
    )

    # 先算结构性统计（含离散度），用于可信度；再按可信度确定宽度（单调）。
    draft = aggregation.compute_stats(comps, ConfidenceLevel.MEDIUM)
    level, evidence = confidence_policy.evaluate(
        comps=comps, subject=subject, stats=draft
    )
    final = aggregation.compute_stats(comps, level)
    lower, upper = final.lower, final.upper
    if calibration is not None and lower is not None and upper is not None:
        # 校准展开（v1.1）：只加宽半宽、不改中心；k/m 按层回退（单调）。
        expanded = expand_interval(
            final.center,
            lower,
            upper,
            calibration.params_for(subject.community_id, level.value),
        )
        if expanded is not None:
            lower, upper = expanded
    status, reason = status_policy.decide(
        confidence=level, not_applicable=not_applicable, in_formal_scope=in_formal_scope
    )

    result = ValuationResultTabled(
        result_id=f"{run_id_out}-RES",
        run_id=run_id_out,
        subject_id=subject.subject_id,
        center=final.center.quantize(Decimal("0.01")),
        range_lower=lower.quantize(Decimal("0.01")) if lower is not None else None,
        range_upper=upper.quantize(Decimal("0.01")) if upper is not None else None,
        confidence=level,
        status=status,
        valuation_date=subject.valuation_date,
        rule_version=rule_version,
        evidence_json=_dump_evidence(evidence),
        reason=reason,
    )

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    result_path = valuation_dir / VALUATION_RESULT_FILENAME
    work = valuation_dir / (VALUATION_RESULT_FILENAME + ".incomplete")
    pq.write_table(valuation_result_table(result), work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=VALUATION_RESULT_TABLE,
            built_at=datetime.now(UTC),
            row_count=1,
            inputs=list(input_refs),
            package_version=__version__,
            notes="WP6-E: 中心/区间/可信度/输出状态（相似度加权汇总）",
        ),
        result_path,
    )
    work.replace(result_path)

    return ValuationResultOutcome(
        result_path=result_path,
        result=result,
        n_comps=len(comps),
        effective_samples=final.effective_samples,
        policy=aggregation_policy,
        max_weight_share=final.max_weight_share,
        dominant_flag=final.dominant_candidate is not None,
        center_fallback=final.center_fallback,
    )