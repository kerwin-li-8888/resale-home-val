"""add-aggregator-policy-experiment：中心构造策略注册表 + 候选等价性/确定性/单调性。

对照 specs/valuation-aggregation-policy 验收：
- 默认策略与现行为逐值等价（合成；真实回放抽样对拍另证于
  04-校验/aggregator-experiment/evidence/）；
- 未登记策略标识拒绝（构造期 + compute_center + apply_aggregation），且不得是
  ValueError（防止 run_estimate 兜底误判为"信息不足"）；
- 每候选确定性 + 退化标记；区间/可信度输入跨候选不变；每候选单调性回归。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pyarrow.parquet as pq
import pytest

from compsval.contract.models import ConfidenceLevel
from compsval.ingest.manifests import InputRef
from compsval.valuation.aggregation import (
    AGGREGATION_POLICIES,
    DEFAULT_AGGREGATION_POLICY,
    AdjustableComp,
    AggregationPolicy,
    ConfidencePolicy,
    UnknownAggregationPolicyError,
    apply_aggregation,
    compute_center,
    weighted_quantile,
)

_ORDER = {ConfidenceLevel.HIGH: 3, ConfidenceLevel.MEDIUM: 2, ConfidenceLevel.LOW: 1}

_PRICES = [40000, 41000, 41500, 42000, 43000, 44000]


def _comp(
    candidate_id: str,
    price: int,
    *,
    similarity: Decimal | None = Decimal("0.9"),
    days_gap: int = 60,
) -> AdjustableComp:
    return AdjustableComp(
        candidate_id=candidate_id,
        sale_event_id=f"s-{candidate_id}",
        unit_price=Decimal(price),
        adjusted_unit_price=Decimal(price),
        similarity=similarity,
        days_gap=days_gap,
        has_numeric_adjustment=True,
    )


def _good_comps() -> list[AdjustableComp]:
    return [_comp(f"c{i}", p) for i, p in enumerate(_PRICES)]


def _degraded_comps() -> list[AdjustableComp]:
    return [
        AdjustableComp(
            candidate_id=f"d{i}",
            sale_event_id=f"s-d{i}",
            unit_price=Decimal(p),
            adjusted_unit_price=Decimal(p),
            similarity=Decimal("0.20"),
            days_gap=3000,
            has_numeric_adjustment=False,
        )
        for i, p in enumerate([40000, 50000, 60000])
    ]


# ---------------------------------------------------------------------------
# 未登记策略拒绝（不得静默回退；不得是 ValueError 子类）
# ---------------------------------------------------------------------------


def test_unknown_policy_rejected_at_construction() -> None:
    with pytest.raises(UnknownAggregationPolicyError):
        AggregationPolicy(aggregation_policy="bogus")


def test_unknown_policy_error_is_not_value_error() -> None:
    """非 ValueError：run_estimate 的聚合兜底不得把非法策略误判为信息不足。"""
    assert not issubclass(UnknownAggregationPolicyError, ValueError)


def test_compute_center_unknown_policy_rejected() -> None:
    with pytest.raises(UnknownAggregationPolicyError):
        compute_center([Decimal("1")], [Decimal("0.5")], "nope")


def test_apply_aggregation_unknown_policy_rejected(tmp_path: Any) -> None:
    from tests.test_aggregation import _subject, _write_valuation_inputs

    data_dir = _write_valuation_inputs(tmp_path)
    with pytest.raises(UnknownAggregationPolicyError):
        apply_aggregation(
            data_dir=data_dir,
            subject=_subject(),
            valid_sale=pq.read_table(data_dir / "marts" / "valid_sale.parquet"),
            input_refs=[InputRef(dataset="chengjiao", fetched_at="20260821")],
            aggregation_policy="bogus",
        )


# ---------------------------------------------------------------------------
# 每候选：确定性 + 退化标记 + 数值手算对拍
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", AGGREGATION_POLICIES)
def test_policies_deterministic(policy: str) -> None:
    comps = _good_comps()
    first = AggregationPolicy(aggregation_policy=policy).compute_stats(
        comps, ConfidenceLevel.MEDIUM
    )
    second = AggregationPolicy(aggregation_policy=policy).compute_stats(
        comps, ConfidenceLevel.MEDIUM
    )
    assert first.center == second.center
    assert first.center_fallback == second.center_fallback
    assert first.policy == policy


def test_c0_default_matches_weighted_median() -> None:
    values = [Decimal(p) for p in _PRICES]
    weights = [Decimal("0.9")] * len(_PRICES)
    stats = AggregationPolicy().compute_stats(_good_comps(), ConfidenceLevel.MEDIUM)
    assert stats.center == weighted_quantile(values, weights, 0.50)
    assert stats.center == Decimal("41500")
    assert stats.center_fallback is False
    assert stats.policy == DEFAULT_AGGREGATION_POLICY


def test_c2_q40_hand_computed() -> None:
    stats = AggregationPolicy(aggregation_policy="c2_weighted_quantile_p40").compute_stats(
        _good_comps(), ConfidenceLevel.MEDIUM
    )
    assert stats.center == Decimal("41200")
    assert stats.center_fallback is False


def test_c1_trim_hand_computed() -> None:
    """等权 6 例：两侧各截 2 例 → 剩余 [41500, 42000] 加权均值 = 41750。"""
    stats = AggregationPolicy(aggregation_policy="c1_trimmed_weighted_mean").compute_stats(
        _good_comps(), ConfidenceLevel.MEDIUM
    )
    assert stats.center == Decimal("41750")
    assert stats.center_fallback is False


def test_c1_small_sample_fallback_flagged() -> None:
    """等权 3 例：两侧各截后剩 1 例 < 2 → 退化为全体加权均值并留退化标记。"""
    comps = [_comp(f"t{i}", p) for i, p in enumerate([40000, 50000, 60000])]
    stats = AggregationPolicy(aggregation_policy="c1_trimmed_weighted_mean").compute_stats(
        comps, ConfidenceLevel.MEDIUM
    )
    assert stats.center == Decimal("50000")
    assert stats.center_fallback is True


def test_c3_blocks_hand_computed() -> None:
    """等权 6 例：k=2 块 → 块均值 40833.33/43000 → 中位数 41916.67。"""
    stats = AggregationPolicy(aggregation_policy="c3_median_of_means").compute_stats(
        _good_comps(), ConfidenceLevel.MEDIUM
    )
    assert abs(float(stats.center) - 41916.6666667) < 1e-4
    assert stats.center_fallback is False


def test_c3_small_sample_falls_back_to_c0_flagged() -> None:
    comps = [_comp(f"t{i}", p) for i, p in enumerate([40000, 50000, 60000])]
    values = [Decimal(p) for p in [40000, 50000, 60000]]
    weights = [Decimal("0.9")] * 3
    stats = AggregationPolicy(aggregation_policy="c3_median_of_means").compute_stats(
        comps, ConfidenceLevel.MEDIUM
    )
    assert stats.center == weighted_quantile(values, weights, 0.50)
    assert stats.center_fallback is True


# ---------------------------------------------------------------------------
# 区间/可信度输入跨候选不变（只有中心变）；默认策略逐值等价
# ---------------------------------------------------------------------------


def test_interval_and_quality_invariant_across_policies() -> None:
    comps = _good_comps()
    ref = AggregationPolicy().compute_stats(comps, ConfidenceLevel.MEDIUM)
    for policy in AGGREGATION_POLICIES:
        stats = AggregationPolicy(aggregation_policy=policy).compute_stats(
            comps, ConfidenceLevel.MEDIUM
        )
        assert stats.lower == ref.lower
        assert stats.upper == ref.upper
        assert stats.effective_samples == ref.effective_samples
        assert stats.max_weight_share == ref.max_weight_share
        assert stats.dispersion_cv == ref.dispersion_cv


def test_centers_differ_across_candidates_on_skewed_sample() -> None:
    comps = _good_comps()
    centers = {
        policy: AggregationPolicy(aggregation_policy=policy).compute_stats(
            comps, ConfidenceLevel.MEDIUM
        ).center
        for policy in AGGREGATION_POLICIES
    }
    assert len(set(centers.values())) == len(AGGREGATION_POLICIES)


def test_default_policy_explicit_equals_implicit() -> None:
    comps = _good_comps()
    implicit = AggregationPolicy().compute_stats(comps, ConfidenceLevel.MEDIUM)
    explicit = AggregationPolicy(
        rule_version="1.0", aggregation_policy=DEFAULT_AGGREGATION_POLICY
    ).compute_stats(comps, ConfidenceLevel.MEDIUM)
    assert implicit.center == explicit.center
    assert implicit.lower == explicit.lower
    assert implicit.upper == explicit.upper


# ---------------------------------------------------------------------------
# 每候选单调性回归（数据更差→区间不更窄/可信度不更高）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", AGGREGATION_POLICIES)
def test_monotonic_confidence_and_range_per_policy(policy: str) -> None:
    agg = AggregationPolicy(aggregation_policy=policy)
    good = agg.compute_stats(_good_comps(), ConfidenceLevel.MEDIUM)
    degraded = agg.compute_stats(_degraded_comps(), ConfidenceLevel.MEDIUM)
    conf = ConfidencePolicy()
    good_level, _ = conf.evaluate(
        comps=_good_comps(), subject=_subject_any(), stats=good
    )
    degraded_level, _ = conf.evaluate(
        comps=_degraded_comps(), subject=_subject_any(), stats=degraded
    )
    assert _ORDER[degraded_level] <= _ORDER[good_level]

    good_at_level = agg.compute_stats(_good_comps(), good_level)
    degraded_at_level = agg.compute_stats(_degraded_comps(), degraded_level)
    if degraded_at_level.lower is not None and degraded_at_level.upper is not None:
        assert good_at_level.lower is not None and good_at_level.upper is not None
        width_good = good_at_level.upper - good_at_level.lower
        width_degraded = degraded_at_level.upper - degraded_at_level.lower
        assert width_degraded >= width_good


def _subject_any() -> Any:
    from tests.test_aggregation import _subject

    return _subject()
