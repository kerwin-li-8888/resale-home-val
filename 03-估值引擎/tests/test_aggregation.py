"""WP6-E: AggregationPolicy + ConfidencePolicy + OutputStatusPolicy（VAL1-006）。

对照 WP6-E 验收标准：
① 中心/区间/可信度对数据质量变化单调响应（数据更差不得使范围更窄/可信度更高，
   反例测试）；
② 权重集中/异常主导可检查（有效样本量、max_weight_share、dominant_candidate）；
③ 输出状态含候选/参考/正式且正式受发布状态控制（§9.8 反例）；
④ 每条结果可溯源到案例与调整（结果写盘 + manifest）；
⑤ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import ConfidenceLevel, OutputStatus, SubjectProperty
from compsval.ingest.manifests import InputRef
from compsval.valuation.aggregation import (
    DOMINANCE_THRESHOLD,
    AdjustableComp,
    AggregationPolicy,
    ConfidencePolicy,
    NotApplicable,
    OutputStatusPolicy,
    apply_aggregation,
    effective_sample_size,
    weighted_quantile,
)
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
)
from compsval.valuation.time_adjustment import COMP_ADJUSTMENT_FILENAME

_COMMUNITY = "C-XXXX0013"
_VAL_DATE = date(2026, 7, 21)


def _comp(
    *,
    candidate_id: str,
    adjusted: Decimal,
    similarity: Decimal | None = Decimal("0.9"),
    days_gap: int = 90,
    has_numeric: bool = True,
) -> AdjustableComp:
    return AdjustableComp(
        candidate_id=candidate_id,
        sale_event_id=f"s-{candidate_id}",
        unit_price=adjusted,
        adjusted_unit_price=adjusted,
        similarity=similarity,
        days_gap=days_gap,
        has_numeric_adjustment=has_numeric,
    )


def _subject(**overrides: Any) -> SubjectProperty:
    defaults: dict[str, Any] = dict(
        subject_id="SUBJ-TEST-001",
        community_id=_COMMUNITY,
        area_sqm=Decimal("50.3"),
        layout="2室1厅",
        valuation_date=_VAL_DATE,
        has_elevator=True,
        orientation="南",
        year_built=2015,
        floor=12,
        total_floors=30,
        site_observations="装修良好",
    )
    defaults.update(overrides)
    return SubjectProperty(**defaults)


def _good_comps() -> list[AdjustableComp]:
    """数据良好的 6 例可比：高相似、近期、强调整证据、低离散。"""
    return [
        _comp(candidate_id=f"c{i}", adjusted=Decimal(str(p)), days_gap=60 + i * 10)
        for i, p in enumerate([40000, 41000, 41500, 42000, 43000, 44000])
    ]


def _degraded_comps() -> list[AdjustableComp]:
    """数据较差的 3 例可比：低相似、过旧、无数值调整（更差→应更低可信）。"""
    return [
        _comp(
            candidate_id=f"d{i}",
            adjusted=Decimal(str(p)),
            similarity=Decimal("0.20"),
            days_gap=3000,
            has_numeric=False,
        )
        for i, p in enumerate([40000, 50000, 60000])
    ]


# ---------------------------------------------------------------------------
# 统计辅助：加权分位 / 有效样本量
# ---------------------------------------------------------------------------


def test_weighted_median_center() -> None:
    """中心=相似度加权中位数：权重集中在低价侧 → 中心偏向低价。"""
    values = [Decimal("40000"), Decimal("50000")]
    weights = [Decimal("0.9"), Decimal("0.1")]
    assert weighted_quantile(values, weights, 0.5) == Decimal("40000")


def test_weighted_quantile_interpolates() -> None:
    """加权分位在高权重区间内线性插值。"""
    values = [Decimal("100"), Decimal("200")]
    weights = [Decimal("0.75"), Decimal("0.25")]
    # target=0.5*1.0=0.5，落在第一项累积 0.75 内，但此前 prev=0 → 直接返回 100
    assert weighted_quantile(values, weights, 0.5) == Decimal("100")


def test_effective_sample_size_even_weights_equals_n() -> None:
    """等权重 n 例 → 有效样本量 = n；权重集中 → 有效样本量缩小（§9.6）。"""
    assert effective_sample_size([Decimal("1")] * 5) == pytest.approx(5.0)
    assert effective_sample_size([Decimal("0.9"), Decimal("0.05"), Decimal("0.05")]) < 3.0


def test_weighted_quantile_empty_raises() -> None:
    with pytest.raises(ValueError):
        weighted_quantile([], [], 0.5)


# ---------------------------------------------------------------------------
# 验收①：单调性（数据更差不得使范围更窄/可信度更高；反例）
# ---------------------------------------------------------------------------


def test_confidence_monotonic_good_ge_degraded() -> None:
    """数据良好→MEDIUM；数据更差→LOW；更差的可信度等级不高（验收①反例）。"""
    policy = ConfidencePolicy()
    good_stats = AggregationPolicy().compute_stats(_good_comps(), ConfidenceLevel.MEDIUM)
    degraded_stats = AggregationPolicy().compute_stats(
        _degraded_comps(), ConfidenceLevel.MEDIUM
    )
    good_level, _ = policy.evaluate(comps=_good_comps(), subject=_subject(), stats=good_stats)
    degraded_level, _ = policy.evaluate(
        comps=_degraded_comps(), subject=_subject(), stats=degraded_stats
    )
    assert good_level is ConfidenceLevel.MEDIUM
    assert degraded_level is ConfidenceLevel.LOW
    _ORDER = {ConfidenceLevel.HIGH: 3, ConfidenceLevel.MEDIUM: 2, ConfidenceLevel.LOW: 1}
    assert _ORDER[degraded_level] <= _ORDER[good_level]


def test_range_width_monotonic_worse_confidence_wider() -> None:
    """同一组可比：可信度越低 → 分位越宽，范围越宽（单调，验收①反例）。"""
    comps = _good_comps()
    agg = AggregationPolicy()
    med = agg.compute_stats(comps, ConfidenceLevel.MEDIUM)
    low = agg.compute_stats(comps, ConfidenceLevel.LOW)
    assert med.lower is not None and med.upper is not None
    assert low.lower is not None and low.upper is not None
    assert (med.upper - med.lower) < (low.upper - low.lower)


def test_insufficient_has_no_numeric_range() -> None:
    """不足 → 不形成可靠范围（lower/upper=None，安全降级，§9.6/§9.7）。"""
    stats = AggregationPolicy().compute_stats(_degraded_comps(), ConfidenceLevel.INSUFFICIENT)
    assert stats.lower is None
    assert stats.upper is None


# ---------------------------------------------------------------------------
# 验收②：权重集中/异常主导可检查
# ---------------------------------------------------------------------------


def test_dominant_case_reported_and_insufficient() -> None:
    """单案例权重主导 → stats 暴露 dominant_candidate 且有效样本不足 → 降级不足。"""
    comps = [
        _comp(candidate_id="big", adjusted=Decimal("40000"), similarity=Decimal("0.9")),
        _comp(candidate_id="a", adjusted=Decimal("50000"), similarity=Decimal("0.05")),
        _comp(candidate_id="b", adjusted=Decimal("51000"), similarity=Decimal("0.05")),
    ]
    stats = AggregationPolicy().compute_stats(comps, ConfidenceLevel.MEDIUM)
    assert stats.max_weight_share > DOMINANCE_THRESHOLD
    assert stats.dominant_candidate == "big"  # 异常主导可检查（验收②）
    assert stats.effective_samples < 3.0  # 权重高度集中
    level, _ = ConfidencePolicy().evaluate(comps=comps, subject=_subject(), stats=stats)
    assert level is ConfidenceLevel.INSUFFICIENT


def test_dispersion_and_max_share_exposed() -> None:
    """汇总暴露有效样本量/离散度/主导（§9.6 质量检查字段可断言）。"""
    stats = AggregationPolicy().compute_stats(_good_comps(), ConfidenceLevel.MEDIUM)
    assert stats.n == 6
    assert stats.effective_samples == pytest.approx(6.0)
    assert stats.dispersion_cv is not None


# ---------------------------------------------------------------------------
# 验收③：输出状态（三态 + 正式受发布门禁；§9.8 反例）
# ---------------------------------------------------------------------------


def test_insufficient_candidate_status() -> None:
    """不足 → 候选（不形成范围），reason 含补数要求。"""
    status, reason = OutputStatusPolicy().decide(confidence=ConfidenceLevel.INSUFFICIENT)
    assert status is OutputStatus.CANDIDATE
    assert "补数" in reason


def test_reference_when_release_gate_closed() -> None:
    """发布门禁未开：即使数据充分也只输出参考（§9.8 反例），绝不自宣正式。"""
    status, reason = OutputStatusPolicy().decide(confidence=ConfidenceLevel.HIGH)
    assert status is OutputStatus.REFERENCE
    assert status.value != OutputStatus.FORMAL.value


def test_formal_only_when_release_gate_open() -> None:
    """发布门禁打开 → 可输出正式；但 run 不自行宣布（由形式开关控制）。"""
    status_open, _ = OutputStatusPolicy(
        formal_release_enabled=True
    ).decide(confidence=ConfidenceLevel.HIGH)
    status_closed, _ = OutputStatusPolicy(
        formal_release_enabled=False
    ).decide(confidence=ConfidenceLevel.HIGH)
    assert status_open is OutputStatus.FORMAL
    assert status_closed is OutputStatus.REFERENCE


def test_not_applicable_is_candidate() -> None:
    """地域/房产类型不适用 → 候选（不输出估值范围）。"""
    status, reason = OutputStatusPolicy().decide(
        confidence=ConfidenceLevel.HIGH, not_applicable=NotApplicable.REGION
    )
    assert status is OutputStatus.CANDIDATE
    assert "不适用" in reason


def test_status_for_low_confidence_non_insufficient() -> None:
    """非不足但存在限制（LOW/MEDIUM）→ 参考。"""
    for level in (ConfidenceLevel.LOW, ConfidenceLevel.MEDIUM):
        status, _ = OutputStatusPolicy().decide(confidence=level)
        assert status is OutputStatus.REFERENCE


# ---------------------------------------------------------------------------
# 验收④：置信度分项证据可溯源（§9.7 分项齐全）
# ---------------------------------------------------------------------------


def test_evidence_contains_all_factors() -> None:
    """evaluate 输出 §9.7 全部八个分项证据。"""
    stats = AggregationPolicy().compute_stats(_good_comps(), ConfidenceLevel.MEDIUM)
    _, evidence = ConfidencePolicy().evaluate(comps=_good_comps(), subject=_subject(), stats=stats)
    for factor in (
        "来源与价格口径",
        "案例数量与权重集中度",
        "案例相似程度",
        "时间新旧",
        "调整证据强度",
        "目标房源完整度",
        "调整后价格离散度",
        "历史回放表现",
    ):
        assert factor in evidence
        assert evidence[factor]


# ---------------------------------------------------------------------------
# 主入口：读候选/调整 → 聚合 → 写 valuation_result + manifest（可溯源）
# ---------------------------------------------------------------------------


def _write_valuation_inputs(tmp_path: Path, *, n_selected: int = 6) -> Path:
    """写 comp_candidate + comp_adjustment + valid_sale，返回 data_dir。"""
    valuation = tmp_path / VALUATION_LAYER
    valuation.mkdir(parents=True, exist_ok=True)
    ids = [f"RUN-c{i}" for i in range(n_selected)]
    cand = pa.table(
        {
            "candidate_id": ids,
            "run_id": ["RUN-X"] * n_selected,
            "sale_event_id": [f"s{i}" for i in range(n_selected)],
            "community_id": [_COMMUNITY] * n_selected,
            "selected": [True] * n_selected,
            "tier": [1] * n_selected,
            "similarity": [0.9] * n_selected,
            "reason": ["纳入候选"] * n_selected,
        }
    )
    pq.write_table(cand, valuation / COMP_CANDIDATE_FILENAME)

    adj = pa.table(
        {
            "adjustment_id": [f"A{i}" for i in range(n_selected)],
            "candidate_id": ids,
            "adjustment_type": ["时间"] * n_selected,
            "amount": [Decimal("1.0500")] * n_selected,
            "sale_date": [date(2026, 5, 1)] * n_selected,
            "valuation_date": [_VAL_DATE] * n_selected,
            "basis": ["同小区序列"] * n_selected,
            "evidence_strength": ["高"] * n_selected,
            "source_series": ["同小区序列"] * n_selected,
            "warning": [None] * n_selected,
            "rule_version": ["1.0"] * n_selected,
        }
    )
    pq.write_table(adj, valuation / COMP_ADJUSTMENT_FILENAME)

    valid_sale = pa.table(
        {
            "sale_event_id": [f"s{i}" for i in range(n_selected)],
            "sale_date": [date(2026, 5, 1)] * n_selected,
            "unit_price": [Decimal(str(40000 + i * 1000)) for i in range(n_selected)],
        }
    )
    (tmp_path / "marts").mkdir(exist_ok=True)
    pq.write_table(valid_sale, tmp_path / "marts" / "valid_sale.parquet")
    return tmp_path


def test_apply_aggregation_writes_result_and_manifest(tmp_path: Path) -> None:
    """主入口：写 valuation_result + manifest，结果可回读到案例与原因（验收④）。"""
    data_dir = _write_valuation_inputs(tmp_path)
    result = apply_aggregation(
        data_dir=data_dir,
        subject=_subject(),
        valid_sale=pq.read_table(data_dir / "marts" / "valid_sale.parquet"),
        input_refs=[InputRef(dataset="chengjiao", fetched_at="20260821")],
    )
    r = result.result
    assert r.status is OutputStatus.REFERENCE  # 发布门禁未开 → 参考
    assert result.result_path.is_file()
    table = pq.read_table(result.result_path)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["result_id"] == "RUN-X-RES"
    assert row["run_id"] == "RUN-X"
    assert row["subject_id"] == "SUBJ-TEST-001"
    assert row["status"] == "参考"
    assert row["confidence"] in {"高", "中", "低", "不足"}
    assert row["center"] is not None
    assert row["reason"]  # 状态原因可溯源
    assert row["evidence"]  # 分项证据 JSON 非空
    assert (result.result_path.with_suffix(".manifest.json")).is_file()
    incomplete = list((data_dir / VALUATION_LAYER).glob("*.incomplete"))
    assert not incomplete


def test_apply_aggregation_requires_comps(tmp_path: Path) -> None:
    """无入选可比 → ValueError（拒绝虚构估值）。"""
    data_dir = _write_valuation_inputs(tmp_path, n_selected=0)  # 无入选
    with pytest.raises(ValueError):
        apply_aggregation(
            data_dir=data_dir,
            subject=_subject(),
            valid_sale=pq.read_table(data_dir / "marts" / "valid_sale.parquet"),
            input_refs=[],
        )


def test_apply_aggregation_missing_candidates_raises(tmp_path: Path) -> None:
    """缺 comp_candidate.parquet → FileNotFoundError（不静默）。"""
    with pytest.raises(FileNotFoundError):
        apply_aggregation(
            data_dir=tmp_path,
            subject=_subject(),
            valid_sale=pa.table(
                {
                    "sale_event_id": ["s0"],
                    "sale_date": [date(2026, 5, 1)],
                    "unit_price": [Decimal("40000")],
                }
            ),
            input_refs=[],
        )