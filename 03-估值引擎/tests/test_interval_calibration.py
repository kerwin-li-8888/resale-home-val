"""区间校准（interval-calibration-g5）：展开函数/配置加载/集成/单调性/反例。

对照 specs delta（valuation-comparable-core）场景：
- 分层宽度配置单调性回归：数据弱度增加 → 区间相对宽度不收窄；
- 校准不改中心值：同 comps 同数据截点，1.0 vs 1.1 中心值相同；
- 配置缺失/损坏/缺字段 → 保守显式失败（不静默回退未校准宽度）；
- 层回退（小区 → 通配可信度 → 全局）与防过宽上限强校验。
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import ConfidenceLevel, SubjectProperty
from compsval.ingest.manifests import InputRef
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from compsval.valuation.aggregation import (
    AdjustableComp,
    AggregationPolicy,
    apply_aggregation,
)
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
)
from compsval.valuation.interval_calibration import (
    INTERVAL_CALIBRATION_RULE_VERSION,
    LEGACY_RULE_VERSION,
    ExpansionParams,
    calibration_config_path,
    expand_interval,
    load_interval_calibration,
)
from compsval.valuation.time_adjustment import COMP_ADJUSTMENT_FILENAME

_COMMUNITY = "C-XXXX0013"
_OTHER_COMMUNITY = "C-XXXX0079"
_VAL_DATE = date(2026, 7, 21)


def _comp(
    *, candidate_id: str, adjusted: Decimal, similarity: Decimal = Decimal("0.9")
) -> AdjustableComp:
    return AdjustableComp(
        candidate_id=candidate_id,
        sale_event_id=f"s-{candidate_id}",
        unit_price=adjusted,
        adjusted_unit_price=adjusted,
        similarity=similarity,
        days_gap=60,
        has_numeric_adjustment=True,
    )


def _good_comps() -> list[AdjustableComp]:
    return [
        _comp(candidate_id=f"c{i}", adjusted=Decimal(str(p)))
        for i, p in enumerate([40000, 41000, 41500, 42000, 43000, 44000])
    ]


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


def _config(*, layers: list[dict[str, Any]] | None = None, **overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "config_version": INTERVAL_CALIBRATION_RULE_VERSION,
        "method": "empirical_quantile_expansion",
        "split_point": "2026-05-31",
        "target_coverage": 0.85,
        "source_dataset_sha256": "a" * 64,
        "n_calibration_samples": 892,
        "min_layer_samples": 30,
        "built_at": "2026-08-31T00:00:00+00:00",
        "caps": {"k_max": 4.0, "m_max": 0.35},
        "layers": layers
        if layers is not None
        else [{"community_id": "*", "confidence": "*", "k": 1.8, "m": 0.08, "n": 892}],
    }
    config.update(overrides)
    return config


def _write_config(data_dir: Path, config: dict[str, Any]) -> Path:
    path = calibration_config_path(data_dir, INTERVAL_CALIBRATION_RULE_VERSION)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")
    return path


def _write_valuation_inputs(tmp_path: Path) -> Path:
    """写 comp_candidate + comp_adjustment + valid_sale（与 test_aggregation 同型）。"""
    valuation = tmp_path / VALUATION_LAYER
    valuation.mkdir(parents=True, exist_ok=True)
    n = 6
    ids = [f"RUN-c{i}" for i in range(n)]
    cand = pa.table(
        {
            "candidate_id": ids,
            "run_id": ["RUN-X"] * n,
            "sale_event_id": [f"s{i}" for i in range(n)],
            "community_id": [_COMMUNITY] * n,
            "selected": [True] * n,
            "tier": [1] * n,
            "similarity": [0.9] * n,
            "reason": ["纳入候选"] * n,
        }
    )
    pq.write_table(cand, valuation / COMP_CANDIDATE_FILENAME)
    adj = pa.table(
        {
            "adjustment_id": [f"A{i}" for i in range(n)],
            "candidate_id": ids,
            "adjustment_type": ["时间"] * n,
            "amount": [Decimal("1.0500")] * n,
            "sale_date": [date(2026, 5, 1)] * n,
            "valuation_date": [_VAL_DATE] * n,
            "basis": ["同小区序列"] * n,
            "evidence_strength": ["高"] * n,
            "source_series": ["同小区序列"] * n,
            "warning": [None] * n,
            "rule_version": ["1.0"] * n,
        }
    )
    pq.write_table(adj, valuation / COMP_ADJUSTMENT_FILENAME)
    valid_sale = pa.table(
        {
            "sale_event_id": [f"s{i}" for i in range(n)],
            "sale_date": [date(2026, 5, 1)] * n,
            "unit_price": [Decimal(str(40000 + i * 1000)) for i in range(n)],
        }
    )
    (tmp_path / "marts").mkdir(exist_ok=True)
    pq.write_table(valid_sale, tmp_path / "marts" / "valid_sale.parquet")
    return tmp_path


# ---------------------------------------------------------------------------
# 展开函数
# ---------------------------------------------------------------------------


def _config_layer_params(*, k: Decimal, m: Decimal) -> Any:
    from compsval.valuation.interval_calibration import ExpansionParams

    return ExpansionParams(k=k, m=m, n=10)


def test_expand_keeps_center_and_only_widens() -> None:
    """中心值不变；k≥1 只加不减（新区间包含原区间）。"""
    center = Decimal("40000")
    lower = Decimal("38000")
    upper = Decimal("42000")
    expanded = expand_interval(
        center, lower, upper, _config_layer_params(k=Decimal("2.0"), m=Decimal("0.05"))
    )
    assert expanded is not None
    new_lower, new_upper = expanded
    assert (center - new_lower) == Decimal("2.0") * (center - lower)
    assert (new_upper - center) == Decimal("2.0") * (upper - center)
    assert new_lower <= lower and new_upper >= upper


def test_expand_floor_applies_to_narrow_side() -> None:
    """相对半宽下限：窄半宽被 m·center 托底。"""
    center = Decimal("40000")
    lower = Decimal("39900")
    upper = Decimal("40100")
    expanded = expand_interval(
        center, lower, upper, _config_layer_params(k=Decimal("1.0"), m=Decimal("0.10"))
    )
    assert expanded is not None
    new_lower, new_upper = expanded
    assert (center - new_lower) == Decimal("4000")  # 0.10 * 40000
    assert (new_upper - center) == Decimal("4000")


def test_expand_none_interval_stays_none() -> None:
    """INSUFFICIENT 不形成区间（None）→ 不展开。"""
    assert (
        expand_interval(
            Decimal("40000"), None, None, _config_layer_params(k=Decimal("2"), m=Decimal("0.1"))
        )
        is None
    )


# ---------------------------------------------------------------------------
# 配置加载（严格校验 + 回退链）
# ---------------------------------------------------------------------------


def test_load_legacy_version_returns_none(tmp_path: Path) -> None:
    """规则版本 1.0 → 不读配置（旧行为，G3 回放证据可复现）。"""
    assert load_interval_calibration(tmp_path, LEGACY_RULE_VERSION) is None


def test_load_missing_config_raises_dependency(tmp_path: Path) -> None:
    """1.1 配置缺失 → MissingDependencyError（保守显式失败，不静默降级）。"""
    with pytest.raises(MissingDependencyError):
        load_interval_calibration(tmp_path, INTERVAL_CALIBRATION_RULE_VERSION)


def test_load_corrupt_config_raises_input(tmp_path: Path) -> None:
    """配置不可解析 → InvalidInputError（退出码 2）。"""
    path = calibration_config_path(tmp_path, INTERVAL_CALIBRATION_RULE_VERSION)
    path.parent.mkdir(parents=True)
    path.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidInputError):
        load_interval_calibration(tmp_path, INTERVAL_CALIBRATION_RULE_VERSION)


@pytest.mark.parametrize(
    "overrides",
    [
        {"method": "other_method"},
        {"config_version": "1.0"},
        {"source_dataset_sha256": "short"},
        {"split_point": ""},
        {"caps": {}},
        {"layers": []},
        {
            "layers": [
                {"community_id": "*", "confidence": "*", "k": 9.0, "m": 0.08, "n": 10}
            ]
        },
        {
            "layers": [
                {"community_id": "*", "confidence": "*", "k": 1.8, "m": 0.50, "n": 10}
            ]
        },
        {
            "layers": [
                {"community_id": "*", "confidence": "*", "k": 0.5, "m": 0.08, "n": 10}
            ]
        },
    ],
)
def test_load_invalid_config_rejected(tmp_path: Path, overrides: dict[str, Any]) -> None:
    """缺字段/越界/错方法一律拒绝（k<m 上限、k≥1、哈希/切分点必需）。"""
    _write_config(tmp_path, _config(**overrides))
    with pytest.raises(InvalidInputError):
        load_interval_calibration(tmp_path, INTERVAL_CALIBRATION_RULE_VERSION)


def test_params_for_layer_fallback_chain(tmp_path: Path) -> None:
    """层回退：(小区, 可信度) → (*, 可信度) → (*, *)。"""
    _write_config(
        tmp_path,
        _config(
            layers=[
                {
                    "community_id": _COMMUNITY,
                    "confidence": "低",
                    "k": 2.0,
                    "m": 0.05,
                    "n": 100,
                },
                {"community_id": "*", "confidence": "*", "k": 1.5, "m": 0.08, "n": 892},
            ]
        ),
    )
    calibration = load_interval_calibration(tmp_path, INTERVAL_CALIBRATION_RULE_VERSION)
    assert calibration is not None
    assert calibration.params_for(_COMMUNITY, "低").k == Decimal("2.0")
    assert calibration.params_for(_COMMUNITY, "高").k == Decimal("1.5")  # 通配可信度缺 → 全局
    assert calibration.params_for(_OTHER_COMMUNITY, "低").k == Decimal("1.5")


# ---------------------------------------------------------------------------
# apply_aggregation 集成：中心值零改动 + 区间加宽 + 配置缺失显式失败
# ---------------------------------------------------------------------------


def test_apply_aggregation_v11_widens_interval_keeps_center(tmp_path: Path) -> None:
    """1.0 vs 1.1：中心值相同、可信度相同、区间只加不窄（校准展开）。"""
    data_dir = _write_valuation_inputs(tmp_path)
    _write_config(data_dir, _config())
    valid_sale = pq.read_table(data_dir / "marts" / "valid_sale.parquet")
    inputs: list[InputRef] = []

    base = apply_aggregation(
        data_dir=data_dir,
        subject=_subject(),
        valid_sale=valid_sale,
        input_refs=inputs,
        rule_version=LEGACY_RULE_VERSION,
    )
    calibrated = apply_aggregation(
        data_dir=data_dir,
        subject=_subject(),
        valid_sale=valid_sale,
        input_refs=inputs,
        rule_version=INTERVAL_CALIBRATION_RULE_VERSION,
    )
    assert base.result.center == calibrated.result.center
    assert base.result.confidence == calibrated.result.confidence
    assert calibrated.result.rule_version == INTERVAL_CALIBRATION_RULE_VERSION
    assert base.result.range_lower is not None and calibrated.result.range_lower is not None
    assert base.result.range_upper is not None and calibrated.result.range_upper is not None
    assert calibrated.result.range_lower <= base.result.range_lower
    assert calibrated.result.range_upper >= base.result.range_upper
    base_width = base.result.range_upper - base.result.range_lower
    new_width = calibrated.result.range_upper - calibrated.result.range_lower
    assert new_width > base_width  # 宽度真实上移（加宽方向）


def test_apply_aggregation_v11_without_config_fails_loudly(tmp_path: Path) -> None:
    """1.1 无配置 → MissingDependencyError（显式失败，而非静默输出未校准区间）。"""
    data_dir = _write_valuation_inputs(tmp_path)
    with pytest.raises(MissingDependencyError):
        apply_aggregation(
            data_dir=data_dir,
            subject=_subject(),
            valid_sale=pq.read_table(data_dir / "marts" / "valid_sale.parquet"),
            input_refs=[],
            rule_version=INTERVAL_CALIBRATION_RULE_VERSION,
        )


# ---------------------------------------------------------------------------
# 单调性回归（specs delta 场景）
# ---------------------------------------------------------------------------


def test_monotonic_interval_across_confidence_after_expansion() -> None:
    """同 comps 下可信度越低 → 展开后区间相对宽度不收窄（HIGH ⊆ MEDIUM ⊆ LOW）。"""
    comps = _good_comps()
    policy = AggregationPolicy()
    params = ExpansionParams(k=Decimal("1.8"), m=Decimal("0.08"), n=892)
    widths: dict[ConfidenceLevel, Decimal] = {}
    intervals: dict[ConfidenceLevel, tuple[Decimal, Decimal]] = {}
    for level in (ConfidenceLevel.HIGH, ConfidenceLevel.MEDIUM, ConfidenceLevel.LOW):
        stats = policy.compute_stats(comps, level)
        expanded = expand_interval(stats.center, stats.lower, stats.upper, params)
        assert expanded is not None
        intervals[level] = expanded
        widths[level] = expanded[1] - expanded[0]
    assert widths[ConfidenceLevel.HIGH] <= widths[ConfidenceLevel.MEDIUM]
    assert widths[ConfidenceLevel.MEDIUM] <= widths[ConfidenceLevel.LOW]
    # 包含关系：弱数据区间覆盖强数据区间（同中心）
    center = policy.compute_stats(comps, ConfidenceLevel.HIGH).center
    assert intervals[ConfidenceLevel.LOW][0] <= intervals[ConfidenceLevel.HIGH][0] <= center
    assert center <= intervals[ConfidenceLevel.HIGH][1] <= intervals[ConfidenceLevel.LOW][1]


def test_weaker_subject_data_not_narrower_after_calibration(tmp_path: Path) -> None:
    """属性缺失（数据更弱）→ 校准后区间不窄于原区间且可信度不高于原可信度。"""
    data_dir = _write_valuation_inputs(tmp_path)
    _write_config(data_dir, _config())
    valid_sale = pq.read_table(data_dir / "marts" / "valid_sale.parquet")

    strong = apply_aggregation(
        data_dir=data_dir,
        subject=_subject(),
        valid_sale=valid_sale,
        input_refs=[],
        rule_version=INTERVAL_CALIBRATION_RULE_VERSION,
    )
    weak = apply_aggregation(
        data_dir=data_dir,
        subject=_subject(
            floor=None,
            total_floors=None,
            has_elevator=None,
            year_built=None,
            orientation="UNKNOWN",
            site_observations=None,
        ),
        valid_sale=valid_sale,
        input_refs=[],
        rule_version=INTERVAL_CALIBRATION_RULE_VERSION,
    )
    assert strong.result.confidence is ConfidenceLevel.MEDIUM
    assert weak.result.confidence is ConfidenceLevel.LOW
    if weak.result.range_lower is not None and weak.result.range_upper is not None:
        assert strong.result.range_lower is not None
        assert strong.result.range_upper is not None
        weak_width = weak.result.range_upper - weak.result.range_lower
        strong_width = strong.result.range_upper - strong.result.range_lower
        assert weak_width >= strong_width
