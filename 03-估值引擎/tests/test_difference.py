"""WP6-D: DifferenceAdjustmentPolicy + comp_adjustment(差异)（VAL1-005）。

对照 WP6-D 验收标准：
① 每个数值修正有市场证据（basis 必填）；无证据时不得输出数值 factor（反例）；
② 不可量化因素保持 direction_only/unknown，不虚构比例（装修/维护、缺失楼层，
   反例）；
③ 调整记录含方向 direction/数值 factor/公式 formula/依据 basis/规则版本；
④ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import CompCandidate, SubjectProperty
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
)
from compsval.valuation.difference import (
    ADJUSTMENT_TYPE_DIFFERENCE,
    ComparableAttributes,
    DifferenceAdjustmentPolicy,
    DifferenceDimension,
    DifferenceDirection,
    LocalEvidenceRecord,
    apply_difference_adjustments,
    comp_difference_table,
)
from compsval.valuation.time_adjustment import (
    COMP_ADJUSTMENT_FILENAME,
    EvidenceStrength,
)

_COMMUNITY = "C-XXXX0013"


def _subject(**overrides: Any) -> SubjectProperty:
    defaults: dict[str, Any] = dict(
        subject_id="SUBJ-TEST-001",
        community_id=_COMMUNITY,
        area_sqm=Decimal("50.3"),
        layout="2室1厅",
        valuation_date=date(2026, 7, 21),
        has_elevator=True,
        orientation="南",
        year_built=2015,
        floor=12,
    )
    defaults.update(overrides)
    return SubjectProperty(**defaults)


def _comparable(**overrides: Any) -> ComparableAttributes:
    defaults: dict[str, Any] = dict(
        area_sqm=Decimal("50.0"),
        layout="2室1厅",
        floor=9,
        total_floors=30,
        has_elevator=True,
        orientation="南",
        year_built=2015,
    )
    defaults.update(overrides)
    return ComparableAttributes(**defaults)


def _candidate(sale_event_id: str = "s1") -> CompCandidate:
    return CompCandidate(
        candidate_id=f"RUN-{sale_event_id}",
        run_id="RUN",
        sale_event_id=sale_event_id,
        community_id=_COMMUNITY,
        selected=True,
        tier=1,
        similarity=Decimal("0.88"),
        reason="TEST",
    )


def _dim(rows: list, feature: str) -> list:
    """取某维度的调整记录。"""
    return [r for r in rows if r.feature == feature]


# ---------------------------------------------------------------------------
# 验收①：每个数值修正有市场证据（basis 必填）；无证据不虚构 factor
# ---------------------------------------------------------------------------


def test_every_row_has_basis_and_rule_version() -> None:
    """每条差异记录都有 basis 与 rule_version（验收①/③）。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(),
        comparable=_comparable(),
    )
    assert len(rows) == 6  # 面积/楼层/电梯/朝向/年代/装修
    for row in rows:
        assert row.basis  # 必填、非空
        assert row.rule_version == "1.0"
        assert row.adjustment_type == ADJUSTMENT_TYPE_DIFFERENCE
        assert row.direction  # 必有方向


def test_counterexample_no_evidence_does_not_fabricate_factor() -> None:
    """反例（验收①/②）：无市场证据时电梯/朝向/年代/楼层只有方向，factor 必须为 None。"""
    subject = _subject(has_elevator=True, orientation="南", year_built=2015, floor=12)
    comparable = _comparable(
        has_elevator=False,  # 电梯不同
        orientation="北",  # 朝向不同且差一个评级
        year_built=2010,  # 年代不同
        floor=9,  # 楼层不同
    )
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=subject,
        comparable=comparable,
        local_evidence={},  # 无已批准市场证据
    )
    for row in _dim(rows, DifferenceDimension.ELEVATOR.value):
        assert row.factor is None
        assert "证据不足" in row.basis
    for row in _dim(rows, DifferenceDimension.ORIENTATION.value):
        assert row.factor is None
    for row in _dim(rows, DifferenceDimension.YEAR_BUILT.value):
        assert row.factor is None
    for row in _dim(rows, DifferenceDimension.FLOOR.value):
        assert row.factor is None  # 无证据不得凭差值写比例
        assert row.direction in {DifferenceDirection.UP.value, DifferenceDirection.DOWN.value}


def test_with_evidence_emits_numeric_factor_and_formula() -> None:
    """有市场证据 → 电梯/朝向/年代输出 factor + formula + basis（验收①/③）。"""
    evidence: dict[str, LocalEvidenceRecord] = {
        DifferenceDimension.ELEVATOR.value: {
            "factor": Decimal("1.0200"),
            "direction": "上调",
            "basis": "同小区成交对电梯溢价 2%",
            "formula": "1 + 2%（电梯溢价）",
        }
    }
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(has_elevator=True, orientation="东南", year_built=2015, floor=12),
        comparable=_comparable(
            has_elevator=False, orientation="西", year_built=2010, floor=9
        ),
        local_evidence=evidence,
    )
    el = _dim(rows, DifferenceDimension.ELEVATOR.value)[0]
    assert el.factor == Decimal("1.0200")
    assert el.formula
    assert el.evidence_strength == EvidenceStrength.MEDIUM


# ---------------------------------------------------------------------------
# 验收②：不可量化因素保持 direction_only/unknown，不虚构比例
# ---------------------------------------------------------------------------


def test_renovation_always_unknown_insufficient() -> None:
    """反例（验收②）：装修/维护不可量化 → 一律 UNKNOWN，factor=None，证据不足。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(),
        comparable=_comparable(),
    )
    ren = _dim(rows, DifferenceDimension.RENOVATION.value)[0]
    assert ren.direction == DifferenceDirection.UNKNOWN.value
    assert ren.factor is None
    assert ren.evidence_strength == EvidenceStrength.INSUFFICIENT
    assert "不可稳定量化" in ren.basis


def test_missing_floor_gives_unknown_not_fabricated() -> None:
    """反例（验收②）：可比单元楼层不可得 → 楼层 UNKNOWN，不臆测方向与数值。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(floor=12),
        comparable=_comparable(floor=None),  # 一期单元楼层不可得
    )
    fl = _dim(rows, DifferenceDimension.FLOOR.value)[0]
    assert fl.direction == DifferenceDirection.UNKNOWN.value
    assert fl.factor is None
    assert fl.warning


def test_missing_attribute_dims_unknown() -> None:
    """目标/可比某维度未知 → 对应维度 UNKNOWN（朝向/电梯/年代缺失）。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(has_elevator=None, orientation="UNKNOWN", year_built=None, floor=None),
        comparable=_comparable(
            has_elevator=None, orientation="UNKNOWN", year_built=None, floor=None
        ),
    )
    for feature in (
        DifferenceDimension.ELEVATOR.value,
        DifferenceDimension.ORIENTATION.value,
        DifferenceDimension.YEAR_BUILT.value,
        DifferenceDimension.FLOOR.value,
    ):
        rows_f = _dim(rows, feature)
        assert rows_f[0].direction == DifferenceDirection.UNKNOWN.value
        assert rows_f[0].factor is None
        assert rows_f[0].warning


# ---------------------------------------------------------------------------
# 正常路径：一致维度 → 平（factor=1.0，确定非猜测）；面积经单价口径
# ---------------------------------------------------------------------------


def test_area_no_adjust_is_determined() -> None:
    """面积由单价口径处理 → NO_ADJUST，factor=1.0（非收益性，非虚构）。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(),
        comparable=_comparable(),
    )
    ar = _dim(rows, DifferenceDimension.AREA.value)[0]
    assert ar.direction == DifferenceDirection.NO_ADJUST.value
    assert ar.factor == Decimal("1.0000")
    assert ar.evidence_strength == EvidenceStrength.HIGH


def test_equal_dimension_is_flat_not_adjust() -> None:
    """一致维度 → FLAT factor=1.0 + REASON_EQUAL（确定，无调整依据）。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(has_elevator=True, orientation="南", year_built=2015, floor=12),
        comparable=_comparable(
            has_elevator=True, orientation="南", year_built=2015, floor=12
        ),
    )
    for feature in (
        DifferenceDimension.ELEVATOR.value,
        DifferenceDimension.ORIENTATION.value,
        DifferenceDimension.YEAR_BUILT.value,
        DifferenceDimension.FLOOR.value,
    ):
        row = _dim(rows, feature)[0]
        assert row.direction == DifferenceDirection.FLAT.value
        assert row.factor == Decimal("1.0000")
        assert row.evidence_strength == EvidenceStrength.HIGH
        assert not row.warning


def test_orientation_unsupported_text_unknown() -> None:
    """朝向无法评级（如“东南/未知”之外的字符串未收录）→ UNKNOWN 不臆测。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(orientation="东南"),
        comparable=_comparable(orientation="花园朝向"),  # 无评级
    )
    ori = _dim(rows, DifferenceDimension.ORIENTATION.value)[0]
    assert ori.direction == DifferenceDirection.UNKNOWN.value
    assert ori.factor is None


# ---------------------------------------------------------------------------
# 证据强度与方向齐全（验收③）
# ---------------------------------------------------------------------------


def test_evolved_evidence_yields_expected_enum_coercion() -> None:
    """有证据时方向被枚举化且 formula/side 齐全（验收③）。"""
    evidence: dict[str, LocalEvidenceRecord] = {
        DifferenceDimension.ORIENTATION.value: {
            "factor": Decimal("0.9800"),
            "direction": "下调",
            "basis": "同小区朝向溢价证据",
            "formula": "1 - 2%（朝向）",
        }
    }
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(orientation="北"),
        comparable=_comparable(orientation="南"),  # 目标更差 → 证据下调
        local_evidence=evidence,
    )
    ori = _dim(rows, DifferenceDimension.ORIENTATION.value)[0]
    assert ori.direction == DifferenceDirection.DOWN.value
    assert ori.factor == Decimal("0.9800")
    assert ori.formula
    assert ori.subject_side == "北"
    assert ori.comparable_side == "南"


# ---------------------------------------------------------------------------
# 写盘主入口：只写 selected；既有时间行保留；diff 幂等
# ---------------------------------------------------------------------------


def _write_comp_candidate(tmp_path: Path) -> Path:
    valuation = tmp_path / VALUATION_LAYER
    valuation.mkdir(parents=True, exist_ok=True)
    path = valuation / COMP_CANDIDATE_FILENAME
    table = pa.table(
        {
            "candidate_id": ["RUN-s1", "RUN-s2"],
            "run_id": ["RUN", "RUN"],
            "sale_event_id": ["s1", "s2"],
            "community_id": [_COMMUNITY, _COMMUNITY],
            "selected": [True, False],
            "tier": [1, None],
            "similarity": [0.88, None],
            "reason": ["TEST", "EXCLUDED"],
        }
    )
    pq.write_table(table, path)
    return path


def _sale_table() -> pa.Table:
    return pa.table(
        {
            "sale_event_id": ["s1"],
            "area_sqm": [Decimal("50.0")],
            "layout": ["2室1厅"],
            "orientation": ["南"],
        }
    )


def _buildings_table() -> pa.Table:
    return pa.table(
        {
            "community_id": [_COMMUNITY],
            "total_floors": [30],
            "has_elevator": [True],
            "year_built": [2015],
        }
    )


def test_apply_writes_only_selected_and_keeps_time_rows(tmp_path: Path) -> None:
    """主入口：只对 selected 可比写差异行；保留既有时间行；无 .incomplete。"""
    _write_comp_candidate(tmp_path)
    # 先写时间行（adjustment_type=时间）
    time_path = tmp_path / VALUATION_LAYER / COMP_ADJUSTMENT_FILENAME
    pq.write_table(
        pa.table(
            {
                "adjustment_id": ["A1"],
                "candidate_id": ["RUN-s1"],
                "adjustment_type": ["时间"],
                "amount": [Decimal("1.1000")],
                "sale_date": [date(2026, 6, 1)],
                "valuation_date": [date(2026, 7, 21)],
                "basis": ["同小区序列"],
                "evidence_strength": ["高"],
                "source_series": ["同小区序列"],
                "warning": [None],
                "rule_version": ["1.0"],
            }
        ),
        time_path,
    )

    result = apply_difference_adjustments(
        data_dir=tmp_path,
        subject=_subject(),
        valid_sale=_sale_table(),
        buildings=_buildings_table(),
        input_refs=[],
    )
    # 1 个 selected + 6 维 = 6 个差异行
    assert len(result.adjustments) == 6
    assert result.adjustment_path.is_file()
    table = pq.read_table(result.adjustment_path)
    # 1 时间行 + 6 差异行
    assert table.num_rows == 7
    types = table.column("adjustment_type").to_pylist()
    assert types.count("差异") == 6
    assert types.count("时间") == 1
    # 差异行含 direction/feature/formula/side，时间行为空（兼容）
    diff_mask = [t == "差异" for t in types]
    feat = table.column("feature").to_pylist()
    assert all(f is None for f, d in zip(feat, diff_mask, strict=False) if not d)
    assert all(f for f, d in zip(feat, diff_mask, strict=False) if d)
    # manifest + 无 .incomplete
    assert (result.adjustment_path.with_suffix(".manifest.json")).is_file()
    incomplete = list((tmp_path / VALUATION_LAYER).glob("*.incomplete"))
    assert not incomplete

    # 幂等：重复运行 diff 只重建差异行，不重复累积（时间行仍 1 条）
    apply_difference_adjustments(
        data_dir=tmp_path,
        subject=_subject(),
        valid_sale=_sale_table(),
        buildings=_buildings_table(),
        input_refs=[],
    )
    again = pq.read_table(result.adjustment_path)
    assert again.num_rows == 7
    again_types = again.column("adjustment_type").to_pylist()
    assert again_types.count("差异") == 6
    assert again_types.count("时间") == 1


def test_apply_requires_comp_candidate(tmp_path: Path) -> None:
    """缺 comp_candidate.parquet → FileNotFoundError（证据缺失不静默）。"""
    with pytest.raises(FileNotFoundError):
        apply_difference_adjustments(
            data_dir=tmp_path,
            subject=_subject(),
            valid_sale=_sale_table(),
            buildings=_buildings_table(),
            input_refs=[],
        )


def test_comp_difference_table_schema() -> None:
    """差异表与扩展 schema 一致，含 direction/factor/feature/formula/side 列。"""
    rows = DifferenceAdjustmentPolicy().adjust(
        candidate=_candidate(),
        subject=_subject(),
        comparable=_comparable(),
    )
    table = comp_difference_table(
        rows, valuation_date=date(2026, 7, 21), rule_version="1.0"
    )
    assert table.num_rows == 6
    for name in (
        "direction",
        "factor",
        "feature",
        "formula",
        "subject_side",
        "comparable_side",
    ):
        assert name in table.column_names