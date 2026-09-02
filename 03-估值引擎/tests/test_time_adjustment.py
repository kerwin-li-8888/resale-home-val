"""WP6-C: TimeAdjustmentPolicy + comp_adjustment(时间)（VAL1-004）。

对照 WP6-C 验收标准：
① 只用当时可得数据（未来成交/未来月份不进入，反例测试）；
② 无证据不强修正、有降级路径（非零时间差无可靠序列 → amount=None，不得默认
   市场不变；同日则确定系数 1.0，非猜测）；
③ 每次修正输出证据强度与警告（降级必有 warning；三条路径均带证据强度）；
④ 每条 comp_adjustment 有 basis（必填、非空）；
⑤ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import CompCandidate, SubjectProperty
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
)
from compsval.valuation.time_adjustment import (
    EvidenceStrength,
    TimeAdjustmentPolicy,
    TimeSourceSeries,
    apply_time_adjustments,
    block_series,
    coefficient_from_index,
    comp_adjustment_table,
    month_of,
    same_community_index,
)

_COMMUNITY = "C-XXXX0013"


def _subject(valuation_date: date = date(2026, 7, 21)) -> SubjectProperty:
    return SubjectProperty(
        subject_id="SUBJ-TEST-001",
        community_id=_COMMUNITY,
        area_sqm=Decimal("50.3"),
        layout="2室1厅",
        valuation_date=valuation_date,
    )


def _sale_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.table(
        {
            "sale_event_id": [r.get("sale_event_id") for r in rows],
            "community_id": [r.get("community_id", _COMMUNITY) for r in rows],
            "sale_date": [r.get("sale_date") for r in rows],
            "unit_price": [r.get("unit_price") for r in rows],
        }
    )


def _market_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.table(
        {
            "series_id": [r.get("series_id", "MS") for r in rows],
            "region": [r.get("region") for r in rows],
            "month": [r.get("month") for r in rows],
            "price": [r.get("price") for r in rows],
        }
    )


def _candidate(
    sale_event_id: str,
    *,
    community_id: str = _COMMUNITY,
    selected: bool = True,
) -> CompCandidate:
    return CompCandidate(
        candidate_id=f"RUN-{sale_event_id}",
        run_id="RUN",
        sale_event_id=sale_event_id,
        community_id=community_id,
        selected=selected,
        tier=1,
        similarity=Decimal("0.88"),
        reason="TEST",
    )


# ---------------------------------------------------------------------------
# 验收①：只用当时可得数据（未来数据不进入）
# ---------------------------------------------------------------------------


def test_same_community_index_excludes_future_sales() -> None:
    """验收购①反例：估值时点之后成交不进入同小区序列。"""
    v = _sale_table(
        [
            {"sale_event_id": "s1", "sale_date": date(2026, 5, 15), "unit_price": 20000},
            # 未来成交：估值时点 2026-06-30 之后 → 必须排除
            {"sale_event_id": "s2", "sale_date": date(2026, 7, 15), "unit_price": 30000},
        ]
    )
    idx = same_community_index(v, _COMMUNITY, valuation_date=date(2026, 6, 30))
    assert len(idx) == 1
    assert idx[0][0] == date(2026, 5, 1)
    assert idx[0][1] == Decimal("20000")


def test_block_series_excludes_future_month() -> None:
    """验收①反例：市值时点后才可知的月度聚合（月末）不得进入板块序列。"""
    m = _market_table(
        [
            # 2026-08 的月度均价月末才可知 → 估值时点 2026-06-30 时不可用
            {"region": "东泊南", "month": date(2026, 8, 1), "price": Decimal("30000")},
            {"region": "东泊南", "month": date(2026, 5, 1), "price": Decimal("28000")},
        ]
    )
    b = block_series(m, "东泊南", valuation_date=date(2026, 6, 30))
    assert [x[0] for x in b] == [date(2026, 5, 1)]


def test_only_future_series_degrades_with_future_reason() -> None:
    """验收①反例：全部序列都是未来数据 → 降级且 basis 指明未来序列。"""
    v = _sale_table([])
    m = _market_table(
        [{"region": "东泊南", "month": date(2026, 8, 1), "price": Decimal("30000")}]
    )
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s9"),
        subject=_subject(date(2026, 7, 21)),
        sale_date=date(2026, 6, 1),
        valid_sale=v,
        market_series=m,
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.amount is None
    assert "未来" in adj.basis or "估值时点之后" in adj.basis


def test_future_sale_date_degrades_not_unchanged() -> None:
    """验收①反例（纵深守卫）：成交日期晚于估值时点 → 不得当“同日”输出系数 1.0。"""
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s9"),
        subject=_subject(date(2026, 7, 21)),
        sale_date=date(2026, 8, 15),  # 未来成交
        valid_sale=_sale_table([]),
        market_series=_market_table([]),
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.amount is None  # 基于当时不可得的数据，不输出系数
    assert adj.evidence_strength == EvidenceStrength.INSUFFICIENT
    assert "晚于估值时点" in adj.basis
    assert adj.source_series == TimeSourceSeries.NONE
    assert adj.warning is not None


# ---------------------------------------------------------------------------
# 验收②：无证据不强修正、有降级路径
# ---------------------------------------------------------------------------


def test_counterexample_nonzero_gap_no_series_not_unchanged() -> None:
    """反例（验收②）：非零时间差 + 无可靠序列 → amount 必须为 None，不得默认 1.0。"""
    v = _sale_table(
        [{"sale_event_id": "s1", "sale_date": date(2026, 5, 1), "unit_price": 20000}]
    )  # 同小区仅 1 个成交月 → 序列不足
    m = _market_table([])  # 无板块序列
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s1"),
        subject=_subject(date(2026, 7, 1)),
        sale_date=date(2026, 5, 1),
        valid_sale=v,
        market_series=m,
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.amount is None  # 不输出虚构系数
    assert adj.evidence_strength == EvidenceStrength.INSUFFICIENT
    assert adj.warning is not None  # 把降级传给下游


def test_same_day_is_determined_coefficient_not_guess() -> None:
    """同日成交（零时间差）→ 系数 1.0（确定，非“默认市场不变”的猜测）。"""
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s1"),
        subject=_subject(date(2026, 7, 21)),
        sale_date=date(2026, 7, 21),
        valid_sale=_sale_table([]),
        market_series=_market_table([]),
        community_block={},
    )
    assert adj.amount == Decimal("1.0")
    assert adj.source_series == TimeSourceSeries.SAME_TIME
    assert adj.evidence_strength == EvidenceStrength.HIGH


def test_insufficient_single_month_degrades() -> None:
    """同小区序列仅 1 个月 → 无市场移动证据 → 降级。"""
    v = _sale_table(
        [{"sale_event_id": "s1", "sale_date": date(2026, 5, 1), "unit_price": 20000}]
    )
    m = _market_table([])
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s1"),
        subject=_subject(date(2026, 7, 1)),
        sale_date=date(2026, 6, 1),
        valid_sale=v,
        market_series=m,
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.amount is None
    assert adj.evidence_strength == EvidenceStrength.INSUFFICIENT


# ---------------------------------------------------------------------------
# 正常路径：同小区序列 / 竞争板块序列（系数由此可溯源地得出）
# ---------------------------------------------------------------------------


def test_same_community_series_coefficient() -> None:
    """同小区 2 个月序列 → 系数 = 时点月价 / 成交月价。"""
    v = _sale_table(
        [
            {"sale_event_id": "s1", "sale_date": date(2026, 2, 15), "unit_price": 20000},
            {"sale_event_id": "s2", "sale_date": date(2026, 5, 15), "unit_price": 22000},
        ]
    )
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s9"),
        subject=_subject(date(2026, 6, 30)),
        sale_date=date(2026, 3, 1),
        valid_sale=v,
        market_series=_market_table([]),
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.amount == Decimal("1.1000")
    assert adj.source_series == TimeSourceSeries.SAME_COMMUNITY
    assert adj.evidence_strength == EvidenceStrength.HIGH
    assert adj.warning is None


def test_competitive_block_series_coefficient() -> None:
    """同小区序列不足、板块序列可得 → 用板块序列，证据中。"""
    v = _sale_table(
        [{"sale_event_id": "s1", "sale_date": date(2026, 5, 1), "unit_price": 20000}]
    )  # 同小区仅 1 个月
    m = _market_table(
        [
            {"region": "东泊南", "month": date(2026, 2, 1), "price": Decimal("28000")},
            {"region": "东泊南", "month": date(2026, 5, 1), "price": Decimal("29400")},
        ]
    )
    adj = TimeAdjustmentPolicy().adjust(
        candidate=_candidate("s1"),
        subject=_subject(date(2026, 6, 30)),
        sale_date=date(2026, 3, 1),
        valid_sale=v,
        market_series=m,
        community_block={_COMMUNITY: "东泊南"},
    )
    assert adj.source_series == TimeSourceSeries.COMPETITIVE_BLOCK
    assert adj.amount == Decimal("1.0500")
    assert adj.evidence_strength == EvidenceStrength.MEDIUM
    assert adj.warning is not None  # 板块序列为平台聚合，带警告


# ---------------------------------------------------------------------------
# 验收③/④：证据强度、警告、basis
# ---------------------------------------------------------------------------


def test_every_adjustment_has_basis_and_strength() -> None:
    """每条修正都带 basis（必填）与证据强度（验收③/④）。"""
    cases = [
        # 同日 → 高
        (_candidate("a"), date(2026, 7, 1), date(2026, 7, 1), _sale_table([]), _market_table([])),
        # 降级 → 不足
        (
            _candidate("b"),
            date(2026, 6, 1),
            date(2026, 7, 1),
            _sale_table(
                [{"sale_event_id": "b", "sale_date": date(2026, 5, 1), "unit_price": 20000}]
            ),
            _market_table([]),
        ),
    ]
    policy = TimeAdjustmentPolicy()
    for cand, sale_date, val_date, v, m in cases:
        adj = policy.adjust(
            candidate=cand,
            subject=_subject(val_date),
            sale_date=sale_date,
            valid_sale=v,
            market_series=m,
            community_block={_COMMUNITY: "东泊南"},
        )
        assert adj.basis  # 必填、非空
        assert adj.evidence_strength in EvidenceStrength  # 必填
    table = comp_adjustment_table(
        [
            policy.adjust(
                candidate=_candidate("b"),
                subject=_subject(date(2026, 7, 1)),
                sale_date=date(2026, 6, 1),
                valid_sale=_sale_table(
                    [{"sale_event_id": "b", "sale_date": date(2026, 5, 1), "unit_price": 20000}]
                ),
                market_series=_market_table([]),
                community_block={_COMMUNITY: "东泊南"},
            )
        ],
        rule_version="1.0",
    )
    assert all(basis for basis in table.column("basis").to_pylist())


def test_coefficient_from_index_requires_forward_anchor() -> None:
    """系数需成交后的前瞻锚点：只有成交月及之前数据 → 无系数（降级）。"""
    index = [(date(2026, 1, 1), Decimal("20000")), (date(2026, 5, 1), Decimal("22000"))]
    # 成交月晚于序列最新月（5月）→ 无成交后锚点
    assert (
        coefficient_from_index(index, date(2026, 5, 1), min_series_points=2) is None
    )
    # 序列月数不足 → None
    assert (
        coefficient_from_index(
            [(date(2026, 5, 1), Decimal("22000"))],
            date(2026, 3, 1),
            min_series_points=2,
        )
        is None
    )
    # 正常：成交月 3 月，最新月 5 月之后 → 系数
    assert coefficient_from_index(index, date(2026, 3, 1), min_series_points=2) == (
        Decimal("1.1000")
    )


# ---------------------------------------------------------------------------
# 写盘主入口：comp_adjustment + manifest，.incomplete 清理
# ---------------------------------------------------------------------------


def _write_comp_candidate(tmp_path: Path, selected: list[dict[str, object]]) -> Path:
    valuation = tmp_path / VALUATION_LAYER
    valuation.mkdir(parents=True, exist_ok=True)
    path = valuation / COMP_CANDIDATE_FILENAME
    table = pa.table(
        {
            "candidate_id": [r.get("candidate_id") for r in selected],
            "run_id": [r.get("run_id", "RUN") for r in selected],
            "sale_event_id": [r.get("sale_event_id") for r in selected],
            "community_id": [r.get("community_id", _COMMUNITY) for r in selected],
            "selected": [r.get("selected", True) for r in selected],
            "tier": [r.get("tier") for r in selected],
            "similarity": [r.get("similarity") for r in selected],
            "reason": [r.get("reason", "TEST") for r in selected],
        }
    )
    pq.write_table(table, path)
    return path


def test_apply_writes_comp_adjustment_and_manifest(tmp_path: Path) -> None:
    """主入口：只对 selected 可比写时间修正行 + DerivedManifest，无 .incomplete 残留。"""
    _write_comp_candidate(
        tmp_path,
        [
            {
                "candidate_id": "RUN-s1",
                "sale_event_id": "s1",
                "community_id": _COMMUNITY,
                "selected": True,
                "tier": 1,
                "similarity": 0.88,
            },
            {
                "candidate_id": "RUN-s2",
                "sale_event_id": "s2",
                "community_id": "C-OTHER",
                "selected": False,  # 被排除的可比不做时间修正
                "tier": None,
                "similarity": None,
            },
        ],
    )
    valid_sale = _sale_table(
        [
            {"sale_event_id": "s1", "sale_date": date(2026, 7, 21), "unit_price": 25800},
        ]
    )
    market_series = _market_table(
        [{"region": "东泊南", "month": date(2026, 8, 1), "price": Decimal("30000")}]
    )  # 未来月 → 降级
    communities = pa.table(
        {"community_id": [_COMMUNITY], "block": ["东泊南"]}
    )

    result = apply_time_adjustments(
        data_dir=tmp_path,
        subject=_subject(date(2026, 7, 21)),
        valid_sale=valid_sale,
        market_series=market_series,
        communities=communities,
        input_refs=[],
    )
    assert result.adjustment_path.is_file()
    # 只有 1 个 selected 可比 → 1 行时间修正
    assert len(result.adjustments) == 1
    # 同日成交 → 系数确定 1.0
    assert result.adjustments[0].amount == Decimal("1.0")
    table = pq.read_table(result.adjustment_path)
    assert table.num_rows == 1
    assert table.column("adjustment_type").to_pylist() == ["时间"]
    assert table.column("basis").to_pylist()[0]  # 非空
    # manifest + 无 .incomplete
    assert (result.adjustment_path.with_suffix(".manifest.json")).is_file()
    incomplete = list((tmp_path / VALUATION_LAYER).glob("*.incomplete"))
    assert not incomplete


def test_apply_requires_comp_candidate(tmp_path: Path) -> None:
    """缺 comp_candidate.parquet → FileNotFoundError（证据缺失不静默）。"""
    with pytest.raises(FileNotFoundError):
        apply_time_adjustments(
            data_dir=tmp_path,
            subject=_subject(date(2026, 7, 21)),
            valid_sale=_sale_table([]),
            market_series=_market_table([]),
            communities=pa.table({"community_id": [], "block": []}),
            input_refs=[],
        )


def test_month_of() -> None:
    assert month_of(date(2026, 7, 21)) == date(2026, 7, 1)