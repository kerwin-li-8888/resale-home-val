"""WP6-A: CandidateRetriever 候选案例池（VAL1-002）。

对照 WP6-A 验收标准：
① 候选只含估值时点之前成交（无未来数据泄漏反例测试）；
② 排除有理由且逐条可溯源（comp_candidate 全量留痕含排除行）；
③ 无必要字段/未匹配/异常记录排除而非静默纳入；
④ 模型字段与数据字典 §3.9-3.11 一致（缺失用 UNKNOWN/None 不用 0）；
⑤ ``compsval catalog`` 可列估值中间结果（val_ 视图）；
⑥ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from compsval import catalog
from compsval.contract import models as contract_models
from compsval.contract.models import CompCandidate, SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    DEFAULT_RULE_VERSION,
    REASON_ABNORMAL,
    REASON_AFTER_CUTOFF,
    REASON_MISSING_FIELDS,
    REASON_NON_RESIDENTIAL,
    REASON_SELECTED,
    REASON_UNMATCHED_COMMUNITY,
    SUBJECT_FILENAME,
    VALUATION_LAYER,
    VALUATION_RUN_FILENAME,
    CandidateRetriever,
    build_valuation,
    run_id_of,
)

_CUTOFF = date(2026, 7, 21)


def _subject(**overrides: Any) -> SubjectProperty:
    base: dict[str, Any] = {
        "subject_id": "SUBJ-TEST-001",
        "community_id": "C-XXXX0013",
        "area_sqm": Decimal("50.3"),
        "layout": "2室1厅",
        "valuation_date": _CUTOFF,
    }
    base.update(overrides)
    return SubjectProperty(**base)


def _valid_sale_table(
    rows: list[dict[str, object]],
) -> pa.Table:
    """构造最小 valid_sale 表（含检索与溯源所需列）。"""
    columns: dict[str, list[object]] = {
        "sale_event_id": [],
        "community_id": [],
        "sale_date": [],
        "layout": [],
        "area_sqm": [],
        "total_price_yuan": [],
        "unit_price": [],
        "anomaly_flag": [],
        "raw_locator": [],
    }
    for row in rows:
        for name in columns:
            columns[name].append(row.get(name))
    return pa.table(columns)


def _complete_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "sale_event_id": "SRC-007-line1",
        "community_id": "C-XXXX0013",
        "sale_date": date(2026, 7, 20),
        "layout": "2室1厅",
        "area_sqm": 50.3,
        "total_price_yuan": 1300000.0,
        "unit_price": 25845,
        "anomaly_flag": "正常",
        "raw_locator": "1",
    }
    base.update(overrides)
    return base


# ---- ① 候选只含截点之前成交（无未来数据泄漏反例） ----
def test_future_sale_never_selected() -> None:
    table = _valid_sale_table(
        [
            _complete_row(),
            _complete_row(
                sale_event_id="SRC-007-line-future",
                sale_date=date(2026, 7, 30),
                raw_locator="2",
            ),
        ]
    )
    candidates = CandidateRetriever().retrieve(table, _subject())
    selected = [c for c in candidates if c.selected]
    assert [c.sale_event_id for c in selected] == ["SRC-007-line1"]
    # 截点之后数据绝不进入候选（验收①反例）
    assert all(c.sale_event_id != "SRC-007-line-future" for c in selected)


def test_after_cutoff_excluded_with_reason() -> None:
    table = _valid_sale_table(
        [_complete_row(sale_date=date(2026, 7, 22), raw_locator="2")]
    )
    candidates = CandidateRetriever().retrieve(table, _subject())
    assert len(candidates) == 1
    assert candidates[0].selected is False
    assert candidates[0].reason == REASON_AFTER_CUTOFF


def test_cutoff_inclusive() -> None:
    # 数据截点当日成交可用（截点=当日及之前）
    table = _valid_sale_table(
        [_complete_row(sale_date=date(2026, 7, 21), raw_locator="1")]
    )
    candidates = CandidateRetriever().retrieve(table, _subject())
    assert candidates[0].selected is True


# ---- ② 排除有理由且逐条可溯源（全量留痕） ----
def test_every_row_has_candidate_and_reason() -> None:
    table = _valid_sale_table(
        [
            _complete_row(sale_event_id="A", sale_date=date(2026, 7, 20)),
            _complete_row(sale_event_id="B", community_id=None),
            _complete_row(sale_event_id="C", sale_date=date(2026, 7, 22)),
        ]
    )
    candidates = CandidateRetriever().retrieve(table, _subject())
    # 每条进入检索范围的成交都有一行留痕
    assert len(candidates) == table.num_rows
    assert {c.sale_event_id for c in candidates} == {"A", "B", "C"}
    # 每条 reason 非空可溯源
    assert all(c.reason != "" for c in candidates)
    # 排除行有明确理由，入选行有纳入理由
    by_event = {c.sale_event_id: c for c in candidates}
    assert by_event["A"].selected and by_event["A"].reason == REASON_SELECTED
    assert not by_event["B"].selected and by_event["B"].reason == REASON_UNMATCHED_COMMUNITY
    assert not by_event["C"].selected and by_event["C"].reason == REASON_AFTER_CUTOFF


def test_candidate_id_traceable_to_sale_event() -> None:
    table = _valid_sale_table([_complete_row()])
    candidates = CandidateRetriever().retrieve(table, _subject())
    run_id = run_id_of(_subject(), _CUTOFF, DEFAULT_RULE_VERSION)
    assert candidates[0].candidate_id == f"{run_id}-SRC-007-line1"
    assert candidates[0].run_id == run_id


# ---- ③ 无必要字段/未匹配/异常记录排除而非静默纳入 ----
def test_unmatched_community_excluded() -> None:
    for community_id in (None, "", "UNKNOWN"):
        table = _valid_sale_table([_complete_row(community_id=community_id)])
        candidate = CandidateRetriever().retrieve(table, _subject())[0]
        assert candidate.selected is False, community_id
        assert candidate.reason == REASON_UNMATCHED_COMMUNITY
        # 排除行 community_id 不虚构，未知=UNKNOWN（验收④）
        assert candidate.community_id == "UNKNOWN"


def test_parking_excluded() -> None:
    table = _valid_sale_table([_complete_row(layout="车位")])
    candidate = CandidateRetriever().retrieve(table, _subject())[0]
    assert candidate.selected is False
    assert candidate.reason == REASON_NON_RESIDENTIAL


def test_abnormal_excluded() -> None:
    table = _valid_sale_table([_complete_row(anomaly_flag="疑似异常单价")])
    candidate = CandidateRetriever().retrieve(table, _subject())[0]
    assert candidate.selected is False
    assert candidate.reason == REASON_ABNORMAL


@pytest.mark.parametrize(
    "field",
    ["sale_date", "area_sqm", "total_price_yuan", "unit_price"],
)
def test_missing_required_field_excluded(field: str) -> None:
    table = _valid_sale_table([_complete_row(**{field: None})])
    candidate = CandidateRetriever().retrieve(table, _subject())[0]
    assert candidate.selected is False, field
    assert candidate.reason == REASON_MISSING_FIELDS


def test_missing_required_column_raises() -> None:
    table = pa.table({"sale_event_id": ["A"]})
    with pytest.raises(ValueError, match="缺少必要列"):
        CandidateRetriever().retrieve(table, _subject())


# ---- ④ 模型与数据字典 §3.9-3.11 一致（缺失用 UNKNOWN/None 不用 0） ----
def test_wp6_models_registered_in_contract() -> None:
    for name in ("subject_property", "valuation_run", "comp_candidate"):
        assert name in contract_models.CONTRACT_MODELS
        schema = contract_models.json_schema(name)
        assert schema["title"]
        assert schema["properties"]


def test_subject_property_rejects_zero_area() -> None:
    # 面积未知必须为 None，不得写成 0
    with pytest.raises(ValidationError):
        _subject(area_sqm=Decimal("0"))


def test_comp_candidate_unknown_numeric_is_none_not_zero() -> None:
    candidate = CompCandidate(
        candidate_id="C1",
        run_id="RUN-1",
        sale_event_id="S1",
        community_id="C-1",
        selected=True,
        reason=REASON_SELECTED,
    )
    assert candidate.tier is None
    assert candidate.similarity is None
    # 未知数值字段不得写成 0
    with pytest.raises(ValidationError):
        CompCandidate(
            candidate_id="C2",
            run_id="RUN-1",
            sale_event_id="S1",
            community_id="C-1",
            selected=True,
            tier=0,
            reason="x",
        )


def test_selected_candidate_has_reason_and_no_fabricated_tier() -> None:
    table = _valid_sale_table([_complete_row()])
    candidate = CandidateRetriever().retrieve(table, _subject())[0]
    assert candidate.selected is True
    assert candidate.reason == REASON_SELECTED
    # WP6-A 不做层级/相似度（归 B/C/D）：未分层= None
    assert candidate.tier is None
    assert candidate.similarity is None


# ---- ⑤ 目录注册：compsval catalog 可列估值中间结果（val_ 视图） ----
def _write_valid_sale_mart(
    data_dir: Path,
    table: pa.Table,
) -> None:
    (data_dir / MARTS_LAYER).mkdir(parents=True, exist_ok=True)
    path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    pq.write_table(table, path, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=MARTS_LAYER,
            table="valid_sale",
            built_at=datetime.now(UTC),
            row_count=table.num_rows,
            inputs=[InputRef(dataset="chengjiao_list", fetched_at="20260821T000000Z")],
            package_version="test",
            notes="test",
        ),
        path,
    )


def test_build_valuation_writes_tables_and_registers_catalog(
    tmp_path: Path,
) -> None:
    _write_valid_sale_mart(tmp_path, _valid_sale_table([_complete_row()]))
    result = build_valuation(_subject(), data_dir=tmp_path)

    assert result.run.run_id == run_id_of(_subject(), _CUTOFF, DEFAULT_RULE_VERSION)
    assert result.candidate_path.name == COMP_CANDIDATE_FILENAME
    assert result.candidate_path.is_file()
    assert result.subject_path.name == SUBJECT_FILENAME
    assert result.run_path.name == VALUATION_RUN_FILENAME
    # 原子写盘：无 .incomplete 残留
    assert not list((tmp_path / VALUATION_LAYER).glob("*.incomplete"))

    out = pq.read_table(result.candidate_path)
    assert out.num_rows == 1
    assert out.column("selected").to_pylist() == [True]
    assert out.column("rule_version").to_pylist() == [DEFAULT_RULE_VERSION]

    # 三个表的 DerivedManifest 就位
    for path in (result.subject_path, result.run_path, result.candidate_path):
        manifest = json.loads(path.with_suffix(".manifest.json").read_text(encoding="utf-8"))
        assert manifest["layer"] == VALUATION_LAYER
        assert manifest["row_count"] >= 1

    # 验收⑤：catalog 注册 val_ 视图，可查询
    con = catalog.connect(tmp_path)
    try:
        # 三个 val_ 视图已注册（可查到视图名）
        tables = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
            ).fetchall()
        }
        assert {"val_subject_property", "val_valuation_run", "val_comp_candidate"} <= tables
        rows = con.execute("SELECT candidate_id FROM val_comp_candidate").fetchall()
        assert len(rows) == 1
    finally:
        con.close()


def test_build_valuation_reproducible(tmp_path: Path) -> None:
    table = _valid_sale_table(
        [_complete_row(), _complete_row(sale_event_id="B", community_id=None)]
    )
    _write_valid_sale_mart(tmp_path, table)
    first = build_valuation(_subject(), data_dir=tmp_path)
    second = build_valuation(_subject(), data_dir=tmp_path)
    assert pq.read_table(first.candidate_path).equals(pq.read_table(second.candidate_path))


def test_build_valuation_missing_marts_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_valuation(_subject(), data_dir=tmp_path)


# ---- 真实数据对拍（27 条成交：6 条匹配可入候选，21 条未匹配排除有理由） ----
def test_real_like_snapshot_6_selected_21_excluded() -> None:
    rows = [
        _complete_row(sale_event_id=f"SRC-007-line{m}") for m in range(6)
    ]  # 6 条已匹配
    rows += [
        _complete_row(sale_event_id=f"unmatched-{i}", community_id=None)
        for i in range(21)
    ]  # 21 条未匹配
    table = _valid_sale_table(rows)
    candidates = CandidateRetriever().retrieve(table, _subject())
    selected = [c for c in candidates if c.selected]
    excluded = [c for c in candidates if not c.selected]
    assert len(selected) == 6
    assert len(excluded) == 21
    assert all(c.reason == REASON_UNMATCHED_COMMUNITY for c in excluded)
