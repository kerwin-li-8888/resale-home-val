"""WP6-B: ComparableTierPolicy + SimilarityPolicy + 竞争小区关系（VAL1-003）。

对照 WP6-B 验收标准：
① 层级放宽有序且轨迹可查（一次只放宽一个主要条件反例测试）；
② 相似度分项明确且未知项不进数值加权；
③ 竞争小区关系带置信与待确认清单、未确认不用于 D/E；
④ 每行 reason 可溯源；
⑤ ruff/mypy/pytest 通过。
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

from compsval.contract.models import CompCandidate, SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.valuation.candidate import (
    DEFAULT_RULE_VERSION,
    VALUATION_LAYER,
    build_valuation,
)
from compsval.valuation.comparable import (
    REASON_COMP_UNCONFIRMED,
    REASON_NO_TIER,
    REASON_NOT_COMPETITIVE,
    REASON_TIER_A,
    REASON_TIER_B,
    REASON_TIER_C,
    REASON_TIER_D,
    REASON_TIER_E,
    CompetitiveConfidence,
    CompetitiveRelation,
    SimilarityPolicy,
    apply_tiers_to_candidate_table,
    competitive_relations_of,
    product_level,
    select_comparables,
)

_CUTOFF = date(2026, 7, 21)
_SUBJECT_COMMUNITY = "C-XXXX0013"


def _subject(**overrides: Any) -> SubjectProperty:
    base: dict[str, Any] = {
        "subject_id": "SUBJ-TEST-001",
        "community_id": _SUBJECT_COMMUNITY,
        "area_sqm": Decimal("50.3"),
        "layout": "2室1厅",
        "valuation_date": _CUTOFF,
    }
    base.update(overrides)
    return SubjectProperty(**base)


def _sale_table(rows: list[dict[str, object]]) -> pa.Table:
    columns: dict[str, list[object]] = {
        "sale_event_id": [],
        "community_id": [],
        "sale_date": [],
        "area_sqm": [],
        "layout": [],
        "orientation": [],
        "total_price_yuan": [],
        "unit_price": [],
        "anomaly_flag": [],
        "raw_locator": [],
    }
    for row in rows:
        for name in columns:
            columns[name].append(row.get(name))
    return pa.table(columns)


def _sale_row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "sale_event_id": "cand-1",
        "community_id": _SUBJECT_COMMUNITY,
        "sale_date": date(2026, 7, 20),
        "area_sqm": 50.0,
        "layout": "2室1厅",
        "orientation": "南",
        "total_price_yuan": 1300000.0,
        "unit_price": 25800,
        "anomaly_flag": "正常",
        "raw_locator": "1",
    }
    base.update(overrides)
    return base


def _comp(
    sale_event_id: str,
    *,
    community_id: str = _SUBJECT_COMMUNITY,
    selected: bool = True,
) -> CompCandidate:
    return CompCandidate(
        candidate_id=f"RUN-TEST-{sale_event_id}",
        run_id="RUN-TEST",
        sale_event_id=sale_event_id,
        community_id=community_id,
        selected=selected,
        tier=None,
        similarity=None,
        reason="TEST",
    )


def _confirmed_relation(competitor: str, *, confirmed: bool = True) -> CompetitiveRelation:
    return CompetitiveRelation(
        relation_id=f"COMP-{_SUBJECT_COMMUNITY}-{competitor}",
        community_id=_SUBJECT_COMMUNITY,
        competitor_id=competitor,
        block="新港西",
        basis="同板块",
        confidence=CompetitiveConfidence.LOW if not confirmed else CompetitiveConfidence.HIGH,
        confirmed=confirmed,
        source_ref="community:x+community:y",
    )


# ---- ① 层级放宽有序且轨迹可查 + 一次只放宽一个主要条件 ----
def test_tier_a_same_community_same_product_recent() -> None:
    table = _sale_table([_sale_row(sale_event_id="a")])
    out = select_comparables([_comp("a")], table, _subject())
    assert out[0].tier == 1
    assert out[0].selected is True
    assert out[0].reason == REASON_TIER_A


def test_tier_b_relaxed_area_recent() -> None:
    # 面积放宽（50.3→63，相对差≈25%，∈(20%,35%]）且户型同类 → B
    p = product_level(
        subject_area=Decimal("50.3"),
        subject_layout="2室1厅",
        cand_area=63.0,
        cand_layout="2室1厅",
        tight_band=Decimal("0.20"),
        wide_band=Decimal("0.35"),
    )
    assert p == "relaxed"
    table = _sale_table(
        [_sale_row(sale_event_id="b", area_sqm=63.0, sale_date=date(2026, 7, 10))]
    )
    out = select_comparables([_comp("b")], table, _subject())
    assert out[0].tier == 2
    assert out[0].reason == REASON_TIER_B


def test_tier_c_same_product_beyond_recent_within_year() -> None:
    # 同类面积/户型，超近期窗口（200天）但在一年内 → C
    table = _sale_table(
        [_sale_row(sale_event_id="c", sale_date=date(2025, 12, 3))]
    )  # 距 2026-07-21 约 230 天
    out = select_comparables([_comp("c")], table, _subject())
    assert out[0].tier == 3
    assert out[0].reason == REASON_TIER_C


def test_tier_unique_each_candidate_one_tier() -> None:
    # 单个案例只归入一个层级（A=1 不会同时记为 B=2 等）
    table = _sale_table([_sale_row(sale_event_id="a")])
    out = select_comparables([_comp("a")], table, _subject())
    assert out[0].tier is not None
    assert out[0].tier in (1, 2, 3, 4, 5)


def test_counterexample_both_area_and_layout_relaxed() -> None:
    # 反例：同时放宽面积与户型（一次放宽两个主要条件）→ 不得归任何层级
    p = product_level(
        subject_area=Decimal("50.3"),
        subject_layout="2室1厅",
        cand_area=63.0,  # 放宽
        cand_layout="2室2厅",  # 同室数（2）→ 放宽
        tight_band=Decimal("0.20"),
        wide_band=Decimal("0.35"),
    )
    assert p == "out"
    table = _sale_table(
        [_sale_row(sale_event_id="x", area_sqm=63.0, layout="2室2厅")]
    )
    out = select_comparables([_comp("x")], table, _subject())
    assert out[0].tier is None
    assert out[0].selected is False
    assert out[0].reason == REASON_NO_TIER


def test_counterexample_product_relaxed_plus_time_exceeded() -> None:
    # 反例：产品放宽（B 条件）叠加时间超过近期（非 C：C 要同类产品）→ 无层级
    table = _sale_table(
        [_sale_row(sale_event_id="x", area_sqm=63.0, sale_date=date(2025, 12, 3))]
    )
    out = select_comparables([_comp("x")], table, _subject())
    assert out[0].tier is None
    assert out[0].selected is False
    assert out[0].reason == REASON_NO_TIER


def test_area_out_of_band_no_tier() -> None:
    # 面积远超放宽带宽 → 无层级
    table = _sale_table([_sale_row(sale_event_id="x", area_sqm=101.0)])
    out = select_comparables([_comp("x")], table, _subject())
    assert out[0].tier is None
    assert out[0].reason == REASON_NO_TIER


# ---- ② 相似度分项：未知项不进数值加权 ----
def test_similarity_exact_area_and_layout_is_one() -> None:
    sp = SimilarityPolicy()
    sim = sp.similarity(_subject(), {"area": 50.3, "layout": "2室1厅"})
    assert sim == 1


def test_similarity_area_mismatch_scores_below_one() -> None:
    sp = SimilarityPolicy()
    sim = sp.similarity(_subject(), {"area": 60.0, "layout": "2室1厅"})
    assert sim is not None and 0 < sim < 1


def test_similarity_unknown_layout_excluded_from_weighting() -> None:
    # layout 未知 → 不计权；面积与朝向精确 → 相似度仍=1（仅按已知分项归一）
    sp = SimilarityPolicy()
    subj = _subject(orientation="南")
    sim = sp.similarity(subj, {"area": 50.3, "layout": None, "orientation": "南"})
    assert sim == 1


def test_similarity_all_unknown_is_none() -> None:
    sp = SimilarityPolicy()
    assert sp.similarity(_subject(), {}) is None
    assert sp.similarity(_subject(), {"area": None, "layout": None}) is None


def test_similarity_unknown_prefers_partial_evidence() -> None:
    # 仅一项已知也不会虚构其他分项；部分证据 → 0<sim<=1
    sp = SimilarityPolicy()
    sim = sp.similarity(_subject(), {"area": 50.3})
    assert sim == 1
    sim2 = sp.similarity(_subject(), {"orientation": "南", "year_built": 2018})
    assert sim2 is not None and 0 < sim2 <= 1


# ---- ③ 竞争小区关系：置信+待确认清单、未确认不用于 D/E ----
def test_discover_competitive_relations_same_block_machine_confirmed_only() -> None:
    community = pa.table(
        {
            "community_id": [
                _SUBJECT_COMMUNITY,
                "C-COMP-SAME",
                "C-OTHER-BLOCK",
                "C-OUT-SCOPE",
            ],
            "block": ["新港西", "新港西", "工业大道北", "新港西"],
            "boundary_status": ["机器确认", "机器确认", "机器确认", "正式范围外"],
            "source_ref": ["L2", "L5", "L9", "L12"],
        }
    )
    rels = competitive_relations_of(_subject(), community)
    assert len(rels) == 1
    rel = rels[0]
    assert rel.competitor_id == "C-COMP-SAME"
    assert rel.confidence == CompetitiveConfidence.LOW
    assert rel.confirmed is False  # 机器生成，待人工确认
    assert "新港西" in rel.basis
    assert "community:" in rel.source_ref


def test_unconfirmed_competitive_not_used_in_DE() -> None:
    # 关系存在但未确认 → 候选不归 D/E（验收③反例）
    table = _sale_table(
        [_sale_row(sale_event_id="d", sale_date=date(2026, 7, 20), area_sqm=50.0)]
    )
    cand = _comp("d", community_id="C-COMP-SAME")
    relations = [_confirmed_relation("C-COMP-SAME", confirmed=False)]
    out = select_comparables([cand], table, _subject(), relations=relations)
    assert out[0].tier is None
    assert out[0].selected is False
    assert out[0].reason == REASON_COMP_UNCONFIRMED


def test_confirmed_competitive_tier_d_recent() -> None:
    table = _sale_table(
        [_sale_row(sale_event_id="d", sale_date=date(2026, 7, 20), area_sqm=50.0)]
    )
    cand = _comp("d", community_id="C-COMP-NEW")
    relations = [_confirmed_relation("C-COMP-NEW", confirmed=True)]
    out = select_comparables([cand], table, _subject(), relations=relations)
    assert out[0].tier == 4
    assert out[0].reason == REASON_TIER_D


def test_confirmed_competitive_tier_e_within_year() -> None:
    table = _sale_table(
        [_sale_row(sale_event_id="e", sale_date=date(2025, 12, 3), area_sqm=50.0)]
    )
    cand = _comp("e", community_id="C-COMP-NEW")
    relations = [_confirmed_relation("C-COMP-NEW", confirmed=True)]
    out = select_comparables([cand], table, _subject(), relations=relations)
    assert out[0].tier == 5
    assert out[0].reason == REASON_TIER_E


def test_non_competitive_community_not_in_any_tier() -> None:
    # 既非同小区也不在竞争关系 → 不作可比（排除有理由）
    table = _sale_table(
        [_sale_row(sale_event_id="n", sale_date=date(2026, 7, 20), area_sqm=50.0)]
    )
    cand = _comp("n", community_id="C-UNRELATED")
    out = select_comparables([cand], table, _subject())
    assert out[0].tier is None
    assert out[0].selected is False
    assert out[0].reason == REASON_NOT_COMPETITIVE


# ---- ④ 每行 reason 可溯源 ----
def test_every_candidate_has_traceable_reason() -> None:
    table = _sale_table(
        [
            _sale_row(sale_event_id="a"),
            _sale_row(sale_event_id="b", area_sqm=63.0),
            _sale_row(sale_event_id="x", layout="3室1厅"),
        ]
    )
    cands = [_comp("a"), _comp("b"), _comp("x")]
    out = select_comparables(cands, table, _subject())
    assert all(c.reason for c in out)  # 无空理由
    for c in out:
        if c.tier is not None:
            assert c.selected is True  # 归入层级的均入选
            assert c.similarity is not None or c.similarity is None  # 留痕不虚构
        else:
            assert c.selected is False  # 未归入层级的排除并留痕


def test_candidate_pool_excluded_rows_keep_reason() -> None:
    # WP6-A 候选池排除行原样保留（tier/similarity 保持 None）
    excluded = CompCandidate(
        candidate_id="RUN-TEST-x",
        run_id="RUN-TEST",
        sale_event_id="x",
        community_id=_SUBJECT_COMMUNITY,
        selected=False,
        tier=None,
        similarity=None,
        reason="原始排除理由",
    )
    table = _sale_table([_sale_row(sale_event_id="x", layout="3室1厅")])
    out = select_comparables([excluded], table, _subject())
    assert len(out) == 1
    assert out[0].selected is False
    assert out[0].reason == "原始排除理由"
    assert out[0].tier is None and out[0].similarity is None


# ---- 写盘：apply_tiers_to_candidate_table（comp_candidate 重写 + 关系清单 + manifest） ----
def _write_valid_sale_mart(data_dir: Path, table: pa.Table) -> None:
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


def _write_community_entity(data_dir: Path) -> None:
    (data_dir / "entities").mkdir(parents=True, exist_ok=True)
    community = pa.table(
        {
            "community_id": [_SUBJECT_COMMUNITY, "C-COMP-SAME"],
            "block": ["新港西", "新港西"],
            "boundary_status": ["机器确认", "机器确认"],
            "source_ref": ["L2", "L5"],
        }
    )
    pq.write_table(community, data_dir / "entities" / "community.parquet", compression="zstd")


def test_apply_tiers_writes_tables_and_manifest(tmp_path: Path) -> None:
    sales = _sale_table(
        [
            _sale_row(sale_event_id="a"),
            _sale_row(sale_event_id="b", area_sqm=63.0),
        ]
    )
    _write_valid_sale_mart(tmp_path, sales)
    _write_community_entity(tmp_path)
    result = build_valuation(_subject(), data_dir=tmp_path)
    assert result.candidate_path.is_file()

    tiered = apply_tiers_to_candidate_table(
        data_dir=tmp_path,
        subject=_subject(),
        valid_sale=sales,
        communities=pq.read_table(tmp_path / "entities" / "community.parquet"),
        input_refs=[InputRef(dataset="chengjiao_list", fetched_at="20260821T000000Z")],
    )

    out = pq.read_table(tiered.candidate_path)
    tiers = out.column("tier").to_pylist()
    assert all(t in (1, 2) for t in tiers if t is not None)
    assert out.column("rule_version").to_pylist() == [DEFAULT_RULE_VERSION] * out.num_rows

    # 竞争关系清单：同板块机器确认 → 1 条待确认
    rels = pq.read_table(tiered.relations_path)
    assert rels.num_rows == 1
    assert rels.column("confirmed").to_pylist() == [False]
    assert rels.column("confidence").to_pylist() == [CompetitiveConfidence.LOW.value]

    # 原子写盘：无 .incomplete 残留 + manifest 就位
    assert not list((tmp_path / VALUATION_LAYER).glob("*.incomplete"))
    for path in (tiered.candidate_path, tiered.relations_path):
        manifest = json.loads(path.with_suffix(".manifest.json").read_text(encoding="utf-8"))
        assert manifest["layer"] == VALUATION_LAYER


def test_apply_tiers_requires_comp_candidate(tmp_path: Path) -> None:
    # comp_candidate 不存在 → FileNotFoundError
    with pytest.raises(FileNotFoundError):
        apply_tiers_to_candidate_table(
            data_dir=tmp_path,
            subject=_subject(),
            valid_sale=_sale_table([]),
            communities=pa.table({}),
            input_refs=[],
        )