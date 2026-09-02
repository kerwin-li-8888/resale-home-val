"""valid_sale 属性回填与估值链读取打通（excel-attribute-enrichment）离线测试。

覆盖：身份键索引（命中/冲突/未映射）、enrich_valid_sale（回填可溯源/无匹配
留 None）、enrich_attributes_mart（显式输出 + manifest 血缘 + 缺列显式报错）、
comparable._candidate_attrs 属性读取与缺列回归、backtest._build_subject 属性
带入与无未来泄漏。不触网，不触碰真实湖。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import SubjectProperty
from compsval.entities.backfill import CommunityIdLookup
from compsval.ingest.attribute_enrich import (
    ENRICH_COLUMNS,
    ENRICH_REF_COLUMN,
    build_attribute_index,
    enrich_attributes_mart,
    enrich_valid_sale,
    identity_key,
)
from compsval.ingest.xlsx_stage import ORDINARY_FILENAME
from compsval.valuation.backtest import _build_subject
from compsval.valuation.comparable import (
    SimilarityPolicy,
    _candidate_attrs,
)

_LOOKUP = CommunityIdLookup(
    canonical={"示例小区121": ("C-XXXX0048", "测试权威表命中")},
    alias_consistent={},
    blocked={},
)


def _identity(community_id: str, area: float, day: str, total: int) -> str:
    return identity_key(community_id, area, date.fromisoformat(day), total)


# ---------------------------------------------------------------------------
# 身份键与索引
# ---------------------------------------------------------------------------


def test_identity_key_none_when_fields_missing() -> None:
    assert identity_key(None, 32.09, date(2023, 12, 17), 700000) is None
    assert identity_key("C-1", None, date(2023, 12, 17), 700000) is None
    assert identity_key("C-1", 32.09, None, 700000) is None
    assert identity_key("C-1", 32.09, date(2023, 12, 17), None) is None
    key = _identity("C-1", 32.09, "2023-12-17", 700000)
    assert key == "C-1|32.09|2023-12-17|700000"


def test_build_attribute_index_hit_conflict_unmapped() -> None:
    table = pa.table(
        {
            "community_name": ["示例小区121", "示例小区121", "未知小区", "示例小区121"],
            "transaction_area_sqm": ["32.09", "32.09", "50.00", "88.50"],
            "sale_date": ["2023-12-17", "2023-12-17", "2023-12-18", "2023-11-13"],
            "total_price_yuan": ["700000", "700000", "1000000", "300000"],
            "floor_bucket": ["高楼层", "高楼层", "中楼层", None],
            "total_floors": [9, 9, 18, None],
            "year_built": [None, 2005, 2000, None],
            "has_elevator": [True, True, True, None],
            "decoration_norm": ["精装", "精装", "简装", None],
        }
    )
    index, conflicts, unmapped = build_attribute_index(table, _LOOKUP)
    assert unmapped == 1  # 未知小区不入索引（不臆测归并）
    assert conflicts == 1  # 同身份键两行
    key = _identity("C-XXXX0048", 32.09, "2023-12-17", 700000)
    assert key in index
    # 丰富度取一：第二行 year_built 已知 → 保留第二行
    assert index[key]["year_built"] == 2005


# ---------------------------------------------------------------------------
# enrich_valid_sale
# ---------------------------------------------------------------------------


def _valid_sale_table() -> pa.Table:
    return pa.table(
        {
            "sale_event_id": ["E1", "E2"],
            "community_id": ["C-XXXX0048", "C-XXXX0191"],
            "area_sqm": [32.09, 55.00],
            "sale_date": [date(2023, 12, 17), date(2023, 12, 18)],
            "total_price_yuan": [700000, 1200000],
            "unit_price": [21814, 21818],
        }
    )


def test_enrich_valid_sale_matched_and_unmatched() -> None:
    index = {
        _identity("C-XXXX0048", 32.09, "2023-12-17", 700000): {
            "total_floors": 9,
            "year_built": 2005,
            "has_elevator": True,
            "decoration_norm": "精装",
        }
    }
    enriched, stats = enrich_valid_sale(_valid_sale_table(), index, source_run_id="R")
    assert stats.matched == 1 and stats.unmatched == 1
    assert enriched.column("total_floors").to_pylist() == [9, None]
    assert enriched.column("has_elevator").to_pylist() == [True, None]
    assert enriched.column("decoration_norm").to_pylist() == ["精装", None]
    refs = enriched.column(ENRICH_REF_COLUMN).to_pylist()
    assert refs[0] is not None and refs[0].startswith("lianjia_ext@R|")
    assert refs[1] is None
    # 输入表不被修改
    assert ENRICH_COLUMNS[0] not in _valid_sale_table().column_names
    assert stats.coverage_after["has_elevator"] == 0.5


# ---------------------------------------------------------------------------
# enrich_attributes_mart：显式输出 + 血缘 + 显式报错
# ---------------------------------------------------------------------------


def _make_lake(tmp_path: Path, *, with_attribute_columns: bool = True) -> Path:
    lake = tmp_path / "lake"
    marts = lake / "marts"
    marts.mkdir(parents=True)
    vs = _valid_sale_table()
    vs_path = marts / "valid_sale.parquet"
    pq.write_table(vs, vs_path)
    (marts / "valid_sale.manifest.json").write_text(
        '{"manifest_version":1,"layer":"marts","table":"valid_sale",'
        '"built_at":"2026-01-01T00:00:00Z","row_count":2,'
        '"inputs":[{"dataset":"chengjiao_list","fetched_at":"20260821T000000Z"}],'
        '"package_version":"0.1.0","notes":"test"}',
        encoding="utf-8",
    )
    run_dir = lake / "staged" / "lianjia_ext" / "runs" / "run_V2"
    run_dir.mkdir(parents=True)
    schema = pa.schema(
        [
            *[pa.field(name, pa.string()) for name in (
                "community_name", "transaction_area_sqm", "sale_date",
                "total_price_yuan",
            )],
            *[
                pa.field(name, field_type)
                for name, field_type in (
                    ("total_floors", pa.int64()),
                    ("year_built", pa.int64()),
                    ("has_elevator", pa.bool_()),
                    ("decoration_norm", pa.string()),
                )
            ],
        ]
    )
    if with_attribute_columns:
        columns = {
            "community_name": ["示例小区121"],
            "transaction_area_sqm": ["32.09"],
            "sale_date": ["2023-12-17"],
            "total_price_yuan": ["700000"],
            "total_floors": [9],
            "year_built": [2005],
            "has_elevator": [True],
            "decoration_norm": ["精装"],
        }
    else:
        columns = {
            "community_name": ["示例小区121"],
            "transaction_area_sqm": ["32.09"],
            "sale_date": ["2023-12-17"],
            "total_price_yuan": ["700000"],
        }
    table = pa.table(
        {name: pa.array(values, type=schema.field(name).type) for name, values in columns.items()},
        schema=schema if with_attribute_columns else None,
    )
    pq.write_table(table, run_dir / ORDINARY_FILENAME)
    entities = lake / "entities"
    entities.mkdir()
    pq.write_table(
        pa.table(
            {
                "community_id": ["C-XXXX0048"],
                "standard_name": ["示例小区121"],
            }
        ),
        entities / "community.parquet",
    )
    return lake


def test_enrich_attributes_mart_writes_explicit_out_with_lineage(
    tmp_path: Path,
) -> None:
    lake = _make_lake(tmp_path)
    out = tmp_path / "out" / "valid_sale_enriched.parquet"
    path, stats = enrich_attributes_mart(data_dir=lake, out_path=out, run_id="V2")
    assert path == out
    assert stats.matched == 1 and stats.rows_total == 2
    assert stats.excel_rows_unmapped_community == 0
    enriched = pq.read_table(out)
    assert "total_floors" in enriched.column_names
    assert enriched.column("year_built").to_pylist() == [2005, None]
    # 既有 mart 未被改写（正式切换属基线确认后动作）
    assert pq.read_table(lake / "marts" / "valid_sale.parquet").num_rows == 2
    assert "total_floors" not in pq.read_table(
        lake / "marts" / "valid_sale.parquet"
    ).column_names
    # 血缘：继承原 inputs + 追加 v2 引用；notes 记录命中率
    from compsval.ingest.manifests import read_derived_manifest

    manifest = read_derived_manifest(out)
    datasets = [item.dataset for item in manifest.inputs]
    assert "chengjiao_list" in datasets
    assert "lianjia_ext_ordinary_residential" in datasets
    assert "matched=1/2" in (manifest.notes or "")


def test_enrich_attributes_mart_explicit_errors(tmp_path: Path) -> None:
    lake_without_attrs = _make_lake(tmp_path, with_attribute_columns=False)
    with pytest.raises(KeyError):
        enrich_attributes_mart(
            data_dir=lake_without_attrs, out_path=tmp_path / "o.parquet", run_id="V2"
        )
    with pytest.raises(FileNotFoundError):
        enrich_attributes_mart(
            data_dir=_make_lake(tmp_path / "other"),
            out_path=tmp_path / "o2.parquet",
            run_id="MISSING",
        )


# ---------------------------------------------------------------------------
# 估值链读取：comparable + backtest
# ---------------------------------------------------------------------------


def _subject() -> SubjectProperty:
    return SubjectProperty(
        subject_id="S1",
        community_id="C-XXXX0048",
        area_sqm=32.09,
        layout="1室1厅",
        valuation_date=date(2024, 1, 1),
        has_elevator=True,
        orientation="南",
        year_built=2005,
    )


def test_enrich_valid_sale_is_idempotent_on_enriched_input() -> None:
    index = {
        _identity("C-XXXX0048", 32.09, "2023-12-17", 700000): {
            "total_floors": 9,
            "year_built": 2005,
            "has_elevator": True,
            "decoration_norm": "精装",
        }
    }
    once, stats_once = enrich_valid_sale(_valid_sale_table(), index, source_run_id="R")
    twice, stats_twice = enrich_valid_sale(once, index, source_run_id="R")
    assert twice.equals(once)
    assert stats_twice.matched == stats_once.matched == 1
    assert not any(
        name in _valid_sale_table().column_names for name in (*ENRICH_COLUMNS, ENRICH_REF_COLUMN)
    )


def test_candidate_attrs_reads_enriched_columns() -> None:
    enriched, _stats = enrich_valid_sale(
        _valid_sale_table(),
        {
            _identity("C-XXXX0048", 32.09, "2023-12-17", 700000): {
                "total_floors": 9,
                "year_built": 2005,
                "has_elevator": True,
                "decoration_norm": "精装",
            }
        },
        source_run_id="R",
    )
    attrs = _candidate_attrs(enriched, "E1")
    assert attrs["elevator"] is True
    assert attrs["year_built"] == 2005
    assert attrs["orientation"] is None  # 朝向原文列本例未回填（valid_sale 既有语义）
    # 相似度：电梯/年代/朝向分项进入加权（未知项才移出分母）
    sim = SimilarityPolicy().similarity(_subject(), {**attrs, "orientation": "南"})
    assert sim is not None and sim > 0


def test_candidate_attrs_without_columns_is_legacy_behavior() -> None:
    attrs = _candidate_attrs(_valid_sale_table(), "E1")
    assert attrs["floor"] is None
    assert attrs["elevator"] is None
    assert attrs["year_built"] is None


def test_build_subject_carries_target_attributes_without_leakage() -> None:
    row = {
        "sale_event_id": "E1",
        "community_id": "C-XXXX0048",
        "area_sqm": 32.09,
        "layout": "1室1厅",
        "unit_price": 21814,
        "has_elevator": True,
        "orientation": "南",
        "year_built": 2005,
        "total_floors": 9,
    }
    subject, reason = _build_subject(row, date(2024, 1, 1))
    assert reason is None and subject is not None
    assert subject.has_elevator is True
    assert subject.orientation == "南"
    assert subject.year_built == 2005
    assert subject.total_floors == 9
    assert subject.floor is None  # 无精确楼层，不臆造


def test_build_subject_without_attribute_columns_unchanged() -> None:
    row = {
        "sale_event_id": "E1",
        "community_id": "C-XXXX0048",
        "area_sqm": 32.09,
        "layout": "1室1厅",
        "unit_price": 21814,
    }
    subject, reason = _build_subject(row, date(2024, 1, 1))
    assert reason is None and subject is not None
    assert subject.has_elevator is None
    assert subject.orientation == "UNKNOWN"
    assert subject.year_built is None
    assert subject.total_floors is None


def test_build_subject_invalid_attributes_stay_none_not_skip() -> None:
    row = {
        "sale_event_id": "E1",
        "community_id": "C-XXXX0048",
        "area_sqm": 32.09,
        "layout": "1室1厅",
        "unit_price": 21814,
        "year_built": "乱文本",
        "has_elevator": None,
    }
    subject, _reason = _build_subject(row, date(2024, 1, 1))
    assert subject is not None
    assert subject.year_built is None
    assert subject.has_elevator is None
