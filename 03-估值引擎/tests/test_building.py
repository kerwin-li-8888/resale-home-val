"""WP5-C: building 楼栋弱实体匹配。

对照 WP5-C 验收标准：
① 每个 building 归属某 community_id；
② 缺失字段用 UNKNOWN 不用 0（year_built/total_floors/has_elevator 未知=None）；
③ 弱匹配输出置信状态（高/中/低/未知）；
④ 低置信（LOW）进待复核、不自动合并（落表 row 不含 LOW）；
⑤ ruff/mypy/pytest 通过（质量门禁）。

数据源：链家成交列表快照（SRC-007，dataset=chengjiao_list）解析出的
``LianjiaRecord``，其 ``楼层(共N层)/YYYY年/塔楼|板楼`` 提供楼栋属性证据。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from compsval.entities.building import (
    BUILDING_FILENAME,
    BUILDING_TABLE,
    MatchConfidence,
    build_building_rows,
    building_id_of,
    building_schema,
    building_table,
    record_to_evidence,
    write_building_entity,
)
from compsval.entities.candidates import candidates_all
from compsval.entities.community import ENTITIES_LAYER, community_id_of
from compsval.ingest.manifests import read_derived_manifest
from compsval.ingest.parsers.lianjia import LianjiaRecord

_SNAPSHOT_ID = "20260821T031400Z"


def _record(
    community: str,
    *,
    layout: str = "2室1厅",
    total_wan: str = "258",
    area_sqm: str = "84.04",
    floor: str = "中楼层",
    total_floors: int | None = 31,
    year_built: int | None = 2009,
    raw_start_line: int = 1,
) -> LianjiaRecord:
    return LianjiaRecord(
        community=community,
        layout=layout,
        area_sqm=Decimal(area_sqm),
        deal_date=date(2026, 7, 21),
        total_price_yuan=(Decimal(total_wan) * 10000).to_integral_value(),
        original_price_text=f"{total_wan}万",
        floor=floor,
        total_floors=total_floors,
        year_built=year_built,
        building_type="塔楼",
        raw_start_line=raw_start_line,
    )


def _first_candidate() -> Any:
    return candidates_all()[0]


def _canonical_name() -> str:
    return str(_first_candidate().standard_name)


def _canonical_key() -> str:
    return str(_first_candidate().source_key)


# ---------------------------------------------------------------------------
# 验收①：每个 building 归属某 community_id；building_id 生成稳定
# ---------------------------------------------------------------------------


def test_building_id_uses_community_key_and_seq() -> None:
    assert building_id_of("2811052010", 1) == "B-2811052010-1"
    assert building_id_of("2811052010", 2) == "B-2811052010-2"


def test_every_building_row_has_a_community_id() -> None:
    records = [
        _record(_canonical_name(), raw_start_line=6),
        _record("不存在的小区A", raw_start_line=12),
    ]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    table = building_table(rows)
    ids = table.column("community_id").to_pylist()
    assert ids  # 至少有一个归属
    assert all(v is not None and str(v).startswith("C-") for v in ids)


# ---------------------------------------------------------------------------
# 验收②：未知字段用 UNKNOWN/None 不用 0
# ---------------------------------------------------------------------------


def test_unknown_fields_use_none_not_zero() -> None:
    # 无年代、无总层数 → 未知用 None；不臆造 has_elevator
    records = [
        _record(
            _canonical_name(),
            total_floors=None,
            year_built=None,
            raw_start_line=9,
        )
    ]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    table = building_table(rows)
    assert table.column("year_built").to_pylist() == [None]
    assert table.column("total_floors").to_pylist() == [None]
    assert table.column("has_elevator").to_pylist() == [None]
    # match_confidence 非空；source_ref 可溯源（行号）
    assert table.column("match_confidence").to_pylist() != [None]
    assert all("链家快照" in ref for ref in table.column("source_ref").to_pylist())


def test_elevator_only_inferred_when_floors_known_and_high() -> None:
    assert (
        record_to_evidence(
            _record(_canonical_name(), total_floors=31, year_built=2000, raw_start_line=1),
            _SNAPSHOT_ID,
        ).has_elevator
        is True
    )
    # 7 层及以下 / 未知 → 不臆造 True（None）
    assert (
        record_to_evidence(
            _record(_canonical_name(), total_floors=7, year_built=2000, raw_start_line=1),
            _SNAPSHOT_ID,
        ).has_elevator
        is None
    )
    assert (
        record_to_evidence(
            _record(_canonical_name(), total_floors=None, year_built=2000, raw_start_line=1),
            _SNAPSHOT_ID,
        ).has_elevator
        is None
    )


# ---------------------------------------------------------------------------
# 验收③：弱匹配输出置信状态（高/中/低/未知）
# ---------------------------------------------------------------------------


def test_confidence_high_on_canonical_hit() -> None:
    records = [_record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1)]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert len(rows) == 1
    assert rows[0].match_confidence is MatchConfidence.HIGH
    assert rows[0].community_id == community_id_of(_canonical_key())


def test_confidence_medium_on_approx_hit_with_features() -> None:
    # 名称近似命中（双向包含）且具楼栋特征 → MEDIUM（近似命中，不升为 HIGH）
    base = _canonical_name()
    approx_name = base + "XX小区" if len(base) >= 3 else "近似小区"
    records = [_record(approx_name, total_floors=31, year_built=2009, raw_start_line=1)]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert len(rows) == 1
    assert rows[0].match_confidence is MatchConfidence.MEDIUM
    assert "近似命中" in rows[0].source_ref
    # 精确命中的同类特征不改变 HIGH 判定
    records_exact = [_record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1)]
    rows_exact, _ = build_building_rows(records_exact, snapshot_id=_SNAPSHOT_ID)
    assert rows_exact[0].match_confidence is MatchConfidence.HIGH


def test_confidence_enum_has_all_four_values() -> None:
    assert {m.value for m in MatchConfidence} == {"高", "中", "低", "未知"}


# ---------------------------------------------------------------------------
# 验收④：低置信（LOW）进待复核、不自动合并（不落表）
# ---------------------------------------------------------------------------


def test_low_confidence_goes_to_review_not_table() -> None:
    # 名称近似命中候选（双向包含）但无楼栋特征 → LOW，只出现在待复核清单
    base = _canonical_name()
    approx_name = base + "XX小区" if len(base) >= 3 else "近似小区"
    records = [_record(approx_name, total_floors=None, year_built=None, raw_start_line=15)]
    rows, low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert rows == []  # LOW 不落表
    assert len(low) == 1
    assert low[0].match_confidence is MatchConfidence.LOW
    assert "不自动合并" in low[0].source_ref


def test_unmatched_community_not_emitted() -> None:
    records = [_record("完全不存在的小区名", total_floors=20, year_built=2010, raw_start_line=3)]
    rows, low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert rows == []
    assert low == []


def test_building_rows_never_contain_low_confidence() -> None:
    # 落表行（HIGH/MEDIUM）与低置信（LOW）严格分离
    records = [
        _record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1),  # HIGH
        _record(
            _canonical_name() + "XX", total_floors=None, year_built=None, raw_start_line=2
        ),  # LOW
    ]
    rows, low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert all(r.match_confidence is not MatchConfidence.LOW for r in rows)
    assert all(low_row.match_confidence is MatchConfidence.LOW for low_row in low)


# ---------------------------------------------------------------------------
# attribute 映射：same 小区不同楼栋指纹 → 不同 building 行
# ---------------------------------------------------------------------------


def test_same_community_different_building_fingerprints_split() -> None:
    records = [
        _record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1),
        _record(_canonical_name(), total_floors=18, year_built=2009, raw_start_line=4),
    ]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert len(rows) == 2
    assert rows[0].building_id != rows[1].building_id
    # 建筑名按楼栋N 编号，且各自保存属性
    names = sorted(r.building_name for r in rows)
    assert names == ["楼栋1", "楼栋2"]


def test_same_community_same_fingerprint_merged() -> None:
    records = [
        _record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1),
        _record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=4),
    ]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    assert len(rows) == 1
    assert rows[0].total_floors == 31
    assert rows[0].year_built == 2009
    # 溯源保留首次出现行号
    assert "行1" in rows[0].source_ref


# ---------------------------------------------------------------------------
# 表结构与原子写盘（parquet + DerivedManifest）
# ---------------------------------------------------------------------------


def test_building_schema_matches_model_fields() -> None:
    schema = building_schema()
    columns = [f.name for f in schema]
    for field in (
        "building_id",
        "community_id",
        "building_name",
        "year_built",
        "total_floors",
        "has_elevator",
    ):
        assert field in columns
    # 弱匹配置信与溯源扩展字段齐全
    assert "match_confidence" in columns
    assert "source_id" in columns
    assert "source_ref" in columns


def test_write_building_entity_writes_table_and_manifest(tmp_path: Path) -> None:
    records = [_record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1)]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    table = building_table(rows)
    path = write_building_entity(
        table,
        data_dir=tmp_path,
        inputs=[
            __import__("compsval.ingest.manifests", fromlist=["InputRef"]).InputRef(  # noqa: E501
                dataset="chengjiao_list", fetched_at=_SNAPSHOT_ID
            )
        ],
        notes="WP5-C test",
    )
    assert path.name == BUILDING_FILENAME
    assert path.parent.name == ENTITIES_LAYER
    assert pq.read_table(path).num_rows == 1

    manifest = read_derived_manifest(path)
    assert manifest.layer == ENTITIES_LAYER
    assert manifest.table == BUILDING_TABLE
    assert manifest.row_count == 1
    assert manifest.inputs[0].dataset == "chengjiao_list"


def test_table_rows_all_trace_to_lianjia_snapshot() -> None:
    records = [
        _record(_canonical_name(), total_floors=31, year_built=2009, raw_start_line=1),
        _record(_canonical_name(), total_floors=18, year_built=2009, raw_start_line=7),
    ]
    rows, _low = build_building_rows(records, snapshot_id=_SNAPSHOT_ID)
    table = building_table(rows)
    refs = table.column("source_ref").to_pylist()
    assert all(_SNAPSHOT_ID in ref for ref in refs)
    assert all("链家快照" in ref for ref in refs)
