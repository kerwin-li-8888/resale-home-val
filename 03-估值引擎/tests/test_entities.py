"""WP5-A: community 小区实体权威表（候选名录骨架）+ 目录注册。

对照 WP5-A 验收标准：
① community 实体模型字段与数据字典 §3.3 一致，缺失语义用 UNKNOWN 不用 0；
② 实体权威表每行可追溯到来源（名录节号+行号 source_ref）；
③ boundary_status 按 DATA-001-C 边界三分（机器确认/边界待定/正式范围外）；
④ 坐标若记录必带 coordinate_system（Community 模型校验器，§7.3 无声明不转换）；
⑤ ``compsval catalog`` 列出实体表（entities 层目录注册）；
⑥ ruff/mypy/pytest 通过（质量门禁，见 self-check）。

骨架期仅转录候选名录中**具来源 ID** 的行（237 个，含 2026-08-22 补数新增
拾光里/示例小区130 2 小区）；"ID 待补" 5 个候选
（晓园花苑等）不进入权威表，见 :data:`ID_PENDING_EXCLUDED`。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from compsval import cli
from compsval.contract.models import BoundaryStatus, Community, CoordinateSystem
from compsval.entities.candidates import (
    ID_PENDING_EXCLUDED,
    CandidateCommunity,
    candidates_all,
)
from compsval.entities.community import (
    CATALOG_INPUT,
    COMMUNITY_FILENAME,
    COMMUNITY_TABLE,
    ENTITIES_LAYER,
    SKELETON_SOURCE_ID,
    build_community_entity,
    community_id_of,
    community_schema,
    community_table,
    to_community,
    write_community_entity,
)
from compsval.ingest.manifests import read_derived_manifest

_BOUNDARY_VALUES = {
    BoundaryStatus.MACHINE_CONFIRMED.value,
    BoundaryStatus.BOUNDARY_PENDING.value,
    BoundaryStatus.OUT_OF_SCOPE.value,
}


def _candidate(index: int = 0) -> CandidateCommunity:
    return candidates_all()[index]


# ---------------------------------------------------------------------------
# 候选名录转录（验收②基础）：行数与来源 ID
# ---------------------------------------------------------------------------


def test_candidates_count_matches_implementable_set() -> None:
    candidates = candidates_all()
    # 12 板块 × 每板块 20 行 − 东泊南 ID 待补 5 行 = 235（具来源 ID 可实施集合）
    # + 2026-08-22 补数新增 2 小区（拾光里 §2.4 行1、示例小区130 §2.9 行1）= 237
    assert len(candidates) == 237
    assert len(ID_PENDING_EXCLUDED) == 5
    assert all(c.source_key for c in candidates)
    assert all(c.source_ref.startswith("候选小区名录-V0.1.md §") for c in candidates)


def test_candidates_cover_all_12_blocks_in_catalog_order() -> None:
    blocks = [c.block for c in candidates_all()]
    assert list(dict.fromkeys(blocks)) == [
        "工业大道北",
        "工业大道南",
        "江南西",
        "宝岗",
        "昌岗路",
        "南洲",
        "江燕路",
        "前进路",
        "滨江西",
        "滨江中",
        "东泊南",
        "新港西",
    ]


# ---------------------------------------------------------------------------
# 验收①：Community 模型字段与数据字典 §3.3 一致、缺失用 UNKNOWN 不用 0
# ---------------------------------------------------------------------------


def test_community_model_field_set_matches_dictionary_section_33() -> None:
    schema = Community.model_json_schema()
    props = schema["properties"]
    required = schema["required"]
    for field in (
        "community_id",
        "standard_name",
        "block",
        "address",
        "latitude",
        "longitude",
        "coordinate_system",
        "boundary_status",
        "source_id",
        "source_key",
        "source_ref",
        "notes",
    ):
        assert field in props
    assert "community_id" in required
    assert "standard_name" in required
    assert "boundary_status" in required


def test_community_model_unknown_uses_none_not_zero() -> None:
    community = Community(
        community_id="C-XXXX0027",
        standard_name="示例小区154",
        block="东泊南",
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
        source_id=SKELETON_SOURCE_ID,
        source_key="2811007172",
        source_ref="候选小区名录-V0.1.md §2.11 东泊南(74_10076) 行15",
    )
    # 未知坐标/地址用 None/UNKNOWN，不得写成 0
    assert community.latitude is None
    assert community.longitude is None
    assert community.address == "UNKNOWN"
    assert community.coordinate_system is CoordinateSystem.UNKNOWN


# ---------------------------------------------------------------------------
# 验收②：实体权威表每行可追溯到来源（名录行/样本）
# ---------------------------------------------------------------------------


def test_to_community_maps_every_field_and_stays_traceable() -> None:
    candidate = _candidate()
    community = to_community(candidate)

    assert community.community_id == community_id_of(candidate.source_key)
    assert community.standard_name == candidate.standard_name
    assert community.block == candidate.block
    assert community.address == candidate.address
    assert community.boundary_status is candidate.boundary
    assert community.source_id == SKELETON_SOURCE_ID
    assert community.source_key == candidate.source_key
    assert community.source_ref == candidate.source_ref
    assert community.notes == candidate.notes
    # 名录节号+行号可溯源（验收②）
    assert "候选小区名录-V0.1.md §" in community.source_ref


def test_every_table_row_traces_to_a_catalog_row(tmp_path: Path) -> None:
    path = build_community_entity(data_dir=tmp_path)
    table = pq.read_table(path)
    source_refs = table.column("source_ref").to_pylist()
    assert len(source_refs) == len(set(source_refs))  # 每行唯一溯源
    assert all("候选小区名录-V0.1.md §" in ref for ref in source_refs)


# ---------------------------------------------------------------------------
# 验收③：boundary_status 按 DATA-001-C 边界三分
# ---------------------------------------------------------------------------


def test_boundary_status_uses_the_three_way_split_only() -> None:
    table = community_table([to_community(c) for c in candidates_all()])
    values = table.column("boundary_status").to_pylist()
    assert set(values) <= _BOUNDARY_VALUES
    # 三分各自真实出现，且"正式范围外/边界待定"确实从名录判定落地
    assert set(values) == _BOUNDARY_VALUES


def test_boundary_status_flagged_rows_match_catalog_notes() -> None:
    pending = [c for c in candidates_all() if c.boundary is BoundaryStatus.BOUNDARY_PENDING]
    out = [c for c in candidates_all() if c.boundary is BoundaryStatus.OUT_OF_SCOPE]
    assert pending  # 道路级/板块级命名等
    assert out  # 示例小区025、示例小区150
    for c in pending + out:
        assert c.notes is not None  # 边界判定必须有名录备注支撑


# ---------------------------------------------------------------------------
# 验收④：坐标若记录必带 coordinate_system（§7.3 无声明不转换）
# ---------------------------------------------------------------------------


def test_coordinates_require_coordinate_system() -> None:
    with pytest.raises(ValidationError):
        Community(
            community_id="C-T",
            standard_name="测试",
            block="宝岗",
            latitude=23.1,
            longitude=113.26,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            source_id=SKELETON_SOURCE_ID,
            source_key="T",
            source_ref="测试",
        )
    # 显式声明坐标系 → 合法
    Community(
        community_id="C-T",
        standard_name="测试",
        block="宝岗",
        latitude=23.1,
        longitude=113.26,
        coordinate_system=CoordinateSystem.WGS84,
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
        source_id=SKELETON_SOURCE_ID,
        source_key="T",
        source_ref="测试",
    )


def test_skeleton_never_fabricates_coordinates() -> None:
    table = community_table([to_community(c) for c in candidates_all()])
    assert all(v is None for v in table.column("latitude").to_pylist())
    assert all(v is None for v in table.column("longitude").to_pylist())
    systems = table.column("coordinate_system").to_pylist()
    assert all(v == CoordinateSystem.UNKNOWN.value for v in systems)
    # 地址缺失=UNKNOWN，不用 0
    assert "0" not in {v for v in table.column("address").to_pylist()}


# ---------------------------------------------------------------------------
# community 实体表构造与原子写盘（parquet + DerivedManifest）
# ---------------------------------------------------------------------------


def test_community_table_matches_schema_and_counts() -> None:
    communities = [to_community(c) for c in candidates_all()]
    table = community_table(communities)
    assert table.schema == community_schema()
    assert table.num_rows == 237  # 235 具来源 ID + 2 补数新增（拾光里/示例小区130）
    required = community_schema().names[:]
    for name in required:
        if community_schema().field(name).nullable:
            continue
        assert all(v is not None for v in table.column(name).to_pylist())


def test_write_community_entity_writes_parquet_and_manifest(tmp_path: Path) -> None:
    table = community_table([to_community(c) for c in candidates_all()])
    path = write_community_entity(
        table,
        data_dir=tmp_path,
        inputs=[CATALOG_INPUT],
        notes="WP5-A 测试写盘",
    )

    assert path.name == COMMUNITY_FILENAME
    assert path.parent.name == ENTITIES_LAYER
    assert path.is_file()
    assert pq.read_table(path).num_rows == 237

    manifest = read_derived_manifest(path)
    assert manifest.layer == ENTITIES_LAYER
    assert manifest.table == COMMUNITY_TABLE
    assert manifest.row_count == 237
    assert [i.dataset for i in manifest.inputs] == [CATALOG_INPUT.dataset]


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_build_community_entity_roundtrip(tmp_path: Path) -> None:
    path = build_community_entity(data_dir=tmp_path)
    table = pq.read_table(path)
    assert table.num_rows == 237
    assert table.column("community_id").to_pylist()[0] == "C-XXXX0069"  # 示例小区132
    assert table.column("standard_name").to_pylist()[0] == "示例小区132"


# ---------------------------------------------------------------------------
# 验收⑤：compsval entities build + compsval catalog 列出实体表
# ---------------------------------------------------------------------------


def test_cli_entities_build_and_catalog_lists_entity(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["entities", "build", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "community" in out
    assert "237 rows" in out

    assert cli.main(["catalog", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "[entities] ent_community" in out
