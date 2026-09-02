"""WP5-A community 小区实体权威表构建（骨架期，候选小区名录）。

从 :mod:`compsval.entities.candidates`（候选小区名录-V0.1.md 的
结构化转录）构建 ``community`` 小区实体权威表，写入 ``data/entities/`` 并附
DerivedManifest（可复现 + 溯源）：

- ``community_id = C-<房天下 loupan ID>``（稳定标识，见数据字典 §3.3）；
- ``standard_name / block / address / boundary_status`` 直接取名录转录；
- 坐标一律 ``None``（名录无坐标，不虚构，§7.3）；未记录坐标时
  ``coordinate_system = UNKNOWN`` 合法（§3.3 校验：仅在记录坐标时强制声明）；
- ``source_id = SRC-005``（房天下骨架），``source_key = loupan ID``，
  ``source_ref = 名录节号+行号``（每行可溯源，验收②）。

骨架期仅覆盖候选名录中**具来源 ID** 的行（235 个）；名录中 5 个 "ID 待补"
候选（:data:`~compsval.entities.candidates.ID_PENDING_EXCLUDED`）
无稳定主键，不进入权威表，待扩充名录后回填（WP5 待定项）。链家全量权威数据
后续经 community_alias 映射到同一 community_id（WP5-B/E 回填），不改写本表。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import Community, CoordinateSystem
from compsval.entities.candidates import CandidateCommunity, candidates_all
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)

ENTITIES_LAYER = "entities"
COMMUNITY_TABLE = "community"
COMMUNITY_FILENAME = f"{COMMUNITY_TABLE}.parquet"

#: 骨架期来源：房天下（候选小区名录，registered SRC-005）。
SKELETON_SOURCE_ID = "SRC-005"

#: 骨架输入：候选小区名录（DATA-001-C 交付物，2026-08-21 采集/边界确认）。
CATALOG_INPUT = InputRef(dataset="candidate_community_catalog", fetched_at="2026-08-21")


def community_id_of(source_key: str) -> str:
    """标准小区 ID：``C-<房天下 loupan ID>``（实体权威表主键）。"""
    return f"C-{source_key}"


def to_community(candidate: CandidateCommunity) -> Community:
    """一条候选名录转录 → ``community`` 实体（骨架期默认值落地）。"""
    return Community(
        community_id=community_id_of(candidate.source_key),
        standard_name=candidate.standard_name,
        block=candidate.block,
        address=candidate.address,
        boundary_status=candidate.boundary,
        source_id=SKELETON_SOURCE_ID,
        source_key=candidate.source_key,
        source_ref=candidate.source_ref,
        notes=candidate.notes,
        # 名录无坐标 → 一律 None，不得虚构；坐标系 UNKNOWN 合法（§7.3）
        coordinate_system=CoordinateSystem.UNKNOWN,
    )


def community_schema() -> pa.Schema:
    """``community`` 实体表 PyArrow 模式（对应数据字典 §3.3 字段）。"""
    return pa.schema(
        [
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("standard_name", pa.string(), nullable=False),
            pa.field("block", pa.string(), nullable=False),
            pa.field("address", pa.string(), nullable=False),
            pa.field("latitude", pa.float64(), nullable=True),
            pa.field("longitude", pa.float64(), nullable=True),
            pa.field("coordinate_system", pa.string(), nullable=False),
            pa.field("boundary_status", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_key", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("notes", pa.string(), nullable=True),
        ]
    )


def community_table(communities: Sequence[Community]) -> pa.Table:
    """把 community 实体序列构造成与 :func:`community_schema` 一致的 PyArrow 表。"""
    rows: dict[str, list[object]] = {name: [] for name in community_schema().names}
    for community in communities:
        row_values: dict[str, object] = {
            "community_id": community.community_id,
            "standard_name": community.standard_name,
            "block": community.block,
            "address": community.address,
            "latitude": community.latitude,
            "longitude": community.longitude,
            "coordinate_system": community.coordinate_system.value,
            "boundary_status": community.boundary_status.value,
            "source_id": community.source_id,
            "source_key": community.source_key,
            "source_ref": community.source_ref,
            "notes": community.notes,
        }
        if list(row_values) != list(rows):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in rows:
            rows[name].append(row_values[name])
    return pa.table(rows, schema=community_schema())


def write_community_entity(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """把 community 实体表及其 DerivedManifest 原子写入 ``data/entities/``。

    先写 ``.incomplete`` 兄弟文件再重命名，避免半写表冒充完整派生表
    （与 staged/marts 写盘纪律一致）。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / COMMUNITY_FILENAME
    work_path = entities_dir / (COMMUNITY_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=COMMUNITY_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=list(inputs),
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def build_community_entity(
    *,
    data_dir: Path,
    notes: str | None = None,
) -> Path:
    """从候选小区名录骨架构建并写入 community 实体权威表（WP5-A 主入口）。

    返回写入的 parquet 路径；行列与候选名录可实施集合（235 个具来源 ID 行）
    一致，可由自检/测试核对。
    """
    communities = [to_community(candidate) for candidate in candidates_all()]
    table = community_table(communities)
    return write_community_entity(
        table,
        data_dir=data_dir,
        inputs=[CATALOG_INPUT],
        notes=notes,
    )
