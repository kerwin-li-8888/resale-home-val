"""WP5-C building 楼栋弱实体匹配（信息不足允许未知，不得用 0）。

从链家成交列表快照（``source=lianjia/dataset=chengjiao_list``，SRC-007 已登记）
的每套成交记录提取楼栋级属性证据——``楼层(共N层)``、``YYYY年``、``塔楼|板楼``
（链家字段最全，见 字段覆盖与案例密度-V0.1 §1），并把楼栋弱实体归属到
``community`` 权威表小区，写入 ``data/entities/building.parquet`` 并附
DerivedManifest（可复现 + 溯源）：

- ``building_id = B-<community source_key>-<社区内序号>``（稳定标识，数据字典 §3.5）；
- ``community_id`` 由成交记录小区名解析：精确命中 community 权威表
  ``standard_name`` → 该标准 ID（HIGH 置信）；候选名录内近似命中（名称双向包含）
  → 具楼栋特征为 MEDIUM、仅名称近似为 LOW，LOW **不自动合并**、进待复核清单（验收④）；
- ``building_name``：同一小区内同楼栋指纹（年代/总层数）合并后按出现顺序编
  ``楼栋N``（不虚构来源原生楼栋编号）；
- ``year_built`` / ``total_floors``：缺失一律 ``None``（不得用 0，验收②）；
- ``has_elevator``：仅当 ``total_floors`` 已知且 > 7 时推断 True（高楼层必配梯，
  数据字典 §3.5 电梯口径），其余未知 → ``None``，不臆造。

本模块**不改写** ``community`` 权威表，也不做任何楼栋到小区的跨来源强匹配断言
（验收③④：低置信进待复核、不自动合并）。未知字段统一 ``None``/``UNKNOWN``，
``0`` 绝不用于缺失值（验收②）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.config import data_dir as _default_data_dir
from compsval.entities.candidates import candidates_all
from compsval.entities.community import ENTITIES_LAYER, community_id_of
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.parsers.lianjia import LianjiaRecord, parse_lianjia_txt

BUILDING_TABLE = "building"
BUILDING_FILENAME = f"{BUILDING_TABLE}.parquet"

#: 链家成交列表快照数据集（SRC-007 链家/贝壳·成交与实体权威）。
LIANJIA_SOURCE_ID = "SRC-007"
LIANJIA_DATASET = "chengjiao_list"
LIANJIA_RAW_ROOT = "source=lianjia"

#: 高楼层必配电梯的层数阈值（> 7 层推断 True；其余未知 → None，不臆造）。
_ELEVATOR_FLOOR_THRESHOLD = 7

#: 候选名录近似匹配允许的最短名称长度（避免过短名称误命中）。
_APPROX_MIN_LENGTH = 3


class MatchConfidence(StrEnum):
    """弱匹配置信状态（验收③）：高/中/低/未知。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"
    UNKNOWN = "未知"


@dataclass(frozen=True)
class BuildingEvidence:
    """一条楼栋属性证据：由一套链家成交记录推导（可溯源到快照行号）。"""

    community: str
    total_floors: int | None
    year_built: int | None
    has_elevator: bool | None
    building_type: str | None
    raw_start_line: int
    source_ref: str


@dataclass(frozen=True)
class BuildingRow:
    """一条 building 实体行（含弱匹配置信与溯源，写表前组装）。"""

    building_id: str
    community_id: str
    building_name: str
    year_built: int | None
    total_floors: int | None
    has_elevator: bool | None
    match_confidence: MatchConfidence
    source_id: str
    source_key: str
    source_ref: str


def building_id_of(community_key: str, seq: int) -> str:
    """楼栋唯一 ID：``B-<社区 source_key>-<序号>``（数据字典 §3.5 主键）。"""
    return f"B-{community_key}-{seq}"


def _elevator_from_floors(total_floors: int | None) -> bool | None:
    """从总层数推断是否电梯楼；层数未知或 ≤7 → None（不臆造，验收②）。"""
    if total_floors is not None and total_floors > _ELEVATOR_FLOOR_THRESHOLD:
        return True
    return None


def _strong_floors(year_built: int | None, total_floors: int | None) -> bool:
    """是否具备强楼栋特征：年代或总层数任一已知。"""
    return year_built is not None or total_floors is not None


def _canonical_lookup() -> dict[str, str]:
    """community 权威表标准名 → community_id（骨架期标准名唯一）。"""
    return {
        candidate.standard_name: community_id_of(candidate.source_key)
        for candidate in candidates_all()
    }


def _approx_candidates(name: str, canonical: dict[str, str]) -> tuple[str | None, str | None]:
    """候选名录近似命中：名称双向包含，返回 (标准名, community_id)。

    仅当候选标准名与来源名长度均 ≥ ``_APPROX_MIN_LENGTH`` 时进行双向包含匹配，
    避免过短名称（如"汐园"）误命中；无近似命中返回 ``(None, None)``。
    """
    if len(name.strip()) < _APPROX_MIN_LENGTH:
        return None, None
    for standard_name, community_id in canonical.items():
        if len(standard_name.strip()) < _APPROX_MIN_LENGTH:
            continue
        if name in standard_name or standard_name in name:
            return standard_name, community_id
    return None, None


def record_to_evidence(record: LianjiaRecord, snapshot_id: str) -> BuildingEvidence:
    """把一条链家成交记录转换为楼栋属性证据（供构建/置信判定）。

    - ``total_floors`` / ``year_built`` 直接取解析结果（缺失 → None，不得用 0）；
    - ``has_elevator`` 仅由已知总层数 > 7 推断 True，否则 None；
    - ``source_ref = 链家快照 <snapshot_id> 行N``（每行可溯源，验收①④）。
    """
    total_floors = record.total_floors
    year_built = record.year_built
    return BuildingEvidence(
        community=record.community,
        total_floors=total_floors,
        year_built=year_built,
        has_elevator=_elevator_from_floors(total_floors),
        building_type=record.building_type,
        raw_start_line=record.raw_start_line or 0,
        source_ref=f"链家快照 {snapshot_id} 行{record.raw_start_line or 0}",
    )


def _resolve_community(
    evidence: BuildingEvidence,
    canonical: dict[str, str],
) -> tuple[str | None, MatchConfidence, str]:
    """楼栋证据 → (community_id, 匹配置信, 溯源理由)。

    - 来源小区名精确命中权威标准名 → HIGH（直接归属）；
    - 候选名录近似命中（双向包含，具强特征）→ MEDIUM（名称近似 + 楼栋特征支撑）；
    - 候选名录近似命中但无强特征 → LOW（仅名称近似，低置信，进待复核不自动合并）；
    - 完全未命中 → (None, UNKNOWN, 说明)。
    """
    name = evidence.community.strip()
    if not name:
        return None, MatchConfidence.UNKNOWN, "成交记录无小区名"
    if name in canonical:
        community_id = canonical[name]
        return community_id, MatchConfidence.HIGH, f"标准名命中 {name}"
    approx_name, approx_id = _approx_candidates(name, canonical)
    if approx_id is not None:
        if _strong_floors(evidence.year_built, evidence.total_floors):
            return (
                approx_id,
                MatchConfidence.MEDIUM,
                f"候选名录近似命中 {approx_name}（含楼栋特征）",
            )
        return (
            approx_id,
            MatchConfidence.LOW,
            f"候选名录近似命中 {approx_name}（仅名称，低置信，不自动合并）",
        )
    return None, MatchConfidence.UNKNOWN, f"{name} 在候选名录中无近似匹配"


def _build_rows_from_evidence(
    evidence: Sequence[BuildingEvidence],
    *,
    snapshot_id: str,
) -> tuple[list[BuildingRow], list[BuildingRow]]:
    """依据楼栋属性证据构建 building 行（弱匹配 + 置信状态）。

    返回 ``(rows, low_rows)``：
    - ``rows``：可归属小区的楼栋行（HIGH/MEDIUM 落表，含强弱特征判定）；
    - ``low_rows``：LOW 置信行（仅名称近似、无强特征），**不落表**，进待复核
      清单（验收④，不自动合并）。

    同一小区内同楼栋指纹（年代/总层数）合并为一行 ``building_name=楼栋N``，
    ``source_ref`` 保留首次出现的快照行号（可溯源）。
    """
    canonical = _canonical_lookup()
    buckets: dict[tuple[str, str], list[tuple[BuildingEvidence, MatchConfidence, str]]] = {}
    order: list[tuple[str, str]] = []
    low_rows: list[BuildingRow] = []

    for record_evidence in evidence:
        community_id, confidence, reason = _resolve_community(record_evidence, canonical)
        if community_id is None:
            continue  # 无归属小区，不落表（未命中 → 不自动合并）
        if confidence is MatchConfidence.LOW:
            # 低置信（仅名称近似、无楼栋特征）只进待复核清单，不落表（验收④）
            low_rows.append(
                BuildingRow(
                    building_id=f"pending-{len(low_rows) + 1}",
                    community_id=community_id,
                    building_name="UNKNOWN",
                    year_built=record_evidence.year_built,
                    total_floors=record_evidence.total_floors,
                    has_elevator=record_evidence.has_elevator,
                    match_confidence=confidence,
                    source_id=LIANJIA_SOURCE_ID,
                    source_key=str(record_evidence.raw_start_line),
                    source_ref=f"{record_evidence.source_ref}（{reason}）",
                )
            )
            continue
        fingerprint = (record_evidence.year_built, record_evidence.total_floors)
        key = (community_id, f"{fingerprint[0] or ''}|{fingerprint[1] or ''}")
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append((record_evidence, confidence, reason))

    rows: list[BuildingRow] = []
    seq_by_community: dict[str, int] = {}
    for community_id, _fingerprint in order:
        group = buckets[(community_id, _fingerprint)]
        first, confidence, reason = group[0]
        seq = seq_by_community.get(community_id, 0) + 1
        seq_by_community[community_id] = seq
        community_key = str(community_id).removeprefix("C-")
        rows.append(
            BuildingRow(
                building_id=building_id_of(community_key, seq),
                community_id=community_id,
                building_name=f"楼栋{seq}",
                year_built=first.year_built,
                total_floors=first.total_floors,
                has_elevator=first.has_elevator,
                # 落表行置信度 = 小区解析置信（HIGH/MEDIUM），不基于特征重算
                match_confidence=confidence,
                source_id=LIANJIA_SOURCE_ID,
                source_key=str(first.raw_start_line),
                source_ref=f"{first.source_ref}（{reason}）",
            )
        )
    return rows, low_rows


def build_building_rows(
    records: Sequence[LianjiaRecord],
    *,
    snapshot_id: str,
) -> tuple[list[BuildingRow], list[BuildingRow]]:
    """从链家成交记录构建 building 实体行（弱匹配 + 置信状态）。

    便捷入口：由完整 ``LianjiaRecord`` 列表驱动（单元测试直接调用），
    内部转为楼栋属性证据后复用 :func:`_build_rows_from_evidence`。
    """
    evidence = [record_to_evidence(record, snapshot_id) for record in records]
    return _build_rows_from_evidence(evidence, snapshot_id=snapshot_id)


def building_schema() -> pa.Schema:
    """``building`` 实体表 PyArrow 模式（数据字典 §3.5 字段 + 弱匹配扩展）。"""
    return pa.schema(
        [
            pa.field("building_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("building_name", pa.string(), nullable=False),
            pa.field("year_built", pa.int32(), nullable=True),
            pa.field("total_floors", pa.int32(), nullable=True),
            pa.field("has_elevator", pa.bool_(), nullable=True),
            pa.field("match_confidence", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_key", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
        ]
    )


def building_table(rows: Sequence[BuildingRow]) -> pa.Table:
    """把 building 实体序列构造成与 :func:`building_schema` 一致的 PyArrow 表。"""
    cols: dict[str, list[object]] = {name: [] for name in building_schema().names}
    for row in rows:
        row_values: dict[str, object] = {
            "building_id": row.building_id,
            "community_id": row.community_id,
            "building_name": row.building_name,
            "year_built": row.year_built,
            "total_floors": row.total_floors,
            "has_elevator": row.has_elevator,
            "match_confidence": row.match_confidence.value,
            "source_id": row.source_id,
            "source_key": row.source_key,
            "source_ref": row.source_ref,
        }
        if list(row_values) != list(cols):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in cols:
            cols[name].append(row_values[name])
    return pa.table(cols, schema=building_schema())


def write_building_entity(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """把 building 实体表及其 DerivedManifest 原子写入 ``data/entities/``。

    与 community/alias 写盘纪律一致：先写 ``.incomplete`` 兄弟文件再重命名。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / BUILDING_FILENAME
    work_path = entities_dir / (BUILDING_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=BUILDING_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=list(inputs),
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def _latest_lianjia_snapshot(data_dir: Path) -> Path | None:
    """定位最新链家成交列表快照目录（``fetched_at`` 按 UTC 字典序即时间序）。"""
    snapshot_root = data_dir / "raw" / LIANJIA_RAW_ROOT / f"dataset={LIANJIA_DATASET}"
    candidates = sorted(snapshot_root.glob("fetched_at=*"))
    if not candidates:
        return None
    latest_dir = candidates[-1]
    data_path = latest_dir / "data.parquet"
    return data_path if data_path.is_file() else None


def load_building_records(
    *,
    data_dir: Path | None = None,
) -> tuple[list[LianjiaRecord], str]:
    """从最新链家成交列表快照读取原始行并解析为成交记录。

    返回 ``(records, snapshot_id)``；快照缺失时返回 ``([], "UNKNOWN")``（与
    无 entities 环境的退化行为一致，不报错）。``snapshot_id`` 由 ``fetched_at`` 推导。
    """
    root = data_dir if data_dir is not None else _default_data_dir()
    data_path = _latest_lianjia_snapshot(root)
    if data_path is None:
        return [], "UNKNOWN"
    table = pq.read_table(data_path)
    lines = [str(c) for c in table.column("content").to_pylist()]
    snapshot_id = data_path.parent.name.removeprefix("fetched_at=")
    return parse_lianjia_txt(lines), snapshot_id


def build_building_entity(
    *,
    data_dir: Path,
    notes: str | None = None,
) -> tuple[Path, list[BuildingRow]]:
    """从链家成交列表快照构建并写入 building 实体表（WP5-C 主入口）。

    返回 ``(写入的 parquet 路径, 低置信待复核清单)``；低置信行**不落表**（验收④）。
    快照缺失或无可归属楼栋时抛 ``FileNotFoundError``。
    """
    records, snapshot_id = load_building_records(data_dir=data_dir)
    if not records:
        raise FileNotFoundError(f"未找到链家成交列表快照（{LIANJIA_DATASET}）可提取楼栋证据")
    rows, low_rows = build_building_rows(records, snapshot_id=snapshot_id)
    if not rows:
        raise FileNotFoundError("链家快照中无任何可归属小区的楼栋证据")
    table = building_table(rows)
    inputs = [InputRef(dataset=LIANJIA_DATASET, fetched_at=snapshot_id)]
    path = write_building_entity(
        table,
        data_dir=data_dir,
        inputs=inputs,
        notes=notes,
    )
    return path, low_rows
