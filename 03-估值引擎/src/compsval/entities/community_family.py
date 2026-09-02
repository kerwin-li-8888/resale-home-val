"""community-family-subarea-census-v1-3 实体构建：家族/子区两级模型落表。

按冻结判定表（``frozen/user-adjudication-table.md``，2026-08-31 用户判定）执行：

- ``community.parquet`` 扩 4 列：``family_id``（默认 UNKNOWN）、``entity_status``
  （默认 active）、``redirect_community_id`` / ``redirect_subarea_name``（默认
  UNKNOWN）；2 处标准名变更、5 处 merged+redirect、2 个 C-29 保留号段新建实体；
- 新增 ``community_family.parquet``（13 行，金汐花园兄弟结构 main=UNKNOWN）与
  ``community_subarea.parquet``（28 行，match_names 按唯一性规则注册）；
- ``community_alias.parquet`` 追加 ``AF-`` 批次 1 行（华标品峰→C-XXXX0125），
  86 → 87 行，既有行逐字节保留；
- 追加式幂等重建：先剔除 v1.3 产物（扩列、C-29 行、AF- 行、两新表）再重放，
  notes/溯源追加带 ``[v1.3-family]`` 标记防重复；同输入重跑逐字节一致；
- 实体不物理删除：merged 行保留全部历史字段，仅加状态与转发映射。

匹配消费语义（norm-v2，见 census v1.3 管线）：来源ID → 标准名 → 子区名/别名；
别名指向 merged 实体经 redirect 转发到承接 ``(community_id, sub_area)``。
判定依据见 proposal 判定表与 design D1/D2，由 ``tests/test_community_family.py`` 保障对拍。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import AliasConflictStatus, CommunityAlias
from compsval.entities.alias import (
    ALIAS_FILENAME,
    alias_table,
    write_alias_entity,
)
from compsval.entities.community import (
    COMMUNITY_FILENAME,
    ENTITIES_LAYER,
    write_community_entity,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)

FAMILY_TABLE = "community_family"
FAMILY_FILENAME = f"{FAMILY_TABLE}.parquet"
SUBAREA_TABLE = "community_subarea"
SUBAREA_FILENAME = f"{SUBAREA_TABLE}.parquet"
FAMILY_BATCH_PREFIX = "AF-"

FAMILY_SUBAREA_SOURCE_ID = "SRC-007"
ADJUDICATION_REF = (
    "community-family-subarea-census-v1-3 frozen/user-adjudication-table.md（2026-08-31 用户判定）"
)
NOTE_MARKER = "[v1.3-family]"

#: community.parquet v1.3 扩列（design D1）
FAMILY_COLUMNS = ("family_id", "entity_status", "redirect_community_id", "redirect_subarea_name")
ENTITY_STATUS_ACTIVE = "active"
ENTITY_STATUS_MERGED = "merged"

_DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[4]
#: 冻结判定表解析（2026-09-01 维护，ext-sale-ingest-scope-v1-2 verify 修复）：
#: change 归档后目录迁至 archive/<date>-<name>，此处按候选顺序回落，
#: 文件内容同一（archive 为移动，非改写）。
_ADJUDICATION_CANDIDATES = (
    _DEFAULT_REPO_ROOT
    / "openspec"
    / "changes"
    / "community-family-subarea-census-v1-3"
    / "frozen"
    / "user-adjudication-table.md",
    _DEFAULT_REPO_ROOT
    / "openspec"
    / "changes"
    / "archive"
    / "2026-09-01-community-family-subarea-census-v1-3"
    / "frozen"
    / "user-adjudication-table.md",
)
_DEFAULT_ADJUDICATION = next(
    (p for p in _ADJUDICATION_CANDIDATES if p.is_file()),
    _ADJUDICATION_CANDIDATES[0],
)


@dataclass(frozen=True)
class FamilyRow:
    """家族登记行（community_family）。"""

    family_id: str
    family_name: str
    main_community_id: str  # 兄弟结构 = UNKNOWN
    note: str


@dataclass(frozen=True)
class SubareaRow:
    """子区登记行（community_subarea）；match_names 用 ``|`` 分隔。"""

    subarea_id: str
    family_id: str
    community_id: str
    sub_area_name: str
    match_names: str
    note: str


@dataclass(frozen=True)
class RenameAction:
    """标准名变更（旧名经子区/别名保持可解析）。"""

    community_id: str
    new_name: str
    old_name: str


@dataclass(frozen=True)
class MergeAction:
    """实体合并降级：merged + redirect，行保留不物理删除。"""

    community_id: str
    old_name: str
    redirect_community_id: str
    redirect_subarea_name: str
    family_id: str


@dataclass(frozen=True)
class NewEntityRow:
    """新建实体（C-29 保留号段，判定表用户确认身份）。"""

    community_id: str
    standard_name: str
    family_id: str
    source_ref: str


FAMILIES: tuple[FamilyRow, ...] = (
    FamilyRow("F-001", "示例小区132", "C-XXXX0069", "主实体=示例小区132(裸名)，7 子区"),
    FamilyRow(
        "F-002",
        "金汐花园",
        "UNKNOWN",
        "兄弟结构、无主实体；示例小区039与示例小区053（2026-09-01 追加裁决）挂 family_id",
    ),
    FamilyRow("F-003", "示例小区164", "C-XXXX0181", "主实体标准名由 示例小区164龙禧 改为 示例小区164"),
    FamilyRow("F-004", "示例小区136", "C-XXXX0033", "拾光里实体 merged 并入"),
    FamilyRow("F-005", "示例小区130", "C-XXXX0063", "A区/B区 子区"),
    FamilyRow("F-006", "示例小区169", "C-XXXX0020", "AB区/C区/东区 子区"),
    FamilyRow("F-007", "示例小区220", "C-XXXX0038", "一期/二期 子区"),
    FamilyRow("F-008", "示例小区188", "C-XXXX0099", "晓园东/南/北/新 子区"),
    FamilyRow(
        "F-009",
        "示例小区143",
        "C-XXXX0029",
        "示例小区245（独立楼盘、同开发商近邻）merged 并入，合并计价",
    ),
    FamilyRow("F-010", "示例小区167", "C-XXXX0018", "示例小区244（二期名）merged 并入"),
    FamilyRow("F-011", "示例小区172", "C-XXXX0098", "宝通街 子区"),
    FamilyRow("F-012", "示例小区165", "C-XXXX0066", "四期 子区"),
    FamilyRow("F-013", "示例小区186", "C-XXXX0145", "102号大院/远洋宿舍 子区"),
)

SUBAREAS: tuple[SubareaRow, ...] = (
    SubareaRow(
        "SA-001",
        "F-001",
        "C-XXXX0069",
        "榕岸",
        "示例小区132榕岸|榕岸",
        "独立子区；本判定表超越前轮榕岸→榕岸华庭(E区) retarget，别名行本体不变",
    ),
    SubareaRow(
        "SA-002",
        "F-001",
        "C-XXXX0069",
        "水岸榕城",
        "示例小区132水岸榕城|水岸榕城",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-003",
        "F-001",
        "C-XXXX0069",
        "澜庭锦榕湾",
        "示例小区132澜庭锦榕湾|澜庭锦榕湾",
        "用户确认澜庭锦榕湾=清晏子区",
    ),
    SubareaRow(
        "SA-004",
        "F-001",
        "C-XXXX0069",
        "和榕风景",
        "示例小区132和榕风景|和榕风景",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-005",
        "F-001",
        "C-XXXX0069",
        "榕景四季(D区)",
        "示例小区132榕景四季(D区)|榕景四季(D区)",
        "独立实体 C-XXXX0128 转 merged",
    ),
    SubareaRow(
        "SA-006",
        "F-001",
        "C-XXXX0069",
        "榕岸华庭(E区)",
        "示例小区132榕岸华庭(E区)|榕岸华庭(E区)",
        "独立实体 C-XXXX0184 转 merged",
    ),
    SubareaRow(
        "SA-007",
        "F-001",
        "C-XXXX0069",
        "榕城尚品公寓",
        "示例小区132榕城尚品公寓|榕城尚品公寓",
        "无实体，直接登记子区",
    ),
    SubareaRow(
        "SA-008",
        "F-002",
        "C-XXXX0189",
        "第一金汐",
        "第一金汐",
        "新建实体承接；连写形态示例小区012由实体标准名直接命中",
    ),
    SubareaRow(
        "SA-009",
        "F-002",
        "C-XXXX0190",
        "第二金汐",
        "第二金汐",
        "新建实体承接；连写形态示例小区011由实体标准名直接命中",
    ),
    SubareaRow("SA-010", "F-003", "C-XXXX0181", "龙禧", "示例小区164龙禧|龙禧", "旧标准名降为子区"),
    SubareaRow(
        "SA-011",
        "F-004",
        "C-XXXX0033",
        "拾光里",
        "示例小区136拾光里|拾光里",
        "实体 C-XXXX0051 转 merged；别名转发目标一致",
    ),
    SubareaRow("SA-012", "F-005", "C-XXXX0063", "A区", "示例小区130A区", "通用后缀不注册裸形态"),
    SubareaRow("SA-013", "F-005", "C-XXXX0063", "B区", "示例小区130B区", "通用后缀不注册裸形态"),
    SubareaRow("SA-014", "F-006", "C-XXXX0020", "AB区", "示例小区169AB区", "通用后缀不注册裸形态"),
    SubareaRow("SA-015", "F-006", "C-XXXX0020", "C区", "示例小区169C区", "通用后缀不注册裸形态"),
    SubareaRow("SA-016", "F-006", "C-XXXX0020", "东区", "示例小区169东区", "通用后缀不注册裸形态"),
    SubareaRow("SA-017", "F-007", "C-XXXX0038", "一期", "示例小区220一期", "通用后缀不注册裸形态"),
    SubareaRow("SA-018", "F-007", "C-XXXX0038", "二期", "示例小区220二期", "通用后缀不注册裸形态"),
    SubareaRow(
        "SA-019",
        "F-008",
        "C-XXXX0099",
        "晓园东",
        "示例小区188晓园东|晓园东",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-020",
        "F-008",
        "C-XXXX0099",
        "晓园南",
        "示例小区188晓园南|晓园南",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-021",
        "F-008",
        "C-XXXX0099",
        "晓园北",
        "示例小区188晓园北|晓园北",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-022",
        "F-008",
        "C-XXXX0099",
        "晓园新",
        "示例小区188晓园新|晓园新",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-023",
        "F-009",
        "C-XXXX0029",
        "示例小区245",
        "示例小区143示例小区245|示例小区245",
        "实体 C-XXXX0049 转 merged；别名转发目标一致",
    ),
    SubareaRow(
        "SA-024",
        "F-010",
        "C-XXXX0018",
        "示例小区244",
        "示例小区167二期示例小区244|示例小区244",
        "实体 C-XXXX0170 转 merged；别名转发目标一致",
    ),
    SubareaRow(
        "SA-025",
        "F-011",
        "C-XXXX0098",
        "宝通街",
        "示例小区172宝通街|宝通街",
        "AC 批次连写别名已一致",
    ),
    SubareaRow("SA-026", "F-012", "C-XXXX0066", "四期", "示例小区165四期", "通用后缀不注册裸形态"),
    SubareaRow(
        "SA-027",
        "F-013",
        "C-XXXX0145",
        "102号大院",
        "示例小区186102号大院|102号大院",
        "AC 批次连写别名已一致",
    ),
    SubareaRow(
        "SA-028",
        "F-013",
        "C-XXXX0145",
        "远洋宿舍",
        "示例小区186远洋宿舍",
        "裸形态与示例小区024竞争候选，不注册",
    ),
)

RENAMES: tuple[RenameAction, ...] = (
    RenameAction("C-XXXX0181", "示例小区164", "示例小区164龙禧"),
    RenameAction("C-XXXX0125", "示例小区089", "华标品峰"),
)

MERGES: tuple[MergeAction, ...] = (
    MergeAction("C-XXXX0051", "拾光里", "C-XXXX0033", "拾光里", "F-004"),
    MergeAction("C-XXXX0184", "示例小区132榕岸华庭(E区)", "C-XXXX0069", "榕岸华庭(E区)", "F-001"),
    MergeAction("C-XXXX0128", "示例小区132榕景四季(D区)", "C-XXXX0069", "榕景四季(D区)", "F-001"),
    MergeAction("C-XXXX0170", "示例小区244", "C-XXXX0018", "示例小区244", "F-010"),
    MergeAction("C-XXXX0049", "示例小区245", "C-XXXX0029", "示例小区245", "F-009"),
)

NEW_ENTITIES: tuple[NewEntityRow, ...] = (
    NewEntityRow(
        "C-XXXX0189", "示例小区012", "F-002", f"{ADJUDICATION_REF}：金汐家族新建成员第一金汐"
    ),
    NewEntityRow(
        "C-XXXX0190", "示例小区011", "F-002", f"{ADJUDICATION_REF}：金汐家族新建成员第二金汐"
    ),
)

#: 新实体之外的 family_id 挂靠（合并实体并入家族）
MEMBER_FAMILY_IDS: dict[str, str] = {
    "C-XXXX0069": "F-001",
    "C-XXXX0184": "F-001",
    "C-XXXX0128": "F-001",
    "C-XXXX0067": "F-002",
    "C-XXXX0097": "F-002",
    "C-XXXX0181": "F-003",
    "C-XXXX0033": "F-004",
    "C-XXXX0051": "F-004",
    "C-XXXX0063": "F-005",
    "C-XXXX0020": "F-006",
    "C-XXXX0038": "F-007",
    "C-XXXX0099": "F-008",
    "C-XXXX0029": "F-009",
    "C-XXXX0049": "F-009",
    "C-XXXX0018": "F-010",
    "C-XXXX0170": "F-010",
    "C-XXXX0098": "F-011",
    "C-XXXX0066": "F-012",
    "C-XXXX0145": "F-013",
}

ALIAS_ADDITION = CommunityAlias(
    alias_id="AF-1-1",
    community_id="C-XXXX0125",
    source_alias="华标品峰",
    source_id=FAMILY_SUBAREA_SOURCE_ID,
    source_ref=f"{ADJUDICATION_REF}：示例小区089改名后旧名承接",
    conflict_status=AliasConflictStatus.CONSISTENT,
)


def family_schema() -> pa.Schema:
    """``community_family`` 表模式。"""
    return pa.schema(
        [
            pa.field("family_id", pa.string(), nullable=False),
            pa.field("family_name", pa.string(), nullable=False),
            pa.field("main_community_id", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("notes", pa.string(), nullable=True),
        ]
    )


def subarea_schema() -> pa.Schema:
    """``community_subarea`` 表模式。"""
    return pa.schema(
        [
            pa.field("subarea_id", pa.string(), nullable=False),
            pa.field("family_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("sub_area_name", pa.string(), nullable=False),
            pa.field("match_names", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("notes", pa.string(), nullable=True),
        ]
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_registry_table(
    table: pa.Table,
    filename: str,
    table_name: str,
    *,
    data_dir: Path,
    inputs: list[InputRef],
    notes: str,
) -> Path:
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / filename
    work_path = entities_dir / (filename + ".incomplete")
    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=table_name,
        built_at=_utcnow(),
        row_count=table.num_rows,
        inputs=inputs,
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _read_community(data_dir: Path) -> pa.Table:
    path = data_dir / ENTITIES_LAYER / COMMUNITY_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"community 表不存在，请先构建骨架：{path}")
    table = pq.read_table(path)
    return _normalize_community(table)


def _normalize_community(table: pa.Table) -> pa.Table:
    """幂等归一：剔除 v1.3 扩列与 C-29 新建行，回到 237 行基线态。"""
    cols = [name for name in table.column_names if name not in FAMILY_COLUMNS]
    table = table.select(cols)
    ids = table.column("community_id").to_pylist()
    keep = [i for i, cid in enumerate(ids) if not cid.startswith("C-29")]
    return table.take(keep)


def _read_existing_aliases(data_dir: Path) -> list[CommunityAlias]:
    path = data_dir / ENTITIES_LAYER / ALIAS_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"alias 表不存在：{path}")
    table = pq.read_table(path)
    kept: list[CommunityAlias] = []
    for aid, cid, name, sid, ref, status in zip(
        table.column("alias_id").to_pylist(),
        table.column("community_id").to_pylist(),
        table.column("source_alias").to_pylist(),
        table.column("source_id").to_pylist(),
        table.column("source_ref").to_pylist(),
        table.column("conflict_status").to_pylist(),
        strict=True,
    ):
        if aid.startswith(FAMILY_BATCH_PREFIX):
            continue
        kept.append(
            CommunityAlias(
                alias_id=aid,
                community_id=cid,
                source_alias=name,
                source_id=sid,
                source_ref=ref,
                conflict_status=AliasConflictStatus(status),
            )
        )
    return kept


def _community_table_with_families(table: pa.Table) -> pa.Table:
    """应用扩列、改名、merged、family_id 与新建实体（全部按 community_id 幂等）。"""
    ids = table.column("community_id").to_pylist()
    names = table.column("standard_name").to_pylist()
    notes = table.column("notes").to_pylist()

    new_names = list(names)
    for action in RENAMES:
        i = ids.index(action.community_id)
        new_names[i] = action.new_name
    rename_note = {
        a.community_id: f"{NOTE_MARKER} 标准名变更：{a.old_name}→{a.new_name}" for a in RENAMES
    }

    family_ids: list[str] = []
    statuses: list[str] = []
    redirect_cids: list[str] = []
    redirect_subs: list[str] = []
    merge_note: dict[str, str] = {}
    for cid in ids:
        family_ids.append(MEMBER_FAMILY_IDS.get(cid, "UNKNOWN"))
        merge = next((m for m in MERGES if m.community_id == cid), None)
        if merge is not None:
            statuses.append(ENTITY_STATUS_MERGED)
            redirect_cids.append(merge.redirect_community_id)
            redirect_subs.append(merge.redirect_subarea_name)
            merge_note[cid] = (
                f"{NOTE_MARKER} merged：{merge.old_name} 并入 "
                f"{merge.redirect_community_id} 子区[{merge.redirect_subarea_name}]，行保留不删除"
            )
        else:
            statuses.append(ENTITY_STATUS_ACTIVE)
            redirect_cids.append("UNKNOWN")
            redirect_subs.append("UNKNOWN")

    new_notes = []
    for cid, note in zip(ids, notes, strict=True):
        extra_parts = [p for p in (rename_note.get(cid), merge_note.get(cid)) if p]
        if not extra_parts:
            new_notes.append(note)
            continue
        base_note = "" if note is None else str(note)
        if NOTE_MARKER in base_note:
            new_notes.append(note)
        else:
            joined = "；".join([base_note, *extra_parts]) if base_note else "；".join(extra_parts)
            new_notes.append(joined)

    out = table.set_column(
        table.schema.get_field_index("standard_name"),
        pa.field("standard_name", pa.string(), nullable=False),
        pa.array(new_names, type=pa.string()),
    ).set_column(
        table.schema.get_field_index("notes"),
        pa.field("notes", pa.string(), nullable=True),
        pa.array(new_notes, type=pa.string()),
    )
    for field_name, values in (
        ("family_id", family_ids),
        ("entity_status", statuses),
        ("redirect_community_id", redirect_cids),
        ("redirect_subarea_name", redirect_subs),
    ):
        out = out.append_column(
            pa.field(field_name, pa.string(), nullable=False), pa.array(values, type=pa.string())
        )

    new_rows = pa.table(
        {
            "community_id": [e.community_id for e in NEW_ENTITIES],
            "standard_name": [e.standard_name for e in NEW_ENTITIES],
            "block": ["UNKNOWN", "UNKNOWN"],
            "address": ["UNKNOWN", "UNKNOWN"],
            "latitude": pa.array([None, None], type=pa.float64()),
            "longitude": pa.array([None, None], type=pa.float64()),
            "coordinate_system": ["UNKNOWN", "UNKNOWN"],
            "boundary_status": ["机器确认", "机器确认"],
            "source_id": [FAMILY_SUBAREA_SOURCE_ID, FAMILY_SUBAREA_SOURCE_ID],
            "source_key": ["UNKNOWN", "UNKNOWN"],
            "source_ref": [e.source_ref for e in NEW_ENTITIES],
            "notes": [
                f"{NOTE_MARKER} 新建实体（家族 {e.family_id} 成员，判定表用户确认身份）"
                for e in NEW_ENTITIES
            ],
            "family_id": [e.family_id for e in NEW_ENTITIES],
            "entity_status": [ENTITY_STATUS_ACTIVE, ENTITY_STATUS_ACTIVE],
            "redirect_community_id": ["UNKNOWN", "UNKNOWN"],
            "redirect_subarea_name": ["UNKNOWN", "UNKNOWN"],
        },
        schema=out.schema,
    )
    import pyarrow.compute as pc

    if pc.any(
        pc.is_in(new_rows.column("community_id"), value_set=out.column("community_id"))
    ).as_py():
        raise AssertionError("新建实体 community_id 与既有行冲突")
    return pa.concat_tables([out, new_rows])


def family_table_rows() -> pa.Table:
    rows = {
        "family_id": [f.family_id for f in FAMILIES],
        "family_name": [f.family_name for f in FAMILIES],
        "main_community_id": [f.main_community_id for f in FAMILIES],
        "status": [ENTITY_STATUS_ACTIVE] * len(FAMILIES),
        "source_id": [FAMILY_SUBAREA_SOURCE_ID] * len(FAMILIES),
        "source_ref": [ADJUDICATION_REF] * len(FAMILIES),
        "notes": [f.note for f in FAMILIES],
    }
    return pa.table(rows, schema=family_schema())


def subarea_table_rows() -> pa.Table:
    rows = {
        "subarea_id": [s.subarea_id for s in SUBAREAS],
        "family_id": [s.family_id for s in SUBAREAS],
        "community_id": [s.community_id for s in SUBAREAS],
        "sub_area_name": [s.sub_area_name for s in SUBAREAS],
        "match_names": [s.match_names for s in SUBAREAS],
        "status": [ENTITY_STATUS_ACTIVE] * len(SUBAREAS),
        "source_id": [FAMILY_SUBAREA_SOURCE_ID] * len(SUBAREAS),
        "source_ref": [f"{ADJUDICATION_REF}：{s.note}" for s in SUBAREAS],
        "notes": [s.note for s in SUBAREAS],
    }
    return pa.table(rows, schema=subarea_schema())


def _assert_consistency(table: pa.Table, alias_rows: list[CommunityAlias]) -> None:
    import re
    import unicodedata

    def _base(s: str) -> str:
        return re.sub(r"\s+", "", unicodedata.normalize("NFKC", s))

    ids = set(table.column("community_id").to_pylist())
    status_by_id = dict(
        zip(
            table.column("community_id").to_pylist(),
            table.column("entity_status").to_pylist(),
            strict=True,
        )
    )
    active_std = {
        _base(name)
        for cid, name in zip(
            table.column("community_id").to_pylist(),
            table.column("standard_name").to_pylist(),
            strict=True,
        )
        if status_by_id[cid] == ENTITY_STATUS_ACTIVE
    }

    for f in FAMILIES:
        if f.main_community_id != "UNKNOWN" and f.main_community_id not in ids:
            raise AssertionError(f"家族 {f.family_id} 主实体外键悬空：{f.main_community_id}")
    seen_keys: set[str] = set()
    for s in SUBAREAS:
        if s.community_id not in ids:
            raise AssertionError(f"子区 {s.subarea_id} 承接实体外键悬空：{s.community_id}")
        for raw in s.match_names.split("|"):
            key = _base(raw)
            if key in seen_keys:
                raise AssertionError(f"子区 match_names 注册键冲突：{raw}")
            if key in active_std:
                raise AssertionError(f"子区 match_names 与 active 标准名冲突：{raw}")
            seen_keys.add(key)
    for m in MERGES:
        if m.redirect_community_id not in ids:
            raise AssertionError(f"合并转发目标悬空：{m.community_id}→{m.redirect_community_id}")
    hengda = "C-XXXX0083"
    fam = dict(
        zip(
            table.column("community_id").to_pylist(),
            table.column("family_id").to_pylist(),
            strict=True,
        )
    )
    if fam[hengda] != "UNKNOWN":
        raise AssertionError("示例小区047必须保持 family_id=UNKNOWN（判定表：不入金汐家族）")
    if (
        len(FAMILIES) != 13
        or len(SUBAREAS) != 28
        or len(MERGES) != 5
        or len(RENAMES) != 2
        or len(NEW_ENTITIES) != 2
    ):
        raise AssertionError("判定表行数与冻结预期不符")
    total = len(alias_rows)
    if total != 87:
        raise AssertionError(f"alias 终态应为 87 行，实际 {total}")


def build_community_family(
    *,
    data_dir: Path,
    adjudication_table: Path | None = None,
    build_map_path: Path | None = None,
    notes: str | None = None,
) -> dict[str, Path]:
    """按冻结判定表执行家族/子区实体构建（追加式幂等）。

    返回写入路径字典；``build_map_path`` 给出时另写判定表行→动作→产物行映射记录。
    """
    adj = adjudication_table or _DEFAULT_ADJUDICATION
    if not adj.is_file():
        raise FileNotFoundError(f"冻结判定表缺失：{adj}")
    adj_sha = _sha256_file(adj)

    community = _community_table_with_families(_read_community(data_dir))
    alias_rows = _read_existing_aliases(data_dir)
    alias_rows.append(ALIAS_ADDITION)
    _assert_consistency(community, alias_rows)

    inputs = [
        InputRef(
            dataset="frozen_user_adjudication_table",
            fetched_at="2026-08-31",
            content_hash=adj_sha,
        )
    ]
    written: dict[str, Path] = {}
    written["community"] = write_community_entity(
        community,
        data_dir=data_dir,
        inputs=inputs,
        notes=notes
        or "community-family-subarea-census-v1-3：扩列 family_id/entity_status/redirect_*，"
        "2 改名 + 5 merged + 2 新建实体（C-29 段），239 行",
    )
    written["family"] = _write_registry_table(
        family_table_rows(),
        FAMILY_FILENAME,
        FAMILY_TABLE,
        data_dir=data_dir,
        inputs=inputs,
        notes="13 家族登记（金汐花园兄弟结构 main=UNKNOWN）",
    )
    written["subarea"] = _write_registry_table(
        subarea_table_rows(),
        SUBAREA_FILENAME,
        SUBAREA_TABLE,
        data_dir=data_dir,
        inputs=inputs,
        notes="28 子区登记（match_names 唯一性规则注册）",
    )
    written["alias"] = write_alias_entity(
        alias_table(alias_rows),
        data_dir=data_dir,
        inputs=inputs,
        notes="追加 AF-1-1 华标品峰→C-XXXX0125（一致），终态 87 行（一致 73 / 待定 10 / 冲突 4）",
    )

    if build_map_path is not None:
        build_map = {
            "adjudication_table": str(adj),
            "adjudication_table_sha256": adj_sha,
            "families": [f.__dict__ for f in FAMILIES],
            "subareas": [s.__dict__ for s in SUBAREAS],
            "renames": [r.__dict__ for r in RENAMES],
            "merges": [m.__dict__ for m in MERGES],
            "new_entities": [e.__dict__ for e in NEW_ENTITIES],
            "member_family_ids": MEMBER_FAMILY_IDS,
            "alias_addition": ALIAS_ADDITION.model_dump(),
            "community_columns_added": list(FAMILY_COLUMNS),
            "products": {k: str(v) for k, v in written.items()},
        }
        build_map_path.parent.mkdir(parents=True, exist_ok=True)
        with open(build_map_path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build_map, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
    return written


__all__ = [
    "ALIAS_ADDITION",
    "FAMILIES",
    "FAMILY_BATCH_PREFIX",
    "FAMILY_COLUMNS",
    "FAMILY_FILENAME",
    "FAMILY_TABLE",
    "MERGES",
    "NEW_ENTITIES",
    "RENAMES",
    "SUBAREAS",
    "SUBAREA_FILENAME",
    "SUBAREA_TABLE",
    "build_community_family",
    "family_schema",
    "subarea_schema",
]
