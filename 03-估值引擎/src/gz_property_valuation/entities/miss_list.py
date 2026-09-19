"""补名单常态（B-026）开口清单新实体批量登记（``L-<链家小区ID>`` 主键体系）。

把冻结开口登记表（change ``routinize-miss-list-loop`` execution/
``开口清单冻结-20260911-v2.csv``，439 行全量（处置=实体登记 437 / 别名登记 1 /
剔除 1，仅实体登记行入登记），SHA256 旁车）批量登记为实体注册表
新实体（design D2/D4，任务 3.1）：

- ``community_id = L-<主链家小区ID>``——与 ``C-<房天下ID>`` 体系显式区分
  （来源可辨、ID 空间不冲突、可溯源链家页面）；
- ``standard_name`` = 冻结表源名（标准名层解析入口，任务 3.2 e2e 验证）；
- 首批每个新实体独立成家族（design D2）：``family_id = LF-<冻结表序号>``
  指向单实体家族，``community_family`` 行 ``main_community_id`` = 自身；
- 22 例「主从并入」（从属链家ID 非空）登记为 sidecar 表
  ``community_source_key``（一行一来源 ID，主/从旗标可审计）；从属 ID 的
  成交在后续入池时并入同一实体（留待 4.x 入池管线消费），本模块只登记
  ID 组、不改名字解析行为；
- 坐标等缺失字段一律 ``None``/``UNKNOWN``（数据字典缺失语义，不得填零或
  臆测值）；``source_id = SRC-007``（链家，registered）；
- 追加式幂等：先剔除本批次产物（``L-`` 实体行 / ``LF-`` 家族行 / sidecar
  ``L-`` 行）再重放，同冻结表重跑两次 parquet 逐字节一致；
- 冻结表为唯一输入（D4）：读入时复算 SHA256 与冻结值比对，不一致拒绝
  登记；每行 ``source_ref`` 指向冻结表序号（manifest ``inputs`` 携带全量
  SHA256），逐行可溯源。

本模块不改写别名表，不触碰 estimate 链与回写交换代码。
"""

from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gz_property_valuation import __version__
from gz_property_valuation.contract.models import BoundaryStatus
from gz_property_valuation.entities.backfill import cmp_norm
from gz_property_valuation.entities.community import (
    COMMUNITY_FILENAME,
    ENTITIES_LAYER,
    write_community_entity,
)
from gz_property_valuation.entities.community_family import (
    ENTITY_STATUS_ACTIVE,
    FAMILY_FILENAME,
    FAMILY_TABLE,
)
from gz_property_valuation.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)

#: community 实体 ID 前缀（与 ``C-<房天下ID>`` 体系显式区分，design D2）。
MISS_LIST_PREFIX = "L-"
#: 本批次单实体家族 ID 前缀（既有 F-001..F-013 家族号段之外，可辨批次归属）。
MISS_LIST_FAMILY_PREFIX = "LF-"
#: 链家来源（registered SRC-007）。
MISS_LIST_SOURCE_ID = "SRC-007"
#: notes 批次标记（幂等重放识别 + 溯源）。
MISS_LIST_NOTE_MARKER = "[miss-list-437]"
#: 冻结开口登记表行数（冻结事实，SHA 锁定）。
MISS_LIST_EXPECTED_COUNT = 437
#: v2 冻结表全量行数（含别名登记 1 行与剔除 1 行，均不入登记）。
MISS_LIST_TOTAL_ROWS = 439
#: 「主从并入」例数（v2 冻结表从属链家ID 非空的实体登记行数；组3/组4 三行从属
#: 经用户处置修剪后为空，22 → 19，冻结事实）。
MISS_LIST_EXPECTED_SUBORDINATE_TOTAL = 19

#: sidecar 表名：实体 ↔ 来源侧小区 ID 组（主/从旗标，主从并入入池留待 4.x）。
SOURCE_KEY_TABLE = "community_source_key"
SOURCE_KEY_FILENAME = f"{SOURCE_KEY_TABLE}.parquet"
KEY_ROLE_PRIMARY = "primary"
KEY_ROLE_SUBORDINATE = "subordinate"

FROZEN_CSV_NAME = "开口清单冻结-20260911-v2.csv"
#: 冻结值：登记输入唯一性锁（复算不符即拒绝，防冻结表被改后静默重放）。
FROZEN_CSV_SHA256 = "74e854aed51b0c41b58841ef5d650c2a3f76d8acfe5af9b7b917c8579b10165c"
FROZEN_FETCHED_AT = "2026-09-11"

_DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[4]


def frozen_miss_list_path() -> Path:
    """冻结开口登记表路径（changes/ 优先，archive 归档目录回落，惯例同判定表解析）。"""
    direct = (
        _DEFAULT_REPO_ROOT
        / "openspec"
        / "changes"
        / "routinize-miss-list-loop"
        / "execution"
        / FROZEN_CSV_NAME
    )
    if direct.is_file():
        return direct
    archive_root = _DEFAULT_REPO_ROOT / "openspec" / "changes" / "archive"
    if archive_root.is_dir():
        matches = sorted(
            archive_root.glob(f"*-routinize-miss-list-loop/execution/{FROZEN_CSV_NAME}")
        )
        if matches:
            return matches[-1]
    raise FileNotFoundError(f"冻结开口登记表缺失：{direct}")


@dataclass(frozen=True)
class MissListEntry:
    """冻结表一行：开口新实体登记输入。"""

    seq: int
    name: str
    primary_key: str
    subordinate_keys: tuple[str, ...]
    adjudication_ref: str


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_frozen_miss_list(csv_path: Path | None = None) -> list[MissListEntry]:
    """读冻结开口登记表（唯一输入，D4）并做冻结校验。

    - SHA256 复算与 ``FROZEN_CSV_SHA256`` 不符 → 拒绝（防冻结表被改后静默
      重放）；测试等显式输入经注入期望值走同一锁行为；
    - 默认路径（真实冻结表）额外校验全量行数 = ``MISS_LIST_TOTAL_ROWS`` 且
      「实体登记」行数 = ``MISS_LIST_EXPECTED_COUNT``（别名登记/剔除行不入
      登记：别名行随首批队列按一致别名批次入库，剔除行留待补清单）；
    - 批内一致性（仅对实体登记行）：主链家小区ID 唯一；从属 ID 不与主 ID 相
      同、批内不重（v1 冻结表 4 组批内 ID 交叉即被本组断言在写盘前拦截，
      v2 经用户处置修剪后通过，fail-safe 保留）。
    """
    explicit_input = csv_path is not None
    path = csv_path or frozen_miss_list_path()
    if not path.is_file():
        raise FileNotFoundError(f"冻结开口登记表缺失：{path}")
    sha = _sha256_file(path)
    if sha != FROZEN_CSV_SHA256:
        raise AssertionError(f"冻结开口登记表 SHA256 不符：{sha} != {FROZEN_CSV_SHA256}")
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not explicit_input and len(rows) != MISS_LIST_TOTAL_ROWS:
        raise AssertionError(
            f"冻结开口登记表全量行数应为 {MISS_LIST_TOTAL_ROWS}，实际 {len(rows)}"
        )
    entries: list[MissListEntry] = []
    seen_any: set[str] = set()
    for seq, row in enumerate(rows, start=1):
        name = row["源名"].strip()
        if not name:
            raise AssertionError(f"冻结表行 {seq} 源名为空")
        if row.get("处置", "实体登记").strip() != "实体登记":
            continue
        primary = row["主链家小区ID"].strip()
        sub_raw = row["从属链家ID"].strip()
        subs = tuple(k.strip() for k in sub_raw.split("|") if k.strip()) if sub_raw else ()
        if not primary:
            raise AssertionError(f"冻结表行 {seq}（{name}）实体登记主链家小区ID 为空")
        if primary in seen_any:
            raise AssertionError(f"冻结表主链家小区ID 重复或与从属 ID 冲突：{primary}")
        for k in subs:
            if k in seen_any:
                raise AssertionError(f"冻结表行 {seq}（{name}）从属 ID 批内重复：{k}")
        seen_any.add(primary)
        seen_any.update(subs)
        entries.append(
            MissListEntry(
                seq=seq,
                name=name,
                primary_key=primary,
                subordinate_keys=subs,
                adjudication_ref=(row.get("主ID判定/处置依据") or row.get("裁决引用", "")).strip(),
            )
        )
    if not explicit_input and len(entries) != MISS_LIST_EXPECTED_COUNT:
        raise AssertionError(
            f"冻结开口登记表实体登记行数应为 {MISS_LIST_EXPECTED_COUNT}，实际 {len(entries)}"
        )
    return entries


def miss_list_community_id(lianjia_id: str) -> str:
    """补名单新实体 ID：``L-<主链家小区ID>``（design D2，与 C- 体系显式区分）。"""
    return f"{MISS_LIST_PREFIX}{lianjia_id}"


def _family_id_of(seq: int) -> str:
    return f"{MISS_LIST_FAMILY_PREFIX}{seq:03d}"


def _source_ref_of(seq: int) -> str:
    return f"开口清单冻结-20260911-v2.csv 行{seq}（SHA256 {FROZEN_CSV_SHA256[:12]}）"


def _assert_no_name_conflicts(entries: list[MissListEntry], base_community: pa.Table) -> None:
    """登记前防御：冻结源名 SHALL NOT 与既有标准名（canonical 层键）撞键。

    撞键会把既有唯一命中多义化（canonical 同键双实体 → 多义不自动解析），
    属解析行为破坏，必须登记前拦截（failures 名单本就未命中，撞键即异常）。
    """
    statuses = (
        base_community.column("entity_status").to_pylist()
        if "entity_status" in base_community.column_names
        else ["active"] * base_community.num_rows
    )
    base_keys = {
        cmp_norm(str(name))
        for name, status in zip(
            base_community.column("standard_name").to_pylist(), statuses, strict=True
        )
        if status != "merged"
    }
    dup = sorted({cmp_norm(e.name) for e in entries} & base_keys)
    if dup:
        raise AssertionError(f"冻结源名与既有标准名撞键（canonical 层将多义化）：{dup}")


def _miss_list_community_rows(entries: list[MissListEntry], schema: pa.Schema) -> pa.Table:
    """437 行 L- 实体（family_id=LF- 自身家族；缺失字段按数据字典缺失语义）。"""
    cols: dict[str, list[object]] = {name: [] for name in schema.names}
    for e in entries:
        sub_note = (
            f"；从属ID组：{'|'.join(e.subordinate_keys)}" if e.subordinate_keys else ""
        )
        values: dict[str, object] = {
            "community_id": miss_list_community_id(e.primary_key),
            "standard_name": e.name,
            "block": "UNKNOWN",
            "address": "UNKNOWN",
            "latitude": None,
            "longitude": None,
            "coordinate_system": "UNKNOWN",
            "boundary_status": BoundaryStatus.MACHINE_CONFIRMED.value,
            "source_id": MISS_LIST_SOURCE_ID,
            "source_key": e.primary_key,
            "source_ref": _source_ref_of(e.seq),
            "notes": (
                f"{MISS_LIST_NOTE_MARKER} 补名单开口新实体"
                f"（用户裁决：真实独立楼盘、主ID+从属组并入，2026-09-12）{sub_note}"
            ),
            "family_id": _family_id_of(e.seq),
            "entity_status": ENTITY_STATUS_ACTIVE,
            "redirect_community_id": "UNKNOWN",
            "redirect_subarea_name": "UNKNOWN",
        }
        for field_name in cols:
            cols[field_name].append(values[field_name])
    return pa.table(cols, schema=schema)


def _miss_list_family_rows(entries: list[MissListEntry], schema: pa.Schema) -> pa.Table:
    """437 行 LF- 单实体家族（main_community_id=自身，design D2）。"""
    cols: dict[str, list[object]] = {name: [] for name in schema.names}
    for e in entries:
        note = (
            f"{MISS_LIST_NOTE_MARKER} 首批开口单实体家族"
            f"（主从并入：从属 {len(e.subordinate_keys)} ID）"
            if e.subordinate_keys
            else f"{MISS_LIST_NOTE_MARKER} 首批开口单实体家族（唯一 ID）"
        )
        values: dict[str, object] = {
            "family_id": _family_id_of(e.seq),
            "family_name": e.name,
            "main_community_id": miss_list_community_id(e.primary_key),
            "status": ENTITY_STATUS_ACTIVE,
            "source_id": MISS_LIST_SOURCE_ID,
            "source_ref": _source_ref_of(e.seq),
            "notes": note,
        }
        for field_name in cols:
            cols[field_name].append(values[field_name])
    return pa.table(cols, schema=schema)


def source_key_schema() -> pa.Schema:
    """``community_source_key`` 表模式：一行一来源侧小区 ID（主/从旗标可审计）。"""
    return pa.schema(
        [
            pa.field("source_key", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("key_role", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("notes", pa.string(), nullable=True),
        ]
    )


def _miss_list_source_key_rows(entries: list[MissListEntry]) -> pa.Table:
    """456 行来源 ID 组（437 主 + 19 例从属组 ID，一行一 ID）。

    组3/组4 三例从属经用户处置修剪为空（见 change execution/冲突处置-20260911.csv）。
    """
    cols: dict[str, list[object]] = {name: [] for name in source_key_schema().names}
    for e in entries:
        cid = miss_list_community_id(e.primary_key)
        members = [(e.primary_key, KEY_ROLE_PRIMARY, "主 ID")]
        members += [
            (k, KEY_ROLE_SUBORDINATE, f"{MISS_LIST_NOTE_MARKER} 主从并入组从属 ID")
            for k in e.subordinate_keys
        ]
        for key, role, note in members:
            cols["source_key"].append(key)
            cols["community_id"].append(cid)
            cols["key_role"].append(role)
            cols["source_id"].append(MISS_LIST_SOURCE_ID)
            cols["source_ref"].append(_source_ref_of(e.seq))
            cols["notes"].append(f"{MISS_LIST_NOTE_MARKER} {note}")
    return pa.table(cols, schema=source_key_schema())


def _assert_registration_consistency(
    entries: list[MissListEntry], community: pa.Table
) -> None:
    """登记一致性：主键唯一、L- 行数=437、家族外键不悬空且与序号一致。"""
    ids = community.column("community_id").to_pylist()
    if len(ids) != len(set(ids)):
        raise AssertionError("community 表主键重复")
    l_rows = sum(1 for cid in ids if str(cid).startswith(MISS_LIST_PREFIX))
    if l_rows != len(entries):
        raise AssertionError(f"L- 行数 {l_rows} != 冻结表 {len(entries)}")
    fam_by_id = dict(
        zip(ids, community.column("family_id").to_pylist(), strict=True)
    )
    for e in entries:
        cid = miss_list_community_id(e.primary_key)
        if cid not in fam_by_id:
            raise AssertionError(f"登记实体缺失：{cid}")
        if fam_by_id[cid] != _family_id_of(e.seq):
            raise AssertionError(f"family_id 与冻结表序号不一致：{cid}")


def _write_registry_sidecar(
    table: pa.Table,
    filename: str,
    table_name: str,
    *,
    data_dir: Path,
    inputs: list[InputRef],
    notes: str,
) -> Path:
    """实体层注册表写盘（.incomplete 原子替换 + DerivedManifest，惯例同 v1.3）。"""
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / filename
    work_path = entities_dir / (filename + ".incomplete")
    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=table_name,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=inputs,
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def register_miss_list_entities(
    *,
    data_dir: Path,
    csv_path: Path | None = None,
    notes: str | None = None,
) -> dict[str, Path]:
    """把冻结开口清单 437 名（实体登记行）批量登记为 ``L-`` 新实体（任务 3.1 主入口）。

    产物（``data/entities/``，追加式幂等：先剔本批次产物再重放）：
    - ``community.parquet``：+437 行 L- 实体（family_id=LF- 自身家族）；
    - ``community_family.parquet``：+437 行 LF- 单实体家族（main=自身）；
    - ``community_source_key.parquet``：+456 行来源 ID 组（437 主 + 19 例从属）。

    同冻结表重跑两次 parquet 逐字节一致；返回写入路径字典。
    """
    entries = load_frozen_miss_list(csv_path)
    entities_dir = data_dir / ENTITIES_LAYER
    community_path = entities_dir / COMMUNITY_FILENAME
    if not community_path.is_file():
        raise FileNotFoundError(f"community 表不存在，请先构建骨架：{community_path}")
    base_community = pq.read_table(community_path)
    rows_before = base_community.num_rows
    ids = base_community.column("community_id").to_pylist()
    base_community = base_community.take(
        pa.array(
            [i for i, cid in enumerate(ids) if not str(cid).startswith(MISS_LIST_PREFIX)],
            type=pa.int64(),
        )
    )
    _assert_no_name_conflicts(entries, base_community)
    new_community = pa.concat_tables(
        [base_community, _miss_list_community_rows(entries, base_community.schema)]
    )
    _assert_registration_consistency(entries, new_community)

    inputs = [
        InputRef(
            dataset="miss_list_frozen_csv",
            fetched_at=FROZEN_FETCHED_AT,
            content_hash=FROZEN_CSV_SHA256,
        )
    ]
    written: dict[str, Path] = {}
    written["community"] = write_community_entity(
        new_community,
        data_dir=data_dir,
        inputs=inputs,
        notes=notes
        or (
            f"routinize-miss-list-loop 任务3.1：补名单开口 {len(entries)} 名登记"
            f"（L-<链家ID>、LF- 单实体家族、{sum(1 for e in entries if e.subordinate_keys)} 例主从并入 sidecar），"
            f"{rows_before}→{new_community.num_rows} 行"
        ),
    )

    family_path = entities_dir / FAMILY_FILENAME
    if not family_path.is_file():
        raise FileNotFoundError(f"community_family 表不存在：{family_path}")
    base_family = pq.read_table(family_path)
    family_ids = base_family.column("family_id").to_pylist()
    base_family = base_family.take(
        pa.array(
            [
                i
                for i, fid in enumerate(family_ids)
                if not str(fid).startswith(MISS_LIST_FAMILY_PREFIX)
            ],
            type=pa.int64(),
        )
    )
    written["family"] = _write_registry_sidecar(
        pa.concat_tables([base_family, _miss_list_family_rows(entries, base_family.schema)]),
        FAMILY_FILENAME,
        FAMILY_TABLE,
        data_dir=data_dir,
        inputs=inputs,
        notes=(
            "routinize-miss-list-loop 任务3.1："
            f"+{len(entries)} LF- 单实体家族（main=自身，design D2）"
        ),
    )

    sidecar_path = entities_dir / SOURCE_KEY_FILENAME
    base_source: pa.Table | None = None
    if sidecar_path.is_file():
        base_source = pq.read_table(sidecar_path)
        source_cids = base_source.column("community_id").to_pylist()
        base_source = base_source.take(
            pa.array(
                [
                    i
                    for i, cid in enumerate(source_cids)
                    if not str(cid).startswith(MISS_LIST_PREFIX)
                ],
                type=pa.int64(),
            )
        )
    sidecar_table = _miss_list_source_key_rows(entries)
    if base_source is not None:
        sidecar_table = pa.concat_tables([base_source, sidecar_table])
    written["source_key"] = _write_registry_sidecar(
        sidecar_table,
        SOURCE_KEY_FILENAME,
        SOURCE_KEY_TABLE,
        data_dir=data_dir,
        inputs=inputs,
        notes=(
            f"routinize-miss-list-loop 任务3.1：{len(entries)} 主 ID"
            f"+ {MISS_LIST_EXPECTED_SUBORDINATE_TOTAL} 例从属组"
            "（主从并入入池消费留待 4.x，本表只登记可审计 ID 组）"
        ),
    )
    return written


__all__ = [
    "FROZEN_CSV_NAME",
    "FROZEN_CSV_SHA256",
    "KEY_ROLE_PRIMARY",
    "KEY_ROLE_SUBORDINATE",
    "MISS_LIST_EXPECTED_COUNT",
    "MISS_LIST_EXPECTED_SUBORDINATE_TOTAL",
    "MISS_LIST_FAMILY_PREFIX",
    "MISS_LIST_NOTE_MARKER",
    "MISS_LIST_PREFIX",
    "MISS_LIST_SOURCE_ID",
    "SOURCE_KEY_FILENAME",
    "SOURCE_KEY_TABLE",
    "MissListEntry",
    "frozen_miss_list_path",
    "load_frozen_miss_list",
    "miss_list_community_id",
    "register_miss_list_entities",
    "source_key_schema",
]
