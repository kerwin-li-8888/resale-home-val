"""WP5-B community_alias 小区别名映射与跨来源冲突登记（不静默合并）。

基于候选小区名录-V0.1.md §3 冲突清单（#1-10）与各来源小区名，登记
``community_alias`` 映射表，写入 ``data/entities/community_alias.parquet``
并附 DerivedManifest（可复现 + 溯源）：

- ``alias_id = A-<冲突项编号>-<序号>``（稳定标识，见数据字典 §3.4）；
- ``community_id`` 由冲突项所锚定的权威小区推导（沿用 WP5-A
  ``community_id_of(source_key)``，见 :mod:`compsval.entities.community`）；
- ``source_alias`` 为来源侧观察到的别名/变体名（曾用名/平台差异/分期名）；
- ``source_id`` 取 :mod:`compsval.contract.registry` 已登记来源
  （跨来源显式命名：中原=SRC-006、安居客=SRC-009；名录基底来源/房源标题类别名
  默认归房天下=SRC-005，来源定位字段标注"名录 §3#N 房源标题"）；
- ``source_ref`` 一律指向名录 §3 冲突清单项与处理建议（每行可溯源，验收②）；
- ``conflict_status`` 三分（一致/冲突/待定）：一致的别名可安全映射；冲突的相似
  命名判定为**未合并**（不同源或地址待核验，不静默归并）；待定为低置信、需人工
  复核（验收③④，README §6.2 第 1 问）。

冲突清单 #1-10 全部登记且未合并：名称类别名（#1-8）落表为 alias 行，非名称类
（#9 达镖国际中心疑似商办、#10 板块级/道路级命名）以及需人工复核的判定统一在
:func:`pending_confirmation` 清单中登记（验收①⑤）。

本模块**不改写** ``community`` 权威表，也不做任何别名到小区 ID 的自动合并。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import AliasConflictStatus, CommunityAlias
from compsval.entities.candidates import candidates_all
from compsval.entities.community import ENTITIES_LAYER, community_id_of
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)

ALIAS_TABLE = "community_alias"
ALIAS_FILENAME = f"{ALIAS_TABLE}.parquet"

#: 名录基底来源（房天下，registered SRC-005）；房源标题类别名默认归此来源。
CATALOG_SOURCE_ID = "SRC-005"
#: 跨来源显式命名：中原=SRC-006、安居客=SRC-009。
CENTANET_SOURCE_ID = "SRC-006"
ANJUKE_SOURCE_ID = "SRC-009"

#: 骨架输入：候选小区名录（DATA-001-C 交付物，2026-08-21 采集/边界确认）。
CATALOG_INPUT = InputRef(dataset="candidate_community_catalog", fetched_at="2026-08-21")

__all__ = [
    "ALIAS_FILENAME",
    "ALIAS_TABLE",
    "ANJUKE_SOURCE_ID",
    "CATALOG_INPUT",
    "CATALOG_SOURCE_ID",
    "CENTANET_SOURCE_ID",
    "ENTITIES_LAYER",
    "alias_id_of",
    "alias_schema",
    "alias_table",
    "build_alias_entity",
    "pending_confirmation",
    "write_alias_entity",
]


@dataclass(frozen=True)
class AliasMapping:
    """一条从冲突清单转录的名称类别名映射（community_alias 行）。"""

    conflict_no: int
    anchor_key: str
    source_alias: str
    source_id: str
    status: AliasConflictStatus
    note: str


@dataclass(frozen=True)
class PendingConflict:
    """待人工确认冲突清单项（验收⑤ 输出；#1-10 全部登记）。"""

    conflict_no: int
    title: str
    status: AliasConflictStatus
    action: str


# ---- 冲突清单 #1-8：名称类别名的落表转录（source_ref 指向名录 §3#N） ----
# source_key 为锚定权威小区的房天下 loupan ID；名称来自候选小区名录-V0.1.md §3。
_ALIAS_MAPPINGS: tuple[AliasMapping, ...] = (
    # #1 星汇目标区湾住宅/公寓拆分（锚定住宅 C-XXXX0188）
    AliasMapping(1, "2812279062", "星汇目标区湾", CENTANET_SOURCE_ID,
                 AliasConflictStatus.CONSISTENT,
                 "中原'星汇目标区湾'=本项目住宅名，与标准名一致"),
    AliasMapping(1, "2812279062", "示例小区242", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "房源标题变体，待核验归属（§3#1）"),
    # #2 示例小区154 vs 春晖花苑（锚定 C-XXXX0027）
    AliasMapping(2, "2811007172", "目标区示例小区154", CATALOG_SOURCE_ID,
                 AliasConflictStatus.CONSISTENT,
                 "房天下成交页标准名写法"),
    AliasMapping(2, "2811007172", "春晖花苑(目标区)", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "房源标题变体，疑似同一，待核验（§3#2）"),
    # #3 示例小区144/示例小区145/示例小区217/示例小区050（锚定 C-XXXX0116，相似命名未合并）
    AliasMapping(3, "2811341104", "示例小区145", CATALOG_SOURCE_ID,
                 AliasConflictStatus.CONFLICT,
                 "相似命名不同小区（C-XXXX0182），未合并（§3#3）"),
    AliasMapping(3, "2811341104", "示例小区217", CATALOG_SOURCE_ID,
                 AliasConflictStatus.CONFLICT,
                 "相似命名不同小区（C-XXXX0023），未合并（§3#3）"),
    AliasMapping(3, "2811341104", "示例小区050", CATALOG_SOURCE_ID,
                 AliasConflictStatus.CONFLICT,
                 "相似命名不同小区（C-XXXX0022），未合并（§3#3）"),
    # #4 示例小区169 vs 示例小区170（锚定 C-XXXX0020）
    AliasMapping(4, "2811007074", "示例小区170", CATALOG_SOURCE_ID,
                 AliasConflictStatus.CONFLICT,
                 "相邻相似命名不同小区（C-XXXX0021），未合并（§3#4）"),
    # #5 示例小区029 vs 二期（锚定 C-XXXX0065）
    AliasMapping(5, "2811040556", "示例小区008", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "同项目分期（C-XXXX0151），待确认保留或并入（§3#5）"),
    # #6 金汐花园系列（锚定 C-XXXX0083，金汐分期/开发商差异）
    AliasMapping(6, "2811175216", "示例小区039", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "金汐分期（C-XXXX0067），待确认归属（§3#6）"),
    AliasMapping(6, "2811175216", "示例小区053", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "金汐分期（C-XXXX0097），待确认归属（§3#6）"),
    # #7 示例小区132系列（锚定 C-XXXX0069，清晏分期）
    AliasMapping(7, "2811052010", "示例小区132榕岸华庭(E区)", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "清晏分期（C-XXXX0184），待确认归属（§3#7）"),
    AliasMapping(7, "2811052010", "示例小区132榕景四季(D区)", CATALOG_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "清晏分期（C-XXXX0128），待确认归属（§3#7）"),
    # #8 示例小区202板块归属分歧（锚定 C-XXXX0052）
    AliasMapping(8, "2811021754", "示例小区202", ANJUKE_SOURCE_ID,
                 AliasConflictStatus.PENDING,
                 "安居客商圈=滨江中 vs 房天下=滨江西，板块待核（§3#8）"),
)

# ---- 待人工确认清单：#1-10 全部登记（验收①⑤）；非名称类冲突在此登记 ----
_PENDING_CONFLICTS: tuple[PendingConflict, ...] = (
    PendingConflict(1, "星汇目标区湾住宅/公寓拆分", AliasConflictStatus.PENDING,
                    "确认住宅/公寓归属与产品类型；公寓(C-XXXX0084) 已 OUT_OF_SCOPE 单列，"
                    "不纳入普通住宅估值"),
    PendingConflict(2, "示例小区154 vs 春晖花苑", AliasConflictStatus.PENDING,
                    "核验两写法是否同一小区，确认后再决定是否合并"),
    PendingConflict(3, "示例小区144/示例小区145/示例小区217/示例小区050", AliasConflictStatus.CONFLICT,
                    "需核验地址区分多个相似命名小区，未合并"),
    PendingConflict(4, "示例小区169 vs 示例小区170", AliasConflictStatus.CONFLICT,
                    "核验相邻相似命名，确认后决定是否区分"),
    PendingConflict(5, "示例小区029 vs 二期", AliasConflictStatus.PENDING,
                    "确认是否作为分期保留或并入母小区"),
    PendingConflict(6, "金汐花园系列", AliasConflictStatus.PENDING,
                    "确认分期/开发商差异的映射方式"),
    PendingConflict(7, "示例小区132系列", AliasConflictStatus.PENDING,
                    "确认分期（E区/D区）的映射方式"),
    PendingConflict(8, "示例小区202板块归属", AliasConflictStatus.PENDING,
                    "核验商圈归属（房天下滨江西/安居客滨江中），影响边界与竞争关系"),
    PendingConflict(9, "达镖国际中心疑似商办/公寓", AliasConflictStatus.PENDING,
                    "识别使用性质（官方样本 DATA-001-B：39/51层、1室1厅0厨）；"
                    "无候选 loupan ID，未入 alias，待核后决定纳入方式"),
    PendingConflict(10, "板块级/道路级命名", AliasConflictStatus.PENDING,
                    "对应社区已 BOUNDARY_PENDING（示例小区017等），实体识别时排除或单列"),
)


def pending_confirmation() -> list[PendingConflict]:
    """登记冲突清单 #1-10 的待人工确认清单（验收①⑤ 输出）。"""
    return list(_PENDING_CONFLICTS)


def alias_id_of(conflict_no: int, seq: int) -> str:
    """别名唯一 ID：``A-<冲突项编号>-<序号>``（数据字典 §3.4 主键）。"""
    return f"A-{conflict_no}-{seq}"


def alias_schema() -> pa.Schema:
    """``community_alias`` 实体表 PyArrow 模式（对应数据字典 §3.4 字段）。"""
    return pa.schema(
        [
            pa.field("alias_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("source_alias", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
            pa.field("conflict_status", pa.string(), nullable=False),
        ]
    )


def _resolve_conflicts(mappings: Sequence[AliasMapping]) -> tuple[CommunityAlias, ...]:
    """把冲突转录映射解析为 CommunityAlias 实体行。

    - 校验锚定 source_key 必须存在于候选小区名录（链表查询 O(n)，数据量小）；
    - ``source_ref`` 指向目录 §3 冲突清单项与处理理由（每行可溯源，验收②）。
    """
    known_keys = {c.source_key for c in candidates_all()}
    resolved: list[CommunityAlias] = []
    seq_by_conflict: dict[int, int] = {}
    for m in mappings:
        if m.anchor_key not in known_keys:
            raise AssertionError(
                f"冲突 #{m.conflict_no} 锚定小区 {m.anchor_key} 不在候选名录，无法建立 alias"
            )
        seq = seq_by_conflict.get(m.conflict_no, 0) + 1
        seq_by_conflict[m.conflict_no] = seq
        resolved.append(
            CommunityAlias(
                alias_id=alias_id_of(m.conflict_no, seq),
                community_id=community_id_of(m.anchor_key),
                source_alias=m.source_alias,
                source_id=m.source_id,
                source_ref=(
                    f"候选小区名录-V0.1.md §3 #{m.conflict_no}：{m.note}"
                ),
                conflict_status=m.status,
            )
        )
    return tuple(resolved)


def alias_table(aliases: Sequence[CommunityAlias]) -> pa.Table:
    """把 community_alias 实体序列构造成与 :func:`alias_schema` 一致的 PyArrow 表。"""
    rows: dict[str, list[object]] = {name: [] for name in alias_schema().names}
    for alias in aliases:
        row_values: dict[str, object] = {
            "alias_id": alias.alias_id,
            "community_id": alias.community_id,
            "source_alias": alias.source_alias,
            "source_id": alias.source_id,
            "source_ref": alias.source_ref,
            "conflict_status": alias.conflict_status.value,
        }
        if list(row_values) != list(rows):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in rows:
            rows[name].append(row_values[name])
    return pa.table(rows, schema=alias_schema())


def write_alias_entity(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """把 community_alias 实体表及其 DerivedManifest 原子写入 ``data/entities/``。

    与 community.py 的写盘纪律一致：先写 ``.incomplete`` 兄弟文件再重命名。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / ALIAS_FILENAME
    work_path = entities_dir / (ALIAS_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=ALIAS_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=list(inputs),
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def build_alias_entity(
    *,
    data_dir: Path,
    notes: str | None = None,
) -> Path:
    """从候选名录冲突清单构建并写入 community_alias 实体表（WP5-B 主入口）。

    返回写入的 parquet 路径；行数 = 名称类别名映射行数（冲突 #1-8），完整冲突
    清单 #1-10 由 :func:`pending_confirmation` 登记（含 #9/#10 非名称类）。
    """
    aliases = _resolve_conflicts(_ALIAS_MAPPINGS)
    table = alias_table(aliases)
    return write_alias_entity(
        table,
        data_dir=data_dir,
        inputs=[CATALOG_INPUT],
        notes=notes,
    )