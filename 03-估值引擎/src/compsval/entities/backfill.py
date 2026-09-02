"""WP5-E staged 事件 community_id 回填（provisional → 标准 ID）。

把 WP4 staged ``sale_event``/``listing_event`` 中 provisional 的 ``community_id``
（= 来源小区名）替换为经 WP5 实体表解析的标准小区 ID：

- 来源小区名精确命中 ``community`` 权威表的 ``standard_name`` → 标准 ID
  （:data:`BackfillOutcome.HIT_CANONICAL`）；
- 命中 ``community_alias`` 中 **CONSISTENT**（一致）别名 → 该别名所锚定的标准 ID
  （:data:`BackfillOutcome.HIT_ALIAS`）；
- 别名在 ``community_alias`` 中为 **PENDING / CONFLICT**（冲突/待定，低置信）
  → **不静默合并**，返回 ``None``（:data:`BackfillOutcome.BLOCKED`）；
- 完全未命中 → ``None``（:data:`BackfillOutcome.UNMATCHED`）。

未命中与低置信项一律登记进未匹配/冲突清单（不静默归并，README §6.2 第 1 问）。
本模块只解析名称→标准 ID，**不改写** ``community``/``community_alias`` 权威表，
也不改动任何清洗/挂牌派生逻辑（仅替换 ``community_id`` 来源，WP5-E 合同禁止项）。

本模块输入是 ``data/entities/community.parquet`` 与 ``data/entities/community_alias.parquet``
（WP5-A/B 产物）；两者缺失时查找表为**空**，回填退化为保留原 provisional 值，
以保证在不加载实体表的环境（如部分单元测试）下行为不变。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import pyarrow.parquet as pq

from compsval.config import data_dir as _default_data_dir
from compsval.contract.models import AliasConflictStatus
from compsval.entities.alias import ALIAS_FILENAME
from compsval.entities.community import COMMUNITY_FILENAME, ENTITIES_LAYER


class BackfillOutcome(Enum):
    """一次小区名→标准 ID 解析的结果类别（用于溯源与冲突登记）。"""

    HIT_CANONICAL = "标准名命中"
    HIT_ALIAS = "别名一致映射"
    BLOCKED = "低置信/冲突，不静默合并"
    UNMATCHED = "未匹配"


@dataclass(frozen=True)
class CommunityIdLookup:
    """从 community / community_alias 实体表构建的名称→标准 ID 查找表。"""

    #: 权威标准名 → (community_id, 溯源理由)
    canonical: dict[str, tuple[str, str]]
    #: CONSISTENT 别名称 → (community_id, 溯源理由)
    alias_consistent: dict[str, tuple[str, str]]
    #: PENDING/CONFLICT 别名称 → 状态（不自动合并）
    blocked: dict[str, str]

    @property
    def empty(self) -> bool:
        """实体表缺失时的空查找（回填退化为保留 provisional 值）。"""
        return not self.canonical and not self.alias_consistent and not self.blocked


def _name_key(name: str | None) -> str:
    """归一化查找键：None/空 → 空串（永不可命中），否则去除首尾空白。"""
    if not name:
        return ""
    return name.strip()


def load_community_lookup(*, data_dir: Path | None = None) -> CommunityIdLookup:
    """从 ``data/entities/`` 读取 community + community_alias 构建查找表。

    实体表不存在或为空时返回 ``empty`` 查找表。
    """
    entities_dir = (data_dir if data_dir is not None else _default_data_dir()) / ENTITIES_LAYER

    canonical: dict[str, tuple[str, str]] = {}
    community_path = entities_dir / COMMUNITY_FILENAME
    if community_path.is_file():
        table = pq.read_table(community_path)
        ids = table.column("community_id").to_pylist()
        names = table.column("standard_name").to_pylist()
        for community_id, standard_name in zip(ids, names, strict=True):
            key = _name_key(standard_name)
            if key:
                canonical[key] = (community_id, "社区权威表 standard_name 命中")

    alias_consistent: dict[str, tuple[str, str]] = {}
    blocked: dict[str, str] = {}
    alias_path = entities_dir / ALIAS_FILENAME
    if alias_path.is_file():
        table = pq.read_table(alias_path)
        source_alias = table.column("source_alias").to_pylist()
        ids = table.column("community_id").to_pylist()
        statuses = table.column("conflict_status").to_pylist()
        refs = table.column("source_ref").to_pylist()
        for alias, community_id, status, ref in zip(
            source_alias, ids, statuses, refs, strict=True
        ):
            key = _name_key(alias)
            if not key:
                continue
            if status == AliasConflictStatus.CONSISTENT.value:
                alias_consistent[key] = (community_id, ref)
            else:
                blocked[key] = status

    return CommunityIdLookup(
        canonical=canonical,
        alias_consistent=alias_consistent,
        blocked=blocked,
    )


def resolve_community_id(
    community: str | None,
    lookup: CommunityIdLookup,
) -> tuple[str | None, BackfillOutcome, str]:
    """解析一个来源小区名 → (标准 ID, 结果类别, 溯源理由)。

    - 标准名命中 → (ID, HIT_CANONICAL, 理由)
    - 一致别名命中 → (ID, HIT_ALIAS, 别名 source_ref)
    - 低置信/冲突（PENDING/CONFLICT 别名）→ (None, BLOCKED, 状态+不合并说明)，不静默合并
    - 均未命中 → (None, UNMATCHED, 说明)

    ``lookup`` 为空（实体表缺失）时不做臆测，返回 UNMATCHED/None。
    """
    key = _name_key(community)
    if key:
        if key in lookup.canonical:
            community_id, reason = lookup.canonical[key]
            return community_id, BackfillOutcome.HIT_CANONICAL, reason
        if key in lookup.blocked:
            status = lookup.blocked[key]
            return None, BackfillOutcome.BLOCKED, (
                f"别名'{key}'为{status}状态，不静默合并，需人工确认"
            )
        if key in lookup.alias_consistent:
            community_id, reason = lookup.alias_consistent[key]
            return community_id, BackfillOutcome.HIT_ALIAS, reason
    return None, BackfillOutcome.UNMATCHED, (
        f"小区'{community or ''}'在 community 权威表与社区别名库中均未命中"
    )


def collect_unmatched_conflicts(
    communities: Iterable[str | None],
    lookup: CommunityIdLookup,
) -> list[str]:
    """未匹配 / 低置信小区名清单（登记冲突、不静默归并，WP5-E 验收③）。

    对一组来源小区名逐一分辨，凡无法回填为标准 ID 的——即解析结果为
    ``BLOCKED``（PENDING/CONFLICT 低置信别名）或 ``UNMATCHED``、community_id
    为 ``None``——都收集为**去重后**的清单；``lookup`` 为空时返回空清单。
    供 ``data_stage`` 在数据质量报告中落地「未匹配清单（进冲突）」。
    """
    if lookup.empty:
        return []
    unmatched: set[str] = set()
    for community in communities:
        if not community or not community.strip():
            continue
        community_id, _outcome, _reason = resolve_community_id(community, lookup)
        if community_id is None:
            unmatched.add(community.strip())
    return sorted(unmatched)