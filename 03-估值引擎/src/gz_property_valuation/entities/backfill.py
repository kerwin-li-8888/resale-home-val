"""WP5-E staged 事件 community_id/sub_area 回填（norm-v2，provisional → 标准身份）。

把 WP4 staged ``sale_event``/``listing_event`` 中 provisional 的 ``community_id``
（= 来源小区名）替换为经实体表解析的标准身份 ``(community_id, sub_area)``。

解析语义（norm-v2，与 census v1.3 普查管线一致，边界见 change
``norm-v2-valuation-ingest``；来源ID捷径层仅普查输入具备，入库不设）：

- 非目标区行政区括注（如 ``(临湖区)``）→ 整行排除
  （:data:`BackfillOutcome.EXCLUDED_OUT_OF_REGION`），单独登记不入池；
- 标准名命中（``merged`` 实体跳过）→ 标准 ID；
- 子区 ``match_names`` 命中（「小区名+子区名」连写或裸子区名）→ ``(标准 ID, 子区)``；
- blocked（待定/冲突/排除别名）→ 不静默合并
  （:data:`BackfillOutcome.BLOCKED`；位置在子区层之后、别名层之前）；
- 一致别名与来源注册表补充层（如链家 ``LIANJIA_COMMUNITY_REGISTRY``，经
  :func:`gz_property_valuation.ingest.marts_build.lianjia_extended_lookup` 注入）
  命中 → 标准 ID；
- 任何层命中 ``merged`` 实体一律经 redirect 转发到承接 ``(community_id, sub_area)``；
- 多义命中（同一键多个目标）不自动解析（UNMATCHED + 留痕）；完全未命中 → UNMATCHED。

比对副本做 NFKC 归一、空白剥离与分期/区标括注剥离，云溪行政区括注等同裸名。
映射结构镜像 census：键 → ``{目标: 溯源}`` 字典，保留多义以供检测。
本模块只解析名称→标准身份，**不改写**实体权威表；实体表缺失时查找表为**空**，
回填退化为保留原 provisional 值。
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import pyarrow.parquet as pq

from gz_property_valuation.config import data_dir as _default_data_dir
from gz_property_valuation.contract.models import AliasConflictStatus
from gz_property_valuation.entities.alias import ALIAS_FILENAME
from gz_property_valuation.entities.community import COMMUNITY_FILENAME, ENTITIES_LAYER
from gz_property_valuation.entities.community_family import SUBAREA_FILENAME

#: 子区登记表（v1.3 起可选：缺失时子区层为空，解析退化为小区层）
_SUBAREA_PATH = SUBAREA_FILENAME

#: 分期/区标括注（比对副本剥离）：``(A区)``/``(二期)``/``(B2期)`` 等结尾形态
_PHASE_SUFFIX = re.compile(
    r"[（(](?:[A-Za-z]{1,3}|[0-9]{1,2}|[一二三四五六七八九十]{1,2})?(?:区|期)[)）]$"
)
#: 目标区（云溪）行政区括注（比对副本剥离、等同裸名）
_TARGET_DISTRICT_SUFFIX = re.compile(r"[（(]云溪区?[)）]$")
#: 任意括注内容（用于外区判定）
_BRACKET = re.compile(r"[（(]([^（）()]+)[)）]")
#: 示例城市行政区（含简写，虚构名）；括注命中且非目标区（云溪）→ 整行排除
NON_TARGET_DISTRICTS = {
    "南亭",
    "临湖",
    "花桥",
    "增江",
    "从岭",
    "荔港",
    "越城",
    "云山",
    "天泽",
    "澜川",
}


class BackfillOutcome(Enum):
    """一次小区名→标准身份解析的结果类别（用于溯源与冲突登记）。"""

    HIT_CANONICAL = "标准名命中"
    HIT_ALIAS = "别名一致映射"
    BLOCKED = "低置信/冲突，不静默合并"
    EXCLUDED_OUT_OF_REGION = "外区括注，整行排除"
    UNMATCHED = "未匹配"


def base_norm(name: str) -> str:
    """基础归一化键：NFKC 折叠 + 移除全部空白。"""
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", name))


def cmp_norm(name: str) -> str:
    """比对副本键：基础归一化后再剥离分期/区标括注与云溪行政区括注。"""
    t = _PHASE_SUFFIX.sub("", base_norm(name))
    return _TARGET_DISTRICT_SUFFIX.sub("", t)


def outside_region_bracket(name: str) -> str | None:
    """源名括注为示例城市非目标区行政区时返回括注内容，否则 ``None``。"""
    m = _BRACKET.search(base_norm(name))
    if not m:
        return None
    content = m.group(1)
    core = content[:-1] if content.endswith("区") else content
    if content in {"云溪", "云溪区"} or core in {"云溪"}:
        return None
    if core in NON_TARGET_DISTRICTS:
        return content
    return None


@dataclass(frozen=True)
class CommunityIdLookup:
    """从实体表构建的名称→标准身份查找表（norm-v2 五层，键→{目标:溯源} 保多义）。"""

    #: 比对副本键(活跃标准名) → {community_id: 溯源理由}
    canonical: dict[str, dict[str, str]]
    #: 一致别名（比对副本键）→ {community_id: source_ref}
    alias_consistent: dict[str, dict[str, str]]
    #: blocked（待定/冲突/排除）别名（基础键）→ 状态（不自动映射）
    blocked: dict[str, str]
    #: 子区 match_names（比对副本键）→ {(community_id, sub_area): 命中登记名}
    subarea: dict[str, dict[tuple[str, str], str]] = field(default_factory=dict)
    #: merged 实体 → (承接 community_id, 承接 sub_area)
    redirect: dict[str, tuple[str, str]] = field(default_factory=dict)
    #: 来源注册表补充层（比对副本键）→ {community_id: 溯源理由}；默认空，由来源侧
    #: 包装（如 lianjia_extended_lookup）注入，键 SHALL NOT 覆盖一致别名
    alias_registry: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def empty(self) -> bool:
        """实体表缺失时的空查找（回填退化为保留 provisional 值）。"""
        return not (
            self.canonical
            or self.alias_consistent
            or self.blocked
            or self.subarea
            or self.redirect
            or self.alias_registry
        )


def _name_key(name: str | None) -> str:
    """归一化查找键：None/空 → 空串（永不可命中），否则去除首尾空白。"""
    if not name:
        return ""
    return name.strip()


def load_community_lookup(*, data_dir: Path | None = None) -> CommunityIdLookup:
    """从 ``data/entities/`` 读取实体表构建 norm-v2 查找表。

    实体表不存在或为空时返回 ``empty`` 查找表；子区表（v1.3）缺失时子区层
    与 redirect 层为空（兼容 v1.3 之前的实体表产物）。
    """
    entities_dir = (data_dir if data_dir is not None else _default_data_dir()) / ENTITIES_LAYER

    canonical: dict[str, dict[str, str]] = {}
    redirect: dict[str, tuple[str, str]] = {}
    community_path = entities_dir / COMMUNITY_FILENAME
    if community_path.is_file():
        table = pq.read_table(community_path)
        ids = table.column("community_id").to_pylist()
        names = table.column("standard_name").to_pylist()
        has_status = "entity_status" in table.column_names
        statuses = (
            table.column("entity_status").to_pylist()
            if has_status
            else ["active"] * table.num_rows
        )
        redirect_cids = (
            table.column("redirect_community_id").to_pylist()
            if "redirect_community_id" in table.column_names
            else ["UNKNOWN"] * table.num_rows
        )
        redirect_subs = (
            table.column("redirect_subarea_name").to_pylist()
            if "redirect_subarea_name" in table.column_names
            else ["UNKNOWN"] * table.num_rows
        )
        for cid, name, status, rc, rsub in zip(
            ids, names, statuses, redirect_cids, redirect_subs, strict=True
        ):
            if status == "merged":
                if rc != "UNKNOWN":
                    redirect[str(cid)] = (str(rc), str(rsub))
                continue
            c = cmp_norm(str(name))
            if c:
                canonical.setdefault(c, {})[str(cid)] = "社区权威表 standard_name 命中"

    subarea: dict[str, dict[tuple[str, str], str]] = {}
    subarea_path = entities_dir / _SUBAREA_PATH
    if subarea_path.is_file():
        table = pq.read_table(subarea_path)
        for cid, sa, names in zip(
            table.column("community_id").to_pylist(),
            table.column("sub_area_name").to_pylist(),
            table.column("match_names").to_pylist(),
            strict=True,
        ):
            for raw in str(names).split("|"):
                c = cmp_norm(raw)
                if c:
                    subarea.setdefault(c, {}).setdefault((str(cid), str(sa)), raw)

    alias_consistent: dict[str, dict[str, str]] = {}
    blocked: dict[str, str] = {}
    alias_path = entities_dir / ALIAS_FILENAME
    if alias_path.is_file():
        table = pq.read_table(alias_path)
        source_alias = table.column("source_alias").to_pylist()
        ids = table.column("community_id").to_pylist()
        statuses = table.column("conflict_status").to_pylist()
        refs = table.column("source_ref").to_pylist()
        for a_name, cid, status, ref in zip(
            source_alias, ids, statuses, refs, strict=True
        ):
            key = _name_key(a_name)
            if not key:
                continue
            if status == AliasConflictStatus.CONSISTENT.value:
                alias_consistent.setdefault(cmp_norm(key), {}).setdefault(
                    str(cid), str(ref)
                )
            else:
                blocked.setdefault(base_norm(key), status)

    return CommunityIdLookup(
        canonical=canonical,
        alias_consistent=alias_consistent,
        blocked=blocked,
        subarea=subarea,
        redirect=redirect,
    )


def _after_redirect(
    lookup: CommunityIdLookup, cid: str, sub_area: str | None
) -> tuple[str, str | None]:
    """命中实体为 ``merged`` 时转发到承接 ``(community_id, sub_area)``。"""
    rc = lookup.redirect.get(cid)
    if rc is not None:
        return rc[0], rc[1]
    return cid, sub_area


def _single_or_none(mapping: dict) -> tuple | None:
    """字典恰有一个条目 → 返回该条目；多个条目 → 返回多义哨兵（``...``）。"""
    if not mapping:
        return None
    if len(mapping) == 1:
        return next(iter(mapping.items()))
    return ...  # 多义：不自动解析


def resolve_community_id(
    community: str | None,
    lookup: CommunityIdLookup,
) -> tuple[str | None, str | None, BackfillOutcome, str]:
    """解析一个来源小区名 → (标准 ID, 子区, 结果类别, 溯源理由)。

    解析顺序（norm-v2 / design D8）：外区括注排除 → 标准名（跳过 merged）
    → 子区 match_names → blocked 检查 → 一致别名与来源注册表补充层 → UNMATCHED；
    任何层命中统一经 redirect 转发；多义命中不自动解析（UNMATCHED + 留痕）。
    ``lookup`` 为空（实体表缺失）时不做臆测，返回 UNMATCHED。
    """
    key = _name_key(community)
    if not key or lookup.empty:
        return (
            None,
            None,
            BackfillOutcome.UNMATCHED,
            f"小区'{community or ''}'在 community 权威表与社区别名库中均未命中",
        )

    excluded = outside_region_bracket(key)
    if excluded is not None:
        return (
            None,
            None,
            BackfillOutcome.EXCLUDED_OUT_OF_REGION,
            f"源名括注'{excluded}'为非目标区行政区，整行排除不入池",
        )

    c = cmp_norm(key)

    # 标准名层：唯一命中即解析；多义按普查语义落入后续层级（不在此拦截）
    std_hits = lookup.canonical.get(c)
    if std_hits is not None and len(std_hits) == 1:
        cid = next(iter(std_hits))
        cid, sub = _after_redirect(lookup, cid, None)
        return cid, sub, BackfillOutcome.HIT_CANONICAL, "标准名命中"

    # 子区层
    sub_hit = _single_or_none(lookup.subarea.get(c) or {})
    if sub_hit is not None:
        if sub_hit is not ...:
            (cid, sa), raw = sub_hit
            forwarded = cid in lookup.redirect
            cid, sub = _after_redirect(lookup, cid, sa)
            return (
                cid,
                sub,
                BackfillOutcome.HIT_CANONICAL,
                f"子区 match_names 命中：{raw}" + ("，经合并转发到承接子区" if forwarded else ""),
            )
        return (
            None,
            None,
            BackfillOutcome.UNMATCHED,
            f"小区'{key}'子区多义命中，不自动解析，需人工裁决",
        )

    # blocked 层（D8：子区层之后、别名/注册表补充层之前）
    status = lookup.blocked.get(base_norm(key))
    if status is not None:
        return (
            None,
            None,
            BackfillOutcome.BLOCKED,
            f"别名'{key}'为{status}状态，不静默合并，需人工确认",
        )

    # 一致别名 + 来源注册表补充层（合并层，按 community_id 判唯一；别名表优先）
    merged_layer: dict[str, str] = dict(lookup.alias_registry.get(c) or {})
    merged_layer.update(lookup.alias_consistent.get(c) or {})
    alias_hit = _single_or_none(merged_layer)
    if alias_hit is not None:
        if alias_hit is not ...:
            cid, reason = alias_hit
            forwarded = cid in lookup.redirect
            cid, sub = _after_redirect(lookup, cid, None)
            via = "一致别名命中" if (lookup.alias_consistent.get(c) or {}) else "来源注册表补充层命中"
            return (
                cid,
                sub,
                BackfillOutcome.HIT_ALIAS,
                f"{via}：{reason}" + ("，经合并转发到承接子区" if forwarded else ""),
            )
        return (
            None,
            None,
            BackfillOutcome.UNMATCHED,
            f"小区'{key}'别名/注册表层多义命中，不自动解析，需人工裁决",
        )

    return (
        None,
        None,
        BackfillOutcome.UNMATCHED,
        f"小区'{community or ''}'在 community 权威表与社区别名库中均未命中",
    )


def collect_unmatched_conflicts(
    communities: Iterable[str | None],
    lookup: CommunityIdLookup,
) -> list[str]:
    """未匹配 / 低置信小区名清单（登记冲突、不静默归并）。

    对一组来源小区名逐一分辨，凡无法回填为标准身份且**不属于外区括注排除**
    的——即解析结果为 ``BLOCKED``（PENDING/CONFLICT 低置信别名）或 ``UNMATCHED``
    ——都收集为**去重后**的清单；``lookup`` 为空时返回空清单。
    外区括注排除行由 :func:`collect_excluded_out_of_region` 单独收集（分册登记）。
    """
    if lookup.empty:
        return []
    unmatched: set[str] = set()
    for community in communities:
        if not community or not community.strip():
            continue
        cid, _sub, outcome, _reason = resolve_community_id(community, lookup)
        if cid is None and outcome is not BackfillOutcome.EXCLUDED_OUT_OF_REGION:
            unmatched.add(community.strip())
    return sorted(unmatched)


def classify_unresolved(
    communities: Iterable[str | None],
    lookup: CommunityIdLookup,
) -> dict[str, list[str]]:
    """未解析小区名三分册（数据质量报告分列呈现）。

    返回 ``{"excluded_out_of_region": [...], "blocked": [...], "unmatched": [...]}``，
    各册去重排序；命中标准身份的行不进任何册。
    """
    buckets: dict[str, list[str]] = {
        "excluded_out_of_region": [],
        "blocked": [],
        "unmatched": [],
    }
    if lookup.empty:
        return buckets
    seen: dict[str, set[str]] = {k: set() for k in buckets}
    for community in communities:
        if not community or not community.strip():
            continue
        cid, _sub, outcome, _reason = resolve_community_id(community, lookup)
        if cid is not None:
            continue
        key = community.strip()
        if outcome is BackfillOutcome.EXCLUDED_OUT_OF_REGION:
            seen["excluded_out_of_region"].add(key)
        elif outcome is BackfillOutcome.BLOCKED:
            seen["blocked"].add(key)
        else:
            seen["unmatched"].add(key)
    for name, values in seen.items():
        buckets[name] = sorted(values)
    return buckets


def collect_excluded_out_of_region(
    communities: Iterable[str | None],
    lookup: CommunityIdLookup,
) -> list[str]:
    """外区括注排除名清单（与未匹配清单分册登记，任务 3.6 分类呈现）。"""
    if lookup.empty:
        return []
    excluded: set[str] = set()
    for community in communities:
        if not community or not community.strip():
            continue
        _cid, _sub, outcome, _reason = resolve_community_id(community, lookup)
        if outcome is BackfillOutcome.EXCLUDED_OUT_OF_REGION:
            excluded.add(community.strip())
    return sorted(excluded)


__all__ = [
    "BackfillOutcome",
    "CommunityIdLookup",
    "NON_TARGET_DISTRICTS",
    "base_norm",
    "cmp_norm",
    "collect_excluded_out_of_region",
    "collect_unmatched_conflicts",
    "load_community_lookup",
    "outside_region_bracket",
    "resolve_community_id",
]
