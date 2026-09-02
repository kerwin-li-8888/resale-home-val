"""WP5-F ScopePolicy 适用范围判断（VAL1-001）：纳入/参考/拒绝。

落地适用范围判断：判断一个小区/房源是否进入正式估值范围。三类判定
（纳入/参考/拒绝）的输入为 community 的 ``boundary_status``（DATA-001-C
边界三分）+ 房产类型 + DATA-001 逐小区支撑等级（11 个冻结可实施 ID）：

- **纳入（INCLUDE）**：机器确认 + DATA-001 可支撑（12 个月 ≥15 案例），
  可进入正式估值范围；
- **参考（REFERENCE）**：边界待定（不能静默纳入）或 有条件支撑
  （案例 8-14，参考级），只可参考性分析、不得伪装成正式估值（README §3.3）；
- **拒绝（REJECT）**：正式范围外（含公寓/非住宅，README §3.3 排除类）、
  非普通住宅房产类型、或暂不支撑（无成交样本，补数后重估）。

规则以 ``rule_version`` 版本化：每条范围判定行都记录 rule_version；规则
改动时以新版本生成新输出文件，旧版本结果保留、不覆盖（验收④）。范围外对象
绝不能进入正式估值（验收②：反例测试）；边界待定/数据稀疏走参考或拒绝、不
静默纳入（验收③）。

范围清单由 community 实体权威表（``data/entities/community.parquet``）+
DATA-001 §4 冻结集合（G3R-D v1.1：8 可支撑 + 4 有条件）派生，写入
``data/entities/scope_policy_v<version>.parquet``
并附 DerivedManifest（可复现 + 溯源）。本模块**不做估值计算**，只做范围判定。
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import BoundaryStatus
from compsval.entities.community import (
    COMMUNITY_FILENAME,
    ENTITIES_LAYER,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER

#: 当前规则版本（验收④：改动规则 → 递增版本，生成新输出不覆盖旧结果）。
DEFAULT_RULE_VERSION = "1.1"

#: 当前生效的 ScopePolicy 版本（formal 纳入名单数据源）。
#: 2026-09-01 由 user 正式基线确认启用 v1.2
#: （基线确认：00-项目总控/基线确认-ScopePolicy-v1.2与ext入表-20260901.md）；
#: 启用前闸门维持读取 v1.1（见 scope-policy-rebaseline spec 生效治理）。
ACTIVE_SCOPE_POLICY_VERSION = "1.2"

#: 范围清单输出文件前缀（版本化文件名：scope_policy_v<version>.parquet）。
SCOPE_POLICY_PREFIX = "scope_policy_v"


class ScopeDecision(StrEnum):
    """适用范围三类判定（VAL1-001）。"""

    INCLUDE = "纳入"
    REFERENCE = "参考"
    REJECT = "拒绝"


class SupportLevel(StrEnum):
    """DATA-001 逐小区支撑等级（§4.1-4.3，初判判据）。"""

    SUPPORTED = "可支撑"
    CONDITIONAL = "有条件支撑"
    UNSUPPORTED = "暂不支撑"


class PropertyType(StrEnum):
    """房产类型（README §3.3 房产范围；默认普通住宅）。"""

    ORDINARY_RESIDENTIAL = "普通住宅"
    NON_RESIDENTIAL = "非普通住宅"


#: DATA-001 §4.1 可支撑第一阶段（12 个月 ≥15）——7 个，本轮冻结。
#: G3R-D v1.1（2026-08-23）：合并后 12 个月真实案例数重定级，新增示例小区130
#: （18 案例，12m≥15）；其余补数小区低于阈值未纳入（拾光里 7/示例小区166 5/
#: 示例小区136 7/示例小区203 5，均 <8 → 维持暂不支撑）。既有 7 个不因新数据降级。
_SUPPORTED_IDS: frozenset[str] = frozenset(
    {
        "C-XXXX0069",  # 示例小区132
        "C-XXXX0048",  # 示例小区121
        "C-XXXX0052",  # 示例小区202
        "C-XXXX0053",  # 示例小区041（新港西西段）
        "C-XXXX0188",  # 示例小区026
        "C-XXXX0105",  # 示例小区219
        "C-XXXX0009",  # 示例小区031
        "C-XXXX0063",  # 示例小区130（G3R-D 新增，12个月 18 案例）
    }
)

#: DATA-001 §4.2 有条件支撑（案例 8-14 或累计多近期少）——4 个，本轮冻结。
_CONDITIONAL_IDS: frozenset[str] = frozenset(
    {
        "C-XXXX0077",  # 示例小区193（8）
        "C-XXXX0120",  # 示例小区045（12）
        "C-XXXX0013",  # 示例小区177（11）
        "C-XXXX0027",  # 示例小区154（累计28但12月5→按近期口径降级为参考）
    }
)

#: DATA-001 §4 本轮冻结可实施集合（G1）：7 可支撑 + 4 有条件 = 11 个小区 ID。
#: G3R-D v1.1：可支撑扩为 8 个（新增示例小区130），有条件维持 4 个。
FROZEN_SUPPORTED_IDS: frozenset[str] = frozenset(_SUPPORTED_IDS)
FROZEN_CONDITIONAL_IDS: frozenset[str] = frozenset(_CONDITIONAL_IDS)


#: 骨架输入：候选小区名录（community 权威表上游，SRC-005，2026-08-21）。
CATALOG_INPUT = InputRef(dataset="candidate_community_catalog", fetched_at="2026-08-21")
#: 数据支撑输入：DATA-001 可行性审计报告 §4（11 个冻结可实施 ID，2026-08-21）。
AUDIT_INPUT = InputRef(dataset="data_feasibility_audit_G1_frozen", fetched_at="2026-08-21")
#: G3R-D 定级输入：合并后 valid_sale 真实 12 个月案例分布（质量报告 §4，2026-08-23）。
G3R_MERGE_INPUT = InputRef(dataset="merged_valid_sale_g3r_quality", fetched_at="2026-08-23")


@dataclass(frozen=True)
class ScopePolicy:
    """适用范围判断规则集（带 rule_version，验收④）。

    冻结 DATA-001 支撑集合 + 规则版本；同版本下同一输入产出同一判定，
    规则改动以新版本落地、不覆盖旧结果。
    """

    rule_version: str
    supported_ids: frozenset[str]
    conditional_ids: frozenset[str]


@dataclass(frozen=True)
class ScopeVerdict:
    """一次适用范围判定的结果（含理由，可溯源）。"""

    decision: ScopeDecision
    reason: str


def default_policy() -> ScopePolicy:
    """当前版本默认规则集（DATA-001 §4 冻结集合 + DEFAULT_RULE_VERSION）。"""
    return ScopePolicy(
        rule_version=DEFAULT_RULE_VERSION,
        supported_ids=FROZEN_SUPPORTED_IDS,
        conditional_ids=FROZEN_CONDITIONAL_IDS,
    )


def support_level_of(community_id: str, policy: ScopePolicy | None = None) -> SupportLevel:
    """community 的数据支撑等级（DATA-001 §4；不在冻结集合 → 暂不支撑）。"""
    policy = policy or default_policy()
    if community_id in policy.supported_ids:
        return SupportLevel.SUPPORTED
    if community_id in policy.conditional_ids:
        return SupportLevel.CONDITIONAL
    return SupportLevel.UNSUPPORTED


def evaluate(
    *,
    community_id: str,
    boundary_status: BoundaryStatus,
    property_type: PropertyType = PropertyType.ORDINARY_RESIDENTIAL,
    policy: ScopePolicy | None = None,
) -> ScopeVerdict:
    """对一个小区/房源执行适用范围判定（验收①，机器可执行）。

    判定顺序（先排除、再边界、最后数据支撑）：
    1. 非普通住宅房产类型 → 拒绝（README §3.3 排除类，验收②反例）；
    2. ``OUT_OF_SCOPE`` → 拒绝（正式范围外，验收②反例）；
    3. ``BOUNDARY_PENDING`` → 参考（边界待定不静默纳入，验收③）；
    4. 机器确认：可支撑 → 纳入；有条件支撑 → 参考（验收③）；否则 → 拒绝。
    """
    policy = policy or default_policy()
    if property_type is not PropertyType.ORDINARY_RESIDENTIAL:
        return ScopeVerdict(
            ScopeDecision.REJECT,
            "非普通住宅（公寓/商办/车位/别墅等，README §3.3 排除类，不进入正式估值）",
        )
    if boundary_status is BoundaryStatus.OUT_OF_SCOPE:
        return ScopeVerdict(
            ScopeDecision.REJECT, "正式范围外（DATA-001-C 边界三分），不进入正式估值"
        )
    if boundary_status is BoundaryStatus.BOUNDARY_PENDING:
        return ScopeVerdict(
            ScopeDecision.REFERENCE, "边界待定，不静默纳入正式范围，仅参考级"
        )
    if community_id in policy.supported_ids:
        return ScopeVerdict(
            ScopeDecision.INCLUDE, "机器确认 + DATA-001 可支撑（12个月≥15），进入正式估值"
        )
    if community_id in policy.conditional_ids:
        return ScopeVerdict(
            ScopeDecision.REFERENCE, "有条件支撑（案例8-14），参考级不纳入正式"
        )
    return ScopeVerdict(
        ScopeDecision.REJECT, "暂不支撑/无成交样本，正式估值范围外（补数后重估）"
    )


def scope_policy_schema() -> pa.Schema:
    """``scope_policy`` 范围清单 PyArrow 模式（判定 + 溯源 + 版本）。"""
    return pa.schema(
        [
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("standard_name", pa.string(), nullable=False),
            pa.field("block", pa.string(), nullable=False),
            pa.field("boundary_status", pa.string(), nullable=False),
            pa.field("support_level", pa.string(), nullable=False),
            pa.field("property_type", pa.string(), nullable=False),
            pa.field("scope_decision", pa.string(), nullable=False),
            pa.field("reason", pa.string(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
        ]
    )


def scope_policy_table(
    community_table: pa.Table,
    policy: ScopePolicy | None = None,
) -> pa.Table:
    """对 community 实体表逐行判定 → 范围清单表（与 schema 一致）。"""
    policy = policy or default_policy()
    boundary_values = community_table.column("boundary_status").to_pylist()
    rows: dict[str, list[object]] = {
        name: [] for name in scope_policy_schema().names
    }
    for i in range(community_table.num_rows):
        community_id = community_table.column("community_id")[i].as_py()
        boundary_status = BoundaryStatus(boundary_values[i])
        verdict = evaluate(
            community_id=community_id,
            boundary_status=boundary_status,
            policy=policy,
        )
        row_values: dict[str, object] = {
            "community_id": community_id,
            "standard_name": community_table.column("standard_name")[i].as_py(),
            "block": community_table.column("block")[i].as_py(),
            "boundary_status": boundary_status.value,
            "support_level": support_level_of(community_id, policy).value,
            "property_type": PropertyType.ORDINARY_RESIDENTIAL.value,
            "scope_decision": verdict.decision.value,
            "reason": verdict.reason,
            "rule_version": policy.rule_version,
            "source_ref": community_table.column("source_ref")[i].as_py(),
        }
        if list(row_values) != list(rows):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in rows:
            rows[name].append(row_values[name])
    return pa.table(rows, schema=scope_policy_schema())


def scope_policy_filename(rule_version: str) -> str:
    """版本化输出文件名：``scope_policy_v<version>.parquet``（验收④：新版本不覆盖旧结果）。"""
    return f"{SCOPE_POLICY_PREFIX}{rule_version}.parquet"


def write_scope_policy(
    table: pa.Table,
    *,
    data_dir: Path,
    rule_version: str,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """把范围清单及其 DerivedManifest 原子写入 ``data/entities/``。

    与 community/alias/building/market_series 写盘纪律一致：先写 ``.incomplete``
    再重命名，避免半写表冒充完整派生表。文件名带 rule_version，旧版本文件保留。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / scope_policy_filename(rule_version)
    work_path = entities_dir / (scope_policy_filename(rule_version) + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=f"scope_policy_v{rule_version}",
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=list(inputs),
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def build_scope_policy(
    *,
    data_dir: Path,
    rule_version: str = DEFAULT_RULE_VERSION,
    notes: str | None = None,
) -> Path:
    """从 community 实体权威表 + DATA-001 冻结集合构建范围清单（WP5-F 主入口）。

    读取 ``data/entities/community.parquet``（community 表缺失时抛
    ``FileNotFoundError``），逐行判定纳入/参考/拒绝，写入版本化范围清单。
    返回写入的 parquet 路径。
    """
    community_path = data_dir / ENTITIES_LAYER / COMMUNITY_FILENAME
    community_table = pq.read_table(community_path)
    policy = default_policy()
    if rule_version != DEFAULT_RULE_VERSION:
        policy = ScopePolicy(
            rule_version=rule_version,
            supported_ids=policy.supported_ids,
            conditional_ids=policy.conditional_ids,
        )
    table = scope_policy_table(community_table, policy)
    inputs = [CATALOG_INPUT, AUDIT_INPUT]
    if policy.rule_version == "1.1":
        # v1.1 定级含 G3R 合并数据证据（真实 12 个月案例分布）
        inputs.append(G3R_MERGE_INPUT)
    return write_scope_policy(
        table,
        data_dir=data_dir,
        rule_version=policy.rule_version,
        inputs=inputs,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# ScopePolicy v1.2 数据驱动重定级（ext-sale-ingest-scope-v1-2，P3）
# ---------------------------------------------------------------------------


#: v1.2 重算数据截点（冻结；全部成交日 ≤ 数据截点 2026-07-19，无未来泄漏）。
SCOPE_V12_AS_OF: date = date(2026, 8, 31)

#: 现行判据阈值（DATA-001/G3R-D 沿用）：12 个月 ≥15 可支撑、8-14 有条件。
V12_SUPPORTED_MIN = 15
V12_CONDITIONAL_MIN = 8

#: 重算输入登记（valid_sale mart 与 community 权威表）。
V12_VALID_SALE_INPUT = "rebuilt_valid_sale"


@dataclass(frozen=True)
class ScopeRebaselineResult:
    """一次 v1.2 数据驱动重定级的结果（判据可机械复现）。"""

    path: Path
    rule_version: str
    as_of: date
    window_start: date
    cases_12m: dict[str, int]
    supported_ids: frozenset[str]
    conditional_ids: frozenset[str]


def window_start_of(as_of: date) -> date:
    """12 个月窗口起点（左闭右开：``[as_of-1年, as_of)``，与普查口径一致）。"""
    return date(as_of.year - 1, as_of.month, as_of.day)


def compute_cases_12m(valid_sale: pa.Table, *, as_of: date) -> dict[str, int]:
    """从重建后 valid_sale 统计逐小区 12 个月案例数（community_id 为准）。

    仅统计 ``community_id`` 已解析为标准 ID 的行；窗口 ``[as_of-1年, as_of)``；
    无成交小区不出现在结果中（真实零由调用方以全量小区框架补齐）。
    """
    start = window_start_of(as_of)
    ids = valid_sale.column("community_id").to_pylist()
    dates = valid_sale.column("sale_date").to_pylist()
    counter: Counter[str] = Counter()
    for community_id, sale_date in zip(ids, dates, strict=True):
        if not community_id or not isinstance(sale_date, date):
            continue
        if start <= sale_date < as_of:
            counter[str(community_id)] += 1
    return dict(counter)


def derive_policy_v1_2(
    cases_12m: dict[str, int],
    *,
    community_ids: Sequence[str],
) -> ScopePolicy:
    """按统一判据从案例数派生 v1.2 支撑集合（全量同一标准，无小区级例外）。

    ≥15 → 可支撑；8-14 → 有条件支撑；<8 或无成交 → 不入集合（暂不支撑）。
    """
    known = set(community_ids)
    supported = frozenset(
        cid for cid, n in cases_12m.items() if cid in known and n >= V12_SUPPORTED_MIN
    )
    conditional = frozenset(
        cid
        for cid, n in cases_12m.items()
        if cid in known and V12_CONDITIONAL_MIN <= n < V12_SUPPORTED_MIN
    )
    return ScopePolicy(
        rule_version="1.2",
        supported_ids=supported,
        conditional_ids=conditional,
    )


def build_scope_policy_v1_2(
    *,
    data_dir: Path,
    as_of: date = SCOPE_V12_AS_OF,
    notes: str | None = None,
) -> ScopeRebaselineResult:
    """v1.2 数据驱动重定级主入口：重建后主表 → 全量小区统一重算 → 落盘。

    判据（design D7 / spec scope-policy-rebaseline）：12 个月案例数从重建后
    ``valid_sale`` 统一重算，≥15 可支撑/8-14 有条件支撑/<8 暂不支撑，边界三分
    门控复用 :func:`evaluate`。写入 ``scope_policy_v1.2.parquet``（新版本文件，
    不覆盖 v1.0/v1.1）；**落盘不等于生效**——formal 纳入名单切换属正式基线
    确认，须由 user 确认后另行切换（estimate formal 闸门维持读取 v1.1）。
    """
    valid_sale_path = data_dir / MARTS_LAYER / "valid_sale.parquet"
    community_path = data_dir / ENTITIES_LAYER / COMMUNITY_FILENAME
    if not valid_sale_path.is_file():
        raise FileNotFoundError(f"valid_sale mart 不存在：{valid_sale_path}")
    if not community_path.is_file():
        raise FileNotFoundError(f"community 表不存在：{community_path}")

    valid_sale = pq.read_table(valid_sale_path)
    community_table = pq.read_table(community_path)
    community_ids = [
        str(cid) for cid in community_table.column("community_id").to_pylist()
    ]

    cases = compute_cases_12m(valid_sale, as_of=as_of)
    policy = derive_policy_v1_2(cases, community_ids=community_ids)
    table = scope_policy_table(community_table, policy)

    manifest_notes = (
        f"ScopePolicy v1.2 数据驱动重定级（ext-sale-ingest-scope-v1-2）："
        f"as_of={as_of.isoformat()} "
        f"窗口=[{window_start_of(as_of).isoformat()},{as_of.isoformat()}) "
        f"判据=12m≥{V12_SUPPORTED_MIN}可支撑/≥{V12_CONDITIONAL_MIN}有条件，边界三分门控；"
        f"纳入={len(policy.supported_ids)} 参考(有条件)={len(policy.conditional_ids)}；"
        "落盘≠生效：formal 纳入名单切换属正式基线确认，由 user 确认后才生效，"
        "生效前 estimate formal 闸门维持 v1.1"
    )
    path = write_scope_policy(
        table,
        data_dir=data_dir,
        rule_version=policy.rule_version,
        inputs=[
            InputRef(
                dataset=V12_VALID_SALE_INPUT,
                fetched_at=as_of.isoformat(),
                content_hash=sha256(valid_sale_path.read_bytes()).hexdigest(),
            ),
            InputRef(
                dataset="community",
                fetched_at=as_of.isoformat(),
                content_hash=sha256(community_path.read_bytes()).hexdigest(),
            ),
        ],
        notes=notes or manifest_notes,
    )
    return ScopeRebaselineResult(
        path=path,
        rule_version=policy.rule_version,
        as_of=as_of,
        window_start=window_start_of(as_of),
        cases_12m=cases,
        supported_ids=policy.supported_ids,
        conditional_ids=policy.conditional_ids,
    )
