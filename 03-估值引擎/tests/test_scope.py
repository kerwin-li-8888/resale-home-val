"""WP5-F: ScopePolicy 适用范围判断（VAL1-001）：纳入/参考/拒绝。

对照 WP5-F 验收标准：
① 三类判定（纳入/参考/拒绝）规则明确且机器可执行；
② 范围外对象不能进入正式估值（反例测试）；
③ 边界待定/数据稀疏走参考或拒绝、不静默纳入；
④ 规则带 rule_version、改动生成新版本不覆盖旧结果；
⑤ ruff/mypy/pytest 通过。

数据支撑等级取自 DATA-001 §4 冻结集合：7 个可支撑（12个月≥15）+ 4 个有条件
支撑（案例8-14 或 累计多近期少），11 个冻结可实施 ID（2026-08-21）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.contract.models import BoundaryStatus
from compsval.entities.community import COMMUNITY_FILENAME, ENTITIES_LAYER
from compsval.valuation.scope import (
    AUDIT_INPUT,
    CATALOG_INPUT,
    DEFAULT_RULE_VERSION,
    FROZEN_CONDITIONAL_IDS,
    FROZEN_SUPPORTED_IDS,
    SCOPE_POLICY_PREFIX,
    PropertyType,
    ScopeDecision,
    SupportLevel,
    build_scope_policy,
    default_policy,
    evaluate,
    scope_policy_filename,
    scope_policy_schema,
    support_level_of,
    write_scope_policy,
)

#: DATA-001 §4 冻结集合（7 可支撑 + 4 有条件 = 11 个真实小区 ID，2026-08-21）。
_SUPPORTED_IDS: tuple[str, ...] = (
    "C-XXXX0069",  # 示例小区132
    "C-XXXX0048",  # 示例小区121
    "C-XXXX0052",  # 示例小区202
    "C-XXXX0053",  # 示例小区041
    "C-XXXX0188",  # 示例小区026
    "C-XXXX0105",  # 示例小区219
    "C-XXXX0009",  # 示例小区031
)
_CONDITIONAL_IDS: tuple[str, ...] = (
    "C-XXXX0077",  # 示例小区193（8 案例）
    "C-XXXX0120",  # 示例小区045（12 案例）
    "C-XXXX0013",  # 示例小区177（11 案例）
    "C-XXXX0027",  # 示例小区154（累计28但12月5 → 参考）
)

#: 其余无支撑小区（不在冻结集合内 → 暂不支撑）。
_UNSUPPORTED_ID = "C-XXXX0176"


def _community_table(
    boundary: dict[str, BoundaryStatus],
    *,
    property_names: dict[str, str] | None = None,
) -> pa.Table:
    """构造一条最小 community 实体表（只含 scope_policy 需要的列）。"""
    property_names = property_names or {}
    rows: dict[str, list[object]] = {
        "community_id": [],
        "standard_name": [],
        "block": [],
        "address": [],
        "latitude": [],
        "longitude": [],
        "coordinate_system": [],
        "boundary_status": [],
        "source_id": [],
        "source_key": [],
        "source_ref": [],
        "notes": [],
    }
    for community_id, status in boundary.items():
        rows["community_id"].append(community_id)
        rows["standard_name"].append(property_names.get(community_id, "测试小区"))
        rows["block"].append("测试板块")
        rows["address"].append("测试地址")
        rows["latitude"].append(None)
        rows["longitude"].append(None)
        rows["coordinate_system"].append("UNKNOWN")
        rows["boundary_status"].append(status.value)
        rows["source_id"].append("SRC-005")
        rows["source_key"].append(community_id.removeprefix("C-"))
        rows["source_ref"].append("候选小区名录-V0.1.md §2.1 测试行")
        rows["notes"].append(None)
    return pa.table(rows)


# ---- ① 三类判定规则明确且机器可执行 ----
def test_fuji_guangchang_include_after_v11() -> None:
    """G3R-D v1.1：示例小区130（12 个月 18 案例）纳入正式范围；未达标的补数小区不纳入。"""
    policy = default_policy()
    assert evaluate(
        community_id="C-XXXX0063",  # 示例小区130
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
        policy=policy,
    ).decision is ScopeDecision.INCLUDE
    for low_support_id in ("C-XXXX0122", "C-XXXX0051"):  # 示例小区166/拾光里
        assert evaluate(
            community_id=low_support_id,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            policy=policy,
        ).decision is ScopeDecision.REJECT


def test_three_decisions_machine_executable() -> None:
    policy = default_policy()
    assert policy.rule_version == DEFAULT_RULE_VERSION
    # 可支撑 → 纳入
    assert (
        evaluate(
            community_id=_SUPPORTED_IDS[0],
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            policy=policy,
        ).decision
        is ScopeDecision.INCLUDE
    )
    # 有条件支撑 → 参考
    assert (
        evaluate(
            community_id=_CONDITIONAL_IDS[0],
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            policy=policy,
        ).decision
        is ScopeDecision.REFERENCE
    )
    # 无支撑 → 拒绝
    assert (
        evaluate(
            community_id=_UNSUPPORTED_ID,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            policy=policy,
        ).decision
        is ScopeDecision.REJECT
    )


def test_decision_enum_three_values() -> None:
    assert {d.value for d in ScopeDecision} == {"纳入", "参考", "拒绝"}


def test_support_level_enum_maps_frozen_ids() -> None:
    assert len(FROZEN_SUPPORTED_IDS) == 8  # 7 + 示例小区130（G3R-D v1.1）
    assert len(FROZEN_CONDITIONAL_IDS) == 4
    policy = default_policy()
    assert (
        support_level_of(_SUPPORTED_IDS[0], policy) is SupportLevel.SUPPORTED
    )
    assert (
        support_level_of(_CONDITIONAL_IDS[0], policy) is SupportLevel.CONDITIONAL
    )
    assert support_level_of(_UNSUPPORTED_ID, policy) is SupportLevel.UNSUPPORTED


def test_verdict_has_traceable_reason() -> None:
    verdict = evaluate(
        community_id=_SUPPORTED_IDS[0],
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
    )
    assert verdict.decision is ScopeDecision.INCLUDE
    assert "DATA-001" in verdict.reason
    assert verdict.reason != ""


# ---- ② 范围外对象不能进入正式估值（反例测试） ----
def test_out_of_scope_never_include() -> None:
    for community_id in (_SUPPORTED_IDS[0], _CONDITIONAL_IDS[0], _UNSUPPORTED_ID):
        verdict = evaluate(
            community_id=community_id,
            boundary_status=BoundaryStatus.OUT_OF_SCOPE,
        )
        assert verdict.decision is ScopeDecision.REJECT, community_id


def test_non_residential_never_include() -> None:
    # 非普通住宅（公寓/商办/车位/别墅）即使可支撑也拒绝（README §3.3 排除类）
    for community_id in (_SUPPORTED_IDS[0], _UNSUPPORTED_ID):
        verdict = evaluate(
            community_id=community_id,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
            property_type=PropertyType.NON_RESIDENTIAL,
        )
        assert verdict.decision is ScopeDecision.REJECT, community_id


# ---- ③ 边界待定/数据稀疏走参考或拒绝、不静默纳入 ----
def test_boundary_pending_goes_reference_not_include() -> None:
    for community_id in (_SUPPORTED_IDS[0], _CONDITIONAL_IDS[0], _UNSUPPORTED_ID):
        verdict = evaluate(
            community_id=community_id,
            boundary_status=BoundaryStatus.BOUNDARY_PENDING,
        )
        assert verdict.decision is ScopeDecision.REFERENCE, community_id


def test_data_sparse_conditional_goes_reference_not_include() -> None:
    verdict = evaluate(
        community_id=_CONDITIONAL_IDS[0],
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
    )
    assert verdict.decision is ScopeDecision.REFERENCE


def test_unsupported_goes_reject_not_include() -> None:
    verdict = evaluate(
        community_id=_UNSUPPORTED_ID,
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
    )
    assert verdict.decision is ScopeDecision.REJECT


# ---- ④ 规则带 rule_version、新版本不覆盖旧结果 ----
def test_default_rule_version() -> None:
    assert default_policy().rule_version == "1.1"  # G3R-D v1.1（示例小区130纳入）
    assert scope_policy_filename("1.0") == f"{SCOPE_POLICY_PREFIX}1.0.parquet"
    assert scope_policy_filename("2.0") == f"{SCOPE_POLICY_PREFIX}2.0.parquet"


def test_versioned_write_keeps_old_version(tmp_path: Path) -> None:
    table = _community_table(
        {_SUPPORTED_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED}
    )
    v1 = write_scope_policy(
        table,
        data_dir=tmp_path,
        rule_version="1.0",
        inputs=[CATALOG_INPUT, AUDIT_INPUT],
        notes="v1",
    )
    v2 = write_scope_policy(
        table,
        data_dir=tmp_path,
        rule_version="2.0",
        inputs=[CATALOG_INPUT, AUDIT_INPUT],
        notes="v2",
    )
    assert v1.name == f"{SCOPE_POLICY_PREFIX}1.0.parquet"
    assert v2.name == f"{SCOPE_POLICY_PREFIX}2.0.parquet"
    # 两个版本并存，新版本不覆盖旧结果
    assert v1.is_file() and v2.is_file()
    assert pq.read_table(v1).num_rows == 1
    assert pq.read_table(v2).num_rows == 1


def test_same_input_same_version_same_verdict() -> None:
    first = evaluate(
        community_id=_SUPPORTED_IDS[0],
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
    )
    second = evaluate(
        community_id=_SUPPORTED_IDS[0],
        boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
    )
    assert first.decision is second.decision
    assert first.reason == second.reason


def test_build_scope_policy_with_custom_version(tmp_path: Path) -> None:
    table = _community_table(
        {_SUPPORTED_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED}
    )
    (tmp_path / ENTITIES_LAYER).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_path / ENTITIES_LAYER / COMMUNITY_FILENAME)
    path = build_scope_policy(data_dir=tmp_path, rule_version="3.0")
    assert path.name == f"{SCOPE_POLICY_PREFIX}3.0.parquet"
    out = pq.read_table(path)
    assert out.num_rows == 1
    assert out.column("scope_decision").to_pylist() == ["纳入"]
    assert out.column("rule_version").to_pylist() == ["3.0"]


# ---- 真实数据抽查：分类与 DATA-001 支撑等级一致 ----
def test_frozen_ids_supported_and_conditional() -> None:
    """11 个冻结 ID 与 DATA-001 §4 一致（7 可支撑 + 4 有条件，全部机器确认）。"""
    policy = default_policy()
    for community_id in _SUPPORTED_IDS:
        assert (
            support_level_of(community_id, policy) is SupportLevel.SUPPORTED
        )
        verdict = evaluate(
            community_id=community_id,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
        )
        assert verdict.decision is ScopeDecision.INCLUDE, community_id
    for community_id in _CONDITIONAL_IDS:
        assert (
            support_level_of(community_id, policy) is SupportLevel.CONDITIONAL
        )
        verdict = evaluate(
            community_id=community_id,
            boundary_status=BoundaryStatus.MACHINE_CONFIRMED,
        )
        assert verdict.decision is ScopeDecision.REFERENCE, community_id


def test_real_community_table_classification() -> None:
    """community 表逐行判定：可支撑→纳入、有条件→参考、无支撑→拒绝、
    边界待定→参考（不静默纳入）、范围外→拒绝（绝不进入正式）。"""
    table = _community_table(
        {
            _SUPPORTED_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED,
            _CONDITIONAL_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED,
            _UNSUPPORTED_ID: BoundaryStatus.MACHINE_CONFIRMED,
            "C-PENDING": BoundaryStatus.BOUNDARY_PENDING,
            "C-OUT": BoundaryStatus.OUT_OF_SCOPE,
        }
    )
    from compsval.valuation.scope import scope_policy_table

    out = scope_policy_table(table, default_policy())
    decisions = {
        out.column("community_id")[i].as_py(): out.column("scope_decision")[i].as_py()
        for i in range(out.num_rows)
    }
    assert decisions[_SUPPORTED_IDS[0]] == "纳入"
    assert decisions[_CONDITIONAL_IDS[0]] == "参考"
    assert decisions[_UNSUPPORTED_ID] == "拒绝"
    assert decisions["C-PENDING"] == "参考"  # 边界待定不静默纳入
    assert decisions["C-OUT"] == "拒绝"  # 范围外绝不进入正式
    assert out.column("rule_version").to_pylist() == ["1.1"] * out.num_rows
    # 每行可溯源（source_ref 非空）
    assert all(r != "" for r in out.column("source_ref").to_pylist())


# ---- 写盘与 manifest ----
def test_write_scope_policy_atomic(tmp_path: Path) -> None:
    table = _community_table(
        {_SUPPORTED_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED}
    )
    path = write_scope_policy(
        table,
        data_dir=tmp_path,
        rule_version="1.0",
        inputs=[CATALOG_INPUT, AUDIT_INPUT],
        notes="测试",
    )
    assert path.name == f"{SCOPE_POLICY_PREFIX}1.0.parquet"
    assert not path.with_name(path.name + ".incomplete").exists()
    manifest_path = path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["layer"] == ENTITIES_LAYER
    assert manifest["table"] == "scope_policy_v1.0"
    assert manifest["row_count"] == 1
    assert manifest["notes"] == "测试"
    assert {"candidate_community_catalog", "data_feasibility_audit_G1_frozen"} <= {
        item["dataset"] for item in manifest["inputs"]
    }


def test_scope_policy_schema_has_traceability_and_version() -> None:
    names = set(scope_policy_schema().names)
    required = {
        "community_id",
        "standard_name",
        "block",
        "boundary_status",
        "support_level",
        "property_type",
        "scope_decision",
        "reason",
        "rule_version",
        "source_ref",
    }
    assert required <= names


def test_rebuild_is_reproducible(tmp_path: Path) -> None:
    table = _community_table(
        {
            _SUPPORTED_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED,
            _CONDITIONAL_IDS[0]: BoundaryStatus.MACHINE_CONFIRMED,
        }
    )
    (tmp_path / ENTITIES_LAYER).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp_path / ENTITIES_LAYER / COMMUNITY_FILENAME)
    first = build_scope_policy(data_dir=tmp_path)
    second = build_scope_policy(data_dir=tmp_path)
    assert pq.read_table(first).equals(pq.read_table(second))
