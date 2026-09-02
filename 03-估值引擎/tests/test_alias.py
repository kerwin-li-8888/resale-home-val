"""WP5-B: community_alias 小区别名映射与跨来源冲突登记（不静默合并）。

对照 WP5-B 验收标准：
① 候选名录冲突清单 #1-10 全部登记且未合并（alias 行 + pending_confirmation）；
② 每个 alias 可追溯到来源（source_id + source_ref 出处）可溯源；
③ conflict_status 三分（一致/冲突/待定）且填写正确；
④ 低置信匹配不自动合并、进待复核（PENDING/CONFLICT 未合并为同一小区）；
⑤ 输出待人工确认清单（pending_confirmation，#1-10 全登记）；
⑥ ruff/mypy/pytest + ``compsval entities build``/``compsval catalog`` 通过。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from compsval import cli
from compsval.contract.models import AliasConflictStatus, CommunityAlias
from compsval.entities.alias import (
    _ALIAS_MAPPINGS,
    ALIAS_FILENAME,
    ALIAS_TABLE,
    CATALOG_INPUT,
    ENTITIES_LAYER,
    _resolve_conflicts,
    alias_id_of,
    alias_schema,
    alias_table,
    build_alias_entity,
    pending_confirmation,
    write_alias_entity,
)
from compsval.entities.community import build_community_entity, community_id_of
from compsval.ingest.manifests import read_derived_manifest

_STATUS_VALUES = {s.value for s in AliasConflictStatus}
_CONFLICT_NOS = set(range(1, 11))


def _aliases() -> list[CommunityAlias]:
    return list(_resolve_conflicts(_ALIAS_MAPPINGS))


# ---------------------------------------------------------------------------
# 验收①：冲突清单 #1-10 全部登记且未合并
# ---------------------------------------------------------------------------


def test_all_ten_conflicts_registered_in_pending_confirmation() -> None:
    conflicts = pending_confirmation()
    assert [c.conflict_no for c in conflicts] == list(range(1, 11))
    assert all(c.title for c in conflicts)
    assert all(c.action for c in conflicts)


def test_alias_mappings_and_pending_cover_all_ten_conflicts() -> None:
    # 落表映射覆盖 #1-8（名称类别名）；#9/#10 为非名称类，但必须进待确认清单
    mapping_nos = {m.conflict_no for m in _ALIAS_MAPPINGS}
    assert mapping_nos <= set(range(1, 9))
    assert {c.conflict_no for c in pending_confirmation()} == _CONFLICT_NOS


def test_no_silent_merge_of_conflicting_names() -> None:
    # 低置信别名（PENDING/CONFLICT）只登记关系、不并入同一 community：
    # 每个 PENDING/CONFLICT 别名都保留 source_ref 指向名录冲突项（未自动合并）
    aliases = _aliases()
    low_confidence = [a for a in aliases if a.conflict_status is not AliasConflictStatus.CONSISTENT]
    assert low_confidence
    for alias in low_confidence:
        assert "§3 #" in alias.source_ref  # 可回溯到被判定为需复核的冲突项


# ---------------------------------------------------------------------------
# 验收②：每个 alias 可追溯到来源（source_id + source_ref 出处）
# ---------------------------------------------------------------------------


def test_each_alias_has_source_id_and_traceable_source_ref() -> None:
    aliases = _aliases()
    assert aliases
    for alias in aliases:
        assert alias.source_id
        assert alias.source_ref.startswith("候选小区名录-V0.1.md §3 #")
    assert len({a.source_ref for a in aliases}) == len(aliases)  # 每行唯一溯源


def test_alias_id_is_unique_and_stable() -> None:
    aliases = _aliases()
    ids = [a.alias_id for a in aliases]
    assert len(ids) == len(set(ids))
    assert all(a.alias_id.startswith("A-") for a in aliases)
    # 序号按冲突项内递增且确定（可复现）
    assert alias_id_of(1, 1) == "A-1-1"
    seq_by_conflict: dict[int, int] = {}
    expected: list[str] = []
    for m in _ALIAS_MAPPINGS:
        seq = seq_by_conflict.get(m.conflict_no, 0) + 1
        seq_by_conflict[m.conflict_no] = seq
        expected.append(alias_id_of(m.conflict_no, seq))
    assert [a.alias_id for a in aliases] == expected


def test_source_names_use_registered_sources() -> None:
    from compsval.contract.registry import registered_sources

    registered = {s.source_id for s in registered_sources()}
    aliases = _aliases()
    assert all(a.source_id in registered for a in aliases)


# ---------------------------------------------------------------------------
# 候选小区锚点：anchor_key 必须在候选名录（映射可解析，不外键悬空）
# ---------------------------------------------------------------------------


def test_every_anchor_exists_in_candidates() -> None:
    from compsval.entities.candidates import candidates_all

    known = {c.source_key for c in candidates_all()}
    assert all(m.anchor_key in known for m in _ALIAS_MAPPINGS)
    # community_id 可复现推导（WP5-A 同一规则）
    assert all(community_id_of(m.anchor_key).startswith("C-") for m in _ALIAS_MAPPINGS)


# ---------------------------------------------------------------------------
# 验收③：conflict_status 枚举合法且填写正确
# ---------------------------------------------------------------------------


def test_conflict_status_uses_three_way_split_only() -> None:
    table = alias_table(_aliases())
    values = table.column("conflict_status").to_pylist()
    assert set(values) <= _STATUS_VALUES
    # WP5-B 名录批次产出一致/冲突/待定 三分（ext-sale-ingest-scope-v1-2 起
    # 枚举扩为四分，新增终态"排除"仅经用户裁决产生，不出现在名录批次构建中）
    assert set(values) == {
        AliasConflictStatus.CONSISTENT.value,
        AliasConflictStatus.CONFLICT.value,
        AliasConflictStatus.PENDING.value,
    }


# ---------------------------------------------------------------------------
# 验收④：低置信匹配不自动合并、进待复核
# ---------------------------------------------------------------------------


def test_low_confidence_entries_flagged_pending_or_conflict() -> None:
    table = alias_table(_aliases())
    values = table.column("conflict_status").to_pylist()
    low = [
        v
        for v in values
        if v in {AliasConflictStatus.PENDING.value, AliasConflictStatus.CONFLICT.value}
    ]
    consistent = [v for v in values if v == AliasConflictStatus.CONSISTENT.value]
    assert consistent
    assert low
    # 需要人工复核的低置信项占多数（本骨架期多数命名待核验）
    assert len(low) > len(consistent)


def test_consistent_only_where_source_name_matches_standard() -> None:
    aliases = _aliases()
    for alias in aliases:
        if alias.conflict_status is AliasConflictStatus.CONSISTENT:
            # 一致项必须是被来源显式确认的中原/房天下标准名写法
            assert alias.source_id in {"SRC-005", "SRC-006"}


# ---------------------------------------------------------------------------
# 验收⑤：输出待人工确认清单
# ---------------------------------------------------------------------------


def test_pending_confirmation_output_complete() -> None:
    conflicts = pending_confirmation()
    assert len(conflicts) == 10
    assert all(isinstance(c.status, AliasConflictStatus) for c in conflicts)


# ---------------------------------------------------------------------------
# community_alias 实体表构造与原子写盘（parquet + DerivedManifest）
# ---------------------------------------------------------------------------


def test_alias_table_matches_schema() -> None:
    table = alias_table(_aliases())
    assert table.schema == alias_schema()
    assert table.num_rows == len(_ALIAS_MAPPINGS)
    for field in alias_schema():
        if not field.nullable:
            assert table.column(field.name).null_count == 0


def test_write_alias_entity_writes_parquet_and_manifest(tmp_path: Path) -> None:
    aliases = _aliases()
    table = alias_table(aliases)
    path = write_alias_entity(
        table,
        data_dir=tmp_path,
        inputs=[CATALOG_INPUT],
        notes="WP5-B 测试写盘",
    )

    assert path.name == ALIAS_FILENAME
    assert path.parent.name == ENTITIES_LAYER
    assert path.is_file()
    assert pq.read_table(path).num_rows == len(aliases)

    manifest = read_derived_manifest(path)
    assert manifest.layer == ENTITIES_LAYER
    assert manifest.table == ALIAS_TABLE
    assert manifest.row_count == len(aliases)
    assert [i.dataset for i in manifest.inputs] == [CATALOG_INPUT.dataset]


def test_build_alias_entity_roundtrip(tmp_path: Path) -> None:
    path = build_alias_entity(data_dir=tmp_path)
    table = pq.read_table(path)
    assert table.num_rows == len(_ALIAS_MAPPINGS)
    assert table.column("alias_id").to_pylist()[0] == "A-1-1"
    assert table.column("community_id").to_pylist()[0] == community_id_of(
        _ALIAS_MAPPINGS[0].anchor_key
    )


def test_every_alias_community_id_has_a_matching_community(tmp_path: Path) -> None:
    # 外键一致性：alias.community_id 必须在 community 权威表中存在（WP5-E 回填依赖）
    community_path = build_community_entity(data_dir=tmp_path)
    community_ids = set(
        pq.read_table(community_path).column("community_id").to_pylist()
    )
    for alias in _aliases():
        assert alias.community_id in community_ids


def test_alias_model_requires_source_ref() -> None:
    # 数据字典 §3.4 新增 source_ref 必填（WP5-B 验收② 每行可溯源）
    with pytest.raises(ValidationError):
        CommunityAlias(  # type: ignore[call-arg]  # 故意缺 source_ref 验证必填
            alias_id="A-1-1",
            community_id="C-XXXX0188",
            source_alias="星汇目标区湾",
            source_id="SRC-006",
            conflict_status=AliasConflictStatus.CONSISTENT,
        )


# ---------------------------------------------------------------------------
# 验收⑥ 效果：compsval entities build 一并生成 community_alias + 输出清单
# ---------------------------------------------------------------------------


def test_cli_entities_build_builds_alias_and_prints_checklist(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["entities", "build", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "community" in out
    assert "community_alias" in out
    assert "237 rows" in out
    assert f"{len(_ALIAS_MAPPINGS)} rows" in out
    # 待人工确认清单输出 #1-10（验收①⑤）
    for n in range(1, 11):
        assert f"#{n} " in out

    # community_alias 对 compsval catalog 可见（entities 层 ent_ 视图前缀）
    assert cli.main(["catalog", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "[entities] ent_community_alias" in out