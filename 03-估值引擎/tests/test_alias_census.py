"""DATA-005 census 别名补录批次（alias_census）行为测试。

对照 community-alias-registry 能力规格（含 pending-resolution 追加裁决与
final-resolution 名录最终裁决）：
① 冻结裁决范围：73 行映射（57 一致 + 16 待定），4 个跨区同名拒绝项不入表；
② 用户裁决 overrides：census 批次 6 条（5 行改一致、1 行冗余移除）→ 终态
   72 行 AC；名录批次 8 条（3 promote + 5 retarget 同名标准小区）；
   泰沙路/工业大道/工业大道南维持 blocked；
③ 追加构建：未裁决既有行逐字节保留、alias_id 唯一、外键不悬空、幂等；
④ 匹配语义：一致别名自动映射，待定 blocked；
⑤ 合同校验：全量行通过 CommunityAlias；首批映射与裁决 CSV 逐行对拍。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import AliasConflictStatus, CommunityAlias
from compsval.entities.alias import (
    ALIAS_FILENAME,
    build_alias_entity,
)
from compsval.entities.alias_census import (
    _ALIAS_REGISTRY_OVERRIDES,
    _CENSUS_ALIAS_MAPPINGS,
    _CENSUS_PENDING_OVERRIDES,
    CENSUS_BATCH_PREFIX,
    build_alias_census_backfill,
    census_alias_rows,
)
from compsval.entities.backfill import (
    BackfillOutcome,
    load_community_lookup,
    resolve_community_id,
)
from compsval.entities.community import build_community_entity

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VERDICTS_CSV = (
    _REPO_ROOT
    / "openspec"
    / "changes"
    / "archive"
    / "2026-08-31-data005-alias-backfill"
    / "review"
    / "alias-review-verdicts.csv"
)
_REJECTED_NAMES = (
    "示例小区154(邻甲区)",
    "示例小区232(邻丙)",
    "示例小区181(邻丙区)",
    "示例小区139(邻乙区)",
)


def _build_all(tmp_path: Path) -> Path:
    build_community_entity(data_dir=tmp_path)
    build_alias_entity(data_dir=tmp_path)
    return build_alias_census_backfill(
        data_dir=tmp_path,
        verdicts_csv=_VERDICTS_CSV,
    )


# ---------------------------------------------------------------------------
# ① 冻结裁决范围（首批映射不改，overrides 单列）
# ---------------------------------------------------------------------------


def test_frozen_mappings_scope_57_consistent_16_pending() -> None:
    assert len(_CENSUS_ALIAS_MAPPINGS) == 73
    statuses = [m.status for m in _CENSUS_ALIAS_MAPPINGS]
    assert statuses.count(AliasConflictStatus.CONSISTENT) == 57
    assert statuses.count(AliasConflictStatus.PENDING) == 16


def test_rejected_cross_district_names_not_in_frozen_mappings() -> None:
    names = {m.source_name for m in _CENSUS_ALIAS_MAPPINGS}
    for name in _REJECTED_NAMES:
        assert name not in names


def test_overrides_scope_matches_adjudication() -> None:
    assert len(_CENSUS_PENDING_OVERRIDES) == 6
    actions = [o.action for o in _CENSUS_PENDING_OVERRIDES]
    assert actions.count("remove") == 1
    assert actions.count("promote") == 1
    assert actions.count("retarget") == 4
    names = {o.source_name for o in _CENSUS_PENDING_OVERRIDES}
    assert names == {
        "示例小区132榕岸",
        "示例小区186远洋宿舍",
        "示例小区143示例小区245",
        "示例小区136拾光里",
        "示例小区167二期示例小区244",
    }


def test_registry_overrides_scope_matches_final_adjudication() -> None:
    assert len(_ALIAS_REGISTRY_OVERRIDES) == 8
    actions = [o.action for o in _ALIAS_REGISTRY_OVERRIDES]
    assert actions.count("promote") == 3
    assert actions.count("retarget") == 5
    assert len(set(actions)) == 2
    expected_targets = {
        "示例小区202": "C-XXXX0052",
        "春晖花苑(目标区)": "C-XXXX0027",
        "示例小区242": "C-XXXX0188",
        "示例小区008": "C-XXXX0151",
        "示例小区039": "C-XXXX0067",
        "示例小区053": "C-XXXX0097",
        "示例小区132榕岸华庭(E区)": "C-XXXX0184",
        "示例小区132榕景四季(D区)": "C-XXXX0128",
    }
    assert {o.source_name: o.community_id for o in _ALIAS_REGISTRY_OVERRIDES} == (
        expected_targets
    )
    registry_names = {o.source_name for o in _ALIAS_REGISTRY_OVERRIDES}
    census_names = {o.source_name for o in _CENSUS_PENDING_OVERRIDES}
    assert not registry_names & census_names


def test_key_mappings_present() -> None:
    by_name = {m.source_name: m for m in _CENSUS_ALIAS_MAPPINGS}
    fuji_a = by_name["示例小区130A区"]
    assert fuji_a.community_id == "C-XXXX0063"
    assert fuji_a.status is AliasConflictStatus.CONSISTENT
    assert by_name["示例小区132榕岸"].status is AliasConflictStatus.PENDING
    assert by_name["泰沙路"].status is AliasConflictStatus.PENDING


def test_frozen_mappings_match_verdicts_csv() -> None:
    if not _VERDICTS_CSV.is_file():
        pytest.skip("裁决 CSV 不在仓库内（独立检出）")
    import csv

    with open(_VERDICTS_CSV, encoding="utf-8-sig", newline="") as f:
        rows = [
            (r["source_name"], r["candidate_community_id"], r["alias_conflict_status"])
            for r in csv.DictReader(f)
            if r["verdict"] != "拒绝不写入"
        ]
    frozen = sorted(
        (m.source_name, m.community_id, m.status.value)
        for m in _CENSUS_ALIAS_MAPPINGS
    )
    assert sorted(rows) == frozen


# ---------------------------------------------------------------------------
# ② 追加构建 + 裁决终态：86 行、既有行保留、移除行缺席
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_build_appends_batch_applies_overrides_and_preserves_existing(
    tmp_path: Path,
) -> None:
    build_alias_entity(data_dir=tmp_path)
    before = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    registry_names = {o.source_name for o in _ALIAS_REGISTRY_OVERRIDES}
    before_names = before.column("source_alias").to_pylist()
    before_mask = pc.and_(
        pc.invert(pc.starts_with(before.column("alias_id"), CENSUS_BATCH_PREFIX)),
        pc.invert(pc.is_in(before.column("source_alias"), value_set=pa.array(registry_names))),
    )
    before_untouched = before.filter(before_mask)
    _build_all(tmp_path)
    after = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    assert after.num_rows == 86
    after_names = after.column("source_alias").to_pylist()
    after_mask = pc.and_(
        pc.invert(pc.starts_with(after.column("alias_id"), CENSUS_BATCH_PREFIX)),
        pc.invert(pc.is_in(after.column("source_alias"), value_set=pa.array(registry_names))),
    )
    after_untouched = after.filter(after_mask)
    assert after_untouched.equals(before_untouched)
    touched_before = [n for n in before_names if n in registry_names]
    touched_after = [n for n in after_names if n in registry_names]
    assert sorted(touched_before) == sorted(touched_after)
    ids = after.column("alias_id").to_pylist()
    ac_ids = [i for i in ids if i.startswith(CENSUS_BATCH_PREFIX)]
    assert len(ac_ids) == 72
    assert len(set(ac_ids)) == 72
    names = dict(
        zip(
            after.column("alias_id").to_pylist(),
            after.column("source_alias").to_pylist(),
            strict=True,
        )
    )
    removed = [
        aid
        for aid, name in names.items()
        if name == "示例小区132榕岸"
    ]
    assert len(removed) == 1


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_adjudicated_rows_final_state(tmp_path: Path) -> None:
    _build_all(tmp_path)
    table = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    statuses = table.column("conflict_status").to_pylist()
    assert statuses.count("一致") == 72
    assert statuses.count("待定") == 10
    assert statuses.count("冲突") == 4
    rows = {
        (name, cid): status
        for name, cid, status in zip(
            table.column("source_alias").to_pylist(),
            table.column("community_id").to_pylist(),
            table.column("conflict_status").to_pylist(),
            strict=True,
        )
    }
    assert rows[("示例小区132榕岸", "C-XXXX0184")] == "一致"
    assert ("示例小区132榕岸", "C-XXXX0069") not in rows
    assert rows[("示例小区186远洋宿舍", "C-XXXX0145")] == "一致"
    assert rows[("示例小区143示例小区245", "C-XXXX0049")] == "一致"
    assert rows[("示例小区136拾光里", "C-XXXX0051")] == "一致"
    assert rows[("示例小区167二期示例小区244", "C-XXXX0170")] == "一致"
    for name, cid in [
        ("示例小区202", "C-XXXX0052"),
        ("春晖花苑(目标区)", "C-XXXX0027"),
        ("示例小区242", "C-XXXX0188"),
        ("示例小区008", "C-XXXX0151"),
        ("示例小区039", "C-XXXX0067"),
        ("示例小区053", "C-XXXX0097"),
        ("示例小区132榕岸华庭(E区)", "C-XXXX0184"),
        ("示例小区132榕景四季(D区)", "C-XXXX0128"),
    ]:
        assert rows[(name, cid)] == "一致", (name, cid)
    for name, cid in [
        ("泰沙路", "C-XXXX0089"),
        ("泰沙路", "C-XXXX0090"),
        ("工业大道", "C-XXXX0135"),
        ("工业大道南", "C-XXXX0141"),
    ]:
        assert rows[(name, cid)] == "待定"
    for name in _REJECTED_NAMES:
        assert not any(k[0] == name for k in rows)


# ---------------------------------------------------------------------------
# ③ 幂等
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_backfill_idempotent_byte_identical(tmp_path: Path) -> None:
    _build_all(tmp_path)
    first = (tmp_path / "entities" / ALIAS_FILENAME).read_bytes()
    build_alias_census_backfill(
        data_dir=tmp_path,
        verdicts_csv=_VERDICTS_CSV,
    )
    second = (tmp_path / "entities" / ALIAS_FILENAME).read_bytes()
    assert first == second


# ---------------------------------------------------------------------------
# ④ 匹配语义：裁决后一致自动映射、维持待定 blocked
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_adjudicated_aliases_resolve_and_pending_blocked(tmp_path: Path) -> None:
    _build_all(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)
    alias_hits = [
        ("示例小区130A区", "C-XXXX0063"),
        ("江南西路", "C-XXXX0150"),
        ("示例小区164", "C-XXXX0181"),
        ("示例小区132榕岸", "C-XXXX0184"),
        ("示例小区186远洋宿舍", "C-XXXX0145"),
        ("示例小区143示例小区245", "C-XXXX0049"),
        ("示例小区136拾光里", "C-XXXX0051"),
        ("示例小区167二期示例小区244", "C-XXXX0170"),
        ("春晖花苑(目标区)", "C-XXXX0027"),
        ("示例小区242", "C-XXXX0188"),
    ]
    for name, expect in alias_hits:
        cid, outcome, _ = resolve_community_id(name, lookup)
        assert (cid, outcome) == (expect, BackfillOutcome.HIT_ALIAS), name
    same_name_hits = [
        ("示例小区202", "C-XXXX0052"),
        ("示例小区008", "C-XXXX0151"),
        ("示例小区039", "C-XXXX0067"),
        ("示例小区053", "C-XXXX0097"),
        ("示例小区132榕岸华庭(E区)", "C-XXXX0184"),
        ("示例小区132榕景四季(D区)", "C-XXXX0128"),
    ]
    for name, expect in same_name_hits:
        cid, outcome, _ = resolve_community_id(name, lookup)
        assert cid == expect, name
        assert outcome in (
            BackfillOutcome.HIT_CANONICAL,
            BackfillOutcome.HIT_ALIAS,
        ), name
    for name in ["泰沙路", "工业大道", "工业大道南"]:
        cid, outcome, _ = resolve_community_id(name, lookup)
        assert cid is None
        assert outcome is BackfillOutcome.BLOCKED, name


# ---------------------------------------------------------------------------
# ⑤ 合同校验
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_all_written_rows_pass_contract_model(tmp_path: Path) -> None:
    _build_all(tmp_path)
    table = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    census_refs = 0
    for aid, cid, name, sid, ref, status in zip(
        table.column("alias_id").to_pylist(),
        table.column("community_id").to_pylist(),
        table.column("source_alias").to_pylist(),
        table.column("source_id").to_pylist(),
        table.column("source_ref").to_pylist(),
        table.column("conflict_status").to_pylist(),
        strict=True,
    ):
        CommunityAlias(
            alias_id=aid,
            community_id=cid,
            source_alias=name,
            source_id=sid,
            source_ref=ref,
            conflict_status=AliasConflictStatus(status),
        )
        if aid.startswith(CENSUS_BATCH_PREFIX):
            census_refs += 1
            assert "alias-review-verdicts.csv" in ref
    assert census_refs == 72
    adjudicated = [
        r
        for r in table.column("source_ref").to_pylist()
        if "2026-08-31 用户裁决" in r
    ]
    assert len(adjudicated) == 13
    registry_adjudicated = [
        r for r in adjudicated if "data005-alias-final-resolution" in r
    ]
    assert len(registry_adjudicated) == 8


def test_census_rows_raw_batch_still_73() -> None:
    rows = census_alias_rows()
    assert len(rows) == 73
    assert all(r.source_id == "SRC-007" for r in rows)
