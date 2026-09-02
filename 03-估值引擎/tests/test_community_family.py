"""community-family-subarea-census-v1-3 实体构建（community_family）行为测试。

对照 community-family-model 能力规格与冻结判定表：
① 判定表对拍：13 家族 / 28 子区 / 2 标准名变更 / 5 merged+redirect / 2 新建实体
   / 1 条 AF 批次别名，alias 终态 87 行（一致 73 / 待定 10 / 冲突 4）；
② 改名后旧名可解析（示例小区164龙禧经子区、华标品峰经别名）；
③ 合并转发五例（拾光里等 → 承接 (community_id, sub_area)），行保留不删除；
④ 家族挂靠：示例小区047 UNKNOWN、金汐兄弟结构 main=UNKNOWN；
⑤ 追加式幂等：重跑逐字节一致，既有 A-/AC- 行与 237 行基线内容保留。
"""

from __future__ import annotations

import pytest

from pathlib import Path

import pyarrow.parquet as pq

from compsval.entities.alias import (
    ALIAS_FILENAME,
    build_alias_entity,
)
from compsval.entities.alias_census import build_alias_census_backfill
from compsval.entities.community import COMMUNITY_FILENAME, build_community_entity
from compsval.entities.community_family import (
    ALIAS_ADDITION,
    FAMILIES,
    FAMILY_FILENAME,
    MERGES,
    NEW_ENTITIES,
    RENAMES,
    SUBAREA_FILENAME,
    SUBAREAS,
    build_community_family,
)

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

GUANGDA = "C-XXXX0069"
HENGDAS = "C-XXXX0083"
THIRD_JB = "C-XXXX0067"
JB_PHASE2 = "C-XXXX0097"
LJM1 = "C-XXXX0189"
LJM2 = "C-XXXX0190"


def _build_all(tmp_path: Path) -> Path:
    build_community_entity(data_dir=tmp_path)
    build_alias_entity(data_dir=tmp_path)
    build_alias_census_backfill(data_dir=tmp_path, verdicts_csv=_VERDICTS_CSV)
    build_community_family(data_dir=tmp_path)
    return tmp_path / "entities"


def _community_rows(entities_dir: Path) -> dict:
    table = pq.read_table(entities_dir / COMMUNITY_FILENAME)
    cols = {name: table.column(name).to_pylist() for name in table.column_names}
    return {
        cid: {name: cols[name][i] for name in table.column_names}
        for i, cid in enumerate(cols["community_id"])
    }


# ---------------------------------------------------------------------------
# ① 判定表对拍
# ---------------------------------------------------------------------------


def test_frozen_table_counts_match_adjudication() -> None:
    assert len(FAMILIES) == 13
    assert len(SUBAREAS) == 28
    assert len(RENAMES) == 2
    assert len(MERGES) == 5
    assert len(NEW_ENTITIES) == 2
    family_ids = [f.family_id for f in FAMILIES]
    assert family_ids == [f"F-{i:03d}" for i in range(1, 14)]
    subarea_ids = [s.subarea_id for s in SUBAREAS]
    assert subarea_ids == [f"SA-{i:03d}" for i in range(1, 29)]


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_build_writes_expected_rows(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    community = pq.read_table(entities_dir / COMMUNITY_FILENAME)
    family = pq.read_table(entities_dir / FAMILY_FILENAME)
    subarea = pq.read_table(entities_dir / SUBAREA_FILENAME)
    alias = pq.read_table(entities_dir / ALIAS_FILENAME)
    assert community.num_rows == 239
    assert family.num_rows == 13
    assert subarea.num_rows == 28
    assert alias.num_rows == 87
    statuses = alias.column("conflict_status").to_pylist()
    assert statuses.count("一致") == 73
    assert statuses.count("待定") == 10
    assert statuses.count("冲突") == 4


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_new_entities_c29_segment_and_family(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    rows = _community_rows(entities_dir)
    for e in NEW_ENTITIES:
        assert e.community_id in rows
        row = rows[e.community_id]
        assert row["standard_name"] == e.standard_name
        assert row["boundary_status"] == "机器确认"
        assert row["source_key"] == "UNKNOWN"
        assert row["family_id"] == "F-002"
        assert row["entity_status"] == "active"
        assert "user-adjudication-table.md" in row["source_ref"]
    assert LJM1 not in {r["community_id"] for r in []}  # noqa: B011 - 占位保持可读
    ids = set(rows)
    assert LJM1 in ids and LJM2 in ids


# ---------------------------------------------------------------------------
# ② 改名后旧名可解析
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_renames_applied_and_old_names_resolvable(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    rows = _community_rows(entities_dir)
    rename_map = {r.community_id: r for r in RENAMES}
    for cid, action in rename_map.items():
        assert rows[cid]["standard_name"] == action.new_name
        assert action.old_name not in {r["standard_name"] for r in rows.values()}
        assert f"{action.old_name}→{action.new_name}" in rows[cid]["notes"]
    subarea_table = pq.read_table(entities_dir / SUBAREA_FILENAME)
    match_names = subarea_table.column("match_names").to_pylist()
    assert any("示例小区164龙禧" in n for n in match_names)
    alias = pq.read_table(entities_dir / ALIAS_FILENAME)
    pairs = dict(
        zip(
            alias.column("source_alias").to_pylist(),
            alias.column("community_id").to_pylist(),
            strict=True,
        )
    )
    assert pairs["华标品峰"] == "C-XXXX0125"


# ---------------------------------------------------------------------------
# ③ 合并转发：行保留 + redirect 映射
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_merges_keep_rows_with_redirect(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    rows = _community_rows(entities_dir)
    for m in MERGES:
        row = rows[m.community_id]
        assert row["entity_status"] == "merged"
        assert row["redirect_community_id"] == m.redirect_community_id
        assert row["redirect_subarea_name"] == m.redirect_subarea_name
        assert row["standard_name"] == m.old_name
        assert "merged" in rows[m.community_id]["notes"]
    targets = {m.redirect_community_id for m in MERGES}
    for t in targets:
        assert rows[t]["entity_status"] == "active"


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_alias_targets_to_merged_entities_forward_consistently(
    tmp_path: Path,
) -> None:
    entities_dir = _build_all(tmp_path)
    alias = pq.read_table(entities_dir / ALIAS_FILENAME)
    by_name = {}
    for name, cid in zip(
        alias.column("source_alias").to_pylist(),
        alias.column("community_id").to_pylist(),
        strict=True,
    ):
        by_name.setdefault(name, []).append(cid)
    rows = _community_rows(entities_dir)
    for name in [
        "拾光里",
        "示例小区143示例小区245",
        "示例小区167二期示例小区244",
        "示例小区132榕岸华庭(E区)",
        "示例小区132榕景四季(D区)",
    ]:
        for cid in by_name.get(name, []):
            if rows[cid]["entity_status"] == "merged":
                redirect = (rows[cid]["redirect_community_id"], rows[cid]["redirect_subarea_name"])
                assert redirect[0] in rows
                assert rows[redirect[0]]["entity_status"] == "active"


# ---------------------------------------------------------------------------
# ④ 家族挂靠
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_family_assignment_and_hengda_excluded(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    rows = _community_rows(entities_dir)
    assert rows[HENGDAS]["family_id"] == "UNKNOWN"
    assert rows[GUANGDA]["family_id"] == "F-001"
    assert rows[THIRD_JB]["family_id"] == "F-002"
    assert rows[JB_PHASE2]["family_id"] == "F-002"
    assert rows[JB_PHASE2]["entity_status"] == "active"
    assert rows[JB_PHASE2]["standard_name"] == "示例小区053"
    for m in MERGES:
        assert rows[m.community_id]["family_id"] == m.family_id
    family = pq.read_table(entities_dir / FAMILY_FILENAME)
    main_by_id = dict(
        zip(
            family.column("family_id").to_pylist(),
            family.column("main_community_id").to_pylist(),
            strict=True,
        )
    )
    assert main_by_id["F-002"] == "UNKNOWN"
    assert main_by_id["F-001"] == GUANGDA


# ---------------------------------------------------------------------------
# ⑤ 幂等与既有行保留
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_rebuild_idempotent_byte_identical(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    paths = [
        entities_dir / COMMUNITY_FILENAME,
        entities_dir / ALIAS_FILENAME,
        entities_dir / FAMILY_FILENAME,
        entities_dir / SUBAREA_FILENAME,
    ]
    first = [p.read_bytes() for p in paths]
    build_community_family(data_dir=tmp_path)
    second = [p.read_bytes() for p in paths]
    assert first == second


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_existing_community_rows_preserved_except_target_columns(
    tmp_path: Path,
) -> None:
    build_community_entity(data_dir=tmp_path)
    build_alias_entity(data_dir=tmp_path)
    build_alias_census_backfill(data_dir=tmp_path, verdicts_csv=_VERDICTS_CSV)
    before = pq.read_table(tmp_path / "entities" / COMMUNITY_FILENAME)
    before_alias = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    build_community_family(data_dir=tmp_path)
    after = pq.read_table(tmp_path / "entities" / COMMUNITY_FILENAME)
    after_alias = pq.read_table(tmp_path / "entities" / ALIAS_FILENAME)
    kept_cols = [c for c in before.column_names if c not in ("standard_name", "notes")]
    b = before.select(kept_cols).sort_by("community_id")
    a = after.select(kept_cols).sort_by("community_id").slice(0, before.num_rows)
    assert b.equals(a)
    assert after.num_rows == before.num_rows + 2
    old_alias = before_alias.select(["alias_id", "community_id", "source_alias", "conflict_status"])
    new_alias = after_alias.select(["alias_id", "community_id", "source_alias", "conflict_status"])
    old_ids = old_alias.column("alias_id").to_pylist()
    new_map = dict(
        zip(
            new_alias.column("alias_id").to_pylist(),
            [
                (c, s, n)
                for c, s, n in zip(
                    new_alias.column("community_id").to_pylist(),
                    new_alias.column("source_alias").to_pylist(),
                    new_alias.column("conflict_status").to_pylist(),
                    strict=True,
                )
            ],
            strict=True,
        )
    )
    for i, aid in enumerate(old_ids):
        expect = (
            old_alias.column("community_id")[i].as_py(),
            old_alias.column("source_alias")[i].as_py(),
            old_alias.column("conflict_status")[i].as_py(),
        )
        assert new_map[aid] == expect, aid
    assert "AF-1-1" in new_map


def test_alias_addition_row_shape() -> None:
    assert ALIAS_ADDITION.alias_id == "AF-1-1"
    assert ALIAS_ADDITION.conflict_status.value == "一致"
    assert "user-adjudication-table.md" in ALIAS_ADDITION.source_ref


@pytest.mark.skip(reason="real catalog id space is excluded from the open-source distribution")
def test_all_family_main_entities_exist_or_unknown(tmp_path: Path) -> None:
    entities_dir = _build_all(tmp_path)
    rows = _community_rows(entities_dir)
    for family_row in FAMILIES:
        if family_row.main_community_id == "UNKNOWN":
            assert family_row.family_name == "金汐花园"
            continue
        assert family_row.main_community_id in rows, family_row.family_id
        assert rows[family_row.main_community_id]["family_id"] == family_row.family_id
