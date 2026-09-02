"""道路级命名终态清零测试（ext-sale-ingest-scope-v1-2，P1）。

覆盖：排除枚举与 blocked 语义、冻结 overrides 加载、应用状态机（待定→排除、
幂等跳过、非法前置拒绝）、行数/alias_id 不变、community.parquet 零触碰、
真实 overrides 文件对拍。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pytest

from compsval.contract.models import AliasConflictStatus
from compsval.entities.alias_terminal import (
    apply_alias_terminal_overrides,
    load_terminal_overrides,
)
from compsval.entities.backfill import load_community_lookup, resolve_community_id

# overrides 文件随 change 归档迁移（2026-09-01）；候选回落保持测试可复现。
_OVERRIDE_CANDIDATES = (
    Path(__file__).resolve().parents[2]
    / "openspec/changes/ext-sale-ingest-scope-v1-2/execution/alias_terminal_overrides.json",
    Path(__file__).resolve().parents[2]
    / (
        "openspec/changes/archive/2026-09-01-ext-sale-ingest-scope-v1-2"
        "/execution/alias_terminal_overrides.json"
    ),
)
_OVERRIDES = next((p for p in _OVERRIDE_CANDIDATES if p.is_file()), None)
if _OVERRIDES is None:
    pytest.skip(
        "alias terminal overrides data is not included in the open-source distribution",
        allow_module_level=True,
    )


def _alias_table(rows: list[tuple[str, str, str, str]]) -> pa.Table:
    """(alias_id, source_alias, conflict_status, community_id) → 别名表。"""
    return pa.table(
        {
            "alias_id": pa.array([r[0] for r in rows], type=pa.string()),
            "community_id": pa.array([r[3] for r in rows], type=pa.string()),
            "source_alias": pa.array([r[1] for r in rows], type=pa.string()),
            "source_id": pa.array(["SRC-007"] * len(rows), type=pa.string()),
            "source_ref": pa.array(
                [f"census merge_candidates + 裁决表：{r[1]}" for r in rows], type=pa.string()
            ),
            "conflict_status": pa.array([r[2] for r in rows], type=pa.string()),
        }
    )


def _seed(lake: Path, table: pa.Table) -> None:
    import pyarrow.parquet as pq

    (lake / "entities").mkdir(parents=True, exist_ok=True)
    pq.write_table(table, lake / "entities" / "community_alias.parquet")


_ROWS = [
    ("AC-63", "工业大道", "待定", "C-XXXX0135"),
    ("AC-64", "工业大道", "待定", "C-XXXX0140"),
    ("AC-65", "工业大道", "待定", "C-XXXX0141"),
    ("AC-66", "工业大道", "待定", "C-XXXX0168"),
    ("AC-67", "工业大道", "待定", "C-XXXX0185"),
    ("AC-68", "工业大道南", "待定", "C-XXXX0141"),
    ("AC-69", "工业大道南", "待定", "C-XXXX0168"),
    ("AC-70", "工业大道南", "待定", "C-XXXX0185"),
    ("AC-72", "泰沙路", "待定", "C-XXXX0089"),
    ("AC-73", "泰沙路", "待定", "C-XXXX0090"),
    ("AC-1", "一致别名甲", "一致", "C-XXXX0005"),
    ("AC-2", "冲突别名乙", "冲突", "C-XXXX0006"),
]


def test_excluded_enum_value_registered() -> None:
    assert AliasConflictStatus.EXCLUDED.value == "排除"


def test_excluded_is_blocked_in_auto_mapping(tmp_path: Path) -> None:
    """排除别名与待定同为 blocked：自动映射不产生结果。"""
    _seed(tmp_path, _alias_table([("AC-72", "泰沙路", "排除", "C-XXXX0089")]))
    lookup = load_community_lookup(data_dir=tmp_path)
    cid, outcome, reason = resolve_community_id("泰沙路", lookup)
    assert cid is None
    assert outcome.value == "低置信/冲突，不静默合并"
    assert "排除" in reason


def test_load_real_overrides_file_has_ten_ids() -> None:
    overrides = load_terminal_overrides(_OVERRIDES)
    assert overrides.alias_ids == (
        "AC-63", "AC-64", "AC-65", "AC-66", "AC-67",
        "AC-68", "AC-69", "AC-70", "AC-72", "AC-73",
    )
    assert overrides.adjudicated_by == "user"
    assert "冲突清单 #10" in overrides.basis


def test_apply_flips_pending_to_excluded(tmp_path: Path) -> None:
    _seed(tmp_path, _alias_table(_ROWS))
    community_path = tmp_path / "entities" / "community.parquet"
    community_path.write_bytes(b"protected")  # 受保护资产哨兵

    out = apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)
    import pyarrow.parquet as pq

    result = pq.read_table(out).to_pylist()
    assert len(result) == len(_ROWS)  # 行数不变
    by_id = {row["alias_id"]: row for row in result}
    overrides = load_terminal_overrides(_OVERRIDES)
    for alias_id in overrides.alias_ids:
        row = by_id[alias_id]
        assert row["conflict_status"] == "排除"
        assert "终态裁决" in row["source_ref"]
        assert "冲突清单 #10" in row["source_ref"]
        assert row["source_ref"].startswith("census merge_candidates")  # 原批次溯源保留
    assert by_id["AC-1"]["conflict_status"] == "一致"  # 非目标行零触碰
    assert by_id["AC-2"]["conflict_status"] == "冲突"
    assert community_path.read_bytes() == b"protected"  # 哨兵未动


def test_apply_idempotent_byte_identical(tmp_path: Path) -> None:
    """同输入连续两次：parquet 逐字节一致，裁决溯源不重复追加。"""
    _seed(tmp_path, _alias_table(_ROWS))
    first = apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)
    first_bytes = first.read_bytes()
    second = apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)
    assert first_bytes == second.read_bytes()
    import pyarrow.parquet as pq

    by_id = {row["alias_id"]: row for row in pq.read_table(first).to_pylist()}
    assert by_id["AC-72"]["source_ref"].count("终态裁决") == 1


def test_apply_rejects_inconsistent_precondition(tmp_path: Path) -> None:
    """目标行状态非待定（已一致）→ 显式拒绝，不静默改写。"""
    _seed(tmp_path, _alias_table([("AC-63", "工业大道", "一致", "C-XXXX0135")]))
    with pytest.raises(ValueError, match="拒绝应用"):
        apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)


def test_apply_rejects_missing_target(tmp_path: Path) -> None:
    _seed(tmp_path, _alias_table([("AC-1", "一致别名甲", "一致", "C-XXXX0005")]))
    with pytest.raises(ValueError, match="缺失"):
        apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)


def test_manifest_records_override_fingerprint(tmp_path: Path) -> None:
    _seed(tmp_path, _alias_table(_ROWS))
    out = apply_alias_terminal_overrides(data_dir=tmp_path, overrides_path=_OVERRIDES)
    manifest = json.loads(out.with_suffix(".manifest.json").read_text(encoding="utf-8"))
    datasets = [item["dataset"] for item in manifest["inputs"]]
    assert "alias_terminal_overrides" in datasets
    override_input = [
        item for item in manifest["inputs"] if item["dataset"] == "alias_terminal_overrides"
    ][0]
    assert override_input["content_hash"] == hashlib.sha256(
        _OVERRIDES.read_bytes()
    ).hexdigest()
    assert manifest["row_count"] == len(_ROWS)
