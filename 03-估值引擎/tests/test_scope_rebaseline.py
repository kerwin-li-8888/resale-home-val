"""ScopePolicy v1.2 数据驱动重定级测试（ext-sale-ingest-scope-v1-2，P3）。

覆盖：12 个月窗口边界（左闭右开）、统一判据派生（≥15/8-14/<8，无小区级
例外）、边界三分门控、版本化落盘（v1.1 零覆盖）、判据机械可复现、formal
闸门不切换（DEFAULT_RULE_VERSION 维持 1.1）。
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.valuation.scope import (
    DEFAULT_RULE_VERSION,
    SCOPE_V12_AS_OF,
    build_scope_policy_v1_2,
    compute_cases_12m,
    derive_policy_v1_2,
    scope_policy_filename,
    window_start_of,
)

AS_OF = date(2026, 8, 31)
WINDOW_START = date(2025, 8, 31)


def _valid_sale(rows: list[tuple[str, date]]) -> pa.Table:
    return pa.table(
        {
            "community_id": pa.array([r[0] for r in rows], type=pa.string()),
            "sale_date": pa.array([r[1] for r in rows], type=pa.date32()),
        }
    )


def test_window_start_and_boundary_dates() -> None:
    """窗口左闭右开：起点日计入，as_of 当日不计入。"""
    table = _valid_sale(
        [
            ("C-A", WINDOW_START),  # 左端计入
            ("C-A", date(2026, 8, 30)),  # 计入
            ("C-A", AS_OF),  # 右端开区间不计入
            ("C-A", date(2025, 8, 30)),  # 窗口前不计入
        ]
    )
    assert window_start_of(AS_OF) == WINDOW_START
    assert compute_cases_12m(table, as_of=AS_OF) == {"C-A": 2}


def test_derive_policy_thresholds_and_unknown_ids() -> None:
    cases = {"C-1": 15, "C-2": 14, "C-3": 8, "C-4": 7, "C-5": 100, "C-X": 50}
    policy = derive_policy_v1_2(cases, community_ids=["C-1", "C-2", "C-3", "C-4", "C-5"])
    assert policy.rule_version == "1.2"
    assert policy.supported_ids == frozenset({"C-1", "C-5"})
    assert policy.conditional_ids == frozenset({"C-2", "C-3"})
    assert "C-4" not in policy.supported_ids | policy.conditional_ids  # <8 暂不支撑
    assert "C-X" not in policy.supported_ids | policy.conditional_ids  # 名录外不计


def _community_table(rows: list[tuple[str, str, str]]) -> pa.Table:
    """(community_id, standard_name, boundary_status) → 最小 community 表。"""
    return pa.table(
        {
            "community_id": pa.array([r[0] for r in rows], type=pa.string()),
            "standard_name": pa.array([r[1] for r in rows], type=pa.string()),
            "block": pa.array(["测试板块"] * len(rows), type=pa.string()),
            "boundary_status": pa.array([r[2] for r in rows], type=pa.string()),
            "source_ref": pa.array(["测试"] * len(rows), type=pa.string()),
        }
    )


def _seed_lake(tmp_path: Path) -> Path:
    """A:25(纳入) B:10(参考) C:3(拒绝) D:25但边界待定(参考) E:范围外(拒绝) F:0(拒绝)。"""
    (tmp_path / "entities").mkdir(parents=True, exist_ok=True)
    (tmp_path / "marts").mkdir(parents=True, exist_ok=True)
    communities = _community_table(
        [
            ("C-A", "小区A", "机器确认"),
            ("C-B", "小区B", "机器确认"),
            ("C-C", "小区C", "机器确认"),
            ("C-D", "小区D", "边界待定"),
            ("C-E", "小区E", "正式范围外"),
            ("C-F", "小区F", "机器确认"),
        ]
    )
    pq.write_table(communities, tmp_path / "entities" / "community.parquet")

    sales = [
        *[("C-A", date(2026, 3, 1))] * 25,
        *[("C-B", date(2026, 3, 1))] * 10,
        *[("C-C", date(2026, 3, 1))] * 3,
        *[("C-D", date(2026, 3, 1))] * 25,
        *[("C-F", date(2024, 1, 1))] * 5,  # 窗口外 → 真实零
    ]
    pq.write_table(_valid_sale(sales), tmp_path / "marts" / "valid_sale.parquet")
    return tmp_path


def test_build_v1_2_decisions_and_zero_overwrite(tmp_path: Path) -> None:
    """全量统一重算 + 边界门控；v1.0/v1.1 文件逐字节不变。"""
    lake = _seed_lake(tmp_path)
    entities = lake / "entities"
    # 预置 v1.0/v1.1（既有版本基线）
    (entities / scope_policy_filename("1.0")).write_bytes(b"v1.0-bytes")
    (entities / scope_policy_filename("1.1")).write_bytes(b"v1.1-bytes")
    baseline = {
        name: hashlib.sha256((entities / name).read_bytes()).hexdigest()
        for name in (scope_policy_filename("1.0"), scope_policy_filename("1.1"))
    }

    result = build_scope_policy_v1_2(data_dir=lake)
    assert result.rule_version == "1.2"
    assert result.as_of == SCOPE_V12_AS_OF
    # 支撑集合为数据面（案例达标即"可支撑"）；边界门控作用在 decision 列
    assert result.supported_ids == frozenset({"C-A", "C-D"})
    assert result.conditional_ids == frozenset({"C-B"})

    table = pq.read_table(result.path)
    decisions = {
        row["community_id"]: (row["scope_decision"], row["support_level"], row["rule_version"])
        for row in table.to_pylist()
    }
    assert decisions["C-A"][0] == "纳入"
    assert decisions["C-B"][0] == "参考"  # 有条件支撑 → 参考级
    assert decisions["C-C"][0] == "拒绝"  # <8 暂不支撑
    assert decisions["C-D"][0] == "参考"  # 案例达标但边界待定 → 不纳入
    assert decisions["C-E"][0] == "拒绝"  # 范围外
    assert decisions["C-F"][0] == "拒绝"  # 窗口外 → 真实零
    assert all(v[2] == "1.2" for v in decisions.values())

    # 版本化零覆盖：新文件仅 v1.2，旧版本逐字节不变
    assert (entities / scope_policy_filename("1.2")).is_file()
    for name, digest in baseline.items():
        assert hashlib.sha256((entities / name).read_bytes()).hexdigest() == digest

    # 判据机械可复现：窗口计数重算 = 表内支撑集合依据
    sales = pq.read_table(lake / "marts" / "valid_sale.parquet")
    assert compute_cases_12m(sales, as_of=AS_OF)["C-A"] == 25
    assert compute_cases_12m(sales, as_of=AS_OF).get("C-F", 0) == 0


def test_default_rule_version_unchanged() -> None:
    """formal 闸门不切换：DEFAULT_RULE_VERSION 维持 1.1（v1.2 落盘≠生效）。"""
    assert DEFAULT_RULE_VERSION == "1.1"


def test_build_v1_2_requires_inputs(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        build_scope_policy_v1_2(data_dir=tmp_path)
