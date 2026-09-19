# -*- coding: utf-8 -*-
"""审2 RV-ECR-VERIFY-01 返工反例：F7 分支停用文件损坏 → fail-closed 停用。

覆盖审2 §5 修复验收：区分文件不存在与文件存在但 schema/条目损坏；后者拒绝出价并
保留原因，不得解释成无停用；覆盖 {}、[]、错误字段类型、无效条目等用例；损坏占位
不受发布记录 resumed_suspensions 恢复。

运行（退出码 0 为过）：
uv run pytest tests/test_stop_switch_rework.py

被测模块设计为 stdlib-only（stop_switch/release_record/formal_binding/
formal_states 只依赖标准库）；并入全量 pytest 套件后不再于文件顶部断言
numpy/polars 未被导入（套件内其他测试可能先导入 numpy/polars）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from gz_property_valuation.phase2 import formal_binding as fb  # noqa: E402
from gz_property_valuation.phase2 import formal_states as fs  # noqa: E402
from gz_property_valuation.phase2 import release_record as rr  # noqa: E402
from gz_property_valuation.phase2 import stop_switch as sw  # noqa: E402

COMBO = fb.compose_nine(
    {"model": "a" * 64, "feature": "b" * 64, "market_asset": "c" * 64,
     "calibration": "d" * 64, "coordination_policy": "e" * 64},
    inference_code_sha="f" * 64, formal_gate_rule_sha="1" * 64,
    adoption_contract_sha="2" * 64, current_block_map_sha="3" * 64,
    production_code_sha="4" * 64)


def write_suspension_file(tmp_path: Path, content) -> Path:
    p = tmp_path / sw.BRANCH_SUSPENDED_FILENAME
    text = content if isinstance(content, str) else json.dumps(content)
    p.write_text(text, encoding="utf-8")
    return p


# ------------------------------------------------------------ 文件不存在 vs 存在但损坏

def test_absent_file_means_no_suspension(tmp_path):
    assert sw.load_suspensions(tmp_path) == []
    check = sw.check_branch_suspended(tmp_path, "normal")
    assert check["suspended"] is False and check["reason_codes"] == []


def test_f7_empty_object_is_corrupt_not_clean(tmp_path):
    """审2 F7 反例原样：branch-suspended.json 内容 {} → 不得静默恢复出价。"""
    write_suspension_file(tmp_path, {})
    rows = sw.load_suspensions(tmp_path)
    assert len(rows) == 1 and rows[0]["corrupted"] is True
    assert rows[0]["branch"] == "*" and sw.RC_STATE_CORRUPT in rows[0]["reason_codes"]
    check = sw.check_branch_suspended(tmp_path, "normal")
    assert check["suspended"] is True
    assert sw.RC_BRANCH_SUSPENDED in check["reason_codes"]
    assert sw.RC_STATE_CORRUPT in check["reason_codes"]


def test_f7_empty_list_and_wrong_top_types_corrupt(tmp_path):
    for content in ([], "string", 42, None,
                    {"suspensions": {}},          # suspensions 错误字段类型
                    {"suspensions": "nope"}):
        tmp2 = tmp_path / str(abs(hash(json.dumps(content, default=str))))
        tmp2.mkdir()
        write_suspension_file(tmp2, content)
        rows = sw.load_suspensions(tmp2)
        assert len(rows) == 1 and rows[0]["corrupted"] is True, content
        assert sw.check_branch_suspended(tmp2, "normal")["suspended"] is True, content


def test_f7_invalid_entries_replaced_by_corrupt_placeholder(tmp_path):
    """无效条目（非 dict/缺键/字段类型错）→ 占位停用；有效条目保留。"""
    write_suspension_file(tmp_path, {"suspensions": [
        "not-a-dict",
        {"branch": "normal"},                       # 缺 suspension_id
        {"suspension_id": "  ", "branch": "normal", "reason_codes": []},  # 空白 id
        {"suspension_id": "S1", "branch": 7, "reason_codes": []},         # branch 类型错
        {"suspension_id": "S2", "branch": "normal", "reason_codes": "not-a-list"},
        {"suspension_id": "S-OK", "branch": "normal", "reason_codes": ["FB_HARD_GATE_FAIL"]},
    ]})
    rows = sw.load_suspensions(tmp_path)
    corrupted = [r for r in rows if r.get("corrupted")]
    valid = [r for r in rows if not r.get("corrupted")]
    assert len(corrupted) == 5 and len(valid) == 1
    assert valid[0]["suspension_id"] == "S-OK"
    check = sw.check_branch_suspended(tmp_path, "normal")
    assert check["suspended"] is True
    assert sw.RC_STATE_CORRUPT in check["reason_codes"]
    other = sw.check_branch_suspended(tmp_path, "degraded_total_floors")
    assert other["suspended"] is True, "损坏占位 branch=* 全局停用（无法确认损坏条目范围，fail-closed）"
    assert sw.RC_STATE_CORRUPT in other["reason_codes"]


def test_f7_corrupt_placeholder_not_resumable_via_release_record(tmp_path):
    """损坏占位不得经发布记录 resumed_suspensions 登记 "CORRUPT" 静默恢复出价。"""
    write_suspension_file(tmp_path, {})
    rec = rr.build_release_record(
        released_at="2026-09-14T00:00:00Z", released_by="S7-user",
        verdict_b_conclusion="PASS", verdict_b_report_path="判定B报告.md",
        verdict_b_report_sha256="v" * 64,
        review2_report_path="审2报告.md", review2_report_sha256="s" * 64,
        s7_confirmed=True, s7_confirmed_by="user", s7_confirmed_at="2026-09-14T00:00:00Z",
        allowed_branches=["normal"], resumed_suspensions=["CORRUPT"],
        market_materials_cutoff="2026-07-20", t_minus_c_days=90, t_minus_l_days=180,
        combination_fingerprint=COMBO)
    check = sw.check_branch_suspended(tmp_path, "normal", rec)
    assert check["suspended"] is True, "损坏登记必须修复文件本身，不得经发布记录恢复"


def test_f7_valid_suspension_still_resumable_via_release_record(tmp_path):
    """健全条目的既有恢复语义不变（对照）。"""
    susp = sw.write_branch_suspension(tmp_path, branch="normal",
                                      reason_codes=["FB_HARD_GATE_FAIL"],
                                      evidence={"month": "2026-08"})
    rec = rr.build_release_record(
        released_at="2026-09-14T00:00:00Z", released_by="S7-user",
        verdict_b_conclusion="PASS", verdict_b_report_path="判定B报告.md",
        verdict_b_report_sha256="v" * 64,
        review2_report_path="审2报告.md", review2_report_sha256="s" * 64,
        s7_confirmed=True, s7_confirmed_by="user", s7_confirmed_at="2026-09-14T00:00:00Z",
        allowed_branches=["normal"], resumed_suspensions=[susp["suspension_id"]],
        market_materials_cutoff="2026-07-20", t_minus_c_days=90, t_minus_l_days=180,
        combination_fingerprint=COMBO)
    check = sw.check_branch_suspended(tmp_path, "normal", rec)
    assert check["suspended"] is False and check["reason_codes"] == []


def test_f7_envelope_layer_suspension_blocks_pricing(tmp_path):
    """信封层对照（formal_estimate 同序消费）：损坏停用 → version_disabled 无价格。"""
    write_suspension_file(tmp_path, {})
    susp = sw.check_branch_suspended(tmp_path, "normal")
    rc = rr.check_release(None, COMBO)
    env = fs.build_envelope(
        request_id="REQ-F7", state=fs.STATE_VERSION_DISABLED,
        reason_codes=list(susp["reason_codes"]), release_check=rc,
        combination=COMBO, branch="normal")
    assert env["state"] == fs.STATE_VERSION_DISABLED
    assert env["formal_price"] is None and env["formal_report"] is None
