# -*- coding: utf-8 -*-
"""任务 2.6 自检：停止开关（状态文件＋运行时检查，与发布记录独立）。

验收语义（tasks 2.6）：开关后**任何请求**返回 version_disabled 且**无正式价格字段**。
运行（退出码 0 为过）：
uv run pytest tests/test_stop_switch.py

说明：停止开关被测逻辑全部在 stdlib-only 模块（stop_switch/formal_states/
release_record/formal_binding）；真实引擎级开关早退（formal_estimate 首门）不在
本文件范围（formal_estimate 属生产注册链，发布树测试当前未覆盖该层）。真实数据区
零写入（状态文件一律用临时目录）。
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


def make_release(**over) -> dict:
    kwargs = dict(
        released_at="2026-09-14T00:00:00Z", released_by="S7-user",
        verdict_b_conclusion="PASS", verdict_b_report_path="判定B报告.md",
        verdict_b_report_sha256="ab" * 32,
        review2_report_path="审2报告.md", review2_report_sha256="cd" * 32,
        s7_confirmed=True, s7_confirmed_by="user", s7_confirmed_at="2026-09-14T00:00:00Z",
        allowed_branches=["normal", "degraded_total_floors"],
        market_materials_cutoff="2026-07-20", t_minus_c_days=90, t_minus_l_days=180,
        combination_fingerprint=COMBO)
    kwargs.update(over)
    return rr.build_release_record(**kwargs)


def formal_gate_output(stop: dict, release: dict | None) -> dict:
    """复现 formal_estimate 前两门的消费顺序：停止开关 → 发布记录（全局层）。"""
    if stop["stopped"]:
        return fs.build_envelope(request_id="REQ-SW", state=fs.STATE_VERSION_DISABLED,
                                 reason_codes=list(stop["reason_codes"]))
    rc = rr.check_release(release, COMBO)
    state = fs.decide(stop_stopped=False, release_ok=rc["ok"], input_rejected=False,
                      gate_eligible=True)
    return fs.build_envelope(request_id="REQ-SW", state=state,
                             reason_codes=list(rc["reason_codes"]),
                             formal_price={"unit_price_per_sqm": 1.0} if state == "priced" else None,
                             formal_report={"unit_price": 1.0} if state == "priced" else None)


def assert_disabled_no_price(env: dict) -> None:
    assert env["state"] == fs.STATE_VERSION_DISABLED
    assert env["formal_price"] is None
    assert env["formal_report"] is None
    assert "unit_price" not in json.dumps(env["formal_price"])


# ---------------------------------------------------------------- 状态文件三态

def test_absent_switch_fails_closed(tmp_path):
    """默认回退＝停止正式出价：状态文件不存在 → 停止（fail-closed）。"""
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] is True and status["present"] is False
    assert sw.RC_DEFAULT_FALLBACK_STOPPED in status["reason_codes"]


def test_switch_on_stopped(tmp_path):
    sw.write_stop_switch(tmp_path, stopped=True, reason="ops-drill", operator="ops")
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] is True and sw.RC_STOPPED in status["reason_codes"]
    assert status["detail"]["reason"] == "ops-drill"


def test_switch_off_is_only_running_state(tmp_path):
    sw.write_stop_switch(tmp_path, stopped=False, reason="released", operator="ops")
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] is False and status["reason_codes"] == []


def test_corrupt_switch_fails_closed(tmp_path):
    """状态文件损坏 → 按停止处理（fail-closed）。"""
    (tmp_path / sw.STOP_SWITCH_FILENAME).write_text("{bad json", encoding="utf-8")
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] is True and sw.RC_STATE_CORRUPT in status["reason_codes"]


def test_semantically_ambiguous_switch_fails_closed(tmp_path):
    (tmp_path / sw.STOP_SWITCH_FILENAME).write_text(
        json.dumps({"schema_version": sw.STOP_SWITCH_SCHEMA, "stopped": "yes"}),
        encoding="utf-8")
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] is True and sw.RC_STATE_CORRUPT in status["reason_codes"]


# ---------------------------------------------------------------- 开关后任何请求

def test_any_request_version_disabled_and_no_price_after_switch(tmp_path):
    """开关后任何请求 → version_disabled 且无正式价格字段（含输入未解析/发布有效两对照）。"""
    sw.write_stop_switch(tmp_path, stopped=True, reason="stop-all")
    stop = sw.check_stop_switch(tmp_path)
    # 发布记录有效＋资格假定通过：开关仍然优先（首启授权≠停止开关）
    env_ok = formal_gate_output(stop, make_release())
    assert_disabled_no_price(env_ok)
    assert sw.RC_STOPPED in env_ok["reason_codes"]
    # 发布记录缺失：同样 version_disabled
    env_missing = formal_gate_output(stop, None)
    assert_disabled_no_price(env_missing)
    # 输入不可解析的请求（未到解析层）：同样 version_disabled
    env_raw = fs.build_envelope(request_id=None, state=fs.STATE_VERSION_DISABLED,
                                reason_codes=list(stop["reason_codes"]))
    assert_disabled_no_price(env_raw)


def test_switch_independent_of_release_record(tmp_path):
    """开关与发布记录独立：同一有效发布记录，开关翻转决定可用性。"""
    release = make_release()
    sw.write_stop_switch(tmp_path, stopped=False)
    env_run = formal_gate_output(sw.check_stop_switch(tmp_path), release)
    assert env_run["state"] == fs.STATE_PRICED
    assert env_run["formal_price"]["unit_price_per_sqm"] is not None
    sw.write_stop_switch(tmp_path, stopped=True, reason="stop")
    env_stop = formal_gate_output(sw.check_stop_switch(tmp_path), release)
    assert_disabled_no_price(env_stop)


# ---------------------------------------------------------------- 分支停用（恢复仅经发布记录）

def test_branch_suspension_resume_only_via_release_record(tmp_path):
    susp = sw.write_branch_suspension(tmp_path, branch="normal",
                                      reason_codes=["FB_HARD_GATE_FAIL"],
                                      evidence={"month": "2026-08"},
                                      evidence_pointer="formal-records.jsonl")
    assert susp["appended"] is True
    active = sw.check_branch_suspended(tmp_path, "normal")
    assert active["suspended"] and active["reason_codes"] == [sw.RC_BRANCH_SUSPENDED]
    other = sw.check_branch_suspended(tmp_path, "degraded_total_floors")
    assert not other["suspended"]
    # 恢复仅经发布记录：resumed_suspensions 登记该 suspension_id 才解除
    released = make_release(resumed_suspensions=[susp["suspension_id"]])
    resumed = sw.check_branch_suspended(tmp_path, "normal", released)
    assert not resumed["suspended"]
    unrelated = make_release(resumed_suspensions=["0" * 64])
    still = sw.check_branch_suspended(tmp_path, "normal", unrelated)
    assert still["suspended"]


def test_suspension_idempotent(tmp_path):
    a = sw.write_branch_suspension(tmp_path, branch="normal",
                                   reason_codes=["FB_HARD_GATE_FAIL"],
                                   evidence={"month": "2026-08"})
    b = sw.write_branch_suspension(tmp_path, branch="normal",
                                   reason_codes=["FB_HARD_GATE_FAIL"],
                                   evidence={"month": "2026-08"})
    assert a["suspension_id"] == b["suspension_id"] and b["appended"] is False
    assert len(sw.load_suspensions(tmp_path)) == 1


def test_default_state_dir_is_data_area_runtime(tmp_path):
    p = sw.stop_switch_path(tmp_path)
    assert p.name == "stop-switch.json" and p.parent == tmp_path
    d = sw.default_state_dir()
    assert d.name == "ops-state"
    assert d.parts[-4:] == ("03-估值引擎", "data", "phase2", "ops-state")
