# -*- coding: utf-8 -*-
"""任务 2.2 自检：发布反例五态（无记录/判定 B 未过/无 S7/指纹错配/分支未列或超期）
均不出正式价，四态构造样例断言通过（tasks 2.2 验收命令）。

运行（退出码 0 为过）：
uv run pytest tests/test_release_record.py

被测模块（release_record/formal_states/formal_binding/stop_switch/formal_gate）
全部 stdlib-only；并入全量 pytest 套件后不再于文件顶部断言 numpy/polars
未被导入（套件内其他测试可能先导入 numpy/polars）。
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from gz_property_valuation.phase2 import formal_binding as fb  # noqa: E402
from gz_property_valuation.phase2 import formal_states as fs  # noqa: E402
from gz_property_valuation.phase2 import release_record as rr  # noqa: E402
from gz_property_valuation.phase2 import stop_switch as sw  # noqa: E402

CURRENT_COMBO = fb.compose_nine(
    {"model": "a" * 64, "feature": "b" * 64, "market_asset": "c" * 64,
     "calibration": "d" * 64, "coordination_policy": "e" * 64},
    inference_code_sha="f" * 64, formal_gate_rule_sha="1" * 64,
    adoption_contract_sha="2" * 64, current_block_map_sha="3" * 64,
    production_code_sha="4" * 64)

VAL_DATE = date(2026, 7, 20)


def make_release(**over) -> dict:
    kwargs = dict(
        released_at="2026-09-14T00:00:00Z", released_by="S7-user",
        verdict_b_conclusion="PASS", verdict_b_report_path="判定B报告.md",
        verdict_b_report_sha256="ab" * 32,
        review2_report_path="审2报告.md", review2_report_sha256="cd" * 32,
        s7_confirmed=True, s7_confirmed_by="user", s7_confirmed_at="2026-09-14T00:00:00Z",
        allowed_branches=["normal", "degraded_total_floors"],
        market_materials_cutoff="2026-07-20", t_minus_c_days=90, t_minus_l_days=180,
        combination_fingerprint=CURRENT_COMBO)
    kwargs.update(over)
    return rr.build_release_record(**kwargs)


def envelope_of(record, combo=CURRENT_COMBO, branch="normal",
                valuation_date=VAL_DATE, stop_stopped=False) -> dict:
    """五态反例的完整判定路径（与 candidate_ops.formal_estimate 同序的状态机消费）。"""
    rc = rr.check_release(record, combo, branch=branch, valuation_date=valuation_date)
    state = fs.decide(stop_stopped=stop_stopped, release_ok=rc["ok"],
                      input_rejected=False, gate_eligible=None)
    return fs.build_envelope(
        request_id="REQ-TEST", state=state, reason_codes=list(rc["reason_codes"]),
        release_check=rc, combination=combo, branch=branch)


def assert_no_formal_price(env: dict) -> None:
    """非 priced 三态共同断言：正式字段为空且不携带资格门 detail 值（防泄露）。"""
    assert env["state"] in (fs.STATE_INELIGIBLE, fs.STATE_REJECTED,
                            fs.STATE_VERSION_DISABLED)
    assert env["formal_price"] is None
    assert env["formal_report"] is None
    assert "detail" not in json.dumps(env["eligibility"] or {})


# ---------------------------------------------------------------- 默认态＝未发布

def test_default_missing_record_means_unreleased(tmp_path):
    assert rr.load_release_record(tmp_path / "none.json") is None
    rc = rr.check_release(None, CURRENT_COMBO, branch="normal",
                          valuation_date=VAL_DATE)
    assert not rc["ok"] and rr.RC_RECORD_MISSING in rc["reason_codes"]


def test_release_record_roundtrip_and_corrupt(tmp_path):
    p = tmp_path / rr.RELEASE_RECORD_FILENAME
    p.write_text(json.dumps(make_release(), ensure_ascii=False), encoding="utf-8")
    loaded = rr.load_release_record(p)
    assert loaded["schema_version"] == rr.SCHEMA_VERSION
    p.write_text("{broken", encoding="utf-8")
    try:
        rr.load_release_record(p)
        raise AssertionError("损坏记录必须抛 ReleaseRecordError")
    except rr.ReleaseRecordError:
        pass


# ---------------------------------------------------------------- 反例五态

def test_counterexample_1_record_missing():
    env = envelope_of(None)
    assert_no_formal_price(env)
    assert env["reason_codes"] == [rr.RC_RECORD_MISSING]


def test_counterexample_2_verdict_b_not_passed():
    env = envelope_of(make_release(verdict_b_conclusion="FAIL"))
    assert_no_formal_price(env)
    assert rr.RC_VERDICT_B_NOT_PASSED in env["reason_codes"]


def test_counterexample_3_s7_not_confirmed():
    env = envelope_of(make_release(s7_confirmed=False))
    assert_no_formal_price(env)
    assert rr.RC_S7_NOT_CONFIRMED in env["reason_codes"]


def test_counterexample_4_combination_mismatch_map_updated_bundle_unchanged():
    """映射更新而五组件（bundle_id）不变 → 组合指纹仍识别为不同组合，拒绝混用。"""
    map_updated = fb.compose_nine(
        {k: CURRENT_COMBO["components"][k] for k in fb.FIVE_COMPONENTS},
        inference_code_sha=CURRENT_COMBO["components"]["inference_code"],
        formal_gate_rule_sha=CURRENT_COMBO["components"]["formal_gate_rule"],
        adoption_contract_sha=CURRENT_COMBO["components"]["adoption_contract"],
        current_block_map_sha="9" * 64,
        production_code_sha=CURRENT_COMBO["components"]["production_code"])
    assert (map_updated["components"].get("model")
            == CURRENT_COMBO["components"]["model"])
    assert map_updated["composition_id"] != CURRENT_COMBO["composition_id"]
    cmp = fb.compare_combinations(CURRENT_COMBO, map_updated)
    assert not cmp["ok"] and cmp["mismatches"] == ["current_block_map"]
    env = envelope_of(make_release(), combo=map_updated)
    assert_no_formal_price(env)
    assert rr.RC_COMBINATION_MISMATCH in env["reason_codes"]
    detail = env["release_check"]["detail"]["combination_check"]
    assert detail["mismatches"] == ["current_block_map"]


def test_counterexample_5a_branch_not_listed():
    env = envelope_of(make_release(allowed_branches=["degraded_total_floors"]),
                      branch="normal")
    assert_no_formal_price(env)
    assert rr.RC_BRANCH_NOT_ALLOWED in env["reason_codes"]


def test_counterexample_5b_expired():
    """资产截止 2026-07-20、T−C=90 → 失效日 2026-10-18；估值日 2026-10-20 超期不出价。"""
    rec = make_release()
    assert rec["validity"]["expires_on"] == "2026-10-18"
    env = envelope_of(rec, valuation_date=date(2026, 10, 20))
    assert_no_formal_price(env)
    assert rr.RC_EXPIRED in env["reason_codes"]


def test_valid_release_passes_global_and_branch_expiry():
    rc = rr.check_release(make_release(), CURRENT_COMBO, branch="normal",
                          valuation_date=VAL_DATE)
    assert rc["ok"] and rc["reason_codes"] == []


# ---------------------------------------------------------------- 四态构造样例

def _priced_payload() -> tuple[dict, dict]:
    formal_price = {"unit_price_per_sqm": 35717.35, "total_price": 3196718.0,
                    "area_sqm": 89.5, "valuation_date": "2026-07-20",
                    "actual_data_cutoff": "2026-07-19",
                    "interval": {"nominal_80": {"low": 30000.0, "high": 42000.0},
                                 "nominal_90": {"low": 28000.0, "high": 45000.0}}}
    formal_report = {"unit_price": formal_price["unit_price_per_sqm"],
                     "total_price": formal_price["total_price"],
                     "valuation_date": "2026-07-20",
                     "actual_data_cutoff": "2026-07-19",
                     "interval_and_width": formal_price["interval"],
                     "main_market_basis": {"b0_level": "community", "b0_pred": 36203.0,
                                           "b0_window_n": 12},
                     "main_limits": ["区间为校准/训练窗内拟合覆盖"],
                     "applicable_scope": {"population": "云溪区·普通住宅",
                                          "branch": "degraded_total_floors"},
                     "formal_adoption_basis": {"contract_id": "DEMO-RELEASE-001",
                                               "composition_id": CURRENT_COMBO["composition_id"]},
                     "release_version": {"composition_id": CURRENT_COMBO["composition_id"]}}
    return formal_price, formal_report


def test_state_priced_sample():
    fp, fr = _priced_payload()
    env = fs.build_envelope(request_id="REQ-P", state=fs.STATE_PRICED,
                            formal_price=fp, formal_report=fr)
    assert env["state"] == "priced"
    assert env["formal_price"]["unit_price_per_sqm"] is not None
    assert all(k in env["formal_report"] for k in fs.FORMAL_REPORT_FIELDS)


def test_state_ineligible_sample():
    gate = {"eligible": False, "failed_gates": ["GATE-05"],
            "reason_codes": ["FG05_SUPPORT_INSUFFICIENT"], "notes": [],
            "gates": [{"gate": "GATE-05", "name": "community_support", "passed": False,
                       "reason_codes": ["FG05_SUPPORT_INSUFFICIENT"], "notes": [],
                       "detail": {"support_case_count": 3, "min_cases": 5}}],
            "formal_gate_version": "formal-gate-v1"}
    state = fs.decide(stop_stopped=False, release_ok=True, input_rejected=False,
                      gate_eligible=False)
    env = fs.build_envelope(request_id="REQ-I", state=state,
                            reason_codes=gate["reason_codes"],
                            eligibility=fs.eligibility_summary(gate))
    assert env["state"] == "ineligible"
    assert_no_formal_price(env)
    assert env["eligibility"]["gates"][0].get("detail") is None


def test_state_rejected_sample():
    state = fs.decide(stop_stopped=False, release_ok=True, input_rejected=True,
                      gate_eligible=None)
    env = fs.build_envelope(request_id="REQ-R", state=state,
                            reason_codes=["FM_INPUT_REJECTED"])
    assert env["state"] == "rejected"
    assert_no_formal_price(env)


def test_state_version_disabled_stop_switch_priority_over_release_and_gate(tmp_path):
    """首启授权≠停止开关：停止开关优先于有效发布记录与资格通过（版本停用）。"""
    sw.write_stop_switch(tmp_path, stopped=True, reason="ops")
    status = sw.check_stop_switch(tmp_path)
    assert status["stopped"] and status["reason_codes"] == [sw.RC_STOPPED]
    state = fs.decide(stop_stopped=True, release_ok=True, input_rejected=False,
                      gate_eligible=True)
    env = fs.build_envelope(request_id="REQ-V", state=state,
                            reason_codes=[sw.RC_STOPPED])
    assert env["state"] == "version_disabled"
    assert_no_formal_price(env)


def test_envelope_rejects_price_on_non_priced_states():
    fp, fr = _priced_payload()
    for bad in (fs.STATE_INELIGIBLE, fs.STATE_REJECTED, fs.STATE_VERSION_DISABLED):
        try:
            fs.build_envelope(request_id="X", state=bad, formal_price=fp,
                              formal_report=fr)
            raise AssertionError(f"{bad} 态携带正式字段必须被拒绝")
        except ValueError:
            pass


def test_four_states_mutually_exclusive_decide_matrix():
    assert fs.decide(stop_stopped=True, release_ok=True, input_rejected=False,
                     gate_eligible=True) == fs.STATE_VERSION_DISABLED
    assert fs.decide(stop_stopped=False, release_ok=False, input_rejected=True,
                     gate_eligible=None) == fs.STATE_VERSION_DISABLED
    assert fs.decide(stop_stopped=False, release_ok=True, input_rejected=True,
                     gate_eligible=None) == fs.STATE_REJECTED
    assert fs.decide(stop_stopped=False, release_ok=True, input_rejected=False,
                     gate_eligible=False) == fs.STATE_INELIGIBLE
    assert fs.decide(stop_stopped=False, release_ok=True, input_rejected=False,
                     gate_eligible=True) == fs.STATE_PRICED


def test_schema_incomplete_release_rejected():
    rec = make_release()
    rec.pop("review2")
    rc = rr.check_release(rec, CURRENT_COMBO, branch="normal",
                          valuation_date=VAL_DATE)
    assert not rc["ok"] and rr.RC_SCHEMA_INVALID in rc["reason_codes"]
