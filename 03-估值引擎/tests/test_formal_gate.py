# -*- coding: utf-8 -*-
"""formal_gate 单测：合同 §6.1 九项资格门各自通过/不通过样例＋总入口聚合断言。

运行（退出码 0 为过）：uv run pytest tests/test_formal_gate.py
被测模块只依赖标准库（stdlib-only）；并入全量 pytest 套件后不再于文件顶部
断言 numpy/polars 未被导入（套件内其他测试可能先导入 numpy/polars）。
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from gz_property_valuation.phase2 import formal_gate as fg  # noqa: E402

GOOD_REQUEST = {"district": "云溪区", "property_use": "普通住宅",
                "community_source_id": "C-XXXX0001", "area_sqm": 89.5}
GOOD_STATUS = {"scope_unverified": False, "out_of_scope": []}
GOOD_INTERVAL = {"nominal_80": {"low": 18000.0, "high": 24000.0},
                 "nominal_90": {"low": 16500.0, "high": 25500.0},
                 "source_layer": "community_C-XXXX0001", "source_layer_n": 42}


def codes_of(gate: dict) -> list[str]:
    return gate["reason_codes"]


# ---------------------------------------------------------------- GATE-01 人群与身份

def test_gate01_pass_in_scope_identity():
    g = fg.gate_01_scope_identity(GOOD_REQUEST, GOOD_STATUS)
    assert g["passed"] and codes_of(g) == []


def test_gate01_fail_out_of_scope_district():
    g = fg.gate_01_scope_identity({**GOOD_REQUEST, "district": "临湖区"}, GOOD_STATUS)
    assert not g["passed"] and "FG01_OUT_OF_SCOPE_DISTRICT" in codes_of(g)


def test_gate01_fail_out_of_scope_use():
    g = fg.gate_01_scope_identity({**GOOD_REQUEST, "property_use": "商业"}, GOOD_STATUS)
    assert not g["passed"] and "FG01_OUT_OF_SCOPE_USE" in codes_of(g)


def test_gate01_fail_scope_unverified():
    g = fg.gate_01_scope_identity(GOOD_REQUEST, {**GOOD_STATUS, "scope_unverified": True})
    assert not g["passed"] and "FG01_SCOPE_UNVERIFIED" in codes_of(g)


def test_gate01_fail_identity_incomplete():
    g = fg.gate_01_scope_identity({**GOOD_REQUEST, "area_sqm": None}, GOOD_STATUS)
    assert not g["passed"] and "FG01_IDENTITY_INCOMPLETE" in codes_of(g)
    g2 = fg.gate_01_scope_identity({**GOOD_REQUEST, "community_source_id": ""}, GOOD_STATUS)
    assert not g2["passed"] and "FG01_IDENTITY_INCOMPLETE" in codes_of(g2)


def test_gate01_fail_status_out_of_scope_list():
    g = fg.gate_01_scope_identity(GOOD_REQUEST,
                                  {**GOOD_STATUS, "out_of_scope": ["out_of_scope_district"]})
    assert not g["passed"] and "FG01_OUT_OF_SCOPE" in codes_of(g)


# ---------------------------------------------------------------- GATE-02 基础域值

def test_gate02_pass_no_error_issues():
    g = fg.gate_02_hard_domain([], [])
    assert g["passed"] and codes_of(g) == []


def test_gate02_pass_limited_not_error():
    issues = [{"field": "total_floors", "severity": "limited",
               "code": "out_of_domain_total_floors", "message": "按未知处理"}]
    g = fg.gate_02_hard_domain(issues, [])
    assert g["passed"]


def test_gate02_fail_hard_domain_area():
    issues = [{"field": "area_sqm", "severity": "error",
               "code": "out_of_domain_area", "message": "面积超域"}]
    g = fg.gate_02_hard_domain(issues, [])
    assert not g["passed"] and "FG02_HARD_DOMAIN_VIOLATION" in codes_of(g)
    assert g["detail"]["error_codes"] == ["out_of_domain_area"]


def test_gate02_fail_reject_reasons():
    rr = [{"field": "valuation_date", "severity": "error",
           "code": "valuation_date_in_future", "message": "估值时点在未来"}]
    g = fg.gate_02_hard_domain([], rr)
    assert not g["passed"] and "FG02_HARD_DOMAIN_VIOLATION" in codes_of(g)


# ---------------------------------------------------------------- GATE-03 适用分支

def test_gate03_pass_normal_branch():
    assert fg.gate_03_branch_applicable("normal")["passed"]


def test_gate03_pass_adopted_missing_rule_branch():
    assert fg.gate_03_branch_applicable("degraded_total_floors")["passed"]


def test_gate03_fail_unknown_state():
    g = fg.gate_03_branch_applicable("degraded_something_else")
    assert not g["passed"] and "FG03_BRANCH_NOT_ADOPTED" in codes_of(g)


def test_gate03_fail_none_state():
    g = fg.gate_03_branch_applicable(None)
    assert not g["passed"] and "FG03_BRANCH_NOT_ADOPTED" in codes_of(g)


# ---------------------------------------------------------------- GATE-04 小区已知与 B0 层

def test_gate04_pass_known_community_layer():
    g = fg.gate_04_community_known(True, "community")
    assert g["passed"] and codes_of(g) == []


def test_gate04_fail_unknown_community():
    g = fg.gate_04_community_known(False, "community")
    assert not g["passed"] and "FG04_COMMUNITY_UNKNOWN" in codes_of(g)


def test_gate04_fail_non_community_b0_layer():
    g = fg.gate_04_community_known(True, "block")
    assert not g["passed"] and "FG04_B0_LAYER_NOT_COMMUNITY" in codes_of(g)


# ---------------------------------------------------------------- GATE-05 小区支持案例数

def test_gate05_pass_at_threshold():
    g = fg.gate_05_community_support(5)
    assert g["passed"] and g["detail"] == {"support_case_count": 5, "min_cases": 5}


def test_gate05_pass_above_threshold():
    assert fg.gate_05_community_support(30)["passed"]


def test_gate05_fail_insufficient():
    g = fg.gate_05_community_support(3)
    assert not g["passed"] and "FG05_SUPPORT_INSUFFICIENT" in codes_of(g)


def test_gate05_fail_unknown():
    g = fg.gate_05_community_support(None)
    assert not g["passed"] and "FG05_SUPPORT_UNKNOWN" in codes_of(g)


# ---------------------------------------------------------------- GATE-06 板块解析

def test_gate06_pass_request_source():
    assert fg.gate_06_block_resolved("request", False, False)["passed"]


def test_gate06_pass_derived_and_translated():
    assert fg.gate_06_block_resolved("derived", False, False)["passed"]
    assert fg.gate_06_block_resolved("translated", False, False)["passed"]


def test_gate06_fail_unresolved():
    g = fg.gate_06_block_resolved("none", False, False)
    assert not g["passed"] and "FG06_BLOCK_UNRESOLVED" in codes_of(g)


def test_gate06_fail_mismatch():
    g = fg.gate_06_block_resolved("derived", True, False)
    assert not g["passed"] and "FG06_BLOCK_MISMATCH" in codes_of(g)


def test_gate06_fail_ambiguous():
    g = fg.gate_06_block_resolved("translated", False, True)
    assert not g["passed"] and "FG06_BLOCK_AMBIGUOUS" in codes_of(g)


# ---------------------------------------------------------------- GATE-07 M1-B0 分歧

def test_gate07_pass_within_threshold():
    g = fg.gate_07_m1_b0_divergence(20000.0, 21000.0)
    assert g["passed"] and abs(g["detail"]["ratio"] - 1000.0 / 21000.0) < 1e-12


def test_gate07_pass_exact_threshold():
    g = fg.gate_07_m1_b0_divergence(11500.0, 10000.0)
    assert g["passed"] and g["detail"]["ratio"] == 0.15


def test_gate07_fail_above_threshold():
    g = fg.gate_07_m1_b0_divergence(11800.0, 10000.0)
    assert not g["passed"] and "FG07_DIVERGENCE_ABOVE_THRESHOLD" in codes_of(g)


def test_gate07_fail_missing_price():
    g = fg.gate_07_m1_b0_divergence(None, 20000.0)
    assert not g["passed"] and "FG07_PRICE_MISSING" in codes_of(g)


def test_gate07_fail_b0_zero():
    g = fg.gate_07_m1_b0_divergence(20000.0, 0.0)
    assert not g["passed"] and "FG07_B0_ZERO" in codes_of(g)


# ---------------------------------------------------------------- GATE-08 A1 冲突

def test_gate08_pass_available_no_conflict():
    g = fg.gate_08_a1_conflict(False, "ok")
    assert g["passed"] and g["notes"] == []


def test_gate08_pass_unavailable_with_note():
    g = fg.gate_08_a1_conflict(False, "no_cases")
    assert g["passed"] and "FG08_NOTE_A1_UNAVAILABLE" in g["notes"]
    assert codes_of(g) == []


def test_gate08_fail_conflict():
    g = fg.gate_08_a1_conflict(True, "ok")
    assert not g["passed"] and "FG08_A1_CONFLICT" in codes_of(g)


def test_gate08_fail_conflict_even_when_unavailable():
    g = fg.gate_08_a1_conflict(True, "ineligible_attribute_incomplete")
    assert not g["passed"] and "FG08_A1_CONFLICT" in codes_of(g)


# ---------------------------------------------------------------- GATE-09 版本与区间

def test_gate09_pass_full():
    g = fg.gate_09_version_interval({"ok": True, "bundle_id": "x"}, GOOD_INTERVAL)
    assert g["passed"] and codes_of(g) == []


def test_gate09_fail_release_missing():
    g = fg.gate_09_version_interval(None, GOOD_INTERVAL)
    assert not g["passed"] and "FG09_RELEASE_CHECK_MISSING" in codes_of(g)


def test_gate09_fail_release_not_ok():
    g = fg.gate_09_version_interval({"ok": False, "reason": "s7 未确认"}, GOOD_INTERVAL)
    assert not g["passed"] and "FG09_RELEASE_CHECK_FAILED" in codes_of(g)


def test_gate09_fail_interval_empty():
    iv = {**GOOD_INTERVAL, "nominal_80": {"low": None, "high": None}}
    g = fg.gate_09_version_interval({"ok": True}, iv)
    assert not g["passed"] and "FG09_INTERVAL_EMPTY" in codes_of(g)


def test_gate09_fail_layer_untraceable():
    iv = {**GOOD_INTERVAL, "source_layer": None, "source_layer_n": None}
    g = fg.gate_09_version_interval({"ok": True}, iv)
    assert not g["passed"] and "FG09_LAYER_UNTRACEABLE" in codes_of(g)


def test_gate09_fail_interval_inverted():
    iv = {**GOOD_INTERVAL, "nominal_90": {"low": 25500.0, "high": 16500.0}}
    g = fg.gate_09_version_interval({"ok": True}, iv)
    assert not g["passed"] and "FG09_INTERVAL_EMPTY" in codes_of(g)


# ---------------------------------------------------------------- 总入口聚合

def _good_result() -> dict:
    return {
        "request_id": "FG-TEST-GOOD",
        "request": {**GOOD_REQUEST, "floor_bucket": "中楼层"},
        "status": {**GOOD_STATUS, "degradation_state": "normal",
                   "known_community": True, "cold_start_community": False,
                   "a1_m1_conflict": False, "block_source": "request",
                   "block_mismatch": False, "block_ambiguous": False,
                   "reject": False, "reject_reasons": []},
        "point": {"m1_pred_unit_price": 20000.0, "m1_pred_total_price": 1790000.0,
                  "area_sqm": 89.5},
        "interval": GOOD_INTERVAL,
        "support": {"b0_level": "community", "b0_pred": 21000.0, "b0_window_n": 42,
                    "a1_status": "ok", "a1_n_cases": 12},
        "issues": [],
    }


def test_evaluate_pass_all_nine():
    out = fg.evaluate(_good_result(), support_case_count=10,
                      release_check={"ok": True, "bundle_id": "x"})
    assert out["eligible"], out
    assert out["failed_gates"] == [] and out["reason_codes"] == []
    assert [g["gate"] for g in out["gates"]] == [f"GATE-0{i}" for i in range(1, 10)]
    assert all(g["passed"] for g in out["gates"])


def test_evaluate_fail_single_gate_flags_reason():
    out = fg.evaluate(_good_result(), support_case_count=3,
                      release_check={"ok": True})
    assert not out["eligible"]
    assert out["failed_gates"] == ["GATE-05"]
    assert out["reason_codes"] == ["FG05_SUPPORT_INSUFFICIENT"]


def test_evaluate_default_unpublished_fails_gate09():
    out = fg.evaluate(_good_result(), support_case_count=10, release_check=None)
    assert not out["eligible"]
    assert "GATE-09" in out["failed_gates"]
    assert "FG09_RELEASE_CHECK_MISSING" in out["reason_codes"]


def test_evaluate_degraded_branch_can_pass_gates():
    res = _good_result()
    res["status"]["degradation_state"] = "degraded_total_floors"
    out = fg.evaluate(res, support_case_count=10, release_check={"ok": True})
    assert out["eligible"]


def test_evaluate_multiple_failures_accumulate():
    res = _good_result()
    res["status"]["known_community"] = False
    res["status"]["block_mismatch"] = True
    out = fg.evaluate(res, support_case_count=2, release_check=None)
    assert not out["eligible"]
    assert set(out["failed_gates"]) == {"GATE-04", "GATE-05", "GATE-06", "GATE-09"}
    assert "FG04_COMMUNITY_UNKNOWN" in out["reason_codes"]
    assert "FG06_BLOCK_MISMATCH" in out["reason_codes"]


def test_structure_report_nine_gates():
    rep = fg.structure_report()
    assert len(rep["nine_gates"]) == 9
    assert rep["formal_gate_version"] == fg.FORMAL_GATE_VERSION


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failed += 1
                print(f"FAIL {name}: {exc}")
    print(f"total_failed={failed}")
    sys.exit(1 if failed else 0)
