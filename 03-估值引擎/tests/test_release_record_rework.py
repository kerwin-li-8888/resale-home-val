# -*- coding: utf-8 -*-
"""审2 RV-ECR-VERIFY-01 返工反例：F3 空发布审核证据拒绝＋F4 时效上限成套校验。

覆盖审2 §5 修复验收：
- F3：必需字段缺失、空白、无效摘要、错误字段类型均拒绝；审核证据与 S7 凭据完整性
  校验边界（真实正式入口反例由 candidate_ops 层引擎级测试另行覆盖）；
- F4：实际资产截止＋请求估值时点＋登记期限成套校验；缺值/类型错误/负数/超运营上限
  （T−C≤90、T−L≤180）/矛盾失效日均拒绝；较短获准期限实际生效；人工延后
  expires_on 反例。

运行（退出码 0 为过）：
uv run pytest tests/test_release_record_rework.py

被测模块设计为 stdlib-only（release_record/formal_binding 只依赖标准库）；
并入全量 pytest 套件后不再于文件顶部断言 numpy/polars 未被导入
（套件内其他测试可能先导入 numpy/polars，该隔离检查仅在单文件运行时有意义）。
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from gz_property_valuation.phase2 import formal_binding as fb  # noqa: E402
from gz_property_valuation.phase2 import release_record as rr  # noqa: E402

COMBO = fb.compose_nine(
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
        combination_fingerprint=COMBO)
    kwargs.update(over)
    return rr.build_release_record(**kwargs)


def mutate_validity(rec: dict, **over) -> dict:
    """在已构造记录上直接变异 validity（绕过构造器，等价于审2 反例的变异路径）。"""
    rec["validity"].update(over)
    return rec


def blank_evidence_release() -> dict:
    """审2 F3 反例原样构造：verdict_b 与 review2 路径/摘要置空串，S7 仅 confirmed=true。"""
    rec = make_release()
    rec["verdict_b"] = {"conclusion": "PASS", "report_path": "", "report_sha256": ""}
    rec["review2"] = {"report_path": "", "report_sha256": ""}
    rec["s7_user_confirmation"] = {"confirmed": True}
    return rec


def check_ok(rec) -> bool:
    return rr.check_release(rec, COMBO, branch="normal",
                            valuation_date=VAL_DATE)["ok"]


def issues_of(rec) -> set[str]:
    return set(rr.validate_schema(rec))


# ---------------------------------------------------------------- F3 空证据拒绝

def test_f3_blank_evidence_rejected():
    rec = blank_evidence_release()
    iss = issues_of(rec)
    assert "verdict_b_report_path_blank" in iss
    assert "verdict_b_report_sha256_invalid" in iss
    assert "review2_report_path_blank" in iss
    assert "review2_report_sha256_invalid" in iss
    assert "s7_confirmed_by_blank" in iss
    assert "s7_confirmed_at_invalid_timestamp" in iss
    rc = rr.check_release(rec, COMBO, branch="normal", valuation_date=VAL_DATE)
    assert not rc["ok"] and rr.RC_SCHEMA_INVALID in rc["reason_codes"]


def test_f3_missing_required_fields_rejected():
    for key in ("verdict_b", "review2", "s7_user_confirmation", "validity",
                "combination_fingerprint", "released_at", "released_by"):
        rec = make_release()
        rec.pop(key)
        assert not check_ok(rec), f"缺 {key} 必须拒绝"


def test_f3_whitespace_values_rejected():
    rec = make_release(verdict_b_report_path="   ",
                       review2_report_sha256="  ",
                       s7_confirmed_by="")
    iss = issues_of(rec)
    assert "verdict_b_report_path_blank" in iss
    assert "review2_report_sha256_invalid" in iss
    assert "s7_confirmed_by_blank" in iss
    assert not check_ok(rec)


def test_f3_invalid_digest_format_rejected():
    for bad in ("0" * 63, "0" * 65, "g" * 64, 12345, None):
        rec = make_release(verdict_b_report_sha256=bad)
        assert "verdict_b_report_sha256_invalid" in issues_of(rec)
        assert not check_ok(rec)


def test_f3_wrong_field_types_rejected():
    rec = make_release()
    rec["s7_user_confirmation"]["confirmed"] = "true"  # 字符串冒充布尔
    assert "s7_confirmed_not_bool" in issues_of(rec)
    assert not check_ok(rec)
    rec2 = mutate_validity(make_release(), t_minus_c_days="90")  # 字符串冒充整数
    assert "validity_t_minus_c_days_not_positive_int" in issues_of(rec2)
    assert not check_ok(rec2)
    rec3 = make_release(allowed_branches=[1, 2])
    assert "allowed_branches_not_string_list" in issues_of(rec3)
    rec4 = make_release(released_by=None)
    assert "released_by_blank_or_not_string" in issues_of(rec4)
    assert not check_ok(rec4)


def test_f3_s7_timestamp_boundaries():
    assert check_ok(make_release())
    for bad in ("", "not-a-ts", "2026-13-40T00:00:00Z", 20260914):
        assert "s7_confirmed_at_invalid_timestamp" in issues_of(
            make_release(s7_confirmed_at=bad)), bad
    assert "s7_confirmed_at_invalid_timestamp" not in issues_of(
        make_release(s7_confirmed_at="2026-09-14T12:00:00+00:00"))


# ---------------------------------------------------------------- F4 时效成套校验

def test_f4_contradictory_expires_on_rejected():
    """审2 F4 反例原样：cutoff=2026-01-01、T−C=1、expires=2030-01-01 → 矛盾失效日拒绝。"""
    rec = make_release(market_materials_cutoff="2026-01-01", t_minus_c_days=1,
                       expires_on="2030-01-01")
    iss = issues_of(rec)
    assert "validity_expires_on_inconsistent_with_cutoff_plus_t_minus_c" in iss
    rc = rr.check_release(rec, COMBO, branch="normal",
                          valuation_date=date(2026, 9, 13))
    assert not rc["ok"] and rr.RC_SCHEMA_INVALID in rc["reason_codes"]


def test_f4_manually_extended_expires_on_rejected():
    rec = make_release(expires_on="2026-12-31")  # 人工延后（应为 2026-10-18）
    assert "validity_expires_on_inconsistent_with_cutoff_plus_t_minus_c" in issues_of(rec)
    assert not check_ok(rec)


def test_f4_operational_caps_enforced():
    assert check_ok(make_release(t_minus_c_days=90, t_minus_l_days=180))
    over_c = make_release(t_minus_c_days=91, expires_on="2026-10-19")
    assert "validity_t_minus_c_days_exceeds_operational_cap" in issues_of(over_c)
    assert not check_ok(over_c)
    over_l = make_release(t_minus_l_days=181)
    assert "validity_t_minus_l_days_exceeds_operational_cap" in issues_of(over_l)
    assert not check_ok(over_l)


def test_f4_missing_negative_and_type_errors_rejected():
    cases = [
        dict(market_materials_cutoff=""),
        dict(market_materials_cutoff=None),
        dict(t_minus_c_days=0),
        dict(t_minus_c_days=-5),
        dict(t_minus_l_days=0),
        dict(t_minus_l_days=-1),
        dict(t_minus_c_days=90.5),
        dict(t_minus_l_days=True),
        dict(t_minus_c_days="90"),
        dict(expires_on="2026/10/18"),
        dict(expires_on=None),
    ]
    for over in cases:
        rec = mutate_validity(make_release(), **over)
        assert not check_ok(rec), f"{over} 必须拒绝"
        assert issues_of(rec), f"{over} 必须给出 schema 问题"


def test_f4_shorter_approval_period_actually_enforced():
    """较短获准期限（T−C=7 → 失效 2026-07-27）实际生效：期限边界外拒绝。"""
    rec = make_release(t_minus_c_days=7)
    assert rec["validity"]["expires_on"] == "2026-07-27"
    assert rr.check_release(rec, COMBO, branch="normal",
                            valuation_date=date(2026, 7, 27))["ok"]
    rc = rr.check_release(rec, COMBO, branch="normal",
                          valuation_date=date(2026, 7, 28))
    assert not rc["ok"] and rr.RC_EXPIRED in rc["reason_codes"]


def test_f4_valid_record_schema_clean():
    assert issues_of(make_release()) == set()
    assert check_ok(make_release())
