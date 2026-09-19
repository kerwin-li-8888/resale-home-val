# -*- coding: utf-8 -*-
"""release_record——发布记录 release-record.json 的 schema、加载与运行时校验（纯标准库）。

行为规格（specs/phase2-formal-release「替代效果验收与发布生效」Requirement；
design D4；审1 F3；tasks 2.2）：

- 发布记录为数据区运行时文件（默认 <data>/phase2/ops-state/release-record.json），
  **默认不存在＝未发布**——工程只实现 schema、写入/校验函数与反例测试，
  不创建真实已发布记录；
- 字段＝判定 B 结论与报告指纹、审2 报告指纹、S7 用户确认记录、允许分支清单、
  时效期限（T−C/T−L 实际值与失效日）、发布组合全件指纹（九件，formal_binding）；
- 运行时逐请求校验：记录缺失 / 判定 B 未过 / 无 S7 确认 / 组合指纹错配 /
  分支未列 / 超期 / 记录损坏 / schema 不符——任一命中即不出正式价（fail-closed）；
- 校验分两层：全局层（不依赖请求字段：缺失/损坏/schema/判定B/S7/组合指纹）与
  请求层（branch / valuation_date 传入时追加分支与超期检查）。
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from . import formal_binding as fb

SCHEMA_VERSION = "phase2-release-record-v1"
RELEASE_RECORD_FILENAME = "release-record.json"
CONCLUSION_PASS = "PASS"

# F4（RV-ECR-VERIFY-01）：时效运营上限＝合同 DEMO-RELEASE-001 §6.1——
# T−C≤90 天（资产截止到失效日）、T−L≤180 天（估值时点到小区最新案例）；登记超上限即拒绝。
MAX_T_MINUS_C_DAYS = 90
MAX_T_MINUS_L_DAYS = 180

_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")

RC_RECORD_MISSING = "RR_RECORD_MISSING"
RC_RECORD_CORRUPT = "RR_RECORD_CORRUPT"
RC_SCHEMA_INVALID = "RR_SCHEMA_INVALID"
RC_VERDICT_B_NOT_PASSED = "RR_VERDICT_B_NOT_PASSED"
RC_S7_NOT_CONFIRMED = "RR_S7_NOT_CONFIRMED"
RC_COMBINATION_MISMATCH = "RR_COMBINATION_MISMATCH"
RC_BRANCH_NOT_ALLOWED = "RR_BRANCH_NOT_ALLOWED"
RC_EXPIRED = "RR_EXPIRED"

REQUIRED_TOP_FIELDS = ("schema_version", "released_at", "released_by", "verdict_b",
                       "review2", "s7_user_confirmation", "allowed_branches",
                       "validity", "combination_fingerprint")


class ReleaseRecordError(ValueError):
    """发布记录文件损坏/不可解析（fail-closed：调用方按不出正式价处理）。"""


def default_state_dir() -> Path:
    return Path(__file__).resolve().parents[4] / "03-估值引擎" / "data" / "phase2" / "ops-state"


def release_record_path(state_dir: Path | str | None = None) -> Path:
    d = Path(state_dir) if state_dir else default_state_dir()
    return d / RELEASE_RECORD_FILENAME


def build_release_record(*, released_at: str, released_by: str,
                         verdict_b_conclusion: str, verdict_b_report_path: str,
                         verdict_b_report_sha256: str, review2_report_path: str,
                         review2_report_sha256: str, s7_confirmed: bool,
                         s7_confirmed_by: str, s7_confirmed_at: str,
                         allowed_branches: list[str],
                         market_materials_cutoff: str, t_minus_c_days: int,
                         t_minus_l_days: int, combination_fingerprint: dict,
                         expires_on: str | None = None,
                         resumed_suspensions: list[str] | None = None,
                         note: str | None = None) -> dict:
    """构造一条完整发布记录（expires_on 缺省＝市场材料截止＋T−C 天）。"""
    if expires_on is None:
        cutoff = date.fromisoformat(market_materials_cutoff)
        expires_on = (cutoff + timedelta(days=int(t_minus_c_days))).isoformat()
    return {
        "schema_version": SCHEMA_VERSION,
        "released_at": released_at, "released_by": released_by,
        "verdict_b": {"conclusion": verdict_b_conclusion,
                      "report_path": verdict_b_report_path,
                      "report_sha256": verdict_b_report_sha256},
        "review2": {"report_path": review2_report_path,
                    "report_sha256": review2_report_sha256},
        "s7_user_confirmation": {"confirmed": s7_confirmed,
                                 "confirmed_by": s7_confirmed_by,
                                 "confirmed_at": s7_confirmed_at},
        "allowed_branches": list(allowed_branches),
        "validity": {"market_materials_cutoff": market_materials_cutoff,
                     "t_minus_c_days": t_minus_c_days,
                     "t_minus_l_days": t_minus_l_days,
                     "expires_on": expires_on},
        "combination_fingerprint": combination_fingerprint,
        "resumed_suspensions": list(resumed_suspensions or []),
        "note": note,
    }


def _is_nonempty_str(v) -> bool:
    return isinstance(v, str) and v.strip() != ""


def _is_sha256(v) -> bool:
    return isinstance(v, str) and bool(_SHA256_RE.match(v))


def _is_iso_date(v) -> bool:
    if not isinstance(v, str):
        return False
    try:
        date.fromisoformat(v)
        return True
    except ValueError:
        return False


def _is_iso_ts(v) -> bool:
    if not isinstance(v, str):
        return False
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _is_day_count(v) -> bool:
    """天数计数：必须 int 且非 bool（Python 中 bool 是 int 子类，须显式排除）。"""
    return isinstance(v, int) and not isinstance(v, bool)


def validate_schema(record: dict | None) -> list[str]:
    """schema 完整性检查（F3/F4 强化：键存在＋值非空＋类型＋格式＋时间关系）。

    - 审核证据（verdict_b/review2）：报告路径非空、报告 SHA-256 须为 64 位十六进制；
      空串占位、空白、无效摘要、错误字段类型一律拒绝（空证据不能冒充已审）；
    - S7 确认凭据：confirmed 必须布尔、confirmed_by 非空、confirmed_at 须为可解析
      ISO-8601 时间戳（可追溯的确认主体与时点）；
    - 时效 validity（F4）：实际资产截止与 expires_on 成套校验——expires_on 必须
      ＝ market_materials_cutoff ＋ t_minus_c_days（人工延后即矛盾失效日，拒绝）；
      t_minus_c_days/t_minus_l_days 须为正整数且不超过运营上限（90/180）；
      缺值/类型错误/负数/超上限/矛盾失效日均拒绝。
    返回问题清单（空＝完整）。
    """
    if not isinstance(record, dict):
        return ["record_not_object"]
    issues = [f"missing:{k}" for k in REQUIRED_TOP_FIELDS if k not in record]
    if record.get("schema_version") != SCHEMA_VERSION:
        issues.append(f"schema_version_unexpected:{record.get('schema_version')!r}")
    if not _is_nonempty_str(record.get("released_at")):
        issues.append("released_at_blank_or_not_string")
    if not _is_nonempty_str(record.get("released_by")):
        issues.append("released_by_blank_or_not_string")
    vb = record.get("verdict_b")
    if not isinstance(vb, dict) or not {"conclusion", "report_path", "report_sha256"} <= set(vb):
        issues.append("verdict_b_fields_incomplete")
    else:
        if not _is_nonempty_str(vb.get("conclusion")):
            issues.append("verdict_b_conclusion_blank")
        if not _is_nonempty_str(vb.get("report_path")):
            issues.append("verdict_b_report_path_blank")
        if not _is_sha256(vb.get("report_sha256")):
            issues.append("verdict_b_report_sha256_invalid")
    r2 = record.get("review2")
    if not isinstance(r2, dict) or not {"report_path", "report_sha256"} <= set(r2):
        issues.append("review2_fields_incomplete")
    else:
        if not _is_nonempty_str(r2.get("report_path")):
            issues.append("review2_report_path_blank")
        if not _is_sha256(r2.get("report_sha256")):
            issues.append("review2_report_sha256_invalid")
    s7 = record.get("s7_user_confirmation")
    if not isinstance(s7, dict) or "confirmed" not in s7:
        issues.append("s7_fields_incomplete")
    else:
        if not isinstance(s7.get("confirmed"), bool):
            issues.append("s7_confirmed_not_bool")
        if not _is_nonempty_str(s7.get("confirmed_by")):
            issues.append("s7_confirmed_by_blank")
        if not _is_iso_ts(s7.get("confirmed_at")):
            issues.append("s7_confirmed_at_invalid_timestamp")
    ab = record.get("allowed_branches")
    if not isinstance(ab, list) or not all(
            isinstance(x, str) and x.strip() != "" for x in ab):
        issues.append("allowed_branches_not_string_list")
    va = record.get("validity")
    if not isinstance(va, dict) or not {"market_materials_cutoff", "t_minus_c_days",
                                        "t_minus_l_days", "expires_on"} <= set(va):
        issues.append("validity_fields_incomplete")
    else:
        cutoff_v, expires_v = va.get("market_materials_cutoff"), va.get("expires_on")
        if not _is_iso_date(cutoff_v):
            issues.append("validity_market_materials_cutoff_invalid_date")
        if not _is_iso_date(expires_v):
            issues.append("validity_expires_on_invalid_date")
        tc, tl = va.get("t_minus_c_days"), va.get("t_minus_l_days")
        if not _is_day_count(tc) or tc < 1:
            issues.append("validity_t_minus_c_days_not_positive_int")
        elif tc > MAX_T_MINUS_C_DAYS:
            issues.append("validity_t_minus_c_days_exceeds_operational_cap")
        if not _is_day_count(tl) or tl < 1:
            issues.append("validity_t_minus_l_days_not_positive_int")
        elif tl > MAX_T_MINUS_L_DAYS:
            issues.append("validity_t_minus_l_days_exceeds_operational_cap")
        if (_is_iso_date(cutoff_v) and _is_iso_date(expires_v)
                and _is_day_count(tc) and tc >= 1):
            expected = (date.fromisoformat(cutoff_v)
                        + timedelta(days=tc)).isoformat()
            if expires_v != expected:
                issues.append("validity_expires_on_inconsistent_with_cutoff_plus_t_minus_c")
    cf = record.get("combination_fingerprint")
    if not isinstance(cf, dict):
        issues.append("combination_fingerprint_not_object")
    else:
        comps = cf.get("components")
        if not isinstance(comps, dict) or sorted(comps) != sorted(fb.NINE_COMPONENTS):
            issues.append("combination_components_not_nine")
        if not cf.get("composition_id"):
            issues.append("composition_id_missing")
    return issues


def load_release_record(path: Path | str | None = None) -> dict | None:
    """加载发布记录：不存在 → None（＝未发布，默认态）；损坏 → ReleaseRecordError。"""
    p = Path(path) if path else release_record_path()
    if not p.exists():
        return None
    try:
        raw = p.read_text(encoding="utf-8-sig")
        record = json.loads(raw)
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ReleaseRecordError(f"{type(exc).__name__}: {exc}") from exc
    if not isinstance(record, dict):
        raise ReleaseRecordError("发布记录顶层必须是 JSON 对象")
    return record


def check_release(record: dict | None, combination_nine: dict | None, *,
                  branch: str | None = None,
                  valuation_date: date | None = None) -> dict:
    """运行时逐请求校验（fail-closed）：任一命中即不出正式价。

    branch / valuation_date 为 None 时跳过对应请求层检查（全局层先行）。
    返回 {"ok", "reason_codes", "detail"}；不抛业务异常（损坏由加载层抛）。
    """
    detail: dict = {}
    if record is None:
        return {"ok": False, "reason_codes": [RC_RECORD_MISSING],
                "detail": {"meaning": "发布记录不存在＝未发布（默认态），不出正式价"}}
    codes: list[str] = []
    schema_issues = validate_schema(record)
    if schema_issues:
        codes.append(RC_SCHEMA_INVALID)
        detail["schema_issues"] = schema_issues
    vb = record.get("verdict_b") or {}
    if vb.get("conclusion") != CONCLUSION_PASS:
        codes.append(RC_VERDICT_B_NOT_PASSED)
        detail["verdict_b_conclusion"] = vb.get("conclusion")
    s7 = record.get("s7_user_confirmation") or {}
    if s7.get("confirmed") is not True:
        codes.append(RC_S7_NOT_CONFIRMED)
        detail["s7_confirmed"] = s7.get("confirmed")
    cmp = fb.compare_combinations(record.get("combination_fingerprint"),
                                  combination_nine)
    if not cmp["ok"]:
        codes.append(RC_COMBINATION_MISMATCH)
        detail["combination_check"] = cmp
    if branch is not None:
        allowed = record.get("allowed_branches") or []
        if branch not in allowed:
            codes.append(RC_BRANCH_NOT_ALLOWED)
            detail["branch"] = branch
            detail["allowed_branches"] = allowed
    if valuation_date is not None:
        va = record.get("validity") or {}
        try:
            expires_on = date.fromisoformat(str(va.get("expires_on")))
        except (TypeError, ValueError):
            codes.append(RC_SCHEMA_INVALID)
            detail.setdefault("schema_issues", []).append("expires_on_unparseable")
        else:
            if valuation_date > expires_on:
                codes.append(RC_EXPIRED)
                detail["expires_on"] = expires_on.isoformat()
                detail["valuation_date"] = valuation_date.isoformat()
    return {"ok": not codes, "reason_codes": codes, "detail": detail}


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.release_record",
        "spec_basis": ("specs/phase2-formal-release「替代效果验收与发布生效」；"
                       "design D4；审1 F3；tasks 2.2"),
        "default": "release-record.json 默认不存在＝未发布；工程不创建真实已发布记录",
        "required_top_fields": list(REQUIRED_TOP_FIELDS),
        "runtime_reason_codes": [RC_RECORD_MISSING, RC_RECORD_CORRUPT, RC_SCHEMA_INVALID,
                                 RC_VERDICT_B_NOT_PASSED, RC_S7_NOT_CONFIRMED,
                                 RC_COMBINATION_MISMATCH, RC_BRANCH_NOT_ALLOWED, RC_EXPIRED],
        "fail_closed": "任一命中即不出正式价",
        "schema_hardening_rv_ecr_verify_01": {
            "F3": "审核证据报告路径/SHA-256 与 S7 确认主体/时点做非空＋格式校验（空占位拒绝）",
            "F4": "expires_on＝资产截止＋T−C 成套校验；T−C≤90、T−L≤180 运营上限；"
                  "缺值/类型错/负数/矛盾失效日拒绝",
        },
        "stdlib_only": True,
    }
