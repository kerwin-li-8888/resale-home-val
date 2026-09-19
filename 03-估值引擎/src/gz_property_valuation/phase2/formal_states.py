# -*- coding: utf-8 -*-
"""formal_states——正式模式四态状态机与响应信封（纯标准库，无 IO）。

行为规格（specs/phase2-formal-release「正式输出语义」Requirement；design D4/D5；
tasks 2.2/2.6）：

- 四态＝priced（正常出价）/ ineligible（资格不通过）/ rejected（输入拒绝）/
  version_disabled（版本停用：停止开关、分支停用、发布记录缺失/未生效/
  指纹错配/分支未列/超期）；四态互斥；
- **正式字段仅 priced 非空**：formal_price 与 formal_report 在其余三态一律为
  None，且信封不携带候选诊断价格字段（资格门明细剥离 detail 值，仅保留
  门 ID/通过位/原因码）——正式价格不得泄露到资格拒绝/版本错误/超期/停止状态；
- decide() 为纯函数优先级仲裁：停止开关 → 发布记录 → 输入拒绝 → 资格门 →
  priced（停止开关优先于一切，含未解析请求）。
"""
from __future__ import annotations

from . import formal_gate as fg

STATE_PRICED = "priced"
STATE_INELIGIBLE = "ineligible"
STATE_REJECTED = "rejected"
STATE_VERSION_DISABLED = "version_disabled"
STATES = (STATE_PRICED, STATE_INELIGIBLE, STATE_REJECTED, STATE_VERSION_DISABLED)

FORMAL_SCHEMA_VERSION = "phase2-formal-estimate-v1"
FORMAL_REPORT_FIELDS = ("unit_price", "total_price", "valuation_date",
                        "actual_data_cutoff", "interval_and_width",
                        "main_market_basis", "main_limits", "applicable_scope",
                        "formal_adoption_basis", "release_version")
PRICE_FIELD_RULE = "正式价格字段仅 priced 非空；其余三态不携带任何价格字段"

RC_STOP_GATE = "SW_STOPPED"
RC_BRANCH_SUSPENDED = "SW_BRANCH_SUSPENDED"


def decide(*, stop_stopped: bool, release_ok: bool, input_rejected: bool,
           gate_eligible: bool | None) -> str:
    """四态仲裁（纯函数）：停止 → 发布 → 拒绝 → 资格 → priced。

    gate_eligible=None 表示未进入资格门（如输入拒绝/版本停用早退）。
    """
    if stop_stopped:
        return STATE_VERSION_DISABLED
    if not release_ok:
        return STATE_VERSION_DISABLED
    if input_rejected:
        return STATE_REJECTED
    if gate_eligible is not True:
        return STATE_INELIGIBLE
    return STATE_PRICED


def strip_gate_details(gates: list[dict]) -> list[dict]:
    """资格门明细去值化：保留 gate/name/passed/reason_codes/notes，剥离 detail
    （detail 内含 M1/B0 等诊断数值，不得进入非 priced 响应）。"""
    return [{"gate": g.get("gate"), "name": g.get("name"),
             "passed": g.get("passed"), "reason_codes": list(g.get("reason_codes") or []),
             "notes": list(g.get("notes") or [])} for g in gates or []]


def eligibility_summary(gate: dict | None) -> dict | None:
    """资格门结果摘要（无价格数值）：供 ineligible/priced 信封登记资格结果。"""
    if gate is None:
        return None
    return {"eligible": bool(gate.get("eligible")),
            "failed_gates": list(gate.get("failed_gates") or []),
            "reason_codes": list(gate.get("reason_codes") or []),
            "notes": list(gate.get("notes") or []),
            "gates": strip_gate_details(gate.get("gates") or []),
            "formal_gate_version": gate.get("formal_gate_version") or fg.FORMAL_GATE_VERSION}


def build_envelope(*, request_id, state: str, as_of=None, anchor=None, branch=None,
                   reason_codes=None, release_check=None, combination=None,
                   eligibility=None, timing_registration=None, limits=None,
                   issues=None, formal_price=None, formal_report=None) -> dict:
    """组装正式响应信封；非 priced 态强制清空正式字段（结构性防泄露）。"""
    if state not in STATES:
        raise ValueError(f"未知正式四态：{state!r}")
    if state != STATE_PRICED:
        if formal_price is not None or formal_report is not None:
            raise ValueError("非 priced 态不得携带正式价格/正式报告字段")
        formal_price = None
        formal_report = None
    elif formal_price is None or formal_report is None:
        raise ValueError("priced 态必须携带 formal_price 与 formal_report")
    return {
        "schema_version": FORMAL_SCHEMA_VERSION,
        "mode": "formal",
        "request_id": request_id,
        "anchor": anchor,
        "as_of": as_of,
        "branch": branch,
        "state": state,
        "reason_codes": list(reason_codes or []),
        "formal_price": formal_price,
        "formal_report": formal_report,
        "eligibility": eligibility,
        "release_check": release_check,
        "combination": combination,
        "timing_registration": timing_registration,
        "limits": list(limits or []),
        "issues": list(issues or []),
        "formal_gate_version": fg.FORMAL_GATE_VERSION,
        "price_field_rule": PRICE_FIELD_RULE,
    }


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.formal_states",
        "spec_basis": "specs/phase2-formal-release「正式输出语义」；design D4/D5；tasks 2.2/2.6",
        "states": list(STATES),
        "decide_priority": ["stop_switch", "release_record", "input_rejected",
                            "formal_gate", "priced"],
        "formal_report_fields": list(FORMAL_REPORT_FIELDS),
        "price_field_rule": PRICE_FIELD_RULE,
        "leak_guard": "非 priced 信封剥离资格门 detail 值并强制清空正式字段",
        "stdlib_only": True,
    }
