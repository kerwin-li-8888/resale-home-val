# -*- coding: utf-8 -*-
"""formal_gate——正式出价逐套资格门（合同 §6.1 九项）唯一共享实现。

行为规格（DEMO-RELEASE-001 V0.1 §6.1；specs/phase2-formal-release
「正式出价资格门」Requirement；design D4）：

- 评估脚本（阶段 2 存量评估）与生产正式入口**共同 import 本模块**，同源性由依赖
  结构保证（F4）：任何一方不得另写一份资格规则；
- 九项检查各自为**纯函数**：只依赖入参、不做 IO、不触网、无落盘副作用；
  本模块只依赖标准库（不 import numpy/polars），可被任意入口安全导入；
- 不通过时输出稳定**原因码**（``FG`` 前缀）与可读明细；任一门不通过即不得出正式价，
  正式价格字段为空，不静默改用 B0/A1 充当正式报价（合同 §6.1）。

九门与合同 §6.1 检查项对照（表格 8 行；"小区支持"行含三个可独立判定的子检查，
拆为 GATE-04 与 GATE-05 两门，合计九门）：

============================  ==========================================  ==========
门                            合同检查项（原文行）                        阈值来源
============================  ==========================================  ==========
GATE-01 scope_identity        人群与身份                                  合同值
GATE-02 hard_domain           基础域值（沿用既有硬域，不以缺失绕过）      合同值
GATE-03 branch_applicable     适用分支（命中已采用分支与缺失处理规则）    合同值
GATE-04 community_known       小区支持·模型已知＋B0 为 community 层       合同值
GATE-05 community_support     小区支持·截点前 365 天去重合格案例 ≥5 条    5 条＝新拟定
GATE-06 block_resolved        板块解析                                    合同值
GATE-07 m1_b0_divergence      M1 与 B0 分歧 abs(M1−B0)/B0≤15%             15%＝既有告警值
GATE-08 a1_conflict           A1（可用时另检原冲突规则）                  合同值
GATE-09 version_interval      版本与区间                                  合同值
============================  ==========================================  ==========

GATE-02 不重复实现域值数值（面积 (10,300] 等硬域仍以 candidate_request.validate 为
唯一判定点），本门消费其 issues/reject 结果做资格语义判定，避免两处数值漂移。
GATE-05 的"去重合格案例数"口径由调用方按截点前 365 天去重合格案例统计后传入，
本门只判阈值与缺失。
"""
from __future__ import annotations

DISTRICT_IN_SCOPE = "云溪区"
PROPERTY_USE_IN_SCOPE = "普通住宅"

M1_B0_DIVERGENCE_THRESHOLD = 0.15
MIN_SUPPORT_CASES_365D = 5
B0_LEVEL_REQUIRED = "community"

ADOPTED_DEGRADATION_STATES = frozenset({"normal", "degraded_total_floors"})

BLOCK_SOURCE_ACCEPTED = frozenset({"request", "derived", "translated"})

SCHEMA_VERSION = "phase2-formal-gate-v1"
FORMAL_GATE_VERSION = "formal-gate-v1"


def _gate(gate_id: str, name: str, passed: bool, reason_codes: list[str],
          detail: dict | None = None, notes: list[str] | None = None) -> dict:
    return {
        "gate": gate_id,
        "name": name,
        "passed": bool(passed),
        "reason_codes": list(reason_codes) if not passed else [],
        "notes": list(notes or []),
        "detail": detail or {},
    }


def gate_01_scope_identity(request: dict, status: dict) -> dict:
    """GATE-01 人群与身份：云溪区·普通住宅·外部小区 ID 与面积明确。"""
    codes: list[str] = []
    district = request.get("district")
    use = request.get("property_use")
    if district is not None and district != DISTRICT_IN_SCOPE:
        codes.append("FG01_OUT_OF_SCOPE_DISTRICT")
    if use is not None and use != PROPERTY_USE_IN_SCOPE:
        codes.append("FG01_OUT_OF_SCOPE_USE")
    if status.get("scope_unverified"):
        codes.append("FG01_SCOPE_UNVERIFIED")
    if status.get("out_of_scope"):
        codes.append("FG01_OUT_OF_SCOPE")
    area = request.get("area_sqm")
    try:
        area_ok = area is not None and float(area) > 0
    except (TypeError, ValueError):
        area_ok = False
    comm_id = request.get("community_source_id")
    if comm_id in (None, "") or not area_ok:
        codes.append("FG01_IDENTITY_INCOMPLETE")
    return _gate("GATE-01", "scope_identity", not codes, codes,
                 {"district": district, "property_use": use,
                  "community_source_id": comm_id, "area_sqm": area})


def gate_02_hard_domain(issues: list[dict] | None, reject_reasons: list[dict] | None) -> dict:
    """GATE-02 基础域值：无未消化 error 级校验（不以缺失或错误字段绕过校验）。"""
    errs = [i for i in list(issues or []) if i.get("severity") == "error"]
    errs += [i for i in list(reject_reasons or []) if i.get("severity") == "error"]
    codes = ["FG02_HARD_DOMAIN_VIOLATION"] if errs else []
    return _gate("GATE-02", "hard_domain", not errs, codes,
                 {"error_codes": sorted({str(i.get("code")) for i in errs})})


def gate_03_branch_applicable(degradation_state: str | None,
                              adopted: frozenset[str] = ADOPTED_DEGRADATION_STATES) -> dict:
    """GATE-03 适用分支：实际输入命中已获采用判定的分支与缺失处理规则。"""
    if degradation_state in adopted:
        return _gate("GATE-03", "branch_applicable", True, [],
                     {"degradation_state": degradation_state,
                      "adopted_states": sorted(adopted)})
    return _gate("GATE-03", "branch_applicable", False, ["FG03_BRANCH_NOT_ADOPTED"],
                 {"degradation_state": degradation_state,
                  "adopted_states": sorted(adopted)})


def gate_04_community_known(known_community, b0_level) -> dict:
    """GATE-04 小区支持（一）：小区为模型已知且 B0 为 community 层。

    "其他层首版不自动正式出价"落在本门：b0_level 非 community 即不通过。
    """
    codes: list[str] = []
    if not known_community:
        codes.append("FG04_COMMUNITY_UNKNOWN")
    if b0_level != B0_LEVEL_REQUIRED:
        codes.append("FG04_B0_LAYER_NOT_COMMUNITY")
    return _gate("GATE-04", "community_known", not codes, codes,
                 {"known_community": known_community, "b0_level": b0_level})


def gate_05_community_support(support_case_count, min_cases: int = MIN_SUPPORT_CASES_365D) -> dict:
    """GATE-05 小区支持（二）：截点前 365 天去重合格案例 ≥5 条（5 条为新拟定）。"""
    if support_case_count is None:
        return _gate("GATE-05", "community_support", False, ["FG05_SUPPORT_UNKNOWN"],
                     {"support_case_count": None, "min_cases": min_cases})
    try:
        n = int(support_case_count)
    except (TypeError, ValueError):
        return _gate("GATE-05", "community_support", False, ["FG05_SUPPORT_UNKNOWN"],
                     {"support_case_count": support_case_count, "min_cases": min_cases})
    if n >= min_cases:
        return _gate("GATE-05", "community_support", True, [],
                     {"support_case_count": n, "min_cases": min_cases})
    return _gate("GATE-05", "community_support", False, ["FG05_SUPPORT_INSUFFICIENT"],
                 {"support_case_count": n, "min_cases": min_cases})


def gate_06_block_resolved(block_source, block_mismatch, block_ambiguous) -> dict:
    """GATE-06 板块解析：request/derived/有依据 translated 均可；未解除 mismatch/ambiguous 不出。"""
    codes: list[str] = []
    if block_source not in BLOCK_SOURCE_ACCEPTED:
        codes.append("FG06_BLOCK_UNRESOLVED")
    if block_mismatch:
        codes.append("FG06_BLOCK_MISMATCH")
    if block_ambiguous:
        codes.append("FG06_BLOCK_AMBIGUOUS")
    return _gate("GATE-06", "block_resolved", not codes, codes,
                 {"block_source": block_source, "block_mismatch": bool(block_mismatch),
                  "block_ambiguous": bool(block_ambiguous)})


def gate_07_m1_b0_divergence(m1, b0,
                             threshold: float = M1_B0_DIVERGENCE_THRESHOLD) -> dict:
    """GATE-07 M1 与 B0 分歧：abs(M1−B0)/B0≤15%（无论 A1 是否可用，硬资格）。"""
    if m1 is None or b0 is None:
        return _gate("GATE-07", "m1_b0_divergence", False, ["FG07_PRICE_MISSING"],
                     {"m1": m1, "b0": b0, "threshold": threshold})
    b0f = float(b0)
    if b0f == 0.0:
        return _gate("GATE-07", "m1_b0_divergence", False, ["FG07_B0_ZERO"],
                     {"m1": m1, "b0": b0, "threshold": threshold})
    ratio = abs(float(m1) - b0f) / b0f
    passed = ratio <= threshold
    return _gate("GATE-07", "m1_b0_divergence", passed,
                 [] if passed else ["FG07_DIVERGENCE_ABOVE_THRESHOLD"],
                 {"m1": m1, "b0": b0, "ratio": ratio, "threshold": threshold})


def gate_08_a1_conflict(a1_m1_conflict, a1_status) -> dict:
    """GATE-08 A1：可用时另检原冲突规则；不可用时如实说明（不豁免也不使合格 M1 失效）。"""
    notes: list[str] = []
    codes: list[str] = []
    if a1_m1_conflict:
        codes.append("FG08_A1_CONFLICT")
    if a1_status != "ok":
        notes.append("FG08_NOTE_A1_UNAVAILABLE")
    return _gate("GATE-08", "a1_conflict", not codes, codes,
                 {"a1_m1_conflict": bool(a1_m1_conflict), "a1_status": a1_status},
                 notes=notes)


def gate_09_version_interval(release_check: dict | None, interval: dict | None) -> dict:
    """GATE-09 版本与区间：发布组合核对通过；区间非空、来源层与样本数可查。"""
    codes: list[str] = []
    if release_check is None:
        codes.append("FG09_RELEASE_CHECK_MISSING")
    elif not release_check.get("ok"):
        codes.append("FG09_RELEASE_CHECK_FAILED")
    iv = interval or {}
    lo80, hi80 = (iv.get("nominal_80") or {}).get("low"), (iv.get("nominal_80") or {}).get("high")
    lo90, hi90 = (iv.get("nominal_90") or {}).get("low"), (iv.get("nominal_90") or {}).get("high")
    interval_ok = all(v is not None for v in (lo80, hi80, lo90, hi90)) and lo80 < hi80 and lo90 < hi90
    if not interval_ok:
        codes.append("FG09_INTERVAL_EMPTY")
    layer_ok = bool(iv.get("source_layer")) and isinstance(iv.get("source_layer_n"), int) \
        and iv.get("source_layer_n", 0) > 0
    if not layer_ok:
        codes.append("FG09_LAYER_UNTRACEABLE")
    return _gate("GATE-09", "version_interval", not codes, codes,
                 {"release_check": release_check,
                  "interval": {"nominal_80": {"low": lo80, "high": hi80},
                               "nominal_90": {"low": lo90, "high": hi90},
                               "source_layer": iv.get("source_layer"),
                               "source_layer_n": iv.get("source_layer_n")}})


def evaluate(result: dict, support_case_count=None, release_check: dict | None = None,
             adopted_degradation_states: frozenset[str] = ADOPTED_DEGRADATION_STATES,
             m1_b0_threshold: float = M1_B0_DIVERGENCE_THRESHOLD,
             min_support_cases: int = MIN_SUPPORT_CASES_365D) -> dict:
    """逐套资格门总入口：从候选估值结果（candidate_ops.estimate 输出）提取字段，
    依序执行九门；任一门不通过即 eligible=False（该套不得出正式价）。

    - ``support_case_count``：调用方按"截点前 365 天去重合格案例"口径统计后传入；
    - ``release_check``：发布组合核对结果（release-record 运行时校验输出），
      缺失（None）＝未发布/未核对 → GATE-09 不通过（默认不存在＝未发布）。
    """
    request = result.get("request") or {}
    status = result.get("status") or {}
    point = result.get("point") or {}
    support = result.get("support") or {}
    interval = result.get("interval")
    gates = [
        gate_01_scope_identity(request, status),
        gate_02_hard_domain(result.get("issues"), status.get("reject_reasons")),
        gate_03_branch_applicable(status.get("degradation_state"),
                                  adopted=adopted_degradation_states),
        gate_04_community_known(status.get("known_community"),
                                support.get("b0_level")),
        gate_05_community_support(support_case_count, min_cases=min_support_cases),
        gate_06_block_resolved(status.get("block_source"), status.get("block_mismatch"),
                               status.get("block_ambiguous")),
        gate_07_m1_b0_divergence(point.get("m1_pred_unit_price"), support.get("b0_pred"),
                                 threshold=m1_b0_threshold),
        gate_08_a1_conflict(status.get("a1_m1_conflict"), support.get("a1_status")),
        gate_09_version_interval(release_check, interval),
    ]
    failed = [g["gate"] for g in gates if not g["passed"]]
    reason_codes = [c for g in gates for c in g["reason_codes"]]
    notes = [n for g in gates for n in g["notes"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "formal_gate_version": FORMAL_GATE_VERSION,
        "request_id": result.get("request_id"),
        "eligible": not failed,
        "gates": gates,
        "failed_gates": failed,
        "reason_codes": reason_codes,
        "notes": notes,
    }


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.formal_gate",
        "spec_basis": ("DEMO-RELEASE-001 V0.1 §6.1；"
                       "specs/phase2-formal-release「正式出价资格门」；design D4（F4）"),
        "role": "唯一资格实现：评估脚本与生产正式入口共同 import，不另写第二份规则",
        "nine_gates": [
            "GATE-01 scope_identity", "GATE-02 hard_domain",
            "GATE-03 branch_applicable", "GATE-04 community_known",
            "GATE-05 community_support", "GATE-06 block_resolved",
            "GATE-07 m1_b0_divergence", "GATE-08 a1_conflict",
            "GATE-09 version_interval",
        ],
        "gate_table_mapping": "合同 §6.1 表 8 行；『小区支持』行拆为 GATE-04/GATE-05，合计九门",
        "thresholds": {
            "m1_b0_divergence": M1_B0_DIVERGENCE_THRESHOLD,
            "min_support_cases_365d": MIN_SUPPORT_CASES_365D,
            "b0_level_required": B0_LEVEL_REQUIRED,
        },
        "reason_code_prefix": "FG",
        "pure_functions": True,
        "stdlib_only": True,
        "no_io": True,
        "formal_gate_version": FORMAL_GATE_VERSION,
    }
