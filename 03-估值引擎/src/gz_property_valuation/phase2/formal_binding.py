# -*- coding: utf-8 -*-
"""formal_binding——完整组合九件指纹与正式记录追加式哈希链（纯标准库，无第三方依赖）。

行为规格（specs/phase2-formal-release「完整组合绑定」Requirement；design D4 任务 2.3）：

- 版本指纹覆盖并成套核对九件：五组件（模型/特征/市场资产/校准表/协调策略，
  取 candidate_ops.build_version_bundle 的组件摘要）＋推理代码＋正式资格规则版本
  ＋采用合同 SHA-256＋current-block-map 指纹；composition_id 为九件摘要的规范化
  JSON 再哈希；
- **映射或代码更新而 bundle_id（五组件）不变 → composition_id 仍改变**，被识别为
  不同组合并拒绝混用（审1 F3 / spec「九件成套校验」场景）；
- 正式记录 formal-records.jsonl 为**追加式**（标签只追加不倒改既有记录）＋**哈希链**
  （每条含前条哈希 prev_sha256 与本条摘要 record_sha256；首条 prev 为空串）；
- 本模块只依赖标准库；组合指纹的输入以摘要字符串传入（调用方负责从资产/文件计算），
  测试可用合成摘要构造，不触发重资产加载。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

FIVE_COMPONENTS = ("model", "feature", "market_asset", "calibration",
                   "coordination_policy")
EXTRA_COMPONENTS = ("inference_code", "formal_gate_rule", "adoption_contract",
                    "current_block_map")
# F6（RV-ECR-VERIFY-01）：新增 production_code 组件——执行正式放行/时效/分支停用/
# 输出/绑定的完整代码依赖（candidate_ops/release_record/stop_switch/formal_binding/
# formal_states/candidate_request 六文件聚合摘要，由 candidate_ops.production_code_digest
# 计算后传入）。历史别名 NINE_COMPONENTS 保留，实际为十件（RELEASE_COMPONENTS）。
RELEASE_COMPONENTS = FIVE_COMPONENTS + EXTRA_COMPONENTS + ("production_code",)
NINE_COMPONENTS = RELEASE_COMPONENTS

CHAIN_FIELD_PREV = "prev_sha256"
CHAIN_FIELD_SELF = "record_sha256"


class FormalChainError(RuntimeError):
    """正式记录哈希链损坏（F8：先验完整性后消费/追加/输出，损坏即拒绝）。"""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path | str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def digest_of(obj) -> str:
    return sha256_bytes(canonical_json(obj).encode("utf-8"))


# ---------------------------------------------------------------- 组合九件指纹

def compose_nine(five_digests: dict, *, inference_code_sha: str,
                 formal_gate_rule_sha: str, adoption_contract_sha: str,
                 current_block_map_sha: str, production_code_sha: str) -> dict:
    """五组件摘要（bundle_digests 输出）＋五件补充摘要 → 组合指纹（十件外层）。

    five_digests 键必须恰为 FIVE_COMPONENTS 五件；任一缺失/多余即 ValueError。
    production_code_sha 为必填：正式执行代码（candidate_ops/release_record/
    stop_switch/formal_binding/formal_states/candidate_request）聚合摘要——
    程序变化必须产生新组合并拒绝旧凭据（F6）。
    """
    missing = [k for k in FIVE_COMPONENTS if k not in five_digests]
    extra = [k for k in five_digests if k not in FIVE_COMPONENTS]
    if missing or extra:
        raise ValueError(f"五组件摘要键不符：缺失 {missing}、多余 {extra}")
    components = {
        "model": str(five_digests["model"]),
        "feature": str(five_digests["feature"]),
        "market_asset": str(five_digests["market_asset"]),
        "calibration": str(five_digests["calibration"]),
        "coordination_policy": str(five_digests["coordination_policy"]),
        "inference_code": str(inference_code_sha),
        "formal_gate_rule": str(formal_gate_rule_sha),
        "adoption_contract": str(adoption_contract_sha),
        "current_block_map": str(current_block_map_sha),
        "production_code": str(production_code_sha),
    }
    return {"components": components,
            "composition_id": digest_of(components)}


def compare_combinations(expected: dict, actual: dict) -> dict:
    """成套核对：逐件比对九件摘要；任一不符/缺失即不通过（不部分混用）。

    返回 {"ok", "composition_equal", "mismatches", "missing", "detail"}；
    mismatches 为值不符组件名清单（如仅映射更新 → ["current_block_map"]）。
    """
    exp = (expected or {}).get("components") or {}
    act = (actual or {}).get("components") or {}
    missing = [k for k in NINE_COMPONENTS if k not in exp or k not in act]
    mismatches = [k for k in NINE_COMPONENTS
                  if k in exp and k in act and exp[k] != act[k]]
    composition_equal = bool(
        (expected or {}).get("composition_id")
        and (actual or {}).get("composition_id")
        and expected["composition_id"] == actual["composition_id"])
    ok = composition_equal and not mismatches and not missing
    return {"ok": ok, "composition_equal": composition_equal,
            "mismatches": mismatches, "missing": missing,
            "detail": {"expected_composition_id": (expected or {}).get("composition_id"),
                       "actual_composition_id": (actual or {}).get("composition_id")}}


# ---------------------------------------------------------------- 追加式哈希链

def record_sha256(record: dict) -> str:
    """本条摘要：对除去 record_sha256 字段的规范化 JSON 计哈希。"""
    payload = {k: v for k, v in record.items() if k != CHAIN_FIELD_SELF}
    return digest_of(payload)


def load_records(path: Path | str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    rows = []
    for line in p.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def append_record(path: Path | str, record: dict) -> dict:
    """追加一条记录：自动接哈希链（prev_sha256＝现末条摘要）并写本条摘要。

    只在文件末尾追加一行，不改既有字节（追加式，标签不倒改）。
    F8：追加前先复验既有链完整性——损坏即抛 FormalChainError，拒绝追加
    （先验完整性后追加，防止在损坏链上继续堆叠记录）。
    """
    p = Path(path)
    if p.parent and str(p.parent):
        p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        chain = verify_chain(p)
        if not chain["ok"]:
            raise FormalChainError(
                f"record_chain_corrupt: n_errors={len(chain['errors'])} "
                f"first={chain['errors'][0] if chain['errors'] else None}")
    rows = load_records(p)
    prev = rows[-1][CHAIN_FIELD_SELF] if rows else ""
    rec = dict(record)
    rec[CHAIN_FIELD_PREV] = prev
    rec.pop(CHAIN_FIELD_SELF, None)
    rec[CHAIN_FIELD_SELF] = record_sha256(rec)
    with open(p, "a", encoding="utf-8", newline="\n") as f:
        f.write(canonical_json(rec) + "\n")
    return rec


def load_records_verified(path: Path | str) -> list[dict]:
    """先验链后返回记录（F8：先验完整性后消费）；损坏抛 FormalChainError。"""
    chain = verify_chain(path)
    if not chain["ok"]:
        raise FormalChainError(
            f"record_chain_corrupt: n_errors={len(chain['errors'])} "
            f"first={chain['errors'][0] if chain['errors'] else None}")
    return load_records(path)


def verify_chain(path: Path | str) -> dict:
    """全链复验：逐条核对 prev_sha256 连接与本条摘要；返回 {ok, n_records, errors}。"""
    rows = load_records(path)
    errors: list[dict] = []
    prev = ""
    for i, rec in enumerate(rows):
        if rec.get(CHAIN_FIELD_PREV) != prev:
            errors.append({"line": i + 1, "code": "CHAIN_BREAK",
                           "detail": {"expected_prev": prev,
                                      "got_prev": rec.get(CHAIN_FIELD_PREV)}})
        if rec.get(CHAIN_FIELD_SELF) != record_sha256(rec):
            errors.append({"line": i + 1, "code": "RECORD_DIGEST_MISMATCH",
                           "detail": {"stored": rec.get(CHAIN_FIELD_SELF),
                                      "recomputed": record_sha256(rec)}})
        prev = rec.get(CHAIN_FIELD_SELF) or ""
    return {"ok": not errors and bool(rows), "n_records": len(rows),
            "errors": errors,
            "note": "空文件 ok=False（无记录可验）；首条 prev 允许为空串"}


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.formal_binding",
        "spec_basis": "specs/phase2-formal-release「完整组合绑定」；design D4；tasks 2.3",
        "nine_components": list(NINE_COMPONENTS),
        "components_total": len(RELEASE_COMPONENTS),
        "five_components": list(FIVE_COMPONENTS),
        "production_code_component_rv_ecr_verify_01_F6": (
            "production_code＝正式执行代码聚合摘要（candidate_ops/release_record/"
            "stop_switch/formal_binding/formal_states/candidate_request 六文件）；"
            "程序变化 → composition_id 变化 → 旧凭据拒绝"),
        "bundle_id_unchanged_scenario": ("current-block-map 更新而五组件（bundle）不变 → "
                                         "composition_id 变化 → compare_combinations 拒绝混用"),
        "records_semantics": "formal-records.jsonl 追加式＋哈希链（prev_sha256/record_sha256）；标签只追加不倒改",
        "integrity_first_rv_ecr_verify_01_F8": (
            "append_record 追加前先验链；load_records_verified 先验后消费；"
            "损坏抛 FormalChainError（先验完整性后消费/追加/输出）"),
        "stdlib_only": True,
    }
