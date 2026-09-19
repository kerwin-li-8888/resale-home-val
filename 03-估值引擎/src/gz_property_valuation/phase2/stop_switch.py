# -*- coding: utf-8 -*-
"""stop_switch——整体停止开关与分支停用状态文件（运营层，与发布记录独立；纯标准库）。

行为规格（specs/phase2-formal-release「上线后监控、补证与停止」Requirement；
design D4/D6；tasks 2.6、2.4）：

- 停止开关状态文件 stop-switch.json：运营层随时可停，**与发布记录独立**
  （首启授权≠停止开关；授权是首次生效门槛，开关是随时切断手段）；
- **默认回退＝停止 105 正式出价（fail-closed）**：状态文件缺失、损坏或语义不明时
  一律按停止处理；显式写入 stopped=false 的状态文件是唯一"运行"态；
- 分支停用 branch-suspended.json：反馈闭环硬门明确失败时自动写入（含证据指针）；
  **恢复仅经发布记录更新**——发布记录的 resumed_suspensions 登记被恢复的
  suspension_id，未登记的一律视为仍在停用；
- 状态文件位于数据区运行时目录（默认 <data>/phase2/ops-state/），
  **默认不存在**；测试一律用临时目录，不在真实数据区留残留。
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

STOP_SWITCH_SCHEMA = "phase2-stop-switch-v1"
BRANCH_SUSPENDED_SCHEMA = "phase2-branch-suspended-v1"
STOP_SWITCH_FILENAME = "stop-switch.json"
BRANCH_SUSPENDED_FILENAME = "branch-suspended.json"

RC_STOPPED = "SW_STOPPED"
RC_DEFAULT_FALLBACK_STOPPED = "SW_DEFAULT_FALLBACK_STOPPED"
RC_STATE_CORRUPT = "SW_STATE_CORRUPT"
RC_BRANCH_SUSPENDED = "SW_BRANCH_SUSPENDED"
RESUME_RULE = "恢复仅经发布记录更新（resumed_suspensions 登记 suspension_id）"


def default_state_dir() -> Path:
    return Path(__file__).resolve().parents[4] / "03-估值引擎" / "data" / "phase2" / "ops-state"


def stop_switch_path(state_dir: Path | str | None = None) -> Path:
    d = Path(state_dir) if state_dir else default_state_dir()
    return d / STOP_SWITCH_FILENAME


def branch_suspended_path(state_dir: Path | str | None = None) -> Path:
    d = Path(state_dir) if state_dir else default_state_dir()
    return d / BRANCH_SUSPENDED_FILENAME


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_stop_switch(state_dir: Path | str | None = None, *, stopped: bool,
                      reason: str | None = None, operator: str | None = None) -> dict:
    """显式写入开关状态（stopped=false 是唯一"运行"态；写入即运营动作留痕）。"""
    doc = {"schema_version": STOP_SWITCH_SCHEMA, "stopped": bool(stopped),
           "reason": reason, "operator": operator, "updated_at": _now_utc(),
           "fail_closed_rule": "状态文件缺失/损坏/语义不明 → 按停止处理（默认回退＝停止正式出价）"}
    p = stop_switch_path(state_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return doc


def check_stop_switch(state_dir: Path | str | None = None) -> dict:
    """运行时检查（fail-closed）：缺失/损坏 → 停止；显式 stopped=false → 运行。"""
    p = stop_switch_path(state_dir)
    if not p.exists():
        return {"stopped": True, "present": False, "reason_codes": [RC_DEFAULT_FALLBACK_STOPPED],
                "detail": {"rule": "默认回退＝停止 105 正式出价（状态文件不存在）",
                           "path": str(p)}}
    try:
        doc = json.loads(p.read_text(encoding="utf-8-sig"))
        stopped = doc.get("stopped")
        if not isinstance(stopped, bool):
            raise ValueError("stopped 非布尔")
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        return {"stopped": True, "present": True, "reason_codes": [RC_STATE_CORRUPT],
                "detail": {"error": f"{type(exc).__name__}: {exc}", "path": str(p)}}
    if stopped:
        return {"stopped": True, "present": True, "reason_codes": [RC_STOPPED],
                "detail": {**doc, "path": str(p)}}
    return {"stopped": False, "present": True, "reason_codes": [],
            "detail": {**doc, "path": str(p)}}


# ---------------------------------------------------------------- 分支停用

def _corrupt_placeholder() -> dict:
    """损坏占位停用（fail-closed）：branch="*" 全局停用，不受 resumed_suspensions 恢复。"""
    return {"suspension_id": "CORRUPT", "branch": "*", "reason_codes": [RC_STATE_CORRUPT],
            "evidence": None, "evidence_pointer": None, "suspended_at": None,
            "resume_requires": RESUME_RULE, "corrupted": True}


def _valid_suspension_row(r) -> bool:
    """条目有效性（F7）：dict 且 suspension_id/branch 为非空字符串、reason_codes 为 list。"""
    return (isinstance(r, dict)
            and isinstance(r.get("suspension_id"), str) and r.get("suspension_id").strip() != ""
            and isinstance(r.get("branch"), str) and r.get("branch").strip() != ""
            and isinstance(r.get("reason_codes"), list))


def load_suspensions(state_dir: Path | str | None = None) -> list[dict]:
    """加载停用登记（F7 fail-closed：区分「文件不存在」与「文件存在但损坏」）。

    - 文件不存在 → []（无停用，正常默认态）；
    - 文件存在但 JSON 损坏、顶层非对象、缺 suspensions、suspensions 非 list
      （如 {}、[]、错误字段类型）→ 单条 CORRUPT 占位＝存在不明停用，拒绝出价；
    - suspensions 内个别条目无效（非 dict/缺键/字段类型错）→ 该条替换为 CORRUPT
      占位（不解释成无停用），有效条目保留。
    """
    p = branch_suspended_path(state_dir)
    if not p.exists():
        return []
    try:
        doc = json.loads(p.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        # 损坏的停用登记按"存在不明停用"处理 → 返回一条占位停用（fail-closed）
        return [_corrupt_placeholder()]
    rows = doc.get("suspensions") if isinstance(doc, dict) else None
    if not isinstance(rows, list):
        return [_corrupt_placeholder()]
    return [r if _valid_suspension_row(r) else _corrupt_placeholder() for r in rows]


def write_branch_suspension(state_dir: Path | str | None = None, *, branch: str,
                            reason_codes: list[str], evidence: dict | None = None,
                            evidence_pointer: str | None = None,
                            note: str | None = None) -> dict:
    """登记一条分支停用（幂等：同一 suspension_id 不重复追加）。"""
    p = branch_suspended_path(state_dir)
    rows = load_suspensions(state_dir)
    seed = json.dumps({"branch": branch, "reason_codes": list(reason_codes),
                       "evidence": evidence}, ensure_ascii=False, sort_keys=True)
    suspension_id = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    if any(r.get("suspension_id") == suspension_id for r in rows):
        return {"suspension_id": suspension_id, "appended": False, "path": str(p),
                "resume_requires": RESUME_RULE}
    rec = {"suspension_id": suspension_id, "branch": branch,
           "reason_codes": list(reason_codes), "evidence": evidence,
           "evidence_pointer": evidence_pointer, "suspended_at": _now_utc(),
           "note": note, "resume_requires": RESUME_RULE}
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"schema_version": BRANCH_SUSPENDED_SCHEMA,
                             "suspensions": rows + [rec]},
                            ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return {**rec, "appended": True, "path": str(p)}


def check_branch_suspended(state_dir: Path | str | None, branch: str,
                           release_record: dict | None = None) -> dict:
    """分支停用运行时检查：未被发布记录 resumed_suspensions 恢复的停用一律生效。

    F7：损坏占位（corrupted=True）不受 resumed_suspensions 恢复——损坏登记必须
    修复文件本身，不得靠发布记录登记 "CORRUPT" 静默恢复出价。
    """
    resumed = set((release_record or {}).get("resumed_suspensions") or [])
    active = [r for r in load_suspensions(state_dir)
              if r.get("branch") in (branch, "*")
              and (r.get("corrupted") or r.get("suspension_id") not in resumed)]
    return {"suspended": bool(active),
            "reason_codes": ([RC_BRANCH_SUSPENDED, RC_STATE_CORRUPT]
                             if any(r.get("corrupted") for r in active)
                             else ([RC_BRANCH_SUSPENDED] if active else [])),
            "active_suspensions": active,
            "resume_rule": RESUME_RULE}


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.stop_switch",
        "spec_basis": ("specs/phase2-formal-release「上线后监控、补证与停止」；"
                       "design D4（首启授权≠停止开关）/D6（硬门失败自动停用）；tasks 2.6/2.4"),
        "files": {"stop_switch": STOP_SWITCH_FILENAME,
                  "branch_suspended": BRANCH_SUSPENDED_FILENAME},
        "fail_closed_default": "状态文件缺失/损坏/语义不明 → 停止（默认回退＝停止 105 正式出价）",
        "independent_of_release_record": True,
        "branch_resume_rule": RESUME_RULE,
        "corrupt_suspension_rule_rv_ecr_verify_01_F7": (
            "文件存在但 schema/条目损坏（{}、[]、顶层非对象、suspensions 缺失/非 list、"
            "无效条目）→ CORRUPT 占位＝停用态拒绝出价并保留原因；不解释成无停用；"
            "占位不受 resumed_suspensions 恢复（须修复文件本身）"),
        "stdlib_only": True,
    }
