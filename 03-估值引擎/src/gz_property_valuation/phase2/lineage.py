# -*- coding: utf-8 -*-
"""phase2 来源指针固定、运行清单（manifest）与评估身份映射。

行为规格（specs/phase2-data-contracts/spec.md）：

- 来源指针与数据哈希固定：构建开始时解析一次 ``current.json`` 并固定实际数据
  run 与哈希写入 manifest；重建一律使用 manifest 固定值，不得在途中重读指针。
- 运行清单与合同冻结：manifest 绑定源数据 run 与哈希、代码版本与未提交改动
  指纹、环境、数据合同参数、产物清单、下一批更新核对规则（更新未到登记未验证，
  不阻塞开发数据准备）。
- 评估身份映射（design D5）：外部 ``community_source_id`` 及名称与现行引擎小区
  实体按名称规范化（去空白、全半角、括号后缀）自动匹配，三态状态落
  ``identity_map.json``；待确认+未匹配合计行数占比 >15% 时停线上报。
  映射只服务评估与请求边界，不改外部训练 ID。

本模块不 import 现有任何子包；引擎实体表只以 parquet 只读方式引用。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[4]
STAGED_DIR = ROOT / "03-估值引擎" / "data" / "staged" / "lianjia_ext"
POINTER_FILE = STAGED_DIR / "current.json"
RUNS_ROOT = ROOT / "examples" / "phase2_demo" / "runs"
ENTITIES_DIR = ROOT / "examples" / "phase2_demo" / "entities"
S0_DIR = ROOT / "examples" / "phase2_demo" / "s0_evidence"
CHECK_DIR = ROOT / "examples" / "phase2_demo" / "s1_data_contract"
CHANGE_DIR = ROOT / "examples" / "phase2_demo" / "change_evidence"
EVIDENCE_DIR = CHANGE_DIR / "evidence"

MANIFEST_NAME = "manifest.json"
MANIFEST_SCHEMA = "phase2-run-manifest-v1"
SOURCE_FILE_NAME = "lianjia_ext_ordinary_residential.parquet"
DEV_CUTOFF = "2026-07-20"
IDENTITY_HALT_THRESHOLD = 0.15


class LineageHalt(RuntimeError):
    """停线上报：确认契约中的停止与升级条件触发。"""


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def rel_root(path: Path) -> str:
    return Path(path).resolve().relative_to(ROOT).as_posix()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- 来源指针 ----------

def resolve_pointer_once(pointer_file: Path = POINTER_FILE) -> dict:
    raw = json.loads(pointer_file.read_text(encoding="utf-8"))
    data_run_id = raw["run_id"]
    rel = raw["ordinary_residential"]
    path = (pointer_file.parent / rel).resolve()
    if not path.exists():
        raise LineageHalt(f"来源指针指向的文件不存在：{path}")
    return {
        "pointer_file": rel_root(pointer_file),
        "resolved_at_utc": utc_now_iso(),
        "fix_rule": "构建开始时解析一次并固定；重建一律使用 manifest.source 固定值，"
                    "不再读取 current.json（spec：指针后续变化不扩散）",
        "data_run_id": data_run_id,
        "file": {
            "path": rel_root(path),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        },
    }


def load_fixed_source(run_dir: Path) -> dict:
    """从 manifest 读取固定来源（不触碰 current.json）。"""
    manifest = json.loads((run_dir / MANIFEST_NAME).read_text(encoding="utf-8"))
    src = manifest["source"]
    path = ROOT / src["file"]["path"]
    if not path.exists():
        raise LineageHalt(f"manifest 固定的源文件缺失：{path}")
    actual = sha256_file(path)
    if actual != src["file"]["sha256"]:
        raise LineageHalt(
            f"manifest 固定哈希与实际文件不一致：{src['file']['sha256']} != {actual}")
    return {"data_run_id": src["data_run_id"], "path": path,
            "sha256": src["file"]["sha256"]}


def cross_check_s0(source_sha256: str) -> dict:
    fp_path = S0_DIR / "fingerprints.json"
    fp = json.loads(fp_path.read_text(encoding="utf-8"))
    entry = fp["files"].get(f"source/{SOURCE_FILE_NAME}")
    if entry is None:
        raise LineageHalt(f"S0 fingerprints.json 缺少源数据登记项：source/{SOURCE_FILE_NAME}")
    return {
        "s0_fingerprints_file": rel_root(fp_path),
        "s0_registered_sha256": entry["sha256"],
        "match": entry["sha256"] == source_sha256,
    }


def scan_staged_after_cutoff(cutoff: str = DEV_CUTOFF) -> dict:
    runs_root = STAGED_DIR / "runs"
    per_run = {}
    for run in sorted(runs_root.glob("run_*")):
        entry = {}
        for name in (SOURCE_FILE_NAME, "lianjia_ext_sale_record.parquet"):
            p = run / name
            if not p.exists():
                continue
            cols = pl.read_parquet_schema(p)
            if "sale_date" not in cols:
                entry[name] = {"rows": None, "rows_after_cutoff": None,
                               "note": "无 sale_date 列，未扫描"}
                continue
            t = pl.read_parquet(p, columns=["sale_date"])
            after = int(t.filter(pl.col("sale_date") > cutoff).height)
            entry[name] = {"rows": t.height, "rows_after_cutoff": after,
                           "max_sale_date": t["sale_date"].max()}
        per_run[run.name] = entry
    fixed_run_id = json.loads(POINTER_FILE.read_text(encoding="utf-8"))["run_id"]
    fixed = per_run.get(f"run_{fixed_run_id}", {})
    return {
        "cutoff_date": cutoff,
        "rule": "2026-07-20 之后成交数据不纳入开发；新批次仅登记存在性，不自行纳入（proposal 不做项）",
        "runs_scanned": sorted(per_run),
        "per_run": per_run,
        "fixed_run_rows_after_cutoff": (fixed.get(SOURCE_FILE_NAME, {})
                                        .get("rows_after_cutoff")),
    }


# ---------- 代码版本与环境 ----------

def _run_git(args: list[str]) -> str | None:
    try:
        out = subprocess.run(["git", *args], cwd=ROOT, capture_output=True,
                             text=True, encoding="utf-8", errors="replace", timeout=60)
    except Exception:
        return None
    return out.stdout if out.returncode == 0 else None


def code_fingerprint() -> dict:
    head = (_run_git(["rev-parse", "HEAD"]) or "").strip() or None
    porcelain = _run_git(["status", "--porcelain"]) or ""
    lines = sorted(ln for ln in porcelain.splitlines() if ln.strip())
    fingerprint = hashlib.sha256(
        ((head or "") + "\n" + "\n".join(lines)).encode("utf-8")).hexdigest()
    return {
        "git_head": head,
        "uncommitted_changes": bool(lines),
        "uncommitted_fingerprint": fingerprint,
        "fingerprint_rule": "sha256(git_head + 排序后 git status --porcelain 行)",
        "note": "未提交改动存在时，指纹用于绑定本次 run 的实际代码状态（蓝图 §12）",
    }


def environment_info() -> dict:
    import numpy
    return {
        "python": sys.version.split()[0],
        "python_executable": str(sys.executable),
        "polars": pl.__version__,
        "numpy": numpy.__version__,
        "venv_note": "复用 S0 评估环境 .venv-data2（只读使用，不修改）",
    }


# ---------- 运行清单 ----------

def update_policy() -> dict:
    return {
        "next_batch": {
            "status": "未验证",
            "note": "下一批外源数据未到达，更新能力未实测（数据门条件 1 的规则部分；"
                    "实测待新批次到达，不阻塞开发数据准备）",
            "checks": [
                "新增：下一批 run 逐行核对 source_record_id 是否已在本轮固定快照出现，仅净新增进入重算",
                "重复：同 source_record_id 重复导出按规则①合并；跨 ID 疑似同套按规则②裁决并更新裁决清单",
                "修订：同 source_record_id 字段值变化（价/面积/日期）不静默覆盖，登记修订前后值与取得时间，人工裁决后重算",
                "取得时间：登记批次抓取时间与来源披露延迟；成交日期与信息可用日期分开登记，回顾性检验限制继续披露",
            ],
            "old_snapshot_preserved": True,
        }
    }


def create_run(district: str = "云溪区") -> tuple[Path, dict]:
    pointer = resolve_pointer_once()
    cc = cross_check_s0(pointer["file"]["sha256"])
    if not cc["match"]:
        raise LineageHalt(
            f"停止条件：源数据哈希与 S0 fingerprints.json 不一致"
            f"（{pointer['file']['sha256']} != {cc['s0_registered_sha256']}）")
    scan = scan_staged_after_cutoff()
    if scan["fixed_run_rows_after_cutoff"]:
        raise LineageHalt("停止条件：固定源 run 内存在 2026-07-20 之后成交数据")
    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "change": "build-phase2-data-contracts",
        "run_id": None,
        "created_at_utc": utc_now_iso(),
        "source": {**pointer, "cross_check_s0": cc, "post_cutoff_scan": scan},
        "code": code_fingerprint(),
        "environment": environment_info(),
        "data_contract": {
            "district_filter": {
                "column_source": "extra_fields_json.区县",
                "value": district,
                "parameterized": True,
                "note": "区县过滤参数化（design D1）；跨区扩样按蓝图 §15 后续另立实验，"
                        "本 run 仅登记全市各区别行数分布画像，不扩样",
                "citywide_profile": None,
            },
            "cleaning": {
                "rules_version": "v0-s0-修正口径",
                "rules": [
                    "① source_record_id 去重（同 ID 小区冲突组先导出裁决清单）",
                    "② 跨 ID 疑似同套：同小区+同成交日+面积差≤0.1㎡+总价差≤1000 元，组内贪心保留首行",
                    "③ 小区 ID 冲突组人工裁决清单（并入①导出）",
                    "④ 价格/面积区间过滤（10 万<总价≤2000 万、10<面积≤300、3000≤单价≤100000）",
                ],
                "s0_reconciliation": None,
            },
            "id_semantics": None,
        },
        "artifacts": {},
        "contract_fingerprint": None,
        "update_policy": update_policy(),
    }
    seed = pointer["file"]["sha256"] + json.dumps(manifest["code"], sort_keys=True)
    manifest["run_id"] = (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                          + "-" + hashlib.sha256(seed.encode()).hexdigest()[:8])
    run_dir = RUNS_ROOT / manifest["run_id"]
    run_dir.mkdir(parents=True)
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return run_dir, manifest


def load_manifest(run_dir: Path) -> dict:
    return json.loads((run_dir / MANIFEST_NAME).read_text(encoding="utf-8"))


def finalize_manifest(run_dir: Path, updates: dict) -> dict:
    """基于最新全文合并更新 manifest 并整文件写回（全局写入纪律）。"""
    manifest = load_manifest(run_dir)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(manifest.get(key), dict):
            manifest[key].update(value)
        else:
            manifest[key] = value
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return manifest


def register_artifact(run_dir: Path, name: str, rows: int | None = None,
                      extra: dict | None = None) -> dict:
    entry: dict = {"sha256": sha256_file(run_dir / name),
                   "size_bytes": (run_dir / name).stat().st_size}
    if rows is not None:
        entry["rows"] = rows
    if extra:
        entry.update(extra)
    manifest = load_manifest(run_dir)
    manifest["artifacts"][name] = entry
    (run_dir / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    return entry


# ---------- 评估身份映射（design D5） ----------

_FULL2HALF = {0x3000: " ", **{0xFF01 + i: chr(0x21 + i) for i in range(0x5E)}}
_BRACKET_SUFFIX = re.compile(r"[（(][^（）()]*[）)]$")


def normalize_name(value: str | None) -> str | None:
    """名称规范化：去空白、全角转半角、去括号后缀（design D5）。"""
    if value is None:
        return None
    s = str(value).translate(_FULL2HALF).strip()
    while True:
        s2 = _BRACKET_SUFFIX.sub("", s).strip()
        if s2 == s:
            break
        s = s2
    s = "".join(s.split()).casefold()
    return s or None


def build_identity_map(master: pl.DataFrame, run_dir: Path) -> dict:
    """外部小区 ↔ 现行引擎实体名称规范化映射，三态状态落 identity_map.json。"""
    community = pl.read_parquet(ENTITIES_DIR / "community.parquet")
    alias = pl.read_parquet(ENTITIES_DIR / "community_alias.parquet")

    name2ids: dict[str, set[str]] = {}
    pending_names: dict[str, set[str]] = {}
    status_counts = alias["conflict_status"].value_counts().to_dicts()
    for r in community.to_dicts():
        n = normalize_name(r["standard_name"])
        if n:
            name2ids.setdefault(n, set()).add(r["community_id"])
    for r in alias.to_dicts():
        n = normalize_name(r["source_alias"])
        if not n:
            continue
        if r["conflict_status"] == "一致":
            name2ids.setdefault(n, set()).add(r["community_id"])
        elif r["conflict_status"] == "待定":
            pending_names.setdefault(n, set()).add(r["community_id"])

    rows = (master.group_by(["community_source_id", "community_name"])
            .len().sort("community_source_id").to_dicts())
    entries = []
    stat_comm = {"自动匹配": 0, "待人工确认": 0, "未匹配": 0}
    stat_rows = {"自动匹配": 0, "待人工确认": 0, "未匹配": 0}
    for r in rows:
        n = normalize_name(r["community_name"])
        ids = name2ids.get(n, set())
        if len(ids) == 1:
            status, engine_id, reason = "自动匹配", next(iter(ids)), "规范化名称唯一对应引擎实体（标准名或一致别名）"
        elif len(ids) > 1:
            status, engine_id = "待人工确认", None
            reason = f"规范化名称对应多个引擎实体：{sorted(ids)}"
        elif n in pending_names:
            status, engine_id = "待人工确认", None
            reason = f"仅待定别名对应（需人工复核）：{sorted(pending_names[n])}"
        else:
            status, engine_id, reason = "未匹配", None, "规范化名称在引擎实体（标准名+一致别名）中无对应"
        stat_comm[status] += 1
        stat_rows[status] += r["len"]
        entries.append({
            "external_community_source_id": r["community_source_id"],
            "external_community_name": r["community_name"],
            "external_name_normalized": n,
            "engine_community_id": engine_id,
            "status": status,
            "reason": reason,
            "master_rows": r["len"],
        })
    total_rows = sum(stat_rows.values())
    halt_ratio = (stat_rows["待人工确认"] + stat_rows["未匹配"]) / total_rows if total_rows else 0.0
    engine_side = {
        "engine_entities_total": community.height,
        "engine_entities_referenced": len({e["engine_community_id"] for e in entries
                                           if e["engine_community_id"]}),
        "alias_rows": alias.height,
        "alias_status_counts": {d[list(d)[0]]: d["count"] for d in status_counts},
    }
    result = {
        "schema_version": "phase2-identity-map-v1",
        "purpose": "仅服务评估与请求边界（算法比较 vs 实际系统比较的共同目标）；"
                   "不改变外部训练主表 ID 体系，一对多或不确定对应不合并、不改写主表行",
        "match_rule": "名称规范化（去空白、全半角、括号后缀）自动候选；裁决权不在本层，"
                      "待人工确认行经用户确认回填后冻结指纹",
        "halt_threshold": IDENTITY_HALT_THRESHOLD,
        "stats": {
            "communities": stat_comm,
            "master_rows_by_status": stat_rows,
            "master_rows_total": total_rows,
            "pending_plus_unmatched_row_ratio": round(halt_ratio, 6),
        },
        "engine_side": engine_side,
        "entries": entries,
    }
    result["halt"] = halt_ratio > IDENTITY_HALT_THRESHOLD
    (run_dir / "identity_map.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    if result["halt"]:
        raise LineageHalt(
            "停止条件：评估身份映射待人工确认+未匹配合计 "
            f"{stat_rows['待人工确认'] + stat_rows['未匹配']} 行，"
            f"占比 {halt_ratio:.2%} > 15%（{total_rows} 行成交）。"
            "映射文件已落盘，映射策略取舍交用户决定。")
    return result


def record_identity_decision(run_dir: Path, decision_file: Path) -> dict:
    """用户对映射策略的停线裁决回填（build-phase2-data-contracts 5.1）。"""
    path = run_dir / "identity_map.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    decision = json.loads(decision_file.read_text(encoding="utf-8"))
    doc["strategy_decision"] = {
        "decided_by": "用户（任务 5.1 停线上报后裁决，2026-09-11）",
        "decision_file": rel_root(decision_file),
        **decision,
    }
    doc["halt"] = False
    doc["halt_note"] = ("阈值触发属实并保留为登记事实（48.47% > 15%）；映射策略经用户"
                        "裁决后继续，映射阈值不改。")
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    register_artifact(run_dir, "identity_map.json", extra={
        "auto_matched_communities": doc["stats"]["communities"]["自动匹配"],
        "comparison_population_rows": doc["stats"]["master_rows_by_status"]["自动匹配"],
        "strategy": doc["strategy_decision"].get("strategy"),
    })
    return doc


# ---------- 命令行入口（内部构建用，非正式 CLI） ----------

def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 lineage 工具")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("verify-source", help="任务 1.1：源哈希与 S0 交叉核对 + staged 截点扫描 + 环境登记")
    sub.add_parser("create-run", help="任务 2.1：解析指针一次，建 run 目录并落 manifest")
    p_map = sub.add_parser("identity-map", help="任务 5.1：生成评估身份映射（参数：run 目录）")
    p_map.add_argument("run_dir")
    p_city = sub.add_parser("city-profile", help="登记全市各区别行数分布画像入 manifest（参数：run 目录）")
    p_city.add_argument("run_dir")
    p_dec = sub.add_parser("identity-decision", help="回填用户映射策略裁决（参数：run 目录 裁决JSON路径）")
    p_dec.add_argument("run_dir")
    p_dec.add_argument("decision_file")
    args = parser.parse_args(argv)

    if args.cmd == "verify-source":
        pointer = resolve_pointer_once()
        cc = cross_check_s0(pointer["file"]["sha256"])
        scan = scan_staged_after_cutoff()
        _print({"source": pointer, "cross_check_s0": cc,
                "cross_check_match": cc["match"],
                "post_cutoff_scan": scan,
                "environment": environment_info(),
                "code_fingerprint": code_fingerprint(),
                "verdict": "PASS" if cc["match"] and not scan["fixed_run_rows_after_cutoff"]
                           else "HALT"})
        return 0 if cc["match"] and not scan["fixed_run_rows_after_cutoff"] else 1

    if args.cmd == "create-run":
        run_dir, manifest = create_run()
        _print({"run_dir": str(run_dir), "run_id": manifest["run_id"],
                "source": manifest["source"], "code": manifest["code"],
                "environment": manifest["environment"],
                "update_policy": manifest["update_policy"]})
        return 0

    if args.cmd == "identity-map":
        run_dir = Path(args.run_dir).resolve()
        master = pl.read_parquet(run_dir / "master_table.parquet")
        result = build_identity_map(master, run_dir)
        _print({"run_dir": str(run_dir), "stats": result["stats"],
                "engine_side": result["engine_side"], "halt": result["halt"]})
        return 0

    if args.cmd == "city-profile":
        run_dir = Path(args.run_dir).resolve()
        src = load_fixed_source(run_dir)
        t = pl.read_parquet(src["path"])
        prof = (t.select(pl.col("extra_fields_json")
                         .map_elements(lambda s: json.loads(s).get("区县"),
                                       return_dtype=pl.String).alias("district"))
                .group_by("district").len().sort("len", descending=True))
        profile = {r["district"]: r["len"] for r in prof.to_dicts()}
        finalize_manifest(run_dir, {"data_contract": {
            "district_filter": {
                "column_source": "extra_fields_json.区县",
                "value": json.loads((run_dir / MANIFEST_NAME).read_text(encoding="utf-8"))["data_contract"]["district_filter"]["value"],
                "parameterized": True,
                "note": "区县过滤参数化（design D1）；跨区扩样按蓝图 §15 后续另立实验，"
                        "本 run 仅登记全市各区别行数分布画像，不扩样",
                "citywide_profile": profile,
            }}})
        _print({"run_dir": str(run_dir), "citywide_profile": profile})
        return 0

    if args.cmd == "identity-decision":
        run_dir = Path(args.run_dir).resolve()
        doc = record_identity_decision(run_dir, Path(args.decision_file).resolve())
        _print({"run_dir": str(run_dir), "stats": doc["stats"], "halt": doc["halt"],
                "strategy": doc["strategy_decision"]["strategy"]})
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
