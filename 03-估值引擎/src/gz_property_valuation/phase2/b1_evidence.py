# -*- coding: utf-8 -*-
"""phase2 B1 黑盒证据：目标清单构造、输出导入与指纹核验（design D1/D4）。

目标清单构造（design D4 / tasks 4.1）：

- 清单 = S1 冻结身份映射匹配子集（identity_map ``status=="自动匹配"``，234 小区 /
  14,410 行）∩ 各折验证窗主表行（验证带 ``[validation.start, validation.end_inclusive]``，
  与 splits 事件口径一致，按 ``sale_date_d`` 判窗）。
- 对齐键 ``source_record_id``（splits ``event_key`` 的记录级分量）；同一行落多折时
  ``fold_ids`` 逐折列全，不复制目标行。
- 只消费 S1 合同 run 产物（identity_map.json / splits.json / master_table.parquet），
  不读 staged 原始数据（design D1 消费边界）；清单哈希先行登记后再运行（tasks 4.1）。

输出导入与指纹核验（import-outputs 子命令，tasks 4.1 后半）：

- 独立重验：直接遍历 b1-evidence 输出树（estimates/valuation_id=*/estimate.json），
  逐文件 SHA-256 并解析包络，不信任运行驱动写入的 run-manifest（manifest 仅作
  交叉核对）；逐房对齐键 ``source_record_id``，对齐字段为包络
  ``result.subject_id`` 与 ``run_id``（run_id 内嵌 subject_id，前会话试点实测确认）。
- 未匹配/不可重建的目标披露为缺失并计数，不编造估值、不以填充值进指标分母
  （design D4）；汇总 JSON 落 b1-evidence 供任务 5.1/5.2 的全折评估消费。
- 适用状态口径（2026-09-12 实测修正）：现行引擎「信息不足」行的 envelope
  ``result`` 为空对象（非 null），不得计入 valued；valued = result 非空且含
  中心价，insufficient = result 空对象，missing = 无输出文件。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import polars as pl

SCHEMA_VERSION = "phase2-b1-target-list-v1"
IMPORT_SCHEMA_VERSION = "phase2-b1-import-summary-v1"
MATCHED_STATUS = "自动匹配"


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_manifest_rows(b1_dir: Path) -> list[dict]:
    rows: list[dict] = []
    for manifest_path in sorted(b1_dir.glob("run-manifest*.jsonl")):
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def load_matched_communities(identity_map: dict) -> dict[tuple[str, str], dict]:
    matched: dict[tuple[str, str], dict] = {}
    for entry in identity_map["entries"]:
        if entry["status"] != MATCHED_STATUS:
            continue
        engine_id = entry.get("engine_community_id")
        if not engine_id:
            raise ValueError(f"matched entry without engine id: {entry['external_community_source_id']}")
        key = (entry["external_community_source_id"], entry["external_community_name"])
        matched[key] = {
            "engine_community_id": engine_id,
            "community_name": entry["external_community_name"],
        }
    return matched


def build_target_list(run_dir: Path, output_path: Path) -> dict:
    run_dir = run_dir.resolve()
    identity_path = run_dir / "identity_map.json"
    splits_path = run_dir / "splits.json"
    master_path = run_dir / "master_table.parquet"

    identity_map = json.loads(identity_path.read_text(encoding="utf-8"))
    splits = json.loads(splits_path.read_text(encoding="utf-8"))
    matched = load_matched_communities(identity_map)

    master = pl.read_parquet(master_path)
    pairs = pl.DataFrame(
        {
            "community_source_id": [k[0] for k in matched],
            "community_name": [k[1] for k in matched],
        },
        schema={"community_source_id": pl.String, "community_name": pl.String},
    )
    subset = master.join(pairs, on=["community_source_id", "community_name"], how="semi")
    stats_sub = {
        "matched_entry_groups": len(matched),
        "frozen_subset_rows": subset.height,
        "identity_map_expected_rows": identity_map["stats"]["master_rows_by_status"][MATCHED_STATUS],
    }
    if stats_sub["frozen_subset_rows"] != stats_sub["identity_map_expected_rows"]:
        raise AssertionError(
            f"frozen subset rows {stats_sub['frozen_subset_rows']} != identity_map expected "
            f"{stats_sub['identity_map_expected_rows']}"
        )
    if subset["source_record_id"].n_unique() != subset.height:
        raise AssertionError("source_record_id not unique in frozen subset")

    folds = [
        {
            "fold_id": f["fold_id"],
            "start": date_from_iso(f["validation"]["start"]),
            "end": date_from_iso(f["validation"]["end_inclusive"]),
        }
        for f in splits["folds"]
    ]

    targets: list[dict] = []
    excluded_null_date = 0
    rows = subset.sort("source_record_id").iter_rows(named=True)
    for row in rows:
        d = row.get("sale_date_d")
        if d is None:
            excluded_null_date += 1
            continue
        fold_ids = sorted(f["fold_id"] for f in folds if f["start"] <= d <= f["end"])
        if not fold_ids:
            continue
        info = matched[(row["community_source_id"], row["community_name"])]
        targets.append({
            "source_record_id": row["source_record_id"],
            "sale_date": d.isoformat(),
            "fold_ids": fold_ids,
            "community_source_id": row["community_source_id"],
            "community_name": info["community_name"],
            "engine_community_id": info["engine_community_id"],
        })

    per_fold: dict[str, int] = {f["fold_id"]: 0 for f in folds}
    multi_fold = 0
    for t in targets:
        for fid in t["fold_ids"]:
            per_fold[fid] += 1
        if len(t["fold_ids"]) > 1:
            multi_fold += 1

    stats = {
        **stats_sub,
        "targets": len(targets),
        "target_communities": len({t["community_source_id"] for t in targets}),
        "rows_per_fold": per_fold,
        "multi_fold_rows": multi_fold,
        "rows_excluded_null_date": excluded_null_date,
    }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_run": {
            "run_id": splits["run_id"],
            "identity_map_sha256": _sha256_file(identity_path),
            "splits_sha256": _sha256_file(splits_path),
            "master_table_sha256": _sha256_file(master_path),
            "event_key": splits["event_key"],
            "alignment_key": "source_record_id",
        },
        "construction_rule": (
            "identity_map status=自动匹配 冻结子集 ∩ 各折验证窗 "
            "[validation.start, validation.end_inclusive]（sale_date_d 判窗，"
            "与 splits 事件口径一致）；fold_ids 列全不复制行"
        ),
        "stats": stats,
        "targets": targets,
    }
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    result = {
        "output": str(output_path),
        "output_sha256": _sha256_file(output_path),
        "stats": stats,
    }
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return result


def import_outputs(
    targets_path: Path, b1_dir: Path, output_path: Path, expect_rule_version: str
) -> dict:
    """扫描 b1-evidence 输出树做逐文件哈希、逐房对齐与缺失计数（design D4）。

    独立重验：以输出树为唯一事实源；run-manifest 仅作哈希交叉核对，不作对齐依据。
    """
    payload = json.loads(targets_path.resolve().read_text(encoding="utf-8"))
    targets = {t["source_record_id"]: t for t in payload["targets"]}
    estimates_root = b1_dir.resolve() / "estimates"
    subjects_dir = b1_dir.resolve() / "subjects"

    manifest_rows = _load_manifest_rows(b1_dir)
    manifest_by_sid: dict[str, list[dict]] = {}
    for r in manifest_rows:
        manifest_by_sid.setdefault(str(r.get("source_record_id")), []).append(r)

    produced: dict[str, dict] = {}
    unreadable: list[dict] = []
    strays: list[str] = []
    orphan_outputs: list[dict] = []
    business_status_counts: dict[str, int] = {}
    confidence_counts: dict[str, int] = {}
    result_empty = 0
    valued = 0

    if estimates_root.is_dir():
        for entry in sorted(estimates_root.iterdir()):
            if not entry.is_dir() or not entry.name.startswith("valuation_id="):
                strays.append(f"estimates/{entry.name}")
                continue
            children = sorted(p.name for p in entry.iterdir())
            for name in children:
                if name != "estimate.json":
                    strays.append(f"estimates/{entry.name}/{name}")
            est_path = entry / "estimate.json"
            if not est_path.is_file():
                continue
            digest = _sha256_file(est_path)
            try:
                envelope = json.loads(est_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                unreadable.append({"path": entry.name, "error": str(exc)[:200]})
                continue
            result = envelope.get("result")
            sid = str(result["subject_id"]) if result else None
            if sid is None:
                run_id = str(envelope.get("run_id") or "")
                sid = run_id[4:].split("-", 1)[0] if run_id.startswith("RUN-") else None
            if sid is None or sid not in targets:
                orphan_outputs.append({"run_id": envelope.get("run_id"), "path": entry.name})
                continue
            business = envelope.get("business_status")
            business_status_counts[business] = business_status_counts.get(business, 0) + 1
            row = {
                "source_record_id": sid,
                "run_id": envelope.get("run_id"),
                "estimate_path": f"estimates/{entry.name}/estimate.json",
                "estimate_sha256": digest,
                "business_status": business,
                "rule_version": envelope.get("rule_version"),
                "warnings": envelope.get("warnings", []),
                "fold_ids": targets[sid]["fold_ids"],
            }
            if result:
                valued += 1
                conf = str(result.get("confidence"))
                confidence_counts[conf] = confidence_counts.get(conf, 0) + 1
                row.update({
                    "center": result.get("center"),
                    "range": result.get("range"),
                    "valuation_date": result.get("valuation_date"),
                    "n_comps": result.get("n_comps"),
                    "status": result.get("status"),
                })
            else:
                result_empty += 1
            produced[sid] = row

    missing = sorted(set(targets) - set(produced))
    per_fold = {f: 0 for f in payload["stats"]["rows_per_fold"]}
    for sid, row in produced.items():
        for f in row["fold_ids"]:
            per_fold[f] += 1

    structural_anomalies: list[str] = []
    subject_files_missing: list[str] = []
    subject_sha_mismatch: list[str] = []
    for sid, row in produced.items():
        if row["run_id"] and row["estimate_path"] != f"estimates/valuation_id={row['run_id']}/estimate.json":
            structural_anomalies.append(f"{sid}:dir_name")
        if row.get("rule_version") != expect_rule_version:
            structural_anomalies.append(f"{sid}:rule_version={row.get('rule_version')}")
        valuation_date = row.get("valuation_date")
        if valuation_date is not None and valuation_date != targets[sid]["sale_date"]:
            structural_anomalies.append(f"{sid}:valuation_date={valuation_date}")
        subject_path = subjects_dir / f"{sid}.json"
        if not subject_path.is_file():
            subject_files_missing.append(sid)
            continue
        rows_for_sid = manifest_by_sid.get(sid, [])
        if any(
            r.get("subject_sha256") and r["subject_sha256"] != _sha256_file(subject_path)
            for r in rows_for_sid
        ):
            subject_sha_mismatch.append(sid)

    manifest_estimate_sha_mismatch = [
        sid
        for sid, row in produced.items()
        for oks in [
            [r for r in manifest_by_sid.get(sid, []) if r.get("status") == "ok"]
        ]
        if oks and oks[-1].get("estimate_sha256") != row["estimate_sha256"]
    ]
    manifest_subject_invalid = [
        {"source_record_id": r.get("source_record_id"), "error": str(r.get("error", ""))[:200]}
        for r in manifest_rows
        if r.get("status") == "subject_invalid"
    ]
    manifest_errors = [
        {"source_record_id": r.get("source_record_id"), "status": r.get("status"), "error": str(r.get("error", ""))[:200]}
        for r in manifest_rows
        if r.get("status") in ("error", "master_row_missing")
    ]

    subject_files_total = len(list(subjects_dir.glob("*.json"))) if subjects_dir.is_dir() else 0
    summary = {
        "schema_version": IMPORT_SCHEMA_VERSION,
        "expect_rule_version": expect_rule_version,
        "targets_file_sha256": _sha256_file(targets_path.resolve()),
        "targets_in_list": len(targets),
        "produced_outputs": len(produced),
        "valued_with_result": valued,
        "result_empty_count": result_empty,
        "missing_count": len(missing),
        "missing_source_record_ids": missing,
        "orphan_outputs": orphan_outputs,
        "unreadable_outputs": unreadable,
        "stray_entries": strays,
        "business_status_counts": business_status_counts,
        "confidence_counts": confidence_counts,
        "produced_per_fold": per_fold,
        "checks": {
            "structural_anomalies": structural_anomalies,
            "subject_files_missing": subject_files_missing,
            "subject_sha_mismatch_vs_manifest": subject_sha_mismatch,
            "manifest_estimate_sha_mismatch": manifest_estimate_sha_mismatch,
            "manifest_rows_total": len(manifest_rows),
            "manifest_subject_invalid": manifest_subject_invalid,
            "manifest_errors": manifest_errors,
            "subject_files_total": subject_files_total,
        },
        "targets": [produced[sid] for sid in sorted(produced)],
    }
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    result = {
        "output": str(output_path),
        "output_sha256": _sha256_file(output_path),
        "targets_in_list": summary["targets_in_list"],
        "produced_outputs": summary["produced_outputs"],
        "valued_with_result": summary["valued_with_result"],
        "result_empty_count": summary["result_empty_count"],
        "missing_count": summary["missing_count"],
        "orphan_outputs": len(orphan_outputs),
        "unreadable_outputs": len(unreadable),
        "stray_entries": len(strays),
        "business_status_counts": business_status_counts,
        "structural_anomalies": len(structural_anomalies),
        "manifest_estimate_sha_mismatch": len(manifest_estimate_sha_mismatch),
    }
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return result


def date_from_iso(value: str):
    from datetime import date

    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="b1-evidence")
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build-target-list", help="构造 B1 目标清单并登记哈希")
    build.add_argument("--run-dir", required=True, type=Path)
    build.add_argument("--output", required=True, type=Path)
    imp = sub.add_parser(
        "import-outputs", help="扫描 b1-evidence 输出树：逐文件哈希、逐房对齐、缺失计数"
    )
    imp.add_argument("--targets", required=True, type=Path)
    imp.add_argument("--b1-dir", required=True, type=Path)
    imp.add_argument("--output", required=True, type=Path)
    imp.add_argument("--expect-rule-version", default="1.4")
    args = parser.parse_args(argv)
    if args.command == "build-target-list":
        build_target_list(args.run_dir, args.output)
    if args.command == "import-outputs":
        import_outputs(
            args.targets, args.b1_dir, args.output, args.expect_rule_version
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
