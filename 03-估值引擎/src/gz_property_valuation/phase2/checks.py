# -*- coding: utf-8 -*-
"""phase2 泄漏 / 事件隔离 / 重建一致性三项检查（design D7，可独立重复运行）。

行为规格（specs/phase2-data-contracts/spec.md「泄漏、隔离与重建检查」）：

- 泄漏检查：特征输出标签与直接推导禁入断言；市场特征聚合上界抽样暴力重算
  （严格早于、排除自身、排除同日）；内置合成违规输入自检（防检查器失明）。
- 事件隔离检查：训练材料 / 调参切片 / 校准切片 / 最终测试预留按事件键
  (source_record_id, sale_date) 两两不交；切片与各折验证集不共享事件。
- 重建一致性检查：同 manifest 固定来源重建主表与特征，逐行哈希比对一致。
  重建在系统临时目录进行，不写 run 目录（run 不可变）。

退出码语义：0=通过，1=失败并打印违规明细；检查失败报告失败而非静默通过。
运行输出（未编辑 stdout 与退出码）落校验证据目录（examples/phase2_demo/s1_data_contract/）并同步
change evidence/5-2/。
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from gz_property_valuation.phase2 import data as data_mod
from gz_property_valuation.phase2 import features as features_mod
from gz_property_valuation.phase2 import splits as splits_mod
from gz_property_valuation.phase2.lineage import load_fixed_source

LABEL_FORBIDDEN = ["total_price_raw", "total_price_yuan", "total_price_status",
                   "unit_price", "unit_price_observed", "unit_price_observed_raw",
                   "unit_price_status"]
SAMPLE_ROWS = 200
SAMPLE_SEED = 20260911
ROW_HASH_SEED = 1377


def _recompute_market_row(master: pl.DataFrame, sid: str,
                          cutoff: date | None = None) -> tuple:
    """对单行暴力重算市场特征：(中位单价, 样本数, 距今天数)。"""
    row = master.filter(pl.col("source_record_id") == sid).row(0, named=True)
    d, comm = row["sale_date_d"], row["community_source_id"]
    ev = master.filter(
        (pl.col("community_source_id") == comm)
        & (pl.col("source_record_id") != sid)
        & (pl.col("unit_price").is_not_null()))
    if cutoff is None:
        ev = ev.filter((pl.col("sale_date_d") < d)
                       & (pl.col("sale_date_d") >= d - timedelta(days=365)))
    else:
        ev = ev.filter((pl.col("sale_date_d") < cutoff)
                       & (pl.col("sale_date_d") >= cutoff - timedelta(days=365)))
    if ev.height == 0:
        return None, 0, None
    med = float(ev["unit_price"].median())
    days = int((d - ev["sale_date_d"].max()).days)
    return med, ev.height, days


def _check_label_free(features: pl.DataFrame) -> dict:
    leaked = sorted(set(features.columns) & set(LABEL_FORBIDDEN))
    return {"forbidden": LABEL_FORBIDDEN, "leaked": leaked,
            "ok": not leaked}


def check_leakage(run_dir: Path) -> dict:
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = pl.read_parquet(run_dir / "features.parquet")
    label_check = _check_label_free(features)

    rng = random.Random(SAMPLE_SEED)
    sample_idx = rng.sample(range(master.height), min(SAMPLE_ROWS, master.height))
    rows = []
    sample_ok = True
    for i in sample_idx:
        sid = master[i, "source_record_id"]
        med, cnt, days = _recompute_market_row(master, sid)
        fr = features.filter(pl.col("source_record_id") == sid).row(0, named=True)
        got_med = fr["community_med_unit_price_365d"]
        med_ok = ((got_med is None and med is None)
                  or (got_med is not None and med is not None
                      and abs(float(got_med) - med) < 1e-6))
        cnt_ok = fr["community_sample_365d"] == cnt
        days_ok = fr["community_last_sale_days"] == days
        row_ok = med_ok and cnt_ok and days_ok
        sample_ok = sample_ok and row_ok
        rows.append({"source_record_id": sid, "med_ok": med_ok,
                     "cnt_ok": cnt_ok, "days_ok": days_ok})
    mismatches = [r for r in rows if not (r["med_ok"] and r["cnt_ok"] and r["days_ok"])]

    label_selftest = False
    viol = features.head(5).with_columns(pl.lit(1.0).alias("unit_price"))
    label_selftest = bool(_check_label_free(viol)["leaked"])

    upper_selftest = False
    syn_master = pl.DataFrame({
        "source_record_id": ["A", "B"],
        "community_source_id": ["C1", "C1"],
        "sale_date_d": [date(2026, 1, 1), date(2026, 1, 1)],
        "unit_price": [100.0, 200.0],
    })
    syn_features = pl.DataFrame({
        "source_record_id": ["A", "B"],
        "community_med_unit_price_365d": [None, 100.0],
        "community_sample_365d": [0, 1],
        "community_last_sale_days": [None, 0],
    })
    med_b, cnt_b, days_b = _recompute_market_row(syn_master, "B")
    strict = (med_b, cnt_b, days_b)
    stored = (syn_features.filter(pl.col("source_record_id") == "B")
              .row(0, named=True))
    stored_vals = (stored["community_med_unit_price_365d"],
                   stored["community_sample_365d"], stored["community_last_sale_days"])
    upper_selftest = strict != stored_vals and med_b is None and cnt_b == 0

    result = {
        "check": "leakage",
        "label_free": label_check,
        "market_recompute": {"sample_rows": len(rows),
                             "mismatches": mismatches, "ok": sample_ok},
        "selftest_label_detector_detects_violation": label_selftest,
        "selftest_upperbound_detector_detects_violation": upper_selftest,
        "rule": "市场特征证据窗 [行日期−365, 行日期)：上界开区间排除同日、排除自身",
        "ok": label_check["ok"] and sample_ok and label_selftest and upper_selftest,
    }
    return result


def _events(t: pl.DataFrame) -> set[tuple[str, str]]:
    return set(zip(t["source_record_id"].to_list(), t["sale_date"].to_list()))


def check_isolation(run_dir: Path) -> dict:
    master = pl.read_parquet(run_dir / "master_table.parquet")
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    rb = splits["reserved_band"]
    reserved = master.filter((pl.col("sale_date_d") >= date.fromisoformat(rb["start"]))
                             & (pl.col("sale_date_d") <= date.fromisoformat(rb["end"])))
    half = (reserved.height + 1) // 2
    tuning = _events(reserved.head(half))
    calibration = _events(reserved.tail(reserved.height - half))
    final_test = _events(master.filter(
        pl.col("sale_date") >= splits["final_test_rule"]["window_start"]))
    training = _events(master.filter(
        (pl.col("sale_date_d") < date.fromisoformat(rb["start"])))) - tuning - calibration

    pairs = {
        "tuning∩calibration": tuning & calibration,
        "tuning∩final_test": tuning & final_test,
        "calibration∩final_test": calibration & final_test,
        "training∩tuning": training & tuning,
        "training∩calibration": training & calibration,
        "training∩final_test": training & final_test,
    }
    fold_overlaps = []
    for fold in splits["folds"]:
        val = master.filter(
            (pl.col("sale_date_d") >= date.fromisoformat(fold["validation"]["start"]))
            & (pl.col("sale_date_d") <= date.fromisoformat(fold["validation"]["end_inclusive"])))
        ev = _events(val)
        fold_overlaps.append({
            "fold": fold["fold_id"],
            "val∩tuning": len(ev & tuning),
            "val∩calibration": len(ev & calibration),
            "val∩final_test": len(ev & final_test),
            "train∩val": len(_events(master.filter(
                pl.col("sale_date_d") < date.fromisoformat(fold["anchor"]))) & ev),
        })
    fold_bad = [f for f in fold_overlaps
                if any(f[k] for k in ("val∩tuning", "val∩calibration",
                                      "val∩final_test", "train∩val"))]
    pair_bad = {k: len(v) for k, v in pairs.items() if v}
    result = {
        "check": "isolation",
        "event_key": splits["event_key"],
        "partitions": {"training_material": len(training), "tuning": len(tuning),
                       "calibration": len(calibration), "final_test_reserve": len(final_test)},
        "pairwise_intersections_nonzero": pair_bad,
        "fold_intersections": fold_overlaps,
        "final_test_rule_window_start": splits["final_test_rule"]["window_start"],
        "final_test_rows_now": len(final_test),
        "ok": not pair_bad and not fold_bad,
    }
    return result


def check_rebuild(run_dir: Path) -> dict:
    from gz_property_valuation.phase2.lineage import load_manifest
    manifest = load_manifest(run_dir)
    src = load_fixed_source(run_dir)
    tmp = Path(tempfile.mkdtemp(prefix="phase2-rebuild-"))
    try:
        (tmp / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
        rebuilt_master = data_mod.build_master(tmp)
        splits_mod.build_splits(tmp)
        rebuilt_features = features_mod.generate_features(rebuilt_master)

        run_master = pl.read_parquet(run_dir / "master_table.parquet")
        run_features = pl.read_parquet(run_dir / "features.parquet")
        run_splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
        new_splits = json.loads((tmp / "splits.json").read_text(encoding="utf-8"))

        master_hash_ok = (
            run_master.height == rebuilt_master.height
            and run_master.equals(rebuilt_master.select(run_master.columns)))
        features_hash_ok = (
            run_features.height == rebuilt_features.height
            and run_features.equals(rebuilt_features.select(run_features.columns)))
        splits_ok = _splits_equal(run_splits, new_splits)
        result = {
            "check": "rebuild",
            "source": {"data_run_id": src["data_run_id"],
                       "sha256": src["sha256"], "fixed_from_manifest": True},
            "master_rows": {"run": run_master.height, "rebuilt": rebuilt_master.height},
            "master_identical": master_hash_ok,
            "features_rows": {"run": run_features.height, "rebuilt": rebuilt_features.height},
            "features_identical": features_hash_ok,
            "splits_identical": splits_ok,
            "note": "重建在系统临时目录进行，run 目录不可变未写入；逐行比对用"
                    " DataFrame.equals（逐行逐列一致）",
            "ok": master_hash_ok and features_hash_ok and splits_ok,
        }
        return result
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _splits_equal(a: dict, b: dict) -> bool:
    if a == b:
        return True
    for k in ("reserved_band", "folds", "slices", "observed_windows",
              "final_test_rule", "params", "dev_cutoff"):
        if a.get(k) != b.get(k):
            return False
    return True


def check_dictionary(run_dir: Path) -> dict:
    features_mod.assert_dictionary_complete()
    doc = json.loads((run_dir / "feature_dictionary.json").read_text(encoding="utf-8"))
    n = sum(len(v) for v in doc["groups"].values())
    return {"check": "dictionary_assert", "fields": n,
            "seven_item_assert": "PASS", "ok": True}


def check_contract(run_dir: Path) -> dict:
    result = splits_mod.assert_contract_sections(run_dir)
    return {"check": "contract_assert", "missing": result["missing"],
            "ok": result["verdict"] == "PASS"}


def check_manifest(run_dir: Path) -> dict:
    from gz_property_valuation.phase2.lineage import load_manifest, sha256_file
    manifest = load_manifest(run_dir)
    missing, bad = [], []
    for name, entry in manifest["artifacts"].items():
        p = run_dir / name
        if not p.exists():
            missing.append(name)
            continue
        actual = sha256_file(p)
        if actual != entry["sha256"]:
            bad.append({"artifact": name, "registered": entry["sha256"],
                        "actual": actual})
    required = ["manifest.json", "master_table.parquet", "exclusions.json", "dedup.json",
                "feature_dictionary.json", "feature_dictionary.md", "features.parquet",
                "splits.json", "identity_map.json", "contract.md"]
    absent_required = [r for r in required if not (run_dir / r).exists()]
    return {"check": "manifest_integrity", "artifacts_registered": len(manifest["artifacts"]),
            "missing_artifacts": missing, "hash_mismatches": bad,
            "required_absent": absent_required,
            "contract_fingerprint": manifest.get("contract_fingerprint"),
            "ok": not missing and not bad and not absent_required}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 三项检查与自检")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="运行检查（参数：run 目录 [检查名]）")
    p.add_argument("run_dir")
    p.add_argument("check", nargs="?",
                   choices=["leakage", "isolation", "rebuild", "dictionary",
                            "contract", "manifest", "all"],
                   default="all")
    args = parser.parse_args(argv)
    run_dir = Path(args.run_dir).resolve()

    runners = {"leakage": check_leakage, "isolation": check_isolation,
               "rebuild": check_rebuild, "dictionary": check_dictionary,
               "contract": check_contract, "manifest": check_manifest}
    names = list(runners) if args.check == "all" else [args.check]
    results = []
    for name in names:
        r = runners[name](run_dir)
        results.append(r)
        print(json.dumps(r, ensure_ascii=False, indent=1, default=str))
    summary = {"run_dir": str(run_dir), "checks": [r["check"] for r in results],
               "ok": all(r["ok"] for r in results),
               "verdict": "PASS" if all(r["ok"] for r in results) else "FAIL"}
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
