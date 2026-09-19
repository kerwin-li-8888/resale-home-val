# -*- coding: utf-8 -*-
"""phase2 全折统一评估：蓝图 §11 指标、双人群报告、逐折逐房输出、重跑一致性断言。

行为规格（specs「统一指标与双人群报告」「S2 run 资产与血缘」、design D5/D6）：

- 指标口径（蓝图 §11 / S1 合同 §7）：APE、MedAPE、±10% 命中率、P90 APE、有符号偏差
  （中位）、严重高估率（开发期以 20% 阈值描述性呈现，正式值 S4 冻结）、可估覆盖；
  B1 另算区间覆盖与相对全宽（分母=有上下界的行，并披露无区间数量）；B0/M1 无区间
  输出，区间两行以"无区间输出"如实呈现。
- 三类分母并列（各自分母构成可复算）：①全部合格目标（B0/M1 轨道，18 折验证带主表
  行并集）；②B1 目标（冻结子集 ∩ 验证带，即 4.1 目标清单 6,205，B1 的应估全集，
  其中 result 空对象的「信息不足」行以适用状态披露并计数、不进指标分母）；
  ③共同可比人群（B0/M1 可估 ∩ B1 valued）。
- 逐折逐房 parquet：预测、真实、误差、适用状态（正常/回退/缺失）、证据层级，
  对齐键 ``source_record_id``，可定位到 S1 合同 run。
- 重跑一致性断言：同输入同参数完整复跑一遍，逐折 parquet 字节哈希与汇总 JSON
  逐字段比对，不一致即 AssertionError（spec「结果可复现」Scenario）。
- run manifest（design D6）：S1 run 指纹、代码文件 SHA-256 清单、参数与种子、
  λ 网格各值表现与选择、各折输出哈希、评估人群定义、B1 证据指纹。

只消费 S1 合同 run 产物与 B1 导入汇总（design D1 消费边界），不读 staged 原始数据；
模型本身为闭式解与确定性聚合，无随机成分，bootstrap 种子仅用于差异不确定性区间。
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from .baselines import fallback_disclosure, fold_anchor, load_blocks, predict_b0
from .models import load_fold, fit_fold_predict, select_lambda_in_fold

SEVERE_OVER_THRESHOLD = 0.20
BOOTSTRAP_DEFAULT = 2000
BOOTSTRAP_SEED = 20260912
B1_VALUED = "valued"
B1_INSUFFICIENT = "insufficient"
B1_MISSING = "missing"
B1_NOT_IN_SCOPE = "not_in_scope"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def metrics_from_ape(ape: np.ndarray, signed: np.ndarray,
                     denominator: int | None = None) -> dict:
    """蓝图 §11 点估值全表；分母=传入数组长度（可估集），覆盖由调用方按应估全集计。"""
    n = int(len(ape))
    assert n > 0, "指标计算空分母"
    return {
        "n": n,
        "med_ape": float(np.median(ape)),
        "within10_ratio": float((ape <= 0.10).mean()),
        "p90_ape": float(np.percentile(ape, 90)),
        "signed_med_bias": float(np.median(signed)),
        "severe_over_20_ratio": float((signed > SEVERE_OVER_THRESHOLD).mean()),
        "severe_over_threshold": SEVERE_OVER_THRESHOLD,
        "coverage": (n / denominator) if denominator else 1.0,
    }


def interval_metrics(low: np.ndarray, high: np.ndarray, center: np.ndarray,
                     true_price: np.ndarray, n_b1_population: int) -> dict:
    """B1 区间覆盖与相对全宽（蓝图 §11 后两行）；分母=有上下界的行并披露无区间数量。"""
    has_interval = ~(np.isnan(low) | np.isnan(high))
    n = int(has_interval.sum())
    no_interval = int(len(low) - n)
    out = {"n_with_interval": n, "n_without_interval": no_interval,
           "b1_population": n_b1_population}
    if n:
        lo, hi = low[has_interval], high[has_interval]
        out["interval_coverage"] = float(((true_price[has_interval] >= lo)
                                          & (true_price[has_interval] <= hi)).mean())
        out["relative_full_width_median"] = float(np.median((hi - lo) / center[has_interval]))
    else:
        out["interval_coverage"] = None
        out["relative_full_width_median"] = None
    return out


def paired_bootstrap_medape_diff(ape_a: np.ndarray, ape_b: np.ndarray,
                                 n_boot: int, seed: int) -> dict:
    """共同人群配对 bootstrap：MedAPE(a−b) 的 95% 区间（差异不确定性披露用）。"""
    rng = np.random.default_rng(seed)
    n = len(ape_a)
    diffs = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        diffs[i] = np.median(ape_a[idx]) - np.median(ape_b[idx])
    return {"n_boot": n_boot, "seed": seed,
            "med_diff": float(np.median(ape_a) - np.median(ape_b)),
            "ci95_low": float(np.percentile(diffs, 2.5)),
            "ci95_high": float(np.percentile(diffs, 97.5))}


def _fold_targets(master: pl.DataFrame, fold: dict) -> pl.DataFrame:
    return master.filter(
        (pl.col("sale_date_d") >= date.fromisoformat(fold["validation"]["start"]))
        & (pl.col("sale_date_d") <= date.fromisoformat(fold["validation"]["end_inclusive"])))


def evaluate_single_pass(run_dir: Path, b1_rows: dict[dict], folds: list[dict],
                         master: pl.DataFrame, features: pl.DataFrame) -> tuple[dict, dict, list[dict]]:
    """一遍全折评估：返回 逐折逐房表字典、逐折 meta 字典、逐折 λ 记录。"""
    blocks = load_blocks(features)
    frames: dict[str, pl.DataFrame] = {}
    metas: dict[str, dict] = {}
    lambda_records: list[dict] = []
    for fold in folds:
        fid = fold["fold_id"]
        anchor = fold_anchor(fold)
        targets = _fold_targets(master, fold)
        pool = master.filter(pl.col("sale_date_d") < anchor)

        b0 = predict_b0(pool, targets, anchor, blocks)
        b0 = b0.with_columns([
            (pl.col("b0_pred") / pl.col("unit_price") - 1.0).abs().alias("b0_ape"),
            (pl.col("b0_pred") / pl.col("unit_price") - 1.0).alias("b0_signed_err"),
            pl.lit("正常").alias("b0_status"),
        ])
        b0_disclosure = fallback_disclosure(b0)
        b0 = b0.select(["source_record_id",
                        pl.col("b0_pred"), pl.col("b0_ape"), pl.col("b0_signed_err"),
                        pl.col("evidence_level").alias("b0_evidence_level"),
                        pl.col("b0_status")])

        _, _, train, val = load_fold(run_dir, fid)
        assert val["source_record_id"].sort().equals(
            targets["source_record_id"].sort()), f"{fid} M1 验证窗与折目标不一致"
        lam_sel = select_lambda_in_fold(train)
        m1_res = fit_fold_predict(train, val, lam_sel["chosen_lambda"])
        lambda_records.append({"fold_id": fid, "chosen_lambda": lam_sel["chosen_lambda"],
                               "grid": lam_sel["grid"],
                               "inner_split": lam_sel["inner_split"],
                               "selection_rule": lam_sel["selection_rule"]})
        m1 = m1_res["predictions"].select([
            "source_record_id",
            pl.col("pred_unit_price").alias("m1_pred"),
            pl.col("ape").alias("m1_ape"),
            pl.col("signed_err").alias("m1_signed_err"),
            pl.col("community_fallback").alias("m1_community_fallback")])
        m1 = m1.with_columns(
            pl.when(pl.col("m1_community_fallback")).then(pl.lit("回退"))
            .otherwise(pl.lit("正常")).alias("m1_status")).drop("m1_community_fallback")

        sids = targets["source_record_id"].to_list()
        rows = [b1_rows.get(s) for s in sids]
        b1_part = targets.select(["source_record_id"]).with_columns([
            pl.Series("in_b1_targets", [r is not None for r in rows]),
            pl.Series("b1_center", [(r.get("center") if r else None) for r in rows],
                      dtype=pl.Float64),
            pl.Series("b1_range_low",
                      [(r["range"][0] if r and r.get("range") else None) for r in rows],
                      dtype=pl.Float64),
            pl.Series("b1_range_high",
                      [(r["range"][1] if r and r.get("range") else None) for r in rows],
                      dtype=pl.Float64),
            pl.Series("b1_business_status",
                      [(r["business_status"] if r else None) for r in rows]),
        ])
        b1_part = b1_part.with_columns(
            pl.when(~pl.col("in_b1_targets"))
            .then(pl.lit(B1_NOT_IN_SCOPE))
            .when(pl.col("b1_center").is_null()
                  & (pl.col("b1_business_status") == "信息不足"))
            .then(pl.lit(B1_INSUFFICIENT))
            .when(pl.col("b1_center").is_null())
            .then(pl.lit(B1_MISSING))
            .otherwise(pl.lit(B1_VALUED)).alias("b1_status")
        )

        frame = (targets.select([
            pl.lit(fid).alias("fold_id"),
            "source_record_id", "community_source_id", "community_name", "sale_date_d",
            pl.col("unit_price").cast(pl.Float64).alias("unit_price_true"),
            pl.col("total_price_yuan").cast(pl.Float64).alias("total_price_true"),
            pl.col("transaction_area_sqm").cast(pl.Float64).alias("area_sqm")])
            .join(b0, on="source_record_id", how="left")
            .join(m1, on="source_record_id", how="left")
            .join(b1_part, on="source_record_id", how="left")
            .with_columns([
                pl.when(pl.col("b1_status") == B1_VALUED)
                .then((pl.col("b1_center") / pl.col("unit_price_true") - 1.0).abs())
                .otherwise(None).alias("b1_ape"),
                pl.when(pl.col("b1_status") == B1_VALUED)
                .then(pl.col("b1_center") / pl.col("unit_price_true") - 1.0)
                .otherwise(None).alias("b1_signed_err"),
            ])
            .sort("source_record_id"))
        assert frame.height == targets.height and frame["b0_ape"].null_count() == 0 \
            and frame["m1_ape"].null_count() == 0, f"{fid} 逐房表装配不完整"
        frames[fid] = frame
        metas[fid] = {
            "anchor": anchor.isoformat(),
            "validation_window": [fold["validation"]["start"], fold["validation"]["end_inclusive"]],
            "targets_rows": targets.height,
            "lambda": lam_sel,
            "m1_train_rows": int(train.height),
            "m1_n_features": m1_res["meta"]["n_features"],
             "b0_fallback_disclosure": b0_disclosure,
        }
    return frames, metas, lambda_records


def collect_populations(frames: dict[str, pl.DataFrame]) -> dict:
    all_fold = pl.concat(list(frames.values()))
    common = all_fold.filter(pl.col("b1_status") == B1_VALUED)
    return {"all": all_fold, "common": common}


def summarize(frames: dict[str, pl.DataFrame], metas: dict[str, dict],
              lambda_records: list[dict], b1_target_total: int,
              b1_valued_total: int, n_boot: int = BOOTSTRAP_DEFAULT) -> dict:
    """双人群 × 三模型的 §11 全表、分组指标、样本进出明细与差异不确定性。"""
    populations = collect_populations(frames)
    all_fold, common = populations["all"], populations["common"]
    n_all = all_fold.height

    def model_block(df: pl.DataFrame, pred: str, denominator: int) -> dict:
        sub = df.filter(pl.col(f"{pred}_ape").is_not_null())
        return metrics_from_ape(sub[f"{pred}_ape"].to_numpy(),
                                sub[f"{pred}_signed_err"].to_numpy(),
                                denominator=denominator)

    overall = {}
    for name, df, denom in (("全部合格目标", all_fold, n_all),
                            ("共同可比人群", common, n_all)):
        overall[name] = {
            "denominator_definition": (
                "18 折验证带主表行并集（B0/M1 应估全集）" if name.startswith("全部")
                else "全部合格目标 ∩ B1 valued（三方均有可用中心价）"),
            "rows": df.height,
            "b0": model_block(df, "b0", denom),
            "m1": model_block(df, "m1", denom),
            "b1": model_block(df.filter(pl.col("b1_status") == B1_VALUED), "b1", denom),
        }
    b1_valued_all = all_fold.filter(pl.col("b1_status") == B1_VALUED)
    overall["B1目标全集（冻结子集∩验证带）"] = {
        "denominator_definition": "4.1 目标清单 6,205（B1 应估全集）",
        "rows": b1_target_total,
        "all_valid_targets_b1_status_counts": {k: int(v) for k, v in
                                               all_fold["b1_status"].value_counts()
                                               .sort("b1_status").iter_rows()},
        "b1": model_block(b1_valued_all, "b1", b1_target_total),
        "coverage_note": f"B1 可估覆盖 = {b1_valued_total}/{b1_target_total}",
    }
    overall["B1目标全集（冻结子集∩验证带）"]["b1_interval"] = interval_metrics(
        b1_valued_all["b1_range_low"].to_numpy().astype(float),
        b1_valued_all["b1_range_high"].to_numpy().astype(float),
        b1_valued_all["b1_center"].to_numpy().astype(float),
        b1_valued_all["unit_price_true"].to_numpy(),
        n_b1_population=b1_target_total)
    overall["全部合格目标"]["b1_interval_note"] = "B0/M1 无区间输出；B1 区间仅在 B1 目标全集口径"

    per_fold = {}
    for fid, df in frames.items():
        n_common_fold = int((df["b1_status"] == B1_VALUED).sum())
        per_fold[fid] = {
            "targets_rows": df.height,
            "b1_valued": n_common_fold,
            "b0": model_block(df, "b0", df.height),
            "m1": model_block(df, "m1", df.height),
            "b1": model_block(df.filter(pl.col("b1_status") == B1_VALUED), "b1", df.height),
        }
        per_fold[fid]["med_ape_diff_m1_minus_b0"] = (
            per_fold[fid]["m1"]["med_ape"] - per_fold[fid]["b0"]["med_ape"])
        per_fold[fid]["med_ape_diff_b1_minus_b0"] = (
            per_fold[fid]["b1"]["med_ape"] - per_fold[fid]["b0"]["med_ape"])

    b0_levels = (all_fold.group_by("b0_evidence_level").agg(pl.len().alias("n"),
                  pl.col("b0_ape").median().alias("med_ape"))
                 .sort("b0_evidence_level"))
    m1_fallback = (all_fold.group_by("m1_status").agg(pl.len().alias("n"),
                   pl.col("m1_ape").median().alias("med_ape"))
                   .sort("m1_status"))
    groups = {
        "b0_by_evidence_level": {r[0]: {"n": r[1], "med_ape": r[2]}
                                 for r in b0_levels.iter_rows()},
        "m1_by_status": {r[0]: {"n": r[1], "med_ape": r[2]} for r in m1_fallback.iter_rows()},
    }

    a0 = common["b0_ape"].to_numpy()
    a1 = common["m1_ape"].to_numpy()
    a2 = common["b1_ape"].to_numpy()
    diff_uncertainty = {
        "population": "共同可比人群（配对）",
        "m1_minus_b0": paired_bootstrap_medape_diff(a1, a0, n_boot, BOOTSTRAP_SEED),
        "b1_minus_b0": paired_bootstrap_medape_diff(a2, a0, n_boot, BOOTSTRAP_SEED),
        "b1_minus_m1": paired_bootstrap_medape_diff(a2, a1, n_boot, BOOTSTRAP_SEED),
        "per_fold_diffs": {fid: {"m1_minus_b0": per_fold[fid]["med_ape_diff_m1_minus_b0"],
                                 "b1_minus_b0": per_fold[fid]["med_ape_diff_b1_minus_b0"]}
                           for fid in frames},
    }

    sample_flow = [{
        "fold_id": fid,
        "anchor": metas[fid]["anchor"],
        "validation_window": metas[fid]["validation_window"],
        "all_valid_targets": frames[fid].height,
        "b1_valued": int((frames[fid]["b1_status"] == B1_VALUED).sum()),
        "b1_insufficient": int((frames[fid]["b1_status"] == B1_INSUFFICIENT).sum()),
        "b1_missing": int((frames[fid]["b1_status"] == B1_MISSING).sum()),
        "b1_not_in_scope": int((frames[fid]["b1_status"] == B1_NOT_IN_SCOPE).sum()),
        "b0_evidence_levels": {lvl: int((frames[fid]["b0_evidence_level"] == lvl).sum())
                               for lvl in ("community", "block", "district")},
        "m1_fallback_rows": int((frames[fid]["m1_status"] == "回退").sum()),
    } for fid in frames]

    return {
        "population_definitions": {
            "all_valid_targets": "18 折验证带主表行并集（折带互不重叠，multi_fold=0）",
            "b1_targets": "冻结子集 ∩ 验证带（4.1 目标清单，B1 应估全集）",
            "common_comparable": "全部合格目标 ∩ B1 valued",
        },
        "overall": overall,
        "per_fold": per_fold,
        "groups": groups,
        "diff_uncertainty": diff_uncertainty,
        "sample_flow": sample_flow,
        "lambda_records": lambda_records,
        "totals": {"all_valid_targets": n_all, "b1_targets": b1_target_total,
                   "b1_valued": b1_valued_total,
                   "b1_insufficient": int((all_fold["b1_status"] == B1_INSUFFICIENT).sum()),
                   "b1_missing": int((all_fold["b1_status"] == B1_MISSING).sum()),
                   "b1_not_in_scope": int((all_fold["b1_status"] == B1_NOT_IN_SCOPE).sum())},
    }


def parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def load_b1_rows(import_path: Path) -> dict:
    summary = json.loads(import_path.read_text(encoding="utf-8"))
    rows = {}
    for row in summary["targets"]:
        rows[row["source_record_id"]] = row
    return rows, summary


def code_fingerprint(phase2_dir: Path) -> list[dict]:
    entries = []
    for p in sorted(phase2_dir.glob("*.py")):
        entries.append({"file": p.name, "sha256": sha256_file(p)})
    return entries


def run_id_for(s1_run_id: str, code_entries: list[dict]) -> str:
    seed = s1_run_id + json.dumps(code_entries, sort_keys=True)
    short = hashlib.sha256(seed.encode()).hexdigest()[:8]
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + short


def main_run(run_dir: Path, b1_import: Path, out_parent: Path,
             only_folds: list[str] | None, n_boot: int, skip_rerun: bool) -> dict:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    folds = [f for f in splits["folds"] if not only_folds or f["fold_id"] in only_folds]
    folds = sorted(folds, key=lambda f: f["fold_id"])
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = pl.read_parquet(run_dir / "features.parquet")
    b1_rows, b1_summary = load_b1_rows(b1_import)

    frames, metas, lambda_records = evaluate_single_pass(
        run_dir, b1_rows, folds, master, features)
    summary = summarize(frames, metas, lambda_records,
                        b1_target_total=len(b1_rows),
                        b1_valued_total=sum(1 for r in b1_rows.values()
                                            if r.get("center") is not None),
                        n_boot=n_boot)

    rerun = {"protocol": "同输入同参数完整复跑一遍；逐折 parquet 字节哈希 + 汇总 JSON 逐字段比对",
             "folds": {}, "summary_match": None}
    if not skip_rerun:
        frames2, metas2, lambda_records2 = evaluate_single_pass(
            run_dir, b1_rows, folds, master, features)
        for fid in frames:
            h1 = sha256_bytes(parquet_bytes(frames[fid]))
            h2 = sha256_bytes(parquet_bytes(frames2[fid]))
            rerun["folds"][fid] = {"pass1_sha256": h1, "pass2_sha256": h2, "match": h1 == h2}
        rerun["summary_match"] = json.dumps(summary, sort_keys=True) == json.dumps(
            summarize(frames2, metas2, lambda_records2,
                      b1_target_total=len(b1_rows),
                      b1_valued_total=sum(1 for r in b1_rows.values()
                                          if r.get("center") is not None),
                      n_boot=n_boot),
            sort_keys=True)
        mismatch = [f for f, r in rerun["folds"].items() if not r["match"]]
        assert not mismatch and rerun["summary_match"], \
            f"重跑一致性断言失败：folds={mismatch} summary_match={rerun['summary_match']}"

    if only_folds:
        print(json.dumps({"mode": "partial-smoke", "folds": [f["fold_id"] for f in folds],
                          "summary_totals": summary["totals"]}, ensure_ascii=False))
        return {"mode": "partial-smoke", "summary": summary, "rerun": rerun}

    phase2_dir = Path(__file__).resolve().parent
    run_id = run_id_for(manifest["run_id"], code_fingerprint(phase2_dir))
    out_dir = out_parent / run_id
    per_fold_dir = out_dir / "per-fold"
    per_fold_dir.mkdir(parents=True, exist_ok=False)

    output_hashes = {}
    for fid, df in frames.items():
        blob = parquet_bytes(df)
        (per_fold_dir / f"{fid}.parquet").write_bytes(blob)
        output_hashes[f"per-fold/{fid}.parquet"] = sha256_bytes(blob)

    code_entries = code_fingerprint(phase2_dir)
    metrics_payload = {
        "schema_version": "phase2-s2-eval-v1",
        "run_id": run_id,
        "s1_run": {"run_id": manifest["run_id"], "manifest_sha256": sha256_file(run_dir / "manifest.json"),
                   "splits_sha256": sha256_file(run_dir / "splits.json"),
                   "master_table_sha256": sha256_file(run_dir / "master_table.parquet"),
                   "features_sha256": sha256_file(run_dir / "features.parquet"),
                   "identity_map_sha256": sha256_file(run_dir / "identity_map.json"),
                   "contract_sha256": sha256_file(run_dir / "contract.md")},
        "b1_evidence": {"import_summary_path": str(b1_import),
                        "import_summary_sha256": sha256_file(b1_import),
                        "target_list_sha256": b1_summary["targets_file_sha256"],
                        "rule_version": b1_summary["expect_rule_version"],
                        "b1_engine_registration": "openspec/changes/build-phase2-baselines/evidence/4-1/4-1-前置登记.md",
                        "produced_outputs": b1_summary["produced_outputs"],
                        "valued_with_result": b1_summary["valued_with_result"],
                        "result_empty_count": b1_summary["result_empty_count"]},
        "code": code_entries,
        "params": {"b0_window_days": 365,
                   "b0_fallback_chain": ["community", "block", "district"],
                   "m1_lambda_grid": [1.0, 2.0, 5.0, 10.0],
                   "m1_lambda_range": [1.0, 10.0],
                   "m1_lambda_selection": "训练切片内部留出末 3 个月内部验证，MedAPE 最小；平局取更小 λ",
                   "severe_over_threshold": SEVERE_OVER_THRESHOLD,
                   "bootstrap": {"n": n_boot, "seed": BOOTSTRAP_SEED},
                   "randomness": "B0/M1 为闭式解与确定性聚合，无随机成分；种子仅用于 bootstrap",
                   "thread_pinning": {
                       "vars": ["OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS",
                                "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
                                "VECLIB_MAXIMUM_THREADS", "POLARS_MAX_THREADS"],
                       "value": "1",
                       "mechanism": "phase2/__init__.py 于 numpy/polars 导入前强制设置，"
                                    "迟导入即抛错（任何入口均先生效）",
                       "reason": "BLAS 多线程归约顺序不定 → M1 闭式解预测 ~1e-12 跨进程抖动、"
                                 "字节哈希不一致（审2 F1，RV-BP2B-VERIFY-01）；"
                                 "单线程闭式解可字节级复现，计算量为分钟级可接受"},},
        "metrics": summary,
        "rerun_consistency": rerun,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    metrics_path = out_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics_payload, ensure_ascii=False, indent=1),
                            encoding="utf-8")
    output_hashes["metrics.json"] = sha256_file(metrics_path)

    manifest_payload = {
        "schema_version": "phase2-s2-run-v1",
        "run_id": run_id,
        "s1_run": metrics_payload["s1_run"],
        "code": code_entries,
        "params": metrics_payload["params"],
        "lambda_records": lambda_records,
        "outputs": output_hashes,
        "population": summary["population_definitions"] | summary["totals"],
        "b1_evidence": metrics_payload["b1_evidence"],
        "rerun_consistency": {"summary_match": rerun["summary_match"],
                              "folds_all_match": all(r["match"] for r in rerun["folds"].values())
                              if rerun["folds"] else None,
                              "protocol": rerun["protocol"]},
        "consumption_boundary": "只读 S1 合同 run 产物与 B1 导入汇总（design D1）",
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest_payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"run_id": run_id, "out_dir": str(out_dir),
                      "totals": summary["totals"],
                      "overall_med_ape": {k: {"b0": v["b0"]["med_ape"], "m1": v["m1"]["med_ape"],
                                              "b1": v["b1"]["med_ape"]}
                                          for k, v in summary["overall"].items()
                                          if k in ("全部合格目标", "共同可比人群")},
                      "rerun_summary_match": rerun["summary_match"]},
                     ensure_ascii=False, indent=1))
    return {"run_id": run_id, "out_dir": str(out_dir), "summary": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase2-evaluate")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="全折评估：§11 全表、双人群、逐折逐房 parquet、重跑一致性")
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--b1-import", required=True, type=Path)
    p.add_argument("--out-parent", required=True, type=Path)
    p.add_argument("--folds", default="all", help="all 或逗号分隔折 ID（冒烟用）")
    p.add_argument("--no-rerun", action="store_true")
    p.add_argument("--bootstrap", type=int, default=BOOTSTRAP_DEFAULT)
    args = parser.parse_args(argv)
    only = None if args.folds == "all" else [s.strip() for s in args.folds.split(",")]
    main_run(args.run_dir.resolve(), args.b1_import.resolve(), args.out_parent.resolve(),
             only, args.bootstrap, args.no_rerun)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
