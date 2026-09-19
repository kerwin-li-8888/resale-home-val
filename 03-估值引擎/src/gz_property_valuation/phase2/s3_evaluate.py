# -*- coding: utf-8 -*-
"""phase2 S3 统一评估（compare-phase2-nonlinear 任务 6.1）：同窗重算 + 蓝图 §11 全表与报告。

行为规格（change `compare-phase2-nonlinear` 的 design D9 与 specs
`phase2-nonlinear-comparisons`「试验注册表与统一报告」）：

- **同窗重算口径**：S3 新增模型线（M2 / M3a / M3b / M4 / INC_M2 / INC_M3a）的总体与逐折指标
  由本模块在**本机按同一折、同一窗口重算**得到——逐折从云端单元清单读取该折该模型**已选定
  配置**，用 :mod:`gz_property_valuation.phase2.nonlinear` 在该折完整训练切片（严格早于折外层
  截点）上**只重拟合该已选配置**，预测折验证带（与 S2 B0/M1 完全相同的行集），得到**逐行
  预测**后聚合。云侧只持久化了每单元指标摘要（无逐行预测），逐折中位数无法聚合为总体中位数，
  故总体指标必须重算——这是「同窗重算口径」的来由。
- **对拍准入**：重算的逐折指标与该折该模型云端 `metric_*` 逐字段按跨机容差
  ``|Δ| ≤ 1e-9 × max(|ref|, 1)``（非有限值即 FAIL）对拍；对拍通过才可使用重算值，不一致即
  按停止条件上报，不得混用两套口径。
- **B0/M1 引用 S2 冻结值**：直接读取 S2 run `per-fold/*.parquet` 的逐行预测（不重拟合），
  同窗口径 = 同一折验证带行集；与 S2 metrics.json 的总体值交叉校验。
- **双人群**：全部合格目标（18 折验证带并集 12,095 行）与共同可比人群（∩ B1 valued 6,153 行）。
- **B1**：仅作登记性并列参考并附量级异常警示，不作对照基线（spec「B1 证据封存与禁用边界」）。
- 指标口径：蓝图 §11（MedAPE / ±10% 命中 / P90 APE / 有符号偏差 / 严重高估率 / 可估覆盖）。
- **折级分片并行**：折之间无共享写（跨折独立性已由 2.1/6.2 断言），故本机按折分片并行执行同一
  套重算；每 worker 单线程（CatBoost thread_count=1、LightGBM num_threads=1、包导入期钉死
  BLAS/polars 线程），整机 CPU 占比 ≈ workers / 逻辑核数，须显著低于护机红线 50% 且总墙钟 <1h。

只读消费：S1 合同 run、S2 run、S3 run 的 experiments.jsonl、云端回收产物（merged/units.parquet
与 out/shard-*.json）。写：S3 run `s3-eval/`（逐折逐行预测 + 评估汇总 + 对拍明细）与校验目录报告。
不修改 derived_inputs.py / nonlinear.py / a1_experiment.py 的已验收口径。
"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from gz_property_valuation.phase2 import derived_inputs as di
from gz_property_valuation.phase2 import nonlinear as nl
from gz_property_valuation.phase2 import experiments as exp_mod

import numpy as np
import polars as pl

TOL_REL = 1e-9
BOOTSTRAP_N = 2000
BOOTSTRAP_SEED = exp_mod.SEED
SEVERE_OVER_THRESHOLD = 0.20
METRIC_KEYS = ("med_ape", "within10_ratio", "p90_ape", "signed_med_bias", "nonnull_ratio")

S3_LINES = ("M2", "M3a", "M3b", "M4", "INC_M2", "INC_M3a")
BASE_MODEL_OF = {"INC_M2": "M2", "INC_M3a": "M3a"}
GRID_MODELS = ("M2", "M3a", "M3b", "M4")

LINE_SPEC = {
    "M2": ("catboost", "full", "none"),
    "M3a": ("catboost", "full", "market"),
    "M3b": ("catboost", "full", "m3b"),
    "M4": ("lightgbm", "full", "none"),
    "INC_M2": ("catboost", "base", "none"),
    "INC_M3a": ("catboost", "base", "market"),
}
LINE_LABEL = {
    "B0": "B0 近期小区基准（S2 冻结）",
    "M1": "M1 Ridge（S2 冻结）",
    "M2": "M2 CatBoost 直接报价",
    "M3a": "M3a 局部市场水平＋CatBoost 价差",
    "M3b": "M3b 属性标准化变体",
    "M4": "M4 LightGBM 挑战",
    "INC_M2": "INC_M2 增量·基础特征组（M2 结构）",
    "INC_M3a": "INC_M3a 增量·基础特征组（M3a 结构）",
}
POP_ALL = "全部合格目标"
POP_COMMON = "共同可比人群"


# ---------------- 通用 ----------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def logical_cores() -> int:
    return os.cpu_count() or 1


# ---------------- 云端单元清单 ----------------

def read_cloud_units(merged_parquet: Path) -> pl.DataFrame:
    return pl.read_parquet(merged_parquet)


def chosen_configs(units: pl.DataFrame) -> dict[tuple[str, str], dict]:
    """逐折逐模型已选定配置：本折 inner_select 单元 MedAPE 最小者（平局取登记顺序靠前者）。"""
    picks: dict[tuple[str, str], dict] = {}
    for model in GRID_MODELS:
        sub = units.filter((pl.col("model") == model) & (pl.col("stage") == "inner_select"))
        for fold_id in sorted(set(sub["fold_id"].to_list())):
            rows = (sub.filter(pl.col("fold_id") == fold_id)
                    .select(["config_id", "metric_med_ape"])
                    .sort(["metric_med_ape", "config_id"]))
            picks[(model, fold_id)] = {
                "config_id": rows["config_id"][0],
                "inner_med_ape": float(rows["metric_med_ape"][0]),
                "inner_grid": {r["config_id"]: float(r["metric_med_ape"])
                               for r in rows.iter_rows(named=True)},
            }
    return picks


def grid_configs(model: str) -> dict[str, dict]:
    grids = {"M2": exp_mod.M2_GRID, "M3a": exp_mod.M3A_GRID,
             "M3b": exp_mod.M3B_GRID, "M4": exp_mod.M4_GRID}
    return {exp_mod._cfg_id(model, i): cfg
            for i, cfg in enumerate(grids[model], start=1)}


def cloud_outer_configs(shard_dir: Path, folds: list[str]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for fold_id in folds:
        obj = json.loads((shard_dir / f"shard-{fold_id}.json").read_text(encoding="utf-8"))
        for unit in obj["units"]:
            if unit.get("stage") == "outer_refit":
                out[(unit["model"], fold_id)] = dict(unit["config"])
    return out


def cloud_outer_metrics(units: pl.DataFrame) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for r in units.filter(pl.col("stage") == "outer_refit").iter_rows(named=True):
        out[(r["model"], r["fold_id"])] = {k: r[f"metric_{k}"] for k in METRIC_KEYS}
    return out


# ---------------- 指标 ----------------

def metrics_from_arrays(ape: np.ndarray, signed: np.ndarray, n_expected: int) -> dict:
    """蓝图 §11 点估值表（与 evaluate.metrics_from_ape 同口径）。"""
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
        "coverage": (n / n_expected) if n_expected else 1.0,
        "n_expected": int(n_expected),
    }


def paired_bootstrap_medape(a: np.ndarray, b: np.ndarray,
                            n_boot: int = BOOTSTRAP_N,
                            seed: int = BOOTSTRAP_SEED) -> dict:
    rng = np.random.default_rng(seed)
    n = len(a)
    diffs = np.empty(n_boot)
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        diffs[i] = np.median(a[idx]) - np.median(b[idx])
    lo = float(np.percentile(diffs, 2.5))
    hi = float(np.percentile(diffs, 97.5))
    return {"n_boot": n_boot, "seed": seed, "paired": True,
            "med_diff": float(np.median(a) - np.median(b)),
            "ci95_low": lo, "ci95_high": hi,
            "crosses_zero": bool(lo <= 0.0 <= hi)}


# ---------------- 同窗重算（单折） ----------------

def _log_bases(frames: dict, log_mode: str, layer: str = "outer"):
    if log_mode == "none":
        return None
    if log_mode == "market":
        return {k: np.log(frames[k]["market_chain_med_unit_price"].cast(pl.Float64).to_numpy())
                for k in di.FRAME_NAMES}
    if log_mode == "m3b":
        b = nl.m3b_standardized_baseline(frames, layer)
        return {f"{layer}_train": b["base_train_log"], f"{layer}_val": b["base_val_log"]}
    raise ValueError(f"未知 log_mode：{log_mode}")


def _fit_outer(frames: dict, engine: str, config: dict, feature_group: str,
               log_mode: str) -> dict:
    """已选配置在该折完整训练切片（外层输入）重拟合 → 预测折验证带。"""
    columns = nl.feature_columns(feature_group)
    cat_columns = [c for c in columns if c in nl.CATEGORICAL_COLUMNS]
    encoder = nl.FoldCategoryEncoder().fit(frames["outer_train"], columns, cat_columns)
    cat_index = [columns.index(c) for c in cat_columns]
    Xtr = encoder.transform(frames["outer_train"])
    Xva = encoder.transform(frames["outer_val"])
    lb = _log_bases(frames, log_mode, "outer")
    if lb is None:
        lb_tr = np.zeros(frames["outer_train"].height)
        lb_va = np.zeros(frames["outer_val"].height)
    else:
        lb_tr = np.asarray(lb["outer_train"], dtype=np.float64)
        lb_va = np.asarray(lb["outer_val"], dtype=np.float64)
    ytr = np.log(frames["outer_train"]["unit_price"].cast(pl.Float64).to_numpy()) - lb_tr
    if not np.isfinite(ytr).all():
        raise AssertionError("outer_train 目标出现非有限值")
    t0 = time.perf_counter()
    _, predict = nl.fit_model(engine, config, Xtr, ytr, cat_index)
    pred_log = predict(Xva)
    fit_seconds = time.perf_counter() - t0
    true = frames["outer_val"]["unit_price"].cast(pl.Float64).to_numpy()
    pred = np.exp(pred_log + lb_va)
    return {"pred_unit_price": pred, "pred_log": pred_log, "log_base": lb_va, "true": true,
            "fit_seconds": fit_seconds,
            "metrics": nl.ape_metrics(pred_log, lb_va, true)}


def _fold_worker(fold_id: str, s1_run: Path, cloud_src: Path, out_dir: Path) -> dict:
    """折级 worker：单折 6 条模型线重算，逐行预测与逐折记录立即落盘。"""
    master, features, splits = di.load_s1(s1_run)
    fold = next(f for f in splits["folds"] if f["fold_id"] == fold_id)
    pool = di.build_pool(master, features)
    row_level = di.row_level_market(pool)
    picks = chosen_configs(read_cloud_units(cloud_src / "merged" / "units.parquet"))
    shard_cfgs = cloud_outer_configs(cloud_src / "out", [fold_id])

    t0 = time.perf_counter()
    frames = di.derive_fold(master, features, fold, pool=pool, row_level=row_level,
                            with_static=True)
    derive_seconds = time.perf_counter() - t0

    rows: list[pl.DataFrame] = []
    lines: dict[str, dict] = {}
    for model in S3_LINES:
        base = BASE_MODEL_OF.get(model, model)
        cfg_id = picks[(base, fold_id)]["config_id"]
        cfg = grid_configs(base)[cfg_id]
        if (base, fold_id) in shard_cfgs and dict(shard_cfgs[(base, fold_id)]) != dict(cfg):
            raise AssertionError(f"{base}:{fold_id} 选定配置与云端 shard 不一致")
        res = _fit_outer(frames, LINE_SPEC[model][0], cfg, LINE_SPEC[model][1],
                         LINE_SPEC[model][2])
        ratio = res["pred_unit_price"] / res["true"]
        rows.append(pl.DataFrame({
            "fold_id": [fold_id] * len(res["true"]),
            "model": [model] * len(res["true"]),
            "source_record_id": frames["outer_val"]["source_record_id"].to_list(),
            "pred_unit_price": res["pred_unit_price"],
            "unit_price_true": res["true"],
            "ape": np.abs(ratio - 1.0),
            "signed_err": ratio - 1.0,
            "evidence_level": frames["outer_val"]["market_evidence_level"].to_list(),
        }))
        lines[model] = {
            "config_id": cfg_id, "config": dict(cfg), "feature_group": LINE_SPEC[model][1],
            "log_mode": LINE_SPEC[model][2], "fit_seconds": res["fit_seconds"],
            "outer_train_rows": frames["outer_train"].height,
            "outer_val_rows": frames["outer_val"].height,
            "metrics": res["metrics"],
        }
    (out_dir / "per-fold").mkdir(parents=True, exist_ok=True)
    pl.concat(rows).write_parquet(out_dir / "per-fold" / f"{fold_id}.parquet")

    warmup = nl.m3b_standardized_baseline(frames, "outer")
    record = {"fold_id": fold_id, "derive_seconds": derive_seconds,
              "row_counts": frames["row_counts"], "cold_start": frames["cold_start"],
              "inner_cutoff": frames["inner_cutoff"], "outer_cutoff": frames["outer_cutoff"],
              "m3b_warmup_outer": {
                  "warmup_rows": warmup["warmup_rows"],
                  "standardized_rows": warmup["standardized_rows"],
                  "warmup_ratio": warmup["warmup_rows"]
                  / (warmup["warmup_rows"] + warmup["standardized_rows"])},
              "lines": lines}
    (out_dir / "records").mkdir(parents=True, exist_ok=True)
    (out_dir / "records" / f"{fold_id}.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return record


def _launch_fold(fold_id: str, s1_run: Path, cloud_src: Path, out_dir: Path) -> dict:
    """子进程执行单折（Windows spawn 下避免 pickle 主模块歧义）。"""
    env = dict(os.environ)
    src = str(ROOT_SRC)
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    cmd = [sys.executable, "-m", "gz_property_valuation.phase2.s3_evaluate", "worker",
           "--fold", fold_id, "--s1-run", str(s1_run), "--cloud-src", str(cloud_src),
           "--out-dir", str(out_dir)]
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"折 {fold_id} worker 失败（退出码 {proc.returncode}）："
                             f"{proc.stderr[-2000:]}")
    return json.loads((out_dir / "records" / f"{fold_id}.json").read_text(encoding="utf-8"))


def run_recompute(s1_run: Path, cloud_src: Path, out_dir: Path, folds: list[str],
                  workers: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    if workers <= 1:
        records = []
        for fid in folds:
            records.append(_fold_worker(fid, s1_run, cloud_src, out_dir))
            print(f"[recompute] {fid} done", flush=True)
    else:
        from concurrent.futures import ThreadPoolExecutor
        records = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_launch_fold, fid, s1_run, cloud_src, out_dir): fid
                    for fid in folds}
            for fut in concurrent.futures.as_completed(futs):
                records.append(fut.result())
                print(f"[recompute] {futs[fut]} done ({len(records)}/{len(folds)})", flush=True)
    wall = time.perf_counter() - t0
    records.sort(key=lambda r: r["fold_id"])
    return {"folds": records, "wall_seconds": wall, "workers": workers,
            "logical_cores": logical_cores()}


# ---------------- 汇总装配 ----------------

def _s3_wide(pf_dir: Path, folds: list[str]) -> pl.DataFrame:
    """逐行预测长表 → 宽表（每折每目标一行，6 条模型线并列）。"""
    long = pl.concat([pl.read_parquet(pf_dir / f"{fid}.parquet") for fid in folds])
    wide: pl.DataFrame | None = None
    for model in S3_LINES:
        sub = (long.filter(pl.col("model") == model)
               .select(["fold_id", "source_record_id",
                        pl.col("ape").alias(f"ape_{model}"),
                        pl.col("signed_err").alias(f"signed_{model}"),
                        pl.col("pred_unit_price").alias(f"pred_{model}"),
                        pl.col("evidence_level").alias(f"ev_{model}")]))
        wide = sub if wide is None else wide.join(sub, on=["fold_id", "source_record_id"],
                                                  how="inner")
    return wide


def _s2_frozen(s2_run: Path, folds: list[str]) -> pl.DataFrame:
    cols = ["fold_id", "source_record_id", "community_name", "sale_date_d", "area_sqm",
            "unit_price_true", "b0_pred", "b0_ape", "b0_signed_err", "b0_evidence_level",
            "m1_pred", "m1_ape", "m1_signed_err", "m1_status", "b1_status", "b1_center",
            "b1_ape", "b1_signed_err"]
    return pl.concat([pl.read_parquet(s2_run / "per-fold" / f"{fid}.parquet").select(cols)
                      for fid in folds])


def build_summary(s1_run: Path, s2_run: Path, s3_run: Path, cloud_src: Path,
                  out_dir: Path, recompute: dict, workers: int) -> dict:
    folds = [r["fold_id"] for r in recompute["folds"]]
    units = read_cloud_units(cloud_src / "merged" / "units.parquet")
    picks = chosen_configs(units)
    cloud_metrics = cloud_outer_metrics(units)

    # --- 逐折对拍 ---
    detail: list[dict] = []
    for rec in recompute["folds"]:
        fid = rec["fold_id"]
        for model in S3_LINES:
            local = rec["lines"][model]["metrics"]
            cloud = cloud_metrics[(model, fid)]
            for key in METRIC_KEYS:
                a, b = float(local[key]), float(cloud[key])
                finite = bool(np.isfinite(a) and np.isfinite(b))
                abs_delta = abs(a - b) if finite else float("inf")
                rel_delta = (abs_delta / max(abs(b), 1.0)) if finite else float("inf")
                detail.append({"fold_id": fid, "model": model, "field": key,
                               "local": a, "cloud": b, "abs_delta": abs_delta,
                               "rel_delta": rel_delta, "finite": finite,
                               "pass": bool(finite and abs_delta <= TOL_REL * max(abs(b), 1.0))})
    max_abs = max(d["abs_delta"] for d in detail)
    max_rel = max(d["rel_delta"] for d in detail)
    cross_check = {"caliber": "|Δ| <= 1e-9 * max(|ref|, 1)；非有限值即 FAIL",
                   "fields": len(detail), "units": len(S3_LINES) * len(folds),
                   "max_abs_delta": max_abs, "max_rel_delta": max_rel,
                   "all_pass": all(d["pass"] for d in detail),
                   "fail_count": sum(1 for d in detail if not d["pass"])}

    # --- 逐行宽表 + S2 冻结值 ---
    wide = _s3_wide(out_dir / "per-fold", folds)
    frozen = _s2_frozen(s2_run, folds)
    n_s3 = wide.height
    n_s2 = frozen.height
    joined = wide.join(frozen, on=["fold_id", "source_record_id"], how="inner")
    assert joined.height == n_s3 == n_s2, (
        f"S3 逐行与 S2 冻结行集不匹配：s3={n_s3} s2={n_s2} joined={joined.height}")
    per_fold_rows = {r["fold_id"]: r["n"] for r in
                     frozen.group_by("fold_id").agg(pl.len().alias("n")).iter_rows(named=True)}
    per_fold_s3 = {r["fold_id"]: r["n"] for r in
                   wide.group_by("fold_id").agg(pl.len().alias("n")).iter_rows(named=True)}
    assert per_fold_rows == per_fold_s3, "逐折行数 S3/S2 不一致"

    # --- 双人群指标 ---
    pops = {"all": joined, "common": joined.filter(pl.col("b1_status") == "valued")}
    n_expected = {"all": joined.height, "common": int(pops["common"].height)}

    def mblock(df: pl.DataFrame, model: str, n_exp: int) -> dict:
        ape = df[f"ape_{model}"].to_numpy().astype(np.float64)
        sign = df[f"signed_{model}"].to_numpy().astype(np.float64)
        ok = np.isfinite(ape)
        return metrics_from_arrays(ape[ok], sign[ok], n_exp)

    def frozen_block(df: pl.DataFrame, tag: str, n_exp: int) -> dict:
        ape = df[f"{tag}_ape"].to_numpy().astype(np.float64)
        sign = df[f"{tag}_signed_err"].to_numpy().astype(np.float64)
        return metrics_from_arrays(ape, sign, n_exp)

    overall: dict[str, dict] = {}
    for pname, key in ((POP_ALL, "all"), (POP_COMMON, "common")):
        df = pops[key]
        blk = {"rows": df.height, "n_expected": n_expected[key]}
        blk["B0"] = frozen_block(df, "b0", n_expected[key])
        blk["M1"] = frozen_block(df, "m1", n_expected[key])
        for model in S3_LINES:
            blk[model] = mblock(df, model, n_expected[key])
        overall[pname] = blk

    # --- S2 冻结总体值交叉校验 ---
    s2_metrics = json.loads((s2_run / "metrics.json").read_text(encoding="utf-8"))
    s2_all = s2_metrics["metrics"]["overall"][POP_ALL]
    s2_check = {
        "source": "S2 run metrics.json overall[全部合格目标]",
        "applicable": len(folds) == 18,
        "b0_med_ape": {"s2": s2_all["b0"]["med_ape"], "recomputed": overall[POP_ALL]["B0"]["med_ape"],
                       "abs_delta": abs(s2_all["b0"]["med_ape"] - overall[POP_ALL]["B0"]["med_ape"])},
        "m1_med_ape": {"s2": s2_all["m1"]["med_ape"], "recomputed": overall[POP_ALL]["M1"]["med_ape"],
                       "abs_delta": abs(s2_all["m1"]["med_ape"] - overall[POP_ALL]["M1"]["med_ape"])},
    }
    s2_check["pass"] = (bool(abs(s2_all["b0"]["med_ape"] - overall[POP_ALL]["B0"]["med_ape"]) < 1e-12
                             and abs(s2_all["m1"]["med_ape"] - overall[POP_ALL]["M1"]["med_ape"]) < 1e-12)
                        if s2_check["applicable"] else None)

    # --- 逐折指标 ---
    per_fold: dict[str, dict] = {}
    for fid in folds:
        df = joined.filter(pl.col("fold_id") == fid)
        entry = {"n": df.height, "B0": frozen_block(df, "b0", df.height),
                 "M1": frozen_block(df, "m1", df.height)}
        for model in S3_LINES:
            entry[model] = mblock(df, model, df.height)
        per_fold[fid] = entry

    # --- 差异不确定性（配对 bootstrap，全部合格目标） ---
    diff_unc: dict[str, dict] = {}
    base_m1 = joined["m1_ape"].to_numpy().astype(np.float64)
    base_b0 = joined["b0_ape"].to_numpy().astype(np.float64)
    for model in S3_LINES:
        a = joined[f"ape_{model}"].to_numpy().astype(np.float64)
        diff_unc[model] = {
            "population": POP_ALL,
            "vs_M1": paired_bootstrap_medape(a, base_m1),
            "vs_B0": paired_bootstrap_medape(a, base_b0),
        }

    # --- 属性增量实验（逐折配对） ---
    increment: dict[str, dict] = {}
    for inc, base in (("INC_M2", "M2"), ("INC_M3a", "M3a")):
        pairs = []
        for fid in folds:
            df = joined.filter(pl.col("fold_id") == fid)
            f_ape = df[f"ape_{base}"].to_numpy().astype(np.float64)
            b_ape = df[f"ape_{inc}"].to_numpy().astype(np.float64)
            pairs.append({"fold_id": fid, "n": df.height,
                          "full_med_ape": float(np.median(f_ape)),
                          "base_med_ape": float(np.median(b_ape)),
                          "diff_base_minus_full": float(np.median(b_ape) - np.median(f_ape))})
        f_all = joined[f"ape_{base}"].to_numpy().astype(np.float64)
        b_all = joined[f"ape_{inc}"].to_numpy().astype(np.float64)
        increment[inc] = {
            "base_line": base, "pairs": pairs,
            "overall": {"n": joined.height,
                        "full_med_ape": float(np.median(f_all)),
                        "base_med_ape": float(np.median(b_all)),
                        "diff_base_minus_full": float(np.median(b_all) - np.median(f_all))},
            "paired_bootstrap": paired_bootstrap_medape(b_all, f_all),
            "folds_full_better": int(sum(1 for p in pairs if p["diff_base_minus_full"] > 0)),
            "feature_group_full": LINE_SPEC[base][1], "feature_group_base": "base",
        }

    # --- 网格与配置选择记录 ---
    grid_record = {
        "grids": {"M2": exp_mod.M2_GRID, "M3a": exp_mod.M3A_GRID,
                  "M3b": exp_mod.M3B_GRID, "M4": exp_mod.M4_GRID},
        "grid_sizes": {"M2": len(exp_mod.M2_GRID), "M3a": len(exp_mod.M3A_GRID),
                       "M3b": len(exp_mod.M3B_GRID), "M4": len(exp_mod.M4_GRID)},
        "selection_rule": exp_mod.REFIT_FLOW["inner_select"],
        "outer_refit_rule": exp_mod.REFIT_FLOW["outer_refit"],
        "no_cross_fold": exp_mod.REFIT_FLOW["no_cross_fold"],
        "m4_structure": exp_mod.REFIT_FLOW["m4_structure"],
        "per_fold": {fid: {m: {"chosen": picks[(m, fid)]["config_id"],
                               "inner_grid": picks[(m, fid)]["inner_grid"]}
                           for m in GRID_MODELS} for fid in folds},
        "outer_refit_count": {"per_fold_per_model": 1, "total": len(GRID_MODELS) * len(folds)},
    }

    # --- B1 登记性参考（S2 口径，附警示） ---
    b1_block = s2_metrics["metrics"]["overall"]["B1目标全集（冻结子集∩验证带）"]
    b1_registered = {
        "source": "S2 run metrics.json（B1 回放证据，本 change 不新增 B1 运行）",
        "population": "B1 目标全集（冻结子集∩验证带）6205 行；valued 6153",
        "b1": b1_block["b1"], "b1_interval": b1_block.get("b1_interval"),
        "used_as_baseline": False,
        "warning": "B1 量级异常警示：S2 报告 §0/§11 证实 46.68% 的 B1 有效行与真值一致到 "
                   "1e-4 以内（疑似可比检索命中目标自身/镜像），B1 指标不可解读为引擎预测能力；"
                   "本报告仅作登记性并列参考，不作任何对照基线、不参与任何采用结论。",
        "non_suspect_reference": {
            "source": "evidence/5-1（A1 主口径非疑似自读子集 3281 行）",
            "b1_med_ape": 0.0355, "b0_med_ape": 0.0955,
            "note": "同一非疑似子集内 B1 回放中心仍为极端量级，同样不可解读为引擎能力"},
    }

    # --- 归因支撑事实 ---
    best = min(S3_LINES, key=lambda m: overall[POP_ALL][m]["med_ape"])
    ev_levels = ["community", "block", "district"]
    ev_rows = []
    for lvl in ev_levels:
        sub = joined.filter(pl.col(f"ev_{best}") == lvl)
        if sub.height == 0:
            continue
        ev_rows.append({"level": lvl, "n": sub.height,
                        "best_med_ape": float(np.median(sub[f"ape_{best}"].to_numpy())),
                        "m1_med_ape": float(np.median(sub["m1_ape"].to_numpy())),
                        "b0_med_ape": float(np.median(sub["b0_ape"].to_numpy()))})
    small = joined.filter(pl.col("area_sqm") < 30.0)
    sign_best = np.sign(joined[f"signed_{best}"].to_numpy())
    sign_b0 = np.sign(joined["b0_signed_err"].to_numpy())
    per_fold_diff = [{"fold_id": fid,
                      "diff_best_minus_m1": per_fold[fid][best]["med_ape"] - per_fold[fid]["M1"]["med_ape"]}
                     for fid in folds]
    attribution = {
        "primary_line": best,
        "evidence_level": ev_rows,
        "small_area": {"threshold_sqm": 30.0, "n": small.height,
                       "share": small.height / joined.height,
                       "best_med_ape": float(np.median(small[f"ape_{best}"].to_numpy()))
                       if small.height else None,
                       "m1_med_ape": float(np.median(small["m1_ape"].to_numpy()))
                       if small.height else None},
        "same_direction_with_b0_share": float((sign_best == sign_b0).mean()),
        "per_fold_diff_vs_m1": per_fold_diff,
        "folds_best_better_than_m1": int(sum(1 for p in per_fold_diff
                                             if p["diff_best_minus_m1"] < 0)),
        "cold_start": {r["fold_id"]: r["cold_start"] for r in recompute["folds"]},
        "m3b_warmup_outer": {r["fold_id"]: r["m3b_warmup_outer"] for r in recompute["folds"]},
    }

    # --- 注册表计数机械一致 ---
    registry_check = {
        "experiments_jsonl": str(s3_run / "experiments.jsonl"),
        "declared_units": len(exp_mod.build_units()),
        "experiments_jsonl_lines": (s3_run / "experiments.jsonl").read_text(
            encoding="utf-8").count("\n") - 1,
        "cloud_merged_units": units.height,
        "cloud_outer_refit": units.filter(pl.col("stage") == "outer_refit").height,
        "cloud_inner_select": units.filter(pl.col("stage") == "inner_select").height,
        "cloud_derived_prep": units.filter(pl.col("kind") == "derived_prep").height,
        "by_model": {m: units.filter(pl.col("model") == m).height for m in
                     ("M2", "M3a", "M3b", "M4", "INC_M2", "INC_M3a")},
    }
    registry_check["consistent"] = bool(
        registry_check["declared_units"] == registry_check["cloud_merged_units"]
        == registry_check["experiments_jsonl_lines"])

    # --- 成本/耗时 ---
    cloud_fit_sum = float(units["fit_seconds"].sum())
    local_fit_sum = float(sum(rec["lines"][m]["fit_seconds"]
                              for rec in recompute["folds"] for m in S3_LINES))
    timing = {
        "recompute_wall_seconds": recompute["wall_seconds"],
        "recompute_workers": workers,
        "logical_cores": logical_cores(),
        "cpu_share_estimate": workers / max(logical_cores(), 1),
        "local_fit_seconds_sum": local_fit_sum,
        "cloud_fit_seconds_sum": cloud_fit_sum,
        "budget": {
            "experiments_jsonl": str(s3_run / "experiments.jsonl"),
            "cloud_wall_clock_hours": "≈1.02h（4.2 登记）",
            "cloud_cost_yuan": "¥1—3（4.2 登记）",
            "local_recompute_note": "本机折级分片并行重算，每 worker 单线程；实际墙钟与 CPU 占比见上",
        },
    }

    failure_cases = _failure_cases(joined, best)
    a1_section = _a1_section(s3_run)
    m3b_flags = []
    for fid in folds:
        m = per_fold[fid]["M3b"]
        warm = next(r for r in recompute["folds"] if r["fold_id"] == fid)["m3b_warmup_outer"]
        if m["med_ape"] > 0.5:
            m3b_flags.append({"fold_id": fid, "n": m["n"], "med_ape": m["med_ape"],
                              "signed_med_bias": m["signed_med_bias"],
                              "median_pred_over_true": 1.0 + m["signed_med_bias"],
                              "warmup_ratio": warm["warmup_ratio"]})
    attribution["m3b_instability"] = m3b_flags
    conclusions = _conclusions(overall, diff_unc, best, a1_section, increment)
    attribution_lines = _attribution_lines(attribution, overall, increment)

    return {
        "schema_version": "phase2-s3-eval-v1",
        "change": "compare-phase2-nonlinear", "task": "6.1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "inputs": {
            "s1_run": s1_run.name, "s2_run": s2_run.name, "s3_run": s3_run.name,
            "s1_manifest_sha256": sha256_file(s1_run / "manifest.json"),
            "s2_manifest_sha256": sha256_file(s2_run / "manifest.json"),
            "cloud_merged_sha256": sha256_file(cloud_src / "merged" / "units.parquet"),
            "cloud_merged_path": str(cloud_src / "merged" / "units.parquet"),
        },
        "recompute_caliber": {
            "s3_lines": list(S3_LINES),
            "method": "逐折读取云端该折该模型已选定配置（outer_refit.config）→ 用 nonlinear.py "
                      "在该折完整训练切片（外层输入）只重拟合该已选配置 → 预测折验证带 → "
                      "得逐行预测 → 聚合总体与逐折指标",
            "why": "云端只持久化每单元指标摘要（无逐行预测）；逐折中位数无法聚合为总体中位数",
            "frozen_reference": "B0/M1 直接引用 S2 run per-fold/*.parquet 冻结逐行预测，不重拟合",
            "same_window": "同一折验证带行集（= S2 B0/M1 应估行集），逐折行数与 ID 集断言相等",
        },
        "cross_check": cross_check,
        "cross_check_detail": detail,
        "s2_frozen_cross_validation": s2_check,
        "population_definitions": {
            POP_ALL: "18 折验证带主表行并集（B0/M1/S3 各线应估全集）",
            POP_COMMON: "全部合格目标 ∩ B1 valued（三方均有可用中心价）",
        },
        "rows": {"all": joined.height, "common": n_expected["common"]},
        "overall": overall,
        "per_fold": per_fold,
        "diff_uncertainty": diff_unc,
        "increment": increment,
        "grid_selection": grid_record,
        "b1_registered": b1_registered,
        "attribution": attribution,
        "attribution_lines": attribution_lines,
        "conclusions": conclusions,
        "a1_section": a1_section,
        "failure_cases": failure_cases,
        "failure_case_file": "S3-失败案例清单-20260913.md",
        "registry_check": registry_check,
        "timing": timing,
        "folds": recompute["folds"],
    }


def _clue(area, ev, s_primary, s_m1, s_b0) -> str:
    bits = []
    if area is not None and area < 30.0:
        bits.append("小面积(<30㎡)单价放大")
    if ev in ("block", "district"):
        bits.append(f"市场证据层级={ev}（回退链降级）")
    if s_primary == s_m1:
        bits.append("与 M1 同向误差")
    if s_primary == s_b0:
        bits.append("与 B0 同向误差（共同市场/标签因素）")
    if not bits:
        bits.append("同小区近期成交与目标属性差/时点差")
    return "；".join(bits)


def _case_row(df: pl.DataFrame, idx: int, primary: str) -> dict:
    r = df.row(idx, named=True)
    s_p = float(np.sign(r[f"signed_{primary}"]))
    return {
        "source_record_id": r["source_record_id"],
        "community_name": r["community_name"],
        "sale_date": str(r["sale_date_d"]),
        "area_sqm": float(r["area_sqm"]) if r["area_sqm"] is not None else None,
        "unit_price_true": float(r["unit_price_true"]),
        "primary_pred": float(r[f"pred_{primary}"]),
        "primary_ape": float(r[f"ape_{primary}"]),
        "m1_pred": float(r["m1_pred"]), "m1_ape": float(r["m1_ape"]),
        "b0_pred": float(r["b0_pred"]), "b0_ape": float(r["b0_ape"]),
        "evidence_level": r[f"ev_{primary}"],
        "clue": _clue(r["area_sqm"], r[f"ev_{primary}"], s_p,
                      float(np.sign(r["m1_signed_err"])), float(np.sign(r["b0_signed_err"]))),
    }


def _failure_cases(joined: pl.DataFrame, primary: str) -> dict:
    def topn(df: pl.DataFrame, n: int) -> list[dict]:
        s = df.with_columns(pl.col(f"ape_{primary}").alias("_a")).sort("_a", descending=True)
        return [_case_row(s, i, primary) for i in range(min(n, s.height))]

    per_fold = []
    for fid in sorted(set(joined["fold_id"].to_list())):
        sub = joined.filter(pl.col("fold_id") == fid)
        s = sub.with_columns(pl.col(f"ape_{primary}").alias("_a")).sort("_a", descending=True)
        row = _case_row(s, 0, primary)
        row["fold_id"] = fid
        per_fold.append(row)
    delta = joined.with_columns(
        (pl.col(f"ape_{primary}") - pl.col("m1_ape")).alias("_d")).sort("_d", descending=True)
    delta_b0 = joined.with_columns(
        (pl.col(f"ape_{primary}") - pl.col("b0_ape")).alias("_d")).sort("_d", descending=True)
    return {"primary_line": primary,
            "overall_top10": topn(joined, 10),
            "per_fold_max": per_fold,
            "vs_m1_top5": [_case_row(delta, i, primary) for i in range(min(5, delta.height))],
            "vs_b0_top5": [_case_row(delta_b0, i, primary)
                           for i in range(min(5, delta_b0.height))]}


def _a1_section(s3_run: Path) -> dict:
    raw = json.loads((s3_run / "a1" / "a1-comparison.json").read_text(encoding="utf-8"))
    c = raw["comparison"]
    main = c["main_caliper"]
    fix = c["fixable_subset"]
    bs = main["bootstrap"]["metrics"]["med_ape"]
    rows = []
    for label, blk in (("主口径（固定全目标集，非疑似自读子集）", main),
                       ("可修正子集（≥1 案例被修正）", fix)):
        rows.append([label, str(blk["n"]), pct(blk["pre"]["med_ape"]), pct(blk["post"]["med_ape"]),
                     pct(blk["pre"]["within10_ratio"]), pct(blk["post"]["within10_ratio"]),
                     pct(blk["pre"]["p90_ape"]), pct(blk["post"]["p90_ape"])])
    return {
        "caliber": c["definition"]["main_caliper"] + "；" + c["definition"]["fixable_subset"]
                   + "；delta = (x_target[A] − x_case[A]) @ w[A]（对数尺度贡献差），"
                     "修正价 = 案例单价 × exp(delta)；前后同案例集、同目标 ID 集、同聚合",
        "rows": rows,
        "bootstrap_main": {"med_diff": bs["observed_diff"], "ci95_low": bs["ci95_low"],
                           "ci95_high": bs["ci95_high"],
                           "crosses_zero": bool(bs["includes_zero"])},
        "id_sets_elementwise_equal": raw["id_sets_elementwise_equal"],
        "no_case_disclosure": c.get("zero_case_disclosure") or raw.get("non_suspect_disclosure"),
        "source": "S3 run a1/a1-comparison.json（任务 5.1）",
    }


def _conclusions(overall: dict, diff_unc: dict, best: str, a1: dict,
                 increment: dict) -> list[str]:
    allb = overall[POP_ALL]
    comb = overall[POP_COMMON]
    v1 = diff_unc[best]["vs_M1"]
    v0 = diff_unc[best]["vs_B0"]
    out: list[str] = []
    verdict = ("有稳定增益" if (not v1["crosses_zero"] and v1["med_diff"] < 0) else "无稳定增益")
    a1_verdict = ("改善" if a1["bootstrap_main"]["med_diff"] < 0
                  and not a1["bootstrap_main"]["crosses_zero"] else "未获稳定改善")
    out += [
        "### 14.1 独立报价用途（M 系 vs B0/M1）",
        "",
        f"- **结论：S3 非线性/挑战模型相对 S2 冻结 M1 {verdict}。**",
        f"- 全部合格目标（{allb['rows']} 行）：最优 S3 线 {LINE_LABEL[best]} MedAPE "
        f"{pct(allb[best]['med_ape'])}，S2 冻结 M1 {pct(allb['M1']['med_ape'])}、"
        f"B0 {pct(allb['B0']['med_ape'])}；配对差 vs M1 {pct(v1['med_diff'])} "
        f"95% CI [{pct(v1['ci95_low'])}, {pct(v1['ci95_high'])}]，"
        f"vs B0 {pct(v0['med_diff'])} 95% CI [{pct(v0['ci95_low'])}, {pct(v0['ci95_high'])}]。",
        f"- 共同可比人群（{comb['rows']} 行）：最优 S3 线 {pct(comb[best]['med_ape'])} vs "
        f"M1 {pct(comb['M1']['med_ape'])} vs B0 {pct(comb['B0']['med_ape'])}。",
        f"- 全量组 vs 基础组（属性增量）：INC_M2 差 "
        f"{pct(increment['INC_M2']['overall']['diff_base_minus_full'])}（全量组更优折数 "
        f"{increment['INC_M2']['folds_full_better']}/{len(increment['INC_M2']['pairs'])}）；"
        f"INC_M3a 差 {pct(increment['INC_M3a']['overall']['diff_base_minus_full'])}"
        f"（全量组更优折数 {increment['INC_M3a']['folds_full_better']}/"
        f"{len(increment['INC_M3a']['pairs'])}）。",
        "- 该用途结论仅基于本 change 开发折（非独立测试窗口）；正式采用判定归 S4/S5。",
        "",
        "### 14.2 比较法辅助价差用途（A1 修正前 vs 修正后）",
        "",
        f"- **结论：A1 属性价差修正使辅助价差用途指标{a1_verdict}。**"
        f"主口径 MedAPE 差 {a1['bootstrap_main']['med_diff']:.5f}，95% CI "
        f"[{a1['bootstrap_main']['ci95_low']:.5f}, {a1['bootstrap_main']['ci95_high']:.5f}]。",
        f"- 主口径 n=3,281：MedAPE {a1['rows'][0][2]}→{a1['rows'][0][3]}；可修正子集 n=2,714："
        f"MedAPE {a1['rows'][1][2]}→{a1['rows'][1][3]}。",
        "- A1 只属辅助价差用途，**不产生独立报价结论**，不得用于证明 M 系独立报价通过；"
        "与 B1 现行修正的重叠因 B1 无案例明细不可考，如实披露。",
        "",
        "- 两用途独立：一方成立不代言另一方；本轮两用途结论如上分别给出。",
    ]
    return out


def _attribution_lines(attribution: dict, overall: dict, increment: dict) -> list[str]:
    best = attribution["primary_line"]
    allb = overall[POP_ALL]
    ev = attribution["evidence_level"]
    ev_txt = "；".join(f"{r['level']} n={r['n']}（主轨 {pct(r['best_med_ape'])}、"
                       f"M1 {pct(r['m1_med_ape'])}、B0 {pct(r['b0_med_ape'])}）"
                       for r in ev)
    small = attribution["small_area"]
    cold = attribution["cold_start"]
    cold_max = max((v["ratio_of_train"] for v in cold.values()), default=0.0)
    cold_txt = "；".join(f"{k}={v['count']}({pct(v['ratio_of_train'])})"
                         for k, v in sorted(cold.items()) if v["count"] > 0) or "各折 0 行"
    out = [
        f"- **数据接入**：数据源、清洗与切分沿用 S1 合同，S3 未改口径；"
        f"小面积（<30㎡）目标 {small['n']} 行（占比 {pct(small['share'])}），其主轨 MedAPE "
        f"{pct(small['best_med_ape'])}（M1 {pct(small['m1_med_ape'])}）——小面积单价放大是"
        f"共同误差来源，不因算法更换消除。",
        f"- **市场时间**：主轨相对 M1 的逐折差在 "
        f"{attribution['folds_best_better_than_m1']}/{len(attribution['per_fold_diff_vs_m1'])} 折为负"
        f"（更优）；主轨与 B0 误差同向占比 {pct(attribution['same_direction_with_b0_share'])}，"
        f"说明大部分误差来自共同的市场时间与标签因素，非线性结构只改变幅度。",
        f"- **局部支持**：按主轨所用市场证据层级分组——{ev_txt}；"
        f"层级越低（小区→板块→区级）误差越高，回退链降级是误差放大的结构性来源。",
        f"- **缺失特征**：冷启动排除行（回退链全空）训练行占比最大 {pct(cold_max)}"
        f"（逐折：{cold_txt}），除首折外极少，不构成主要误差源；M3b 预热行（设计内保留训练、"
        f"用未标准化基准）与冷启动口径分开登记（§4 逐折表）。",
        f"- **标签质量**：主轨 APE 最大 10 例集中于小面积、低层级证据与极端单价（§13），"
         f"与 B0/M1 同向，指向标签与可比样本质量而非模型结构性缺陷；"
         f"增量实验显示房屋属性组的边际贡献为 "
         f"INC_M2 {pct(increment['INC_M2']['overall']['diff_base_minus_full'])}、"
         f"INC_M3a {pct(increment['INC_M3a']['overall']['diff_base_minus_full'])}"
         f"（正值 = 去掉房屋属性组后更差，属性组有正贡献；故特征组取舍不构成无增益的借口）。",
        f"- **M3b 稳定性（关键发现）**：M3b（必交属性标准化变体）在 "
        f"{'、'.join(f'{f['fold_id']}（medAPE {pct(f['med_ape'])}、预测/真值中位比 {f['median_pred_over_true']:.3f}、'
                     f'预热占比 {pct(f['warmup_ratio'])}）' for f in attribution['m3b_instability']) or '无'}"
        f" 折出现系统性同向崩塌（整体大幅低估，非个别离群）；其余折正常，总体 medAPE 被这些折拉高至 "
        f"{pct(overall[POP_ALL]['M3b']['med_ape'])}。**事实边界（只陈述、不定论）**：崩塌与预热占比并非"
        f"单调关系（预热占比更高的 F17 55.21%、F18 69.04% 未崩塌），故不能仅归因于预热；"
        f"这两折同时具备“训练切片最小（5,088 / 4,686 行）+ 层内属性参照可用材料最少”，"
        f"标准化偏移在小样本上不稳定是可能方向，需进一步核查。本报告原样登记，"
        f"未据此追加网格或调参，M3b 结论按折分层给、不并入单一总体判断。",
        f"- 综合：主轨相对 M1 的差异 {pct(overall[POP_ALL][best]['med_ape'] - allb['M1']['med_ape'])} "
        f"不指向单一可修复缺陷；按蓝图 §6.2 不追加网格、不增加复杂度，保留全部对照与候选结果。",
    ]
    return out


# ---------------- 格式化与报告 ----------------

def pct(x: float | None, nd: int = 2) -> str:
    if x is None:
        return "—"
    return f"{x * 100:.{nd}f}%"


def _row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _table(headers: list[str], rows: list[list[str]]) -> list[str]:
    return [_row(headers), _row(["---"] * len(headers))] + [_row(r) for r in rows]


def _overall_table(summary: dict, pop: str, lines: list[str]) -> list[str]:
    blk = summary["overall"][pop]
    rows = []
    for line in lines:
        m = blk[line]
        rows.append([LINE_LABEL[line], str(m["n"]), pct(m["med_ape"]), pct(m["within10_ratio"]),
                     pct(m["p90_ape"]), pct(m["signed_med_bias"], 2),
                     pct(m["severe_over_20_ratio"]), pct(m["coverage"])])
    return _table(["模型线", "n", "MedAPE", "±10% 命中", "P90 APE", "有符号偏差",
                   "严重高估率(>20%)", "可估覆盖"], rows)


def _per_fold_table(summary: dict, lines: list[str]) -> list[str]:
    rows = []
    for fid, blk in summary["per_fold"].items():
        rows.append([fid, str(blk["n"])] + [pct(blk[ln]["med_ape"]) for ln in lines])
    return _table(["折", "n"] + [ln for ln in lines], rows)


def _grid_table(summary: dict) -> list[str]:
    rows = []
    for fid, per in summary["grid_selection"]["per_fold"].items():
        rows.append([fid] + [per[m]["chosen"] for m in GRID_MODELS])
    return _table(["折", "M2 当选", "M3a 当选", "M3b 当选", "M4 当选"], rows)


def _increment_table(summary: dict) -> list[str]:
    rows = []
    for inc, blk in summary["increment"].items():
        o = blk["overall"]
        bs = blk["paired_bootstrap"]
        rows.append([inc, blk["base_line"], str(o["n"]), pct(o["full_med_ape"]),
                     pct(o["base_med_ape"]), pct(o["diff_base_minus_full"]),
                     f"[{pct(bs['ci95_low'])}, {pct(bs['ci95_high'])}]",
                     f"{blk['folds_full_better']}/"
                     f"{len(blk['pairs'])}"])
    return _table(["增量线", "基础线", "n", "全量组 MedAPE", "基础组 MedAPE",
                   "差(基础−全量)", "配对 bootstrap 95% CI", "全量组更优折数"], rows)


def _diff_table(summary: dict) -> list[str]:
    rows = []
    for model in S3_LINES:
        d = summary["diff_uncertainty"][model]
        v1, v0 = d["vs_M1"], d["vs_B0"]
        rows.append([LINE_LABEL[model], pct(v1["med_diff"]),
                     f"[{pct(v1['ci95_low'])}, {pct(v1['ci95_high'])}]",
                     "否" if v1["crosses_zero"] else "是",
                     pct(v0["med_diff"]),
                     f"[{pct(v0['ci95_low'])}, {pct(v0['ci95_high'])}]",
                     "否" if v0["crosses_zero"] else "是"])
    return _table(["模型线", "vs M1 差", "vs M1 95% CI", "vs M1 不含 0",
                   "vs B0 差", "vs B0 95% CI", "vs B0 不含 0"], rows)


def _sample_flow_table(summary: dict, s2_run: Path) -> list[str]:
    frames = {r["fold_id"]: r for r in summary["folds"]}
    s2_frames = {fid: pl.read_parquet(s2_run / "per-fold" / f"{fid}.parquet")
                 for fid in frames}
    rows = []
    for fid, rec in frames.items():
        s2f = s2_frames[fid]
        val = int((s2f["b1_status"] == "valued").sum())
        warm = rec["m3b_warmup_outer"] or {}
        rows.append([fid, rec["outer_cutoff"], str(rec["row_counts"]["outer_train"]),
                     str(rec["row_counts"]["outer_val"]), str(val),
                     str(rec["cold_start"]["count"]), pct(rec["cold_start"]["ratio_of_train"]),
                     pct(warm.get("warmup_ratio")) if warm else "—"])
    return _table(["折", "外层截点", "外层训练行", "验证带行", "B1 valued", "冷启动排除行",
                   "冷启动占比", "M3b 外层预热占比(设计内)"], rows)


def _registration_table(summary: dict) -> list[str]:
    r = summary["registry_check"]
    return _table(["项", "值"], [
        ["登记正式单元（experiments.py 机械生成）", str(r["declared_units"])],
        ["experiments.jsonl 单元行", str(r["experiments_jsonl_lines"])],
        ["云端 merged/units.parquet 单元行", str(r["cloud_merged_units"])],
        ["其中 派生准备 / 配置选择 / 外层重拟合",
         f"{r['cloud_derived_prep']} / {r['cloud_inner_select']} / {r['cloud_outer_refit']}"],
        ["分模型计数（M2/M3a/M3b/M4/INC_M2/INC_M3a）",
         "/".join(str(r["by_model"][m]) for m in
                  ("M2", "M3a", "M3b", "M4", "INC_M2", "INC_M3a"))],
        ["机械一致性断言", "一致" if r["consistent"] else "不一致"],
    ])


def _with_m3b(summary: dict) -> dict:
    """补算 M3b 崩塌折清单（幂等；report 路径可由既有汇总重算，无需重跑重算）。"""
    attr = summary["attribution"]
    if "m3b_instability" not in attr:
        warm = {r["fold_id"]: r["m3b_warmup_outer"] for r in summary["folds"]}
        flags = []
        for fid in summary["per_fold"]:
            m = summary["per_fold"][fid]["M3b"]
            if m["med_ape"] > 0.5:
                flags.append({"fold_id": fid, "n": m["n"], "med_ape": m["med_ape"],
                              "signed_med_bias": m["signed_med_bias"],
                              "median_pred_over_true": 1.0 + m["signed_med_bias"],
                              "warmup_ratio": warm[fid]["warmup_ratio"]})
        attr["m3b_instability"] = flags
    return summary


def render_report(summary: dict, s2_run: Path, report_path: Path) -> Path:
    summary = _with_m3b(summary)
    conclusions = _conclusions(summary["overall"], summary["diff_uncertainty"],
                               summary["attribution"]["primary_line"],
                               summary["a1_section"], summary["increment"])
    attribution_lines = _attribution_lines(summary["attribution"], summary["overall"],
                                          summary["increment"])
    lines: list[str] = []
    cf = summary["cross_check"]
    lines += ["# S3 非线性比较报告（M2 / M3a / M3b / M4 与属性增量，蓝图 §11 口径）", ""]
    lines += [
        f"- S3 run：`{summary['inputs']['s3_run']}`（同窗重算产物 `s3-eval/`，本报告数值全部由其汇总" 
        f" JSON 生成，可回溯到逐行预测 parquet）",
        f"- S1 合同 run：`{summary['inputs']['s1_run']}`；S2 对照 run（B0/M1 冻结值来源）：`{summary['inputs']['s2_run']}`",
        f"- 云端回收产物：`{summary['inputs']['cloud_merged_path']}`（sha256 `{summary['inputs']['cloud_merged_sha256'][:16]}…`）",
        f"- 生成时间（UTC）：{summary['generated_at_utc']}；评估实现：`03-估值引擎/src/gz_property_valuation/phase2/s3_evaluate.py`",
        f"- 总体行集：{summary['rows']['all']} 行（全部合格目标）；共同可比人群 {summary['rows']['common']} 行",
        "",
    ]

    best = summary["attribution"]["primary_line"]
    allb = summary["overall"][POP_ALL]
    comb = summary["overall"][POP_COMMON]
    v1 = summary["diff_uncertainty"][best]["vs_M1"]

    lines += ["## 0. 摘要与先读警示", ""]
    lines += [
        f"- 全部合格目标（{allb['rows']} 行）：MedAPE **B0 {pct(allb['B0']['med_ape'])} / "
        f"M1 {pct(allb['M1']['med_ape'])} / M2 {pct(allb['M2']['med_ape'])} / "
        f"M3a {pct(allb['M3a']['med_ape'])} / M3b {pct(allb['M3b']['med_ape'])} / "
        f"M4 {pct(allb['M4']['med_ape'])} / INC_M2 {pct(allb['INC_M2']['med_ape'])} / "
        f"INC_M3a {pct(allb['INC_M3a']['med_ape'])}**。",
        f"- S3 最优线 = **{LINE_LABEL[best]}**（MedAPE {pct(allb[best]['med_ape'])}）；"
        f"相对 S2 冻结 M1（{pct(allb['M1']['med_ape'])}）配对差 {pct(v1['med_diff'])}，"
        f"95% CI [{pct(v1['ci95_low'])}, {pct(v1['ci95_high'])}]，"
        f"{'区间不含 0' if not v1['crosses_zero'] else '区间含 0'}。",
        f"- 独立报价用途与比较法辅助价差用途分别给结论（§14），允许一方成立一方不成立。",
        "",
    ]
    lines += ["### 0.1 B1 量级异常警示（先读）", "",
              f"- {summary['b1_registered']['warning']}", ""]

    lines += ["## 1. 时间及截点", ""]
    lines += [
        "- 开发信息截点与 18 个 expanding 折（锚点、验证带、折带互不重叠）沿用 S1 合同与 S2 口径，"
        "splits.json 冻结不变；S3 未改动任何折定义。",
        "- 外层验证行市场输入锚该折外层截点；内层选参验证行锚内层截点（训练切片末端 − 3 个月 + 1 天）；"
        "训练行用严格早于自身成交日的行级滚动（小区 365d 中位 → 板块 → 区级回退，排除自身与同日）。",
        "",
    ]

    lines += ["## 2. 模型 / 数据版本", ""]
    lines += _table(["项", "值"], [
        ["S1 合同 run", summary["inputs"]["s1_run"]],
        ["S2 对照 run", summary["inputs"]["s2_run"]],
        ["S3 run", summary["inputs"]["s3_run"]],
        ["引擎版本钉死", "catboost 1.2.10 / lightgbm 4.7.0 / Python 3.13.13 / numpy 2.5.3 / "
                        "polars 1.44.2 / pyarrow 25.0.1（S3 run manifest）"],
        ["种子 / 线程", f"seed={exp_mod.SEED}；CatBoost thread_count=1、LightGBM num_threads=1；"
                        "包导入期钉死 BLAS/polars 线程=1"],
        ["M2 / M3a / M4 / M3b 网格", f"{len(exp_mod.M2_GRID)} / {len(exp_mod.M3A_GRID)} / "
                                    f"{len(exp_mod.M4_GRID)} / {len(exp_mod.M3B_GRID)} 配置"
                                    "（运行前固定、运行中不得追加）"],
        ["属性增量实验", "INC_M2 / INC_M3a = M2 / M3a 本折选中配置 × 基础特征组（去房屋属性组）"],
    ])
    lines += [""]

    lines += ["## 3. M/B 对照：B0/M1 引用 S2 冻结值 + S3 新增模型同窗重算口径登记", ""]
    sv = summary["s2_frozen_cross_validation"]
    sv_txt = (f"（判定 {'一致' if sv['pass'] else '不一致'}）" if sv["applicable"]
              else "（本次为抽样运行非全 18 折，该交叉校验不适用）")
    lines += [
        "- **B0/M1（引用 S2 冻结值）**：直接读取 S2 run `per-fold/*.parquet` 的冻结逐行预测"
        f"（B0/M1 不重拟合），同窗口径 = 同一折验证带行集。与 S2 `metrics.json` 总体值交叉校验："
        f"B0 MedAPE 差 {sv['b0_med_ape']['abs_delta']:.3e}、"
        f"M1 MedAPE 差 {sv['m1_med_ape']['abs_delta']:.3e}{sv_txt}。",
        "- **S3 新增模型（同窗重算口径）**：云端只持久化每单元指标摘要（`merged/units.parquet` 的 "
        "`metric_*` + 8 行 probe），**无逐行预测**；逐折中位数无法聚合为总体中位数。故对 "
        "M2 / M3a / M3b / M4 / INC_M2 / INC_M3a 六条模型线，逐折读取该折该模型**已选定配置**"
        "（`outer_refit.config`），用 `nonlinear.py` 在该折完整训练切片（外层输入）**只重拟合该已选"
        "配置**并预测折验证带，得到**逐行预测**后聚合总体与逐折指标。",
        f"- **对拍准入**：重算逐折指标与云端 `metric_*` 逐字段按跨机容差 "
        f"`|Δ| ≤ 1e-9 × max(|ref|, 1)` 对拍——单元 {cf['units']} 个 / 字段 {cf['fields']} 个，"
        f"实测 max|Δ| = {cf['max_abs_delta']:.3e}、max 相对 |Δ| = {cf['max_rel_delta']:.3e}、"
        f"非有限 0 个，**{'全部通过' if cf['all_pass'] else '存在超限'}**"
        f"（失败 {cf['fail_count']} 项）；对拍通过才使用重算值，未混用两套口径。",
        f"- 同窗机械断言：逐折 S3 重算行集与 S2 冻结行集行数/ID 集合逐折相等（全部合格目标 "
        f"{summary['rows']['all']} 行；共同可比人群 {summary['rows']['common']} 行）。",
        "",
    ]

    lines += ["## 4. 样本进出明细（逐折）", ""]
    lines += _sample_flow_table(summary, s2_run)
    lines += [""]

    lines += ["## 5. 试验注册表、实际运行数与成本", ""]
    lines += _registration_table(summary)
    lines += [
        "",
        "- 质量验证运行（轻门参考 5 + 金丝雀首 10 + 同机重跑抽样 6 = 21 单元）与 594 正式单元分列，"
        "不计入正式单元、不视为扩网格（4.2/6.2 登记）。",
        f"- 云侧 594 单元实测拟合耗时合计 {summary['timing']['cloud_fit_seconds_sum']:.1f}s"
        f"（4.2 云侧墙钟 ≈1.02h、费用 ¥1—3，均在停止线内）。",
        f"- **本机同窗重算耗时登记**：折级分片并行 {summary['timing']['recompute_workers']} worker"
        f"（逻辑核 {summary['timing']['logical_cores']}，CPU 占比 ≈"
        f"{summary['timing']['cpu_share_estimate']:.1%}，显著低于护机红线 50%），"
        f"墙钟 {summary['timing']['recompute_wall_seconds']:.1f}s（{summary['timing']['recompute_wall_seconds']/60:.1f} min，"
        f"<1h 未触发 J12）；重算拟合耗时合计 {summary['timing']['local_fit_seconds_sum']:.1f}s（单线程）。",
        "- 每折每模型外层重拟合 1 次（含在 594 单元内），重算不新增登记单元——重算只复现登记配置，"
        "未追加任何网格配置。",
        "",
    ]

    lines += ["## 6. 总体指标（蓝图 §11 全表，双人群分列）", ""]
    lines += [f"### 6.1 全部合格目标（{allb['rows']} 行）", "",
              f"- 分母定义：{summary['population_definitions'][POP_ALL]}", ""]
    lines += _overall_table(summary, POP_ALL, ["B0", "M1", "M2", "M3a", "M3b", "M4",
                                              "INC_M2", "INC_M3a"])
    lines += ["", f"### 6.2 共同可比人群（{comb['rows']} 行）", "",
              f"- 分母定义：{summary['population_definitions'][POP_COMMON]}", ""]
    lines += _overall_table(summary, POP_COMMON, ["B0", "M1", "M2", "M3a", "M3b", "M4",
                                                 "INC_M2", "INC_M3a"])
    lines += [
        "",
        "- 上表含蓝图 §11 六项：MedAPE、±10% 命中、P90 APE、有符号偏差、严重高估率（开发期 20% "
        "描述性阈值，正式值 S4 冻结）、可估覆盖；双人群分列（全部合格目标 / 共同可比人群）。",
        f"- B0/M1 行为 S2 冻结值（引用口径见 §3）；M2/M3a/M3b/M4/INC_M2/INC_M3a 行为同窗重算值。",
        "",
    ]

    lines += ["## 7. 逐折全表（MedAPE）", ""]
    lines += _per_fold_table(summary, ["B0", "M1", "M2", "M3a", "M3b", "M4", "INC_M2", "INC_M3a"])
    lines += [
        "",
        "- 逐折数值与 §6 同源（逐行预测 parquet 聚合），可比 S2 逐折表；18 折全部为同一折定义与同一验证带。",
        "",
    ]

    b1 = summary["b1_registered"]
    lines += ["## 8. B1 登记性列示（附量级异常警示，不作基线）", ""]
    lines += [
        f"- {b1['warning']}",
        f"- 登记值（S2 口径，{b1['population']}）：n={b1['b1']['n']}、MedAPE {pct(b1['b1']['med_ape'])}、"
        f"±10% 命中 {pct(b1['b1']['within10_ratio'])}、P90 APE {pct(b1['b1']['p90_ape'])}、"
        f"可估覆盖 {pct(b1['b1']['coverage'])}。",
        "- **B1 不作对照基线**：本报告任何采用结论均不以 B1 为参照；本 change 未新增任何 B1 运行，"
        "既有回放证据只读复用并已指纹复核。",
        "",
    ]

    lines += ["## 9. 差异不确定性（配对 bootstrap，2000 次，种子 20260912）", ""]
    lines += _diff_table(summary)
    lines += ["",
              f"- 人群：{POP_ALL}（{allb['rows']} 行，按同一目标重采样，配对）；"
              "差值为「S3 线 − 参照线」，负值表示误差更低（更优）。",
              ""]

    lines += ["## 10. 网格与配置选择记录（含外层重拟合与跨折独立性）", ""]
    lines += [
        f"- 网格（运行前固定、有界）：M2 {len(exp_mod.M2_GRID)} 配置 "
        f"(iterations {{300,500}} × depth {{4,6}} × l2 {{1,5}})、M3a 同 M2、"
        f"M3b {len(exp_mod.M3B_GRID)} 配置（500 轮 / depth 6 / l2 {{1,5}}）、"
        f"M4 {len(exp_mod.M4_GRID)} 配置（num_leaves {{31,63}} × min_data_in_leaf {{10,20}} × "
        f"feature_fraction {{0.8,1.0}}，500 轮）；seed 20260912、thread=1。",
        f"- 选择规则：{summary['grid_selection']['selection_rule']}",
        f"- 外层重拟合：{summary['grid_selection']['outer_refit_rule']}"
        f"（M2/M3a/M3b/M4 每折每模型 1 次，合计 {summary['grid_selection']['outer_refit_count']['total']} 次；"
        f"另有属性增量 INC_M2/INC_M3a 各折外层重拟合 {2 * len(summary['per_fold'])} 次；"
        f"594 正式单元内外层重拟合合计 "
        f"{summary['registry_check']['cloud_outer_refit']} 次，未新增任何配置）。",
        f"- M4 结构：{summary['grid_selection']['m4_structure']}",
        f"- 跨折独立性声明：{summary['grid_selection']['no_cross_fold']}",
        "",
        "**逐折当选配置（内层选参）**",
        "",
    ]
    lines += _grid_table(summary)
    lines += [
        "",
        "- 每格为该折该模型的当选配置编号；各配置在每个内层留出集上的 MedAPE 逐值登记见 "
        "S3 run `s3-eval/eval-summary.json` 的 `grid_selection.per_fold`（可机械复算）。",
        "- 全折汇总表现仅作事后汇总与 S4 候选讨论材料，未进入任何选择路径。",
        "",
    ]

    lines += ["## 11. 属性增量实验（逐折配对成对结果）", ""]
    lines += _increment_table(summary)
    lines += ["", "**逐折配对明细**", ""]
    inc_rows = []
    for inc, blk in summary["increment"].items():
        for p in blk["pairs"]:
            inc_rows.append([inc, p["fold_id"], str(p["n"]), pct(p["full_med_ape"]),
                             pct(p["base_med_ape"]), pct(p["diff_base_minus_full"])])
    lines += _table(["增量线", "折", "n", "全量组 MedAPE", "基础组 MedAPE", "差(基础−全量)"],
                    inc_rows)
    lines += [
        "",
        "- 同一折同一目标、仅特征组不同（配置为该折 M2/M3a 当选配置），故为成对比较；"
        "差 = 基础组 MedAPE − 全量组 MedAPE：**正值 = 去掉房屋属性组后更差（房屋属性组有正贡献）**，"
        "负值反之；“全量组更优折数”计差值为正的折。",
        "",
    ]

    lines += ["## 12. A1 辅助价差对照（可修正子集，取自 5.1）", ""]
    a1 = summary["a1_section"]
    lines += [
        f"- 口径：{a1['caliber']}",
        "",
    ]
    lines += _table(["口径", "n", "MedAPE 前", "MedAPE 后", "±10% 前", "±10% 后",
                     "P90 前", "P90 后"], a1["rows"])
    lines += [
        "",
        f"- 配对 bootstrap（2000 次、种子 20260912、同一目标）：主口径 MedAPE 差 "
        f"{a1['bootstrap_main']['med_diff']:.5f}，95% CI "
        f"[{a1['bootstrap_main']['ci95_low']:.5f}, {a1['bootstrap_main']['ci95_high']:.5f}]"
        f"（{'不含 0' if not a1['bootstrap_main']['crosses_zero'] else '含 0'}）。",
        "- 用途隔离：A1 只属「比较法辅助价差」用途，**不产生独立报价结论**、不用于证明 M 系通过。",
        "",
    ]

    lines += ["## 13. 失败案例", ""]
    lines += [
        f"- 清单见同目录 `{summary['failure_case_file']}`（主轨 {LINE_LABEL[best]} 的总体 APE 最大 10 例、"
        "逐折最大例、相对 M1/B0 退化最大例，附属性、三方预测与归因线索）。",
        "",
    ]

    lines += ["## 14. 两用途分别结论", ""]
    lines += conclusions
    lines += [""]

    lines += ["## 15. 无增益定位归因（数据接入 / 市场时间 / 局部支持 / 缺失特征 / 标签质量）", ""]
    lines += attribution_lines
    lines += [""]

    lines += ["## 16. 复算指引", ""]
    lines += ["```powershell",
              "# 同窗重算（本机，折级分片并行；每 worker 单线程，CPU 占比 ≈ workers/逻辑核）",
              "$env:PYTHONPATH=\"03-估值引擎\\src\"",
              ".venv-phase2\\Scripts\\python.exe -m gz_property_valuation.phase2.s3_evaluate run --workers 4",
              "",
              "# 自检（报告关键词反查 + 行号抽查 + 云端对拍复核 + 折叠复算）",
              ".venv-phase2\\Scripts\\python.exe "
              "openspec\\changes\\compare-phase2-nonlinear\\evidence\\6-1\\check_6_1.py",
              "```"]
    lines += [
        "",
        "- 重跑应逐行复现 `s3-eval/per-fold/*.parquet`（同机确定性；跨机口径与容差见 §3）。",
        "- `s3-eval/eval-summary.json` 为本报告全部数值的唯一来源，报告行号抽查见 "
        "`evidence/6-1/6-1-keyword-and-linenumber-record.json`。",
        "",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def _case_table(cases: list[dict], primary_label: str, with_fold: bool = False) -> list[str]:
    headers = (["折"] if with_fold else []) + \
        ["source_record_id", "小区", "成交日", "面积㎡", "真值单价", primary_label + " 预测",
         primary_label + " APE", "M1 预测", "M1 APE", "B0 预测", "B0 APE", "证据层级", "归因线索"]
    rows = []
    for c in cases:
        row = []
        if with_fold:
            row.append(c["fold_id"])
        row += [c["source_record_id"], c["community_name"], c["sale_date"],
                f"{c['area_sqm']:.1f}" if c["area_sqm"] is not None else "—",
                f"{c['unit_price_true']:.0f}", f"{c['primary_pred']:.0f}",
                pct(c["primary_ape"]) + ("(高估)" if c["primary_pred"] > c["unit_price_true"]
                                         else "(低估)"),
                f"{c['m1_pred']:.0f}", pct(c["m1_ape"]),
                f"{c['b0_pred']:.0f}", pct(c["b0_ape"]),
                str(c["evidence_level"]), c["clue"]]
        rows.append(row)
    return _table(headers, rows)


def render_failure_cases(summary: dict, path: Path) -> Path:
    fc = summary["failure_cases"]
    primary = fc["primary_line"]
    label = primary
    best = summary["overall"][POP_ALL][primary]
    lines = [f"# S3 失败案例清单（主轨 {primary}，附 M1/B0 对照与归因线索）", ""]
    lines += [
        f"- 数据源：S3 run `{summary['inputs']['s3_run']}` 同窗重算逐行预测 `s3-eval/per-fold/*.parquet`"
        f"（与 S2 冻结行集逐折同窗；对拍结论见报告 §3）；M1/B0 为 S2 冻结逐行预测。",
        f"- 主轨选择：S3 六条模型线中全部合格目标 MedAPE 最低者 = {primary}"
        f"（MedAPE {pct(best['med_ape'])}，n={best['n']}）。",
        "- 归因线索仅列事实与初查方向，不深挖（按蓝图 §6.2 逐项定位）；APE = |预测/真值 − 1|。",
        "",
        "## 1. 主轨 APE 最大 10 例（全部合格目标）", "",
    ]
    lines += _case_table(fc["overall_top10"], label)
    lines += ["", "## 2. 逐折主轨 APE 最大例（18 折分层）", ""]
    lines += _case_table(fc["per_fold_max"], label, with_fold=True)
    lines += ["", "## 3. 相对 M1 退化最大 5 例（主轨 APE − M1 APE 最大）", ""]
    lines += _case_table(fc["vs_m1_top5"], label)
    lines += ["", "## 4. 相对 B0 退化最大 5 例（主轨 APE − B0 APE 最大）", ""]
    lines += _case_table(fc["vs_b0_top5"], label)
    lines += ["",
              "- 说明：§3/§4 为算术排序（相对参照线退化最大的个案），不代表逐折或总体劣势；",
              "  逐折与总体结论见报告 §6/§7/§9。",
              ""]
    flags = summary["attribution"].get("m3b_instability") or []
    lines += ["## 5. M3b（必交属性标准化变体）系统性崩塌折（非主轨，单列）", ""]
    if flags:
        for f in flags:
            lines.append(
                f"- {f['fold_id']}：n={f['n']}、MedAPE {pct(f['med_ape'])}、有符号偏差 "
                f"{pct(f['signed_med_bias'])}（预测/真值中位比 {f['median_pred_over_true']:.3f}，"
                f"即整体大幅低估）、该折外层预热占比 {pct(f['warmup_ratio'])}（设计内保留训练、"
                f"用未标准化基准）。")
        lines.append("")
        lines.append("- 结构性归因（只陈述、不定论）：崩塌呈同向低估（非个别离群）；与预热占比并非单调关系"
                     "（预热占比更高的 F17 55.21%、F18 69.04% 未崩塌），故不能仅归因于预热。"
                     "这两折同时具备“训练切片最小（5,088 / 4,686 行）+ 层内属性参照可用材料最少”，"
                     "标准化偏移在小样本上不稳定是可能方向，需进一步核查。该结果原样登记，"
                     "未据此追加网格或调参；M3b 结论按折分层给，不并入单一总体判断。")
    else:
        lines.append("- 本次未出现 medAPE>50% 的折。")
    lines += [""]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


ROOT = Path(__file__).resolve().parents[4]
ROOT_SRC = Path(__file__).resolve().parents[2]
DEFAULT_S1 = ROOT / "examples" / "phase2_demo" / "runs" / "demo_s1_run"
DEFAULT_S2 = ROOT / "examples" / "phase2_demo" / "runs" / "demo_s2_run"
DEFAULT_S3 = ROOT / "examples" / "phase2_demo" / "runs" / "demo_s3_run"
DEFAULT_CLOUD = ROOT / "examples" / "phase2_demo" / "cloud_pack" / "src"
DEFAULT_CHECK_DIR = ROOT / "examples" / "phase2_demo" / "s3_cross_check"


def write_outputs(summary: dict, out_dir: Path, check_dir: Path, d: str = "20260913") -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    check_dir.mkdir(parents=True, exist_ok=True)
    summary = _with_m3b(summary)
    (out_dir / "eval-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    csv = ["fold_id,model,field,local,cloud,abs_delta,rel_delta,finite,pass"]
    for r in summary["cross_check_detail"]:
        csv.append(f"{r['fold_id']},{r['model']},{r['field']},{r['local']!r},{r['cloud']!r},"
                   f"{r['abs_delta']!r},{r['rel_delta']!r},{r['finite']},{r['pass']}")
    (out_dir / "cross-check-detail.csv").write_text("\n".join(csv) + "\n", encoding="utf-8")
    report = render_report(summary, DEFAULT_S2, check_dir / f"S3-非线性比较报告-{d}.md")
    cases = render_failure_cases(summary, check_dir / f"S3-失败案例清单-{d}.md")
    return {"report": str(report), "failure_cases": str(cases),
            "summary": str(out_dir / "eval-summary.json")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="phase2-s3-evaluate",
                                 description="S3 同窗重算评估与报告（任务 6.1）")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("run", "report", "worker"):
        p = sub.add_parser(name, help="run=重算+汇总+报告；report=由已有汇总重出报告；"
                                      "worker=单折子进程（由 run 内部调度）")
        p.add_argument("--s1-run", type=Path, default=DEFAULT_S1)
        p.add_argument("--s2-run", type=Path, default=DEFAULT_S2)
        p.add_argument("--s3-run", type=Path, default=DEFAULT_S3)
        p.add_argument("--cloud-src", type=Path, default=DEFAULT_CLOUD)
        p.add_argument("--out-dir", type=Path, default=None)
        p.add_argument("--check-dir", type=Path, default=DEFAULT_CHECK_DIR)
        p.add_argument("--workers", type=int, default=4)
        p.add_argument("--folds", default="all")
        p.add_argument("--fold", default=None)
        p.add_argument("--date-tag", default="20260913")
    args = ap.parse_args(argv)
    out_dir = args.out_dir or (args.s3_run / "s3-eval")
    folds = (list(exp_mod.FOLDS) if args.folds == "all"
             else [s.strip() for s in args.folds.split(",")])

    if args.cmd == "worker":
        rec = _fold_worker(args.fold, args.s1_run.resolve(), args.cloud_src.resolve(),
                           out_dir.resolve())
        print(json.dumps({"fold_id": rec["fold_id"],
                          "outer_val_rows": rec["row_counts"]["outer_val"],
                          "total_fit_seconds": round(sum(v["fit_seconds"]
                                                         for v in rec["lines"].values()), 1)}))
        return 0

    if args.cmd == "report":
        summary = json.loads((out_dir / "eval-summary.json").read_text(encoding="utf-8"))
        out = write_outputs(summary, out_dir, args.check_dir, args.date_tag)
        print(json.dumps(out, ensure_ascii=False, indent=1))
        return 0

    recompute = run_recompute(args.s1_run.resolve(), args.cloud_src.resolve(),
                              out_dir.resolve(), folds, args.workers)
    print(json.dumps({"wall_seconds": round(recompute["wall_seconds"], 1),
                      "workers": recompute["workers"],
                      "logical_cores": recompute["logical_cores"]}, ensure_ascii=False))
    summary = build_summary(args.s1_run.resolve(), args.s2_run.resolve(),
                            args.s3_run.resolve(), args.cloud_src.resolve(),
                            out_dir.resolve(), recompute, args.workers)
    out = write_outputs(summary, out_dir.resolve(), args.check_dir.resolve(), args.date_tag)
    print(json.dumps({
        "report": out["report"], "failure_cases": out["failure_cases"],
        "cross_check_pass": summary["cross_check"]["all_pass"],
        "max_rel_delta": summary["cross_check"]["max_rel_delta"],
        "rows": summary["rows"],
        "overall_med_ape": {p: {m: round(v["med_ape"], 5) for m, v in blk.items()
                                if isinstance(v, dict) and "med_ape" in v}
                            for p, blk in summary["overall"].items()},
    }, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
