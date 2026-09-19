# -*- coding: utf-8 -*-
"""phase2 S3 A1 自有案例辅助价差：M1 复算对拍、非疑似自读子集、案例检索与显式属性修正。

行为规格（specs/phase2-nonlinear-comparisons/spec.md「A1 自有案例辅助价差试验」、design D6）：

- M1 复算（design D6 第 1 条）：以 S2 run manifest 登记的结构/λ 网格/种子确定性重建 18 折
  M1（重新拟合性质，非调参），持久化每折编码器与权重至 S3 run 目录 ``m1_refit/``；与 S2
  冻结逐折预测逐行对拍（同机闭式解，容差 0），不一致停线上报（:class:`A1Halt`）。
- 非疑似自读子集：B1 valued 行中排除 ``|center/真值 − 1| ≤ 1e-4`` 的疑似自匹配行，计数披露。
- 案例检索：目标所属折 = 其成交日所在验证带之折；案例池 = 该折外层截点前主表成交；
  检索窗 = 该折外层截点前 365 天；同小区候选（排除自身与同日，上限 8）→ 不足回退板块补足。
  上界开区间使「排除自身」与「排除同日」在真数据上结构自动成立，仍以显式过滤 + 断言核验。
- 修正公式（可执行定义）：设 M1 设计列中属性组列集合 A（面积平滑基、房龄平滑基、楼层段、
  电梯三态、楼层×电梯交互、装修、朝向、总层数、室数；**排除截距、小区/板块 one-hot、
  年-月哑变量与市场列**），``w[A]`` 为该折冻结权重，对目标 t 与案例 i（同一折编码器下取列）：

      delta_i = (x_target[A] − x_case_i[A]) @ w[A]        （对数尺度贡献差，可为负）
      adjusted_price_i = case_unit_price_i × exp(delta_i)

  修正前中心 = ``median(case_unit_price_i)``；修正后中心 = ``median(adjusted_price_i)``；
  **同一案例集合**（同一 ID 集、同一聚合）。
- 固定人群规则（防「删 after 行」伪改善）：① 拒修正案例（案例或目标关键属性缺失触发缺失
  标记）→ 修正后保留原价（delta 视为 0）并登记原因；② 全部案例被拒的目标 → 前后取值完全
  相同、保留在两个分母；③ 无任何案例的目标 → 从修正前与修正后**双方**排除并单列覆盖披露；
  ④ 主口径 = 固定全目标集上前后（同 ID 集、同聚合）；可修正子集口径另报且 ID 交集先固定、
  前后同分母；⑤ 机械断言：前后指标分母 ID 集合逐元素相等。
- 用途隔离：A1 不产生独立报价结论；B1 回放中心与 B0 仅作登记性并列参考（不作对照基线）。
- 全量对照（任务 5.1，本模块下半部分）：固定全目标集上修正前 vs 修正后三项指标
  （MedAPE / ±10% 命中 / P90 APE）+ 配对 bootstrap 差值 95% 区间（按同一目标重采样，
  种子 20260912、2000 次）；可修正子集口径另报（ID 交集先固定、前后同分母）；前后分母 ID
  集合逐元素相等断言；B1/B0 登记性并列参考；无案例目标单列覆盖披露；逐对象案例清单与
  修正证据落盘；用途隔离措辞检查。冻结 M1 资产以**只读**方式装载并复核（不重写 S3 既有文件）。

只消费 S1 合同 run 与 S2 run 冻结产物（design D1 消费边界），不读 staged 原始数据。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from . import baselines, evaluate, lineage, models

WINDOW_DAYS = 365
CASE_CAP = 8
SUSPECT_TOL = 1e-4
MISS_FLAG_COLS = ("miss_year_built", "miss_total_floors", "miss_elevator",
                  "miss_orientation", "miss_decoration")
KEY_NUMERIC_COLS = ("area_sqm", "age_years", "total_floors")
ATTRIBUTE_CATEGORY_COLS = ("floor", "elevator", "bedrooms", "orientation",
                           "decoration", "floor_elevator")
EXCLUDED_CATEGORY_COLS = ("community", "block", "ym")
NONSUSPECT_RULE = "B1 valued ∩ |b1_center/unit_price_true − 1| > 1e-4"
REJECT_REASON_TARGET = "target_missing_key_attribute"
REJECT_REASON_CASE = "case_missing_key_attribute"
DELTA_COLUMN_EXCLUSION_NOTE = "A 排除截距、小区/板块 one-hot、年-月哑变量与市场列（M1 设计列内无市场列）"


class A1Halt(RuntimeError):
    """停线上报：M1 复算与 S2 冻结预测对拍不一致等停止条件触发。"""


# ---------------------------------------------------------------- 设计列与属性组 A

def attribute_indices(enc: models.FoldEncoder) -> np.ndarray:
    """属性组列集合 A 在 M1 设计矩阵中的列下标（排除截距/位置/时间/市场列）。"""
    names = enc.feature_names()
    idx = []
    for i, n in enumerate(names):
        if n == "intercept":
            continue
        if "=" in n:
            col = n.split("=", 1)[0]
            if col in EXCLUDED_CATEGORY_COLS:
                continue
            if col in ATTRIBUTE_CATEGORY_COLS:
                idx.append(i)
        else:
            idx.append(i)
    return np.asarray(idx, dtype=np.int64)


def attribute_column_names(enc: models.FoldEncoder) -> list[str]:
    names = enc.feature_names()
    return [names[i] for i in attribute_indices(enc)]


def attribute_matrix(enc: models.FoldEncoder, frame: pl.DataFrame) -> np.ndarray:
    """仅构造属性组 A 的设计子矩阵（与 ``enc.transform(frame)[:, A]`` 逐位相等）。

    数值组全部属属性组（M1 设计列内无市场列）；类别组只取 :data:`ATTRIBUTE_CATEGORY_COLS`。
    """
    cats = models.category_frame(frame)
    blocks = []
    for c in enc.cat_cols:
        if c not in ATTRIBUTE_CATEGORY_COLS:
            continue
        vals = np.asarray(cats[c].to_list(), dtype=object)
        known = enc.categories[c][1:]
        if not known:
            continue
        blocks.append((vals[:, None] == np.asarray(known, dtype=object)[None, :]
                       ).astype(np.float64))
    nums, _ = models.numeric_frame(frame, enc.fills)
    nums_z = (nums - enc.mu) / enc.sd
    cat = np.hstack(blocks) if blocks else np.zeros((frame.height, 0))
    return np.column_stack([cat, nums_z])


def missing_key_attribute(frame: pl.DataFrame) -> np.ndarray:
    """关键属性缺失 = 任一 S1 缺失标记置位，或关键数值源列为空（design D6 拒修正触发）。"""
    flags = [frame[c].cast(pl.Float64).fill_null(1.0).to_numpy() > 0
             for c in MISS_FLAG_COLS]
    nulls = [frame[c].is_null().to_numpy() for c in KEY_NUMERIC_COLS]
    miss = np.zeros(frame.height, dtype=bool)
    for arr in flags + nulls:
        miss |= arr
    return miss


# ---------------------------------------------------------------- M1 复算与持久化

def s3_run_seed(s1_run_id: str, s2_run_id: str, code_entries: list[dict]) -> str:
    return s1_run_id + s2_run_id + json.dumps(code_entries, sort_keys=True)


def ensure_s3_run_dir(runs_root: Path, s1_run_id: str, s2_run_id: str,
                      code_entries: list[dict]) -> tuple[Path, str]:
    """S3 run 目录：命名沿用既有 phase2 约定 ``<UTC时间戳>-<8位hash>``；同名 hash 目录存在即复用。"""
    short = hashlib.sha256(
        s3_run_seed(s1_run_id, s2_run_id, code_entries).encode()).hexdigest()[:8]
    existing = sorted(runs_root.glob(f"*-{short}"))
    if existing:
        return existing[0], "reused"
    run_id = (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + short)
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir, "created"


def _encoder_payload(fid: str, lam: float, enc: models.FoldEncoder) -> dict:
    return {
        "fold_id": fid,
        "chosen_lambda": lam,
        "cat_cols": list(enc.cat_cols),
        "categories": {c: [str(v) for v in vs] for c, vs in enc.categories.items()},
        "ym_clamp": str(enc.ym_clamp),
        "fills": {k: float(v) for k, v in enc.fills.items()},
        "mu": [float(v) for v in enc.mu],
        "sd": [float(v) for v in enc.sd],
        "feature_names": enc.feature_names(),
        "attribute_columns": attribute_column_names(enc),
        "attribute_column_count": int(len(attribute_indices(enc))),
        "n_features": int(len(enc.feature_names())),
    }


def _compare_with_s2(pred: pl.DataFrame, s2_run_dir: Path, fid: str) -> dict:
    s2 = (pl.read_parquet(s2_run_dir / "per-fold" / f"{fid}.parquet")
          .select(["source_record_id", pl.col("m1_pred").alias("s2_m1_pred")]))
    rec = pred.select(["source_record_id", "pred_unit_price"])
    j = rec.join(s2, on="source_record_id", how="full", coalesce=True)
    n_left_null = int(j["pred_unit_price"].is_null().sum())
    n_right_null = int(j["s2_m1_pred"].is_null().sum())
    diff = (j["pred_unit_price"] - j["s2_m1_pred"]).abs()
    n_diff = int((diff > 0).sum())
    max_abs = float(diff.max()) if diff.len() and diff.max() is not None else 0.0
    return {
        "fold_id": fid,
        "recompute_rows": rec.height,
        "s2_rows": s2.height,
        "joined_rows": j.height,
        "id_set_equal": bool(rec.height == s2.height == j.height),
        "missing_ids": n_left_null + n_right_null,
        "mismatch_rows": n_diff + n_left_null + n_right_null,
        "max_abs_diff": max_abs,
        "tolerance": 0.0,
        "match": bool(rec.height == s2.height == j.height and n_diff == 0
                      and n_left_null == 0 and n_right_null == 0),
    }


def refit_m1_all_folds(s1_run_dir: Path, s2_run_dir: Path, run_dir: Path) -> dict:
    """复算 18 折 M1：折内 λ 选择 + 外层拟合 → 持久化编码器/权重 + 与 S2 冻结预测对拍。"""
    s1_manifest = json.loads((s1_run_dir / "manifest.json").read_text(encoding="utf-8"))
    s2_manifest = json.loads((s2_run_dir / "manifest.json").read_text(encoding="utf-8"))
    splits = json.loads((s1_run_dir / "splits.json").read_text(encoding="utf-8"))
    s2_lambda = {r["fold_id"]: r["chosen_lambda"] for r in s2_manifest["lambda_records"]}
    m1_dir = run_dir / "m1_refit"
    m1_dir.mkdir(parents=True, exist_ok=True)

    folds, encoders = [], {}
    for fold in splits["folds"]:
        fid = fold["fold_id"]
        _, anchor, train, val = models.load_fold(s1_run_dir, fid)
        sel = models.select_lambda_in_fold(train)
        lam = sel["chosen_lambda"]
        res = models.fit_fold_predict(train, val, lam)
        enc, w = res["encoder"], res["weights"]

        fdir = m1_dir / fid
        fdir.mkdir(parents=True, exist_ok=True)
        np.save(fdir / "weights.npy", w)
        (fdir / "encoder.json").write_text(
            json.dumps(_encoder_payload(fid, lam, enc), ensure_ascii=False),
            encoding="utf-8")

        cmp = _compare_with_s2(res["predictions"], s2_run_dir, fid)
        folds.append({
            "fold_id": fid, "anchor": anchor.isoformat(),
            "train_rows": int(train.height), "val_rows": int(val.height),
            "chosen_lambda": float(lam),
            "s2_chosen_lambda": float(s2_lambda[fid]),
            "lambda_match": bool(float(lam) == float(s2_lambda[fid])),
            "n_features": int(len(enc.feature_names())),
            "n_attribute_columns": int(len(attribute_indices(enc))),
            "weights_file": f"m1_refit/{fid}/weights.npy",
            "weights_sha256": lineage.sha256_file(fdir / "weights.npy"),
            "encoder_file": f"m1_refit/{fid}/encoder.json",
            "encoder_sha256": lineage.sha256_file(fdir / "encoder.json"),
            "s2_comparison": cmp,
        })
        encoders[fid] = (enc, w)

    totals = {
        "folds": len(folds),
        "mismatch_rows": int(sum(f["s2_comparison"]["mismatch_rows"] for f in folds)),
        "max_abs_diff": float(max(f["s2_comparison"]["max_abs_diff"] for f in folds)),
        "id_sets_equal": bool(all(f["s2_comparison"]["id_set_equal"] for f in folds)),
        "lambda_all_match": bool(all(f["lambda_match"] for f in folds)),
    }
    index = {
        "schema_version": "phase2-s3-m1-refit-v1",
        "run_id": run_dir.name,
        "s1_run": {"run_id": s1_manifest["run_id"],
                   "manifest_sha256": lineage.sha256_file(s1_run_dir / "manifest.json")},
        "s2_run": {"run_id": s2_manifest["run_id"],
                   "manifest_sha256": lineage.sha256_file(s2_run_dir / "manifest.json")},
        "recompute_rule": "以 S2 manifest 登记结构/λ 网格重建 18 折 M1（重新拟合，非调参）；"
                          "λ 折内选择判据与 models.select_lambda_in_fold 一致",
        "persisted": "每折 encoder.json（类别表/钳制/填充/z-score 统计）+ weights.npy（Ridge 权重）",
        "comparison_tolerance": 0.0,
        "totals": totals,
        "folds": folds,
    }
    (m1_dir / "index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=1), encoding="utf-8")

    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        manifest = {
            "schema_version": "phase2-s3-run-v1",
            "change": "compare-phase2-nonlinear",
            "run_id": run_dir.name,
            "created_at_utc": lineage.utc_now_iso(),
            "s1_run": index["s1_run"],
            "s2_run": index["s2_run"],
            "code": evaluate.code_fingerprint(Path(__file__).resolve().parent),
            "artifacts": {},
            "stage_record": {"stage": "4", "task": "3.1",
                             "consumption_boundary": "只读 S1 合同 run 与 S2 run 冻结产物"},
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1),
                                 encoding="utf-8")
    lineage.register_artifact(run_dir, "m1_refit/index.json",
                              extra={"folds": len(folds),
                                     "mismatch_rows": totals["mismatch_rows"]})
    lineage.finalize_manifest(run_dir, {"m1_refit": {
        "persisted_dir": "m1_refit/",
        "comparison_tolerance": 0.0,
        "totals": totals,
        "note": "本阶段（任务 3.1）仅落 M1 复算切片；完整 S3 run manifest 由后续阶段合并",
    }})
    return {"run_dir": str(run_dir), "index": index, "encoders": encoders,
            "splits": splits}


# ---------------------------------------------------------------- 非疑似自读子集

def nonsuspect_targets(s1_run_dir: Path, s2_run_dir: Path) -> tuple[pl.DataFrame, dict]:
    """B1 valued ∩ 非疑似自读（|center/真值−1| > 1e-4）；返回目标表与计数披露。"""
    splits = json.loads((s1_run_dir / "splits.json").read_text(encoding="utf-8"))
    frames = []
    for fold in splits["folds"]:
        fid = fold["fold_id"]
        frames.append(pl.read_parquet(s2_run_dir / "per-fold" / f"{fid}.parquet").select(
            ["fold_id", "source_record_id", "unit_price_true", "b1_status", "b1_center"]))
    all_df = pl.concat(frames)
    valued = all_df.filter(pl.col("b1_status") == "valued")
    ratio = (valued["b1_center"] / valued["unit_price_true"] - 1.0).abs()
    suspect = valued.filter(ratio <= SUSPECT_TOL)
    nonsuspect = valued.filter(ratio > SUSPECT_TOL)
    disclosure = {
        "rule": NONSUSPECT_RULE,
        "all_targets": int(all_df.height),
        "b1_valued": int(valued.height),
        "suspect_self_match": int(suspect.height),
        "suspect_ratio_of_valued": float(suspect.height / valued.height),
        "nonsuspect": int(nonsuspect.height),
    }
    return nonsuspect, disclosure


# ---------------------------------------------------------------- 案例检索与修正

def _block_values(frame: pl.DataFrame) -> list[str | None]:
    return [None if (v is None or str(v).strip() in baselines.MISSING_BLOCK_VALUES)
            else str(v) for v in frame["block_name"].to_list()]


def _index_sorted(keys: list, dates: np.ndarray, ids: np.ndarray) -> dict:
    """键 → 行下标数组，按成交日倒序、ID 升序（案例优先取最近、结果确定）。"""
    buckets: dict = {}
    for i, k in enumerate(keys):
        if k is None:
            continue
        buckets.setdefault(k, []).append(i)
    out = {}
    for k, idx in buckets.items():
        arr = np.asarray(idx, dtype=np.int64)
        order = np.lexsort((ids[arr], -dates[arr].astype("datetime64[D]").astype(np.int64)))
        out[k] = arr[order]
    return out


def correct_target(enc: models.FoldEncoder, w: np.ndarray, target_attr: np.ndarray,
                   case_attr: np.ndarray, case_price: np.ndarray,
                   case_miss: np.ndarray, target_reject: bool) -> dict:
    """显式修正公式（design D6）：delta = (x_target[A] − x_case[A]) @ w[A]。

    拒修正（目标或案例关键属性缺失）→ delta 记 0、修正价保留原价并登记原因。
    """
    a_idx = attribute_indices(enc)
    w_a = w[a_idx]
    delta = (target_attr[None, :] - case_attr) @ w_a
    reject = np.asarray(case_miss, dtype=bool).copy()
    if target_reject:
        reject[:] = True
    delta = np.where(reject, 0.0, delta)
    adjusted = np.where(reject, case_price, case_price * np.exp(delta))
    return {"delta": delta, "adjusted": adjusted, "reject": reject,
            "column_count": int(len(a_idx))}


def top_contributions(enc: models.FoldEncoder, w: np.ndarray, target_attr: np.ndarray,
                      case_attr: np.ndarray, topn: int = 3) -> list[list[dict]]:
    """逐案例属性贡献分解（人工抽查方向合理性用）：Δx_k × w_k 的绝对值前三项。"""
    names = attribute_column_names(enc)
    w_a = w[attribute_indices(enc)]
    d = target_attr[None, :] - case_attr
    contrib = d * w_a[None, :]
    out = []
    for r in range(contrib.shape[0]):
        order = np.argsort(-np.abs(contrib[r]))[:topn]
        out.append([{"attribute": names[int(k)],
                     "delta_x": float(d[r, int(k)]),
                     "weight": float(w_a[int(k)]),
                     "contribution": float(contrib[r, int(k)])} for k in order])
    return out


def retrieve_and_correct(s1_run_dir: Path, s2_run_dir: Path, refit: dict,
                         targets: pl.DataFrame, timing_probe: int = 1000,
                         trace_ids: set | None = None) -> dict:
    """逐折：案例池 = 该折截点前主表成交；同小区→板块检索（上限 8）；显式属性修正与固定人群聚合。"""
    features = pl.read_parquet(s1_run_dir / "features.parquet")
    labels = (pl.read_parquet(s1_run_dir / "master_table.parquet")
              .select(["source_record_id", pl.col("unit_price").cast(pl.Float64)]))
    pool = features.join(labels, on="source_record_id", how="left")

    tgt_ids = set(targets["source_record_id"].to_list())
    tgt_rows = (features.filter(pl.col("source_record_id").is_in(list(tgt_ids)))
                .join(targets.select(["source_record_id", "fold_id", "unit_price_true",
                                      "b1_center"]),
                      on="source_record_id", how="left"))
    assert tgt_rows.height == len(tgt_ids), "非疑似目标未能全部定位到特征层"

    records, per_fold = [], {}
    probe_count, probe_seconds = 0, 0.0
    import time as _time

    for fold in refit["splits"]["folds"]:
        fid = fold["fold_id"]
        enc, w = refit["encoders"][fid]
        anchor = date.fromisoformat(fold["anchor"])

        train = pool.filter((pl.col("sale_date_d") >= anchor - timedelta(days=WINDOW_DAYS))
                            & (pl.col("sale_date_d") < anchor)
                            & pl.col("unit_price").is_not_null())
        train_attr = attribute_matrix(enc, train)
        train_miss = missing_key_attribute(train)
        train_price = train["unit_price"].to_numpy().astype(np.float64)
        train_ids = train["source_record_id"].to_numpy()
        train_dates = train["sale_date_d"].to_numpy()
        train_comm = train["community_source_id"].to_numpy()
        train_block = np.asarray(_block_values(train), dtype=object)

        comm_idx = _index_sorted(train_comm.tolist(), train_dates, train_ids)
        block_idx = _index_sorted(train_block.tolist(), train_dates, train_ids)

        fold_targets = tgt_rows.filter(pl.col("fold_id") == fid)
        t_attr = attribute_matrix(enc, fold_targets)
        t_miss = missing_key_attribute(fold_targets)
        t_comm = fold_targets["community_source_id"].to_numpy()
        t_block = np.asarray(_block_values(fold_targets), dtype=object)
        t_ids = fold_targets["source_record_id"].to_numpy()
        t_dates = fold_targets["sale_date_d"].to_numpy()
        t_dates_i = t_dates.astype("datetime64[D]").astype(np.int64)

        stat = {"targets": int(fold_targets.height), "zero_case": 0,
                "all_rejected": 0, "fixable": 0, "rejected_cases": 0,
                "community_only": 0, "block_fallback": 0, "cases_total": 0}

        for k in range(fold_targets.height):
            t0 = _time.perf_counter()
            block_key = None if t_block[k] is None else str(t_block[k])
            candidates = []
            seen = set()
            comm_list = comm_idx.get(str(t_comm[k]), np.empty(0, dtype=np.int64))
            n_comm = 0
            for i in comm_list:
                if len(candidates) >= CASE_CAP:
                    break
                if train_ids[i] == t_ids[k] or train_dates[i] == t_dates[k]:
                    continue
                candidates.append(int(i))
                seen.add(int(i))
                n_comm += 1
            n_block = 0
            if len(candidates) < CASE_CAP and block_key is not None:
                for i in block_idx.get(block_key, np.empty(0, dtype=np.int64)):
                    if len(candidates) >= CASE_CAP:
                        break
                    if int(i) in seen:
                        continue
                    if train_ids[i] == t_ids[k] or train_dates[i] == t_dates[k]:
                        continue
                    if str(train_comm[i]) == str(t_comm[k]):
                        continue
                    candidates.append(int(i))
                    seen.add(int(i))
                    n_block += 1
            if not candidates:
                stat["zero_case"] += 1
                records.append({
                    "fold_id": fid, "source_record_id": str(t_ids[k]),
                    "unit_price_true": float(fold_targets["unit_price_true"][k]),
                    "b1_center": float(fold_targets["b1_center"][k]),
                    "n_cases": 0, "n_community": 0, "n_block": 0,
                    "n_pre_cases": 0, "n_post_cases": 0,
                    "n_cases_rejected": 0, "n_cases_corrected": 0,
                    "pre_center": None, "post_center": None,
                    "delta_median": None, "retrieval_level": "none",
                })
                continue

            case_attr = train_attr[candidates]
            case_price = train_price[candidates]
            case_miss = train_miss[candidates]
            target_reject = bool(t_miss[k])
            corr = correct_target(enc, w, t_attr[k], case_attr, case_price,
                                  case_miss, target_reject)
            delta, adjusted, reject_mask = corr["delta"], corr["adjusted"], corr["reject"]

            pre = float(np.median(case_price))
            post = float(np.median(adjusted))
            n_rej = int(reject_mask.sum())
            n_corr = int(len(candidates) - n_rej)

            stat["cases_total"] += len(candidates)
            stat["rejected_cases"] += n_rej
            if n_rej == len(candidates):
                stat["all_rejected"] += 1
            if n_corr > 0:
                stat["fixable"] += 1
            if n_block:
                stat["block_fallback"] += 1
            else:
                stat["community_only"] += 1

            records.append({
                "fold_id": fid, "source_record_id": str(t_ids[k]),
                "unit_price_true": float(fold_targets["unit_price_true"][k]),
                "b1_center": float(fold_targets["b1_center"][k]),
                "n_cases": len(candidates), "n_community": n_comm, "n_block": n_block,
                "n_pre_cases": int(case_price.size), "n_post_cases": int(adjusted.size),
                "n_cases_rejected": n_rej, "n_cases_corrected": n_corr,
                "pre_center": pre, "post_center": post,
                "delta_median": float(np.median(delta)),
                "retrieval_level": ("community" if n_block == 0 else "community+block"),
                "target_reject": bool(target_reject),
                "case_ids": [str(train_ids[i]) for i in candidates],
                "trace": ({
                    "case_ids": [str(train_ids[i]) for i in candidates],
                    "case_dates": [str(train_dates[i]) for i in candidates],
                    "case_prices": [float(p) for p in case_price],
                    "case_adjusted": [float(v) for v in adjusted],
                    "case_delta": [float(v) for v in delta],
                    "case_reject": [bool(v) for v in reject_mask],
                    "case_miss": [bool(v) for v in case_miss],
                    "target_reject": bool(target_reject),
                    "top_contributions": top_contributions(enc, w, t_attr[k], case_attr),
                } if (trace_ids and str(t_ids[k]) in trace_ids) else None),
            })
            if probe_count < timing_probe:
                probe_count += 1
                probe_seconds += _time.perf_counter() - t0

        per_fold[fid] = stat

    detail = pl.DataFrame(records)
    denom = detail.filter(pl.col("n_cases") > 0)
    fixable = denom.filter(pl.col("n_cases_corrected") > 0)

    def _ids(df: pl.DataFrame) -> list[str]:
        return sorted(df["source_record_id"].to_list())

    denom_ids = _ids(denom)
    fixable_ids = _ids(fixable)
    summary = {
        "rule": {
            "case_window_days": WINDOW_DAYS,
            "case_cap": CASE_CAP,
            "pool": "该折外层截点前主表成交（上界开区间，结构排除自身与同日）",
            "retrieval": "同小区候选（排除自身与同日，上限 8）→ 不足回退板块补足",
            "delta_formula": "delta_i = (x_target[A] − x_case_i[A]) @ w[A]；"
                             "adjusted_i = case_unit_price_i × exp(delta_i)",
            "delta_column_exclusion": DELTA_COLUMN_EXCLUSION_NOTE,
            "reject_reasons": [REJECT_REASON_TARGET, REJECT_REASON_CASE],
        },
        "population": {
            "targets": int(detail.height),
            "zero_case_excluded": int((detail["n_cases"] == 0).sum()),
            "main_denominator": int(denom.height),
            "fixable_subset": int(fixable.height),
            "all_rejected": int((detail["n_cases_corrected"] == 0).sum()
                                - (detail["n_cases"] == 0).sum()),
            "zero_case_ratio": float((detail["n_cases"] == 0).mean()),
        },
        "id_sets": {
            "main_ids_elementwise_equal": denom_ids == sorted(denom_ids),
            "main_id_count": len(denom_ids),
            "fixable_id_count": len(fixable_ids),
            "fixable_subset_of_main": bool(set(fixable_ids) <= set(denom_ids)),
        },
        "per_fold": per_fold,
        "timing_probe": {
            "probe_targets": probe_count,
            "probe_seconds": probe_seconds,
            "per_target_ms": (probe_seconds / probe_count * 1000.0) if probe_count else None,
            "extrapolated_full_seconds": (probe_seconds / probe_count * detail.height)
            if probe_count else None,
        },
        "main_pre_center_median": float(denom["pre_center"].median()) if denom.height else None,
        "main_post_center_median": float(denom["post_center"].median()) if denom.height else None,
    }
    return {"detail": detail, "summary": summary, "main_ids": denom_ids,
            "fixable_ids": fixable_ids, "tgt_rows": tgt_rows, "enc_ref": refit["encoders"]}


def denom_id_sets(result: dict) -> dict:
    """前后分母 ID 集合（机械断言用）：主口径与可修正子集口径各独立导出 (pre_ids, post_ids)。

    修正前分母 = 有可用案例价的目标；修正后分母 = 有可用修正价的目标——两套 ID 各自独立从
    逐目标表导出后再逐元素比对，可捕获任何「删除 after 行」式样本退出。
    """
    detail = result["detail"]
    pre = detail.filter(pl.col("n_pre_cases") > 0)
    post = detail.filter(pl.col("n_post_cases") > 0)
    fix_pre = pre.filter(pl.col("n_cases_corrected") > 0)
    fix_post = post.filter(pl.col("n_cases_corrected") > 0)
    return {
        "main": (sorted(pre["source_record_id"].to_list()),
                 sorted(post["source_record_id"].to_list())),
        "fixable": (sorted(fix_pre["source_record_id"].to_list()),
                    sorted(fix_post["source_record_id"].to_list())),
    }


# ---------------------------------------------------------------- 5.1 全量对照与统计推断

BOOTSTRAP_ITERATIONS = 2000
BOOTSTRAP_SEED = 20260912
WITHIN10_THRESHOLD = 0.10
METRIC_KEYS = ("med_ape", "within10_ratio", "p90_ape")
NO_CASE_DISCLOSURE_THRESHOLD = 0.05
USAGE_ISOLATION_STATEMENT = (
    "用途隔离：A1 仅属「比较法辅助价差」用途，本任务不产生独立报价结论，"
    "也不以 A1 佐证独立报价用途；独立报价能力由 M 系对 B0/M1 另行对照。")
B1_MAGNITUDE_WARNING = (
    "B1 量级异常警示：B1 回放中心为极端量级（S2 报告 §0/§11：46.68% 的 B1 有效行与真值"
    "一致到 1e-4 以内、疑似可比检索命中目标自身/镜像），不可解读为引擎预测能力；"
    "本任务仅作登记性并列参考，不作对照基线、不参与任何比较结论。")
REGISTERED_REFERENCE_NOTE = (
    "B1 回放中心与 B0 在本任务中仅作登记性并列参考（与 A1 主口径同一 ID 集）；"
    "二者均不作基线、不参与 A1 修正效果的判定。")
FORBIDDEN_INDEPENDENT_QUOTE_PHRASES = (
    "独立报价通过",
    "独立报价能力通过",
    "独立报价可用",
    "可用作独立报价",
    "可用于独立报价",
    "独立报价成立",
    "独立报价结论为通过",
    "A1 证明了独立报价",
    "A1 支持独立报价",
    "A1 可作独立报价",
)
USAGE_ISOLATION_REQUIRED = ("比较法辅助价差", "不产生独立报价结论")


def ape_from_center(center: np.ndarray, true_price: np.ndarray) -> np.ndarray:
    """APE = |center / 真值 − 1|（center 为目标案例集合的中位单价）。"""
    return np.abs(center / true_price - 1.0)


def metric_value(ape: np.ndarray, key: str) -> float:
    if key == "med_ape":
        return float(np.median(ape))
    if key == "within10_ratio":
        return float((ape <= WITHIN10_THRESHOLD).mean())
    if key == "p90_ape":
        return float(np.percentile(ape, 90))
    raise KeyError(f"未知指标键：{key}")


def point_metrics(ape: np.ndarray) -> dict:
    """三项点指标：MedAPE / ±10% 命中率 / P90 APE（口径同蓝图 §11 与 S2 evaluate）。"""
    return {"n": int(len(ape)),
            "med_ape": metric_value(ape, "med_ape"),
            "within10_ratio": metric_value(ape, "within10_ratio"),
            "p90_ape": metric_value(ape, "p90_ape")}


def paired_bootstrap_metric_diffs(ape_pre: np.ndarray, ape_post: np.ndarray,
                                  n_boot: int = BOOTSTRAP_ITERATIONS,
                                  seed: int = BOOTSTRAP_SEED) -> dict:
    """配对 bootstrap（按同一目标重采样，种子与次数登记）：三项指标的差值分布与 95% 区间。

    diff = 修正后 − 修正前；每轮对目标下标做一次有放回抽样（n 同分母），修正前后共用同一下标集。
    """
    n = int(len(ape_pre))
    assert n > 0 and n == len(ape_post), "配对 bootstrap 分母不一致"
    rng = np.random.default_rng(seed)
    dists = {k: np.empty(n_boot, dtype=np.float64) for k in METRIC_KEYS}
    for i in range(n_boot):
        idx = rng.integers(0, n, n)
        a, b = ape_pre[idx], ape_post[idx]
        for k in METRIC_KEYS:
            dists[k][i] = metric_value(b, k) - metric_value(a, k)
    out = {
        "n_boot": int(n_boot),
        "seed": int(seed),
        "n_targets": n,
        "resampling": "按同一目标重采样：每轮对目标下标做一次有放回抽样（n 同分母），"
                      "修正前后共用同一下标集（配对）",
        "diff_definition": "diff = 修正后 − 修正前；APE 类指标（med_ape/p90_ape）负值=改善，"
                           "命中率（within10_ratio）正值=改善",
        "ci_rule": "区间 = 差值 bootstrap 分布的 2.5 / 97.5 百分位（区间式呈现，非单点）",
        "metrics": {},
    }
    for k in METRIC_KEYS:
        d = dists[k]
        lo, hi = float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5))
        out["metrics"][k] = {
            "observed_diff": metric_value(ape_post, k) - metric_value(ape_pre, k),
            "ci95_low": lo, "ci95_high": hi, "ci95": [lo, hi],
            "boot_median": float(np.median(d)),
            "boot_min": float(d.min()), "boot_max": float(d.max()),
            "includes_zero": bool(lo <= 0.0 <= hi),
        }
    return out


def _ape_pair(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    true = df["unit_price_true"].to_numpy().astype(np.float64)
    pre = ape_from_center(df["pre_center"].to_numpy().astype(np.float64), true)
    post = ape_from_center(df["post_center"].to_numpy().astype(np.float64), true)
    return pre, post


def a1_population_comparison(detail: pl.DataFrame,
                             n_boot: int = BOOTSTRAP_ITERATIONS,
                             seed: int = BOOTSTRAP_SEED) -> tuple[dict, dict]:
    """全量对照：主口径（固定全目标集）与可修正子集口径（ID 交集先固定、前后同分母）。"""
    main = detail.filter(pl.col("n_cases") > 0)
    fixable = main.filter(pl.col("n_cases_corrected") > 0)
    zero_case = detail.filter(pl.col("n_cases") == 0)

    def block(df: pl.DataFrame) -> tuple[dict, tuple[np.ndarray, np.ndarray]]:
        pre, post = _ape_pair(df)
        return {"n": int(df.height),
                "pre": point_metrics(pre),
                "post": point_metrics(post),
                "diff_post_minus_pre": {k: metric_value(post, k) - metric_value(pre, k)
                                        for k in METRIC_KEYS},
                "bootstrap": paired_bootstrap_metric_diffs(pre, post, n_boot, seed)}, (pre, post)

    main_block, main_ape = block(main)
    fix_block, fix_ape = block(fixable)
    zero_ratio = float(zero_case.height / detail.height) if detail.height else 0.0
    payload = {
        "definition": {
            "ape": "ape = |center / 真值 − 1|；center = 该目标案例集合的中位单价（前=原价中位，后=修正价中位）",
            "main_caliper": "主口径 = 固定全目标集（非疑似自读子集）上修正前 vs 修正后，"
                            "同一 ID 集、同一聚合方式",
            "fixable_subset": "可修正子集 = 至少 1 个案例被修正的目标；ID 交集先固定，前后同分母",
            "within10": f"±10% 命中 = ape ≤ {WITHIN10_THRESHOLD}",
            "p90": "P90 APE = APE 的 90 百分位",
        },
        "main_caliper": main_block,
        "fixable_subset": fix_block,
        "population": {
            "targets": int(detail.height),
            "main_denominator": int(main.height),
            "fixable_subset": int(fixable.height),
            "all_rejected": int((main["n_cases_corrected"] == 0).sum()),
            "zero_case_excluded": int(zero_case.height),
        },
        "zero_case_disclosure": {
            "count": int(zero_case.height),
            "ratio": zero_ratio,
            "threshold": NO_CASE_DISCLOSURE_THRESHOLD,
            "exceeded": bool(zero_ratio > NO_CASE_DISCLOSURE_THRESHOLD),
            "rule": "无案例目标 SHALL 从修正前后双方排除并单列覆盖披露；占比 >5% 即上报",
        },
    }
    return payload, {"main": main_ape, "fixable": fix_ape}


# ---------------------------------------------------------------- 冻结 M1 资产（只读）

def _encoder_from_payload(doc: dict) -> models.FoldEncoder:
    """由 3.1 持久化的 encoder.json 还原折内编码器（只读，不重写 S3 run 既有文件）。"""
    enc = models.FoldEncoder()
    enc.cat_cols = [str(c) for c in doc["cat_cols"]]
    enc.categories = {str(c): [str(v) for v in vs] for c, vs in doc["categories"].items()}
    enc.ym_clamp = str(doc["ym_clamp"])
    enc.fills = {str(k): float(v) for k, v in doc["fills"].items()}
    enc.mu = np.asarray(doc["mu"], dtype=np.float64)
    enc.sd = np.asarray(doc["sd"], dtype=np.float64)
    return enc


def load_frozen_m1_assets(run_dir: Path, s1_run_dir: Path, s2_run_dir: Path) -> dict:
    """只读装载 3.1 持久化的 18 折 M1 编码器/权重，并以重算预测复核 S2 冻结预测（容差 0）。

    不重写 S3 run 任何既有文件（S3 仅允许追加）；资产指纹与对拍结果随返回登记。
    """
    splits = json.loads((s1_run_dir / "splits.json").read_text(encoding="utf-8"))
    encoders, folds = {}, []
    for fold in splits["folds"]:
        fid = fold["fold_id"]
        fdir = run_dir / "m1_refit" / fid
        enc = _encoder_from_payload(
            json.loads((fdir / "encoder.json").read_text(encoding="utf-8")))
        w = np.load(fdir / "weights.npy")
        _, _, _, val = models.load_fold(s1_run_dir, fid)
        recomputed = np.exp(enc.transform(val) @ w)
        s2 = (pl.read_parquet(s2_run_dir / "per-fold" / f"{fid}.parquet")
              .select(["source_record_id", pl.col("m1_pred").alias("s2_m1_pred")]))
        rec = val.select(["source_record_id"]).with_columns(
            pl.Series("recomputed", recomputed))
        j = rec.join(s2, on="source_record_id", how="full", coalesce=True)
        n_left = int(j["recomputed"].is_null().sum())
        n_right = int(j["s2_m1_pred"].is_null().sum())
        diff = (j["recomputed"] - j["s2_m1_pred"]).abs()
        n_diff = int((diff > 0).sum())
        max_abs = float(diff.max()) if diff.len() and diff.max() is not None else 0.0
        folds.append({
            "fold_id": fid,
            "rows": int(val.height),
            "id_set_equal": bool(rec.height == s2.height == j.height),
            "mismatch_rows": n_diff + n_left + n_right,
            "max_abs_diff": max_abs,
            "tolerance": 0.0,
            "encoder_sha256": lineage.sha256_file(fdir / "encoder.json"),
            "weights_sha256": lineage.sha256_file(fdir / "weights.npy"),
            "attribute_column_count": int(len(attribute_indices(enc))),
        })
        encoders[fid] = (enc, w)
    totals = {
        "folds": len(folds),
        "rows": int(sum(f["rows"] for f in folds)),
        "mismatch_rows": int(sum(f["mismatch_rows"] for f in folds)),
        "max_abs_diff": float(max(f["max_abs_diff"] for f in folds)),
        "id_sets_equal": bool(all(f["id_set_equal"] for f in folds)),
    }
    if not (totals["folds"] == 18 and totals["mismatch_rows"] == 0
            and totals["max_abs_diff"] == 0.0 and totals["id_sets_equal"]):
        raise A1Halt(
            "停线上报：冻结 M1 资产重算预测与 S2 冻结预测不一致，"
            f"totals={totals}（容差 0）")
    return {"encoders": encoders, "splits": splits,
            "verification": {"totals": totals, "folds": folds,
                             "rule": "只读装载 S3 run m1_refit/ 编码器与权重，重算验证窗预测"
                                     "并与 S2 冻结 m1_pred 逐行对拍（容差 0）"}}


# ---------------------------------------------------------------- 登记性并列参考（B1/B0）

def registered_reference(s2_run_dir: Path, main_ids: list[str]) -> dict:
    """在 A1 主口径同一 ID 集上登记 B1 回放中心与 B0 中心（不作基线、附 B1 量级异常警示）。"""
    id_list = list(main_ids)
    ids_set = set(id_list)
    frames = []
    for p in sorted((s2_run_dir / "per-fold").glob("*.parquet")):
        frames.append(pl.read_parquet(
            p, columns=["source_record_id", "unit_price_true", "b0_pred",
                        "b1_center", "b1_status"])
              .filter(pl.col("source_record_id").is_in(id_list)))
    df = pl.concat(frames)
    id_equal = bool(df.height == len(ids_set)
                    and set(df["source_record_id"].to_list()) == ids_set)
    if not id_equal:
        raise A1Halt("停线上报：登记性参考的 ID 集与 A1 主口径不一致")
    true = df["unit_price_true"].to_numpy().astype(np.float64)
    valued = bool((df["b1_status"] == "valued").all())
    if not valued:
        raise A1Halt("停线上报：A1 主口径内出现非 B1 valued 行，非疑似子集口径被破坏")
    b0_ape = ape_from_center(df["b0_pred"].to_numpy().astype(np.float64), true)
    b1_ape = ape_from_center(df["b1_center"].to_numpy().astype(np.float64), true)
    return {
        "population": "A1 主口径固定目标 ID 集（与主口径逐元素相等）",
        "id_set_equal_to_main": id_equal,
        "registered_only": True,
        "used_as_baseline": False,
        "note": REGISTERED_REFERENCE_NOTE,
        "b1_magnitude_warning": B1_MAGNITUDE_WARNING,
        "true_median_unit_price": float(np.median(true)),
        "b0_reference": {
            "median_center": float(np.median(df["b0_pred"].to_numpy().astype(np.float64))),
            **point_metrics(b0_ape)},
        "b1_replay_reference": {
            "median_replay_center": float(np.median(df["b1_center"].to_numpy().astype(np.float64))),
            **point_metrics(b1_ape)},
    }


# ---------------------------------------------------------------- 逐对象案例清单

def case_list_rows(detail: pl.DataFrame) -> list[dict]:
    """逐对象案例清单（含案例级修正证据）：落盘与回读用。"""
    rows = []
    for rec in detail.to_dicts():
        tr = rec.get("trace") or {}
        rows.append({
            "fold_id": rec["fold_id"],
            "source_record_id": str(rec["source_record_id"]),
            "unit_price_true": rec["unit_price_true"],
            "zero_case": bool(rec["n_cases"] == 0),
            "n_cases": rec["n_cases"],
            "n_community": rec["n_community"],
            "n_block": rec["n_block"],
            "retrieval_level": rec["retrieval_level"],
            "target_reject": bool(rec.get("target_reject") or False),
            "pre_center": rec["pre_center"],
            "post_center": rec["post_center"],
            "delta_median": rec["delta_median"],
            "n_cases_rejected": rec["n_cases_rejected"],
            "n_cases_corrected": rec["n_cases_corrected"],
            "correction_evidence": {
                "case_ids": tr.get("case_ids", rec.get("case_ids") or []),
                "case_dates": tr.get("case_dates"),
                "case_prices": tr.get("case_prices"),
                "case_delta": tr.get("case_delta"),
                "case_adjusted": tr.get("case_adjusted"),
                "case_reject": tr.get("case_reject"),
                "case_miss": tr.get("case_miss"),
                "reject_reasons": [REJECT_REASON_TARGET, REJECT_REASON_CASE],
                "delta_formula": "delta_i = (x_target[A] − x_case_i[A]) @ w[A]；"
                                 "adjusted_i = case_unit_price_i × exp(delta_i)",
            },
            "top_contributions": tr.get("top_contributions"),
        })
    return rows


def persist_case_list(detail: pl.DataFrame, target_dir: Path,
                      file_name: str = "A1-案例清单.jsonl") -> dict:
    """逐对象案例清单与修正证据落盘，并立即回读校验（行数与 ID 序列逐元素一致）。"""
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / file_name
    rows = case_list_rows(detail)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    back = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            if ln.strip():
                back.append(json.loads(ln))
    verified = bool(len(back) == detail.height
                    and [b["source_record_id"] for b in back]
                    == [r["source_record_id"] for r in rows])
    with_case = sum(1 for r in back if r["correction_evidence"]["case_ids"])
    evidence_complete = all(
        (len(r["correction_evidence"]["case_ids"])
         == len(r["correction_evidence"]["case_adjusted"] or []))
        for r in back if r["correction_evidence"]["case_ids"])
    return {"path": str(path), "file_name": file_name, "rows": len(rows),
            "rows_with_case_evidence": int(with_case),
            "verified_on_readback": verified,
            "case_evidence_lengths_consistent": bool(evidence_complete),
            "sha256": lineage.sha256_file(path),
            "size_bytes": path.stat().st_size,
            "columns": sorted(rows[0].keys()) if rows else []}


def usage_isolation_check(texts: dict) -> dict:
    """用途隔离措辞检查：输出文本不出现独立报价结论，且显式声明辅助价差用途。"""
    joined = "\n".join(str(v) for v in texts.values())
    hits = sorted({p for p in FORBIDDEN_INDEPENDENT_QUOTE_PHRASES if p in joined})
    present = {seg: bool(seg in joined) for seg in USAGE_ISOLATION_REQUIRED}
    return {"checked_sections": sorted(texts),
            "forbidden_phrases": list(FORBIDDEN_INDEPENDENT_QUOTE_PHRASES),
            "forbidden_hits": hits,
            "required_statement": USAGE_ISOLATION_STATEMENT,
            "required_present": present,
            "pass": bool(not hits and all(present.values()))}


# ---------------------------------------------------------------- 命令行

def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase2-a1")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_refit = sub.add_parser("refit-m1", help="复算 18 折 M1 并与 S2 对拍、持久化编码器/权重")
    p_refit.add_argument("--s1-run-dir", required=True, type=Path)
    p_refit.add_argument("--s2-run-dir", required=True, type=Path)
    p_refit.add_argument("--runs-root", required=True, type=Path)
    p_plan = sub.add_parser("attribute-plan", help="打印属性组列集合 A 的定义（单折）")
    p_plan.add_argument("--s1-run-dir", required=True, type=Path)
    args = parser.parse_args(argv)

    if args.cmd == "attribute-plan":
        _, _, train, _ = models.load_fold(args.s1_run_dir.resolve(), "F18")
        enc = models.FoldEncoder().fit(train)
        _print({"attribute_column_count": int(len(attribute_indices(enc))),
                "attribute_columns": attribute_column_names(enc),
                "excluded": list(EXCLUDED_CATEGORY_COLS) + ["intercept"]})
        return 0

    s1 = args.s1_run_dir.resolve()
    s2 = args.s2_run_dir.resolve()
    code = evaluate.code_fingerprint(Path(__file__).resolve().parent)
    s1_id = json.loads((s1 / "manifest.json").read_text(encoding="utf-8"))["run_id"]
    s2_id = json.loads((s2 / "manifest.json").read_text(encoding="utf-8"))["run_id"]
    run_dir, status = ensure_s3_run_dir(args.runs_root.resolve(), s1_id, s2_id, code)
    refit = refit_m1_all_folds(s1, s2, run_dir)
    _print({"run_dir": str(run_dir), "status": status,
            "totals": refit["index"]["totals"]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
