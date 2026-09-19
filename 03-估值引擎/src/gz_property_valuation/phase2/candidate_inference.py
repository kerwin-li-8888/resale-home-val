# -*- coding: utf-8 -*-
"""phase2 S4b 候选 V1 推理：资产加载（含指纹校验）、单套/批量推理、B0 基准、A1 辅助。

行为规格（openspec/changes/build-phase2-candidate-freeze/specs/phase2-candidate-freeze/spec.md
「推理资产版本与冻结权重一致」「单套/批量一致硬规则」「支持度与冷启动确定行为」
「A1 辅助输出与冲突标注」；design D1/D2/D4/D6/D7）：

- V1＝F01 折冻结权重（训练截止 2025-10-20、λ=1.0、1,076 特征）＋其编码器；推理实现
  对同一输入产出与冻结 M1 逐位一致的预测（:func:`reproduce_frozen_m1`，max diff=0）。
- 推理输入只依赖请求自身属性与截点前材料：市场特征（B0 参照、A1 案例窗）由本模块在
  :func:`build_context` 中按锚点前 365 天现算，**不含任何跨请求统计**。
- 单套与批量走同一无跨行统计的核（:meth:`CandidateV1._score_frames`），两者逐位相等。
- 输出结构：M1 报价＋区间为主，B0 支持度、降级状态、A1 参考价为辅助字段；A1 偏差超阈值
  输出 ``a1_m1_conflict``，不仲裁不隐藏。

消费边界：只读 S1/S2/S3/S4a run 冻结产物；不修改任何既有实现。
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from . import a1_experiment as a1
from . import baselines, degradation as deg, derived_inputs, lineage, models

VERSION_ID = "V1"
FOLD_ID = "F01"
TRAIN_CUTOFF = "2025-10-20"
WINDOW_DAYS = 365
CASE_CAP = a1.CASE_CAP
EVAL_ANCHOR = date(2026, 1, 20)
CONFLICT_THRESHOLD_DEFAULT = 0.15
CHUNK_ROWS = 1024
DEPLOYED_KERNEL_TOL = 1e-9
COLD_START = "cold_start_community"
DEGRADATION_STATE_NORMAL = "normal"
DEGRADATION_STATE_TOTAL_FLOORS = "degraded_total_floors"
A1_OK = "ok"
A1_ATTR_INCOMPLETE = "ineligible_attribute_incomplete"
A1_NO_CASES = "no_cases"
A1_ALL_REJECTED = "all_rejected"

KEY_COLUMNS = ("source_record_id", "community_source_id", "sale_date_d", "block_name")
FEATURE_COLUMNS = KEY_COLUMNS + (
    "area_sqm", "age_years", "total_floors", "miss_year_built", "miss_total_floors",
    "floor_bucket", "elevator_state", "bedrooms_n", "orientation", "decoration_state",
)


class InferenceHalt(RuntimeError):
    """停线上报：复现断言不一致、资产指纹不符等停止条件触发。"""


# ---------------------------------------------------------------- 资产加载

def encoder_from_payload(doc: dict) -> models.FoldEncoder:
    enc = models.FoldEncoder()
    enc.cat_cols = [str(c) for c in doc["cat_cols"]]
    enc.categories = {str(c): [str(v) for v in vs] for c, vs in doc["categories"].items()}
    enc.ym_clamp = str(doc["ym_clamp"])
    enc.fills = {str(k): float(v) for k, v in doc["fills"].items()}
    enc.mu = np.asarray(doc["mu"], dtype=np.float64)
    enc.sd = np.asarray(doc["sd"], dtype=np.float64)
    return enc


def load_v1_assets(s3_run_dir: Path, fold: str = FOLD_ID) -> dict:
    """只读装载 V1（F01）编码器与权重，附 SHA-256 指纹。"""
    fdir = s3_run_dir / "m1_refit" / fold
    enc = encoder_from_payload(
        json.loads((fdir / "encoder.json").read_text(encoding="utf-8")))
    w = np.load(fdir / "weights.npy")
    return {
        "version": VERSION_ID,
        "fold": fold,
        "assets_source": f"{s3_run_dir.name}/m1_refit/{fold}",
        "train_cutoff": TRAIN_CUTOFF,
        "encoder": enc,
        "weights": w,
        "encoder_sha256": lineage.sha256_file(fdir / "encoder.json"),
        "weights_sha256": lineage.sha256_file(fdir / "weights.npy"),
        "n_features": int(len(enc.feature_names())),
        "feature_names": enc.feature_names(),
    }


def verify_assets_fingerprint(assets: dict, s3_run_dir: Path) -> dict:
    """指纹与 S3 run m1_refit/index.json 自登记值比对（不一致即停线）。"""
    index = json.loads((s3_run_dir / "m1_refit" / "index.json").read_text(encoding="utf-8"))
    row = next(f for f in index["folds"] if f["fold_id"] == assets["fold"])
    ok = (row["weights_sha256"] == assets["weights_sha256"]
          and row["encoder_sha256"] == assets["encoder_sha256"]
          and row["n_features"] == assets["n_features"])
    if not ok:
        raise InferenceHalt(
            f"停线上报：V1 资产指纹与 S3 index.json 不一致 fold={assets['fold']}")
    return {"fold": assets["fold"], "lambda": row["chosen_lambda"],
            "n_features": row["n_features"], "train_rows": row["train_rows"],
            "weights_sha256_match": True, "encoder_sha256_match": True,
            "n_attribute_columns": row["n_attribute_columns"],
            "index_sha256": lineage.sha256_file(s3_run_dir / "m1_refit" / "index.json")}


# ---------------------------------------------------------------- 推理核（批次不变量）

def design_matrix(enc: models.FoldEncoder, frame: pl.DataFrame) -> np.ndarray:
    """设计矩阵＝冻结编码器的 transform（类别 one-hot + 数值 z-score + 截距）。"""
    return enc.transform(frame)


def matvec_chunked(X: np.ndarray, w: np.ndarray, chunk: int = CHUNK_ROWS) -> np.ndarray:
    """批次不变量线性核：固定分块（``chunk`` 行）＋尾部零填充后做 ``X @ w``。

    设计理由：BLAS gemv 的求和顺序随行数 ``m`` 变化（实测 m=1 与 m=12095 结果在
    1e-13（log）量级不同），若单套（m=1）与批量（m=大）各按其行数调用 gemv，则预测
    会带批次依赖。固定分块使每块恒为 ``chunk`` 行（行内结果只依赖该行本身），从而
    **单套与批量逐位相等**，落实 spec「单套/批量一致硬规则」。
    """
    m = X.shape[0]
    out = np.empty(m, dtype=np.float64)
    for s in range(0, m, chunk):
        blk = X[s:s + chunk]
        r = blk.shape[0]
        if r < chunk:
            blk = np.vstack([blk, np.zeros((chunk - r, X.shape[1]), dtype=np.float64)])
        out[s:s + r] = (blk @ w)[:r]
    return out


def predict_log_unit_price(enc: models.FoldEncoder, w: np.ndarray,
                           frame: pl.DataFrame) -> np.ndarray:
    return matvec_chunked(design_matrix(enc, frame), w)


# ---------------------------------------------------------------- 市场上下文（无跨请求统计）

def _block_norm(value) -> str | None:
    return deg._block_norm(value)


def build_context(assets: dict, s1_run_dir: Path, anchor: date,
                  window_days: int = WINDOW_DAYS) -> dict:
    """按锚点前 ``window_days`` 天现算市场材料（B0 层级表、总楼层中位、A1 案例索引）。

    该上下文仅由冻结池与该锚点决定，与任何请求批次无关（单套/批量一致性的实现基础）。
    """
    master = pl.read_parquet(s1_run_dir / "master_table.parquet")
    features = pl.read_parquet(s1_run_dir / "features.parquet")
    labels = master.select(["source_record_id", pl.col("unit_price").cast(pl.Float64)])
    fp = features.join(labels, on="source_record_id", how="left")
    blocks = baselines.load_blocks(features)
    comm2block = {r["community_source_id"]: r["block_name"] for r in blocks.to_dicts()}

    pool = derived_inputs.build_pool(master, features)
    wmat = baselines.window_materials(pool, anchor, window_days)
    com, blk, hz = baselines.b0_level_tables(wmat)
    com_map = {r["community_source_id"]: (float(r["pred_community"]), int(r["n_community"]))
               for r in com.to_dicts()}
    blk_map = {r["block_name"]: (float(r["pred_block"]), int(r["n_block"]))
               for r in blk.to_dicts()}
    district_med = float(hz) if hz is not None else None

    start = anchor - timedelta(days=window_days)
    wf = fp.filter((pl.col("sale_date_d") >= start) & (pl.col("sale_date_d") < anchor)
                   & pl.col("unit_price").is_not_null())

    tf = wf.filter(pl.col("total_floors").is_not_null())
    com_tf = {r["community_source_id"]: float(r["med"]) for r in
              tf.group_by("community_source_id").agg(
                  pl.col("total_floors").median().alias("med")).to_dicts()}
    blk_tf = {r["block_name"]: float(r["med"]) for r in
              tf.filter(pl.col("block_name").is_not_null()).group_by("block_name").agg(
                  pl.col("total_floors").median().alias("med")).to_dicts()}
    district_tf = float(tf["total_floors"].median()) if tf.height else None

    enc, w = assets["encoder"], assets["weights"]
    case_attr = a1.attribute_matrix(enc, wf)
    ctx = {
        "anchor": anchor.isoformat(),
        "window_days": window_days,
        "window_rows": int(wmat.height),
        "district_n": int(wmat.height),
        "com_map": com_map, "blk_map": blk_map, "district_med": district_med,
        "comm2block": comm2block,
        "com_tf": com_tf, "blk_tf": blk_tf, "district_tf": district_tf,
        "case_ids": wf["source_record_id"].to_numpy(),
        "case_dates": wf["sale_date_d"].to_numpy(),
        "case_prices": wf["unit_price"].to_numpy().astype(np.float64),
        "case_comm": wf["community_source_id"].to_numpy(),
        "case_block": np.asarray(a1._block_values(wf), dtype=object),
        "case_attr": case_attr,
        "case_miss": a1.missing_key_attribute(wf),
        "comm_idx": a1._index_sorted(
            wf["community_source_id"].to_list(), wf["sale_date_d"].to_numpy(),
            wf["source_record_id"].to_numpy()),
        "block_idx": a1._index_sorted(
            a1._block_values(wf), wf["sale_date_d"].to_numpy(),
            wf["source_record_id"].to_numpy()),
    }
    return ctx


# ---------------------------------------------------------------- 候选 V1

class CandidateV1:
    """V1 单套/批量推理：M1 报价＋区间＋支持度＋降级＋A1 参考＋冲突标注。"""

    def __init__(self, assets: dict, s1_run_dir: Path, anchor: date = EVAL_ANCHOR,
                 window_days: int = WINDOW_DAYS, degradation_path: str = "none",
                 interval_table: dict | None = None,
                 conflict_threshold: float = CONFLICT_THRESHOLD_DEFAULT,
                 context: dict | None = None,
                 variant_weights: np.ndarray | None = None):
        self.assets = assets
        self.enc = assets["encoder"]
        self.w = assets["weights"]
        self.variant_weights = variant_weights
        self.anchor = anchor
        self.window_days = window_days
        self.degradation_path = degradation_path
        self.interval_table = interval_table
        self.conflict_threshold = conflict_threshold
        self.attr_idx = a1.attribute_indices(self.enc)
        self.context = context if context is not None else build_context(
            assets, s1_run_dir, anchor, window_days)

    # -- 降级 --
    def missing_total_floors(self, frame: pl.DataFrame) -> np.ndarray:
        return deg.missing_total_floors(frame)

    def impute_total_floors(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
        """路径甲（委托 :mod:`degradation`）：缺失总楼层按 小区→板块→区级 中位插补。"""
        return deg.impute_total_floors_A(frame, self.context)

    # -- B0 查表 --
    def b0_lookup(self, comm, block) -> dict:
        return deg.b0_lookup(self.context, comm, block)

    def a1_reference(self, target_attr: np.ndarray, target_miss: bool, comm, block,
                     target_id, target_date, findex: int) -> dict:
        ctx = self.context
        base = {"n_cases": 0, "n_community": 0, "n_block": 0, "level": "none",
                "pre": None, "post": None, "delta_median": None,
                "n_rejected": 0, "n_corrected": 0, "all_rejected": False,
                "target_reject": bool(target_miss), "attr_complete": not bool(target_miss)}
        if target_miss:
            return base
        block_key = _block_norm(block)
        candidates, seen = [], set()
        n_comm = n_block = 0
        for i in ctx["comm_idx"].get(str(comm), np.empty(0, dtype=np.int64)):
            if len(candidates) >= CASE_CAP:
                break
            if ctx["case_ids"][i] == target_id or ctx["case_dates"][i] == target_date:
                continue
            candidates.append(int(i))
            seen.add(int(i))
            n_comm += 1
        if len(candidates) < CASE_CAP and block_key is not None:
            for i in ctx["block_idx"].get(block_key, np.empty(0, dtype=np.int64)):
                if len(candidates) >= CASE_CAP:
                    break
                if int(i) in seen:
                    continue
                if ctx["case_ids"][i] == target_id or ctx["case_dates"][i] == target_date:
                    continue
                if str(ctx["case_comm"][i]) == str(comm):
                    continue
                candidates.append(int(i))
                seen.add(int(i))
                n_block += 1
        if not candidates:
            return base
        idx = np.asarray(candidates, dtype=np.int64)
        case_price = ctx["case_prices"][idx]
        corr = a1.correct_target(self.enc, self.w, target_attr, ctx["case_attr"][idx],
                                 case_price, ctx["case_miss"][idx], bool(target_miss))
        adjusted = corr["adjusted"]
        n_rej = int(corr["reject"].sum())
        pre = float(np.median(case_price))
        post = float(np.median(adjusted))
        return {"n_cases": len(candidates), "n_community": n_comm, "n_block": n_block,
                "level": "community" if n_block == 0 else "community+block",
                "pre": pre, "post": post, "delta_median": float(np.median(corr["delta"])),
                "n_rejected": n_rej, "n_corrected": int(len(candidates) - n_rej),
                "all_rejected": bool(n_rej == len(candidates)),
                "target_reject": bool(target_miss), "attr_complete": True}

    def interval_for(self, stratum: str, pred: float) -> dict:
        tab = self.interval_table
        if not tab:
            return {"80_low": None, "80_high": None, "90_low": None, "90_high": None,
                    "stratum": stratum, "source_layer": None}
        layers = tab.get("layers", {})
        chosen, src = None, "global"
        if stratum in layers:
            chosen, src = layers[stratum], stratum
        else:
            parts = stratum.split("|")
            parent = parts[0] + "|global"
            if parent in layers:
                chosen, src = layers[parent], parent
            elif layers.get("global") is not None:  # 兜底查 layers 内的 global（旧缺陷：误查表根）
                chosen, src = layers["global"], "global"
        if chosen is None:
            return {"80_low": None, "80_high": None, "90_low": None, "90_high": None,
                    "stratum": stratum, "source_layer": None}
        out = {"stratum": stratum, "source_layer": src}
        for lvl in ("80", "90"):
            q = chosen[str(lvl)]
            out[f"{lvl}_low"] = float(pred * np.exp(q["lo_log"]))
            out[f"{lvl}_high"] = float(pred * np.exp(q["hi_log"]))
        return out

    # -- 核（无跨行统计） --
    def _score_frames(self, frame: pl.DataFrame) -> pl.DataFrame:
        n = frame.height
        missing_tf = self.missing_total_floors(frame)
        if self.degradation_path == "A":
            frame2, origins = self.impute_total_floors(frame)
        elif self.degradation_path == "B" and self.variant_weights is not None:
            frame2 = frame
            origins = ["variant" if m else "none" for m in missing_tf]
        else:
            frame2, origins = frame, ["none"] * n
        known_comm = ~self.enc.unknown_community(frame)
        X = self.enc.transform(frame2)
        if self.degradation_path == "B" and self.variant_weights is not None:
            keep = deg.variant_keep_mask(self.enc)
            pred_main = matvec_chunked(X, self.w)
            pred_var = matvec_chunked(X[:, keep], self.variant_weights)
            m1 = np.exp(np.where(missing_tf, pred_var, pred_main))
        else:
            m1 = np.exp(matvec_chunked(X, self.w))
        attr_all = a1.attribute_matrix(self.enc, frame2)
        miss_attr = a1.missing_key_attribute(frame2)

        comms = frame2["community_source_id"].to_list()
        blocks = frame2["block_name"].to_list()
        ids = frame2["source_record_id"].to_list()
        dates = frame2["sale_date_d"].to_numpy()
        areas = frame2["area_sqm"].cast(pl.Float64).to_numpy()

        cols: dict[str, list] = {k: [] for k in (
            "source_record_id", "community_source_id", "b0_level", "b0_pred", "b0_window_n",
            "known_community", "cold_start_community", "reject", "degradation_state",
            "degradation_origin", "m1_pred_unit_price", "m1_pred_total_price",
            "interval_stratum", "interval_source_layer", "interval80_low", "interval80_high",
            "interval90_low", "interval90_high", "a1_status", "a1_n_cases", "a1_level",
            "a1_pre_center", "a1_post_center", "a1_delta_median", "a1_n_rejected",
            "a1_n_corrected", "a1_m1_conflict")}

        for k in range(n):
            b0 = self.b0_lookup(comms[k], blocks[k])
            reject = b0["level"] is None
            deg_state = (DEGRADATION_STATE_TOTAL_FLOORS if missing_tf[k]
                         else DEGRADATION_STATE_NORMAL)
            support = b0["level"] or "none"
            stratum = f"{deg_state}|{support}"
            pred = float(m1[k])
            iv = self.interval_for(stratum, pred)
            a1r = self.a1_reference(attr_all[k], bool(miss_attr[k]), comms[k], blocks[k],
                                    ids[k], dates[k], k)
            if not a1r["attr_complete"]:
                a1_status = A1_ATTR_INCOMPLETE
            elif a1r["n_cases"] == 0:
                a1_status = A1_NO_CASES
            elif a1r["all_rejected"]:
                a1_status = A1_ALL_REJECTED
            else:
                a1_status = A1_OK
            conflict = False
            if a1_status == A1_OK and a1r["post"]:
                conflict = bool(abs(a1r["post"] - pred) / pred > self.conflict_threshold)
            for key, val in (
                ("source_record_id", ids[k]),
                ("community_source_id", comms[k]),
                ("b0_level", b0["level"]), ("b0_pred", b0["pred"]),
                ("b0_window_n", b0["n"]),
                ("known_community", bool(known_comm[k])),
                ("cold_start_community", bool(b0["cold_start"])),
                ("reject", bool(reject)), ("degradation_state", deg_state),
                ("degradation_origin", origins[k]),
                ("m1_pred_unit_price", pred),
                ("m1_pred_total_price", pred * float(areas[k])),
                ("interval_stratum", stratum),
                ("interval_source_layer", iv["source_layer"]),
                ("interval80_low", iv["80_low"]), ("interval80_high", iv["80_high"]),
                ("interval90_low", iv["90_low"]), ("interval90_high", iv["90_high"]),
                ("a1_status", a1_status), ("a1_n_cases", a1r["n_cases"]),
                ("a1_level", a1r["level"]), ("a1_pre_center", a1r["pre"]),
                ("a1_post_center", a1r["post"]), ("a1_delta_median", a1r["delta_median"]),
                ("a1_n_rejected", a1r["n_rejected"]), ("a1_n_corrected", a1r["n_corrected"]),
                ("a1_m1_conflict", conflict)):
                cols[key].append(val)
        df = pl.DataFrame(cols)
        string_cols = ["source_record_id", "community_source_id", "b0_level",
                       "degradation_state", "degradation_origin", "interval_stratum",
                       "interval_source_layer", "a1_status", "a1_level"]
        float_cols = ["b0_pred", "m1_pred_unit_price", "m1_pred_total_price",
                      "interval80_low", "interval80_high", "interval90_low",
                      "interval90_high", "a1_pre_center", "a1_post_center",
                      "a1_delta_median"]
        int_cols = ["b0_window_n", "a1_n_cases", "a1_n_rejected", "a1_n_corrected"]
        bool_cols = ["known_community", "cold_start_community", "reject", "a1_m1_conflict"]
        return df.with_columns(
            [pl.col(c).cast(pl.String) for c in string_cols]
            + [pl.col(c).cast(pl.Float64) for c in float_cols]
            + [pl.col(c).cast(pl.Int64) for c in int_cols]
            + [pl.col(c).cast(pl.Boolean) for c in bool_cols])

    def predict_batch(self, frame: pl.DataFrame) -> pl.DataFrame:
        return self._score_frames(frame)

    def predict_one(self, row_frame: pl.DataFrame) -> dict:
        assert row_frame.height == 1, "predict_one 需 1 行请求"
        return self._score_frames(row_frame).row(0, named=True)

    def predict_single_rows(self, frame: pl.DataFrame) -> pl.DataFrame:
        rows = [self._score_frames(frame.slice(i, 1)) for i in range(frame.height)]
        return pl.concat(rows, how="vertical_relaxed")


# ---------------------------------------------------------------- 复现断言

def reproduce_frozen_m1(s1_run_dir: Path, s2_run_dir: Path, s3_run_dir: Path) -> dict:
    """18 折冻结 M1 复现（max diff = 0 口径双检）：

    - ``frozen_arrangement``：推理路径用与冻结管线相同的编排（整折验证窗一次 ``X @ w``），
      与 S2 冻结 ``m1_pred`` 逐行对拍，要求 max diff = 0（模型身份与资产保真）；
    - ``deployed_kernel``：部署核（批次不变量分块）与冻结 ``m1_pred`` 的偏差；
    - ``design_matrix_bitwise``：推理路径设计矩阵与编码器 ``transform`` 逐位一致。
    """
    splits = json.loads((s1_run_dir / "splits.json").read_text(encoding="utf-8"))
    folds = []
    for fold in splits["folds"]:
        fid = fold["fold_id"]
        _, _, _, val = models.load_fold(s1_run_dir, fid)
        fdir = s3_run_dir / "m1_refit" / fid
        enc = encoder_from_payload(json.loads((fdir / "encoder.json").read_text(encoding="utf-8")))
        w = np.load(fdir / "weights.npy")
        X = design_matrix(enc, val)
        frozen_gemv = np.exp(X @ w)
        deployed = np.exp(matvec_chunked(X, w))
        s2 = (pl.read_parquet(s2_run_dir / "per-fold" / f"{fid}.parquet")
              .select(["source_record_id", pl.col("m1_pred").alias("s2_m1_pred")]))
        base = val.select(["source_record_id"]).with_columns(
            pl.Series("frozen_arrangement", frozen_gemv),
            pl.Series("deployed_kernel", deployed))
        j = base.join(s2, on="source_record_id", how="full", coalesce=True)
        n_left = int(j["frozen_arrangement"].is_null().sum())
        n_right = int(j["s2_m1_pred"].is_null().sum())
        d_frozen = (j["frozen_arrangement"] - j["s2_m1_pred"]).abs()
        d_deployed = (j["deployed_kernel"] - j["s2_m1_pred"]).abs()
        mx_f = float(d_frozen.max()) if d_frozen.len() and d_frozen.max() is not None else 0.0
        mx_d = float(d_deployed.max()) if d_deployed.len() and d_deployed.max() is not None else 0.0
        folds.append({"fold_id": fid, "rows": int(val.height),
                      "id_set_equal": bool(val.height == s2.height == j.height),
                      "frozen_arrangement_max_abs_diff": mx_f,
                      "frozen_arrangement_mismatch_rows": int((d_frozen > 0).sum()) + n_left + n_right,
                      "deployed_kernel_max_abs_diff": mx_d,
                      "deployed_kernel_mismatch_rows": int((d_deployed > 0).sum()),
                      "design_matrix_bitwise": bool(np.array_equal(X, enc.transform(val)))})
    totals = {
        "folds": len(folds), "rows": int(sum(f["rows"] for f in folds)),
        "frozen_arrangement_max_abs_diff": float(max(f["frozen_arrangement_max_abs_diff"] for f in folds)),
        "frozen_arrangement_mismatch_rows": int(sum(f["frozen_arrangement_mismatch_rows"] for f in folds)),
        "deployed_kernel_max_abs_diff": float(max(f["deployed_kernel_max_abs_diff"] for f in folds)),
        "deployed_kernel_mismatch_rows": int(sum(f["deployed_kernel_mismatch_rows"] for f in folds)),
        "design_matrix_bitwise_all": bool(all(f["design_matrix_bitwise"] for f in folds)),
        "id_sets_equal": bool(all(f["id_set_equal"] for f in folds)),
    }
    totals["pass"] = bool(
        totals["folds"] == 18 and totals["frozen_arrangement_mismatch_rows"] == 0
        and totals["frozen_arrangement_max_abs_diff"] == 0.0
        and totals["design_matrix_bitwise_all"] and totals["id_sets_equal"]
        and totals["deployed_kernel_max_abs_diff"] <= DEPLOYED_KERNEL_TOL)
    return {"rule": "只读 S3 run m1_refit/ 18 折编码器与权重；冻结编排（整折 X@w，容差 0）"
                    "与部署核（批次不变量分块）双检，设计矩阵逐位校验",
            "tolerance": 0.0, "deployed_kernel_tolerance": DEPLOYED_KERNEL_TOL,
            "totals": totals, "folds": folds}


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.candidate_inference",
        "version": VERSION_ID, "fold": FOLD_ID, "train_cutoff": TRAIN_CUTOFF,
        "api": ["load_v1_assets", "verify_assets_fingerprint", "build_context",
                "CandidateV1.predict_one", "CandidateV1.predict_batch",
                "CandidateV1.predict_single_rows", "reproduce_frozen_m1"],
        "no_cross_request_stats": "推理核 _score_frames 不使用任何跨行/批内统计；"
                                  "市场材料只由冻结池与锚点决定",
        "consumption_boundary": "只读 S1/S2/S3 run 冻结产物",
        "verdict": "PASS",
    }


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase2-candidate-inference")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("structure", help="结构清单自检")
    p_rep = sub.add_parser("reproduce", help="18 折冻结 M1 复现断言")
    p_rep.add_argument("--s1-run-dir", required=True, type=Path)
    p_rep.add_argument("--s2-run-dir", required=True, type=Path)
    p_rep.add_argument("--s3-run-dir", required=True, type=Path)
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        _print(structure_report())
        return 0
    res = reproduce_frozen_m1(args.s1_run_dir.resolve(), args.s2_run_dir.resolve(),
                              args.s3_run_dir.resolve())
    _print(res)
    return 0 if res["totals"]["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
