# -*- coding: utf-8 -*-
"""phase2 S3 非线性模型：M2 / M3a / M3b / M4 折内统一 fit/predict（design D3/D4/D5）。

行为规格（specs/phase2-nonlinear-comparisons/spec.md「M2 直接报价模型的折内拟合边界」
「M3 局部基准价差结构」「M4 挑战模型的预算对等与折内调参」）：

- M2 CatBoost 直接报价 ``log(成交单价)``：输入 = S1 静态属性 + D2 派生市场特征（特征集 v2）；
  类别特征原生处理，类别码表仅由该层训练材料生成（折内类别编码，禁止先全量编码再切分）；
  有界网格 8 配置先固定后运行；配置选择仅用本折内层材料（``inner_train`` 拟合 +
  ``inner_val`` 按内层截点输入评估），选中配置在该折完整训练切片以外层输入重拟合后预测
  验证带（``outer_val``）。
- M3a：基准 = D2 派生局部中位链（训练行行级严格历史、验证行按所属层截点锚定），
  学习残差 ``log(单价) − log(基准)``；CatBoost 网格与 M2 相同 8 配置。
- M3b：**每个评估层各自**在其训练材料内部按日历半年分块——块 k 属性参照 = 用块 1..k−1
  全部材料拟合 M1 结构 Ridge（闭式，去市场列/去位置列的属性组），对块 k 局部中位做构成
  标准化；验证行基准 = 该层截点锚定中位经该层全部训练材料参照标准化；两层参照独立生成、
  不得复用；预热行（首块及早块池 < 2,000 行）保留训练但用未标准化基准，与冷启动排除行
  分开计数；静态固定 2 配置，折内 2 选 1 后外层重拟合；预测合成
  ``log(单价) = log(基准) + CatBoost(残差)``。
- M4：LightGBM 结构**预固定**（v1 与 M2 相同直接报价结构与全量特征组，无任何跨折选型），
  真实网格 8 配置，选择流程同 M2。

纪律：全部拟合 ``thread_count=1`` / ``num_threads=1``；seed 20260912；只消费 S1 合同 run
与 :mod:`gz_property_valuation.phase2.derived_inputs` 派生帧（design D1 消费边界）。

模型输入列（M2/M3a/M3b/M4 共用，``feature_group="full"``）为特征集 v2 去除两类列：

- ``property_tenure_raw``：S1 特征字典明示「登记列，不作为预测输入」；
- ``community_sample_365d``：S1 行级滚动市场列（granularity 小区×行，与
  ``community_med_unit_price_365d`` 同截点规则）；spec D2「验证行市场输入 SHALL NOT
  直接消费行级滚动特征」，故不进模型输入。

类别列 = design D4 声明的 7 列（``D4_CATEGORICAL_COLUMNS``）＋ 输入集中其余字符串列
（``ownership_shared`` / ``tax_status`` / ``property_tenure_mark`` / ``market_evidence_level``）。
后者为 D2 明列的特征集 v2「证据层级」及 S1 声明可用于预测的房屋属性列，均为字符串型，
按 spec「类别特征 SHALL 采用原生类别处理」一并以原生类别处理（类别集为 D4 清单的超集，
属实现细节，报告为疑似发现交审2 判定）。
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl
import lightgbm as lgb
from catboost import CatBoostRegressor

from gz_property_valuation.phase2 import derived_inputs as di
from gz_property_valuation.phase2 import experiments as experiments_mod
from gz_property_valuation.phase2 import models as models_mod

SEED = experiments_mod.SEED
THREAD = experiments_mod.THREAD

CATBOOST_VERSION_PIN = experiments_mod.ENGINES["catboost"]["version"]
LIGHTGBM_VERSION_PIN = experiments_mod.ENGINES["lightgbm"]["version"]

EXCLUDED_MODEL_INPUT_COLUMNS = ["property_tenure_raw", "community_sample_365d"]

D4_CATEGORICAL_COLUMNS = ["community_source_id", "block_name", "floor_bucket",
                          "house_type_norm", "elevator_state", "orientation",
                          "decoration_state"]
ADDITIONAL_CATEGORICAL_COLUMNS = ["ownership_shared", "tax_status",
                                  "property_tenure_mark", "market_evidence_level"]
CATEGORICAL_COLUMNS = list(D4_CATEGORICAL_COLUMNS) + list(ADDITIONAL_CATEGORICAL_COLUMNS)

LOCATION_COLUMNS = ["community_source_id", "block_name"]
HOUSE_ATTRIBUTE_COLUMNS = ["floor_bucket", "elevator_state", "age_years",
                           "decoration_state", "orientation", "ladder_per_household"]

MODEL_INPUT_COLUMNS = [c for c in di.FEATURE_SET_V2
                       if c not in EXCLUDED_MODEL_INPUT_COLUMNS]

M3B_MIN_POOL = 2000
M3B_RIDGE_LAMBDA = 10.0
FRAME_NAMES = di.FRAME_NAMES
MISSING_CAT_VALUES = ("", "暂无数据")


# ---------------- 输入列与特征组 ----------------

def feature_columns(feature_group: str = "full") -> list[str]:
    """模型输入列；``base`` = 全量组去掉房屋属性组（design D5 属性增量实验）。"""
    if feature_group == "full":
        return list(MODEL_INPUT_COLUMNS)
    if feature_group == "base":
        return [c for c in MODEL_INPUT_COLUMNS if c not in HOUSE_ATTRIBUTE_COLUMNS]
    raise ValueError(f"未知特征组：{feature_group}")


def categorical_columns(feature_group: str = "full") -> list[str]:
    return [c for c in feature_columns(feature_group) if c in CATEGORICAL_COLUMNS]


def attribute_columns(feature_group: str = "full") -> list[str]:
    """去市场列、去位置列的属性组（M3b 构成标准化参照用）。"""
    return [c for c in feature_columns(feature_group)
            if c not in LOCATION_COLUMNS and c not in di.DERIVED_MARKET_COLUMNS]


# ---------------- 折内类别码表 ----------------

def _cat_values(series: pl.Series) -> list[str]:
    out = []
    for v in series.to_list():
        if v is None:
            out.append("未知")
            continue
        s = str(v).strip()
        out.append("未知" if s in MISSING_CAT_VALUES else s)
    return out


class FoldCategoryEncoder:
    """折内类别编码器：码表与数值填充值仅由调用 :meth:`fit` 的那一层训练材料生成。"""

    def __init__(self) -> None:
        self.columns: list[str] = []
        self.cat_columns: list[str] = []
        self.categories: dict[str, list[str]] = {}
        self.codes: dict[str, dict[str, int]] = {}
        self.unseen_code: dict[str, int] = {}
        self.fills: dict[str, float] = {}

    def fit(self, frame: pl.DataFrame, columns: list[str],
            cat_columns: list[str]) -> "FoldCategoryEncoder":
        self.columns = list(columns)
        self.cat_columns = [c for c in self.columns if c in cat_columns]
        for c in self.cat_columns:
            values = sorted(set(_cat_values(frame[c])))
            self.categories[c] = values
            self.codes[c] = {v: i for i, v in enumerate(values)}
            self.unseen_code[c] = len(values)
        for c in self.columns:
            if c in self.cat_columns:
                continue
            med = frame[c].cast(pl.Float64).drop_nulls().median()
            self.fills[c] = float(med) if med is not None else 0.0
        return self

    def transform(self, frame: pl.DataFrame) -> pd.DataFrame:
        data: dict[str, np.ndarray] = {}
        for c in self.columns:
            if c in self.cat_columns:
                mapping = self.codes[c]
                unseen = self.unseen_code[c]
                data[c] = np.array([mapping.get(v, unseen) for v in _cat_values(frame[c])],
                                   dtype=np.int64)
            else:
                data[c] = (frame[c].cast(pl.Float64).fill_null(self.fills[c])
                           .to_numpy().astype(np.float64))
        return pd.DataFrame(data)

    def cat_index(self) -> list[int]:
        return [self.columns.index(c) for c in self.cat_columns]


# ---------------- 模型拟合 ----------------

def fit_model(engine: str, config: dict, X: pd.DataFrame, y: np.ndarray,
              cat_index: list[int]) -> tuple[object, Callable[[pd.DataFrame], np.ndarray]]:
    """按引擎拟合一次；返回 (模型, 预测函数)。配置键为注册表冻结键名。"""
    if engine == "catboost":
        params = dict(config)
        params["random_seed"] = int(params.pop("seed", SEED))
        params.setdefault("verbose", 0)
        model = CatBoostRegressor(cat_features=list(cat_index), **params)
        model.fit(X, np.asarray(y, dtype=np.float64))
        return model, lambda D: np.asarray(model.predict(D), dtype=np.float64)
    if engine == "lightgbm":
        params = dict(config)
        rounds = int(params.pop("n_estimators", 500))
        params.pop("seed", None)
        params["seed"] = int(config.get("seed", SEED))
        params["verbose"] = -1
        dataset = lgb.Dataset(X, label=np.asarray(y, dtype=np.float64),
                              categorical_feature=list(cat_index), free_raw_data=False)
        booster = lgb.train(params, dataset, num_boost_round=rounds)
        return booster, lambda D: np.asarray(booster.predict(D), dtype=np.float64)
    raise ValueError(f"未知引擎：{engine}")


def ape_metrics(pred_log: np.ndarray, log_base: np.ndarray,
                true_price: np.ndarray) -> dict:
    pred = np.exp(np.asarray(pred_log, dtype=np.float64)
                  + np.asarray(log_base, dtype=np.float64))
    ratio = pred / np.asarray(true_price, dtype=np.float64)
    ape = np.abs(ratio - 1.0)
    return {"med_ape": float(np.median(ape)),
            "within10_ratio": float((ape <= 0.10).mean()),
            "p90_ape": float(np.percentile(ape, 90)),
            "signed_med_bias": float(np.median(ratio - 1.0)),
            "nonnull_ratio": float(np.isfinite(pred).mean())}


def _predictions(frame: pl.DataFrame, pred_log: np.ndarray,
                 log_base: np.ndarray) -> pl.DataFrame:
    true = frame["unit_price"].cast(pl.Float64).to_numpy()
    pred = np.exp(np.asarray(pred_log, dtype=np.float64)
                  + np.asarray(log_base, dtype=np.float64))
    ratio = pred / true
    return pl.DataFrame({
        "source_record_id": frame["source_record_id"].to_list(),
        "pred_unit_price": pred,
        "unit_price_true": true,
        "ape": np.abs(ratio - 1.0),
        "signed_err": ratio - 1.0,
    })


# ---------------- 通用折内 fit 流程 ----------------

def run_generic_fold(frames: dict, engine: str, grid: list[dict],
                     feature_group: str = "full",
                     log_bases: dict | None = None) -> dict:
    """单折流程：inner 选配置（8/2 次）→ 选中配置外层重拟合（1 次）→ 预测 outer_val。"""
    columns = feature_columns(feature_group)
    cat_columns = [c for c in columns if c in CATEGORICAL_COLUMNS]
    enc_inner = FoldCategoryEncoder().fit(frames["inner_train"], columns, cat_columns)
    enc_outer = FoldCategoryEncoder().fit(frames["outer_train"], columns, cat_columns)
    X = {k: (enc_inner if k.startswith("inner") else enc_outer).transform(frames[k])
         for k in FRAME_NAMES}
    cat_index = [columns.index(c) for c in cat_columns]

    if log_bases is None:
        log_base = {k: np.zeros(frames[k].height) for k in FRAME_NAMES}
    else:
        log_base = {k: np.asarray(log_bases[k], dtype=np.float64) for k in FRAME_NAMES}
    price = {k: frames[k]["unit_price"].cast(pl.Float64).to_numpy() for k in FRAME_NAMES}
    y = {k: np.log(price[k]) - log_base[k] for k in FRAME_NAMES}
    for k in FRAME_NAMES:
        if not np.isfinite(y[k]).all():
            raise AssertionError(f"帧 {k} 目标出现非有限值（基准或单价异常）")

    records: list[dict] = []
    for order, config in enumerate(grid, start=1):
        t0 = time.perf_counter()
        model, predict = fit_model(engine, config, X["inner_train"], y["inner_train"], cat_index)
        inner_pred_log = predict(X["inner_val"])
        fit_seconds = time.perf_counter() - t0
        records.append({
            "order": order,
            "config_id": f"{engine}_c{order:02d}",
            "config": dict(config),
            "inner_val": ape_metrics(inner_pred_log, log_base["inner_val"], price["inner_val"]),
            "fit_seconds": fit_seconds,
        })

    best = min(records, key=lambda r: (r["inner_val"]["med_ape"], r["order"]))
    chosen_config = grid[best["order"] - 1]
    t0 = time.perf_counter()
    outer_model, outer_predict = fit_model(engine, chosen_config, X["outer_train"],
                                           y["outer_train"], cat_index)
    outer_pred_log = outer_predict(X["outer_val"])
    outer_fit_seconds = time.perf_counter() - t0

    return {
        "engine": engine,
        "feature_group": feature_group,
        "feature_columns": columns,
        "categorical_columns": cat_columns,
        "cat_index": cat_index,
        "inner_encoder_categories": {c: len(v) for c, v in enc_inner.categories.items()},
        "outer_encoder_categories": {c: len(v) for c, v in enc_outer.categories.items()},
        "inner_encoder": enc_inner,
        "outer_encoder": enc_outer,
        "selection": {
            "grid_size": len(grid),
            "records": records,
            "chosen": best,
            "inner_train_rows": frames["inner_train"].height,
            "inner_val_rows": frames["inner_val"].height,
            "outer_train_rows": frames["outer_train"].height,
            "outer_val_rows": frames["outer_val"].height,
        },
        "outer_fit_seconds": outer_fit_seconds,
        "log_base": log_base,
        "outer_val_predictions": _predictions(frames["outer_val"], outer_pred_log,
                                              log_base["outer_val"]),
        "outer_val_metrics": ape_metrics(outer_pred_log, log_base["outer_val"],
                                         price["outer_val"]),
        "outer_model": outer_model,
    }


def run_m2_fold(frames: dict, feature_group: str = "full") -> dict:
    return run_generic_fold(frames, "catboost", experiments_mod.M2_GRID,
                            feature_group, None)


def run_m4_fold(frames: dict, feature_group: str = "full") -> dict:
    return run_generic_fold(frames, "lightgbm", experiments_mod.M4_GRID,
                            feature_group, None)


def run_m3a_fold(frames: dict, feature_group: str = "full") -> dict:
    log_bases = {k: np.log(frames[k]["market_chain_med_unit_price"]
                           .cast(pl.Float64).to_numpy()) for k in FRAME_NAMES}
    return run_generic_fold(frames, "catboost", experiments_mod.M3A_GRID,
                            feature_group, log_bases)


# ---------------- M3b：分块前推构成标准化 ----------------

def _half_year_key(d: date) -> tuple[int, int]:
    return (d.year, 1 if d.month <= 6 else 2)


def m3b_block_plan(train_frame: pl.DataFrame, min_pool: int = M3B_MIN_POOL) -> dict:
    """训练材料内按日历半年分块；预热行 = 首块及早块池（前序块）< ``min_pool`` 的行。"""
    dates = train_frame["sale_date_d"].to_list()
    keys = [_half_year_key(d) for d in dates]
    unique_keys = sorted(set(keys))
    all_index = np.arange(len(dates), dtype=np.int64)
    blocks: list[dict] = []
    cum = 0
    for key in unique_keys:
        row_index = np.array([i for i, k in enumerate(keys) if k == key], dtype=np.int64)
        prefix_index = np.array([i for i, k in enumerate(keys) if k < key], dtype=np.int64)
        warmup = cum < min_pool
        blocks.append({
            "block_key": f"{key[0]}H{key[1]}",
            "key": key,
            "rows": int(row_index.size),
            "cum_before": int(cum),
            "warmup": bool(warmup),
            "row_index": row_index,
            "prefix_index": prefix_index,
        })
        cum += int(row_index.size)
    warmup_rows = int(sum(b["rows"] for b in blocks if b["warmup"]))
    boundaries = []
    for b in blocks:
        idx = b["row_index"]
        boundaries.append({
            "block_key": b["block_key"],
            "rows": b["rows"],
            "cum_before": b["cum_before"],
            "warmup": b["warmup"],
            "date_min": str(min(dates[i] for i in idx)) if idx.size else None,
            "date_max": str(max(dates[i] for i in idx)) if idx.size else None,
        })
    return {
        "block_count": len(blocks),
        "blocks": blocks,
        "train_rows": len(dates),
        "warmup_rows": warmup_rows,
        "standardized_rows": len(dates) - warmup_rows,
        "warmup_ratio": (warmup_rows / len(dates)) if dates else None,
        "all_index": all_index,
        "boundaries": boundaries,
    }


class AttributeReference:
    """M1 结构 Ridge 的属性参照（去市场列、去位置列）：类别 one-hot + 数值 z-score + 截距。"""

    def fit(self, frame: pl.DataFrame, feature_group: str = "full") -> "AttributeReference":
        attrs = attribute_columns(feature_group)
        self.cat_columns = [c for c in attrs if c in CATEGORICAL_COLUMNS]
        self.num_columns = [c for c in attrs if c not in self.cat_columns]
        self.categories: dict[str, list[str]] = {}
        self.code_maps: dict[str, dict[str, int]] = {}
        for c in self.cat_columns:
            values = sorted(set(_cat_values(frame[c])))
            self.categories[c] = values
            self.code_maps[c] = {v: i for i, v in enumerate(values)}
        self.mu: dict[str, float] = {}
        self.sd: dict[str, float] = {}
        for c in self.num_columns:
            v = frame[c].cast(pl.Float64).fill_null(0.0).to_numpy().astype(np.float64)
            self.mu[c] = float(np.mean(v))
            self.sd[c] = float(np.std(v)) + 1e-9
        return self

    def design(self, frame: pl.DataFrame, index: np.ndarray | None = None) -> np.ndarray:
        columns: list[np.ndarray] = []
        for c in self.cat_columns:
            mapping = self.code_maps[c]
            codes = np.array([mapping.get(v, -1) for v in _cat_values(frame[c])])
            for k in range(1, len(self.categories[c])):
                columns.append((codes == k).astype(np.float64))
        for c in self.num_columns:
            v = frame[c].cast(pl.Float64).fill_null(self.mu[c]).to_numpy().astype(np.float64)
            columns.append((v - self.mu[c]) / self.sd[c])
        columns.append(np.ones(frame.height))
        matrix = np.column_stack(columns)
        return matrix if index is None else matrix[index]


def m3b_standardized_baseline(frames: dict, layer: str,
                              feature_group: str = "full",
                              min_pool: int = M3B_MIN_POOL,
                              lam: float = M3B_RIDGE_LAMBDA) -> dict:
    """某评估层的分块前推构成标准化基准（对数尺度）。

    - 训练行：块 k（非预热）基准 = 行级中位 log + (均值属性贡献差 c(前缀) − c(块 k))；
      预热行用未标准化基准；
    - 验证行：基准 = 该层截点锚定中位 log + (c(该层全部训练材料) − c(验证行))。
    """
    train_frame = frames["inner_train"] if layer == "inner" else frames["outer_train"]
    val_frame = frames["inner_val"] if layer == "inner" else frames["outer_val"]
    plan = m3b_block_plan(train_frame, min_pool)
    reference = AttributeReference().fit(train_frame, feature_group)

    raw_log_train = np.log(train_frame["market_chain_med_unit_price"]
                           .cast(pl.Float64).to_numpy())
    raw_log_val = np.log(val_frame["market_chain_med_unit_price"]
                         .cast(pl.Float64).to_numpy())
    log_price_train = np.log(train_frame["unit_price"].cast(pl.Float64).to_numpy())

    base_train = raw_log_train.copy()
    reference_records: list[dict] = []
    for b in plan["blocks"]:
        if b["warmup"]:
            continue
        prefix = b["prefix_index"]
        design_prefix = reference.design(train_frame, prefix)
        w = models_mod.ridge_solve(design_prefix, log_price_train[prefix], lam)
        c_ref = float(np.mean(design_prefix @ w))
        design_block = reference.design(train_frame, b["row_index"])
        c_block = float(np.mean(design_block @ w))
        base_train[b["row_index"]] = raw_log_train[b["row_index"]] + (c_ref - c_block)
        reference_records.append({
            "block_key": b["block_key"],
            "prefix_rows": int(prefix.size),
            "offset": float(c_ref - c_block),
        })

    design_all = reference.design(train_frame, plan["all_index"])
    w_layer = models_mod.ridge_solve(design_all, log_price_train, lam)
    c_ref_all = float(np.mean(design_all @ w_layer))
    design_val = reference.design(val_frame)
    c_val = float(np.mean(design_val @ w_layer))
    base_val = raw_log_val + (c_ref_all - c_val)

    return {
        "layer": layer,
        "plan": plan,
        "base_train_log": base_train,
        "base_val_log": base_val,
        "reference_records": reference_records,
        "attribute_columns": attribute_columns(feature_group),
        "layer_reference": {
            "fit_rows": int(plan["all_index"].size),
            "design_columns": int(design_all.shape[1]),
            "weights_head": [float(x) for x in w_layer[:5]],
            "val_offset": float(c_ref_all - c_val),
        },
        "warmup_rows": plan["warmup_rows"],
        "standardized_rows": plan["standardized_rows"],
    }


def run_m3b_fold(frames: dict, feature_group: str = "full") -> dict:
    inner = m3b_standardized_baseline(frames, "inner", feature_group)
    outer = m3b_standardized_baseline(frames, "outer", feature_group)
    log_bases = {
        "inner_train": inner["base_train_log"],
        "inner_val": inner["base_val_log"],
        "outer_train": outer["base_train_log"],
        "outer_val": outer["base_val_log"],
    }
    result = run_generic_fold(frames, "catboost", experiments_mod.M3B_GRID,
                              feature_group, log_bases)
    result["m3b"] = {
        "inner": {k: inner[k] for k in ("plan", "reference_records", "layer_reference",
                                        "attribute_columns", "warmup_rows",
                                        "standardized_rows")},
        "outer": {k: outer[k] for k in ("plan", "reference_records", "layer_reference",
                                        "attribute_columns", "warmup_rows",
                                        "standardized_rows")},
    }
    return result


# ---------------- 静态块边界/预热预期演示 ----------------

def m3b_block_preview(run_dir: Path) -> dict:
    """各折 M3b 静态块边界/行数/预热预期占比（不拟合模型，纯静态分块）。"""
    master, features, splits = di.load_s1(run_dir)
    pool = di.build_pool(master, features)
    row_level = di.row_level_market(pool)
    folds_out = []
    for fold in splits["folds"]:
        d = di.derive_fold(master, features, fold, pool=pool, row_level=row_level,
                           with_static=False)
        inner_plan = m3b_block_plan(d["inner_train"])
        outer_plan = m3b_block_plan(d["outer_train"])
        folds_out.append({
            "fold_id": fold["fold_id"],
            "inner": {"train_rows": inner_plan["train_rows"],
                      "block_count": inner_plan["block_count"],
                      "warmup_rows": inner_plan["warmup_rows"],
                      "warmup_ratio": inner_plan["warmup_ratio"],
                      "boundaries": inner_plan["boundaries"]},
            "outer": {"train_rows": outer_plan["train_rows"],
                      "block_count": outer_plan["block_count"],
                      "warmup_rows": outer_plan["warmup_rows"],
                      "warmup_ratio": outer_plan["warmup_ratio"],
                      "boundaries": outer_plan["boundaries"]},
        })
    return {"run_id": run_dir.name, "min_pool": M3B_MIN_POOL,
            "folds": folds_out, "verdict": "PASS"}


# ---------------- 加载与冒烟 ----------------

def load_fold_frames(run_dir: Path, fold_id: str = "F18") -> tuple[dict, dict, pl.DataFrame, pl.DataFrame]:
    master, features, splits = di.load_s1(run_dir)
    fold = next(f for f in splits["folds"] if f["fold_id"] == fold_id)
    pool = di.build_pool(master, features)
    row_level = di.row_level_market(pool)
    derived = di.derive_fold(master, features, fold, pool=pool, row_level=row_level,
                             with_static=True)
    return derived, fold, pool, row_level


def smoke_fold(run_dir: Path, fold_id: str = "F18") -> dict:
    """单折冒烟：M2 / M4 / M3a / M3b 四路径端到端（F18 最小折）。"""
    frames, fold, _, _ = load_fold_frames(run_dir, fold_id)
    m2 = run_m2_fold(frames)
    m4 = run_m4_fold(frames)
    m3a = run_m3a_fold(frames)
    m3b = run_m3b_fold(frames)
    return {
        "run_id": run_dir.name,
        "fold_id": fold_id,
        "anchor": frames["outer_cutoff"] if isinstance(frames, dict) else None,
        "row_counts": {k: frames[k].height for k in FRAME_NAMES},
        "paths": {
            "M2": {"chosen": m2["selection"]["chosen"]["config_id"],
                   "outer_val_med_ape": m2["outer_val_metrics"]["med_ape"],
                   "nonnull_ratio": m2["outer_val_metrics"]["nonnull_ratio"]},
            "M4": {"chosen": m4["selection"]["chosen"]["config_id"],
                   "outer_val_med_ape": m4["outer_val_metrics"]["med_ape"],
                   "nonnull_ratio": m4["outer_val_metrics"]["nonnull_ratio"]},
            "M3a": {"chosen": m3a["selection"]["chosen"]["config_id"],
                    "outer_val_med_ape": m3a["outer_val_metrics"]["med_ape"],
                    "nonnull_ratio": m3a["outer_val_metrics"]["nonnull_ratio"]},
            "M3b": {"chosen": m3b["selection"]["chosen"]["config_id"],
                    "outer_val_med_ape": m3b["outer_val_metrics"]["med_ape"],
                    "nonnull_ratio": m3b["outer_val_metrics"]["nonnull_ratio"]},
        },
        "verdict": "PASS",
    }


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.nonlinear",
        "engines": {"catboost": CATBOOST_VERSION_PIN, "lightgbm": LIGHTGBM_VERSION_PIN},
        "feature_group_full_columns": MODEL_INPUT_COLUMNS,
        "excluded_model_input_columns": EXCLUDED_MODEL_INPUT_COLUMNS,
        "d4_categorical_columns": D4_CATEGORICAL_COLUMNS,
        "additional_categorical_columns": ADDITIONAL_CATEGORICAL_COLUMNS,
        "cat_index_note": "类别码表折内构造，仅该层训练材料（FoldCategoryEncoder）",
        "grids": {
            "M2": {"size": len(experiments_mod.M2_GRID)},
            "M3a": {"size": len(experiments_mod.M3A_GRID)},
            "M3b": {"size": len(experiments_mod.M3B_GRID)},
            "M4": {"size": len(experiments_mod.M4_GRID)},
        },
        "flow": experiments_mod.REFIT_FLOW,
        "m3b": {"min_pool": M3B_MIN_POOL, "ridge_lambda": M3B_RIDGE_LAMBDA,
                "blocks": "日历半年块；块 k 参照=块 1..k−1 拟合；预热行未标准化基准"},
        "thread_pinning": "CatBoost thread_count=1 / LightGBM num_threads=1；包导入期钉死 BLAS/polars",
        "lightgbm_api": "lightgbm 原生 lgb.train（避免引入 scikit-learn 依赖）",
        "consumption_boundary": "只读 S1 合同 run 与 D2 派生帧",
        "verdict": "PASS",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 S3 非线性模型 M2/M3a/M3b/M4")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("structure", help="结构清单自检")
    p_pv = sub.add_parser("block-preview", help="M3b 静态块边界/预热预期（全折）")
    p_pv.add_argument("run_dir")
    p_sm = sub.add_parser("smoke", help="单折冒烟 M2/M4/M3a/M3b")
    p_sm.add_argument("run_dir")
    p_sm.add_argument("--fold", default="F18")
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        print(json.dumps(structure_report(), ensure_ascii=False, indent=1))
        return 0
    if args.cmd == "block-preview":
        print(json.dumps(m3b_block_preview(Path(args.run_dir).resolve()),
                         ensure_ascii=False, indent=1, default=str))
        return 0
    result = smoke_fold(Path(args.run_dir).resolve(), args.fold)
    print(json.dumps(result, ensure_ascii=False, indent=1, default=str))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
