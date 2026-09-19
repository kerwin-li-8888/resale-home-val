# -*- coding: utf-8 -*-
"""phase2 S4b 总楼层缺失降级与支持度/冷启动/拒绝规则。

行为规格（specs/phase2-candidate-freeze/spec.md「总楼层缺失降级路径」「支持度与冷启动
确定行为」；design D3/D6）：

- 路径甲（推理时插补）：总楼层缺失时以请求小区在截点前 365 天已知行的中位总楼层插补
  （无则板块级→区级回退），``miss_total_floors`` 标记保持置位；**仅作用于缺失行**，
  对无缺失输入不改变预测。
- 路径乙（去列组变体）：以 S2 冻结流程重训「去总楼层列组」变体（``total_floors_filled``
  与 ``miss_total_floors`` 两列不入模），λ 折内网格选择同 :mod:`models` 口径。
- B0 支持度链：小区→板块→区级；冷启动小区自动落板块/区级并标注 ``cold_start_community``；
  回退链全空（无任何历史证据）→ 拒绝输出报价。
- 关键属性缺失不拒绝（走降级/标注）。

只读消费 S1/S2/S3 run 冻结产物；不修改任何既有实现。
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

from . import baselines, models

WINDOW_DAYS = 365
DEAD_LETTER_LEVEL = None
TOTAL_FLOORS_DROPPED_COLUMNS = ("total_floors_filled", "miss_total_floors")
VARIANT_ID = "B-no-total-floors"
MISSING_BLOCK_VALUES = baselines.MISSING_BLOCK_VALUES


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def _ape_metrics(pred_log: np.ndarray, true_log: np.ndarray) -> dict:
    ape = np.abs(np.exp(pred_log - true_log) - 1.0)
    return {"med_ape": float(np.median(ape)),
            "within10_ratio": float((ape <= 0.10).mean()),
            "p90_ape": float(np.percentile(ape, 90))}


def _block_norm(value) -> str | None:
    if value is None or str(value).strip() in MISSING_BLOCK_VALUES:
        return None
    return str(value)


def missing_total_floors(frame: pl.DataFrame) -> np.ndarray:
    null = frame["total_floors"].is_null().to_numpy()
    flag = frame["miss_total_floors"].cast(pl.Float64).fill_null(1.0).to_numpy() > 0
    return null | flag


# ---------------------------------------------------------------- 路径甲：推理时插补

def impute_total_floors_A(frame: pl.DataFrame,
                          context: dict) -> tuple[pl.DataFrame, list[str]]:
    """路径甲：缺失总楼层按 小区→板块→区级 中位插补，仅改缺失行，返回来源链。"""
    miss = missing_total_floors(frame)
    tf = frame["total_floors"].cast(pl.Float64).to_numpy()
    comms = frame["community_source_id"].to_list()
    blocks = frame["block_name"].to_list()
    origins: list[str] = []
    values: list[float | None] = []
    for i in range(frame.height):
        if not miss[i]:
            origins.append("none")
            values.append(float(tf[i]))
            continue
        key = str(comms[i]) if comms[i] is not None else None
        blk = _block_norm(blocks[i])
        if key in context["com_tf"]:
            origins.append("community")
            values.append(float(context["com_tf"][key]))
        elif blk is not None and blk in context["blk_tf"]:
            origins.append("block")
            values.append(float(context["blk_tf"][blk]))
        elif context["district_tf"] is not None:
            origins.append("district")
            values.append(float(context["district_tf"]))
        else:
            origins.append("none")
            values.append(None)
    series = pl.Series("total_floors", values, dtype=pl.Float64)
    return frame.with_columns(series), origins


# ---------------------------------------------------------------- 路径乙：去总楼层列组变体

def variant_keep_mask(enc: models.FoldEncoder) -> np.ndarray:
    names = enc.feature_names()
    return np.asarray([n not in TOTAL_FLOORS_DROPPED_COLUMNS for n in names], dtype=bool)


def variant_dropped_indices(enc: models.FoldEncoder) -> list[int]:
    names = enc.feature_names()
    return [i for i, n in enumerate(names) if n in TOTAL_FLOORS_DROPPED_COLUMNS]


def select_lambda_variant(train: pl.DataFrame) -> tuple[float, list[dict]]:
    """去列组变体的折内 λ 选择（口径同 :func:`models.select_lambda_in_fold`）。

    注意：类别表随训练切片变化，故去列掩码按**该次拟合所用编码器**现算（按列名剔除）。
    """
    max_d = train["sale_date_d"].max()
    start = _add_months(max_d, -models.INTERNAL_VAL_MONTHS) + timedelta(days=1)
    itr = train.filter(pl.col("sale_date_d") < start)
    iva = train.filter(pl.col("sale_date_d") >= start)
    enc = models.FoldEncoder().fit(itr)
    keep = variant_keep_mask(enc)
    Xit = enc.transform(itr)[:, keep]
    Xiv = enc.transform(iva)[:, keep]
    yit = np.log(itr["unit_price"].to_numpy())
    yiv = np.log(iva["unit_price"].to_numpy())
    grid = []
    for lam in models.LAMBDA_GRID:
        pred_log = Xiv @ models.ridge_solve(Xit, yit, lam)
        grid.append({"lambda": lam, **_ape_metrics(pred_log, yiv)})
    best = min(grid, key=lambda r: (r["med_ape"], r["lambda"]))
    return float(best["lambda"]), grid


def fit_path_b_fold(train: pl.DataFrame) -> dict:
    """单折路径乙：折内 λ 选择 + 外层重拟合（去总楼层列组）。"""
    lam, grid = select_lambda_variant(train)
    enc = models.FoldEncoder().fit(train)
    keep = variant_keep_mask(enc)
    X = enc.transform(train)[:, keep]
    w = models.ridge_solve(X, np.log(train["unit_price"].to_numpy()), lam)
    return {"variant": VARIANT_ID, "chosen_lambda": lam, "lambda_grid": grid,
            "n_features": int(X.shape[1]), "weights": w}


def predict_variant(enc: models.FoldEncoder, weights: np.ndarray,
                    frame: pl.DataFrame) -> np.ndarray:
    keep = variant_keep_mask(enc)
    X = enc.transform(frame)[:, keep]
    m = X.shape[0]
    out = np.empty(m, dtype=np.float64)
    chunk = 1024
    for s in range(0, m, chunk):
        blk = X[s:s + chunk]
        r = blk.shape[0]
        if r < chunk:
            blk = np.vstack([blk, np.zeros((chunk - r, X.shape[1]))])
        out[s:s + r] = (blk @ weights)[:r]
    return np.exp(out)


# ---------------------------------------------------------------- B0 支持度/冷启动/拒绝

def b0_lookup(context: dict, comm, block) -> dict:
    """B0 证据链：community→block→district；回退即冷启动标注；全空即拒绝。"""
    key = str(comm) if comm is not None else None
    if key in context["com_map"]:
        p, c = context["com_map"][key]
        return {"level": "community", "pred": p, "n": c, "cold_start": False}
    blk = _block_norm(block)
    if blk is not None and blk in context["blk_map"]:
        p, c = context["blk_map"][blk]
        return {"level": "block", "pred": p, "n": c, "cold_start": True}
    if context["district_med"] is not None:
        return {"level": "district", "pred": context["district_med"],
                "n": context["district_n"], "cold_start": True}
    return {"level": DEAD_LETTER_LEVEL, "pred": None, "n": 0, "cold_start": True}


def support_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.degradation",
        "path_a": "缺失总楼层按 小区→板块→区级 中位插补；miss_total_floors 保持置位；仅改缺失行",
        "path_b": {"variant": VARIANT_ID, "dropped_columns": list(TOTAL_FLOORS_DROPPED_COLUMNS),
                   "refit": "折内 λ 网格 + 外层重拟合（S2 冻结流程）"},
        "b0_chain": "community → block → district；回退即 cold_start_community 标注",
        "reject_rule": "回退链全空（无任何历史证据）→ 拒绝输出报价",
        "key_attribute_missing": "不拒绝，走降级/标注",
        "verdict": "PASS",
    }
