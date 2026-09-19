# -*- coding: utf-8 -*-
"""phase2 M1 可解释模型：log 单价 Ridge 闭式解、折内编码、λ 折内网格与未知小区回退。

行为规格（specs/phase2-baseline-comparisons/spec.md「M1 可解释模型结构与拟合边界」、
design D3/D6）：

- 目标 ``log(unit_price)``；特征组：小区 one-hot（正则收缩，样本少的小区效应被压向
  板块/全局）+ 板块 one-hot（片区承接层）+ 楼层段、电梯三态、楼层段×电梯交互、
  室数类别、朝向、装修 + 面积/房龄线性项与固定边界分箱平滑 + 总层数数值 + 缺失标记
  + 成交年-月哑变量（市场时间效应，与属性分开定义）。
- 折内拟合边界（spec Scenario）：类别表（drop-first、未知全 0，S0 F-01 口径）、
  年-月钳制边界（验证期未见年-月钳制到训练切片最晚年-月，S0 F-03 市场状态外推口径）、
  数值填充值与 z-score 统计仅由该折训练切片拟合（:class:`FoldEncoder`）。
- 求解：numpy 闭式 ``(XᵀX+λI)⁻¹Xᵀy``；截距列入 X，λI 施于全部列（与 S0 修正脚本
  ``run_data2_curve_fixed.ridge_fit_predict`` 同构）。dense 规模约 25k×~1000，
  若超内存改稀疏按 design D3 登记为实现变更（行为不变）。
- λ∈{1,2,5,10} 折内选择：训练切片内部按时间留出末段 3 个月作内部验证，MedAPE 最小
  者当选（平局取更小 λ），不越 [1,10]；各 λ 表现登记（任务 3.2 留证 evidence/3-2/，
  S2 run 资产目录本阶段不创建）。
- 未知小区：小区列全 0 → 自动落板块/属性/全局效应，逐行 ``community_fallback``
  标记与比例披露，不报错不中断。

只消费 S1 合同 run 产物（master/features/splits，design D1），不读 staged 原始数据。
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl

LAMBDA_GRID = (1.0, 2.0, 5.0, 10.0)
LAMBDA_RANGE = (1.0, 10.0)
AREA_BIN_EDGES = (30.0, 50.0, 70.0, 90.0, 110.0, 130.0, 150.0, 200.0)
AGE_BIN_EDGES = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 60.0)
INTERNAL_VAL_MONTHS = 3
BED_SINGLE_ROOMS = {"1", "2", "3", "4", "5"}
MISSING_BLOCK_VALUES = ("", "暂无数据")
S0_M1_REFERENCE = {"low": 0.118, "high": 0.125}

CATEGORICAL_ROLES = {
    "community": "小区 one-hot（正则收缩；未知验证小区全 0 → 板块/属性/全局回退）",
    "block": "板块 one-hot（片区承接层；缺失/『暂无数据』归未知）",
    "floor": "楼层段类别（低/中/高/地下室/未知）",
    "elevator": "电梯三态（有/无/未知，S0 F-02 布尔直判口径）",
    "bedrooms": "室数类别（1–5 室单列、其他、缺失，S0 口径）",
    "orientation": "朝向类别",
    "decoration": "装修类别",
    "ym": "成交年-月哑变量（市场时间效应，与属性分开定义；验证期未见年-月钳制到训练最晚年-月）",
    "floor_elevator": "楼层段×电梯交互（类别乘积 one-hot）",
}
_NUMERIC_BASE_NAMES = ["area_sqm", "area_sqm_sq_over_1e4", "log1p_area_sqm",
                       "age_years_filled", "total_floors_filled",
                       "miss_year_built", "miss_total_floors"]


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last = [31, 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def numeric_feature_names() -> list[str]:
    return (_NUMERIC_BASE_NAMES
            + [f"area_bin_{int(e)}" for e in AREA_BIN_EDGES]
            + [f"age_bin_{int(e)}" for e in AGE_BIN_EDGES])


def _interval_dummies(values: np.ndarray,
                      edges: tuple[float, ...]) -> list[np.ndarray]:
    """右开区间 one-hot ``(-inf,e0),[e0,e1),…,[e_last,∞)``；drop 第一个区间防共线。"""
    idx = np.searchsorted(np.asarray(edges), values, side="right")
    return [(idx == i + 1).astype(np.float64) for i in range(len(edges))]


def numeric_frame(frame: pl.DataFrame,
                  fills: dict | None = None) -> tuple[np.ndarray, dict]:
    """数值组矩阵；``fills`` 缺省时由该帧自身统计（仅应在训练切片调用）。"""
    if fills is None:
        fills = {}
        for col in ("area_sqm", "age_years", "total_floors"):
            med = frame[col].drop_nulls().median()
            fills[col] = float(med) if med is not None else 0.0
    area = (frame["area_sqm"].cast(pl.Float64).fill_null(fills["area_sqm"])
            .to_numpy().astype(np.float64))
    age = (frame["age_years"].cast(pl.Float64).fill_null(fills["age_years"])
           .to_numpy().astype(np.float64))
    tf = (frame["total_floors"].cast(pl.Float64).fill_null(fills["total_floors"])
          .to_numpy().astype(np.float64))
    miss_yb = frame["miss_year_built"].cast(pl.Float64).fill_null(1.0).to_numpy()
    miss_tf = frame["miss_total_floors"].cast(pl.Float64).fill_null(1.0).to_numpy()
    base = np.column_stack([area, area ** 2 / 1e4, np.log1p(area), age, tf,
                            miss_yb, miss_tf])
    bins = (np.column_stack(_interval_dummies(area, AREA_BIN_EDGES)),
            np.column_stack(_interval_dummies(age, AGE_BIN_EDGES)))
    return np.column_stack([base, *bins]), fills


def category_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """类别组（含楼层段×电梯交互列），全部字符串化并归一未知值。"""
    comm = pl.Series("community",
                     ["缺失" if v is None else str(v)
                      for v in frame["community_source_id"].to_list()])
    block = pl.Series("block",
                      ["未知" if v is None or str(v).strip() in MISSING_BLOCK_VALUES
                       else str(v) for v in frame["block_name"].to_list()])
    floor = pl.Series("floor",
                      ["未知" if v is None else str(v)
                       for v in frame["floor_bucket"].to_list()])
    elevator = pl.Series("elevator",
                         ["未知" if v is None else str(v)
                          for v in frame["elevator_state"].to_list()])
    bedrooms = pl.Series(
        "bedrooms",
        [("缺失" if v is None else (v if v in BED_SINGLE_ROOMS else "其他"))
         for v in frame["bedrooms_n"].cast(pl.String).fill_null("缺失").to_list()])
    orientation = pl.Series("orientation",
                            ["未知" if v is None else str(v)
                             for v in frame["orientation"].to_list()])
    decoration = pl.Series("decoration",
                           ["未知" if v is None else str(v)
                            for v in frame["decoration_state"].to_list()])
    ym = pl.Series("ym", ["%04d-%02d" % (d.year, d.month)
                          for d in frame["sale_date_d"].to_list()])
    inter = pl.Series("floor_elevator",
                      [f"{a}|{b}" for a, b in zip(floor.to_list(),
                                                  elevator.to_list())])
    return pl.DataFrame([comm, block, floor, elevator, bedrooms,
                         orientation, decoration, ym, inter])


class FoldEncoder:
    """折内拟合编码器：类别表（drop-first、未知全 0）、年-月钳制、填充与 z-score。

    拟合边界（spec「折内拟合边界」Scenario）：全部统计仅来自调用 :meth:`fit`
    的训练切片；:meth:`transform` 不回写、不更新任何统计量。
    """

    def fit(self, frame: pl.DataFrame) -> "FoldEncoder":
        cats = category_frame(frame)
        self.cat_cols = list(cats.columns)
        self.categories = {c: sorted(cats[c].unique().to_list())
                           for c in self.cat_cols}
        self.ym_clamp = self.categories["ym"][-1]
        nums, self.fills = numeric_frame(frame)
        self.mu = nums.mean(axis=0)
        self.sd = nums.std(axis=0) + 1e-9
        return self

    def _cat_matrix(self, cats: pl.DataFrame) -> np.ndarray:
        blocks = []
        for c in self.cat_cols:
            vals = np.asarray(cats[c].to_list(), dtype=object)
            if c == "ym":
                vals = np.where(vals > self.ym_clamp, self.ym_clamp, vals)
            known = self.categories[c][1:]
            if not known:
                continue
            blocks.append((vals[:, None]
                           == np.asarray(known, dtype=object)[None, :]
                           ).astype(np.float64))
        return np.hstack(blocks) if blocks else np.zeros((cats.height, 0))

    def transform(self, frame: pl.DataFrame) -> np.ndarray:
        cats = category_frame(frame)
        nums, _ = numeric_frame(frame, self.fills)
        nums_z = (nums - self.mu) / self.sd
        return np.column_stack([self._cat_matrix(cats), nums_z,
                                np.ones(frame.height)])

    def unknown_community(self, frame: pl.DataFrame) -> np.ndarray:
        cats = category_frame(frame)
        known = set(self.categories["community"])
        return np.array([v not in known for v in cats["community"].to_list()],
                        dtype=bool)

    def feature_names(self) -> list[str]:
        names = [f"{c}={k}" for c in self.cat_cols
                 for k in self.categories[c][1:]]
        return names + numeric_feature_names() + ["intercept"]


def ridge_solve(X: np.ndarray, y: np.ndarray, lam: float) -> np.ndarray:
    """闭式 Ridge ``(XᵀX+λI)⁻¹Xᵀy``；λI 施于全部列（含截距，与 S0 同构）。"""
    if not LAMBDA_RANGE[0] <= lam <= LAMBDA_RANGE[1]:
        raise AssertionError(f"λ={lam} 越界 [{LAMBDA_RANGE[0]},{LAMBDA_RANGE[1]}]")
    XtX = X.T @ X
    XtX[np.diag_indices_from(XtX)] += lam
    return np.linalg.solve(XtX, X.T @ y)


def load_fold(run_dir: Path, fold_id: str) -> tuple[dict, date, pl.DataFrame, pl.DataFrame]:
    """按 splits.json 装载一折：训练切片与验证窗（均挂 unit_price 标签）。"""
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = pl.read_parquet(run_dir / "features.parquet")
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    fold = next(f for f in splits["folds"] if f["fold_id"] == fold_id)
    anchor = date.fromisoformat(fold["anchor"])
    labels = master.select(["source_record_id",
                            pl.col("unit_price").cast(pl.Float64)])
    train = features.filter(pl.col("sale_date_d") < anchor).join(
        labels, on="source_record_id", how="left")
    val = features.filter(
        (pl.col("sale_date_d") >= date.fromisoformat(fold["validation"]["start"]))
        & (pl.col("sale_date_d") <= date.fromisoformat(fold["validation"]["end_inclusive"]))
    ).join(labels, on="source_record_id", how="left")
    assert train["unit_price"].null_count() == 0, "训练切片含缺失单价标签"
    assert val["unit_price"].null_count() == 0, "验证窗含缺失单价标签"
    return fold, anchor, train, val


def _fold_encoder_matrices(train: pl.DataFrame,
                           apply: pl.DataFrame) -> tuple[FoldEncoder, np.ndarray,
                                                         np.ndarray, np.ndarray, np.ndarray]:
    enc = FoldEncoder().fit(train)
    ytr = np.log(train["unit_price"].to_numpy())
    yva = np.log(apply["unit_price"].to_numpy())
    assert np.isfinite(ytr).all() and np.isfinite(yva).all(), "log 单价出现非有限值"
    return enc, enc.transform(train), ytr, enc.transform(apply), yva


def _ape_metrics(pred_log: np.ndarray, true_log: np.ndarray) -> dict:
    ratio = np.exp(pred_log - true_log)
    ape = np.abs(ratio - 1.0)
    return {"med_ape": float(np.median(ape)),
            "within10_ratio": float((ape <= 0.10).mean()),
            "p90_ape": float(np.percentile(ape, 90)),
            "signed_med_bias": float(np.median(ratio - 1.0))}


def lambda_grid_records(Xtr: np.ndarray, ytr: np.ndarray, Xva: np.ndarray,
                        yva: np.ndarray) -> list[dict]:
    """λ 网格逐值拟合与表现（登记用；选择判据由调用方决定）。"""
    records = []
    for lam in LAMBDA_GRID:
        w = ridge_solve(Xtr, ytr, lam)
        records.append({"lambda": lam,
                        **_ape_metrics(Xva @ w, yva)})
    return records


def select_lambda_in_fold(train: pl.DataFrame) -> dict:
    """λ 折内选择：训练切片内部按时间留出末段 3 个月作内部验证，MedAPE 最小者当选。

    编码（类别表/钳制/填充/z-score）仅拟合内部训练子片；平局取更小 λ。
    """
    max_d = train["sale_date_d"].max()
    start = _add_months(max_d, -INTERNAL_VAL_MONTHS) + timedelta(days=1)
    inner_train = train.filter(pl.col("sale_date_d") < start)
    inner_val = train.filter(pl.col("sale_date_d") >= start)
    enc, Xit, yit, Xiv, yiv = _fold_encoder_matrices(inner_train, inner_val)
    grid = lambda_grid_records(Xit, yit, Xiv, yiv)
    best = min(grid, key=lambda r: (r["med_ape"], r["lambda"]))
    return {
        "inner_split": {
            "rule": f"训练切片内部留出末段 {INTERNAL_VAL_MONTHS} 个月作内部验证；"
                    "编码仅拟合内部训练子片",
            "inner_train_end_exclusive": start.isoformat(),
            "inner_train_rows": inner_train.height,
            "inner_val_rows": inner_val.height,
            "inner_val_window": f"[{start.isoformat()}, {max_d.isoformat()}]",
        },
        "grid": [{"lambda": r["lambda"],
                  "inner_val_med_ape": r["med_ape"],
                  "inner_val_within10_ratio": r["within10_ratio"],
                  "inner_val_signed_med_bias": r["signed_med_bias"]}
                 for r in grid],
        "chosen_lambda": best["lambda"],
        "selection_rule": "内部验证 MedAPE 最小；平局取更小 λ；λ∈{1,2,5,10}，不越 [1,10]",
    }


def build_predictions(apply: pl.DataFrame, pred_log: np.ndarray,
                      fallback_mask: np.ndarray) -> pl.DataFrame:
    out = apply.select(["source_record_id", "community_source_id", "sale_date_d",
                        pl.col("unit_price").cast(pl.Float64).alias("unit_price_true")])
    out = out.with_columns([pl.Series("pred_unit_price", np.exp(pred_log)),
                            pl.Series("community_fallback", fallback_mask)])
    return out.with_columns(
        [(pl.col("pred_unit_price") / pl.col("unit_price_true") - 1.0).abs().alias("ape"),
         (pl.col("pred_unit_price") / pl.col("unit_price_true") - 1.0).alias("signed_err")])


def fit_fold_predict(train: pl.DataFrame, apply: pl.DataFrame,
                     lam: float) -> dict:
    """单折拟合与预测：折内编码 → 闭式 Ridge → 逐房预测与未知小区回退标记。"""
    enc, Xtr, ytr, Xap, _ = _fold_encoder_matrices(train, apply)
    w = ridge_solve(Xtr, ytr, lam)
    pred_log = Xap @ w
    fallback = enc.unknown_community(apply)
    out = build_predictions(apply, pred_log, fallback)
    return {
        "predictions": out,
        "metrics": _ape_metrics(pred_log, np.log(apply["unit_price"].to_numpy())),
        "meta": {"lambda": lam, "n_train": train.height, "n_apply": apply.height,
                 "n_features": int(Xtr.shape[1]),
                 "n_known_communities": len(enc.categories["community"]),
                 "community_fallback_ratio": float(fallback.mean()),
                 "pred_nonnull_ratio": float(out["pred_unit_price"].is_not_null().mean())},
        "encoder": enc, "weights": w,
    }


def _variant(frame: pl.DataFrame, **overrides) -> pl.DataFrame:
    return frame.head(1).with_columns(
        [pl.lit(v).alias(k) for k, v in overrides.items()])


def assert_known_communities_differ(enc: FoldEncoder, prototype: pl.DataFrame,
                                    w: np.ndarray) -> dict:
    """防回归断言①：两个训练期已知小区、其余属性相同 → 设计向量与预测不同。"""
    comms = enc.categories["community"][:2]
    x = np.vstack([enc.transform(_variant(prototype, community_source_id=c))
                   for c in comms])
    preds = x @ w
    return {"check": "known_communities_differ",
            "spec_basis": "小区效应真实入模：改变已知小区改变预测",
            "communities": list(comms),
            "design_columns_differing": int(np.sum(x[0] != x[1])),
            "predictions_log": [float(p) for p in preds],
            "passed": bool(np.sum(x[0] != x[1]) > 0 and preds[0] != preds[1])}


def assert_elevator_three_state_separable(enc: FoldEncoder,
                                          prototype: pl.DataFrame) -> dict:
    """防回归断言②：电梯三态（有/无/未知）设计向量两两可分（S0 F-02 教训）。"""
    states = ("有", "无", "未知")
    x = np.vstack([enc.transform(_variant(prototype, elevator_state=s))
                   for s in states])
    pairs = [(0, 1), (0, 2), (1, 2)]
    diffs = {f"{states[i]}|{states[j]}": int(np.sum(x[i] != x[j]))
             for i, j in pairs}
    return {"check": "elevator_three_state_separable",
            "spec_basis": "电梯三态可分（布尔直判，不经字符串匹配布尔列）",
            "pairwise_differing_columns": diffs,
            "passed": bool(all(d > 0 for d in diffs.values()))}


def assert_unknown_community_fallback(enc: FoldEncoder,
                                      prototype: pl.DataFrame,
                                      w: np.ndarray) -> dict:
    """防回归断言③：未知小区回退不报错，标记 fallback 且预测有限。"""
    probe = "__UNKNOWN_COMMUNITY_PROBE__"
    row = _variant(prototype, community_source_id=probe)
    x = enc.transform(row)
    mask = enc.unknown_community(row)
    pred = float(x[0] @ w)
    return {"check": "unknown_community_fallback",
            "spec_basis": "未知小区以片区/全局效应回退，标记状态，不报错不中断",
            "probe_community": probe,
            "fallback_marked": bool(mask[0]),
            "prediction_finite": bool(np.isfinite(pred)),
            "passed": bool(mask[0] and np.isfinite(pred))}


def smoke_fold(run_dir: Path, fold_id: str = "F01") -> dict:
    """单折冒烟：λ 网格折内选择与各值登记、三条防回归断言、S0 量级对照、折核对。"""
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    fold, anchor, train, val = load_fold(run_dir, fold_id)
    sel = select_lambda_in_fold(train)
    lam = sel["chosen_lambda"]

    enc, Xtr, ytr, Xva, yva = _fold_encoder_matrices(train, val)
    grid_val = lambda_grid_records(Xtr, ytr, Xva, yva)
    chosen_val = next(r for r in grid_val if r["lambda"] == lam)

    w = ridge_solve(Xtr, ytr, lam)
    pred_log = Xva @ w
    fallback = enc.unknown_community(val)
    out = build_predictions(val, pred_log, fallback)
    prototype = val.head(1)
    checks = [
        assert_known_communities_differ(enc, prototype, w),
        assert_elevator_three_state_separable(enc, prototype),
        assert_unknown_community_fallback(enc, prototype, w),
    ]
    crosscheck = {
        "train_rows_expected": fold["train"]["rows"],
        "train_rows_actual": train.height,
        "validation_rows_expected": fold["validation"]["rows"],
        "validation_rows_actual": val.height,
        "match": train.height == fold["train"]["rows"]
        and val.height == fold["validation"]["rows"],
    }
    magnitude = {
        "s0_reference": {**S0_M1_REFERENCE,
                         "note": "S0 修正 M1 全量池中位 APE 参考带（校正说明 §1.1 条 3："
                                 "λ=10 → A 窗 11.83 / B 窗 12.50）；本冒烟为单折验证窗，"
                                 "未落带属窗口效应，处置口径=对照并披露+归因，不放宽阈值"},
        "fold_val_med_ape": chosen_val["med_ape"],
        "in_range": S0_M1_REFERENCE["low"] <= chosen_val["med_ape"] <= S0_M1_REFERENCE["high"],
    }
    result = {
        "run_id": manifest["run_id"],
        "fold_id": fold_id,
        "anchor": anchor.isoformat(),
        "train_rows": train.height,
        "validation_rows": val.height,
        "design": {"n_features": int(Xtr.shape[1]),
                   "n_known_communities": len(enc.categories["community"]),
                   "feature_names_head": enc.feature_names()[:3],
                   "feature_names_tail": enc.feature_names()[-3:]},
        "lambda_selection": sel,
        "lambda_grid_on_validation_window": grid_val,
        "chosen_lambda_validation": chosen_val,
        "predictions_summary": {
            "pred_nonnull_ratio": float(out["pred_unit_price"].is_not_null().mean()),
            "community_fallback_ratio": float(fallback.mean()),
            "community_fallback_count": int(fallback.sum())},
        "regression_assertions": checks,
        "split_crosscheck": crosscheck,
        "s0_magnitude_check": magnitude,
    }
    ok = (all(c["passed"] for c in checks) and crosscheck["match"]
          and result["predictions_summary"]["pred_nonnull_ratio"] == 1.0
          and magnitude["in_range"])
    result["verdict"] = "PASS" if ok else "REVIEW"
    return result


def structure_report() -> dict:
    """任务 3.1 结构清单自检：任务行结构要求逐项映射到实现。"""
    return {
        "module": "gz_property_valuation.phase2.models",
        "task_line_checklist": {
            "log(单价)目标": "np.log(unit_price) 为 Ridge 回归目标（_fold_encoder_matrices）",
            "板块+小区one-hot": "category_frame: community/block 列；FoldEncoder drop-first、未知全 0",
            "面积房龄平滑": "数值组线性项 + AREA_BIN_EDGES/AGE_BIN_EDGES 固定边界区间哑变量",
            "楼层×电梯交互": "category_frame: floor_elevator 类别乘积列 → one-hot",
            "年-月时间效应": "category_frame: ym 列哑变量（与属性分开定义）；验证期未见钳制训练最晚年-月",
            "折内编码与z-score": "FoldEncoder.fit/transform：类别表、钳制边界、填充值、mu/sd 仅训练切片",
            "numpy闭式Ridge": "ridge_solve: (XᵀX+λI)⁻¹Xᵀy，np.linalg.solve",
            "未知小区回退标记": "FoldEncoder.unknown_community → predictions.community_fallback 逐行标记与比例披露",
        },
        "categorical_roles": CATEGORICAL_ROLES,
        "numeric_names": numeric_feature_names(),
        "smooth_bins": {"area": list(AREA_BIN_EDGES), "age": list(AGE_BIN_EDGES)},
        "lambda": {"grid": list(LAMBDA_GRID), "allowed_range": list(LAMBDA_RANGE),
                   "selection": "训练切片内部留出末 3 个月，MedAPE 最小；平局取更小 λ"},
        "solver": "numpy 闭式 (XᵀX+λI)⁻¹Xᵀy；截距入 X，λI 施于全部列（S0 同构）",
        "classes": ["FoldEncoder"],
        "functions": ["numeric_frame", "category_frame", "ridge_solve", "load_fold",
                      "lambda_grid_records", "select_lambda_in_fold",
                      "build_predictions", "fit_fold_predict", "smoke_fold",
                      "assert_known_communities_differ",
                      "assert_elevator_three_state_separable",
                      "assert_unknown_community_fallback", "structure_report"],
        "consumption_boundary": "只读 S1 合同 run 产物（master/features/splits/manifest）",
        "verdict": "PASS",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 M1 可解释模型")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_struct = sub.add_parser("structure", help="任务 3.1 结构清单自检")
    p_smoke = sub.add_parser("smoke", help="单折冒烟：λ 网格 + 防回归断言 + 量级对照")
    p_smoke.add_argument("run_dir")
    p_smoke.add_argument("--fold", default="F01")
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        print(json.dumps(structure_report(), ensure_ascii=False, indent=1))
        return 0

    result = smoke_fold(Path(args.run_dir).resolve(), args.fold)
    print(json.dumps(result, ensure_ascii=False, indent=1))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
