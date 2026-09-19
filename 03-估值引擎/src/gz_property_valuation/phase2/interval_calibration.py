# -*- coding: utf-8 -*-
"""phase2 S4b 区间校准：校准段 462 条隔离读取 + 分层经验分位 + bootstrap 宽度置信。

行为规格（specs/phase2-candidate-freeze/spec.md「校准段隔离与区间校准」；design D5）：

- 预留校准段 462 条（2026-05-30~07-20 行序后半）在本能力中的**唯一合法用途**为区间校准
  拟合；接触点登记，不得用于候选选择/结构选择/阈值试探。
- 以 V1 残差（log 空间 ``e = log 真值 − log 预测``）按降级状态 × B0 支持度层级交叉分层做
  经验分位；层内样本 <40 时并入父层（``<deg>|global`` → ``global``），并层规则登记。
- 输出名义 80% / 90% 价格区间（``pred × exp(q_lo)``、``pred × exp(q_hi)``）；宽度分布与
  内拟合覆盖登记；bootstrap（2000 次）给宽度置信区间。
- 限度标注：覆盖为校准段内拟合值，**外检验归 S5**。

只读消费 S1 run 主表；不读 staged 原始数据；不修改任何既有实现。
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

import numpy as np

NOMINAL_LEVELS = (0.80, 0.90)
MIN_LAYER_ROWS = 40
BOOTSTRAP_ITERATIONS = 2000
BOOTSTRAP_SEED = 20260913
RESERVED_START = date(2026, 4, 21)
RESERVED_END = date(2026, 7, 20)
TUNING_ROWS = 463
CALIBRATION_ROWS = 462
LIMIT_STATEMENT = ("内拟合覆盖、外检验归 S5：区间覆盖为校准段 462 条内拟合值，"
                   "不构成独立样本验收；S5 新成交检验前不得据此宣称预测区间有效。")


def calibration_segment(master) -> "object":
    """校准段 462 条：预留带按 (sale_date_d, source_record_id) 排序后按行数二分的后半。

    与 S4-A E4 段规则同口径（s4a_05）：奇数行归调参段，故同日可跨两段边界。
    """
    import polars as pl
    reserved = (master.filter((pl.col("sale_date_d") >= RESERVED_START)
                              & (pl.col("sale_date_d") <= RESERVED_END))
                .sort(["sale_date_d", "source_record_id"]))
    assert reserved.height == TUNING_ROWS + CALIBRATION_ROWS, \
        f"预留带行数异常 {reserved.height}"
    return reserved.tail(CALIBRATION_ROWS)


def layer_quantiles(e: np.ndarray, nominal: float) -> dict:
    alpha = 1.0 - nominal
    lo = float(np.quantile(e, alpha / 2.0))
    hi = float(np.quantile(e, 1.0 - alpha / 2.0))
    return {"lo_log": lo, "hi_log": hi, "n": int(e.size),
            "width_ratio": float(np.exp(hi) - np.exp(lo))}


def fit_interval_table(residuals: np.ndarray, strata: list[str],
                       nominal_levels=NOMINAL_LEVELS,
                       min_layer: int = MIN_LAYER_ROWS) -> dict:
    """分层经验分位表：层内 <min_layer 并入父层；返回各层 80/90 分位与并层登记。"""
    buckets: dict[str, list] = defaultdict(list)
    for e, s in zip(np.asarray(residuals, dtype=np.float64), strata):
        buckets[s].append(float(e))
    layers: dict[str, dict] = {}
    merge_log: list[dict] = []
    parent_buckets: dict[str, list] = defaultdict(list)
    glob: list[float] = []
    for s, vals in sorted(buckets.items()):
        deg = s.split("|")[0]
        if len(vals) >= min_layer:
            layers[s] = {str(int(nl * 100)): layer_quantiles(np.asarray(vals), nl)
                         for nl in nominal_levels}
            merge_log.append({"stratum": s, "n": len(vals), "action": "own_layer"})
        else:
            parent_buckets[deg].extend(vals)
            merge_log.append({"stratum": s, "n": len(vals),
                              "action": "merged_to", "target": f"{deg}|global"})
    for deg, vals in sorted(parent_buckets.items()):
        key = f"{deg}|global"
        if len(vals) >= min_layer:
            layers[key] = {str(int(nl * 100)): layer_quantiles(np.asarray(vals), nl)
                           for nl in nominal_levels}
            merge_log.append({"stratum": key, "n": len(vals), "action": "parent_layer"})
        else:
            glob.extend(vals)
            merge_log.append({"stratum": key, "n": len(vals),
                              "action": "merged_to", "target": "global"})
    if glob:
        layers["global"] = {str(int(nl * 100)): layer_quantiles(np.asarray(glob), nl)
                            for nl in nominal_levels}
        merge_log.append({"stratum": "global", "n": len(glob), "action": "global_layer"})
    return {"nominal_levels": [float(x) for x in nominal_levels],
            "min_layer_rows": int(min_layer),
            "residual_definition": "e = log(真值单价) − log(预测单价)；区间 = 预测 × exp(分位)",
            "merge_rule": "层内样本 <min_layer 并入父层（<deg>|global → global）",
            "layers": layers, "merge_log": merge_log,
            "total_rows": int(len(residuals))}


def bootstrap_width_ci(layers: dict, residuals: np.ndarray, strata: list[str],
                       nominal_levels=NOMINAL_LEVELS, n_boot: int = BOOTSTRAP_ITERATIONS,
                       seed: int = BOOTSTRAP_SEED) -> dict:
    """按层重抽残差（有放回，n 同层样本量）给各层宽度的 95% 置信区间。"""
    buckets: dict[str, list] = defaultdict(list)
    for e, s in zip(np.asarray(residuals, dtype=np.float64), strata):
        buckets[s].append(float(e))
    # 构造 layer → 样本（含并层）
    layer_samples: dict[str, list] = defaultdict(list)
    stratum_to_layer = {}
    for s, vals in buckets.items():
        if s in layers:
            stratum_to_layer[s] = s
        else:
            deg = s.split("|")[0]
            if f"{deg}|global" in layers:
                stratum_to_layer[s] = f"{deg}|global"
            else:
                stratum_to_layer[s] = "global"
        layer_samples[stratum_to_layer[s]].extend(vals)
    rng = np.random.default_rng(seed)
    out = {}
    for name, vals in sorted(layer_samples.items()):
        e = np.asarray(vals, dtype=np.float64)
        dist = {str(int(nl * 100)): [] for nl in nominal_levels}
        for _ in range(n_boot):
            idx = rng.integers(0, e.size, e.size)
            sample = e[idx]
            for nl in nominal_levels:
                q = layer_quantiles(sample, nl)
                dist[str(int(nl * 100))].append(q["width_ratio"])
        out[name] = {lvl: {"width_ratio_median": float(np.median(v)),
                           "ci95": [float(np.percentile(v, 2.5)),
                                    float(np.percentile(v, 97.5))]}
                     for lvl, v in dist.items()}
    return {"iterations": int(n_boot), "seed": int(seed),
            "resampling": "按层对残差有放回重抽，每层样本量固定为该层 n",
            "layers": out}


def resolve_layer(layers: dict, stratum: str) -> str:
    """按并层规则解析某分层实际使用的层键（无自身层 → 父层 → global）。"""
    if stratum in layers:
        return stratum
    deg = stratum.split("|")[0]
    if f"{deg}|global" in layers:
        return f"{deg}|global"
    return "global"


def contact_registration(run_id: str, n_rows: int, columns_used: list[str]) -> dict:
    return {
        "run_id": run_id, "purpose": "区间校准拟合（唯一合法用途）",
        "segment": "预留校准段 462 条（2026-05-30~2026-07-20 行序后半）",
        "rows": int(n_rows),
        "columns_used": columns_used,
        "contact_points": 1,
        "other_uses": "无（候选选择/结构选择/阈值试探均未接触本段）",
        "rule": "出现区间校准以外的用途即越界停线（proposal 停止条件）",
    }


def structure_report() -> dict:
    return {"module": "gz_property_valuation.phase2.interval_calibration",
            "calibration_segment_rows": CALIBRATION_ROWS,
            "nominal_levels": list(NOMINAL_LEVELS),
            "min_layer_rows": MIN_LAYER_ROWS,
            "bootstrap": {"iterations": BOOTSTRAP_ITERATIONS, "seed": BOOTSTRAP_SEED},
            "limit": LIMIT_STATEMENT,
            "verdict": "PASS"}


# ---------------------------------------------------------------- 双源区间校准（S4C 新增）
# 本段为 fix-phase2-interval-normal-strata 新增：正常分层以训练窗 18 折验证带 OOF 残差拟合
# （train_oof），降级分层沿用预留校准段真保持残差（reserved_holdout，值自旧表逐位复用）。
# 既有函数（calibration_segment / layer_quantiles / fit_interval_table / bootstrap_width_ci /
# resolve_layer / contact_registration / structure_report）行为零改动。

OOF_SOURCE = "train_oof"
RESERVED_SOURCE = "reserved_holdout"


def oof_residual(true_unit_price, pred_unit_price) -> "np.ndarray":
    """折外（OOF）残差：``e = log(真值单价) − log(折外预测单价)``（与区间分位同 log 空间）。

    输入为未取对数的单价；两数组形状须一致。调用方须保证预测来自**未训练该行的折模型**
    （验证带行），不得以折内预测冒充折外（无泄漏要求）。
    """
    true = np.asarray(true_unit_price, dtype=np.float64)
    pred = np.asarray(pred_unit_price, dtype=np.float64)
    if true.shape != pred.shape:
        raise ValueError(f"真值/预测形状不一致：{true.shape} != {pred.shape}")
    return np.log(true) - np.log(pred)


def assemble_dual_source_table(normal_residuals, normal_strata, degraded_layers,
                               supersedes: dict, degraded_merge_log: list | None = None,
                               nominal_levels=NOMINAL_LEVELS,
                               min_layer: int = MIN_LAYER_ROWS) -> dict:
    """双源区间表组装（schema v2）：

    - **正常层**：由 OOF 残差按既有 :func:`fit_interval_table` 规则（min_layer 并层）拟合，
      逐层附 ``residual_source="train_oof"`` 与层样本量 ``n``；
    - **降级层**：``degraded_layers`` 自旧表**原样复制**（分位浮点值不动），逐层附
      ``residual_source="reserved_holdout"`` 与 ``n``；
    - 顶层登记 ``supersedes``（旧 run id、换代原因）与残差来源说明。

    本函数不修改既有函数行为；降级层分位值不做任何再计算（调用方负责逐位断言）。
    """
    normal = fit_interval_table(normal_residuals, normal_strata, nominal_levels, min_layer)
    normal_layers = dict(normal["layers"])
    normal_merge_log = [dict(m) for m in normal["merge_log"]]
    if "global" in normal_layers:
        # fit_interval_table 的最终兜底 ``global`` 键在本双源表中留给降级复制层
        # （reserved_holdout），故正常侧兜底改名为 ``normal|global``，避免同键覆盖；
        # 与 design D4「并层 <deg>|global」一致。
        normal_layers["normal|global"] = normal_layers.pop("global")
        for m in normal_merge_log:
            if m.get("stratum") == "global":
                m["stratum"] = "normal|global"
                m["renamed_from"] = "global"
            if m.get("target") == "global":
                m["target"] = "normal|global"
    layers: dict[str, dict] = {}
    for key in sorted(normal_layers):
        lvls = normal_layers[key]
        layer = {lvl: dict(q) for lvl, q in lvls.items()}
        layer["residual_source"] = OOF_SOURCE
        layer["n"] = int(lvls["80"]["n"])
        layers[key] = layer
    copied_log: list[dict] = []
    for key in sorted(degraded_layers):
        lvls = degraded_layers[key]
        layer = {lvl: dict(q) for lvl, q in lvls.items()}
        layer["residual_source"] = RESERVED_SOURCE
        layer["n"] = int(lvls["80"]["n"])
        layers[key] = layer
        copied_log.append({"stratum": key, "n": int(lvls["80"]["n"]),
                           "action": "copied_from_old_table",
                           "residual_source": RESERVED_SOURCE})
    degraded_part = ([{**m, "residual_source": RESERVED_SOURCE} for m in degraded_merge_log]
                     if degraded_merge_log else copied_log)
    merge_log = ([{**m, "residual_source": OOF_SOURCE} for m in normal_merge_log]
                 + degraded_part)
    return {
        "schema_version": "phase2-interval-table-v2",
        "nominal_levels": [float(x) for x in nominal_levels],
        "min_layer_rows": int(min_layer),
        "residual_definition": normal["residual_definition"],
        "merge_rule": normal["merge_rule"],
        "layers": layers,
        "merge_log": merge_log,
        "normal_layers": sorted(normal_layers),
        "degraded_layers": sorted(degraded_layers),
        "normal_total_rows": int(normal["total_rows"]),
        "residual_sources": {"normal": OOF_SOURCE, "degraded": RESERVED_SOURCE},
        "supersedes": dict(supersedes),
    }
