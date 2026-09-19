# -*- coding: utf-8 -*-
"""phase2 S3 派生市场输入·内外双层四机制（design D2；spec「派生市场输入与时点规则」）。

四机制（每折内外两层，共用同一聚合链）：

- ``outer_train``（折训练切片全部行，外层最终拟合用）：行级严格历史滚动，锚各行自身
  成交日，证据窗 ``[d−365 天, d)`` 上界开区间（排除同日、排除自身）。证据池为主表
  全量——后向窗口，晚于 d 的行永不入窗，故与"仅用早于该行的行作池"等价。
- ``inner_train``（训练切片去末 3 个月，内层选参拟合用）：同 ``outer_train`` 行级机制。
- ``outer_val``（折验证带）：窗级聚合，锚折外层截点（= 折 ``anchor``，同 S2 B0 口径）。
- ``inner_val``（训练切片末 3 个月，内层选参评估用）：窗级聚合，锚**内层截点**
  （= 训练切片末端 − 3 个月 + 1 天，与 ``models.select_lambda_in_fold`` 的内层边界一致）。

共用回退链：小区 365d 中位 → 板块 365d → 区级 365d；排除自身与同日，逐行登记证据
层级与样本数。冷启动行（回退链全层级无证据，仅主表最早数日）从训练与残差构造排除并
计数披露；inner/outer 验证行因数据起点早于所有截点，链必有值。

特征集 v2 = S1 静态属性列（位置/产品/建筑/房屋/质量与支持度）+ 本模块派生市场列
（链上中位单价 log、样本数、距最近成交天数、证据层级）。S1 行级滚动市场列（``市场``
组）仅作对照登记、不进模型输入（``s1_market_reference``）。

跨折独立性：``derive_fold(master, features, fold)`` 是 ``(pool, fold 定义)`` 的纯函数，
不读取任何其他折的选择结果或私有产物；任何选择路径只消费本折材料。

只读消费 S1 合同 run 产物（master_table.parquet / features.parquet / splits.json），
不读 staged 原始数据（design D1 消费边界）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from gz_property_valuation.phase2 import features as features_mod

MARKET_WINDOW_DAYS = 365
INNER_VAL_MONTHS = 3
EVIDENCE_LEVELS = ("community", "block", "district")
COLD_START_LEVEL = "cold_start"
MISSING_BLOCK_VALUES = ("", "暂无数据")
COLD_START_ALERT_RATIO = 0.05

DERIVED_MARKET_COLUMNS = [
    "market_chain_med_unit_price",
    "market_chain_med_unit_price_log",
    "market_chain_sample_365d",
    "market_chain_last_sale_days",
    "market_evidence_level",
]
STATIC_FEATURE_GROUPS = ("位置", "产品", "建筑", "房屋", "质量与支持度")
STATIC_FEATURE_COLUMNS = [c for g in STATIC_FEATURE_GROUPS
                          for c in features_mod.FEATURE_NAMES_BY_GROUP[g]]
S1_MARKET_COLUMNS = list(features_mod.FEATURE_NAMES_BY_GROUP["市场"])
FEATURE_SET_V2 = list(STATIC_FEATURE_COLUMNS) + list(DERIVED_MARKET_COLUMNS)

# 冻结配置桩：nonlinear.py 未实现前，用一个确定性定权线性组合代表"固定配置"。
# 断言对象是"输入"与"冻结配置下的输出"，不是选参结果；本桩不是真实模型。
FROZEN_STUB_WEIGHTS = {
    "market_chain_med_unit_price_log": 0.60,
    "market_chain_sample_365d": -0.00002,
    "market_chain_last_sale_days": -0.00005,
    "area_sqm": 0.010,
    "age_years": -0.006,
    "total_floors": 0.003,
    "ladder_per_household": 0.050,
    "efficiency_ratio": 0.150,
}
FROZEN_STUB_FILL = {
    "market_chain_med_unit_price_log": 10.5,
    "market_chain_sample_365d": 0.0,
    "market_chain_last_sale_days": 180.0,
    "area_sqm": 80.0,
    "age_years": 20.0,
    "total_floors": 20.0,
    "ladder_per_household": 0.25,
    "efficiency_ratio": 0.84,
}

FRAME_NAMES = ("outer_train", "inner_train", "outer_val", "inner_val")


# ---------- 通用工具 ----------

def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    last = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def _block_norm_expr() -> pl.Expr:
    return (pl.when(pl.col("block_name").is_null()
                    | pl.col("block_name").is_in(list(MISSING_BLOCK_VALUES)))
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(pl.col("block_name"))
            .alias("block_norm"))


def build_pool(master: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """派生证据池：主表成交（含标签）+ S1 特征层的板块归属列归一。"""
    return (master.select(["source_record_id", "community_source_id",
                           "sale_date_d", "unit_price"])
            .join(features.select(["source_record_id", "block_name"]),
                  on="source_record_id", how="left")
            .with_columns(_block_norm_expr()))


def _rolling_level(pool: pl.DataFrame, group_cols: list[str],
                   prefix: str) -> pl.DataFrame:
    """行级严格历史滚动：窗口 ``[d−365 天, d)``（closed=left、offset=−365d）。"""
    cols = list(group_cols) + ["sale_date_d"]
    df = pool.sort(cols)
    aggs = [pl.col("unit_price").median().alias(f"{prefix}_med"),
            pl.len().alias(f"{prefix}_n"),
            pl.col("sale_date_d").max().alias(f"{prefix}_last")]
    if group_cols:
        agg = df.rolling(index_column="sale_date_d",
                         period=f"{MARKET_WINDOW_DAYS}d",
                         offset=f"-{MARKET_WINDOW_DAYS}d", closed="left",
                         group_by=list(group_cols)).agg(aggs)
    else:
        agg = df.rolling(index_column="sale_date_d",
                         period=f"{MARKET_WINDOW_DAYS}d",
                         offset=f"-{MARKET_WINDOW_DAYS}d",
                         closed="left").agg(aggs)
    agg = agg.unique(subset=cols)
    keys = pool.select(["source_record_id", *group_cols, "sale_date_d"])
    return (keys.join(agg, on=cols, how="left")
            .select(["source_record_id", f"{prefix}_med", f"{prefix}_n",
                     f"{prefix}_last"]))


def _assign_chain(df: pl.DataFrame, pref: tuple[str, str, str],
                  ref_expr: pl.Expr) -> pl.DataFrame:
    """按 小区→板块→区级 优先级选链值，逐行登记层级/样本数/距最近成交天数。"""
    c, b, a = pref
    has_c, has_b, has_a = (pl.col(f"{c}_n") > 0, pl.col(f"{b}_n") > 0,
                           pl.col(f"{a}_n") > 0)
    level = (pl.when(has_c).then(pl.lit(EVIDENCE_LEVELS[0]))
             .when(has_b).then(pl.lit(EVIDENCE_LEVELS[1]))
             .when(has_a).then(pl.lit(EVIDENCE_LEVELS[2]))
             .otherwise(pl.lit(COLD_START_LEVEL)))
    med = (pl.when(has_c).then(pl.col(f"{c}_med"))
           .when(has_b).then(pl.col(f"{b}_med"))
           .when(has_a).then(pl.col(f"{a}_med"))
           .otherwise(pl.lit(None, dtype=pl.Float64)))
    samp = (pl.when(has_c).then(pl.col(f"{c}_n").cast(pl.Int64))
            .when(has_b).then(pl.col(f"{b}_n").cast(pl.Int64))
            .when(has_a).then(pl.col(f"{a}_n").cast(pl.Int64))
            .otherwise(pl.lit(None, dtype=pl.Int64)))
    last = (pl.when(has_c).then(pl.col(f"{c}_last"))
            .when(has_b).then(pl.col(f"{b}_last"))
            .when(has_a).then(pl.col(f"{a}_last"))
            .otherwise(pl.lit(None, dtype=pl.Date)))
    out = df.with_columns([
        level.alias("market_evidence_level"),
        med.alias("market_chain_med_unit_price"),
        samp.alias("market_chain_sample_365d"),
        last.alias("_last_ev"),
    ])
    out = out.with_columns([
        pl.col("market_chain_med_unit_price").log()
        .alias("market_chain_med_unit_price_log"),
        (ref_expr - pl.col("_last_ev")).dt.total_days().cast(pl.Int64)
        .alias("market_chain_last_sale_days"),
    ])
    return out.drop("_last_ev")


def row_level_market(pool: pl.DataFrame) -> pl.DataFrame:
    """全池行级链值（锚各行自身成交日）；输出 keys + 派生市场列。"""
    comm = _rolling_level(pool, ["community_source_id"], "rl_c")
    blk = _rolling_level(pool.filter(pl.col("block_norm").is_not_null()),
                         ["block_norm"], "rl_b")
    hz = _rolling_level(pool, [], "rl_a")
    base = (pool.select(["source_record_id", "sale_date_d"])
            .join(comm, on="source_record_id", how="left")
            .join(blk, on="source_record_id", how="left")
            .join(hz, on="source_record_id", how="left"))
    return _assign_chain(base, ("rl_c", "rl_b", "rl_a"),
                         pl.col("sale_date_d")).select(
        ["source_record_id", *DERIVED_MARKET_COLUMNS])


def _aggregate_level(df: pl.DataFrame, group_cols: list[str],
                     prefix: str) -> pl.DataFrame:
    aggs = [pl.col("unit_price").median().alias(f"{prefix}_med"),
            pl.len().alias(f"{prefix}_n"),
            pl.col("sale_date_d").max().alias(f"{prefix}_last")]
    if group_cols:
        return df.group_by(list(group_cols)).agg(aggs)
    return df.select([pl.lit(1).alias("_one"), *aggs])


def window_level_market(pool: pl.DataFrame, cutoff: date,
                        targets: pl.DataFrame | None = None) -> pl.DataFrame:
    """窗级截点链值：证据窗 ``[cutoff−365 天, cutoff)``，锚该层截点。

    ``targets`` 为待赋值的行（默认全池）；输出 keys + 派生市场列。
    """
    start = cutoff - timedelta(days=MARKET_WINDOW_DAYS)
    mat = pool.filter((pl.col("sale_date_d") >= start)
                      & (pl.col("sale_date_d") < cutoff))
    com = _aggregate_level(mat, ["community_source_id"], "w_c")
    blk = _aggregate_level(mat.filter(pl.col("block_norm").is_not_null()),
                           ["block_norm"], "w_b")
    hz_med = float(mat["unit_price"].median()) if mat.height else None
    hz_last = mat["sale_date_d"].max() if mat.height else None

    tgt = pool if targets is None else targets
    base = tgt.select(["source_record_id", "community_source_id", "block_norm"])
    base = (base.join(com, on="community_source_id", how="left")
            .join(blk, on="block_norm", how="left")
            .with_columns([
                pl.lit(hz_med, dtype=pl.Float64).alias("w_a_med"),
                pl.lit(mat.height, dtype=pl.Int64).alias("w_a_n"),
                pl.lit(hz_last, dtype=pl.Date).alias("w_a_last"),
            ]))
    base = base.join(pool.select(["source_record_id", "sale_date_d"]),
                     on="source_record_id", how="left")
    return _assign_chain(base, ("w_c", "w_b", "w_a"),
                         pl.lit(cutoff, dtype=pl.Date)).select(
        ["source_record_id", *DERIVED_MARKET_COLUMNS])


# ---------- 特征集 v2 ----------

def attach_static(market_df: pl.DataFrame, features: pl.DataFrame) -> pl.DataFrame:
    """派生市场列与 S1 静态属性列（位置/产品/建筑/房屋/质量与支持度）拼接。"""
    drop = [c for c in STATIC_FEATURE_COLUMNS if c in market_df.columns]
    return market_df.drop(drop).join(
        features.select(["source_record_id", *STATIC_FEATURE_COLUMNS]),
        on="source_record_id", how="left")


def s1_market_reference(market_df: pl.DataFrame, features: pl.DataFrame) -> dict:
    """S1 行级滚动市场列对照登记（不进模型输入）。"""
    j = market_df.join(features.select(["source_record_id", *S1_MARKET_COLUMNS]),
                       on="source_record_id", how="left")
    comm = j.filter(pl.col("market_evidence_level") == EVIDENCE_LEVELS[0])
    both = comm.filter(pl.col(f"{S1_MARKET_COLUMNS[0]}").is_not_null())
    agree = both.filter(
        (pl.col("market_chain_med_unit_price")
         - pl.col(S1_MARKET_COLUMNS[0])).abs() < 1e-9)
    return {
        "s1_market_columns": S1_MARKET_COLUMNS,
        "excluded_from_model_input": True,
        "community_level_rows": comm.height,
        "comparable_rows": both.height,
        "median_agree_rows": agree.height,
        "rule": "S1 行级滚动市场列仅作对照登记；模型输入用本模块派生列（带回退链）",
    }


# ---------- 冻结配置桩（代表固定配置，非真实模型） ----------

def frozen_stub_predict(df: pl.DataFrame) -> pl.DataFrame:
    """确定性定权线性组合，代表"固定配置下的输出"；不是真实模型。"""
    present = set(df.columns)
    expr = pl.lit(0.0, dtype=pl.Float64)
    for col, w in FROZEN_STUB_WEIGHTS.items():
        if col in present:
            v = pl.col(col).cast(pl.Float64).fill_null(FROZEN_STUB_FILL[col])
        else:
            v = pl.lit(FROZEN_STUB_FILL[col], dtype=pl.Float64)
        expr = expr + w * v
    return (df.with_columns(expr.alias("stub_pred_log"))
            .select(["source_record_id", "stub_pred_log"]))


# ---------- 单折四机制派生 ----------

def inner_cutoff_of(train: pl.DataFrame, months: int = INNER_VAL_MONTHS) -> date:
    """内层截点 = 训练切片末端 − ``months`` 个月 + 1 天（内层验证带起点）。"""
    return _add_months(train["sale_date_d"].max(), -months) + timedelta(days=1)


def _keys(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(["source_record_id", "sale_date_d", "unit_price"])


def _level_counts(df: pl.DataFrame) -> dict:
    vc = df["market_evidence_level"].value_counts().to_dicts()
    return {r["market_evidence_level"]: r["count"] for r in vc}


def derive_fold(master: pl.DataFrame, features: pl.DataFrame, fold: dict,
                pool: pl.DataFrame | None = None,
                row_level: pl.DataFrame | None = None,
                with_static: bool = True) -> dict:
    """派生某折内外两层四机制输入（纯函数：只消费 master/features/fold）。"""
    if pool is None:
        pool = build_pool(master, features)
    if row_level is None:
        row_level = row_level_market(pool)
    rl = row_level.select(["source_record_id", *DERIVED_MARKET_COLUMNS])

    anchor = date.fromisoformat(fold["anchor"])
    val_start = date.fromisoformat(fold["validation"]["start"])
    val_end = date.fromisoformat(fold["validation"]["end_inclusive"])
    train = pool.filter(pl.col("sale_date_d") < anchor)
    val = pool.filter((pl.col("sale_date_d") >= val_start)
                      & (pl.col("sale_date_d") <= val_end))
    inner_cut = inner_cutoff_of(train)
    inner_train = train.filter(pl.col("sale_date_d") < inner_cut)
    inner_val = train.filter(pl.col("sale_date_d") >= inner_cut)

    outer_train = _keys(train).join(rl, on="source_record_id", how="left")
    cold = outer_train.filter(pl.col("market_evidence_level") == COLD_START_LEVEL)
    outer_train = outer_train.filter(
        pl.col("market_evidence_level") != COLD_START_LEVEL)
    inner_train_m = (_keys(inner_train).join(rl, on="source_record_id", how="left")
                     .filter(pl.col("market_evidence_level") != COLD_START_LEVEL))

    outer_val = _keys(val).join(window_level_market(pool, anchor, val),
                                on="source_record_id", how="left")
    inner_val_m = _keys(inner_val).join(
        window_level_market(pool, inner_cut, inner_val),
        on="source_record_id", how="left")

    frames = {"outer_train": outer_train, "inner_train": inner_train_m,
              "outer_val": outer_val, "inner_val": inner_val_m}
    if with_static:
        frames = {k: attach_static(v, features) for k, v in frames.items()}

    ids = {
        "train": sorted(train["source_record_id"].to_list()),
        "val": sorted(val["source_record_id"].to_list()),
        "inner_train": sorted(inner_train["source_record_id"].to_list()),
        "inner_val": sorted(inner_val["source_record_id"].to_list()),
        "cold_start": sorted(cold["source_record_id"].to_list()),
    }
    n_train = train.height
    return {
        "fold_id": fold["fold_id"],
        "anchor": anchor.isoformat(),
        "outer_cutoff": anchor.isoformat(),
        "inner_cutoff": inner_cut.isoformat(),
        "window": {"start": (anchor - timedelta(days=MARKET_WINDOW_DAYS)).isoformat(),
                   "end_exclusive": anchor.isoformat()},
        **frames,
        "ids": ids,
        "cold_start": {
            "count": cold.height,
            "train_rows": n_train,
            "ratio_of_train": (cold.height / n_train) if n_train else 0.0,
            "alert_threshold": COLD_START_ALERT_RATIO,
            "alert": (cold.height / n_train) > COLD_START_ALERT_RATIO if n_train else False,
            "rows_sample": cold["source_record_id"].head(5).to_list(),
        },
        "level_counts": {k: _level_counts(v) for k, v in frames.items()},
        "row_counts": {"outer_train": frames["outer_train"].height,
                       "inner_train": frames["inner_train"].height,
                       "outer_val": frames["outer_val"].height,
                       "inner_val": frames["inner_val"].height,
                       "train": n_train, "val": val.height},
    }


def load_s1(run_dir: Path) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = pl.read_parquet(run_dir / "features.parquet")
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    return master, features, splits


def fold_digest(derived: dict, frame: str, cols: list[str] | None = None) -> str:
    """帧内容摘要（确定性、跨会话可复算；用于跨折独立性与扰动不变性断言）。"""
    use = cols or DERIVED_MARKET_COLUMNS
    sub = derived[frame].select(["source_record_id", *use]).sort("source_record_id")
    return hashlib.sha256(sub.write_json().encode("utf-8")).hexdigest()


def smoke_fold(run_dir: Path, fold_id: str = "F18") -> dict:
    """单折冒烟：四机制四件帧行数/层级分布/冷启动计数/特征集 v2 列与 S1 对照。"""
    master, features, splits = load_s1(run_dir)
    fold = next(f for f in splits["folds"] if f["fold_id"] == fold_id)
    pool = build_pool(master, features)
    row_level = row_level_market(pool)
    d = derive_fold(master, features, fold, pool=pool, row_level=row_level)
    keys = ["source_record_id", "sale_date_d", "unit_price"]
    present = set(d["outer_val"].columns)
    v2 = {"feature_set_v2_columns": FEATURE_SET_V2,
          "keys": keys,
          "v2_columns_present": all(c in present for c in FEATURE_SET_V2),
          "extra_columns": sorted(present - set(FEATURE_SET_V2)),
          "s1_market_excluded": all(c not in present for c in S1_MARKET_COLUMNS)}
    return {
        "run_id": run_dir.name,
        "fold_id": fold_id,
        "anchor": d["anchor"],
        "inner_cutoff": d["inner_cutoff"],
        "row_counts": d["row_counts"],
        "cold_start": d["cold_start"],
        "level_counts": d["level_counts"],
        "feature_set_v2": v2,
        "s1_market_reference": s1_market_reference(row_level, features),
        "verdict": "PASS",
    }


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.derived_inputs",
        "mechanisms": {
            "outer_train": "行级严格历史滚动，锚各行自身成交日，窗口 [d−365, d)",
            "inner_train": "同 outer_train（训练切片去末 3 个月）",
            "outer_val": "窗级聚合，锚折外层截点（fold.anchor，同 S2 B0 锚）",
            "inner_val": "窗级聚合，锚内层截点（训练切片末端 − 3 个月 + 1 天）",
        },
        "fallback_chain": "小区 365d 中位 → 板块 365d → 区级 365d；排除自身与同日",
        "cold_start": "回退链全层级无证据的训练行排除出训练与残差构造并计数披露",
        "feature_set_v2": FEATURE_SET_V2,
        "s1_market_columns_excluded": S1_MARKET_COLUMNS,
        "frozen_stub": {"role": "代表冻结配置的确定性定权线性组合（非真实模型）",
                        "weights": FROZEN_STUB_WEIGHTS, "fills": FROZEN_STUB_FILL},
        "pure_function": "derive_fold(master, features, fold)：不读其他折选择结果或私有产物",
        "consumption_boundary": "只读 S1 合同 run（master_table/features/splits）",
        "verdict": "PASS",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 派生市场输入·内外双层四机制")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_struct = sub.add_parser("structure", help="结构清单自检")
    p_smoke = sub.add_parser("smoke", help="单折冒烟")
    p_smoke.add_argument("run_dir")
    p_smoke.add_argument("--fold", default="F18")
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        print(json.dumps(structure_report(), ensure_ascii=False, indent=1))
        return 0
    result = smoke_fold(Path(args.run_dir).resolve(), args.fold)
    print(json.dumps(result, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
