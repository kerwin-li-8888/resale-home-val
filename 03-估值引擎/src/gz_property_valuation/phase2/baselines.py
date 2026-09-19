# -*- coding: utf-8 -*-
"""phase2 B0 近期小区基准与统一截点聚合。

行为规格（specs/phase2-baseline-comparisons/spec.md「B0 近期小区基准口径」、design D2）：

- 以统一信息截点为锚（折验证窗锚点 anchor），窗口 ``[anchor−365天, anchor)``，上界
  开区间；同一评估窗内全部目标共用同一锚点，窗口不随抽样后个体最后成交漂移。
- 预测链：同小区成交单价中位 → 无小区样本回退板块（S1 特征层 ``block_name``，缺失
  或「暂无数据」跳过）→ 区级（云溪区）中位；逐房登记证据层级，回退比例逐级披露。
- 聚合材料严格早于锚点，结构性排除目标自身与同日成交；实现按折断言核验（窗口材料
  日期上界 < 锚点 ≤ 验证目标日期下界，且窗口材料与验证目标记录 ID 不交）。
- 消费边界（design D1）：只读 S1 合同 run 产物（主表 + 特征层），不读 staged 原始数据。
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

WINDOW_DAYS = 365
EVIDENCE_LEVELS = ("community", "block", "district")
S0_M0_REFERENCE = {"low": 0.135, "high": 0.148}
MISSING_BLOCK_VALUES = ("", "暂无数据")


def fold_anchor(fold: dict) -> date:
    return date.fromisoformat(fold["anchor"])


def load_blocks(features: pl.DataFrame) -> pl.DataFrame:
    """从 S1 特征层提取 community_source_id → block_name 映射（小区内取排序首位，确定性）。"""
    return (features.select(["community_source_id", "block_name"])
            .filter(pl.col("block_name").is_not_null()
                    & (~pl.col("block_name").is_in(list(MISSING_BLOCK_VALUES))))
            .sort(["community_source_id", "block_name"])
            .unique(subset=["community_source_id"], keep="first", maintain_order=True))


def window_materials(pool: pl.DataFrame, anchor: date,
                     window_days: int = WINDOW_DAYS) -> pl.DataFrame:
    """统一截点窗口材料 ``[anchor−window_days, anchor)``，上界开区间。"""
    start = anchor - timedelta(days=window_days)
    return pool.filter(
        pl.col("sale_date_d").is_not_null()
        & (pl.col("sale_date_d") >= start)
        & (pl.col("sale_date_d") < anchor)
    )


def b0_level_tables(w: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, float]:
    com = w.group_by("community_source_id").agg(
        pl.col("unit_price").median().alias("pred_community"),
        pl.len().alias("n_community"))
    blk = w.group_by("block_name").agg(
        pl.col("unit_price").median().alias("pred_block"),
        pl.len().alias("n_block"))
    district_med = float(w["unit_price"].median())
    return com, blk, district_med


def predict_b0(pool: pl.DataFrame, targets: pl.DataFrame, anchor: date,
               blocks: pl.DataFrame, window_days: int = WINDOW_DAYS) -> pl.DataFrame:
    """对验证窗逐房目标给出 B0 预测（排除自身与同日的断言核验内嵌）。"""
    pool = pool.join(blocks, on="community_source_id", how="left")
    targets = targets.join(blocks, on="community_source_id", how="left")
    w = window_materials(pool, anchor, window_days)
    target_ids = set(targets["source_record_id"].to_list())
    overlap = int(w.filter(pl.col("source_record_id").is_in(target_ids)).height)
    w_max = w["sale_date_d"].max() if w.height else None
    t_min = targets["sale_date_d"].min()
    assert overlap == 0, f"窗口材料与验证目标记录 ID 相交 {overlap} 行"
    assert w_max is None or w_max < anchor, f"窗口材料上界 {w_max} 未严格早于锚点 {anchor}"
    assert t_min >= anchor, f"验证目标日期下界 {t_min} 早于锚点 {anchor}"

    com, blk, district_med = b0_level_tables(w)
    pred = (targets.select(["source_record_id", "community_source_id", "sale_date_d",
                            "unit_price"])
            .join(blocks, on="community_source_id", how="left")
            .join(com, on="community_source_id", how="left")
            .join(blk, on="block_name", how="left"))
    pred = pred.with_columns(
        pl.when(pl.col("pred_community").is_not_null()).then(pl.lit("community"))
        .when(pl.col("pred_block").is_not_null()).then(pl.lit("block"))
        .otherwise(pl.lit("district")).alias("evidence_level"))
    pred = pred.with_columns(
        pl.coalesce(["pred_community", "pred_block", pl.lit(district_med)])
        .alias("b0_pred"))
    return pred


def fallback_disclosure(pred: pl.DataFrame) -> dict:
    n = pred.height
    levels = pred["evidence_level"]
    counts = {lvl: int((levels == lvl).sum()) for lvl in EVIDENCE_LEVELS}
    return {lvl: {"count": counts[lvl], "ratio": counts[lvl] / n if n else None}
            for lvl in EVIDENCE_LEVELS}


def s0_m0r_variant(pool: pl.DataFrame, targets: pl.DataFrame) -> pl.DataFrame:
    """S0 脚本 M0' 精确同构变体（仅冒烟对照用，非 B0 口径）：

    窗口 [训练池最后成交日−1年, 最后成交日] 闭区间锚定（随最后成交漂移）、
    小区中位、无样本回退全训练池（全历史）中位，对应 run_data2_curve_fixed.m0r_predict。
    """
    last = pool["sale_date_d"].max()
    try:
        start = date(last.year - 1, last.month, last.day)
    except ValueError:
        start = date(last.year - 1, last.month, 28)
    recent = pool.filter(pl.col("sale_date_d") >= start)
    glob = float(pool["unit_price"].median())
    med = recent.group_by("community_source_id").agg(pl.col("unit_price").median())
    mp = dict(zip(med["community_source_id"].to_list(), med["unit_price"].to_list()))
    return targets.select(["source_record_id", "community_source_id", "unit_price"]).with_columns(
        pl.col("community_source_id").replace_strict(mp, default=glob).alias("m0r_pred"))


def smoke_fold(run_dir: Path, fold_id: str = "F01") -> dict:
    """单折冒烟：非空率、回退比例、MedAPE 与 S0 M0' 期望量级对照。"""
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = pl.read_parquet(run_dir / "features.parquet")
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    fold = next(f for f in splits["folds"] if f["fold_id"] == fold_id)
    anchor = fold_anchor(fold)

    pool = master.filter(pl.col("sale_date_d") < anchor)
    targets = master.filter(
        (pl.col("sale_date_d") >= date.fromisoformat(fold["validation"]["start"]))
        & (pl.col("sale_date_d") <= date.fromisoformat(fold["validation"]["end_inclusive"])))
    blocks = load_blocks(features)

    pred = predict_b0(pool, targets, anchor, blocks)
    pred = pred.with_columns(
        (pl.col("b0_pred") / pl.col("unit_price") - 1.0).abs().alias("ape"))
    nonnull = int(pred["b0_pred"].is_not_null().sum())
    med_ape = float(pred["ape"].median())
    signed_bias = float((pred["b0_pred"] / pred["unit_price"] - 1.0).median())
    disclosure = fallback_disclosure(pred)
    w = window_materials(pool, anchor)
    v = s0_m0r_variant(pool, targets).with_columns(
        (pl.col("m0r_pred") / pl.col("unit_price") - 1.0).abs().alias("ape"))
    v_med_ape = float(v["ape"].median())
    v_signed = float((v["m0r_pred"] / v["unit_price"] - 1.0).median())
    result = {
        "run_id": manifest["run_id"],
        "fold_id": fold_id,
        "anchor": anchor.isoformat(),
        "window": {"start": (anchor - timedelta(days=WINDOW_DAYS)).isoformat(),
                   "end_exclusive": anchor.isoformat(),
                   "material_rows": w.height,
                   "communities": w["community_source_id"].n_unique()},
        "targets_rows": pred.height,
        "nonnull_predictions": nonnull,
        "nonnull_ratio": nonnull / pred.height if pred.height else None,
        "fallback_disclosure": disclosure,
        "med_ape": med_ape,
        "signed_med_bias": signed_bias,
        "s0_m0_reference": {**S0_M0_REFERENCE,
                            "in_range": S0_M0_REFERENCE["low"] <= med_ape <= S0_M0_REFERENCE["high"]},
        "s0_m0r_same_construct_variant": {
            "note": "S0 脚本 m0r_predict 精确同构（漂移锚定+全历史中位回退），仅作量级归因对照，非 B0 口径",
            "med_ape": v_med_ape,
            "signed_med_bias": v_signed,
            "in_range": S0_M0_REFERENCE["low"] <= v_med_ape <= S0_M0_REFERENCE["high"],
        },
        "split_crosscheck": {
            "train_rows_expected": fold["train"]["rows"],
            "train_rows_actual": pool.height,
            "validation_rows_expected": fold["validation"]["rows"],
            "validation_rows_actual": pred.height,
            "match": pool.height == fold["train"]["rows"]
            and pred.height == fold["validation"]["rows"],
        },
    }
    ok = (result["nonnull_ratio"] == 1.0 and result["split_crosscheck"]["match"]
          and result["s0_m0_reference"]["in_range"])
    result["verdict"] = "PASS" if ok else "REVIEW"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 B0 近期小区基准")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_smoke = sub.add_parser("smoke", help="单折冒烟（默认 F01）")
    p_smoke.add_argument("run_dir")
    p_smoke.add_argument("--fold", default="F01")
    args = parser.parse_args(argv)

    if args.cmd == "smoke":
        result = smoke_fold(Path(args.run_dir).resolve(), args.fold)
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result["verdict"] == "PASS" else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
