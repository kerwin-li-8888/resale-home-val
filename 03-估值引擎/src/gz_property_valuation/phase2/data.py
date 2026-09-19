# -*- coding: utf-8 -*-
"""phase2 训练主表构建与清洗规则 v0（S0 修正口径独立重写）。

行为规格（specs/phase2-data-contracts/spec.md）：

- 一行一个成交事件；保留来源记录 ID、小区 ID、成交日期、总价、面积、原值与
  解析值、质量状态和来源血缘。
- 清洗按 S0 修正口径四条规则执行，清洗后行数与 S0 修正链 27,964 对账一致，
  不一致时构建流程报告差异并停止；排除清单逐条登记各步行数、剔除原因与裁决明细。
- 记录 ID 语义（房源/挂牌/成交事件）与判断依据随产物登记；去重仅合并确认为
  同一交易的重复记录，同一套房真实不同次成交全部保留。
- 区县过滤参数化（默认云溪区，design D1）；全市各区别行数分布画像随 run 登记。

不 import 校验证据目录下任何实验脚本（design D2：证据区只读，生产不反向依赖）。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from gz_property_valuation.phase2.lineage import (
    DEV_CUTOFF,
    LineageHalt,
    S0_DIR,
    finalize_manifest,
    load_fixed_source,
    register_artifact,
    utc_now_iso,
)

DEFAULT_DISTRICT = "云溪区"
CROSSID_AREA_TOL = 0.1
CROSSID_PRICE_TOL = 1000.0
PRICE_MIN, PRICE_MAX = 100_000.0, 20_000_000.0
AREA_MIN, AREA_MAX = 10.0, 300.0
UNIT_PRICE_MIN, UNIT_PRICE_MAX = 3_000.0, 100_000.0

MASTER_COLUMNS = [
    "source_record_id", "source_data_run_id", "source_row_number",
    "community_source_id", "community_name", "district",
    "sale_date", "sale_date_d", "sale_date_precision",
    "total_price_raw", "total_price_yuan", "total_price_status",
    "transaction_area_sqm", "area_status",
    "unit_price", "unit_price_observed", "unit_price_status",
    "building_area_detail_sqm", "building_area_status",
    "house_type", "layout_raw", "bedrooms_raw", "living_rooms_raw",
    "floor_raw", "floor_bucket", "total_floors",
    "year_built", "built_year_raw", "has_elevator", "has_elevator_raw",
    "orientation", "decoration", "decoration_norm",
    "property_use_raw", "property_use_norm",
    "extra_fields_json",
]

_EXCLUDE_FIELDS = ["source_record_id", "community_source_id", "community_name",
                   "sale_date", "total_price_yuan", "transaction_area_sqm"]


def _district_of(s: str) -> str | None:
    try:
        return json.loads(s).get("区县")
    except Exception:
        return None


def _row_dict(row: dict, rule: str, reason: str) -> dict:
    return {"rule": rule, "reason": reason,
            **{k: (str(row[k]) if row.get(k) is not None else None)
               for k in _EXCLUDE_FIELDS}}


def _crossid_dedup(t: pl.DataFrame) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    """清洗规则②：跨 ID 疑似同套（同小区+同成交日+面积差≤0.1㎡+总价差≤1000 元）。

    组内贪心配对，保留首行（已按成交日、source_record_id 排序），其余剔除；
    每组全部成员导出裁决清单（人工可查，不静默）。仅比较价格面积均非空行。
    语义与 S0 ``run_data2_curve_fixed.py`` 逐行对齐。
    """
    suspected: list[dict] = []
    removed_rows: list[dict] = []
    drop_ids: list[str] = []
    groups = (t.group_by(["community_source_id", "sale_date"]).agg(pl.len().alias("n"))
              .filter(pl.col("n") > 1))
    for comm, date in zip(groups["community_source_id"].to_list(),
                          groups["sale_date"].to_list()):
        g = t.filter((pl.col("community_source_id") == comm)
                     & (pl.col("sale_date") == date))
        rws = g.to_dicts()
        gone: set[int] = set()
        for i in range(len(rws)):
            if i in gone:
                continue
            for j in range(i + 1, len(rws)):
                if j in gone:
                    continue
                a_i, a_j = rws[i]["transaction_area_sqm"], rws[j]["transaction_area_sqm"]
                p_i, p_j = rws[i]["total_price_yuan"], rws[j]["total_price_yuan"]
                if None in (a_i, a_j, p_i, p_j):
                    continue
                if (abs(float(a_i) - float(a_j)) <= CROSSID_AREA_TOL + 1e-9
                        and abs(float(p_i) - float(p_j)) <= CROSSID_PRICE_TOL + 1e-9):
                    suspected.append({
                        "kept_source_record_id": rws[i]["source_record_id"],
                        "removed_source_record_id": rws[j]["source_record_id"],
                        "community_source_id": comm, "sale_date": date,
                        "area_sqm": [str(a_i), str(a_j)],
                        "total_price_yuan": [str(p_i), str(p_j)],
                        "all_member_ids": [r["source_record_id"] for r in rws],
                        "basis": "同小区+同成交日+面积差≤0.1㎡+总价差≤1000 元，判定为同一交易重复导出",
                    })
                    gone.add(j)
                    drop_ids.append(rws[j]["source_record_id"])
                    removed_rows.append(_row_dict(
                        rws[j], "规则②跨ID疑似同套",
                        f"与 {rws[i]['source_record_id']} 同小区同日、面积/总价差在容差内"))
    if drop_ids:
        t = t.filter(~pl.col("source_record_id").is_in(drop_ids))
    return t, suspected, removed_rows


def clean_v0(raw: pl.DataFrame, district: str = DEFAULT_DISTRICT) -> tuple[
        pl.DataFrame, dict, dict]:
    """清洗规则 v0（S0 修正口径）四条全部落地，逐步行数登记并返回排除清单。"""
    t = raw.filter(pl.col("extra_fields_json").map_elements(
        lambda s: _district_of(s) == district, return_dtype=pl.Boolean))
    n0 = t.height

    conflict = (t.group_by("source_record_id")
                .agg(pl.col("community_source_id").n_unique().alias("n_comm"))
                .filter(pl.col("n_comm") > 1))
    conflict_ids = conflict["source_record_id"].to_list()
    conflict_rows = (t.filter(pl.col("source_record_id").is_in(conflict_ids))
                     .select(_EXCLUDE_FIELDS).to_dicts() if conflict_ids else [])
    dup_mask = t["source_record_id"].is_duplicated()
    rule1_removed_rows = [
        _row_dict(r, "规则①source_record_id去重",
                  "同 ID 重复导出行（保留首行；同 ID 小区冲突组另见 dedup 裁决清单）")
        for r in t.filter(dup_mask).to_dicts()]
    t = t.unique(subset=["source_record_id"], keep="first")
    n1 = t.height

    t = t.sort(["sale_date", "source_record_id"])
    t, suspected, rule2_removed_rows = _crossid_dedup(t)
    n1b = t.height

    in_rule4 = (pl.col("sale_date").is_not_null()
                & pl.col("total_price_yuan").is_not_null()
                & pl.col("transaction_area_sqm").is_not_null()
                & (pl.col("total_price_yuan").cast(pl.Float64) > PRICE_MIN)
                & (pl.col("total_price_yuan").cast(pl.Float64) <= PRICE_MAX)
                & (pl.col("transaction_area_sqm").cast(pl.Float64) > AREA_MIN)
                & (pl.col("transaction_area_sqm").cast(pl.Float64) <= AREA_MAX))
    rule4_removed_rows = []
    for r in t.filter(~in_rule4).to_dicts():
        reasons = []
        if r["sale_date"] is None:
            reasons.append("成交日期缺失")
        if r["total_price_yuan"] is None:
            reasons.append("总价缺失")
        elif not (PRICE_MIN < float(r["total_price_yuan"]) <= PRICE_MAX):
            reasons.append(f"总价超出区间（{PRICE_MIN:g}<总价≤{PRICE_MAX:g}）")
        if r["transaction_area_sqm"] is None:
            reasons.append("面积缺失")
        elif not (AREA_MIN < float(r["transaction_area_sqm"]) <= AREA_MAX):
            reasons.append(f"面积超出区间（{AREA_MIN:g}<面积≤{AREA_MAX:g}）")
        rule4_removed_rows.append(_row_dict(r, "规则④价格/面积区间过滤", "；".join(reasons)))
    t = t.filter(in_rule4)
    n2 = t.height

    t = t.with_columns((pl.col("total_price_yuan").cast(pl.Float64)
                        / pl.col("transaction_area_sqm").cast(pl.Float64))
                       .alias("unit_price"))
    in_up = ((pl.col("unit_price") >= UNIT_PRICE_MIN)
             & (pl.col("unit_price") <= UNIT_PRICE_MAX))
    up_removed_rows = []
    for r in t.filter(~in_up).to_dicts():
        up_removed_rows.append(_row_dict(
            r, "单价区间过滤",
            f"单价超出区间（{UNIT_PRICE_MIN:g}≤单价≤{UNIT_PRICE_MAX:g}，"
            f"实测单价 {r['unit_price']:.0f}）"))
    t = t.filter(in_up)

    steps = {
        "raw_district": n0,
        "rule1_source_record_dedup": n1,
        "rule1_community_conflict_groups": len(conflict_ids),
        "rule2_crossid_same_unit_removed_rows": n1 - n1b,
        "after_rule2": n1b,
        "rule4_price_area_filter": n2,
        "after_unitprice_filter": t.height,
        "crossid_rule": {"area_tol_sqm": CROSSID_AREA_TOL,
                         "price_tol_yuan": CROSSID_PRICE_TOL},
    }
    exclusions = {
        "schema_version": "phase2-exclusions-v1",
        "district_filter": {"column_source": "extra_fields_json.区县", "value": district},
        "dev_cutoff": DEV_CUTOFF,
        "steps": [
            {"step": "raw_district", "description": f"区县过滤（{district}）后原始行数",
             "rows": n0},
            {"step": "rule1_source_record_dedup", "description": "规则①去重后行数",
             "rows": n1, "removed": n0 - n1},
            {"step": "rule2_crossid_same_unit", "description": "规则②疑似同套剔除后行数",
             "rows": n1b, "removed": n1 - n1b},
            {"step": "rule4_price_area_filter", "description": "规则④价格/面积过滤后行数",
             "rows": n2, "removed": n1b - n2},
            {"step": "unitprice_filter", "description": "单价区间过滤后行数",
             "rows": t.height, "removed": n2 - t.height},
        ],
        "rule1_removed": rule1_removed_rows,
        "rule2_removed": rule2_removed_rows,
        "rule4_removed": rule4_removed_rows,
        "unitprice_removed": up_removed_rows,
    }
    dedup = {
        "schema_version": "phase2-dedup-v1",
        "conflict_groups": len(conflict_ids),
        "conflict_rows": conflict_rows,
        "suspected_same_unit": suspected,
        "adjudication_principle": "疑似同套只删同小区同日、面积/总价差在容差内的重复导出；"
                                  "不因属性相近删除不同日成交；同一套房真实不同次成交按时间顺序全部保留",
    }
    return t, exclusions, dedup


def _id_semantics_evidence(t_after_rule1: pl.DataFrame) -> dict:
    """记录 ID 语义的实测判断依据（任务 2.2 随产物登记）。"""
    g = t_after_rule1.group_by("source_record_id").agg(
        pl.col("sale_date").n_unique().alias("n_dates"), pl.len().alias("n"))
    ids_multi_date = int(g.filter(pl.col("n_dates") > 1).height)
    dup_ids = int(g.filter(pl.col("n") > 1).height)
    rep = t_after_rule1.filter(pl.col("transaction_area_sqm").is_not_null()).with_columns(
        pl.col("transaction_area_sqm").cast(pl.Float64).round(1).alias("a"))
    r2 = rep.group_by(["community_source_id", "a"]).agg(
        pl.len().alias("n"), pl.col("sale_date").n_unique().alias("n_dates"))
    repeat_sale_groups = int(r2.filter(pl.col("n_dates") > 1).height)
    return {
        "source_record_id_semantics": "成交记录级导出 ID（每条导出记录唯一），非房源/挂牌持续标识",
        "judgment_basis": [
            f"同 source_record_id 跨多个成交日期的组数为 {ids_multi_date}（若为房源/挂牌 ID 复用应显著大于 0）",
            f"同 source_record_id 重复导出组 {dup_ids} 组，字段全一致（S0 校正说明 §2：导出层重复）",
            f"同小区+同面积（0.1㎡）不同日期成交组 {repeat_sale_groups} 组——同一套房真实不同次成交，"
            "按时间顺序全部保留，规则②只裁同日近重复",
        ],
        "measured": {"ids_with_multiple_dates": ids_multi_date,
                     "duplicate_id_groups": dup_ids,
                     "community_area_multidate_groups": repeat_sale_groups},
        "dedup_boundary": "仅规则①（同 ID 重复导出）与规则②（同小区+同日+面积≤0.1㎡+总价≤1000 元）"
                          "合并同一交易；不同日成交一律保留",
    }


def reconcile_s0(steps: dict) -> dict:
    anchor_path = S0_DIR / "data2_curve_fixed.json"
    anchor = json.loads(anchor_path.read_text(encoding="utf-8"))["cleaning_steps"]
    keys = ["raw_district", "rule1_source_record_dedup",
            "rule1_community_conflict_groups", "rule2_crossid_same_unit_removed_rows",
            "after_rule2", "rule4_price_area_filter", "after_unitprice_filter"]
    detail = {k: {"ours": steps.get(k), "s0": anchor.get(k),
                  "match": steps.get(k) == anchor.get(k)} for k in keys}
    ok = all(v["match"] for v in detail.values())
    if not ok:
        raise LineageHalt(
            "停止条件：清洗落地与 S0 修正链对账不平：" + json.dumps(detail, ensure_ascii=False))
    return {"anchor_file": "examples/phase2_demo/s0_evidence/data2_curve_fixed.json",
            "anchor_final_rows": anchor["after_unitprice_filter"],
            "steps_detail": detail, "match": True}


def build_master(run_dir: Path, district: str = DEFAULT_DISTRICT) -> pl.DataFrame:
    """从 manifest 固定来源构建主表并落盘（master/exclusions/dedup + manifest 登记）。"""
    src = load_fixed_source(run_dir)
    raw = pl.read_parquet(src["path"])
    cleaned, exclusions, dedup = clean_v0(raw, district)
    recon = reconcile_s0({
        "raw_district": exclusions["steps"][0]["rows"],
        "rule1_source_record_dedup": exclusions["steps"][1]["rows"],
        "rule1_community_conflict_groups": dedup["conflict_groups"],
        "rule2_crossid_same_unit_removed_rows": exclusions["steps"][2]["removed"],
        "after_rule2": exclusions["steps"][2]["rows"],
        "rule4_price_area_filter": exclusions["steps"][3]["rows"],
        "after_unitprice_filter": exclusions["steps"][4]["rows"],
    })
    raw_district = raw.filter(pl.col("extra_fields_json").map_elements(
        lambda s: _district_of(s) == district, return_dtype=pl.Boolean))
    id_sem = _id_semantics_evidence(raw_district)
    dedup["id_semantics"] = id_sem

    master = cleaned.with_columns(
        pl.lit(src["data_run_id"]).alias("source_data_run_id"),
        pl.col("row_number").alias("source_row_number"),
        pl.col("total_price_yuan").cast(pl.Float64).alias("total_price_yuan"),
        pl.col("transaction_area_sqm").cast(pl.Float64).alias("transaction_area_sqm"),
        pl.col("unit_price_observed").cast(pl.Float64).alias("unit_price_observed"),
        pl.col("building_area_detail_sqm").cast(pl.Float64).alias("building_area_detail_sqm"),
        pl.col("sale_date").str.to_date("%Y-%m-%d").alias("sale_date_d"),
        pl.lit(district).alias("district"),
    ).select(MASTER_COLUMNS).sort("source_record_id")

    master.write_parquet(run_dir / "master_table.parquet")
    (run_dir / "exclusions.json").write_text(
        json.dumps(exclusions, ensure_ascii=False, indent=1), encoding="utf-8")
    (run_dir / "dedup.json").write_text(
        json.dumps(dedup, ensure_ascii=False, indent=1), encoding="utf-8")

    finalize_manifest(run_dir, {"data_contract": {
        "cleaning": {
            "rules_version": "v0-s0-修正口径",
            "rules": [
                "① source_record_id 去重（同 ID 小区冲突组先导出裁决清单）",
                "② 跨 ID 疑似同套：同小区+同成交日+面积差≤0.1㎡+总价差≤1000 元，组内贪心保留首行",
                "③ 小区 ID 冲突组人工裁决清单（并入①导出）",
                "④ 价格/面积区间过滤（10 万<总价≤2000 万、10<面积≤300、3000≤单价≤100000）",
            ],
            "s0_reconciliation": recon,
        },
        "id_semantics": id_sem,
        "built_at_utc": utc_now_iso(),
    }})
    register_artifact(run_dir, "master_table.parquet", rows=master.height,
                      extra={"grain": "一行一个成交事件", "district_filter": district})
    register_artifact(run_dir, "exclusions.json")
    register_artifact(run_dir, "dedup.json")
    return master


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 主表构建")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_build = sub.add_parser("build", help="构建主表并对账 S0（参数：run 目录）")
    p_build.add_argument("run_dir")
    p_build.add_argument("--district", default=DEFAULT_DISTRICT)
    args = parser.parse_args(argv)

    if args.cmd == "build":
        run_dir = Path(args.run_dir).resolve()
        master = build_master(run_dir, args.district)
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        print(json.dumps({
            "run_dir": str(run_dir),
            "master_rows": master.height,
            "master_columns": master.width,
            "cleaning_steps": manifest["data_contract"]["cleaning"]["s0_reconciliation"]["steps_detail"],
            "s0_reconciliation_match": manifest["data_contract"]["cleaning"]["s0_reconciliation"]["match"],
            "id_semantics": manifest["data_contract"]["id_semantics"]["measured"],
            "verdict": "PASS",
        }, ensure_ascii=False, indent=1))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
