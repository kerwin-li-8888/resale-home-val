# -*- coding: utf-8 -*-
"""phase2 时间切分、用途切片、独立测试接收规则与开发验证合同渲染。

行为规格（specs/phase2-data-contracts/spec.md「时间切分与用途切片」「最终测试
接收规则预注册」等）：

- 登记 A/B 窗为已观察开发证据（observed_by: DATA2 / S0-correction），禁止改称
  独立测试；
- expanding 滚动折（design D4）：锚点间隔 6 个月、验证带 3 个月、首折最小训练
  样本量门槛；折边界由数据实际分布计算；
- 最近验证带整体保留为预留带，按时间先后二分为调参切片与区间校准切片；切片与
  各折验证集互不重叠（检查项）；更早验证带只作滚动评估；
- 最终测试接收规则预注册：只接收 2026-07-20 之后且此前未用于分析的新成交，
  不得依据表现挑选（承接数据门条件 3；本 run 该窗为空，冻结的是规则）；
- 每折的预处理、时间指数、市场聚合与类别编码仅在该折训练材料中拟合；
- 开发验证合同 contract.md 九项内容随 run 落盘（人读权威），checks 做章节断言。
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from gz_property_valuation.phase2.lineage import DEV_CUTOFF, register_artifact

ANCHOR_STEP_MONTHS = 6
VALIDATION_MONTHS = 3
MIN_FIRST_TRAIN_ROWS = 2000
FINAL_TEST_WINDOW_START = "2026-07-21"
OBSERVED_BY = ["DATA2", "S0-correction"]
OBSERVED_WINDOWS = {
    "A_2026H1": {"start": "2026-01-01", "end": "2026-07-20",
                 "note": "代号含 H1 但实际包含 7 月，不按上半年重新解释（蓝图 §4.3.1）"},
    "B_2025H2": {"start": "2025-07-21", "end": "2025-12-31"},
}

_FIT_BOUNDARY_RULE = ("每折的预处理、时间指数、市场聚合与类别编码仅在该折训练材料中拟合"
                      "（spec：折内拟合边界）；评估窗预测使用窗级截点市场特征"
                      "（features.community_market_snapshot，蓝图 §4.3.3）")

_SLICE_BISECTION_RULE = ("最近验证带（预留带）按成交时间排序后按行数二分：前半为调参切片、"
                         "后半为区间校准切片；奇数行归前半；切片与各折验证集互不重叠")


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    day = min(d.day, [31, 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28,
                      31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date(y, m, day)


def _event_ids(t: pl.DataFrame) -> set[tuple[str, str]]:
    return set(zip(t["source_record_id"].to_list(), t["sale_date"].to_list()))


def _seg_stats(t: pl.DataFrame) -> dict:
    return {"rows": t.height,
            "communities": t["community_source_id"].n_unique(),
            "date_min": str(t["sale_date_d"].min()) if t.height else None,
            "date_max": str(t["sale_date_d"].max()) if t.height else None}


def build_splits(run_dir: Path) -> dict:
    master = pl.read_parquet(run_dir / "master_table.parquet")
    cutoff = master["sale_date_d"].max()
    if str(cutoff) != DEV_CUTOFF:
        raise SystemExit(f"主表最大成交日 {cutoff} != 开发截点 {DEV_CUTOFF}，拒绝生成切分")

    reserved_start = _add_months(cutoff, -VALIDATION_MONTHS) + _one_day()
    reserved = master.filter((pl.col("sale_date_d") >= reserved_start)
                             & (pl.col("sale_date_d") <= cutoff)).sort(
        ["sale_date_d", "source_record_id"])
    half = (reserved.height + 1) // 2
    tuning = reserved.head(half)
    calibration = reserved.tail(reserved.height - half)
    boundary = (str(tuning["sale_date_d"].max()), str(calibration["sale_date_d"].min())) \
        if half and reserved.height - half else None

    anchor = reserved_start
    folds = []
    skipped_early = 0
    while True:
        prev_anchor = _add_months(anchor, -ANCHOR_STEP_MONTHS)
        train = master.filter(pl.col("sale_date_d") < prev_anchor)
        if train.height < MIN_FIRST_TRAIN_ROWS:
            skipped_early += 1
            break
        val_start, val_end = prev_anchor, _add_months(prev_anchor, VALIDATION_MONTHS) - _one_day()
        valid = master.filter((pl.col("sale_date_d") >= val_start)
                              & (pl.col("sale_date_d") <= val_end))
        folds.append({
            "fold_id": f"F{len(folds) + 1:02d}",
            "anchor": str(prev_anchor),
            "train": {**_seg_stats(train), "end_exclusive": str(prev_anchor),
                      "rule": "expanding：锚点前全部主表行"},
            "validation": {**_seg_stats(valid), "start": str(val_start),
                           "end_inclusive": str(val_end),
                           "rule": f"锚点后 {VALIDATION_MONTHS} 个月验证带"},
        })
        anchor = prev_anchor

    for i, fold in enumerate(folds):
        fold["validation"]["overlap_with_slices_events"] = len(
            _event_ids(master.filter(
                (pl.col("sale_date_d") >= date.fromisoformat(fold["validation"]["start"]))
                & (pl.col("sale_date_d") <= date.fromisoformat(fold["validation"]["end_inclusive"]))))
            & _event_ids_reserved(reserved))

    overlap = sum(f["validation"]["overlap_with_slices_events"] for f in folds)
    if overlap:
        raise SystemExit(f"用途切片与折验证集重叠 {overlap} 个事件，切分合同失败")

    final_test = master.filter(pl.col("sale_date") >= FINAL_TEST_WINDOW_START)
    splits = {
        "schema_version": "phase2-splits-v1",
        "run_id": run_dir.name,
        "dev_cutoff": str(cutoff),
        "params": {"anchor_step_months": ANCHOR_STEP_MONTHS,
                   "validation_months": VALIDATION_MONTHS,
                   "min_first_train_rows": MIN_FIRST_TRAIN_ROWS},
        "reserved_band": {"start": str(reserved_start), "end": str(cutoff),
                          "rows": reserved.height,
                          "purpose": "最近验证带整体保留，二分为调参/校准切片；不作为折验证集"},
        "folds": folds,
        "folds_note": f"expanding 折共 {len(folds)} 个（锚点间隔 {ANCHOR_STEP_MONTHS} 个月）；"
                      f"更早锚点因训练池 < {MIN_FIRST_TRAIN_ROWS} 行停止生成（停止于 {skipped_early} 个候选锚点）；"
                      "更早验证带只作滚动评估",
        "slices": {
            "tuning": {**_seg_stats(tuning), "rule": "调参切片（预留带前半）"},
            "calibration": {**_seg_stats(calibration), "rule": "区间校准切片（预留带后半）"},
            "bisection_rule": _SLICE_BISECTION_RULE,
            "boundary_dates": {"tuning_last": boundary[0] if boundary else None,
                               "calibration_first": boundary[1] if boundary else None},
            "overlap_with_fold_validation_events": overlap,
        },
        "observed_windows": {
            name: {**w, "observed_by": OBSERVED_BY,
                   "status": "已观察开发证据，不得改称独立测试（内部用途重新划分不等于"
                             "获得新的独立测试集，蓝图 §4.3.2）",
                   "rows": master.filter((pl.col("sale_date") >= w["start"])
                                         & (pl.col("sale_date") <= w["end"])).height}
            for name, w in OBSERVED_WINDOWS.items()},
        "final_test_rule": {
            "window_start": FINAL_TEST_WINDOW_START,
            "eligibility": [
                "来源记录 ID 不在本轮主表（本轮 27,964 行）中",
                "事件时间（成交日期）在接收窗口内",
                "通过与本轮相同的清洗资格规则（规则①–④，不入开发对账口径）",
            ],
            "registration": "按批次登记到达时间与数据指纹；接收行数在接收时登记"
                            "（本 run 该窗为空，冻结的是规则不是行数；承接数据门条件 3）",
            "no_cherry_pick": "不得依据候选表现挑选样本；曾进入开发分析的材料不得重新"
                              "命名为独立测试，接收检查拒绝并登记原因",
            "current_rows_in_window": final_test.height,
        },
        "fit_boundary_rule": _FIT_BOUNDARY_RULE,
        "next_batch_reconciliation": "见 manifest.update_policy.next_batch（新增/重复/修订/"
                                     "取得时间四项核对；旧快照保留；更新未到登记未验证）",
        "event_key": "(source_record_id, sale_date)",
    }
    (run_dir / "splits.json").write_text(
        json.dumps(splits, ensure_ascii=False, indent=1), encoding="utf-8")
    register_artifact(run_dir, "splits.json",
                      extra={"folds": len(folds),
                             "tuning_rows": tuning.height,
                             "calibration_rows": calibration.height,
                             "final_test_rows_now": final_test.height})
    return splits


def _one_day() -> timedelta:
    return timedelta(days=1)


def _event_ids_reserved(reserved: pl.DataFrame) -> set[tuple[str, str]]:
    return _event_ids(reserved)


# ---------- 开发验证合同（任务 4.2，九项内容） ----------

def render_contract(run_dir: Path) -> str:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    splits = json.loads((run_dir / "splits.json").read_text(encoding="utf-8"))
    id_path = run_dir / "identity_map.json"
    id_map = (json.loads(id_path.read_text(encoding="utf-8"))
              if id_path.exists() else None)
    src = manifest["source"]
    steps = splits["params"]
    if id_map:
        st = id_map["stats"]
        identity_block = f"""### 9.1 共同目标实测冻结（本 run）

- 可匹配子集（B1 实际系统比较共同目标）：{st['communities']['自动匹配']} 小区 /
  {st['master_rows_by_status']['自动匹配']} 行成交（占比
  {st['master_rows_by_status']['自动匹配'] / st['master_rows_total']:.1%}）。
- 未匹配 {st['communities']['未匹配']} 小区 / {st['master_rows_by_status']['未匹配']} 行：
  主因为范围边界差异——现行引擎实体表（239 实体）源自目标区西部板块候选名录，
  链家研究样本覆盖全区（蓝图 §2.1.3）；属范围差异而非名称匹配质量问题。
  待确认+未匹配 48.5% 曾触发 15% 停线阈值，经用户裁决（2026-09-11）按上述口径
  冻结子集继续，扩容事项登记 backlog【实体扩容全区】（裁决记录见
  identity_map.json strategy_decision）。
- 算法比较轨道（B0/M1–M4 互比）使用全主表，不受映射子集限制。

"""
    else:
        identity_block = ""
    text = f"""# 开发验证合同（S1 数据合同，run {run_dir.name}）

> 性质：本合同为第二阶段 S2 起所有模型比较的共同前提（蓝图 §2.2：开发验证合同在 S1 冻结）。
> 机器可读配套：manifest.json / splits.json / feature_dictionary.json / exclusions.json；
> 可机导出部分由代码生成并与本文件一致，checks 做九项章节存在性断言。

## 1. 预测目标

- 首轮固定对数单价目标：`log(成交单价)`，成交单价 = 成交总价 / 交易面积（主表 unit_price）；
  总价目标经同一真实面积换算时百分比误差一致（蓝图 §11）。
- 成交总价（total_price_yuan）与直接推导单价（unit_price）仅作标签；目标成交后信息、
  未来回填属性、挂牌价不进入历史预测输入。
- 输出要求（S4 冻结）：点估值 + 区间 + 可信度/适用状态；本合同只冻结输入与评估前提。

## 2. 适用范围

- 研究样本边界：示例城市目标区普通住宅（extra_fields_json『区县』= 云溪区；
  区县过滤参数化，跨区扩样按蓝图 §15 另立实验并单独验证）。
- 来源：链家外部成交导出（staged/lianjia_ext），普通住宅口径（非住宅已在源头分表排除）。
- 一行一个成交事件；同 source_record_id 重复导出合并；同一套房真实不同次成交全部保留。

## 3. 原始快照

- 固定数据 run：`{src["data_run_id"]}`（构建时解析一次 current.json 并固定，重建不再读指针）。
- 源文件：`{src["file"]["path"]}`；sha256 `{src["file"]["sha256"]}`；
  与 S0 fingerprints.json 交叉核对一致（{src["cross_check_s0"]["match"]}）。
- 开发信息截点：{splits["dev_cutoff"]}；staged 目录扫描确认无此后成交数据进入本 run。

## 4. 清洗规则（v0，S0 修正口径）

四条规则与对账（详见 exclusions.json / dedup.json）：

1. ① source_record_id 去重（同 ID 小区冲突组先导出裁决清单，目标区实测 0 组）；
2. ② 跨 ID 疑似同套：同小区+同成交日+面积差≤0.1㎡+总价差≤1000 元，组内贪心保留首行；
3. ③ 小区 ID 冲突组人工裁决清单（并入①导出）；
4. ④ 价格/面积区间过滤（10 万<总价≤2000 万、10<面积≤300、3000≤单价≤100000）。

对账锚点：清洗后行数与 S0 修正链一致（28,249→28,063→28,060→27,976→27,964）；
裁决原则：疑似同套只删同一交易重复导出，不因属性相近删除不同日成交。

## 5. 时间切分

- A 窗 2026-01-01～2026-07-20、B 窗 2025-07-21～2025-12-31 为已观察开发证据
  （observed_by: DATA2, S0-correction），不得改称独立测试。
- expanding 滚动折 {len(splits["folds"])} 个：锚点间隔 {steps["anchor_step_months"]} 个月、
  验证带 {steps["validation_months"]} 个月、首折最小训练样本量 {steps["min_first_train_rows"]} 行；
  每折登记训练池行数/时间跨度/小区数（splits.json）。
- 预留带（{splits["reserved_band"]["start"]} ~ {splits["reserved_band"]["end"]}）整体保留，
  按时间先后二分：前半调参切片、后半区间校准切片；与各折验证集互不重叠（重叠 0）。
- 固定信息截点规则：训练行市场特征仅用严格早于该行、排除自身与同日的证据
  （365 天窗，上界开区间）；评估窗预测使用窗级截点（窗口内成交价不进入该窗特征）。
- 最终测试接收规则（预注册）：只接收 {splits["final_test_rule"]["window_start"]} 之后、
  此前未用于分析的新成交；资格与登记条款见 splits.json；不得依据表现挑选。
- 成交日期与信息可用日期分开登记；历史数据缺首次披露时间，属回顾性检验，限制继续披露；
  装修、电梯等可变化属性以纳入为基准并登记纳入/排除敏感性实验安排。

## 6. 模型角色（对照框架）

双轨比较（蓝图 §4.4），报告中分开显示：

- **算法比较**（同外源数据、同目标、同信息边界）：B0 近期小区基准（截点前 365 天同小区
  单价中位，统一截点锚定，回退比例披露）；M1 可解释模型（正则回归+有限平滑项/交互候选，
  小区价位保留、样本少向更大范围收缩）；M2 CatBoost 直接报价；M3 局部市场水平+价差模型
  （基准不含自身与未来信息）；M4 LightGBM 挑战（开发折内调参，有界搜索清单）。
- **实际系统比较**：B1 现行引擎冻结版本（实际数据源），同一目标、同一截点；
  数据覆盖不同导致的收益不归因于算法。
- A1 辅助比较法修正项按蓝图 §6.1 单独比较，实验分支保留原案例与拒绝原因。
- M1 不能赢过 B0 不自动结束第二阶段，先检查位置、时间处理与支持范围。

## 7. 指标定义（沿用蓝图 §11 口径）

令真实总价 P、预测 P_hat；单价预测按同一真实面积换算时百分比误差一致：

| 指标 | 计算/分母 | 用途 |
|---|---|---|
| APE | abs(P_hat / P − 1) | 每套房百分比误差 |
| MedAPE | APE 中位数 | 典型误差；不可与平均 MAPE 混用 |
| ±10% 命中 | APE ≤ 0.10 的比例 | 点估值表现 |
| P90 APE | APE 第 90 百分位 | 尾部错误 |
| 有符号偏差 | P_hat / P − 1 的中位数 | 是否持续高估/低估 |
| 严重高估率 | 有符号误差超阈值比例（开发期先用 20% 描述，正式值 S4 冻结） | 买方风险 |
| 可估覆盖 | 有可用价格中心的目标数 / 全部合格目标数 | 是否靠拒绝困难房源改善指标 |
| 区间覆盖 | 真实价格落在上下界内比例（明确分母，报告无区间数量） | 区间可靠程度 |
| 相对全宽 | (上界−下界)/预测中心 | 覆盖的代价 |

每份比较报告同时包含：时间及截点、模型/数据版本、样本进出明细、共同目标、全体覆盖、
总体指标、分组指标、差异不确定性、失败案例、用途结论与下一步；分组未达最低证据量时
列出数量与结果，不强作稳定性判断。

## 8. 数据泄漏检查项

可重复运行（checks.py，退出码语义 0=通过 / 1=失败并打印违规明细）：

1. 泄漏检查：特征输出标签衍生禁入断言；市场特征聚合上界抽样暴力重算
   （严格早于、排除自身、排除同日）；合成违规输入自检（防检查器失明）。
2. 事件隔离检查：训练材料 / 调参切片 / 校准切片 / 最终测试预留按事件键
   (source_record_id, sale_date) 两两不交；切片与各折验证集不共享事件。
3. 重建一致性检查：同 manifest 固定来源与代码重建主表、切分与特征，逐行哈希比对一致。

配套：特征字典七项元信息非空断言；合同九项章节存在性断言；重建一致性锚点为
manifest 产物清单哈希。

## 9. 比较人群与试验预算（占位）

- 共同目标：主表内通过清洗资格、且身份映射可匹配现行引擎实体的成交事件
  （identity_map.json；映射只服务评估与请求边界，不改外部训练 ID）。
{identity_block}- 引擎无法按合同重建的历史输入披露为缺失，不编造对应估值。
- 试验预算：S3 有界实验顺序与成本经小样本测量后写入对应提案（蓝图 §6.1）；
  本合同仅占位，不预设具体预算数。
- 正式验收数值门槛在 S4 冻结（最终验收合同），不得查看独立测试表现后再改。
"""
    (run_dir / "contract.md").write_text(text, encoding="utf-8")
    register_artifact(run_dir, "contract.md")
    return text


CONTRACT_SECTIONS = ["预测目标", "适用范围", "原始快照", "清洗规则", "时间切分",
                     "模型角色", "指标定义", "数据泄漏检查项", "比较人群与试验预算"]


def assert_contract_sections(run_dir: Path) -> dict:
    text = (run_dir / "contract.md").read_text(encoding="utf-8")
    missing = [s for s in CONTRACT_SECTIONS
               if f"## " in text and not any(
                   line.startswith("## ") and s in line for line in text.splitlines())]
    return {"sections": CONTRACT_SECTIONS, "missing": missing,
            "verdict": "PASS" if not missing else "FAIL"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 切分与合同")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, help_ in (("build", "生成分割清单 splits.json"),
                        ("contract", "渲染开发验证合同 contract.md"),
                        ("assert-contract", "九项章节存在性断言")):
        p = sub.add_parser(name, help=help_)
        p.add_argument("run_dir")
    args = parser.parse_args(argv)
    run_dir = Path(args.run_dir).resolve()

    if args.cmd == "build":
        splits = build_splits(run_dir)
        print(json.dumps({
            "run_dir": str(run_dir),
            "folds": len(splits["folds"]),
            "fold_rows": [{"fold": f["fold_id"], "anchor": f["anchor"],
                           "train_rows": f["train"]["rows"],
                           "train_communities": f["train"]["communities"],
                           "validation_rows": f["validation"]["rows"]}
                          for f in splits["folds"]],
            "reserved_band": splits["reserved_band"],
            "slices": {k: splits["slices"][k] for k in ("tuning", "calibration")},
            "overlap_with_fold_validation_events":
                splits["slices"]["overlap_with_fold_validation_events"],
            "final_test_rows_now": splits["final_test_rule"]["current_rows_in_window"],
            "observed_windows": {k: v["rows"] for k, v in splits["observed_windows"].items()},
            "verdict": "PASS",
        }, ensure_ascii=False, indent=1))
        return 0

    if args.cmd == "contract":
        render_contract(run_dir)
        result = assert_contract_sections(run_dir)
        print(json.dumps({"run_dir": str(run_dir), **result}, ensure_ascii=False))
        return 0 if result["verdict"] == "PASS" else 1

    if args.cmd == "assert-contract":
        result = assert_contract_sections(run_dir)
        print(json.dumps({"run_dir": str(run_dir), **result}, ensure_ascii=False))
        return 0 if result["verdict"] == "PASS" else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
