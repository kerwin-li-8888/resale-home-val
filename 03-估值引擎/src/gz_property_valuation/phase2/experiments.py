# -*- coding: utf-8 -*-
"""phase2 S3 试验注册表：任务清单生成与冻结（compare-phase2-nonlinear 任务 1.2）。

行为规格（change `compare-phase2-nonlinear` 的 design D4/D5/D7 与 specs
`phase2-nonlinear-comparisons`「试验注册表与统一报告」「重计算云执行与复现性」）：

- 594 个正式任务单元从登记的网格、折数与拟合流程机械生成、机械复算：
  派生准备 18 + M2 (8+1)×18=162 + M3a (8+1)×18=162 + M3b (2+1)×18=54
  + 属性增量 2×18=36（以上 CatBoost 共 414）+ M4 (8+1)×18=162 = 594；
- 网格、特征组、外层重拟合流程、种子 20260912、thread=1 全部在注册表内钉死登记，
  运行中不得追加配置（spec「网格先固定后运行」）；
- 质量验证运行（轻门参考生成、云侧金丝雀/轻门对拍、同机重跑抽样、跨机抽查）
  与 594 正式单元单列，不计入正式单元、不视为扩网格（spec「质量验证运行单列」）；
- 预算对账数值化：数量 × 单位耗时（注明来源：实测/外推），可机械复算。

本模块只做清单生成与预算算术，不含任何数值库依赖（可在 numpy/polars 之前导入；
导入本模块会照常触发 :mod:`gz_property_valuation.phase2` 的线程钉死检查）。
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

SCHEMA_VERSION = "phase2-s3-experiments-v1"
CHANGE = "compare-phase2-nonlinear"
SEED = 20260912
THREAD = 1

FOLDS = [f"F{i:02d}" for i in range(1, 19)]

# ---- 网格（design D4：运行前固定、有界，运行中不得追加） ----

CATBOOST_COMMON = {
    "learning_rate": 0.05,
    "seed": SEED,
    "thread_count": THREAD,
    "allow_writing_files": False,
}

M2_GRID = [
    {"iterations": it, "depth": dp, "l2_leaf_reg": l2, **CATBOOST_COMMON}
    for it in (300, 500) for dp in (4, 6) for l2 in (1, 5)
]

M3A_GRID = [dict(cfg) for cfg in M2_GRID]

M3B_GRID = [
    {"iterations": 500, "depth": 6, "l2_leaf_reg": l2, **CATBOOST_COMMON}
    for l2 in (1, 5)
]

M4_GRID = [
    {"num_leaves": nl, "min_data_in_leaf": mdl, "feature_fraction": ff,
     "learning_rate": 0.05, "n_estimators": 500, "deterministic": True,
     "force_row_wise": True, "num_threads": THREAD, "seed": SEED}
    for nl in (31, 63) for mdl in (10, 20) for ff in (0.8, 1.0)
]

ENGINES = {
    "catboost": {"package": "catboost", "version": "1.2.10"},
    "lightgbm": {"package": "lightgbm", "version": "4.7.0"},
}

FEATURE_GROUPS = {
    "full": {
        "id": "full",
        "content": "S1 冻结静态属性（位置/产品/建筑/房屋）+ D2 派生市场特征 v2"
                   "（链上中位单价 log、样本数、距最近成交天数、证据层级）",
        "used_by": ["M2", "M3b", "M4"],
    },
    "base": {
        "id": "base",
        "content": "全量组去掉房屋属性组（楼层段、电梯、房龄、装修、朝向、梯户比及其交互）",
        "used_by": ["INC_M2", "INC_M3a"],
    },
}

REFIT_FLOW = {
    "selection_metric": "MedAPE（对单价百分比误差的中位数）",
    "inner_select": "对每折每个配置，在 inner_train 上拟合（类别码表仅由该层训练材料生成），"
                    "以 inner_val（内层截点锚定输入）上的 MedAPE 选优；平局取登记顺序靠前者",
    "outer_refit": "选中配置在该折完整训练切片上、以外层输入（行级严格历史滚动）重新拟合一次",
    "predict": "以重拟合模型预测 outer_val（折验证带，外层截点锚定输入）",
    "no_cross_fold": "任何选择路径只消费本折材料；全折汇总表现仅作事后汇总，不进入选择",
    "category_encoding": "折内拟合，禁止先对全量编码再切分（spec「类别处理折内拟合」）",
    "m3b_standardization": "M3b 属性参照按层独立前推拟合（内层仅 inner_train、外层完整训练切片），"
                           "两层不复用；预热行保留训练用未标准化基准",
    "m4_structure": "M4 结构预固定（与 M2 相同直接报价结构 + 全量特征组），不跨折选型",
}

TARGET = "log(unit_price)，unit_price = 成交总价 / 交易面积"

SOURCE = "proposal.md「本机计算量估算」+ evidence/0-1-timing-probe/probe-stdout.txt（实测）"

# 单位耗时（秒），来源：实测 / 外推；低—高区间
UNIT_SECONDS = {
    "catboost_fit": {"low": 55.3, "high": 56.4, "source": "实测 28k 行单线程代表配置 55.3—56.4s"
                                                       "（probe-stdout.txt）；早期折切片更小属保守上界"},
    "lightgbm_fit": {"low": 2.3, "high": 2.5, "source": "实测 28k 行单线程 2.3—2.5s（probe-stdout.txt）"},
    "derived_prep": {"low": 120.0, "high": 240.0, "source": "外推 2—4 分钟/折（S2 全折评估分钟级先例"
                                                            " + Ridge 闭式解秒级，双层翻倍）"},
}

# 云侧并行度与折算（design D7 / proposal 计算式）
CLOUD_VCPU = 16
LOCAL_WORKERS_FOR_PARALLEL = 12


def _grid_configs(model: str) -> list[dict]:
    return {"M2": M2_GRID, "M3a": M3A_GRID, "M3b": M3B_GRID, "M4": M4_GRID}[model]


def _cfg_id(model: str, idx: int) -> str:
    return f"{model}_c{idx:02d}"


def build_units() -> list[dict]:
    """机械生成 594 个正式任务单元（顺序固定、可复算）。"""
    units: list[dict] = []

    for fold in FOLDS:
        units.append({
            "unit_id": f"prep:{fold}",
            "kind": "derived_prep",
            "model": None,
            "engine": None,
            "fold_id": fold,
            "stage": "derive",
            "config_id": None,
            "config": None,
            "feature_group": "full",
            "detail": "D2 内外双层四机制派生市场特征 + D3 两层 M3 基准与 M3b 前推 Ridge；"
                      "含静态块边界/行数/预热预期占比演示输出",
            "unit_seconds": [UNIT_SECONDS["derived_prep"]["low"],
                             UNIT_SECONDS["derived_prep"]["high"]],
        })

    grid_units = (
        ("M2", "catboost", M2_GRID),
        ("M3a", "catboost", M3A_GRID),
        ("M3b", "catboost", M3B_GRID),
        ("M4", "lightgbm", M4_GRID),
    )
    for model, engine, grid in grid_units:
        sec = UNIT_SECONDS["catboost_fit" if engine == "catboost" else "lightgbm_fit"]
        for fold in FOLDS:
            for i, cfg in enumerate(grid, start=1):
                units.append({
                    "unit_id": f"{model}:{fold}:select:{_cfg_id(model, i)}",
                    "kind": "model_fit",
                    "model": model,
                    "engine": engine,
                    "fold_id": fold,
                    "stage": "inner_select",
                    "config_id": _cfg_id(model, i),
                    "config": cfg,
                    "feature_group": "full",
                    "detail": "inner_train 拟合 + inner_val（内层截点）评估，参与本折配置选优",
                    "unit_seconds": [sec["low"], sec["high"]],
                })
            units.append({
                "unit_id": f"{model}:{fold}:outer_refit",
                "kind": "model_fit",
                "model": model,
                "engine": engine,
                "fold_id": fold,
                "stage": "outer_refit",
                "config_id": "selected@inner",
                "config": None,
                "feature_group": "full",
                "detail": "选中配置在完整训练切片以外层输入重拟合，预测折验证带",
                "unit_seconds": [sec["low"], sec["high"]],
            })

    for inc_model, base_model in (("INC_M2", "M2"), ("INC_M3a", "M3a")):
        sec = UNIT_SECONDS["catboost_fit"]
        for fold in FOLDS:
            units.append({
                "unit_id": f"{inc_model}:{fold}:outer_refit",
                "kind": "model_fit",
                "model": inc_model,
                "engine": "catboost",
                "fold_id": fold,
                "stage": "outer_refit",
                "config_id": f"selected@{base_model}:inner",
                "config": None,
                "feature_group": "base",
                "detail": f"属性增量实验：{base_model} 本折选中配置 × 基础特征组（去房屋属性组）",
                "unit_seconds": [sec["low"], sec["high"]],
            })

    return units


def count_units(units: list[dict]) -> dict:
    prep = sum(1 for u in units if u["kind"] == "derived_prep")
    by_model: dict[str, int] = {}
    for u in units:
        if u["model"]:
            by_model[u["model"]] = by_model.get(u["model"], 0) + 1
    catboost = sum(1 for u in units if u["engine"] == "catboost")
    lightgbm = sum(1 for u in units if u["engine"] == "lightgbm")
    return {
        "total": len(units),
        "derived_prep": prep,
        "M2": by_model.get("M2", 0),
        "M3a": by_model.get("M3a", 0),
        "M3b": by_model.get("M3b", 0),
        "INCREMENT": by_model.get("INC_M2", 0) + by_model.get("INC_M3a", 0),
        "M4": by_model.get("M4", 0),
        "catboost_fits": catboost,
        "lightgbm_fits": lightgbm,
    }


def budget_reconciliation() -> dict:
    """594 单元 × 单位耗时（实测/外推）= 本机单线程连续墙钟；再折算云侧并行。"""
    units = build_units()
    counts = count_units(units)

    def span(pairs):
        low = sum(p[0] * n for p, n in pairs)
        high = sum(p[1] * n for p, n in pairs)
        return low, high

    cat = UNIT_SECONDS["catboost_fit"]
    lgb = UNIT_SECONDS["lightgbm_fit"]
    prep = UNIT_SECONDS["derived_prep"]

    cat_s = (cat["low"] * counts["catboost_fits"], cat["high"] * counts["catboost_fits"])
    lgb_s = (lgb["low"] * counts["lightgbm_fits"], lgb["high"] * counts["lightgbm_fits"])
    prep_s = (prep["low"] * counts["derived_prep"], prep["high"] * counts["derived_prep"])
    total_s = (cat_s[0] + lgb_s[0] + prep_s[0], cat_s[1] + lgb_s[1] + prep_s[1])

    return {
        "formula": "本机单线程总时长 = CatBoost拟合数 × 55.3—56.4s + LightGBM拟合数 × 2.3—2.5s"
                   " + 派生准备数 × 120—240s",
        "counts": counts,
        "components_seconds": {
            "catboost": {"fits": counts["catboost_fits"], "low": cat_s[0], "high": cat_s[1]},
            "lightgbm": {"fits": counts["lightgbm_fits"], "low": lgb_s[0], "high": lgb_s[1]},
            "derived_prep": {"fits": counts["derived_prep"], "low": prep_s[0], "high": prep_s[1]},
        },
        "local_single_thread_seconds": {"low": total_s[0], "high": total_s[1]},
        "local_single_thread_hours": {"low": round(total_s[0] / 3600, 2),
                                      "high": round(total_s[1] / 3600, 2)},
        "parallel_12_workers_minutes": {"low": round(total_s[0] / 12 / 60, 1),
                                        "high": round(total_s[1] / 12 / 60, 1)},
        "cloud_16vcpu_minutes": {"low": round(total_s[0] / CLOUD_VCPU / 60, 1),
                                 "high": round(total_s[1] / CLOUD_VCPU / 60, 1)},
        "unit_seconds": UNIT_SECONDS,
        "notes": [
            "12 worker 近饱和 16 核违反护机红线（>2 分钟且 CPU ≥50%），故全量拟合转云执行；"
            "本机仅留单折冒烟、计时探针与轻门参考预生成",
            "云侧墙钟 ≈ 计算 + 备包/自举/轻门/回收 ≈ 1—1.5h（design D7）",
        ],
        "regression_guard": "若任一环节本机将超 2 分钟且 CPU ≥50% → 转云；"
                            "网格/预算冲动扩展超出 594 单元 → 不执行，记 backlog",
    }


def local_aux_compute() -> dict:
    """本机 A1/M1/评估段计算式（proposal 计算式，审1 F4 要求数值化）。"""
    return {
        "M1_refit": {
            "formula": "18 折 × 5 次拟合（4λ + 外层）× 10—30s/次",
            "low": 18 * 5 * 10, "high": 18 * 5 * 30,
            "source": "外推（S2 全折评估分钟级背书）；闭式解单线程",
            "unit": "seconds",
        },
        "A1_case_retrieval": {
            "formula": "3,281 目标 × ≤8 案例 = ≤26,248 案例次 × 0.1—0.5ms/案例次"
                       " + parquet/JSON 读写 I/O ≤2 分钟",
            "low": round(3281 * 8 * 0.0001, 3), "high": round(3281 * 8 * 0.0005, 3),
            "source": "外推（polars 向量化 join+算术；执行期首 1,000 目标实测回填）",
            "unit": "seconds",
        },
        "paired_bootstrap": {
            "formula": "2000 次 × 3,281 行重采样聚合 × 0.5—2.5ms/次 = 1.0—5.0s"
                       "（3,281 为每次聚合规模，不再与单次耗时重复相乘）",
            "low": round(2000 * 0.0005, 3), "high": round(2000 * 0.0025, 3),
            "source": "外推（numpy 向量化）",
            "unit": "seconds",
        },
        "independent_quote_eval": {
            "formula": "全折合格目标 12,095 行（每折验证带行数不等，均 ≤2,000）× "
                       "约 6 模型线 × 单位聚合耗时 ≪1ms/行 ≈ 分钟级；单 worker 单线程",
            "low": 60.0, "high": 600.0,
            "source": "外推（S2 evaluate.py 实测先例）；与 594 正式单元无关，纯本机聚合",
            "unit": "seconds",
        },
        "total_hours": {"low": 0.4, "high": 1.0},
        "note": "整机 CPU <50%，不触护机红线；M1 复算为其中最大项且已单列",
    }


def quality_validation_runs() -> dict:
    """质量验证运行：与 594 正式单元分列，注明是否复用正式运行结果（审1 F4）。"""
    cat = UNIT_SECONDS["catboost_fit"]
    return {
        "declared_units": 21,
        "note": "21 为审1/提案登记的「质量验证运行」合计口径（5 参考 + 首 10 条金丝雀 + 6 重跑），"
                "用于与 594 正式单元分列；其中含预测行与任务单元两类，下表按类拆分数值口径",
        "items": [
            {
                "item": "本机预生成轻门参考",
                "task_units": 5,
                "cost_seconds": [round(5 * cat["low"], 1), round(5 * cat["high"], 1)],
                "where": "本机（单线程、单核、≤5 分钟）",
                "reuses_formal_run": False,
                "note": "云跑前生成，作为跨机对拍参考",
            },
            {
                "item": "云侧轻门对拍 + 金丝雀首 10 条",
                "task_units": 15,
                "compare_calls": 15,
                "cost_seconds": [0.0, 0.0],
                "where": "云侧（读取比对）",
                "reuses_formal_run": True,
                "note": "复用正式运行产物（轻门 5 条 + 正式前 10 条），无额外模型拟合；"
                        "金丝雀检查以任务产物的逐房预测行数计，不折成任务单元",
            },
            {
                "item": "同机重跑抽样",
                "task_units": 6,
                "cost_seconds": [round(6 * cat["low"], 1), round(6 * cat["high"], 1)],
                "where": "云实例内（额外重拟合）",
                "reuses_formal_run": False,
                "note": "字节级一致口径；云侧 6 × 55.3—56.4s ÷ 16 vCPU ≈ 21s，成本可忽略",
            },
            {
                "item": "跨机容差抽查（任务 6.2）",
                "task_units": 1,
                "cost_seconds": [round(cat["low"], 1), round(cat["high"], 1)],
                "where": "本机（1 个小折任务）",
                "reuses_formal_run": False,
                "note": "相对容差 1e-9×max(|ref|,1)，非有限值即 FAIL",
            },
        ],
        "formal_units_unchanged": 594,
        "cloud_extra_cost_estimate_yuan": [0.0, 0.05],
    }


def build_registry() -> dict:
    units = build_units()
    counts = count_units(units)
    return {
        "schema_version": SCHEMA_VERSION,
        "change": CHANGE,
        "target": TARGET,
        "seed": SEED,
        "thread": THREAD,
        "folds": FOLDS,
        "engines": ENGINES,
        "grids": {
            "M2": M2_GRID,
            "M3a": M3A_GRID,
            "M3b": M3B_GRID,
            "M4": M4_GRID,
        },
        "grid_sizes": {k: len(v) for k, v in
                       (("M2", M2_GRID), ("M3a", M3A_GRID),
                        ("M3b", M3B_GRID), ("M4", M4_GRID))},
        "feature_groups": FEATURE_GROUPS,
        "refit_flow": REFIT_FLOW,
        "declarations": {
            "seed": SEED,
            "thread": THREAD,
            "thread_pinning": "phase2 包导入期钉死 BLAS/OMP/MKL/NumExpr/Veclib/polars 线程 = 1；"
                              "CatBoost thread_count=1、LightGBM num_threads=1",
            "grid_frozen": "网格有界、运行前固定、运行中不得追加配置",
            "budget_frozen": "594 正式单元；超出即不执行并记 backlog",
            "quality_runs_separate": "质量验证运行与 594 单列，不计入正式单元、不视为扩网格",
        },
        "counts": counts,
        "budget": budget_reconciliation(),
        "local_aux_compute": local_aux_compute(),
        "quality_validation_runs": quality_validation_runs(),
        "source": SOURCE,
        "units": units,
    }


def _canonical(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def to_jsonl_lines(registry: dict | None = None) -> list[str]:
    """meta 行 + 逐单元行（顺序固定，可字节级复现）。"""
    reg = registry or build_registry()
    meta = {k: v for k, v in reg.items() if k != "units"}
    lines = [_canonical({"record_type": "registry", **meta})]
    for u in reg["units"]:
        lines.append(_canonical({"record_type": "unit", **u}))
    return lines


def write_jsonl(path: Path, registry: dict | None = None) -> int:
    lines = to_jsonl_lines(registry)
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 S3 试验注册表（594 单元）")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_gen = sub.add_parser("write", help="生成并写出 experiments.jsonl")
    p_gen.add_argument("out_path")
    p_sum = sub.add_parser("summary", help="打印计数与预算对账（stdout JSON）")
    args = parser.parse_args(argv)

    if args.cmd == "write":
        n = write_jsonl(Path(args.out_path))
        print(_canonical({"out": str(args.out_path), "lines": n,
                          **count_units(build_units())}))
        return 0
    if args.cmd == "summary":
        reg = build_registry()
        print(json.dumps({"counts": reg["counts"], "budget": reg["budget"],
                          "quality_validation_runs": reg["quality_validation_runs"]},
                         ensure_ascii=False, indent=1))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
