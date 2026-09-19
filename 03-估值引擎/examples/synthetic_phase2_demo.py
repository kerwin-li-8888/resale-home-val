# -*- coding: utf-8 -*-
"""synthetic_phase2_demo——纯离线合成数据端到端演示（固定种子，秒级完成）。

五段演示（对应 phase2 条件发布机制的核心语义）：

1. 固定种子造数：numpy 随机源（SEED=20260918）生成云溪区 8 个虚构小区、3 个虚构
   板块的合成成交主表（含缺失值与降级样本，schema 与 S1 特征层一致）；
2. B0/M1 训练评估：B0 走引擎真模块 baselines（community→block→district 回退链，
   含排除自身断言与回退比例披露）；M1 走引擎真模块 models（FoldEncoder 闭式解
   Ridge，log 域训练、预测回单价）；
3. 冻结与版本指纹：demo 资产五组件＋正式执行代码经 formal_binding.compose_nine
   得九件组合指纹，预测/标签记录经 append_record 落追加式哈希链并 verify_chain；
4. 候选调用含降级路径：三个演示请求分别命中 B0 的 community/block/district 层
   （回退链 B0_FALLBACK_CHAIN 语义），M1-B0 分歧超阈值时按协调策略标注不仲裁；
   全程输出候选参考价，不产出正式价；
5. 未发布态发布门禁演示：无发布记录（默认态）→ release_record fail-closed
   （RC_RECORD_MISSING）→ 状态机 version_disabled 无正式价 → formal_gate GATE-09
   （FG09_RELEASE_CHECK_MISSING）→ 停止开关默认停止。本演示不执行任何正式启用
   操作，与「暂不可正式启用」业务结论一致。

运行方式（在 03-估值引擎/ 目录下）：

    uv run python examples/synthetic_phase2_demo.py

- 全程离线：不访问网络；产物落 examples/phase2_demo/synthetic_demo/；
- 确定性：随机源固定、无运行时刻写入，连续两次运行的关键产物（stdout 摘要与
  demo_summary.json）逐字段一致；
- 本脚本仅用于演示机制语义，不构成任何估值建议。
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import gz_property_valuation.phase2  # noqa: F401  必须先于 numpy/polars 导入（线程钉死守卫）

import numpy as np
import polars as pl

from gz_property_valuation.phase2 import baselines
from gz_property_valuation.phase2 import candidate_inference as ci
from gz_property_valuation.phase2 import formal_binding as fb
from gz_property_valuation.phase2 import formal_gate as fg
from gz_property_valuation.phase2 import formal_states as fs
from gz_property_valuation.phase2 import release_record as rr
from gz_property_valuation.phase2 import stop_switch as sw

ENGINE_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ENGINE_ROOT / "examples" / "phase2_demo" / "synthetic_demo"
PHASE2_DIR = ENGINE_ROOT / "src" / "gz_property_valuation" / "phase2"

SEED = 20260918
DISTRICT = "云溪区"
ADOPTION_CONTRACT_ID = "DEMO-RELEASE-001"
ANCHOR = date(2026, 7, 1)
DATA_START = date(2024, 7, 1)
DATA_END = date(2026, 7, 31)
VAL_END = date(2026, 7, 31)
M1_LAMBDA = 1.0

COMMUNITIES = [
    ("C-XXXX0001", "云溪参考花园", "云溪中央板块", 0.00),
    ("C-XXXX0002", "云溪学府里", "云溪中央板块", 0.12),
    ("C-XXXX0003", "云溪湖畔小区", "云溪湖畔板块", 0.20),
    ("C-XXXX0004", "云溪梧桐苑", "云溪湖畔板块", -0.08),
    ("C-XXXX0005", "云溪东郡", "云溪东板块", -0.15),
    ("C-XXXX0006", "云溪祥和家园", "云溪东板块", 0.06),
    ("C-XXXX0007", "云溪老街坊", "云溪中央板块", -0.22),
    ("C-XXXX0008", "云溪翠庭", "云溪东板块", 0.09),
]
FLOOR_BUCKETS = ("低楼层", "中楼层", "高楼层")
ELEVATOR_STATES = ("有电梯", "无电梯")
ORIENTATIONS = ("南北", "朝南", "朝东")
DECORATIONS = ("精装", "简装", "毛坯")
BEDROOMS = ("2", "3", "4")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_obj(obj) -> str:
    return sha256_bytes(json.dumps(obj, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":")).encode("utf-8"))


# ---------------------------------------------------------------- 第 1 段：造数

def synthetic_master() -> pl.DataFrame:
    rng = np.random.default_rng(SEED)
    rows = []
    rid = 0
    for comm_id, comm_name, block, comm_eff in COMMUNITIES:
        n = 90 if comm_id != "C-XXXX0007" else 60
        dates = [DATA_START + timedelta(days=int(d))
                 for d in rng.integers(0, (DATA_END - DATA_START).days + 1, size=n)]
        areas = np.round(rng.uniform(55.0, 140.0, size=n), 1)
        for i in range(n):
            rid += 1
            total_floors = None if comm_id == "C-XXXX0007" else int(rng.integers(6, 33))
            age_years = float(np.round(rng.uniform(1.0, 24.0), 1))
            elevator = ELEVATOR_STATES[0] if total_floors and total_floors >= 9 \
                else ELEVATOR_STATES[int(rng.integers(0, 2))]
            fb_idx = int(rng.integers(0, 3))
            log_price = (9.72 + comm_eff + 0.10 * fb_idx + 0.002 * (areas[i] - 90.0)
                         + (0.05 if elevator == ELEVATOR_STATES[0] else 0.0)
                         + float(rng.normal(0.0, 0.04)))
            rows.append({
                "source_record_id": f"SYN-{rid:06d}",
                "community_source_id": comm_id,
                "community_name": comm_name,
                "block_name": block,
                "sale_date_d": dates[i],
                "unit_price": round(float(np.exp(log_price)), 2),
                "area_sqm": float(areas[i]),
                "age_years": age_years,
                "total_floors": total_floors,
                "miss_year_built": 0.0,
                "miss_total_floors": 1.0 if total_floors is None else 0.0,
                "floor_bucket": FLOOR_BUCKETS[fb_idx],
                "elevator_state": elevator,
                "bedrooms_n": BEDROOMS[int(rng.integers(0, 3))],
                "orientation": ORIENTATIONS[int(rng.integers(0, 3))],
                "decoration_state": DECORATIONS[int(rng.integers(0, 3))],
            })
    return pl.DataFrame(rows)


# ---------------------------------------------------------------- 第 2 段：B0/M1

def run_b0_m1(master: pl.DataFrame) -> dict:
    pool = master.filter(pl.col("sale_date_d") < ANCHOR)
    targets = master.filter((pl.col("sale_date_d") >= ANCHOR)
                            & (pl.col("sale_date_d") <= VAL_END))
    blocks = baselines.load_blocks(master)

    b0 = baselines.predict_b0(pool, targets, ANCHOR, blocks)
    b0 = b0.with_columns(
        (pl.col("b0_pred") / pl.col("unit_price") - 1.0).abs().alias("ape"))
    b0_med_ape = float(b0["ape"].median())
    disclosure = baselines.fallback_disclosure(b0)

    enc = models_fit(pool)
    w = ridge_fit(enc, pool)
    pred_log = enc.transform(targets) @ w
    m1_pred = np.exp(pred_log)
    m1_ape = np.abs(m1_pred / targets["unit_price"].to_numpy() - 1.0)
    m1_med_ape = float(np.median(m1_ape))
    assert np.all(np.isfinite(m1_pred)) and np.all(m1_pred > 0), "M1 预测必须为正且有限"
    assert b0.height == targets.height and bool(b0["b0_pred"].is_not_null().all()), \
        "B0 预测必须逐行非空（district 层兜底）"

    residual = np.log(targets["unit_price"].to_numpy()) - pred_log
    interval_table = {
        "layers": {"district": {"n": int(residual.size)}},
        "nominal_levels": {"nominal_80": [0.10, 0.90], "nominal_90": [0.05, 0.95]},
        "nominal_80": [float(np.quantile(residual, 0.10)), float(np.quantile(residual, 0.90))],
        "nominal_90": [float(np.quantile(residual, 0.05)), float(np.quantile(residual, 0.95))],
        "residual_space": "log 空间经验分位（e = log 真值 − log 预测）",
    }
    return {"pool": pool, "targets": targets, "blocks": blocks, "enc": enc, "w": w,
            "b0_med_ape": b0_med_ape, "m1_med_ape": m1_med_ape,
            "b0_disclosure": disclosure, "interval_table": interval_table}


def models_fit(pool: pl.DataFrame):
    from gz_property_valuation.phase2 import models as md
    return md.FoldEncoder().fit(pool)


def ridge_fit(enc, pool: pl.DataFrame) -> np.ndarray:
    from gz_property_valuation.phase2 import models as md
    X = enc.transform(pool)
    y = np.log(pool["unit_price"].to_numpy().astype(np.float64))
    return md.ridge_solve(X, y, M1_LAMBDA)


# ---------------------------------------------------------------- 第 3 段：指纹与哈希链

def version_fingerprint(m1: dict) -> dict:
    weights_bytes = m1["w"].astype(np.float64).tobytes()
    encoder_doc = {
        "cat_cols": m1["enc"].cat_cols,
        "categories": m1["enc"].categories,
        "ym_clamp": m1["enc"].ym_clamp,
        "fills": m1["enc"].fills,
        "mu": m1["enc"].mu.tolist(), "sd": m1["enc"].sd.tolist(),
    }
    window = baselines.window_materials(m1["pool"], ANCHOR)
    com, blk, district_med = baselines.b0_level_tables(window)
    # group_by 聚合输出行序不定（polars 多线程），进指纹前按 key 排序钉死序列化顺序
    com_rows = sorted(com.to_dicts(), key=lambda r: r["community_source_id"])
    blk_rows = sorted(blk.to_dicts(), key=lambda r: r["block_name"])
    market_asset = {"com": com_rows, "blk": blk_rows,
                    "district_med": district_med, "window_days": baselines.WINDOW_DAYS}
    policy = {
        "id": "synthetic-demo-coordination-v1",
        "b0_fallback_chain": ["community", "block", "district"],
        "m1_b0_divergence_threshold": ci.CONFLICT_THRESHOLD_DEFAULT,
    }
    five = {
        "model": sha256_bytes(weights_bytes),
        "feature": sha256_obj(encoder_doc),
        "market_asset": sha256_obj(market_asset),
        "calibration": sha256_obj(m1["interval_table"]),
        "coordination_policy": sha256_obj(policy),
    }
    demo_src = Path(__file__).resolve().read_bytes()
    production = b"".join(
        (PHASE2_DIR / name).read_bytes()
        for name in ("candidate_ops.py", "release_record.py", "stop_switch.py",
                     "formal_binding.py", "formal_states.py", "candidate_request.py"))
    combo = fb.compose_nine(
        five,
        inference_code_sha=sha256_bytes(demo_src),
        formal_gate_rule_sha=sha256_obj({"formal_gate_version": "formal-gate-v1",
                                         "nine_gates": fg.NINE_GATES
                                         if hasattr(fg, "NINE_GATES") else 9}),
        adoption_contract_sha=fb.digest_of({"contract_id": ADOPTION_CONTRACT_ID}),
        current_block_map_sha=fb.digest_of(sorted({c[2] for c in COMMUNITIES})),
        production_code_sha=sha256_bytes(production))
    return {"combo": combo, "five": five}


def append_demo_records(combo: dict, m1: dict) -> dict:
    records_path = OUT_DIR / "formal-records.jsonl"
    comp = combo["composition_id"]
    first = m1["targets"].head(1).to_dicts()[0]
    rec1 = fb.append_record(records_path, {
        "record_type": "prediction", "prediction_id": "SYN-DEMO-P1",
        "composition_id": comp, "prediction": first["unit_price"]})
    rec2 = fb.append_record(records_path, {
        "record_type": "label", "prediction_id": "SYN-DEMO-P1",
        "composition_id": comp, "label_seq": 1,
        "label": {"unit_price": first["unit_price"]}})
    verified = fb.verify_chain(records_path)
    assert verified["ok"], f"哈希链验证失败：{verified['errors']}"
    return {"chain": {"records": verified["n_records"],
                      "first_sha256": rec1["record_sha256"][:16],
                      "prev_link_ok": rec2["prev_sha256"] == rec1["record_sha256"],
                      "verify": verified["ok"]}}


# ---------------------------------------------------------------- 第 4 段：候选调用（含降级路径）

def candidate_calls(m1: dict) -> list[dict]:
    window = baselines.window_materials(m1["pool"], ANCHOR)
    com, blk, district_med = baselines.b0_level_tables(window)
    com_map = {r["community_source_id"]: float(r["pred_community"]) for r in com.to_dicts()}
    blk_map = {r["block_name"]: float(r["pred_block"]) for r in blk.to_dicts()}

    requests = [
        {"request_id": "DEMO-REQ-01", "community_source_id": "C-XXXX0001",
         "block_name": "云溪中央板块", "branch": "normal",
         "note": "小区窗口内案例充足 → B0 命中 community 层"},
        {"request_id": "DEMO-REQ-02", "community_source_id": "C-XXXX9999",
         "block_name": "云溪湖畔板块", "branch": "degraded_community_cold_start",
         "note": "小区为窗口外新小区 → B0 回退 block 层"},
        {"request_id": "DEMO-REQ-03", "community_source_id": "C-XXXX8888",
         "block_name": None, "branch": "degraded_block_cold_start",
         "note": "小区与板块均未知 → B0 回退 district 层（区级中位）"},
    ]
    base = {"district": DISTRICT, "property_use": "普通住宅", "area_sqm": 89.5,
            "age_years": 10.0, "total_floors": 18, "miss_year_built": 0.0,
            "miss_total_floors": 0.0, "floor_bucket": "中楼层",
            "elevator_state": "有电梯", "bedrooms_n": "3",
            "orientation": "南北", "decoration_state": "精装",
            "sale_date_d": ANCHOR}
    out = []
    for req in requests:
        row = {**base, "community_source_id": req["community_source_id"],
               "block_name": req["block_name"]}
        b0_pred = (com_map.get(req["community_source_id"])
                   or blk_map.get(req["block_name"]) or district_med)
        b0_level = ("community" if req["community_source_id"] in com_map
                    else "block" if req["block_name"] in blk_map else "district")
        m1_pred = float(np.exp(m1["enc"].transform(pl.DataFrame([row])) @ m1["w"])[0])
        divergence = abs(m1_pred - b0_pred) / b0_pred
        flagged = divergence > ci.CONFLICT_THRESHOLD_DEFAULT
        out.append({
            "request_id": req["request_id"], "branch": req["branch"],
            "b0_level": b0_level, "b0_pred": round(b0_pred, 2),
            "m1_pred_unit_price": round(m1_pred, 2),
            "m1_b0_divergence": round(divergence, 6),
            "m1_b0_divergence_flagged": bool(flagged),
            "decision": "candidate_reference",
            "formal_price": None,
            "note": req["note"] + ("；M1-B0 分歧超阈值仅标注，不仲裁不隐藏"
                                   if flagged else ""),
        })
    return out


# ---------------------------------------------------------------- 第 5 段：未发布态门禁

def release_gate_demo(combo: dict) -> dict:
    rc = rr.check_release(None, combo, branch="normal", valuation_date=ANCHOR)
    record_missing = rr.RC_RECORD_MISSING in rc["reason_codes"] and not rc["ok"]

    state = fs.decide(stop_stopped=False, release_ok=rc["ok"], input_rejected=False,
                      gate_eligible=None)
    env = fs.build_envelope(request_id="DEMO-REQ-01", state=state,
                            reason_codes=list(rc["reason_codes"]),
                            release_check=rc, combination=combo, branch="normal")
    no_price = env["formal_price"] is None and env["formal_report"] is None

    gate = fg.evaluate({
        "request_id": "DEMO-REQ-01",
        "request": {"district": DISTRICT, "property_use": "普通住宅",
                    "community_source_id": "C-XXXX0001", "area_sqm": 89.5},
        "status": {"scope_unverified": False, "out_of_scope": [],
                   "degradation_state": "normal", "known_community": True,
                   "cold_start_community": False, "a1_m1_conflict": False,
                   "block_source": "request", "block_mismatch": False,
                   "block_ambiguous": False, "reject": False, "reject_reasons": []},
        "point": {"m1_pred_unit_price": 21000.0, "m1_pred_total_price": 1879500.0,
                  "area_sqm": 89.5},
        "interval": {"nominal_80": {"low": 18000.0, "high": 24000.0},
                     "nominal_90": {"low": 16500.0, "high": 25500.0},
                     "source_layer": "community_C-XXXX0001", "source_layer_n": 42},
        "support": {"b0_level": "community", "b0_pred": 21000.0, "b0_window_n": 42,
                    "a1_status": "ok", "a1_n_cases": 12},
        "issues": [],
    }, support_case_count=10, release_check=None)
    gate09_blocked = ("GATE-09" in gate["failed_gates"]
                      and "FG09_RELEASE_CHECK_MISSING" in gate["reason_codes"])

    switch = sw.check_stop_switch(OUT_DIR / "empty-ops-state")
    default_stopped = switch["stopped"] and not switch["present"]

    assert record_missing and no_price and gate09_blocked and default_stopped, \
        "未发布态四重门禁必须全部 fail-closed"
    return {"unreleased": True, "record_missing": record_missing,
            "state": env["state"], "formal_price_present": not no_price,
            "gate09_reason": "FG09_RELEASE_CHECK_MISSING",
            "stop_switch_default_stopped": default_stopped,
            "conclusion": "默认态＝未发布，正式出价不可用（fail-closed）；"
                          "本演示不执行正式启用，与「暂不可正式启用」结论一致"}


# ---------------------------------------------------------------- 汇总

def main() -> int:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    (OUT_DIR / "empty-ops-state").mkdir(parents=True)

    master = synthetic_master()
    m1 = run_b0_m1(master)
    fp = version_fingerprint(m1)
    chain = append_demo_records(fp["combo"], m1)
    candidates = candidate_calls(m1)
    gate = release_gate_demo(fp["combo"])

    summary = {
        "seed": SEED, "district": DISTRICT, "anchor": ANCHOR.isoformat(),
        "contract_id": ADOPTION_CONTRACT_ID,
        "master_rows": master.height,
        "communities": master["community_source_id"].n_unique(),
        "b0_med_ape": round(m1["b0_med_ape"], 6),
        "m1_med_ape": round(m1["m1_med_ape"], 6),
        "b0_fallback_disclosure": {k: (None if v["ratio"] is None else round(v["ratio"], 6))
                                   for k, v in m1["b0_disclosure"].items()},
        "composition_id": fp["combo"]["composition_id"],
        "five_component_digests": {k: v[:16] for k, v in fp["five"].items()},
        "hash_chain": chain["chain"],
        "candidates": candidates,
        "release_gate": gate,
    }
    (OUT_DIR / "demo_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

    print("== synthetic_phase2_demo 摘要（确定性输出，两次运行应逐字段一致）==")
    print(f"seed={SEED} district={DISTRICT} anchor={ANCHOR.isoformat()}")
    print(f"[1] 造数: rows={master.height} communities={summary['communities']} "
          f"blocks={master['block_name'].n_unique()}")
    print(f"[2] B0 medAPE={summary['b0_med_ape']:.4%}  M1 medAPE={summary['m1_med_ape']:.4%} "
          f"fallback={summary['b0_fallback_disclosure']}")
    print(f"[3] composition_id={summary['composition_id']}")
    print(f"    hash_chain: records={chain['chain']['records']} "
          f"prev_link_ok={chain['chain']['prev_link_ok']} verify={chain['chain']['verify']}")
    for c in candidates:
        print(f"[4] {c['request_id']} branch={c['branch']} B0[{c['b0_level']}]="
              f"{c['b0_pred']:.2f} M1={c['m1_pred_unit_price']:.2f} "
              f"divergence={c['m1_b0_divergence']:.2%} "
              f"flagged={c['m1_b0_divergence_flagged']} decision={c['decision']}")
    print(f"[5] 未发布态门禁: state={gate['state']} formal_price_present="
          f"{gate['formal_price_present']} gate09={gate['gate09_reason']} "
          f"stop_switch_default_stopped={gate['stop_switch_default_stopped']}")
    print(f"结论: {gate['conclusion']}")
    print(f"产物目录: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
