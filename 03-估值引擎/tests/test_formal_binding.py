# -*- coding: utf-8 -*-
"""错配组合拒绝（含映射更新 bundle_id 不变场景）、追加式哈希链断言。

运行（退出码 0 为过）：
uv run pytest tests/test_formal_binding.py

被测模块设计为 stdlib-only（formal_binding 只依赖标准库）；
并入全量 pytest 套件后不再于文件顶部断言 numpy/polars 未被导入
（套件内其他测试可能先导入 numpy/polars，该隔离检查仅在单文件运行时有意义）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from gz_property_valuation.phase2 import formal_binding as fb  # noqa: E402

FIVE = {"model": "a" * 64, "feature": "b" * 64, "market_asset": "c" * 64,
        "calibration": "d" * 64, "coordination_policy": "e" * 64}
EXTRA = {"inference_code_sha": "f" * 64, "formal_gate_rule_sha": "1" * 64,
         "adoption_contract_sha": "2" * 64, "current_block_map_sha": "3" * 64,
         "production_code_sha": "4" * 64}


def compose(**over) -> dict:
    five = {**FIVE, **over.pop("five", {})}
    kw = {**EXTRA, **over}
    return fb.compose_nine(five, **kw)


# ---------------------------------------------------------------- 九件组合指纹

def test_compose_nine_components_and_composition_id():
    combo = compose()
    assert sorted(combo["components"]) == sorted(fb.NINE_COMPONENTS)
    assert combo["composition_id"] == fb.digest_of(combo["components"])
    assert len(combo["composition_id"]) == 64


def test_compose_nine_rejects_bad_five_keys():
    try:
        fb.compose_nine({k: v for k, v in FIVE.items() if k != "model"},
                        **EXTRA)
        raise AssertionError("缺组件必须拒绝")
    except ValueError:
        pass


def test_any_component_change_changes_composition():
    base = compose().get("composition_id")
    variants = [
        compose(five={"model": "0" * 64}),
        compose(inference_code_sha="0" * 64),
        compose(formal_gate_rule_sha="0" * 64),
        compose(adoption_contract_sha="0" * 64),
        compose(current_block_map_sha="0" * 64),
    ]
    for v in variants:
        assert v["composition_id"] != base, "九件任一更新必须得到不同组合"


# ---------------------------------------------------------------- 错配拒绝

def test_compare_ok_when_identical():
    cmp = fb.compare_combinations(compose(), compose())
    assert cmp["ok"] and cmp["composition_equal"]
    assert cmp["mismatches"] == [] and cmp["missing"] == []


def test_mismatch_rejected_map_update_bundle_id_unchanged():
    """映射更新而 bundle_id（五组件）不变 → 识别为不同组合并拒绝混用（spec 场景）。"""
    registered = compose()
    updated = compose(current_block_map_sha="9" * 64)
    assert registered["components"]["model"] == updated["components"]["model"]
    assert registered["composition_id"] != updated["composition_id"]
    for a, b in ((registered, updated), (updated, registered)):
        cmp = fb.compare_combinations(a, b)
        assert not cmp["ok"]
        assert cmp["mismatches"] == ["current_block_map"]
        assert not cmp["composition_equal"]


def test_mismatch_rejected_each_component():
    registered = compose()
    cases = {
        "inference_code": compose(inference_code_sha="0" * 64),
        "formal_gate_rule": compose(formal_gate_rule_sha="0" * 64),
        "adoption_contract": compose(adoption_contract_sha="0" * 64),
        "model": compose(five={"model": "0" * 64}),
    }
    for name, actual in cases.items():
        cmp = fb.compare_combinations(registered, actual)
        assert not cmp["ok"] and cmp["mismatches"] == [name]


def test_mismatch_rejected_missing_component():
    registered = compose()
    broken = {"components": {k: v for k, v in registered["components"].items()
                             if k != "calibration"},
              "composition_id": registered["composition_id"]}
    cmp = fb.compare_combinations(registered, broken)
    assert not cmp["ok"] and cmp["missing"] == ["calibration"]


# ---------------------------------------------------------------- 追加式哈希链

def _pred_record(pid: str, comp: str, price=None) -> dict:
    return {"record_type": "prediction", "prediction_id": pid,
            "composition_id": comp, "prediction": price}


def test_append_and_verify_chain(tmp_path):
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    r1 = fb.append_record(p, _pred_record("P1", comp, 35717.35))
    r2 = fb.append_record(p, _pred_record("P2", comp, None))
    r3 = fb.append_record(p, {"record_type": "label", "prediction_id": "P1",
                              "composition_id": comp, "label_seq": 1,
                              "label": {"unit_price": 36000.0}})
    assert r1["prev_sha256"] == "" and r2["prev_sha256"] == r1["record_sha256"]
    assert r3["prev_sha256"] == r2["record_sha256"]
    v = fb.verify_chain(p)
    assert v["ok"] and v["n_records"] == 3 and v["errors"] == []


def test_append_only_existing_lines_byte_identical(tmp_path):
    """标签只追加不倒改：追加后既有字节逐位不变。"""
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    fb.append_record(p, _pred_record("P1", comp, 35717.35))
    fb.append_record(p, {"record_type": "label", "prediction_id": "P1",
                         "composition_id": comp, "label_seq": 1,
                         "label": {"unit_price": 36000.0}})
    before = p.read_bytes()
    fb.append_record(p, {"record_type": "label", "prediction_id": "P1",
                         "composition_id": comp, "label_seq": 2,
                         "label": {"unit_price": 36100.0, "note": "修正标签"}})
    after = p.read_bytes()
    assert after.startswith(before) and len(after) > len(before)
    assert fb.verify_chain(p)["ok"]


def test_tampered_record_detected(tmp_path):
    """倒改检测：改写既有记录任一字段 → 记录摘要失配。"""
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    fb.append_record(p, _pred_record("P1", comp, 35717.35))
    fb.append_record(p, _pred_record("P2", comp, 28000.0))
    rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()]
    rows[0]["prediction"] = 99999.0  # 模拟倒改历史预测
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")) for r in rows) + "\n",
                 encoding="utf-8")
    v = fb.verify_chain(p)
    assert not v["ok"]
    codes = {e["code"] for e in v["errors"]}
    assert "RECORD_DIGEST_MISMATCH" in codes


def test_tampered_prev_link_detected(tmp_path):
    """断链检测：改写中间条目摘要 → 后条 prev 连接断裂被同时发现。"""
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    fb.append_record(p, _pred_record("P1", comp))
    fb.append_record(p, _pred_record("P2", comp))
    fb.append_record(p, _pred_record("P3", comp))
    rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()]
    rows[1]["record_sha256"] = "0" * 64
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")) for r in rows) + "\n",
                 encoding="utf-8")
    v = fb.verify_chain(p)
    assert not v["ok"]
    codes = {e["code"] for e in v["errors"]}
    assert "CHAIN_BREAK" in codes and "RECORD_DIGEST_MISMATCH" in codes


def test_verify_empty_file_not_ok(tmp_path):
    v = fb.verify_chain(tmp_path / "none.jsonl")
    assert v["ok"] is False and v["n_records"] == 0


def test_structure_report_nine_components():
    rep = fb.structure_report()
    assert rep["nine_components"] == list(fb.NINE_COMPONENTS)
