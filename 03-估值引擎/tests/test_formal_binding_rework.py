# -*- coding: utf-8 -*-
"""审2 RV-ECR-VERIFY-01 返工反例：F6 production_code 组合指纹＋F8 哈希链先验完整性。

覆盖审2 §5 修复验收：
- F6：正式执行代码（candidate_ops/release_record/stop_switch/formal_binding/
  formal_states/candidate_request）纳入确定性代码清单/聚合摘要——正式入口文件单独
  变化而五组件不变 → composition_id 变化 → 旧凭据拒绝（compare_combinations）；
- F8：append_record 追加前先验链（损坏拒绝追加）；load_records_verified 先验后消费。

运行（退出码 0 为过）：
uv run pytest tests/test_formal_binding_rework.py

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


# ---------------------------------------------------------------- F6 production_code

def test_f6_production_code_is_required_component():
    assert "production_code" in fb.RELEASE_COMPONENTS
    combo = compose()
    assert sorted(combo["components"]) == sorted(fb.NINE_COMPONENTS)
    assert combo["components"]["production_code"] == "4" * 64
    assert combo["composition_id"] == fb.digest_of(combo["components"])


def test_f6_compose_rejects_missing_production_code():
    try:
        fb.compose_nine(FIVE, inference_code_sha="f" * 64,
                        formal_gate_rule_sha="1" * 64,
                        adoption_contract_sha="2" * 64,
                        current_block_map_sha="3" * 64)
        raise AssertionError("缺 production_code_sha 必须拒绝（不能默许生产代码缺席）")
    except TypeError:
        pass


def test_f6_entry_file_change_with_five_components_unchanged():
    """正式入口文件单独变化（五组件不变）→ 新组合 → 旧凭据拒绝（mismatches 单列）。"""
    registered = compose()
    updated = compose(production_code_sha="9" * 64)
    assert (updated["components"]["model"]
            == registered["components"]["model"])
    five_same = all(updated["components"][k] == registered["components"][k]
                    for k in fb.FIVE_COMPONENTS)
    assert five_same, "五组件不变前提"
    assert updated["composition_id"] != registered["composition_id"]
    cmp = fb.compare_combinations(registered, updated)
    assert not cmp["ok"]
    assert cmp["mismatches"] == ["production_code"]
    cmp_rev = fb.compare_combinations(updated, registered)
    assert not cmp_rev["ok"] and cmp_rev["mismatches"] == ["production_code"]


def test_f6_any_component_change_changes_composition():
    base = compose()["composition_id"]
    variants = [compose(production_code_sha="9" * 64),
                compose(inference_code_sha="9" * 64),
                compose(current_block_map_sha="9" * 64),
                compose(five={"calibration": "9" * 64})]
    for v in variants:
        assert v["composition_id"] != base


# ---------------------------------------------------------------- F8 链先验完整性

def _pred_record(pid: str, comp: str) -> dict:
    return {"record_type": "prediction", "prediction_id": pid,
            "composition_id": comp, "prediction": 35717.35}


def _tamper_first_record_price(p: Path, new_price=99999.0) -> None:
    rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()]
    rows[0]["prediction"] = new_price
    p.write_text("\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")) for r in rows) + "\n",
                 encoding="utf-8")


def test_f8_append_rejected_on_corrupt_chain(tmp_path):
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    fb.append_record(p, _pred_record("P1", comp))
    fb.append_record(p, _pred_record("P2", comp))
    _tamper_first_record_price(p)
    try:
        fb.append_record(p, _pred_record("P3", comp))
        raise AssertionError("损坏链上追加必须被拒绝（先验完整性后追加）")
    except fb.FormalChainError:
        pass
    rows = fb.load_records(p)
    assert len(rows) == 2, "被拒绝的追加不得改动文件"


def test_f8_load_records_verified_rejects_corrupt(tmp_path):
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    fb.append_record(p, _pred_record("P1", comp))
    good = fb.load_records_verified(p)
    assert len(good) == 1
    _tamper_first_record_price(p)
    try:
        fb.load_records_verified(p)
        raise AssertionError("损坏链消费必须被拒绝（先验完整性后消费）")
    except fb.FormalChainError:
        pass


def test_f8_verified_load_passes_intact_chain(tmp_path):
    p = tmp_path / "formal-records.jsonl"
    comp = compose()["composition_id"]
    for pid in ("P1", "P2", "P3"):
        fb.append_record(p, _pred_record(pid, comp))
    rows = fb.load_records_verified(p)
    assert [r["prediction_id"] for r in rows] == ["P1", "P2", "P3"]
