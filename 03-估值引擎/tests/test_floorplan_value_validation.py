"""floorplan-value-validation 组1：冻结输入重建与门禁测试。

覆盖（对应 tasks 1.1-1.4）：
- 1.1 版本指针/冻结清单解析、输入只读登记、运行前后冻结文件哈希不变；
- 1.2 派生表确定性重建（同输入同 run_id 重跑 → 同派生表哈希）；
- 1.3 重建门禁（正确输入通过；计数不符输出缺口且不产出「通过」结论）；
- 1.4 血缘来源登记、schema 校验、排除清单说明。
合成 fixture 测试纯函数；真实冻结数据端到端测试在版本指针存在时执行。
全部离线，不触网、不付费。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from compsval.valuation.floorplan_value_validation import (
    EXPECTED_ANNOTATION_ROWS,
    EXPECTED_ASSET_COUNT,
    EXPECTED_STATE_COUNTS,
    EXPECTED_UNIQUE_IMAGE_HASHES,
    EXPECTED_UNIQUE_SOURCE_IDS,
    RebuildGateError,
    align_comparable_features,
    annotation_state_counts,
    build_area_quality,
    build_room_features,
    check_rebuild_gate,
    compute_pair_metrics,
    count_production_manifest,
    decide_round1_conclusion,
    decide_round2_gate,
    estimate_full_comparable,
    estimate_with_candidates,
    excel_layout_features,
    filter_candidates,
    freeze_round1_config,
    isolation_counts,
    load_frozen_round1_config,
    load_version_pointer,
    run_rebuild,
    run_round1_pair,
    similarity_overlay_score,
    time_coefficient,
    time_split_units,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "03-估值引擎" / "data"
VERSION_POINTER = DATA_DIR / "versions" / "lianjia_ext_latest.json"

_HAS_FROZEN_DATA = VERSION_POINTER.is_file()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# 合成 fixture
# ---------------------------------------------------------------------------


def _make_production_manifest(root: Path, n: int = 6, dup: int = 2) -> Path:
    """合成生产选择清单：n 条记录，前 dup 个 source_record_id 重复出现。"""
    records = []
    for i in range(n):
        source = str(108400000000 + i)
        records.append(
            {
                "source_record_id": source,
                "row_number": 160000 + i,
                "url_seq": 1,
                "url": f"http://ke-image.ljcdn.com/hdic-frame/{i}.png",
                "normalized_url": f"https://ke-image.ljcdn.com/hdic-frame/{i}.png",
                "domain": "ke-image.ljcdn.com",
            }
        )
    # 前 dup 个 source_record_id 各再出现一次（不同 url → 资产重复）
    for i in range(dup):
        records.append(
            {
                "source_record_id": str(108400000000 + i),
                "row_number": 170000 + i,
                "url_seq": 2,
                "url": f"http://ke-image.ljcdn.com/hdic-frame/dup-{i}.png",
                "normalized_url": f"https://ke-image.ljcdn.com/hdic-frame/dup-{i}.png",
                "domain": "ke-image.ljcdn.com",
            }
        )
    path = root / "selection" / "lianjia_ext" / "floorplan" / "production_manifest.json"
    _write_json(
        path,
        {"selection_rule_version": "EXTFP4-SELECT-1.0", "record_count": n + dup, "records": records},  # noqa: E501
    )
    return path


def _make_ocr_run(root: Path, n_tasks: int = 6) -> Path:
    """合成 OCR run 记录：n_tasks 个任务，image_sha256 全部唯一。"""
    tasks = [
        {
            "ocr_task_id": f"{i:064x}",
            "ocr_run_id": "floorplan-ocr-data_selection_l",
            "asset_id": f"{i + 1:064x}",
            "image_sha256": f"{1000 + i:064x}",
            "image_path": f"{i}.jpg",
            "width": 1440,
            "height": 1080,
            "mime_type": "image/jpeg",
            "provider": "aliyun_model_studio",
            "region": "cn-beijing",
            "model_requested": "qwen-vl-ocr-2025-11-20",
            "model_returned": None,
            "task": "advanced_recognition",
            "request_contract_version": "EXTFP3-A-OCR-1.0",
            "parser_version": "EXTFP3-B-NO-PARSER",
            "request_hash": f"{2000 + i:064x}",
            "provider_request_id": f"req-{i}",
            "state": "OCR_SUCCEEDED",
            "attempts": 1,
            "started_at": "2026-08-30T09:49:23.000000+00:00",
            "completed_at": "2026-08-30T09:49:37.000000+00:00",
            "finish_reason": "stop",
            "prompt_tokens": 100,
            "image_tokens": 100,
            "completion_tokens": 100,
            "response_status": "http-200",
        }
        for i in range(n_tasks)
    ]
    run_dir = root / "raw" / "source=lianjia_ext" / "dataset=floorplan_ocr_run" / "run_x"
    _write_json(run_dir / "ocr_run.json", {"ocr_run_id": "floorplan-ocr-data_selection_l", "tasks": tasks})  # noqa: E501
    return run_dir


# ---------------------------------------------------------------------------
# 1.3 门禁
# ---------------------------------------------------------------------------


def test_gate_passes_on_expected_counts() -> None:
    gaps = check_rebuild_gate(
        asset_count=EXPECTED_ASSET_COUNT,
        unique_source_record_id=EXPECTED_UNIQUE_SOURCE_IDS,
        unique_image_sha256=EXPECTED_UNIQUE_IMAGE_HASHES,
        annotation_rows=EXPECTED_ANNOTATION_ROWS,
        state_counts=dict(EXPECTED_STATE_COUNTS),
    )
    assert gaps == []


def test_gate_reports_each_mismatch() -> None:
    bad_state = dict(EXPECTED_STATE_COUNTS)
    bad_state["ACCEPTED"] = 1000
    gaps = check_rebuild_gate(
        asset_count=228,
        unique_source_record_id=100,
        unique_image_sha256=200,
        annotation_rows=1800,
        state_counts=bad_state,
    )
    joined = "\n".join(gaps)
    assert any("资产链数" in gap for gap in gaps)
    assert any("唯一 source_record_id" in gap for gap in gaps)
    assert any("唯一图片哈希" in gap for gap in gaps)
    assert any("标注行数" in gap for gap in gaps)
    assert any("状态 ACCEPTED" in gap for gap in gaps)
    assert "通过" not in joined


def test_state_counts_include_zero_for_missing_states() -> None:
    table = pa.table(
        {"parse_state": ["ACCEPTED", "ACCEPTED", "ROOM_ONLY"]},
        schema=pa.schema([pa.field("parse_state", pa.string())]),
    )
    counts = annotation_state_counts(table)
    assert counts["ACCEPTED"] == 2
    assert counts["ROOM_ONLY"] == 1
    assert counts["NEEDS_REVIEW"] == 0
    assert counts["CONFLICT"] == 0


# ---------------------------------------------------------------------------
# 1.3 生产计数（合成）
# ---------------------------------------------------------------------------


def test_count_production_manifest_synthetic(tmp_path: Path) -> None:
    prod = _make_production_manifest(tmp_path, n=6, dup=2)  # 8 条记录 / 6 唯一 id
    run_dir = _make_ocr_run(tmp_path, n_tasks=6)
    counts = count_production_manifest(prod, run_dir)
    assert counts["asset_count"] == 8
    assert counts["unique_source_record_id"] == 6
    assert counts["unique_image_sha256"] == 6
    assert counts["ocr_task_count"] == 6


# ---------------------------------------------------------------------------
# 1.1 版本指针（合成）
# ---------------------------------------------------------------------------


def test_load_version_pointer_requires_fields(tmp_path: Path) -> None:
    versions = tmp_path / "versions"
    _write_json(versions / "lianjia_ext_latest.json", {"version_id": "v1"})
    with pytest.raises(ValueError):
        load_version_pointer(tmp_path)
    _write_json(
        versions / "lianjia_ext_latest.json",
        {"version_id": "v1", "manifest": "m.json", "change_ref": "c"},
    )
    assert load_version_pointer(tmp_path)["version_id"] == "v1"


# ---------------------------------------------------------------------------
# 真实冻结数据端到端（1.1-1.4；版本指针存在时执行）
# ---------------------------------------------------------------------------


_XFAIL_FREEZE_REFRESH = pytest.mark.xfail(
    strict=False,
    reason=(
        "FVV-FREEZE-REFRESH: round-1 期望过期待新冻结配置生效，"
        "见 openspec/changes/add-fvv-frozen-expectation-refresh"
    ),
)


@_XFAIL_FREEZE_REFRESH
@pytest.mark.skipif(not _HAS_FROZEN_DATA, reason="冻结版本指针不存在，跳过真实数据测试")
def test_real_frozen_rebuild_gate(tmp_path: Path) -> None:
    out_root = tmp_path
    result = run_rebuild(DATA_DIR, out_root)
    assert result.rebuilt_table.num_rows == EXPECTED_ANNOTATION_ROWS
    assert result.state_counts == EXPECTED_STATE_COUNTS
    assert result.gate["passed"] is True
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["counts"]["asset_count"] == EXPECTED_ASSET_COUNT
    assert report["counts"]["unique_source_record_id"] == EXPECTED_UNIQUE_SOURCE_IDS
    assert report["counts"]["unique_image_sha256"] == EXPECTED_UNIQUE_IMAGE_HASHES
    # schema 断言（1.4）：重建派生表列与固定 schema 一致
    assert list(result.rebuilt_table.column_names) == list(
        _expected_rebuild_columns()
    )


@_XFAIL_FREEZE_REFRESH
@pytest.mark.skipif(not _HAS_FROZEN_DATA, reason="冻结版本指针不存在，跳过真实数据测试")
def test_real_rebuild_deterministic(tmp_path: Path) -> None:
    out_root = tmp_path
    run_id = "fvv-repro-test"
    r1 = run_rebuild(DATA_DIR, out_root, run_id=run_id)
    h1 = _sha256(r1.rebuilt_annotation_path)
    r2 = run_rebuild(DATA_DIR, out_root, run_id=run_id)
    h2 = _sha256(r2.rebuilt_annotation_path)
    assert h1 == h2
    assert r1.state_counts == r2.state_counts


def _expected_rebuild_columns() -> list[str]:
    from compsval.valuation import floorplan_value_validation as fvv

    return list(fvv.REBUILD_ANNOTATION_COLUMNS)


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# 组2：单位清单、候选池、切分、特征（合成）
# ---------------------------------------------------------------------------


def _synthetic_sale_table() -> pa.Table:
    """合成 ordinary_residential 风格成交表（source_record_id 维度）。"""
    return pa.table(
        {
            "source_record_id": [
                "A1",
                "B2",
                "C3",
                "D4",
                "E5",
                "F6",
            ],
            "sale_date": [
                date(2025, 8, 1),
                date(2025, 9, 1),
                date(2025, 10, 1),
                date(2025, 11, 1),
                date(2025, 12, 1),
                date(2026, 1, 1),
            ],
            "community_name": ["小区甲"] * 6,
            "transaction_area_sqm": [60.0, 80.0, 90.0, 100.0, 110.0, 120.0],
            "building_area_detail_sqm": [65.0, 85.0, 95.0, 105.0, 115.0, 125.0],
            "layout_raw": ["2室1厅", "3室1厅", "3室2厅", "3室2厅", "4室2厅", "4室2厅"],
        }
    )


def _synthetic_annotation() -> pa.Table:
    """合成标注表：task T1 混合状态；T2 ROOM_ONLY；T3 全 CONFLICT。"""
    return pa.table(
        {
            "ocr_task_id": ["T1", "T1", "T1", "T1", "T2", "T3"],
            "parse_state": ["ACCEPTED", "ACCEPTED", "ACCEPTED", "ROOM_ONLY", "ROOM_ONLY", "CONFLICT"],  # noqa: E501
            "standard_room_type": ["bedroom", "bedroom", "living_room", "kitchen", "bathroom", "bedroom"],  # noqa: E501
            "area_value": [12.0, 10.0, 20.0, None, None, 9.0],
        }
    )


def test_build_room_features_accepted_and_room_only() -> None:
    features = build_room_features(_synthetic_annotation())
    t1 = features["T1"]
    assert t1["accepted_room_count"] == 3
    assert t1["accepted_area_total"] == 42.0
    assert t1["accepted_area_composition"]["bedroom"] == round(22.0 / 42.0, 4)
    assert t1["room_only_count"] == 1
    assert t1["accepted_area_by_type"]["bedroom"] == 22.0
    t2 = features["T2"]
    assert t2["room_only_count"] == 1
    assert t2["accepted_area_total"] is None  # ROOM_ONLY 面积保持缺失
    t3 = features["T3"]
    assert t3["isolated_conflict"] == 1


def test_isolation_counts() -> None:
    counts = isolation_counts(_synthetic_annotation())
    assert counts["CONFLICT"] == 1
    assert counts["NEEDS_REVIEW"] == 0
    assert counts["OUT_OF_SCOPE"] == 0


def test_filter_candidates_strict_before_and_excludes_target() -> None:
    pool = _synthetic_sale_table()
    # 目标 = A1（2025-08-01，小区甲）；cutoff=10-01：B2（09-01）严格早于；
    # C3（10-01）为同日 → 排除并计入同日计数；A1 自身排除
    cand, excluded = filter_candidates(
        pool,
        community_name="小区甲",
        cutoff=date(2025, 10, 1),
        exclude_source_ids={"A1"},
    )
    ids = cand.column("source_record_id").to_pylist()
    assert ids == ["B2"]
    assert excluded == 1  # C3 与 cutoff 同日被排除


def test_filter_candidates_excludes_same_day() -> None:
    pool = _synthetic_sale_table()
    cand, excluded = filter_candidates(
        pool,
        community_name="小区甲",
        cutoff=date(2025, 10, 1),
        exclude_source_ids={"A1", "B2"},
    )
    ids = cand.column("source_record_id").to_pylist()
    assert ids == []  # C3 为同日 → 排除
    assert excluded == 1


def test_time_split_clusters_do_not_cross() -> None:
    units = _synthetic_units_table()
    dev, holdout = time_split_units(units, dev_ratio=0.5)
    assert set(dev).isdisjoint(set(holdout))
    assert len(dev) + len(holdout) == units.num_rows
    # 同一 cluster 内的记录必须全部同侧（不跨开发/确认集合）
    dev_set = set(dev)
    cluster_ids = units.column("cluster_id").to_pylist()
    source_ids = units.column("source_record_id").to_pylist()
    for cid in set(cluster_ids):
        members = {
            source_ids[j] for j in range(units.num_rows) if cluster_ids[j] == cid
        }
        assert members.issubset(dev_set) or members.isdisjoint(dev_set)


def _synthetic_units_table() -> pa.Table:
    rows = []
    for i, sid in enumerate(["A1", "B2", "C3", "D4", "E5", "F6"]):
        rows.append(
            {
                "source_record_id": sid,
                "cluster_id": f"cluster-{i + 1:03d}",
                "inventory_rows": 1,
                "is_duplicate": False,
                "sale_date": [date(2025, 8, 1), date(2025, 9, 1), date(2025, 10, 1), date(2025, 11, 1), date(2025, 12, 1), date(2026, 1, 1)][i],  # noqa: E501
                "community_name": "小区甲",
                "transaction_area_sqm": [60.0, 80.0, 90.0, 100.0, 110.0, 120.0][i],
                "building_area_detail_sqm": None,
                "layout_raw": None,
                "ocr_task_id": f"T{i + 1}",
                "ocr_accepted_count": 3 if i == 0 else 0,
                "ocr_room_only_count": 0,
                "ocr_conflict_count": 0,
                "ocr_needs_review_count": 0,
            }
        )
    return pa.Table.from_pylist(rows)


def test_build_area_quality_preserves_normal_difference() -> None:
    units = _synthetic_units_table()
    features = {"T1": {"accepted_area_total": 42.0}}
    quality = build_area_quality(units, features)
    row = quality.to_pylist()[0]
    assert row["ocr_area_total"] == 42.0
    assert row["abs_diff_ocr_transaction"] == round(42.0 - 60.0, 4)
    assert row["rel_diff_ocr_transaction"] == round((42.0 - 60.0) / 60.0, 4)
    # 正常差额不触发强制排除：无 extreme 标记（42 与 60 都在 (0,500]）
    assert row["extreme_flags"] is None


def test_align_comparable_features_marks_sources() -> None:
    pool = _synthetic_sale_table()
    aligned = align_comparable_features(
        pool,
        features_by_task={"T1": {"accepted_room_count": 3}},
        task_to_source={"A1": "T1"},
    )
    marks = aligned.column("feature_sources").to_pylist()
    assert marks[0] == "ocr,excel_area,excel_layout"
    assert all("excel_area" in m for m in marks[1:])


# ---------------------------------------------------------------------------
# 组3：相似度 overlay 与第一轮成对回放（合成）
# ---------------------------------------------------------------------------


def test_excel_layout_features_parses_rooms() -> None:
    feat = excel_layout_features("3室2厅")
    assert feat == {
        "room_types": ["bedroom"] * 3 + ["living_room"] * 2,
        "room_count": 5,
        "area_total": None,
    }
    assert excel_layout_features(None) is None
    assert excel_layout_features("车位") is None


def test_similarity_score_noop_without_common_fields() -> None:
    target = {"accepted_room_count": 3, "accepted_room_types": ["bedroom", "living_room"]}
    candidate = {}  # 无任何合格字段
    score, reason = similarity_overlay_score(target, candidate)
    assert score is None
    assert "无共同合格户型字段" in reason


def test_similarity_score_counts_rooms_and_types() -> None:
    target = {
        "accepted_room_count": 3,
        "accepted_room_types": ["bedroom", "bedroom", "living_room"],
    }
    candidate = {
        "room_types": ["bedroom", "bedroom", "living_room", "kitchen"],
        "room_count": 4,
    }
    score, reason = similarity_overlay_score(target, candidate)
    assert reason == ""
    assert 0.0 < score < 1.0
    # 房间数 3 vs 4 → 1 - 1/4 = 0.75；类型 jaccard {bed,liv}∩{bed,liv,kit}=2/3
    # → 平均 (0.75 + 2/3)/2 = 0.7083
    assert round(score, 4) == 0.7083


def test_similarity_perfect_match_is_one() -> None:
    target = {"accepted_room_count": 2, "accepted_room_types": ["bedroom", "living_room"]}
    candidate = {"room_types": ["bedroom", "living_room"], "room_count": 2}
    score, _ = similarity_overlay_score(target, candidate)
    assert score == 1.0


def _candidate_table_with_prices() -> pa.Table:
    return pa.table(
        {
            "source_record_id": ["C1", "C2", "C3"],
            "community_name": ["小区甲", "小区甲", "小区甲"],
            "sale_date": [date(2025, 7, 1), date(2025, 7, 2), date(2025, 7, 3)],
            "transaction_area_sqm": [80.0, 80.0, 80.0],
            "total_price_yuan": [4000000.0, 4400000.0, 4800000.0],
            "unit_price_observed": [50000.0, 55000.0, 60000.0],
        }
    )


def test_estimate_base_is_median_center() -> None:
    cand = _candidate_table_with_prices()
    base = estimate_with_candidates(cand, target_area=80.0)
    assert base["center"] == 55000.0
    assert base["status"] == "正式"
    assert base["lower"] == 50000.0
    assert base["upper"] == 60000.0


def test_estimate_trt_weighted_median() -> None:
    cand = _candidate_table_with_prices()
    # 权重偏向最低价候选 → 加权中位数应低于普通中位数
    trt = estimate_with_candidates(
        cand, target_area=80.0, similarity_weights=[1.0, 0.0, 0.0]
    )
    assert trt["center"] == 50000.0
    assert trt["lower"] == 50000.0  # 区间沿用基准分位口径


def test_estimate_insufficient_without_candidates() -> None:
    empty = _candidate_table_with_prices().slice(0, 0)
    result = estimate_with_candidates(empty, target_area=80.0)
    assert result["status"] == "信息不足"
    assert result["center"] is None


def test_run_round1_pair_pairs_and_same_pool() -> None:
    pool = _candidate_table_with_prices()
    unit = {
        "source_record_id": "T1",
        "sale_date": date(2025, 8, 1),
        "community_name": "小区甲",
        "transaction_area_sqm": 80.0,
        "actual_total_price": 4400000.0,
        "actual_unit_price": 55000.0,
        "ocr_task_id": "TT1",
    }
    row = run_round1_pair(
        unit=unit,
        pool=pool,
        features_by_task={
            "TT1": {"accepted_room_count": 2, "accepted_room_types": ["bedroom", "living_room"]}
        },
        task_to_source={"C1": "", "C2": "", "C3": ""},
        exclude_source_ids=set(),
        community_index={
            ("小区甲", 2025, 6): 52000.0,
            ("小区甲", 2025, 7): 55000.0,
        },
    )
    assert row["comp_n"] == 3
    assert row["base_center"] == 55000.0
    assert row["trt_center"] is not None
    assert row["base_status"] == row["trt_status"] == "正式"
    assert row["base_n"] == row["trt_n"] == 3


def test_run_round1_pair_overlay_index_with_duplicate_candidate_ids() -> None:
    """候选池含重复 source_record_id 时 overlay 权重按行对齐（S1 修复）。

    重复 id "A" 两行 layout 不同：修复前第二行经 ``list.index`` 错取第一行
    overlay（0.375），加权中位数漂移到 70000；修复后按行对齐（第二行 1.0），
    加权中位数为 60000。
    """
    pool = pa.table(
        {
            "source_record_id": ["A", "A", "B"],
            "community_name": ["小区甲"] * 3,
            "sale_date": [date(2025, 7, 1), date(2025, 7, 2), date(2025, 7, 3)],
            "transaction_area_sqm": [80.0] * 3,
            "layout_raw": ["1室0厅", "3室1厅", "3室1厅"],
            "total_price_yuan": [4000000.0, 4800000.0, 5600000.0],
            "unit_price_observed": [50000.0, 60000.0, 70000.0],
        }
    )
    unit = {
        "source_record_id": "T1",
        "sale_date": date(2025, 8, 1),
        "community_name": "小区甲",
        "transaction_area_sqm": 80.0,
        "layout_raw": "3室1厅",
        "actual_total_price": 4800000.0,
        "actual_unit_price": 60000.0,
        "ocr_task_id": "TT1",
    }
    row = run_round1_pair(
        unit=unit,
        pool=pool,
        features_by_task={
            "TT1": {
                "accepted_room_count": 4,
                "accepted_room_types": ["bedroom", "living_room"],
            }
        },
        task_to_source={},
        exclude_source_ids=set(),
        community_index={},
    )
    assert row["base_center"] == 60000.0
    assert row["trt_center"] == 60000.0
    assert row["feature_effective"] is True


# ---------------------------------------------------------------------------
# 组5：成对指标与三选一结论器（合成）
# ---------------------------------------------------------------------------


def _pair_table(actuals: list[float], base: list[float], trt: list[float]) -> pa.Table:
    return pa.table(
        {
            "source_record_id": [f"S{i}" for i in range(len(actuals))],
            "community_name": ["小区甲"] * len(actuals),
            "target_date": [date(2025, 8, 1)] * len(actuals),
            "target_area_sqm": [80.0] * len(actuals),
            "layout_raw": ["3室1厅"] * len(actuals),
            "actual_unit_price": actuals,
            "base_center": base,
            "base_lower": [0.9 * b for b in base],
            "base_upper": [1.1 * b for b in base],
            "base_status": ["正式"] * len(actuals),
            "base_n": [5] * len(actuals),
            "trt_center": trt,
            "trt_lower": [0.9 * b for b in trt],
            "trt_upper": [1.1 * b for b in trt],
            "trt_status": ["正式"] * len(actuals),
            "trt_n": [5] * len(actuals),
            "feature_effective": [True] * len(actuals),
            "feature_source": ["ocr"] * len(actuals),
            "no_op_reason": [None] * len(actuals),
            "comp_n": [10] * len(actuals),
            "comp_align_ocr_n": [8] * len(actuals),
            "comp_align_excel_n": [2] * len(actuals),
            "excluded_same_day_n": [1] * len(actuals),
        }
    )


def test_pair_metrics_same_target_set() -> None:
    actuals = [100.0, 100.0, 100.0, 100.0]
    base = [110.0, 90.0, 105.0, 95.0]
    trt = [105.0, 95.0, 102.0, 98.0]
    pairs = _pair_table(actuals, base, trt)
    m = compute_pair_metrics(pairs)
    assert m["n_targets"] == m["n_estimated"] == 4
    assert m["ape_delta_median"] < 0  # 处理组更接近
    assert m["trt_ape_median"] < m["base_ape_median"]
    assert m["trt_better_share"] == 1.0
    # 区间 ±10%：S1 base=90 上界 99 < actual 100 → 3/4 覆盖
    assert m["base_range_coverage"] == 0.75


def test_conclusion_candidate_integration() -> None:
    pairs = _pair_table([100.0, 100.0, 100.0, 100.0], [120.0, 120.0, 120.0, 120.0], [105.0, 105.0, 105.0, 105.0])  # noqa: E501
    m = compute_pair_metrics(pairs)
    conclusion, reasons = decide_round1_conclusion(
        m, min_estimated=4, improvement_threshold=0.05, max_coverage_regression=0.2
    )
    assert conclusion == "CANDIDATE_INTEGRATION"


def test_conclusion_no_value_when_worse() -> None:
    pairs = _pair_table([100.0, 100.0, 100.0, 100.0], [105.0, 105.0, 105.0, 105.0], [120.0, 120.0, 120.0, 120.0])  # noqa: E501
    m = compute_pair_metrics(pairs)
    conclusion, reasons = decide_round1_conclusion(
        m, min_estimated=4, improvement_threshold=0.05, max_coverage_regression=0.2
    )
    assert conclusion == "NO_VALUE"


def test_conclusion_evidence_insufficient_when_small() -> None:
    pairs = _pair_table([100.0, 100.0], [105.0, 105.0], [100.0, 100.0])
    m = compute_pair_metrics(pairs)
    conclusion, reasons = decide_round1_conclusion(
        m, min_estimated=30, improvement_threshold=0.05, max_coverage_regression=0.2
    )
    assert conclusion == "EVIDENCE_INSUFFICIENT"


# ---------------------------------------------------------------------------
# 组4：第二轮门禁（合成）
# ---------------------------------------------------------------------------


def test_round2_gate_blocked_on_no_value() -> None:
    triggered, reasons = decide_round2_gate(
        round1_conclusion="NO_VALUE",
        metrics={"n_targets": 10, "n_effective": 10, "comp_align_ocr_total": 0, "comp_align_excel_total": 10},  # noqa: E501
        min_effective_coverage=0.5,
        min_ocr_align_ratio=0.1,
    )
    assert triggered is False
    assert any("NO_VALUE" in r for r in reasons)


def test_round2_gate_blocked_on_insufficient_evidence() -> None:
    triggered, reasons = decide_round2_gate(
        round1_conclusion="EVIDENCE_INSUFFICIENT",
        metrics={},
        min_effective_coverage=0.5,
        min_ocr_align_ratio=0.1,
    )
    assert triggered is False
    assert any("EVIDENCE_INSUFFICIENT" in r for r in reasons)


def test_round2_gate_blocked_on_coverage() -> None:
    triggered, reasons = decide_round2_gate(
        round1_conclusion="CANDIDATE_INTEGRATION",
        metrics={"n_targets": 10, "n_effective": 3, "comp_align_ocr_total": 0, "comp_align_excel_total": 10},  # noqa: E501
        min_effective_coverage=0.5,
        min_ocr_align_ratio=0.1,
    )
    assert triggered is False
    assert any("特征生效覆盖" in r for r in reasons)


def test_round2_gate_blocked_on_ocr_align() -> None:
    triggered, reasons = decide_round2_gate(
        round1_conclusion="CANDIDATE_INTEGRATION",
        metrics={"n_targets": 10, "n_effective": 10, "comp_align_ocr_total": 0, "comp_align_excel_total": 100},  # noqa: E501
        min_effective_coverage=0.5,
        min_ocr_align_ratio=0.1,
    )
    assert triggered is False
    assert any("OCR 对齐占比" in r for r in reasons)


def test_round2_gate_triggered_when_satisfied() -> None:
    triggered, reasons = decide_round2_gate(
        round1_conclusion="CANDIDATE_INTEGRATION",
        metrics={"n_targets": 10, "n_effective": 10, "comp_align_ocr_total": 20, "comp_align_excel_total": 80},  # noqa: E501
        min_effective_coverage=0.5,
        min_ocr_align_ratio=0.1,
    )
    assert triggered is True


# ---------------------------------------------------------------------------
# 3.4 冻结配置哈希（合成）
# ---------------------------------------------------------------------------


def test_freeze_and_load_frozen_config_roundtrip(tmp_path: Path) -> None:
    path, config = freeze_round1_config(
        out_dir=tmp_path,
        run_id="test-run",
        min_estimated=30,
        improvement_threshold=0.02,
        max_coverage_regression=0.05,
        similarity_weights={"room_count": 1.0},
        min_effective_coverage=0.5,
        confirmed_by="tester",
    )
    assert path.is_file()
    loaded = load_frozen_round1_config(path)
    assert loaded["min_estimated"] == 30
    assert loaded["improvement_threshold"] == 0.02


def test_frozen_config_detects_tampering(tmp_path: Path) -> None:
    path, _ = freeze_round1_config(
        out_dir=tmp_path,
        run_id="test-run",
        min_estimated=30,
        improvement_threshold=0.02,
        max_coverage_regression=0.05,
        similarity_weights={},
        min_effective_coverage=0.5,
        confirmed_by="tester",
    )
    # 篡改门槛值后哈希校验必须失败
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["improvement_threshold"] = 0.99
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(RebuildGateError):
        load_frozen_round1_config(path)


# ---------------------------------------------------------------------------
# C3/C5 修复验证（完整比较法、严格退化、判别力）
# ---------------------------------------------------------------------------


def test_time_coefficient_basic() -> None:
    index = {
        ("小区甲", 2025, 1): 40000.0,
        ("小区甲", 2025, 2): 42000.0,
        ("小区甲", 2025, 3): 43000.0,
        ("小区甲", 2025, 4): 44000.0,
        ("小区甲", 2025, 5): 45000.0,
        ("小区甲", 2025, 6): 46000.0,
        ("小区甲", 2025, 7): 48000.0,
    }
    # 序列 7 个月 ≥ 6；成交月 (2025,3)，目标月 (2025,7) → 48000/43000
    coeff = time_coefficient(
        index, "小区甲", (2025, 3), (2025, 7)
    )
    assert coeff == round(48000.0 / 43000.0, 4)
    # 序列不足 6 个月 → None（不虚构系数）
    small = {k: v for k, v in index.items() if k[2] <= 4}
    assert time_coefficient(small, "小区甲", (2025, 1), (2025, 4)) is None


def test_room_count_sums_accepted_and_room_only() -> None:
    """ACCEPTED 与 ROOM_ONLY 同时存在时房间数求和（C3 修复）。"""
    target = {
        "accepted_room_count": 3,
        "room_only_count": 2,
        "accepted_room_types": ["bedroom", "living_room"],
        "room_only_types": ["kitchen"],
    }
    candidate = {"room_types": ["bedroom", "living_room", "kitchen"], "room_count": 3}
    score, _ = similarity_overlay_score(target, candidate)
    # 目标 5 间 vs 候选 3 间 → 1 - 2/5 = 0.6；类型 jaccard 3/3 = 1.0 → 均值 0.8
    assert round(score, 4) == 0.8


def test_full_comparable_trt_degrades_strictly_when_overlay_missing() -> None:
    """部分候选缺户型特征时 overlay 中性化 → 处理组严格等于基准（C3 修复）。"""
    pool = _candidate_table_with_prices()
    index = {("小区甲", 2025, 5): 50000.0, ("小区甲", 2025, 6): 55000.0}
    target = {
        "sale_date": date(2025, 8, 1),
        "community_name": "小区甲",
        "transaction_area_sqm": 80.0,
        "layout_raw": "3室1厅",
        "source_record_id": "T1",
    }
    base = estimate_full_comparable(pool, target=target, community_index=index)
    # overlay 全 None（无共同户型字段）→ 处理组权重 = 非户型相似度 = 基准权重
    trt = estimate_full_comparable(
        pool, target=target, community_index=index, overlay_weights=[None, None, None]
    )
    assert trt["center"] == base["center"]
    assert trt["status"] == base["status"] == "正式"


def test_conclusion_zero_delta_share_insufficient() -> None:
    """零差值占比超限 → EVIDENCE_INSUFFICIENT（C5 判别力检查）。"""
    pairs = _pair_table([100.0] * 8, [100.0] * 8, [100.0] * 8)  # 全部零差
    m = compute_pair_metrics(pairs)
    conclusion, reasons = decide_round1_conclusion(
        m, min_estimated=5, improvement_threshold=0.05, max_coverage_regression=0.2
    )
    assert conclusion == "EVIDENCE_INSUFFICIENT"
    assert any("零差值占比" in r for r in reasons)
