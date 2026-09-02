"""EXTFP4 生产批次核心层的离线测试（change extfp4-production-batch）。

覆盖：生产 profile 选择（小区/时间过滤 + 幂等 + 派生锚点 + 排除清单）、
冻结 OCR 配置与请求合同校验、磁盘门禁、批次合同/确认/投放门禁/成本台账、
§19.2 停止条件（下载回调 + 运行后评估）、已知限制引用校验、范围外标注隔离、
一致性与差异清单、完整性门禁、质量报告装配。全程不触网、不付费。
"""

from __future__ import annotations

import json
import shutil as shutil_mod
import time
from pathlib import Path

import httpx
import polars as pl
import pytest

from compsval.ingest.floorplan_batch_report import (
    ConsistencyEntry,
    IntegrityReport,
    OutOfScopeEntry,
    OutOfScopeRegistry,
    apply_out_of_scope_marks,
    build_batch_quality_report,
    build_consistency_report,
    check_batch_integrity,
    valid_denominator_annotations,
)
from compsval.ingest.floorplan_download import run_download
from compsval.ingest.floorplan_ocr import OcrRunRecord, OcrState, OcrTaskRecord
from compsval.ingest.floorplan_ocr_contract import OcrCostConfig, OcrRequestContract
from compsval.ingest.floorplan_production import (
    CHANGE_BUDGET_CAP_YUAN,
    CHANGE_REF,
    EXTFP6_CHANGE_BUDGET_CAP_YUAN,
    EXTFP6_CHANGE_REF,
    FULLHISTORY_FILTER_TEXT,
    FULLHISTORY_PROFILE,
    FULLHISTORY_RULE_TEXT,
    FULLHISTORY_RULE_VERSION,
    KNOWN_LIMITATIONS,
    PRODUCTION_DATE_MAX,
    PRODUCTION_DATE_MIN,
    PRODUCTION_PROFILE,
    AutoStopCondition,
    BatchConfirmation,
    BatchContract,
    BatchCostEntry,
    BatchGates,
    CostLedger,
    DispatchBlockedError,
    assert_dispatch_allowed,
    assert_frozen_request_contract,
    build_batch_contract,
    build_production_selection,
    check_disk_gate,
    check_known_limitations_cited,
    compute_record_ids_hash,
    evaluate_post_download,
    evaluate_post_ocr,
    evaluate_pre_dispatch_gates,
    make_download_stop_check,
    production_ocr_run_config,
    record_batch_cost,
    supported_community_names,
)
from compsval.ingest.floorplan_selection import build_selection
from compsval.ingest.floorplan_transcribe import AnnotationState, RoomAnnotationRecord

# ---------------------------------------------------------------------------
# fixtures：合成 staged parquet + 合成 entities 表
# ---------------------------------------------------------------------------

GOOD_URL = "http://ke-image.ljcdn.com/hdic-frame/{}.jpg?from=ke.com"


def _staged_row(
    row_number: int,
    record_id: str,
    community: str | None,
    sale_date: str | None,
    *,
    use: str = "普通住宅",
    url_status: str = "URLS_OK",
    candidate_count: int = 1,
    raw: str | None = None,
) -> dict[str, object]:
    return {
        "row_number": row_number,
        "source_record_id": record_id,
        "community_name": community,
        "sale_date": sale_date,
        "property_use_norm": use,
        "floorplan_url_status": url_status,
        "floorplan_candidate_count": candidate_count,
        "floorplan_url_list_raw": raw if raw is not None else f"[{GOOD_URL.format(record_id)!r}]",
    }


def _write_staged(path: Path) -> pl.DataFrame:
    rows = [
        # 命中：示例小区132窗口内（2 条）
        _staged_row(1, "G1", "示例小区132", "2025-08-01"),
        _staged_row(2, "G2", "示例小区132榕岸华庭(E区)", "2026-01-15"),  # 别名命中
        # 命中小区但窗口外
        _staged_row(3, "G3", "示例小区132", "2024-01-01"),
        # 窗口内但小区未命中（近似名缺口）
        _staged_row(4, "G4", "示例小区130A区", "2025-09-09"),
        _staged_row(5, "G5", "无关小区", "2026-02-02"),
        # 窗口外且未命中
        _staged_row(6, "G6", "无关小区", "2016-05-05"),
        # 基线外（非普通住宅）
        _staged_row(7, "G7", "示例小区132", "2025-10-10", use="商住两用"),
    ]
    df = pl.DataFrame(rows)
    df.write_parquet(path)
    return df


def _write_entities(entities_dir: Path) -> None:
    entities_dir.mkdir(parents=True, exist_ok=True)
    community = pl.DataFrame(
        {
            "community_id": ["C-XXXX0069", "C-XXXX0063"],
            "standard_name": ["示例小区132", "示例小区130"],
            "block": ["工业大道北", "滨江西"],
            "boundary_status": ["机器确认", "机器确认"],
        }
    )
    community.write_parquet(entities_dir / "community.parquet")
    alias = pl.DataFrame(
        {
            "community_id": ["C-XXXX0069"],
            "source_alias": ["示例小区132榕岸华庭(E区)"],
            "source_id": ["SRC-005"],
            "conflict_status": ["一致"],
        }
    )
    alias.write_parquet(entities_dir / "community_alias.parquet")


def _write_entities_with_pending_alias(entities_dir: Path) -> None:
    """EXTFP6 fixture：在基础 entities 上追加一致富基别名与待定别名（blocked 对照）。"""
    _write_entities(entities_dir)
    alias_path = entities_dir / "community_alias.parquet"
    alias = pl.read_parquet(alias_path)
    extra = pl.DataFrame(
        {
            "community_id": ["C-XXXX0063", "C-XXXX0063", "C-XXXX0069"],
            "source_alias": ["示例小区130A区", "示例小区130B区", "示例小区132榕岸"],
            "source_id": ["SRC-007", "SRC-007", "SRC-007"],
            "conflict_status": ["一致", "一致", "待定"],
        }
    )
    pl.concat([alias, extra]).write_parquet(alias_path)


@pytest.fixture()
def staged_path(tmp_path: Path) -> Path:
    path = tmp_path / "staged.parquet"
    _write_staged(path)
    return path


@pytest.fixture()
def entities_dir(tmp_path: Path) -> Path:
    entities = tmp_path / "entities"
    _write_entities(entities)
    return entities


# ---------------------------------------------------------------------------
# 任务 1.1：生产 profile 选择（小区/时间过滤 + 幂等 + 派生锚点 + 排除清单）
# ---------------------------------------------------------------------------


class TestProductionSelection:
    def test_subset_selection_counts_and_anchor(
        self, staged_path: Path, entities_dir: Path, tmp_path: Path
    ) -> None:
        full = build_selection(staged_path)
        full_json = tmp_path / "selection_manifest.json"
        full_json.write_text(full.model_dump_json(indent=2), encoding="utf-8")

        manifest, exclusion = build_production_selection(
            staged_path,
            entities_dir,
            out_json=tmp_path / "production_manifest.json",
            exclusion_out_json=tmp_path / "production_exclusion_report.json",
            full_manifest_path=full_json,
        )
        # 命中：G1（标准名）+ G2（别名）；G3 窗口外；G4/G5 窗口内未命中；G6 双不命中
        assert manifest.record_count == 2
        assert manifest.asset_count == 2
        assert manifest.matched_community_counts == {"C-XXXX0069": 2}
        assert manifest.baseline_record_count == 6  # G7 非普通住宅不计基线
        assert manifest.baseline_record_ids_hash == full.record_ids_hash
        assert exclusion.excluded_matched_out_of_window == 1  # G3
        assert exclusion.excluded_unmatched_in_window == 2  # G4/G5
        assert exclusion.excluded_unmatched_outside_window == 1  # G6
        assert any(
            "示例小区130A区" in n["community_name"] for n in exclusion.unmatched_in_window_top_names
        )
        # manifest 授权字段
        assert manifest.change_budget_cap_yuan == CHANGE_BUDGET_CAP_YUAN
        assert manifest.attempt_cap == 3  # ceil(2 × 1.10)
        assert manifest.budget_expected_yuan == round(2 * 0.001094, 4)
        assert manifest.selection_rule_version == "EXTFP4-SELECT-1.0"
        assert manifest.workpackage_ref == "EXTFP4"

    def test_idempotent_and_sidecar(
        self, staged_path: Path, entities_dir: Path, tmp_path: Path
    ) -> None:
        out1 = tmp_path / "a" / "production_manifest.json"
        out2 = tmp_path / "b" / "production_manifest.json"
        m1, _ = build_production_selection(staged_path, entities_dir, out_json=out1)
        m2, _ = build_production_selection(staged_path, entities_dir, out_json=out2)
        assert m1.record_ids_hash == m2.record_ids_hash
        assert m1.model_dump() == m2.model_dump()
        sidecar = out1.with_name(out1.name + ".sha256")
        assert sidecar.is_file()
        assert m1.record_ids_hash == compute_record_ids_hash(
            [e.source_record_id for e in m1.records]
        )

    def test_anchor_mismatch_rejects(
        self, staged_path: Path, entities_dir: Path, tmp_path: Path
    ) -> None:
        full = build_selection(staged_path)
        full_json = tmp_path / "selection_manifest.json"
        data = full.model_dump()
        data["record_count"] = data["record_count"] + 1  # 篡改基线
        full_json.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        with pytest.raises(ValueError, match="派生锚点不一致"):
            build_production_selection(
                staged_path,
                entities_dir,
                out_json=tmp_path / "p.json",
                full_manifest_path=full_json,
            )

    def test_full_selection_unchanged_by_production_params(
        self, staged_path: Path, tmp_path: Path
    ) -> None:
        m1 = build_selection(staged_path)
        m2 = build_selection(
            staged_path,
            selection_rule_version=m1.selection_rule_version,
            selection_rule_text=m1.selection_rule_text,
            filter_condition_text=m1.filter_condition,
            community_names=None,
            date_min=None,
            date_max=None,
        )
        assert m1.record_ids_hash == m2.record_ids_hash
        assert m1.record_count == m2.record_count == 6

    def test_community_filter_requires_column(self, entities_dir: Path, tmp_path: Path) -> None:
        bare = tmp_path / "bare.parquet"
        pl.DataFrame({"row_number": [1], "source_record_id": ["X"]}).write_parquet(bare)
        with pytest.raises(Exception):  # noqa: B017 - polars 缺列或显式 ValueError
            build_production_selection(bare, entities_dir, out_json=tmp_path / "p.json")

    def test_supported_community_names_alias(self, entities_dir: Path) -> None:
        names = supported_community_names(entities_dir)
        assert names["示例小区132"] == "C-XXXX0069"
        assert names["示例小区132榕岸华庭(E区)"] == "C-XXXX0069"
        assert "示例小区130A区" not in names  # 别名缺口如实暴露

    def test_production_dedup_records_and_disclosure(
        self, tmp_path: Path, entities_dir: Path
    ) -> None:
        """生产 profile 记录级去重（SUGGESTION ②）：同 source_record_id 多行只保留一行并披露。"""
        dup_path = tmp_path / "dup_staged.parquet"
        df = pl.DataFrame(
            [
                _staged_row(1, "D1", "示例小区132", "2025-08-01"),
                _staged_row(2, "D2", "示例小区132", "2025-08-02"),
                _staged_row(3, "D1", "示例小区132", "2025-08-03"),  # 同 id 第二行（窗口内命中）
            ]
        )
        df.write_parquet(dup_path)
        full = build_selection(dup_path)
        full_json = tmp_path / "full.json"
        full_json.write_text(full.model_dump_json(indent=2), encoding="utf-8")

        manifest, exclusion = build_production_selection(
            dup_path,
            entities_dir,
            out_json=tmp_path / "prod.json",
            exclusion_out_json=tmp_path / "excl.json",
            full_manifest_path=full_json,
        )
        # 去重后：D1 一行 + D2 一行 → 2 记录/2 资产
        assert manifest.record_count == 2
        assert manifest.asset_count == 2
        assert manifest.dedupe_record_count == 1
        assert manifest.dedupe_record_sample == ["D1"]
        # 披露进排除报告 + notes
        assert exclusion.deduped_record_count == 1
        assert exclusion.deduped_record_sample == ["D1"]
        assert any("去重" in n for n in exclusion.notes)
        # 锚点仍为未去重基线（3 行，全量锚点核对保持原口径）
        assert manifest.baseline_record_count == 3
        assert manifest.baseline_record_ids_hash == full.record_ids_hash
        # 保留行 = row_number 最小（D1 首行 URL）
        d1 = [e for e in manifest.records if e.source_record_id == "D1"]
        assert len(d1) == 1
        assert manifest.matched_community_counts == {"C-XXXX0069": 2}


# ---------------------------------------------------------------------------
# 任务 1.2（EXTFP6）：全历史 profile + 一致别名匹配 + change_ref/预算继承
# ---------------------------------------------------------------------------


class TestFullHistorySelection:
    @pytest.fixture()
    def entities_fuji(self, tmp_path: Path) -> Path:
        entities = tmp_path / "entities_fuji"
        _write_entities_with_pending_alias(entities)
        return entities

    @pytest.fixture()
    def staged_full(self, tmp_path: Path) -> Path:
        """在基础 staged 上加：富基A区窗外行 + 待定别名行（G9）。"""
        path = tmp_path / "staged_full.parquet"
        rows = _write_staged(path)
        extra = pl.DataFrame(
            [
                _staged_row(8, "G8", "示例小区130A区", "2021-03-03"),  # 一致别名，窗外
                _staged_row(9, "G9", "示例小区132榕岸", "2021-06-06"),  # 待定别名，blocked
            ]
        )
        pl.concat([rows, extra]).write_parquet(path)
        return path

    def _fullhistory_kwargs(self) -> dict[str, object]:
        return {
            "date_min": None,
            "date_max": None,
            "profile": FULLHISTORY_PROFILE,
            "change_ref": EXTFP6_CHANGE_REF,
            "workpackage_ref": "EXTFP6",
            "change_budget_cap_yuan": EXTFP6_CHANGE_BUDGET_CAP_YUAN,
            "selection_rule_version": FULLHISTORY_RULE_VERSION,
            "selection_rule_text": FULLHISTORY_RULE_TEXT,
            "filter_condition_text": FULLHISTORY_FILTER_TEXT,
        }

    def test_fullhistory_matches_consistent_alias_and_ignores_pending(
        self, staged_full: Path, entities_fuji: Path, tmp_path: Path
    ) -> None:
        manifest, exclusion = build_production_selection(
            staged_full,
            entities_fuji,
            out_json=tmp_path / "prod_fh.json",
            exclusion_out_json=tmp_path / "excl_fh.json",
            **self._fullhistory_kwargs(),  # type: ignore[arg-type]
        )
        # 命中：G1（标准名）+ G2（一致别名）+ G3（窗外但全历史入池）
        #       + G4/G8（富基A区窗内/窗外，经一致别名命中）
        # G9 待定别名 blocked
        assert manifest.record_count == 5
        assert manifest.change_ref == EXTFP6_CHANGE_REF
        assert manifest.production_profile == FULLHISTORY_PROFILE
        assert manifest.change_budget_cap_yuan == EXTFP6_CHANGE_BUDGET_CAP_YUAN == 10.0
        assert manifest.date_window_min == manifest.date_window_max == "FULL_HISTORY"
        assert manifest.selection_rule_version == "EXTFP6-SELECT-1.0"
        assert manifest.workpackage_ref == "EXTFP6"
        assert manifest.matched_community_counts == {
            "C-XXXX0063": 2,  # G4 + G8 富基A区
            "C-XXXX0069": 3,  # G1 + G2 + G3
        }
        # 全历史口径：matched 无窗口外概念
        assert exclusion.excluded_matched_out_of_window == 0
        # 未匹配（全体基线口径）：G5/G6 无关小区 + G9 待定别名 blocked
        assert exclusion.excluded_unmatched_in_window == 3
        assert exclusion.change_ref == EXTFP6_CHANGE_REF
        assert any("全历史" in n for n in exclusion.notes)
        top = {n["community_name"] for n in exclusion.unmatched_in_window_top_names}
        assert "示例小区132榕岸" in top  # 待定别名未参与匹配，如实进排除清单
        # 派生清单实际成交时间范围如实披露（不限窗的佐证）
        assert manifest.date_range_min is not None and manifest.date_range_max is not None

    def test_extfp4_default_params_unchanged(
        self, staged_path: Path, entities_dir: Path, tmp_path: Path
    ) -> None:
        """默认参数派生行为与 EXTFP4 口径完全一致（change_ref/日期窗/规则版本）。"""
        manifest, exclusion = build_production_selection(
            staged_path,
            entities_dir,
            out_json=tmp_path / "prod_default.json",
        )
        assert manifest.change_ref == CHANGE_REF
        assert manifest.production_profile == PRODUCTION_PROFILE
        assert manifest.date_window_min == PRODUCTION_DATE_MIN
        assert manifest.date_window_max == PRODUCTION_DATE_MAX
        assert manifest.change_budget_cap_yuan == CHANGE_BUDGET_CAP_YUAN
        assert manifest.selection_rule_version == "EXTFP4-SELECT-1.0"
        assert manifest.workpackage_ref == "EXTFP4"
        assert exclusion.change_ref == CHANGE_REF
        assert not any("全历史" in n for n in exclusion.notes)

    def test_extfp6_contract_and_dispatch_gate(
        self, staged_full: Path, entities_fuji: Path, tmp_path: Path
    ) -> None:
        manifest_path = tmp_path / "prod_fh.json"
        manifest, _ = build_production_selection(
            staged_full,
            entities_fuji,
            out_json=manifest_path,
            **self._fullhistory_kwargs(),  # type: ignore[arg-type]
        )
        authorization = (
            "openspec/changes/add-extfp6-full-history-ocr-batch"
            "（提案 + 用户实施授权 + 逐批批次确认）"
        )
        contract = build_batch_contract(manifest, manifest_path, authorization_ref=authorization)
        assert contract.change_ref == EXTFP6_CHANGE_REF
        assert contract.gates.change_budget_cap_yuan == 10.0
        assert contract.authorization_ref == authorization
        sha = contract.selection_manifest_sha256
        # 同 change 确认 → 放行
        confirmation = _make_confirmation(
            change_ref=EXTFP6_CHANGE_REF,
            manifest_sha256=sha,
            task_count_cap=manifest.asset_count,
            batch_amount_cap_yuan=1.5,
        )
        assert_dispatch_allowed(
            contract,
            confirmation,
            planned_images=manifest.asset_count,
            manifest_sha256=sha,
        )
        # 跨 change 确认 → 拒绝
        wrong = _make_confirmation(
            manifest_sha256=sha,
            batch_amount_cap_yuan=1.5,
        )
        with pytest.raises(DispatchBlockedError, match="不属于本 change"):
            assert_dispatch_allowed(
                contract,
                wrong,
                planned_images=1,
                manifest_sha256=sha,
            )

    def test_supported_community_names_pending_blocked(self, entities_fuji: Path) -> None:
        names = supported_community_names(entities_fuji)
        assert names["示例小区130A区"] == "C-XXXX0063"
        assert names["示例小区130B区"] == "C-XXXX0063"
        assert "示例小区132榕岸" not in names  # 待定别名 blocked


# ---------------------------------------------------------------------------
# 任务 4.1：冻结 OCR 配置与请求合同校验
# ---------------------------------------------------------------------------


class TestFrozenOcrConfig:
    def test_production_config_frozen_request_and_gates(self) -> None:
        cfg = production_ocr_run_config(max_images=300, hard_cap_yuan=0.5)
        assert cfg.request.model == "qwen-vl-ocr-2025-11-20"
        assert cfg.request.task == "advanced_recognition"
        assert (cfg.request.min_pixels, cfg.request.max_pixels) == (3072, 8388608)
        assert cfg.request.enable_rotate is False and cfg.request.stream is False
        assert cfg.cost.max_retries == min(50, 300 // 10)
        assert cfg.cost.baseline_cost_per_image_yuan == 0.004
        assert cfg.cost.pause_ratio_threshold == 1.2
        assert_frozen_request_contract(cfg)  # 不抛错

    def test_deviation_rejected(self) -> None:
        cfg = production_ocr_run_config(max_images=10, hard_cap_yuan=0.1)
        deviated = cfg.model_copy(
            update={"request": cfg.request.model_copy(update={"enable_rotate": True})}
        )
        with pytest.raises(ValueError, match="偏离 frozen_extfp4_config"):
            assert_frozen_request_contract(deviated)

    def test_request_contract_only_contract_type(self) -> None:
        contract = OcrRequestContract()
        cost = OcrCostConfig(hard_cap_yuan=1.0, max_images=5)
        assert contract.model == "qwen-vl-ocr-2025-11-20"
        assert cost.max_retries >= 1


# ---------------------------------------------------------------------------
# 任务 1.3/3.3：磁盘门禁
# ---------------------------------------------------------------------------


class TestDiskGate:
    def test_ok_when_free_exceeds_multiplier(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeUsage:
            free = 10_000_000
            total = 20_000_000
            used = 10_000_000

        monkeypatch.setattr(shutil_mod, "disk_usage", lambda _p: FakeUsage())
        result = check_disk_gate(tmp_path, required_bytes=1_000_000)
        assert result.ok is True
        assert result.required_bytes == int(1_000_000 * 1.5)

    def test_insufficient_stops(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeUsage:
            free = 10
            total = 100
            used = 90

        monkeypatch.setattr(shutil_mod, "disk_usage", lambda _p: FakeUsage())
        result = check_disk_gate(tmp_path, required_bytes=1_000_000)
        assert result.ok is False
        assert result.required_bytes == int(1_000_000 * 1.5)


# ---------------------------------------------------------------------------
# 任务 1.4/2.1：批次合同 / 批次确认 / 投放门禁 / 成本台账
# ---------------------------------------------------------------------------


def _make_contract(tmp_path: Path, manifest_path: Path) -> BatchContract:
    from compsval.ingest.floorplan_production import ProductionSelectionManifest

    manifest = ProductionSelectionManifest(
        selection_rule_version="EXTFP4-SELECT-1.0",
        selection_rule_text="t",
        geoscope="g",
        filter_condition="f",
        record_count=2,
        asset_count=10,
        record_ids_hash="hash",
        estimated_download_bytes=1_000,
        storage_cap_bytes=1_500,
        budget_cap_yuan=0.03,
        avg_bytes_estimate=100,
        budget_expected_yuan=0.01,
        attempt_cap=11,
        baseline_record_count=2,
        baseline_record_ids_hash="base-hash",
    )
    manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    contract = build_batch_contract(manifest, manifest_path)
    contract_path = tmp_path / "batch_contract.json"
    contract_path.write_text(contract.model_dump_json(indent=2), encoding="utf-8")
    return contract


def _make_confirmation(**overrides: object) -> BatchConfirmation:
    base = dict(
        batch_id="batch-001",
        confirmed_at="2026-08-30T12:00:00+00:00",
        decision="approved",
        commit_sha="11edf14",
        manifest_sha256="a" * 64,
        task_count_cap=10,
        batch_amount_cap_yuan=0.5,
        cumulative_cost_before_yuan=0.0,
    )
    base.update(overrides)
    return BatchConfirmation(**base)  # type: ignore[arg-type]


class TestDispatchGate:
    def test_happy_path(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        confirmation = _make_confirmation(manifest_sha256=contract.selection_manifest_sha256)
        assert_dispatch_allowed(
            contract,
            confirmation,
            planned_images=10,
            manifest_sha256=contract.selection_manifest_sha256,
        )

    def test_missing_confirmation_file(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        with pytest.raises(DispatchBlockedError, match="不存在"):
            assert_dispatch_allowed(
                contract,
                __import__(
                    "compsval.ingest.floorplan_production",
                    fromlist=["load_batch_confirmation"],
                ).load_batch_confirmation(tmp_path / "nope.json"),
                planned_images=1,
                manifest_sha256=contract.selection_manifest_sha256,
            )

    def test_not_approved_rejected(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        confirmation = _make_confirmation(
            decision="pending", manifest_sha256=contract.selection_manifest_sha256
        )
        with pytest.raises(DispatchBlockedError, match="未获批准"):
            assert_dispatch_allowed(
                contract,
                confirmation,
                planned_images=1,
                manifest_sha256=contract.selection_manifest_sha256,
            )

    def test_manifest_sha_mismatch(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        confirmation = _make_confirmation(manifest_sha256="b" * 64)
        with pytest.raises(DispatchBlockedError, match="SHA256"):
            assert_dispatch_allowed(
                contract,
                confirmation,
                planned_images=1,
                manifest_sha256=contract.selection_manifest_sha256,
            )

    def test_task_count_over_cap(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        sha = contract.selection_manifest_sha256
        confirmation = _make_confirmation(manifest_sha256=sha, task_count_cap=5)
        with pytest.raises(DispatchBlockedError, match="上限"):
            assert_dispatch_allowed(contract, confirmation, planned_images=6, manifest_sha256=sha)

    def test_amount_over_change_cap(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        sha = contract.selection_manifest_sha256
        confirmation = _make_confirmation(
            manifest_sha256=sha, batch_amount_cap_yuan=CHANGE_BUDGET_CAP_YUAN + 1.0
        )
        with pytest.raises(DispatchBlockedError, match="硬上限"):
            assert_dispatch_allowed(contract, confirmation, planned_images=1, manifest_sha256=sha)

    def test_ledger_mismatch(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        sha = contract.selection_manifest_sha256
        confirmation = _make_confirmation(manifest_sha256=sha, cumulative_cost_before_yuan=1.0)
        ledger = CostLedger(entries=[])
        with pytest.raises(DispatchBlockedError, match="不一致"):
            assert_dispatch_allowed(
                contract,
                confirmation,
                planned_images=1,
                manifest_sha256=sha,
                ledger=ledger,
            )


class TestCostLedger:
    def test_record_and_cap(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        ledger_path = tmp_path / "cost_ledger.json"
        ledger = record_batch_cost(
            ledger_path,
            BatchCostEntry(
                batch_id="b1",
                stage="ocr",
                cost_yuan=0.5,
                images=100,
                attempts=105,
                recorded_at="t",
            ),
        )
        assert ledger.total_cost_yuan == 0.5
        reloaded = record_batch_cost(
            ledger_path,
            BatchCostEntry(
                batch_id="b2", stage="ocr", cost_yuan=0.4, images=80, attempts=82, recorded_at="t2"
            ),
        )
        assert reloaded.total_cost_yuan == 0.9
        with pytest.raises(DispatchBlockedError, match="超出 change 级硬上限"):
            record_batch_cost(
                ledger_path,
                BatchCostEntry(
                    batch_id="b3",
                    stage="ocr",
                    cost_yuan=CHANGE_BUDGET_CAP_YUAN,
                    images=1,
                    attempts=1,
                    recorded_at="t3",
                ),
                contract=contract,
            )


# ---------------------------------------------------------------------------
# 任务 7.1：§19.2 停止条件
# ---------------------------------------------------------------------------


class _FakeTask:
    def __init__(self, last_error: str | None) -> None:
        self.last_error = last_error


class TestStopConditions:
    def test_download_stop_check_consecutive_errors(self) -> None:
        check, collect = make_download_stop_check(max_consecutive_errors=3)
        assert check(_FakeTask(None)) is None
        assert check(_FakeTask("http-500")) is None
        assert check(_FakeTask("http-500")) is None
        reason = check(_FakeTask("ReadTimeout"))
        assert reason is not None
        triggers = collect()
        assert triggers[0].condition == AutoStopCondition.ERROR_PLACEHOLDER_MIME_ANOMALY.value

    def test_download_stop_check_access_control(self) -> None:
        check, collect = make_download_stop_check(max_consecutive_errors=10)
        reason = check(_FakeTask("http-403"))
        assert reason is not None
        assert collect()[0].condition == AutoStopCondition.SOURCE_ACCESS_CONTROL.value

    def test_download_stop_check_resets_on_success(self) -> None:
        check, collect = make_download_stop_check(max_consecutive_errors=2)
        check(_FakeTask("http-500"))
        assert check(_FakeTask(None)) is None
        check(_FakeTask("http-500"))
        assert check(_FakeTask(None)) is None  # 重置后不再触发
        assert collect() == []

    def test_post_download_ratio_and_access(self) -> None:
        record = type(
            "R",
            (),
            {
                "tasks": [
                    _FakeTask("http-403"),
                    _FakeTask("http-500"),
                    _FakeTask(None),
                ]
            },
        )()
        triggers = evaluate_post_download(record, max_failed_ratio=0.5)
        conditions = {t.condition for t in triggers}
        assert AutoStopCondition.SOURCE_ACCESS_CONTROL.value in conditions
        assert AutoStopCondition.ERROR_PLACEHOLDER_MIME_ANOMALY.value in conditions

    def test_post_ocr_model_mismatch_and_cost(self) -> None:
        task_ok = _ocr_task("t1", OcrState.OCR_SUCCEEDED)
        task_bad = _ocr_task("t2", OcrState.NEEDS_REVIEW, error_code="model_mismatch")
        record = OcrRunRecord(
            ocr_run_id="r",
            asset_manifest_ref="m",
            sourced=True,
            state_counts={},
            cost={"limit_hit": "cost_cap_yuan", "total_cost_yuan": 1.0, "hard_cap_yuan": 1.0},
            tasks=[task_ok, task_bad],
            created_at="c",
            updated_at="u",
            run_dir="d",
        )
        triggers = evaluate_post_ocr(record)
        conditions = {t.condition for t in triggers}
        assert AutoStopCondition.MODEL_MISMATCH.value in conditions
        assert AutoStopCondition.COST_CAP.value in conditions

    def test_pre_dispatch_scope_and_disk(self, tmp_path: Path) -> None:
        from compsval.ingest.floorplan_selection import SelectionManifest

        manifest = SelectionManifest(
            selection_rule_version="v",
            selection_rule_text="t",
            geoscope="g",
            filter_condition="f",
            record_count=0,
            asset_count=0,
            record_ids_hash="h",
            estimated_download_bytes=10,
            storage_cap_bytes=15,
            budget_cap_yuan=0.0,
            avg_bytes_estimate=1,
            forbidden_domain_count=1,
            forbidden_domains=["evil.example.com"],
        )
        disk = check_disk_gate(tmp_path, required_bytes=1)
        triggers = evaluate_pre_dispatch_gates(
            None,  # type: ignore[arg-type]
            manifest,
            disk,
        )
        assert triggers[0].condition == AutoStopCondition.SCOPE_VIOLATION.value

        class FakeUsage:
            free = 0
            total = 10
            used = 10

        import compsval.ingest.floorplan_production as fp

        original = fp.shutil.disk_usage
        fp.shutil.disk_usage = lambda _p: FakeUsage()  # type: ignore[assignment]
        try:
            disk_bad = check_disk_gate(tmp_path, required_bytes=100)
        finally:
            fp.shutil.disk_usage = original
        triggers = evaluate_pre_dispatch_gates(None, manifest, disk_bad)  # type: ignore[arg-type]
        assert any(t.condition == AutoStopCondition.DISK_INSUFFICIENT.value for t in triggers)


def _ocr_task(
    task_id: str,
    state: OcrState,
    *,
    error_code: str | None = None,
    raw_name: str | None = None,
    raw_sha: str | None = None,
    raw_file_sha: str | None = None,
) -> OcrTaskRecord:
    return OcrTaskRecord(
        ocr_task_id=task_id,
        ocr_run_id="r",
        asset_id=f"asset-{task_id}",
        image_sha256="img",
        request_hash="rh",
        state=state,
        error_code=error_code,
        raw_response_path=raw_name,
        raw_response_sha256=raw_sha,
        raw_response_file_sha256=raw_file_sha,
        model_requested="qwen-vl-ocr-2025-11-20",
        model_returned="qwen-vl-ocr-2025-11-20",
    )


# ---------------------------------------------------------------------------
# 任务 6.3：已知限制引用
# ---------------------------------------------------------------------------


class TestKnownLimitations:
    def test_all_cited_passes(self) -> None:
        text = "结论引用：" + "；".join(f"{kid} {note}" for kid, note in KNOWN_LIMITATIONS)
        missing, forbidden = check_known_limitations_cited(text)
        assert missing == []
        assert forbidden == []

    def test_missing_citation_detected(self) -> None:
        text = "仅引用 KL-1 与 KL-2"
        missing, _ = check_known_limitations_cited(text)
        assert "KL-3" in missing and "KL-4" in missing

    def test_forbidden_phrase_detected(self) -> None:
        text = "本次批次证明 H3 已获正式认证"
        _, forbidden = check_known_limitations_cited(text)
        assert forbidden


# ---------------------------------------------------------------------------
# 任务 5.1/5.2：范围外标注隔离 + 一致性差异清单
# ---------------------------------------------------------------------------


def _annotation(task_id: str, state: str = "ACCEPTED") -> RoomAnnotationRecord:
    return RoomAnnotationRecord(
        annotation_id=f"ann-{task_id}",
        ocr_run_id="r",
        ocr_task_id=task_id,
        parse_state=state,
    )


class _FakeAsset:
    def __init__(self, asset_id: str, source_record_id: str) -> None:
        self.asset_id = asset_id
        self.source_record_id = source_record_id


class TestOutOfScope:
    def test_registry_add_conflict_and_roundtrip(self, tmp_path: Path) -> None:
        registry = OutOfScopeRegistry()
        entry = OutOfScopeEntry(
            source_record_id="S1", reason="多层户型", judged_by="user", judged_at="t"
        )
        registry.add(entry)
        registry.add(entry.model_copy())  # 相同条目幂等
        with pytest.raises(ValueError, match="冲突"):
            registry.add(
                OutOfScopeEntry(
                    source_record_id="S1", reason="其他", judged_by="user", judged_at="t2"
                )
            )
        path = tmp_path / "out_of_scope_registry.json"
        registry.save(path)
        loaded = OutOfScopeRegistry.load(path)
        assert loaded.is_out_of_scope("S1") is not None
        assert OutOfScopeRegistry.load(tmp_path / "none.json").entries == []

    def test_apply_marks_and_valid_denominator(self) -> None:
        ocr_tasks = [
            _ocr_task("t1", OcrState.OCR_SUCCEEDED),
            _ocr_task("t2", OcrState.OCR_SUCCEEDED),
        ]
        assets = {
            "asset-t1": _FakeAsset("asset-t1", "S1"),
            "asset-t2": _FakeAsset("asset-t2", "S2"),
        }
        registry = OutOfScopeRegistry(
            entries=[
                OutOfScopeEntry(
                    source_record_id="S1", reason="多层户型", judged_by="user", judged_at="t"
                )
            ]
        )
        annotations = [_annotation("t1"), _annotation("t2")]
        marked, counts = apply_out_of_scope_marks(
            annotations, ocr_tasks=ocr_tasks, assets_by_asset_id=assets, registry=registry
        )
        assert marked[0].parse_state == AnnotationState.OUT_OF_SCOPE.value
        assert "out_of_scope:多层户型" in (marked[0].isolation_reason or "")
        assert marked[1].parse_state == AnnotationState.ACCEPTED.value
        assert counts[AnnotationState.OUT_OF_SCOPE.value] == 1
        valid = valid_denominator_annotations(marked)
        assert len(valid) == 1 and valid[0].ocr_task_id == "t2"


class TestConsistencyReport:
    def test_entries_and_integrity(self, tmp_path: Path) -> None:
        good = tmp_path / "raw_good.json"
        payload = json.dumps({"output": {"choices": []}}).encode("utf-8")
        good.write_bytes(payload)
        import hashlib

        sha = hashlib.sha256(payload).hexdigest()
        tasks = [
            _ocr_task(
                "ok",
                OcrState.OCR_SUCCEEDED,
                raw_name="raw_good.json",
                raw_sha=sha,
            ),
            _ocr_task(
                "partial",
                OcrState.OCR_PARTIAL,
                error_code="finish_reason_length",
                raw_name="raw_good.json",
                raw_sha=sha,
            ),
            _ocr_task("failed", OcrState.OCR_FAILED, error_code="network_error"),
            # 成功但原始响应文件缺失 → 完整性缺口
            _ocr_task("gone", OcrState.OCR_SUCCEEDED, raw_name="nope.json", raw_sha="x"),
        ]
        record = OcrRunRecord(
            ocr_run_id="r",
            asset_manifest_ref="m",
            sourced=True,
            state_counts={},
            tasks=tasks,
            created_at="c",
            updated_at="u",
            run_dir=str(tmp_path),
        )
        entries = build_consistency_report(record, tmp_path)
        kinds = {e.kind for e in entries}
        assert "partial" in kinds
        assert "ocr_failed" in kinds
        assert "missing_raw" in kinds  # gone 任务的原始响应缺失

        annotations = [_annotation("ok")]
        integrity = check_batch_integrity(
            selection_manifest=_FakeSelection(asset_count=3),
            download_record=type("D", (), {"tasks": []})(),
            ocr_record=record,
            annotations=annotations,
            run_dir=tmp_path,
        )
        assert integrity.passed is False
        assert any("数量不一致" in g for g in integrity.gaps)
        assert any("原始响应缺失" in g for g in integrity.gaps)

    def test_dual_caliber_primary_gate_and_disclosure(self, tmp_path: Path) -> None:
        """新运行双口径（SUGGESTION ①）：落盘口径主核对通过；原始口径差异（CRLF）仅披露计数。"""
        import hashlib

        payload = json.dumps({"output": {"choices": []}}, indent=2).encode(
            "utf-8"
        )  # 原始响应字节（含 \n）
        raw_path = tmp_path / "raw_dual.json"
        # 模拟落盘：CRLF 翻译 + 净化引入一处空格（`[]`→`[ ]`），归一化后仍与原始字节不同
        raw_path.write_bytes(payload.replace(b"\n", b"\r\n").replace(b"[]", b"[ ]"))
        tasks = [
            _ocr_task(
                "d1",
                OcrState.OCR_SUCCEEDED,
                raw_name="raw_dual.json",
                raw_sha=hashlib.sha256(payload).hexdigest(),  # 原始字节口径
                raw_file_sha=hashlib.sha256(raw_path.read_bytes()).hexdigest(),  # 落盘口径
            ),
        ]
        record = OcrRunRecord(
            ocr_run_id="r",
            asset_manifest_ref="m",
            sourced=True,
            state_counts={},
            tasks=tasks,
            created_at="c",
            updated_at="u",
            run_dir=str(tmp_path),
        )
        # 一致性：落盘口径匹配 → 无 sha_mismatch（原始口径差异不计差异）
        entries = build_consistency_report(record, tmp_path)
        assert not any(e.kind == "sha_mismatch" for e in entries)

        dl = type("D", (), {"tasks": [_FakeDlTask("asset-d1")]})()
        integrity = check_batch_integrity(
            selection_manifest=_FakeSelection(asset_count=1),
            download_record=dl,
            ocr_record=record,
            annotations=[],
            run_dir=tmp_path,
        )
        assert integrity.passed is True
        raw_check = next(c for c in integrity.checks if c["name"] == "raw_response_integrity")
        assert raw_check["missing_or_bad"] == 0
        assert raw_check["file_caliber_mismatch"] == 0
        assert raw_check["raw_caliber_disclosed_mismatch"] == 1  # 并列披露计数
        assert raw_check["legacy_records_checked"] == 0


class _FakeDlTask:
    """最小下载任务（check_batch_integrity 资产哈希核对用）。"""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = asset_id
        self.content_sha256 = "img"  # 与 _ocr_task.image_sha256 一致
        self.last_error = None
        self.state = "DOWNLOADED"


class _FakeSelection:
    def __init__(self, asset_count: int) -> None:
        self.asset_count = asset_count


# ---------------------------------------------------------------------------
# 任务 6.1：质量报告装配（机器生成数字）
# ---------------------------------------------------------------------------


class TestBatchQualityReport:
    def test_report_assembly(self) -> None:
        entry = ConsistencyEntry(ocr_task_id="t", kind="partial", detail="d")
        integrity = IntegrityReport(passed=True, checks=[], gaps=[])
        report = build_batch_quality_report(
            batch_id="batch-001",
            selection_manifest=_FakeSelectionForReport(),
            exclusion_report={"selected_record_count": 2},
            download_record=type("D", (), {"tasks": [], "state_counts": {}, "run_id": "dl"})(),
            ocr_record=type(
                "O", (), {"ocr_run_id": "ocr", "state_counts": {}, "cost": {}, "performance": None}
            )(),
            annotation_state_counts={"ACCEPTED": 3, "OUT_OF_SCOPE": 1, "annotations_total": 4},
            annotations_total=4,
            consistency_entries=[entry],
            integrity=integrity,
            stop_triggers=[],
            known_limitations=[{"id": kid, "text": note} for kid, note in KNOWN_LIMITATIONS],
        )
        assert report.selection["asset_count"] == 7
        assert report.transcription["valid_denominator_total"] == 3
        assert report.consistency["by_kind"] == {"partial": 1}
        assert len(report.known_limitations) == 4
        dumped = json.loads(report.model_dump_json())
        assert dumped["batch_id"] == "batch-001"


class _FakeSelectionForReport:
    selection_rule_version = "EXTFP4-SELECT-1.0"
    geoscope = "g"
    date_window_min = "2025-07-20"
    date_window_max = "2026-07-20"
    record_count = 7
    asset_count = 7
    forbidden_domain_count = 0
    record_ids_hash = "h"


# ---------------------------------------------------------------------------
# 任务 3.1/7.1：下载运行中停止回调集成（mock transport，离线）
# ---------------------------------------------------------------------------


class TestDownloadStopCheckIntegration:
    def test_run_stops_on_consecutive_failures(self, tmp_path: Path) -> None:
        manifest = build_selection(_single_community_staged(tmp_path, n=8))
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            time.sleep(0.02)  # 节流：保证主线程按完成序处理，触发即停可复现
            return httpx.Response(500)

        check, collect = make_download_stop_check(max_consecutive_errors=3)
        record = run_download(
            manifest,
            tmp_path / "out",
            max_concurrency=1,
            max_attempts=1,
            timeout=1.0,
            transport=httpx.MockTransport(handler),
            stop_check=check,
        )
        assert record.stop_reason is not None
        assert len(calls) < 8  # 触发即停，未投完全部任务
        assert collect()[0].condition == AutoStopCondition.ERROR_PLACEHOLDER_MIME_ANOMALY.value
        # 状态文件已持久化 stop_reason
        state = json.loads(
            (Path(record.run_dir) / "download_state.json").read_text(encoding="utf-8")
        )
        assert state["stop_reason"] == record.stop_reason


def _single_community_staged(tmp_path: Path, n: int) -> Path:
    rows = [_staged_row(i + 1, f"P{i}", "示例小区132", "2025-08-01") for i in range(n)]
    df = pl.DataFrame(rows)
    path = tmp_path / "staged_single.parquet"
    df.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# 合同序列化往返
# ---------------------------------------------------------------------------


class TestContractSerialization:
    def test_contract_and_gates_roundtrip(self, tmp_path: Path) -> None:
        contract = _make_contract(tmp_path, tmp_path / "m.json")
        gates: BatchGates = contract.gates
        assert gates.change_budget_cap_yuan == CHANGE_BUDGET_CAP_YUAN
        assert gates.ocr_concurrency == 8
        assert gates.ocr_timeout_s == 60.0
        assert gates.single_image_baseline_yuan == 0.004
        assert len(contract.stop_conditions) >= 9  # §19.2 九项 + 磁盘扩展
        assert len(contract.known_limitations) == 4
        reloaded = BatchContract.model_validate(
            json.loads((tmp_path / "batch_contract.json").read_text(encoding="utf-8"))
        )
        assert reloaded.selection_manifest_sha256 == contract.selection_manifest_sha256
