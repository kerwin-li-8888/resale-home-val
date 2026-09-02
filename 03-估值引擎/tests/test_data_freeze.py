"""EXTFP5 数据冻结登记离线测试（change extfp5-data-freeze）。

覆盖四类用例：manifest 字段齐全、哈希不一致即停（版本未发布）、
指针切换不删目录、披露缺项阻止发布；外加一次端到端 run_freeze。
全部离线，不触网、不付费。
"""

from __future__ import annotations

import json
from pathlib import Path

from compsval.ingest.data_freeze import (
    FREEZE_VERSION_ID,
    check_disclosures,
    collect_freezable_assets,
    run_freeze,
    verify_manifest,
    write_manifest,
    write_version_pointer,
)


def _touch(path: Path, content: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_fixture_tree(root: Path) -> Path:
    """构造与 collect_freezable_assets 引用一致的完整最小资产树，返回 data_dir。"""
    data = root / "03-估值引擎" / "data"
    # staged
    _touch(data / "staged" / "lianjia_ext" / "current.json", '{"run_id":"r1"}')
    _touch(data / "staged" / "lianjia_ext" / "runs" / "run_r1" / "t.parquet")
    for t in ["floorplan_asset", "floorplan_ocr_word", "floorplan_room_annotation"]:
        _touch(data / "staged" / f"{t}.parquet")
    _touch(data / "staged" / "sale_event.parquet")
    _touch(data / "staged" / "sale_event.manifest.json")
    _touch(data / "staged" / "listing_event.parquet")
    # selection
    sel = data / "selection" / "lianjia_ext" / "floorplan"
    for f in [
        "production_manifest.json",
        "production_manifest.json.sha256",
        "production_exclusion_report.json",
        "production_out_of_scope_registry.json",
        "acceptance_manifest.json",
        "golden_label_template.csv",
        "golden_sample_image_map.csv",
        "selection_manifest.json",
    ]:
        _touch(sel / f)
    for sub in ["e2e", "concurrency_subset_20", "ocrnext_d_20", "independent_reverification_40"]:
        _touch(sel / sub / "a.txt")
    # raw
    img = (
        data
        / "raw"
        / "source=lianjia_ext"
        / "dataset=floorplan_image"
        / "batch_id=floorplan-extfp4-batch-01"
    )
    for i in range(3):
        _touch(img / f"{i:064d}.img")
    _touch(img / "download_run.json", '{"run_id":"floorplan-dl-x"}')
    _touch(img / "download_state.json", '{"state_counts":{"DOWNLOADED":3}}')
    ocr = data / "raw" / "source=lianjia_ext" / "dataset=floorplan_ocr_run"
    ocr_batch = ocr / "batch_id=floorplan-extfp4-batch-01"
    for i in range(3):
        _touch(ocr_batch / f"{i:064d}.jpg")
    _touch(ocr_batch / "floorplan_asset_manifest.json", '{"assets":[]}')
    for run in [
        "run_floorplan-ocr-data_selection_l",
        "run_floorplan-ocr-data_selection_l-20260827T094212Z",
        "run_floorplan-ocr-independent-reve",
        "run_floorplan-ocr-concurrency-subs-20260830T065639Z",
        "run_floorplan-ocr-concurrency-subs-20260830T070309Z",
        "run_floorplan-ocr-concurrency-subs-20260830T070814Z",
        "run_floorplan-ocr-ocrnext-d-subset-20260828T114752Z",
        "run_floorplan-ocr-ocrnext-d-subset-20260828T115044Z",
        "run_floorplan-ocr-ocrnext-d-subset-20260828T115946Z",
        "run_floorplan-ocr-01-_____________",
    ]:
        _touch(ocr / run / "ocr_run.json", '{"tasks":[]}')
        _touch(ocr / run / "raw_response_a.json")
    # excel snapshot
    xlsx_dir = (
        data
        / "raw"
        / "source=lianjia_ext"
        / "dataset=chengjiao_xlsx"
        / "fetched_at=20260824T000000Z"
    )
    _touch(xlsx_dir / "data.bin")
    _touch(xlsx_dir / "manifest.json")
    _touch(xlsx_dir / "provenance.json")
    # download acceptance300
    acc = data / "download" / "lianjia_ext" / "floorplan" / "acceptance300"
    for i in range(3):
        _touch(acc / f"{i:064d}.img")
    _touch(acc / "download_run.json")
    _touch(acc / "download_state.json")
    # 10 debug samples (repo-level, outside data/)
    debug = root / "01-数据" / "外部数据" / "户型图样本-20260824"
    for i in range(2):
        _touch(debug / f"huxingtu_{i:02d}.jpg")
    _touch(debug / "样本来源清单.md")
    # portrait + archive
    portrait = (
        root
        / "01-数据"
        / "外部数据"
        / "画像报告"
        / "20260830-外部链家OCR-EXTFP4-生产批次.json"
    )
    _touch(
        portrait,
        json.dumps(
            {
                "selection": {"asset_count": 3},
                "ocr": {"cost": {"total_cost_yuan": 0.1}},
                "transcription": {"annotations_total": 5},
                "integrity": {"exemption": "exempted_with_disclosure"},
            },
            ensure_ascii=False,
        ),
    )
    arc = root / "openspec" / "changes" / "archive" / "2026-08-30-extfp4-production-batch"
    _touch(arc / "batch_contract.json")
    return data


class TestBuildManifestFields:
    def test_fields_complete(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        manifest = collect_freezable_assets(data_dir)
        assert manifest.version_id == FREEZE_VERSION_ID
        assert manifest.change_ref == "extfp5-data-freeze"
        assert manifest.frozen_at
        assert set(manifest.sections) == {
            "staged",
            "production_batch_229",
            "validation_assets",
            "raw_excel_snapshot",
            "reports",
        }
        for section, entries in manifest.sections.items():
            assert entries, f"section {section} empty"
            for e in entries:
                assert e.path
                assert e.sha256
                if e.kind == "file":
                    assert e.count == 1
                else:
                    assert e.count is not None

    def test_missing_path_recorded(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        # 删除一个资产后 collect 应登记 MISSING 且 verify 报缺口
        missing = (
            data_dir
            / "selection"
            / "lianjia_ext"
            / "floorplan"
            / "production_manifest.json"
        )
        missing.unlink()
        manifest = collect_freezable_assets(data_dir)
        verification = verify_manifest(manifest, data_dir.parent.parent)
        assert not verification.ok
        assert any("路径缺失" in g for g in verification.gaps)


class TestVerifyHashMismatch:
    def test_hash_tamper_blocks_publish(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        manifest = collect_freezable_assets(data_dir)
        # 篡改一个登记哈希
        manifest.sections["reports"][0].sha256 = "0" * 64
        verification = verify_manifest(manifest, data_dir.parent.parent)
        assert not verification.ok
        assert any("SHA256 不符" in g for g in verification.gaps)

    def test_count_mismatch_reported(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        manifest = collect_freezable_assets(data_dir)
        # 篡改目录文件数
        for e in manifest.sections["production_batch_229"]:
            if e.key == "raw_image_batch":
                e.count = 999
        verification = verify_manifest(manifest, data_dir.parent.parent)
        assert not verification.ok
        assert any("文件数不符" in g for g in verification.gaps)


class TestPointerSwitchPreservesOld:
    def test_pointer_switch_keeps_old_dirs(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        versions = data_dir / "versions"
        v1_manifest = versions / "m_v1.json"
        v1_manifest.parent.mkdir(parents=True, exist_ok=True)
        v1_manifest.write_text("{}", encoding="utf-8")
        write_version_pointer(data_dir, "versions/m_v1.json", version_id="v1")
        # 切换指针到 v2：不删除旧 manifest / versions 目录
        v2_manifest = versions / "m_v2.json"
        v2_manifest.write_text("{}", encoding="utf-8")
        write_version_pointer(data_dir, "versions/m_v2.json", version_id="v2")
        assert versions.is_dir()
        assert v1_manifest.is_file(), "旧版本 manifest 不应被删除"
        pointer = versions / "lianjia_ext_latest.json"
        payload = json.loads(pointer.read_text(encoding="utf-8"))
        assert payload["version_id"] == "v2"
        assert payload["manifest"] == "versions/m_v2.json"


class TestDisclosureCheck:
    def test_report_passes_disclosure(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        manifest = collect_freezable_assets(data_dir)
        verification = verify_manifest(manifest, data_dir.parent.parent)
        assert verification.ok
        manifest_path, _ = write_manifest(manifest, data_dir / "versions")
        pointer = write_version_pointer(data_dir, f"versions/{manifest_path.name}")
        from compsval.ingest.data_freeze import build_freeze_report

        report = build_freeze_report(data_dir.parent, manifest, verification, pointer)
        missing = check_disclosures(report)
        assert missing == []

    def test_missing_keyword_flagged(self) -> None:
        text = "一段普通文本，不含任何披露关键词"
        missing = check_disclosures(text)
        assert "示例小区130" in missing
        assert "KL-1" in missing


class TestRunFreezeEndToEnd:
    def test_run_freeze_success(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        result = run_freeze(data_dir)
        assert result["ok"], result.get("gaps")
        assert result["stage"] == "done"
        versions = data_dir / "versions"
        assert (versions / "freeze_manifest_lianjia_ext_v1_20260830.json").is_file()
        assert (versions / "freeze_manifest_lianjia_ext_v1_20260830.json.sha256").is_file()
        assert (versions / "lianjia_ext_latest.json").is_file()
        assert (versions / "freeze_report_lianjia_ext_v1_20260830.md").is_file()
        # 复校验
        manifest = collect_freezable_assets(data_dir)
        assert verify_manifest(manifest, data_dir.parent.parent).ok

    def test_run_freeze_blocked_on_missing(self, tmp_path: Path) -> None:
        data_dir = build_fixture_tree(tmp_path)
        (data_dir / "selection" / "lianjia_ext" / "floorplan" / "production_manifest.json").unlink()
        result = run_freeze(data_dir)
        assert not result["ok"]
        assert result["stage"] == "verify"
        assert not (data_dir / "versions" / "lianjia_ext_latest.json").exists(), (
            "校验失败不得写指针（版本保持未发布）"
        )
