"""Data-contract registry (WP3-D): ExampleCity sources/datasets and evidence
snapshots with live sha256 fingerprints that stay stable across re-runs."""

import hashlib
from pathlib import Path

from compsval.contract import registry
from compsval.contract.models import (
    SnapshotFormat,
    SnapshotParseStatus,
    SourceRole,
)


def test_sources_registered_with_unique_ids() -> None:
    sources = registry.registered_sources()
    ids = [source.source_id for source in sources]
    assert len(ids) == len(set(ids))
    assert "SRC-005" in ids
    assert "SRC-007" in ids
    assert all(source.role in (SourceRole.P0, SourceRole.P1) for source in sources)
    # P0 来源必须给出可重复性结论（DATA-002 输出）
    for source in sources:
        assert source.repeatability is not None


def test_datasets_registered_with_sources() -> None:
    datasets = registry.registered_datasets()
    names = {dataset.dataset for dataset in datasets}
    assert {"chengjiao", "community_list", "surplus_house", "listing"} <= names
    for dataset in datasets:
        assert dataset.source_ids
        assert dataset.kind
        assert dataset.description


def test_evidence_snapshots_scanned_from_layout(tmp_path: Path) -> None:
    page_dir = (
        tmp_path / "source=fang_esf" / "dataset=chengjiao" / "fetched_at=20260821"
    )
    page_dir.mkdir(parents=True)
    (page_dir / "page.png").write_bytes(b"page-bytes")

    snapshots = registry.list_evidence_snapshots(tmp_path)
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.source_id == "SRC-005"
    assert snapshot.dataset == "chengjiao"
    assert snapshot.format == SnapshotFormat.PNG
    assert snapshot.parse_status == SnapshotParseStatus.NOT_PARSED
    assert snapshot.file_count == 1
    assert snapshot.record_count == 0
    assert snapshot.content_hash == hashlib.sha256(b"page-bytes").hexdigest()


def test_evidence_fingerprint_stable_across_runs(tmp_path: Path) -> None:
    page_dir = (
        tmp_path / "source=lianjia" / "dataset=chengjiao_list" / "fetched_at=20260821"
    )
    page_dir.mkdir(parents=True)
    (page_dir / "list.txt").write_bytes(b"stable-bytes")

    first = registry.list_evidence_snapshots(tmp_path)
    second = registry.list_evidence_snapshots(tmp_path)
    assert len(first) == 1
    assert first[0] == second[0]
    assert first[0].content_hash == hashlib.sha256(b"stable-bytes").hexdigest()


def test_evidence_fingerprint_changes_with_bytes(tmp_path: Path) -> None:
    page_dir = tmp_path / "source=fang_esf" / "dataset=chengjiao" / "fetched_at=20260821"
    page_dir.mkdir(parents=True)
    target = page_dir / "page.png"
    target.write_bytes(b"v1")
    before = registry.list_evidence_snapshots(tmp_path)[0].content_hash
    target.write_bytes(b"v2")
    after = registry.list_evidence_snapshots(tmp_path)[0].content_hash
    assert before != after


def test_evidence_scan_skips_unrelated_files(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("not evidence", encoding="utf-8")
    shallow = tmp_path / "source=fang_esf" / "dataset=chengjiao"
    shallow.mkdir(parents=True)
    (shallow / "loose.png").write_bytes(b"x")  # 非 4 层布局，跳过
    assert registry.list_evidence_snapshots(tmp_path) == []


def test_lianjia_ext_registered_as_independent_source() -> None:
    """EXTFP0-A：外部链家来源独立登记，不得与内采 SRC-007 静默合并。"""
    sources = {source.source_id: source for source in registry.registered_sources()}
    assert "SRC-011" in sources
    ext = sources["SRC-011"]
    assert ext.role is SourceRole.P0
    assert ext.granularity.value == "逐套成交"
    assert ext.repeatability is not None
    assert "lianjia_ext" in registry.SOURCE_ID_BY_DIR
    assert registry.SOURCE_ID_BY_DIR["lianjia_ext"] == "SRC-011"
    # 与内采链家来源是不同 ID，且各自唯一
    assert "SRC-007" in sources
    assert ext.source_id != sources["SRC-007"].source_id


def test_lianjia_ext_datasets_registered() -> None:
    datasets = {
        dataset.dataset: dataset for dataset in registry.registered_datasets()
    }
    for name in ("chengjiao_xlsx", "floorplan_image"):
        assert name in datasets
        assert "SRC-011" in datasets[name].source_ids
