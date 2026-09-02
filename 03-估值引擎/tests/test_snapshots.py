"""Immutable raw snapshot: layout, manifest provenance, and write-atomicity of
write_raw_snapshot (the source-agnostic primitive kept from upstream)."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval import __version__
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    SnapshotManifest,
    read_derived_manifest,
    read_manifest,
    write_derived_manifest,
    write_manifest,
)
from compsval.ingest.snapshots import write_raw_snapshot

ROW = {"house_id": ["1", "2"], "price": [3_200_000.0, 4_500_000.0]}


def test_write_raw_snapshot_layout_and_manifest(tmp_path: Path) -> None:
    fetched_at = datetime(2026, 8, 21, 3, 4, 5, tzinfo=UTC)
    result = write_raw_snapshot(
        pa.table(ROW),
        root=tmp_path,
        source="lianjia",
        dataset="chengjiao",
        fetched_at=fetched_at,
        query="SELECT house_id, price FROM ...",
        endpoint="https://gz.lianjia.test/chengjiao",
        source_row_count=2,
    )

    # layout: data/raw/source=<src>/dataset=<name>/fetched_at=<UTC stamp>/
    assert result.directory.parent.name == "dataset=chengjiao"
    assert result.directory.parent.parent.name == "source=lianjia"
    assert result.directory.name == "fetched_at=20260821T030405Z"
    assert result.directory.parent.parent == tmp_path / "raw" / "source=lianjia"
    # no in-flight sibling survives
    assert not list(result.directory.parent.glob("*.incomplete"))

    table = pq.read_table(result.directory / "data.parquet")
    assert table.num_rows == 2
    assert table.column_names == ["house_id", "price"]

    manifest = read_manifest(result.directory)
    assert manifest.source == "lianjia"
    assert manifest.dataset == "chengjiao"
    assert manifest.row_count == 2
    assert manifest.source_row_count == 2
    assert manifest.package_version == __version__
    assert [c.name for c in manifest.columns] == ["house_id", "price"]

    (file_info,) = manifest.files
    data_bytes = (result.directory / file_info.path).read_bytes()
    assert file_info.size_bytes == len(data_bytes)
    assert file_info.sha256 == hashlib.sha256(data_bytes).hexdigest()

    # manifest.json is valid JSON on disk
    raw = json.loads((result.directory / "manifest.json").read_text(encoding="utf-8"))
    assert raw["manifest_version"] == 1


def test_write_raw_snapshot_is_immutable(tmp_path: Path) -> None:
    fetched_at = datetime(2026, 8, 21, 3, 4, 5, tzinfo=UTC)
    write_raw_snapshot(
        pa.table(ROW),
        root=tmp_path,
        source="lianjia",
        dataset="chengjiao",
        fetched_at=fetched_at,
        query="SELECT ...",
    )
    # A second identical snapshot at the same stamp must not overwrite the first.
    with pytest.raises(FileExistsError):
        write_raw_snapshot(
            pa.table(ROW),
            root=tmp_path,
            source="lianjia",
            dataset="chengjiao",
            fetched_at=fetched_at,
            query="SELECT ...",
        )


# ---- CXWP5-001 回归：manifest 必须显式 UTF-8，不依赖运行环境本地编码 ----
# 在 Windows 中文区域（cp936）下，缺省 write_text 会把非 ASCII 备注写成本地
# 编码，导致按 UTF-8 读取时 UnicodeDecodeError。以下测试直接校验磁盘字节为
# 合法 UTF-8 且 read_* 往返一致。


def _snapshot_manifest(notes: str) -> SnapshotManifest:
    fetched_at = datetime(2026, 8, 21, 3, 4, 5, tzinfo=UTC)
    return SnapshotManifest(
        source="lianjia",
        dataset="chengjiao",
        endpoint="https://gz.lianjia.test/chengjiao",
        query="SELECT house_id, price FROM ...",
        fetched_at=fetched_at,
        completed_at=fetched_at,
        duration_seconds=0.0,
        source_row_count=None,
        row_count=1,
        page_size=1,
        num_pages=1,
        order_key=None,
        columns=[],
        files=[],
        package_version=__version__,
        notes=notes,
    )


def test_write_manifest_utf8_bytes_with_non_ascii_notes(tmp_path: Path) -> None:
    """原始 manifest 写盘字节必须是合法 UTF-8（备注含中文也不回退本地编码）。"""
    path = write_manifest(_snapshot_manifest("测试备注：链家成交列表快照"), tmp_path)
    raw = path.read_bytes()
    assert raw.decode("utf-8")  # 非 UTF-8（如 GBK）字节会在此抛 UnicodeDecodeError
    assert json.loads(path.read_text(encoding="utf-8"))["notes"] == "测试备注：链家成交列表快照"
    assert read_manifest(tmp_path).notes == "测试备注：链家成交列表快照"


def test_write_derived_manifest_utf8_bytes_with_non_ascii_notes(
    tmp_path: Path,
) -> None:
    """派生 manifest（<table>.manifest.json）写盘字节必须是合法 UTF-8。"""
    manifest = DerivedManifest(
        layer="entities",
        table="scope_policy_v1.0",
        built_at=datetime(2026, 8, 22, 0, 0, 0, tzinfo=UTC),
        row_count=1,
        inputs=[
            InputRef(
                dataset="candidate_community_catalog",
                fetched_at="2026-08-21",
            )
        ],
        package_version=__version__,
        notes="测试备注：WP5-F 范围清单",
    )
    table_path = tmp_path / "scope_policy_v1.0.parquet"
    path = write_derived_manifest(manifest, table_path)
    raw = path.read_bytes()
    assert raw.decode("utf-8")  # 非 UTF-8 字节会在此抛 UnicodeDecodeError
    assert path.name == "scope_policy_v1.0.manifest.json"
    assert read_derived_manifest(table_path).notes == "测试备注：WP5-F 范围清单"