"""DuckDB catalog: snapshot discovery ignores incomplete/bare dirs and keeps
the latest snapshot per dataset, exposing read-only raw_<dataset> views."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import catalog
from compsval.ingest.manifests import (
    ColumnInfo,
    FileInfo,
    SnapshotManifest,
    write_manifest,
)


def _write_snapshot(data_dir: Path, dataset: str, fetched_at: str, rows: dict[str, list]) -> Path:
    directory = (
        data_dir / "raw" / "source=lianjia" / f"dataset={dataset}" / f"fetched_at={fetched_at}"
    )
    directory.mkdir(parents=True)
    table = pa.table(rows)
    pq.write_table(table, directory / "data.parquet")
    now = datetime.now(UTC)
    manifest = SnapshotManifest(
        source="lianjia",
        dataset=dataset,
        endpoint="https://example.test/sql",
        query="SELECT ...",
        fetched_at=now,
        completed_at=now,
        duration_seconds=0.0,
        source_row_count=table.num_rows,
        row_count=table.num_rows,
        page_size=100,
        num_pages=1,
        order_key="id",
        columns=[ColumnInfo(name=name, arrow_type="string") for name in rows],
        files=[FileInfo(path="data.parquet", rows=table.num_rows, size_bytes=1, sha256="x")],
        package_version="test",
    )
    write_manifest(manifest, directory)
    return directory


def _populate(tmp_path: Path) -> None:
    _write_snapshot(
        tmp_path, "alpha", "20260101T000000Z", {"house_id": ["1"], "value": ["old"]}
    )
    _write_snapshot(
        tmp_path,
        "alpha",
        "20260301T000000Z",
        {"house_id": ["1", "2"], "value": ["new", "new"]},
    )
    _write_snapshot(
        tmp_path, "beta", "20260201T000000Z", {"house_id": ["2"], "year": ["2026"]}
    )
    # an in-flight fetch and a bare directory must both be ignored
    incomplete = (
        tmp_path
        / "raw"
        / "source=lianjia"
        / "dataset=alpha"
        / "fetched_at=20260401T000000Z.incomplete"
    )
    incomplete.mkdir(parents=True)
    empty = tmp_path / "raw" / "source=lianjia" / "dataset=gamma" / "fetched_at=20260101T000000Z"
    empty.mkdir(parents=True)


def test_list_snapshots_finds_only_complete_snapshots(tmp_path: Path) -> None:
    _populate(tmp_path)
    refs = catalog.list_snapshots(tmp_path)
    assert [(r.dataset, r.fetched_at) for r in refs] == [
        ("alpha", "20260101T000000Z"),
        ("alpha", "20260301T000000Z"),
        ("beta", "20260201T000000Z"),
    ]
    assert all(r.source == "lianjia" for r in refs)


def test_latest_snapshots_picks_newest_per_dataset(tmp_path: Path) -> None:
    _populate(tmp_path)
    latest = catalog.latest_snapshots(tmp_path)
    assert set(latest) == {"alpha", "beta"}
    assert latest["alpha"].fetched_at == "20260301T000000Z"
    assert latest["alpha"].manifest().row_count == 2


def test_connect_registers_views_over_latest_snapshots(tmp_path: Path) -> None:
    _populate(tmp_path)
    con = catalog.connect(tmp_path)
    assert con.sql("SELECT value FROM raw_alpha WHERE house_id = '1'").fetchone() == ("new",)
    joined = con.sql(
        """
        SELECT a.house_id, b.year
        FROM raw_alpha a
        JOIN raw_beta b USING (house_id)
        """
    ).fetchall()
    assert joined == [("2", "2026")]


def test_connect_with_no_snapshots_yields_no_views(tmp_path: Path) -> None:
    con = catalog.connect(tmp_path)
    assert con.sql("SELECT count(*) FROM duckdb_views() WHERE NOT internal").fetchone() == (0,)