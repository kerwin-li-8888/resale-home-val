"""Capture raw snapshots of source data as Parquet plus a manifest.

Ported from Philly Fair Measure (fixed SHA e163eba6). The Philadelphia Carto /
ArcGIS fetch clients are dropped from this skeleton; what remains is the
generic, source-agnostic immutable-write primitive — write a raw PyArrow table
into the source-partitioned lake only after its manifest is written, by writing
into an ``.incomplete`` sibling and renaming.

Layout::

    data/raw/source=<source>/dataset=<name>/fetched_at=<UTC stamp>/data.parquet
                                                                    /manifest.json

Snapshots are immutable once written. A crashed write can never masquerade as
a valid snapshot because the directory is only renamed after the manifest
lands. The concrete ExampleCity sources and their ingestion adapters belong to
WP3/WP4.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__, config
from compsval.ingest.manifests import (
    ColumnInfo,
    FileInfo,
    SnapshotManifest,
    write_manifest,
)

logger = logging.getLogger(__name__)

DATA_FILENAME = "data.parquet"
FETCHED_AT_FORMAT = "%Y%m%dT%H%M%SZ"


@dataclass(frozen=True)
class SnapshotResult:
    directory: Path
    manifest: SnapshotManifest


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_raw_snapshot(
    table: pa.Table,
    *,
    root: Path | None = None,
    source: str,
    dataset: str,
    fetched_at: datetime,
    query: str,
    endpoint: str = "",
    completed_at: datetime | None = None,
    page_size: int = 1,
    num_pages: int = 1,
    order_key: str | None = None,
    source_row_count: int | None = None,
    row_limit: int | None = None,
    excluded_columns: list[str] | None = None,
) -> SnapshotResult:
    """Write an immutable raw snapshot (parquet + manifest) for a dataset.

    The table is written without touching any Philadelphia-specific client or
    schema: any PyArrow table can become a raw snapshot, which is the boundary
    WP3/WP4 use to persist ExampleCity source evidence with provenance.
    """
    completed_at = completed_at or datetime.now(UTC)
    data_root = root if root is not None else config.data_dir()
    final_dir = (
        data_root
        / "raw"
        / f"source={source}"
        / f"dataset={dataset}"
        / f"fetched_at={fetched_at.strftime(FETCHED_AT_FORMAT)}"
    )
    work_dir = final_dir.with_name(final_dir.name + ".incomplete")
    work_dir.mkdir(parents=True, exist_ok=False)
    data_path = work_dir / DATA_FILENAME

    pq.write_table(table, data_path, compression="zstd")

    manifest = SnapshotManifest(
        source=source,
        dataset=dataset,
        endpoint=endpoint,
        query=query,
        fetched_at=fetched_at,
        completed_at=completed_at,
        duration_seconds=0.0,
        source_row_count=source_row_count,
        row_count=table.num_rows,
        page_size=page_size,
        num_pages=num_pages,
        order_key=order_key,
        row_limit=row_limit,
        excluded_columns=sorted(excluded_columns or []),
        columns=[
            ColumnInfo(name=name, arrow_type=str(type_).__str__())
            for name, type_ in zip(table.column_names, table.schema.types, strict=True)
        ],
        files=[
            FileInfo(
                path=DATA_FILENAME,
                rows=table.num_rows,
                size_bytes=data_path.stat().st_size,
                sha256=_sha256(data_path),
            )
        ],
        package_version=__version__,
    )
    write_manifest(manifest, work_dir)
    work_dir.rename(final_dir)
    logger.info("snapshot complete: %s rows -> %s", f"{table.num_rows:,}", final_dir)
    return SnapshotResult(directory=final_dir, manifest=manifest)