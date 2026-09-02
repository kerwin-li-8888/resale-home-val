"""DuckDB catalog over raw Parquet snapshots.

Ported and renamed from Philly Fair Measure (fixed SHA e163eba6), unmodified in
behavior: discovers snapshots under ``data/raw/source=<source>/dataset=<dataset>/
fetched_at=<stamp>/`` and registers one DuckDB view per dataset —
``raw_<dataset>`` — pointing at the *latest* snapshot's Parquet file. Analyses
always read the newest immutable raw data without copying it; pass an older
``SnapshotRef`` explicitly to pin a historical snapshot.

Dataset names are assumed unique across sources (true today; revisit if a
second source ever publishes a colliding name).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from compsval import config
from compsval.ingest.binary_snapshot import BINARY_FILENAME
from compsval.ingest.manifests import (
    MANIFEST_FILENAME,
    DerivedManifest,
    SnapshotManifest,
    read_derived_manifest,
    read_manifest,
)
from compsval.ingest.snapshots import DATA_FILENAME

RAW_VIEW_PREFIX = "raw_"
DERIVED_LAYERS = {
    "staged": "stg_",
    "marts": "mart_",
    "entities": "ent_",
    "valuation": "val_",
    "backtest": "bt_",
}
_INCOMPLETE_SUFFIX = ".incomplete"


@dataclass(frozen=True)
class SnapshotRef:
    source: str
    dataset: str
    fetched_at: str
    directory: Path

    @property
    def data_path(self) -> Path:
        """快照主体文件：parquet 快照为 data.parquet；二进制快照为 data.bin。

        EXTFP1-B：二进制快照目录只有 data.bin（无 data.parquet），返回存在的文件。
        """
        parquet_path = self.directory / DATA_FILENAME
        if parquet_path.exists():
            return parquet_path
        return self.directory / BINARY_FILENAME

    @property
    def view_name(self) -> str:
        return RAW_VIEW_PREFIX + self.dataset

    def manifest(self) -> SnapshotManifest:
        return read_manifest(self.directory)


def _partition_value(name: str) -> str:
    return name.split("=", 1)[1]


def list_snapshots(data_dir: Path | None = None) -> list[SnapshotRef]:
    """All complete snapshots on disk, sorted by (dataset, fetched_at).

    A snapshot directory is recognized when it holds ``data.parquet`` (structured
    snapshot) or ``data.bin`` (EXTFP1-B binary snapshot) plus a manifest.
    """
    root = (data_dir if data_dir is not None else config.data_dir()) / "raw"
    refs = []
    for snapshot_dir in root.glob("source=*/dataset=*/fetched_at=*"):
        if snapshot_dir.name.endswith(_INCOMPLETE_SUFFIX):
            continue
        if not (snapshot_dir / DATA_FILENAME).exists() and not (
            snapshot_dir / BINARY_FILENAME
        ).exists():
            continue
        if not (snapshot_dir / MANIFEST_FILENAME).exists():
            continue
        refs.append(
            SnapshotRef(
                source=_partition_value(snapshot_dir.parent.parent.name),
                dataset=_partition_value(snapshot_dir.parent.name),
                fetched_at=_partition_value(snapshot_dir.name),
                directory=snapshot_dir,
            )
        )
    return sorted(refs, key=lambda ref: (ref.dataset, ref.fetched_at))


def latest_snapshots(data_dir: Path | None = None) -> dict[str, SnapshotRef]:
    """Latest complete snapshot per dataset, keyed by dataset name.

    The fetched_at stamp (%Y%m%dT%H%M%SZ) sorts lexicographically as UTC time.
    """
    latest: dict[str, SnapshotRef] = {}
    for ref in list_snapshots(data_dir):
        current = latest.get(ref.dataset)
        if current is None or ref.fetched_at > current.fetched_at:
            latest[ref.dataset] = ref
    return latest


@dataclass(frozen=True)
class DerivedRef:
    layer: str
    table: str
    path: Path

    @property
    def view_name(self) -> str:
        return DERIVED_LAYERS[self.layer] + self.table

    def manifest(self) -> DerivedManifest:
        return read_derived_manifest(self.path)


def list_derived(data_dir: Path | None = None) -> list[DerivedRef]:
    """Staged, mart and entity tables on disk, sorted by (layer, table).

    EXTFP1（CX-EXTFP1-001 修复）：``staged/lianjia_ext/`` 下为不可变 run 版本
    产物（``runs/run_<id>/*.parquet``），仅 ``current.json`` 指针指向的两张表
    作为当前派生表列出。
    """
    root = data_dir if data_dir is not None else config.data_dir()
    refs = []
    for layer in DERIVED_LAYERS:
        for path in sorted((root / layer).glob("*.parquet")):
            refs.append(DerivedRef(layer=layer, table=path.stem, path=path))
    # EXTFP1 表：staged/lianjia_ext/current.json → 两张当前表
    current_path = root / "staged" / "lianjia_ext" / "current.json"
    if current_path.is_file():
        import json

        try:
            current = json.loads(current_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            current = {}
        for table, rel in (
            ("lianjia_ext_sale_record", current.get("sale_record", "")),
            ("lianjia_ext_ordinary_residential", current.get("ordinary_residential", "")),
        ):
            path = root / "staged" / "lianjia_ext" / rel
            if rel and path.is_file():
                refs.append(DerivedRef(layer="staged", table=table, path=path))
    return sorted(refs, key=lambda ref: (ref.layer, ref.table))


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def connect(data_dir: Path | None = None, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Open DuckDB with views over the data lake.

    raw_<dataset> reads each dataset's latest snapshot; stg_<table> and
    mart_<table> read staged and mart tables.

    Only parquet snapshots are registered as ``read_parquet`` views: a binary
    snapshot (EXTFP1-B ``data.bin``) is raw evidence bytes, not a tabular table,
    and must not be read as parquet (RV-EXTFP1-B-01#F1 修复).
    """
    con = duckdb.connect(database)
    views = [
        (ref.view_name, ref.data_path)
        for ref in latest_snapshots(data_dir).values()
        if ref.data_path.suffix == ".parquet"
    ]
    views += [(ref.view_name, ref.path) for ref in list_derived(data_dir)]
    for view_name, path in views:
        con.execute(
            f"CREATE OR REPLACE VIEW {_quote_identifier(view_name)} AS "
            f"SELECT * FROM read_parquet({str(path)!r})"
        )
    return con