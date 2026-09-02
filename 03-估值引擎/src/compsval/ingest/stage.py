"""WP4-E reproducible ``data stage`` pipeline + marts layer (DATA-006/007).

``data_stage`` orchestrates the whole reproducibility loop from an immutable
raw snapshot to stage/mart derived tables with end-to-end lineage:

1. read the raw snapshot's stored bytes (one line per row) and parse them back
   into WP4-B :class:`LianjiaRecord` records with the same parser;
2. re-derive the ``sale_event`` staging table (WP4-C cleaning/dedup/anomaly);
3. re-derive the ``listing_event`` staging table (WP4-D listing derivation);
4. build the valuation-ready ``marts`` layer — ``valid_sale`` (formal pool:
   records whose anomaly_flag is NORMAL) and ``valid_listing`` (effective
   listing events) — mapping the generic staged ``event_date`` to the semantic
   ``sale_date`` / ``listing_date`` the marts layer promises (RV-WP4-D-01 F1);
5. emit the data-quality report (技术方案 §8.4) as Markdown + JSON.

Nothing here rewrites the raw snapshot; every mart row keeps ``raw_locator`` /
``source_record_id`` so it traces back to the original evidence line.
Reproducibility: the same snapshot + rule versions produce the same derived
tables (historical staged tables are *regenerated*, not appended).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from compsval import __version__
from compsval.catalog import SnapshotRef
from compsval.contract.registry import SOURCE_ID_BY_DIR
from compsval.entities.backfill import (
    CommunityIdLookup,
    collect_unmatched_conflicts,
    load_community_lookup,
)
from compsval.ingest.clean import (
    CleaningSummary,
    clean_sales,
    sale_event_table,
    write_sale_event_stage,
)
from compsval.ingest.listing import (
    listing_event_table,
    write_listing_event_stage,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.parsers.lianjia import (
    LianjiaRecord,
    parse_lianjia_txt,
)
from compsval.ingest.quality import (
    build_quality_report,
    write_quality_report,
)

MARTS_LAYER = "marts"
VALID_SALE_TABLE = "valid_sale"
VALID_SALE_FILENAME = f"{VALID_SALE_TABLE}.parquet"
VALID_LISTING_TABLE = "valid_listing"
VALID_LISTING_FILENAME = f"{VALID_LISTING_TABLE}.parquet"


def snapshot_id_of(ref: SnapshotRef) -> str:
    """Stable snapshot id for a raw lake snapshot (source-dataset-stamp)."""
    return f"{ref.source}-{ref.dataset}-{ref.fetched_at}"


def source_id_of(ref: SnapshotRef) -> str:
    """Registered source id for a raw lake partition directory."""
    return SOURCE_ID_BY_DIR.get(ref.source, "UNKNOWN")


def raw_snapshot_to_records(snapshot: SnapshotRef) -> list[LianjiaRecord]:
    """Reconstruct the raw text lines from the immutable parquet and parse them.

    The WP4-A import stores byte-exact ``(line_no, content)`` rows; we read back
    the ``content`` column in order and hand it to the same WP4-B parser, so the
    staged/mart tables are a *faithful re-derivation* of the evidence bytes.
    """
    table = pq.read_table(snapshot.data_path)
    lines = [str(line) for line in table.column("content").to_pylist()]
    return parse_lianjia_txt(lines)


# ---------------------------------------------------------------------------
# marts layer: effective tables for valuation (成交 ≠ 挂牌, never merged)
# ---------------------------------------------------------------------------


class NoFieldPlaceholder(Exception):  # pragma: no cover - defensive
    pass


def _rename_event_date(table: pa.Table, semantic_name: str) -> pa.Table:
    """Rename the generic staged ``event_date`` to a marts semantic date."""
    return table.rename_columns(
        [semantic_name if name == "event_date" else name for name in table.column_names]
    )


def valid_sale_table(sale_table: pa.Table) -> pa.Table:
    """The valuation-ready ``valid_sale`` mart: the formal (NORMAL) sale pool.

    Filters the staged sale_event to records not flagged parking / duplicate /
    abnormal unit price, and maps ``event_date`` → ``sale_date``. Flattened to
    the sale columns a comparable-sales engine reads; provenance columns are
    preserved so every row stays traceable.
    """
    mask = pc.equal(sale_table.column("anomaly_flag"), "正常")
    keep = [
        "sale_event_id", "source_id", "source_record_id", "snapshot_id",
        "raw_locator", "fetched_at", "parser_version",
        "event_date", "event_date_precision",
        "community", "community_id", "layout",
        "area_sqm", "total_price_yuan", "original_price_text",
        "unit_price", "unit_price_observed", "unit_price_formula", "orientation",
        "listing_price_yuan", "listing_period_days",
        "anomaly_flag", "verification_status",
    ]
    selected = sale_table.filter(mask).select([c for c in keep if c in sale_table.column_names])
    return _rename_event_date(selected, "sale_date")


def valid_listing_table(listing_table: pa.Table) -> pa.Table:
    """The effective ``valid_listing`` mart (every emitted listing event).

    All staged listing rows are effective events (WP4-D only emits records with
    listing evidence); no further filter applies. ``event_date`` → ``listing_date``.
    """
    keep = [
        "listing_event_id", "source_id", "source_record_id", "snapshot_id",
        "raw_locator", "fetched_at", "parser_version",
        "event_date", "event_date_precision",
        "community", "community_id", "listing_id",
        "price_yuan", "price_adjustments", "delist_date", "status",
        "listing_days", "price_benchmark", "verification_status",
    ]
    selected = listing_table.select([c for c in keep if c in listing_table.column_names])
    return _rename_event_date(selected, "listing_date")


def write_valid_sale_mart(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    return _write_mart(table, VALID_SALE_TABLE, VALID_SALE_FILENAME, data_dir, inputs, notes)


def write_valid_listing_mart(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    return _write_mart(table, VALID_LISTING_TABLE, VALID_LISTING_FILENAME, data_dir, inputs, notes)


def _write_mart(
    table: pa.Table,
    table_name: str,
    filename: str,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None,
) -> Path:
    dir_ = data_dir / MARTS_LAYER
    dir_.mkdir(parents=True, exist_ok=True)
    final = dir_ / filename
    work = dir_ / (filename + ".incomplete")
    pq.write_table(table, work, compression="zstd")
    manifest = DerivedManifest(
        layer=MARTS_LAYER,
        table=table_name,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        # 保留输入文件指纹（ext-sale-ingest-scope-v1-2 修复：此前重建 InputRef
        # 时剥离 content_hash，spec 要求的"manifest 登记 run 标识/文件指纹"未
        # 真正落盘；既有 manifest 的 null 值不受影响）。
        inputs=[
            InputRef(
                dataset=i.dataset,
                fetched_at=i.fetched_at,
                content_hash=i.content_hash,
            )
            for i in inputs
        ],
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final)
    work.replace(final)
    return final


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StageResult:
    snapshot_id: str
    source_id: str
    fetched_at: datetime
    records: list[LianjiaRecord]
    summary: CleaningSummary
    sale_event_path: Path
    listing_event_path: Path
    valid_sale_path: Path
    valid_listing_path: Path
    quality_report_md: Path
    quality_report_json: Path


def data_stage(
    snapshot: SnapshotRef,
    *,
    data_dir: Path,
    abnormal_factor: Decimal | None = None,
) -> StageResult:
    """Re-derive staged + marts tables and provenance from one raw snapshot."""
    records = raw_snapshot_to_records(snapshot)
    source_id = source_id_of(snapshot)
    snapshot_id = snapshot_id_of(snapshot)
    fetched_at = snapshot.manifest().fetched_at
    inputs = [InputRef(dataset=snapshot.dataset, fetched_at=fetched_at.isoformat())]

    # WP5-E: load the community / alias entity authority tables so the staged
    # events' provisional community_id can be backfilled to the standard id.
    # When the entities layer is absent the lookup is empty and provisional
    # values are preserved (behaviour unchanged).
    community_lookup: CommunityIdLookup = load_community_lookup(data_dir=data_dir)

    # staged: sale_event (cleaning/dedup/anomaly) + listing_event (listing)
    if abnormal_factor is None:
        cleaned, summary = clean_sales(records)
    else:
        cleaned, summary = clean_sales(records, abnormal_factor=abnormal_factor)
    sale_table = sale_event_table(
        cleaned,
        source_id=source_id,
        snapshot_id=snapshot_id,
        fetched_at=fetched_at,
        community_lookup=community_lookup,
    )
    listing_table = listing_event_table(
        records,
        source_id=source_id,
        snapshot_id=snapshot_id,
        fetched_at=fetched_at,
        community_lookup=community_lookup,
    )
    sale_event_path = write_sale_event_stage(
        sale_table, data_dir=data_dir, inputs=inputs, notes=f"WP4-E re-derivation @ {snapshot_id}"
    )
    listing_event_path = write_listing_event_stage(
        listing_table,
        data_dir=data_dir,
        inputs=inputs,
        notes=f"WP4-E re-derivation @ {snapshot_id}",
    )

    # marts: effective tables
    vs = valid_sale_table(sale_table)
    vl = valid_listing_table(listing_table)
    valid_sale_path = write_valid_sale_mart(
        vs, data_dir=data_dir, inputs=inputs, notes=f"WP4-E valid sale @ {snapshot_id}"
    )
    valid_listing_path = write_valid_listing_mart(
        vl, data_dir=data_dir, inputs=inputs, notes=f"WP4-E valid listing @ {snapshot_id}"
    )

    # data-quality report (§8.4), MD + JSON describing the same frozen data.
    # WP5-E 验收③：未匹配 / 低置信小区登记冲突清单（不静默归并），从两表事件
    # 的 community 列收集可回填为标准 ID 之外的小区名。
    unmatched = collect_unmatched_conflicts(
        [
            *sale_table.column("community").to_pylist(),
            *listing_table.column("community").to_pylist(),
        ],
        community_lookup,
    )
    report = build_quality_report(
        records,
        cleaned,
        summary,
        sale_table,
        listing_table,
        snapshot_id=snapshot_id,
        source_id=source_id,
        fetched_at=fetched_at,
        unmatched_conflicts=unmatched,
    )
    quality_md, quality_json = write_quality_report(
        report, data_dir=data_dir, notes=f"WP4-E quality report @ {snapshot_id}"
    )

    return StageResult(
        snapshot_id=snapshot_id,
        source_id=source_id,
        fetched_at=fetched_at,
        records=records,
        summary=summary,
        sale_event_path=sale_event_path,
        listing_event_path=listing_event_path,
        valid_sale_path=valid_sale_path,
        valid_listing_path=valid_listing_path,
        quality_report_md=quality_md,
        quality_report_json=quality_json,
    )


__all__ = [
    "MARTS_LAYER",
    "StageResult",
    "data_stage",
    "raw_snapshot_to_records",
    "source_id_of",
    "snapshot_id_of",
    "valid_listing_table",
    "valid_sale_table",
    "write_valid_listing_mart",
    "write_valid_sale_mart",
]