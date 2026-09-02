"""Listing-event derivation (WP4-D / DATA-007).

Derives ``listing_event`` staging rows from WP4-B :class:`LianjiaRecord`
records, kept strictly separate from ``sale_event`` (成交 ≠ 挂牌, never merged).
A listing event is only emitted when the record carries listing evidence
(a listing price and/or a listing-period on the 链家成交 list). The listing
price is recorded as ``price_yuan`` (平台挂牌口径); it is never written into
any sale-price column. No real adjustment sequence exists for 链家 (a single
listing price per snapshot), so ``price_adjustments`` is always empty — an
absence is recorded truthfully, never fabricated.

The first-listing date is *derived* from the disclosed deal date and
listing-period (``listing_date = deal_date - listing_period_days``); when
either is unknown the date stays ``None`` rather than inventing one. The liquid
listing ends in the recorded sale, so ``status``/``delist_date`` are derived
only when a deal date is present.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import (
    EventDatePrecision,
    ListingPriceBenchmark,
    ListingStatus,
    MissingSemantics,
    VerificationStatus,
)
from compsval.entities.backfill import CommunityIdLookup, resolve_community_id
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.parsers.lianjia import (
    PARSER_VERSION,
    LianjiaRecord,
)

STAGED_LAYER = "staged"
LISTING_EVENT_TABLE = "listing_event"
LISTING_EVENT_FILENAME = f"{LISTING_EVENT_TABLE}.parquet"


def has_listing_evidence(record: LianjiaRecord) -> bool:
    """True when the record carries listing evidence to derive an event from.

    Parking / non-residential records (and any record with neither a listing
    price nor a listing-period) have no listing evidence and produce no event.
    """
    if record.layout == "车位":
        return False
    return record.listing_price_yuan is not None or record.listing_period_days is not None


def listing_date_of(record: LianjiaRecord) -> tuple[date | None, EventDatePrecision]:
    """Derive the first-listing date from deal date and listing-period.

    ``deal_date - listing_period_days`` is the only legitimate inference 链家
    supports (成交周期 = 挂牌到成交天数). Missing either side ⇒ unknown, never
    a fabricated date. Returns ``(date, precision)``.
    """
    if record.deal_date is None or record.listing_period_days is None:
        return None, EventDatePrecision.UNKNOWN
    return (
        record.deal_date - timedelta(days=record.listing_period_days),
        EventDatePrecision.DAY,
    )


def listing_status_of(record: LianjiaRecord) -> ListingStatus:
    """The liquid listing ended in the recorded sale (成交列表) ⇒ SOLD.

    Without a deal date we cannot claim a sale, so status stays UNKNOWN.
    """
    if record.deal_date is not None:
        return ListingStatus.SOLD
    return ListingStatus.UNKNOWN


def _listing_event_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("listing_event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_record_id", pa.string(), nullable=False),
            pa.field("snapshot_id", pa.string(), nullable=False),
            pa.field("raw_locator", pa.string(), nullable=False),
            pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("parser_version", pa.string(), nullable=False),
            pa.field("event_date", pa.date32(), nullable=True),
            pa.field("event_date_precision", pa.string(), nullable=False),
            pa.field("community", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=True),
            pa.field("listing_id", pa.string(), nullable=False),
            pa.field("price_yuan", pa.int64(), nullable=True),
            pa.field("price_adjustments", pa.list_(pa.int64()), nullable=False),
            pa.field("delist_date", pa.date32(), nullable=True),
            pa.field("status", pa.string(), nullable=False),
            pa.field("listing_days", pa.int64(), nullable=True),
            pa.field("price_benchmark", pa.string(), nullable=False),
            pa.field("verification_status", pa.string(), nullable=False),
        ]
    )


def listing_event_table(
    records: Sequence[LianjiaRecord],
    *,
    source_id: str,
    snapshot_id: str,
    fetched_at: datetime,
    community_lookup: CommunityIdLookup | None = None,
) -> pa.Table:
    """Build the ``listing_event`` staging table from chain-listing records.

    Only records with listing evidence (``has_listing_evidence``) are emitted.
    Amounts are integer yuan; missing numeric stays ``None`` (never ``0``);
    ``price_adjustments`` is truthfully empty (no real sequence for 链家).
    ``community_id`` is provisional (the source community name) unless a WP5
    ``community_lookup`` is supplied and matches, in which case it is backfilled
    to the standard community id (unresolved / low-confidence → ``None``).
    """
    rows: dict[str, list[object]] = {
        name: [] for name in _listing_event_schema().names
    }

    def _int(value: Decimal | None) -> int | None:
        if value is None:
            return None
        return int(value.to_integral_value())

    apply_lookup = community_lookup is not None and not community_lookup.empty

    def _community_id(community: str | None) -> str | None:
        if not apply_lookup:
            return community
        assert community_lookup is not None  # guarded by apply_lookup (mypy narrowing)
        cid, _outcome, _reason = resolve_community_id(community, community_lookup)
        return cid

    for record in records:
        if not has_listing_evidence(record):
            continue
        fetched = fetched_at.replace(tzinfo=UTC) if fetched_at.tzinfo is None else fetched_at
        listing_date, date_precision = listing_date_of(record)
        status = listing_status_of(record)
        row_values: dict[str, object] = {
            "listing_event_id": f"{source_id}-listing-line{record.raw_start_line}",
            "source_id": source_id,
            "source_record_id": record.source_record_id,
            "snapshot_id": snapshot_id,
            "raw_locator": (
                f"{record.raw_start_line}"
                if record.raw_start_line is not None
                else MissingSemantics.UNKNOWN.value
            ),
            "fetched_at": fetched,
            "parser_version": PARSER_VERSION,
            "event_date": listing_date,
            "event_date_precision": date_precision.value,
            "community": record.community,
            "community_id": _community_id(record.community),  # WP5 backfill
            "listing_id": MissingSemantics.UNKNOWN.value,
            "price_yuan": _int(record.listing_price_yuan),
            "price_adjustments": [],
            "delist_date": (
                record.deal_date if status == ListingStatus.SOLD else None
            ),
            "status": status.value,
            "listing_days": record.listing_period_days,
            "price_benchmark": ListingPriceBenchmark.PLATFORM_LISTING.value,
            "verification_status": VerificationStatus.UNVERIFIED.value,
        }
        if list(row_values) != list(rows):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in rows:
            rows[name].append(row_values[name])
    return pa.table(rows, schema=_listing_event_schema())


def write_listing_event_stage(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """Atomically write the listing_event table and its DerivedManifest.

    Writes to a ``.incomplete`` sibling then renames so a partially flushed
    table can never masquerade as a complete derived table.
    """
    staged_dir = data_dir / STAGED_LAYER
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / LISTING_EVENT_FILENAME
    work_path = staged_dir / (LISTING_EVENT_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=STAGED_LAYER,
        table=LISTING_EVENT_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=[InputRef(dataset=i.dataset, fetched_at=i.fetched_at) for i in inputs],
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path