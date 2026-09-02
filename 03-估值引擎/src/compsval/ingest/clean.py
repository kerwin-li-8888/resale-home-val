"""Sale cleaning, dedup, and anomaly flagging (WP4-C / DATA-006).

Consumes WP4-B :class:`~compsval.ingest.parsers.lianjia.LianjiaRecord`
records and derives a cleaned ``sale_event`` staging table on top of the
*immutable* raw snapshot. Nothing here rewrites the raw snapshot or the
WP4-B parsed records:

- parking / non-residential records (``layout == "车位"``) are flagged
  ``SUSPECT_PARKING`` and excluded from the formal sale pool (no area, no
  derived unit price);
- true duplicates sharing one *transaction identity* (community + area +
  deal date + total price + listing price — deliberately excluding layout and
  listing-period, which can vary on re-listings) keep the first occurrence as
  the canonical record and flag the rest ``SUSPECT_DUPLICATE``;
- unit-price anomalies are flagged ``SUSPECT_ABNORMAL_UNIT_PRICE`` when a
  community has enough residential records and one record's derived unit price
  deviates from the community median beyond ``abnormal_factor``.

Every deletion / flag links back to the original record's ``raw_start_line``
(the ``raw_locator`` into the raw parquet snapshot) so provenance is always
traceable. No missing value is ever written as ``0``; unknown numeric fields
stay ``None``, enum fields use the explicit ``UNKNOWN`` codes.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import (
    AnomalyFlag,
    EventDatePrecision,
    MissingSemantics,
    VerificationStatus,
)
from compsval.entities.backfill import CommunityIdLookup, resolve_community_id
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.parsers.fang_esf import FangEsfRecord
from compsval.ingest.parsers.lianjia import (
    PARSER_VERSION,
    LianjiaRecord,
)

#: 可进入清洗链的规范化成交记录（链家 TXT / 房天下 CSV 两类解析器产物，
#: 字段面一致；G3R-C 多源 marts 合并消费）。
SaleRecord = LianjiaRecord | FangEsfRecord

#: Default relative deviation from the community median that qualifies as an
#: abnormal unit price (0.65 matches the sample 示例小区154 65% discrepancy).
DEFAULT_ABNORMAL_FACTOR = Decimal("0.65")

#: Rounding-difference tolerance (元/㎡) applied when the derived unit price is
#: compared to the platform-disclosed value (RV-WP4-B-01 F1): the parser rounds
#: with ``ROUND_HALF_EVEN`` while 链家 discloses with ``ceil``, producing a
#: systematic 1 元/㎡ gap. Any ``abs(derived - observed) <= this`` is treated as
#: the same观察价, not as an anomaly.
ROUNDING_TOLERANCE_YUAN = Decimal("2")


def within_rounding_tolerance(derived: Decimal | None, observed: Decimal | None) -> bool:
    """True when derived and observed unit prices agree within rounding noise.

    Guards anomaly checks against the systematic 1 元/㎡ rounding-direction gap
    between the derived value and 链家's disclosed (``ceil``) value. Either side
    unknown ⇒ not comparable, so returns False (caller falls back to the
    derived-only cross-section check).
    """
    if derived is None or observed is None:
        return False
    return abs(derived - observed) <= ROUNDING_TOLERANCE_YUAN


@dataclass(frozen=True)
class CleanedSale:
    """One sale mapped through cleaning with its anomaly / verification marks."""

    record: SaleRecord
    anomaly_flag: AnomalyFlag
    verification_status: VerificationStatus
    dedup_key: str | None = None
    flag_note: str = ""


@dataclass(frozen=True)
class CleaningSummary:
    total: int
    parking_flagged: int
    duplicate_flagged: int
    abnormal_unit_price_flagged: int
    formal_pool: int


# --------------------------------------------------------------------------
# classification helpers (pure)
# --------------------------------------------------------------------------


def is_parking(record: SaleRecord) -> bool:
    """True for parking / non-residential records (no usable floor area)."""
    return record.layout == "车位"


def transaction_key(record: SaleRecord) -> str | None:
    """Transaction identity used for duplicate detection.

    Excludes layout and listing-period, which can legitimately vary between the
    two listings of the same underlying transaction (see 瑾雅苑 109.6㎡ sample:
    one run lists it as 3室2厅/44天, the other 3室1厅/47天 but both are
    309万/2026-07-19/挂牌380万). Returns ``None`` when any identity field is
    unknown so such records are never collapsed blind.
    """
    if record.deal_date is None or record.total_price_yuan is None:
        return None
    if record.area_sqm is None or record.listing_price_yuan is None:
        return None
    return "|".join(
        [
            record.community,
            f"{record.area_sqm.normalize()}",
            record.deal_date.isoformat(),
            f"{record.total_price_yuan}",
            f"{record.listing_price_yuan}",
        ]
    )


def _unit_price_deviation_ratio(unit_price: Decimal, community_median: Decimal) -> Decimal:
    if community_median <= 0:
        return Decimal("0")
    return abs(unit_price - community_median) / community_median


def _classify_abnormal_unit_prices(
    cleaned: list[CleanedSale], abnormal_factor: Decimal
) -> list[CleanedSale]:
    # Group derived unit prices per community over residential, non-flagged
    # records so the median baseline is not polluted by duplicates or parking.
    by_community: dict[str, list[Decimal]] = defaultdict(list)
    for sale in cleaned:
        if is_parking(sale.record):
            continue
        unit = sale.record.unit_price_derived
        if unit is None or unit <= 0:
            continue
        if sale.anomaly_flag in (AnomalyFlag.SUSPECT_DUPLICATE, AnomalyFlag.SUSPECT_PARKING):
            continue
        by_community[sale.record.community].append(unit)

    # A community needs at least two residential records before a unit price
    # can be judged against a within-community baseline.
    medians = {
        community: Decimal(median(units))
        for community, units in by_community.items()
        if len(units) >= 2
    }

    out: list[CleanedSale] = []
    for sale in cleaned:
        unit = sale.record.unit_price_derived
        if (
            is_parking(sale.record)
            or unit is None
            or sale.anomaly_flag in (AnomalyFlag.SUSPECT_DUPLICATE, AnomalyFlag.SUSPECT_PARKING)
            or sale.record.community not in medians
        ):
            out.append(sale)
            continue
        med = medians[sale.record.community]
        if _unit_price_deviation_ratio(unit, med) < abnormal_factor:
            out.append(sale)
            continue
        ratio = _unit_price_deviation_ratio(unit, med)
        out.append(
            CleanedSale(
                record=sale.record,
                anomaly_flag=AnomalyFlag.SUSPECT_ABNORMAL_UNIT_PRICE,
                verification_status=sale.verification_status,
                dedup_key=sale.dedup_key,
                flag_note=(
                    f"小区 {sale.record.community} 派生产单价 {unit} 与中位价 "
                    f"{med} 相对偏差 {ratio:.0%}"
                ),
            )
        )
    return out


def clean_sales(
    records: Sequence[SaleRecord],
    *,
    abnormal_factor: Decimal = DEFAULT_ABNORMAL_FACTOR,
) -> tuple[list[CleanedSale], CleaningSummary]:
    """Clean source records into flagged sale records plus a summary.

    Ordering ensures single, precise flags: parking first (hard non-residential
    exclusion, precedence over duplication), then duplicates, then unit-price
    anomalies. All records are returned (nothing is hard-deleted) so every flag
    stays traceable; ``formal_pool`` counts the remaining normal records that
    may enter valuation.
    """
    # 1) parking flag
    cleaned: list[CleanedSale] = []
    for record in records:
        if is_parking(record):
            cleaned.append(
                CleanedSale(
                    record=record,
                    anomaly_flag=AnomalyFlag.SUSPECT_PARKING,
                    verification_status=VerificationStatus.UNVERIFIED,
                    flag_note="车位/非住宅记录，无有效面积，不进入正式成交池",
                )
            )
        else:
            cleaned.append(
                CleanedSale(
                    record=record,
                    anomaly_flag=AnomalyFlag.NORMAL,
                    verification_status=VerificationStatus.UNVERIFIED,
                )
            )

    # 2) duplicate detection on transaction identity: keep the first occurrence
    seen: dict[str, int] = {}  # dedup_key -> index of the canonical record
    for i, sale in enumerate(cleaned):
        key = transaction_key(sale.record)
        if key is None or is_parking(sale.record):
            continue
        if key not in seen:
            seen[key] = i
            continue
        canonical = cleaned[seen[key]].record
        cleaned[i] = CleanedSale(
            record=sale.record,
            anomaly_flag=AnomalyFlag.SUSPECT_DUPLICATE,
            verification_status=VerificationStatus.UNVERIFIED,
            dedup_key=key,
            flag_note=(
                f"与记录@{canonical.raw_start_line}（{canonical.community} "
                f"{canonical.area_sqm}㎡/{canonical.total_price_yuan}元）为同一笔"
                f"交易的多录，保留首条，去除本条"
            ),
        )

    # 3) abnormal unit price per community
    cleaned = _classify_abnormal_unit_prices(cleaned, abnormal_factor)

    summary = CleaningSummary(
        total=len(cleaned),
        parking_flagged=sum(1 for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_PARKING),
        duplicate_flagged=sum(
            1 for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_DUPLICATE
        ),
        abnormal_unit_price_flagged=sum(
            1 for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_ABNORMAL_UNIT_PRICE
        ),
        formal_pool=sum(1 for s in cleaned if s.anomaly_flag == AnomalyFlag.NORMAL),
    )
    return cleaned, summary


# --------------------------------------------------------------------------
# staged sale_event derivation (parquet + DerivedManifest)
# --------------------------------------------------------------------------

#: Staged parquet layout relative to ``data_dir``.
STAGED_LAYER = "staged"
SALE_EVENT_TABLE = "sale_event"
SALE_EVENT_FILENAME = f"{SALE_EVENT_TABLE}.parquet"


def _sale_event_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("sale_event_id", pa.string(), nullable=False),
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
            pa.field("layout", pa.string(), nullable=False),
            pa.field("area_sqm", pa.float64(), nullable=True),
            pa.field("total_price_yuan", pa.int64(), nullable=True),
            pa.field("original_price_text", pa.string(), nullable=False),
            pa.field("unit_price", pa.int64(), nullable=True),
            pa.field("unit_price_observed", pa.int64(), nullable=True),
            pa.field("unit_price_formula", pa.string(), nullable=False),
            pa.field("orientation", pa.string(), nullable=False),
            pa.field("listing_price_yuan", pa.int64(), nullable=True),
            pa.field("listing_period_days", pa.int64(), nullable=True),
            pa.field("anomaly_flag", pa.string(), nullable=False),
            pa.field("verification_status", pa.string(), nullable=False),
            pa.field("dedup_key", pa.string(), nullable=True),
            pa.field("flag_note", pa.string(), nullable=False),
        ]
    )


def sale_event_table(
    cleaned: Sequence[CleanedSale],
    *,
    source_id: str,
    snapshot_id: str,
    fetched_at: datetime,
    community_lookup: CommunityIdLookup | None = None,
) -> pa.Table:
    """Build the cleaned ``sale_event`` staging table from cleaned sales.

    Amounts are stored as integer yuan; area as square metres; missing numeric
    fields stay ``None`` (never ``0``). ``community_id`` is provisional (the
    source community name) unless a WP5 ``community_lookup`` is supplied and
    non-empty, in which case it is backfilled to the standard community id
    (unresolved / low-confidence names become ``None`` and are not merged).
    """
    rows: dict[str, list[object]] = {name: [] for name in _sale_event_schema().names}

    def _int(value: Decimal | None) -> int | None:
        if value is None:
            return None
        return int(value.to_integral_value())

    apply_lookup = community_lookup is not None and not community_lookup.empty

    def _community_id(community: str | None) -> str | None:
        """Provisional (source name) unless looked up to a canonical id."""
        if not apply_lookup:
            return community
        assert community_lookup is not None  # guarded by apply_lookup (mypy narrowing)
        cid, _outcome, _reason = resolve_community_id(community, community_lookup)
        return cid

    for sale in cleaned:
        r = sale.record
        fetched = fetched_at.replace(tzinfo=UTC) if fetched_at.tzinfo is None else fetched_at
        row_values: dict[str, object] = {
            "sale_event_id": f"{source_id}-line{r.raw_start_line}",
            "source_id": source_id,
            "source_record_id": r.source_record_id,
            "snapshot_id": snapshot_id,
            "raw_locator": (
                f"{r.raw_start_line}"
                if r.raw_start_line is not None
                else MissingSemantics.UNKNOWN.value
            ),
            "fetched_at": fetched,
            "parser_version": PARSER_VERSION,
            "event_date": r.deal_date,
            "event_date_precision": (
                r.deal_date_precision
                if r.deal_date_precision is not None
                else EventDatePrecision.UNKNOWN.value
            ),
            "community": r.community,
            "community_id": _community_id(r.community),  # WP5 backfill
            "layout": r.layout,
            "area_sqm": float(r.area_sqm) if r.area_sqm is not None else None,
            "total_price_yuan": _int(r.total_price_yuan),
            "original_price_text": r.original_price_text,
            "unit_price": _int(r.unit_price_derived),
            "unit_price_observed": _int(r.unit_price_observed)
            if r.unit_price_observed is not None
            else None,
            "unit_price_formula": r.unit_price_formula,
            "orientation": r.orientation,
            "listing_price_yuan": _int(r.listing_price_yuan),
            "listing_period_days": r.listing_period_days,
            "anomaly_flag": sale.anomaly_flag.value,
            "verification_status": sale.verification_status.value,
            "dedup_key": sale.dedup_key,
            "flag_note": sale.flag_note,
        }
        if list(row_values) != list(rows):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in rows:
            rows[name].append(row_values[name])
    return pa.table(rows, schema=_sale_event_schema())


def write_sale_event_stage(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """Atomically write the sale_event table and its DerivedManifest to staged/.

    Writes to a ``.incomplete`` sibling then renames so a partially flushed
    table can never masquerade as a complete derived table.
    """
    staged_dir = data_dir / STAGED_LAYER
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / SALE_EVENT_FILENAME
    work_path = staged_dir / (SALE_EVENT_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=STAGED_LAYER,
        table=SALE_EVENT_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=[InputRef(dataset=i.dataset, fetched_at=i.fetched_at) for i in inputs],
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path
