"""Reproducible sample data for the Phase-1 data contract (验收标准⑤).

Deterministic example records for the four WP3-D entities — ``source_registry``,
``raw_snapshot``, ``sale_event``, ``listing_event`` — aligned with
数据字典-V0.1 §1-§3 and the WP2/WP3-A evidence. All values are frozen, so a
re-run reproduces identical data; the contract tests use these as fixtures and
WP4 may import them as examples.

Missing-value discipline (§7.3): numeric unknowns are ``None`` (never ``0``);
textual unknowns are the explicit ``UNKNOWN`` code.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from compsval.contract.models import (
    AnomalyFlag,
    EventDatePrecision,
    ListingEvent,
    ListingPriceBenchmark,
    ListingStatus,
    PriceBenchmark,
    RawSnapshot,
    SaleEvent,
    SnapshotFormat,
    SnapshotParseStatus,
    SourceAccessCondition,
    SourceAcquisitionMethod,
    SourceGranularity,
    SourceRegistry,
    SourceRepeatability,
    SourceRole,
    SourceStatus,
    SourceUpdateFrequency,
    VerificationStatus,
)

# 固定示例指纹：sha256 十六进制占位，内容字节不变则稳定
_SAMPLE_HASH = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

_FETCHED_AT = datetime(2026, 8, 21, 9, 0, 0, tzinfo=UTC)
_REGISTERED_AT = datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC)

_KANGTAI_SNAPSHOT_ID = "fang_esf-chengjiao-20260821-fang_kangtai_chengjiao_20260821"
_ZFCJ_SNAPSHOT_ID = "gov_zfcj-surplus_house-20260820-zfcj_surplus_house_targetdistrict_20260820"


def sample_sources() -> list[SourceRegistry]:
    """Two registered sources: 房天下 (P0) and 链家 (P0, 人工配合)."""
    return [
        SourceRegistry(
            source_id="SRC-005",
            name="房天下·示例城市目标区·小区成交记录",
            publisher="房天下（fang.com）",
            role=SourceRole.P0,
            granularity=SourceGranularity.SALE_UNIT,
            entry_url="https://esf.fang.com.example/loupan/2811007172/chengjiao/",
            price_benchmark="平台披露",
            update_frequency=SourceUpdateFrequency.CONTINUOUS,
            access_condition=SourceAccessCondition.PUBLIC,
            acquisition_method=SourceAcquisitionMethod.BROWSE,
            repeatability=SourceRepeatability.AUTOMATIC,
            status=SourceStatus.ACTIVE,
            registered_at=_REGISTERED_AT,
        ),
        SourceRegistry(
            source_id="SRC-007",
            name="链家/贝壳·示例城市·成交与实体权威",
            publisher="链家（lianjia.com）/ 贝壳",
            role=SourceRole.P0,
            granularity=SourceGranularity.SALE_UNIT,
            entry_url="https://lianjia.com.example/chengjiao/targetdistrict/",
            price_benchmark="平台披露",
            update_frequency=SourceUpdateFrequency.CONTINUOUS,
            access_condition=SourceAccessCondition.CAPTCHA,
            acquisition_method=SourceAcquisitionMethod.MANUAL,
            repeatability=SourceRepeatability.CONDITIONAL,
            status=SourceStatus.ACTIVE,
            registered_at=_REGISTERED_AT,
        ),
    ]


def sample_snapshots() -> list[RawSnapshot]:
    """One non-tabular evidence snapshot (房天下成交页截图，未解析)."""
    return [
        RawSnapshot(
            snapshot_id=_KANGTAI_SNAPSHOT_ID,
            source_id="SRC-005",
            dataset="chengjiao",
            fetched_at=_FETCHED_AT,
            query="https://esf.fang.com.example/loupan/2811007172/chengjiao/",
            content_hash=_SAMPLE_HASH,
            file_count=1,
            record_count=0,
            format=SnapshotFormat.PNG,
            parse_status=SnapshotParseStatus.NOT_PARSED,
        )
    ]


def sample_sales() -> list[SaleEvent]:
    """Two sale events: one complete record and one with unknown values."""
    return [
        SaleEvent(
            sale_event_id="SALE-0001",
            source_id="SRC-005",
            source_record_id="UNKNOWN",
            snapshot_id=_KANGTAI_SNAPSHOT_ID,
            raw_locator="row-1",
            fetched_at=_FETCHED_AT,
            published_at=None,
            event_date=date(2026, 8, 10),
            event_date_precision=EventDatePrecision.DAY,
            verification_status=VerificationStatus.UNVERIFIED,
            parser_version="parser-0.1.0",
            content_hash=_SAMPLE_HASH,
            community_id="C-XXXX0027",
            sale_date=date(2026, 8, 10),
            total_price_yuan=Decimal("1500000"),
            original_price_text="150万",
            area_sqm=Decimal("73.6"),
            unit_price=Decimal("20380"),
            unit_price_formula="total_price_yuan / area_sqm，四舍五入到元",
            layout="3室2厅",
            floor=12,
            total_floors=33,
            orientation="南",
            has_elevator=True,
            price_benchmark=PriceBenchmark.PLATFORM_DISCLOSED,
            listing_price_yuan=None,
            listing_period_days=None,
            anomaly_flag=AnomalyFlag.NORMAL,
        ),
        SaleEvent(
            sale_event_id="SALE-0002",
            source_id="SRC-006",
            source_record_id="UNKNOWN",
            snapshot_id="centanet-chengjiao-20260820-centanet_xinghui_chengjiao_20260820",
            raw_locator="UNKNOWN",
            fetched_at=_FETCHED_AT,
            published_at=None,
            event_date=None,
            event_date_precision=EventDatePrecision.MONTH,
            verification_status=VerificationStatus.UNVERIFIED,
            parser_version="parser-0.1.0",
            content_hash=_SAMPLE_HASH,
            community_id="C-XXXX0188",
            sale_date=None,
            total_price_yuan=None,
            original_price_text="总价面谈",
            area_sqm=None,
            unit_price=None,
            unit_price_formula="UNKNOWN",
            layout="UNKNOWN",
            floor=None,
            total_floors=None,
            orientation="UNKNOWN",
            has_elevator=None,
            price_benchmark=PriceBenchmark.UNKNOWN,
            listing_price_yuan=None,
            listing_period_days=None,
            anomaly_flag=AnomalyFlag.NORMAL,
        ),
    ]


def sample_listings() -> list[ListingEvent]:
    """One official surplus-house listing (阳光家缘存量房)."""
    return [
        ListingEvent(
            listing_event_id="LIST-0001",
            source_id="SRC-002",
            source_record_id="pyid-1ca861e3cf804e8084990c402a0c8c50",
            snapshot_id=_ZFCJ_SNAPSHOT_ID,
            raw_locator="UNKNOWN",
            fetched_at=_FETCHED_AT,
            published_at=None,
            event_date=None,
            event_date_precision=EventDatePrecision.UNKNOWN,
            verification_status=VerificationStatus.UNVERIFIED,
            parser_version="parser-0.1.0",
            content_hash=_SAMPLE_HASH,
            community_id="C-XXXX0120",
            listing_id="pyid-1ca861e3cf804e8084990c402a0c8c50",
            listing_date=None,
            price_yuan=Decimal("2350000"),
            price_adjustments=[Decimal("2350000")],
            delist_date=None,
            status=ListingStatus.ON_SALE,
            listing_days=None,
            price_benchmark=ListingPriceBenchmark.OFFICIAL_LISTING,
        )
    ]
