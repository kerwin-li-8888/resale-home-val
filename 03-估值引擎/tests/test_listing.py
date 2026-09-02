"""Unit tests for WP4-D listing-event derivation (DATA-007).

Verifies the acceptance criteria of the WP4-D contract:
① the listing price is recorded on ``listing_event`` (never on ``sale_event``);
② the first-listing date is derived from the deal date and listing-period but
   a fabricated date is never written when either side is missing;
③ without a real adjustment sequence ``price_adjustments`` stays empty;
④ listing history is separated from (not merged with) sale events, and absent
   a deal date the status stays truthful (UNKNOWN) rather than claiming a sale.
Parking / non-residential records and records with no listing evidence produce
no listing event.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from compsval.contract.models import (
    EventDatePrecision,
    ListingPriceBenchmark,
    ListingStatus,
)
from compsval.ingest.listing import (
    has_listing_evidence,
    listing_date_of,
    listing_event_table,
    listing_status_of,
    write_listing_event_stage,
)
from compsval.ingest.manifests import DerivedManifest, InputRef
from compsval.ingest.parsers.lianjia import LianjiaRecord, parse_lianjia_txt

_NOW = datetime(2026, 8, 21, 3, 14, 0)

_RESIDENTIAL_BLOCK = [
    "瑾雅苑 3室2厅 109.6平米",
    "南 | 精装2026.07.19 309万",
    "高楼层(共25层) 2008年塔楼28300元/平",
    "挂牌380万成交周期44天",
]


def _listing_record(
    *,
    community: str = "瑾雅苑",
    deal_date: date | None = date(2026, 7, 19),
    listing_price: str | None = "380",
    listing_period: int | None = 44,
    raw_line: int | None = 1,
    layout: str = "3室2厅",
) -> LianjiaRecord:
    return LianjiaRecord(
        community=community,
        layout=layout,
        deal_date=deal_date,
        listing_price_yuan=(Decimal(listing_price) * 10000).to_integral_value()
        if listing_price
        else None,
        listing_period_days=listing_period,
        raw_start_line=raw_line,
        source_record_id=f"rec-{raw_line}",
    )


def _parking_record(*, community: str = "示例小区220二期", raw_line: int | None = 0) -> LianjiaRecord:
    return LianjiaRecord(community=community, layout="车位", raw_start_line=raw_line)


def _row(table: pa.Table) -> dict[str, object]:
    """First row as a dict, skipping tz-aware ``fetched_at`` (needs zoneinfo)."""
    return {
        name: table.column(name).to_pylist()[0]
        for name in table.column_names
        if name != "fetched_at"
    }


# ---------------------------------------------------------------------------
# listing-evidence rule (验收①、③)
# ---------------------------------------------------------------------------


def test_has_listing_evidence_excludes_parking_and_bare_records() -> None:
    assert has_listing_evidence(_parking_record()) is False
    # 无挂牌价也无周期 → 无挂牌证据
    assert has_listing_evidence(
        _listing_record(listing_price=None, listing_period=None)
    ) is False
    # 只要其一存在即可派生挂牌事件
    assert has_listing_evidence(_listing_record(listing_period=None)) is True
    assert has_listing_evidence(_listing_record(listing_price=None)) is True


def test_residential_block_from_txt_has_listing_evidence() -> None:
    recs = parse_lianjia_txt(_RESIDENTIAL_BLOCK)
    assert len(recs) == 1
    assert recs[0].listing_price_yuan == Decimal("3800000")
    assert recs[0].listing_period_days == 44
    assert has_listing_evidence(recs[0]) is True


# ---------------------------------------------------------------------------
# listing-date derivation (验收②)
# ---------------------------------------------------------------------------


def test_listing_date_derived_from_deal_and_period() -> None:
    rec = _listing_record(deal_date=date(2026, 7, 19), listing_period=44)
    lst_date, precision = listing_date_of(rec)
    assert lst_date == date(2026, 7, 19) - timedelta(days=44) == date(2026, 6, 5)
    assert precision is EventDatePrecision.DAY


def test_listing_date_unknown_when_either_side_missing() -> None:
    # 缺成交日均不可推算 → 不虚构日期
    d1, p1 = listing_date_of(_listing_record(deal_date=None, listing_period=44))
    assert d1 is None and p1 is EventDatePrecision.UNKNOWN
    # 缺周期同样不可推算
    d2, p2 = listing_date_of(_listing_record(deal_date=date(2026, 7, 19), listing_period=None))
    assert d2 is None and p2 is EventDatePrecision.UNKNOWN


# ---------------------------------------------------------------------------
# listing status (验收②)
# ---------------------------------------------------------------------------


def test_listing_status_sold_only_with_deal_date() -> None:
    assert listing_status_of(_listing_record(deal_date=date(2026, 7, 19))) is ListingStatus.SOLD
    assert listing_status_of(_listing_record(deal_date=None)) is ListingStatus.UNKNOWN


# ---------------------------------------------------------------------------
# listing_event staged table: separation + missing discipline (验收①③④)
# ---------------------------------------------------------------------------

def test_listing_event_table_uses_listing_price_and_empty_adjustments() -> None:
    recs = [
        _listing_record(
            community="瑾雅苑", deal_date=date(2026, 7, 19), listing_price="380",
            listing_period=44, raw_line=1,
        )
    ]
    table = listing_event_table(recs, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW)
    assert table.num_rows == 1
    row = _row(table)
    # ① 挂牌价落到 listing_event，且无任何成交量价字段被写入
    assert row["price_yuan"] == 3800000
    assert row["price_benchmark"] == ListingPriceBenchmark.PLATFORM_LISTING.value
    # ③ 无真实调价序列 → 空表，不编造
    assert row["price_adjustments"] == []
    # ② 挂牌日由成交日倒推；状态仅在成交日存在时声明
    assert row["event_date"] == date(2026, 6, 5)
    assert row["event_date_precision"] == EventDatePrecision.DAY.value
    assert row["status"] == ListingStatus.SOLD.value
    # ④ 与成交分离但可关联：溯源回原始行，类型齐全
    assert row["listing_event_id"] == "SRC-007-listing-line1"
    assert row["raw_locator"] == "1"
    assert row["community"] == "瑾雅苑"


def test_listing_event_table_separates_and_stays_honest_without_deal() -> None:
    # 无成交日 → 状态 UNKNOWN、无下架日、挂牌日未知；price_adjustments 仍空
    rec = _listing_record(
        community="瑾雅苑", deal_date=None, listing_price="380",
        listing_period=None, raw_line=2,
    )
    table = listing_event_table(
        [rec], source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW
    )
    assert table.num_rows == 1
    row = _row(table)
    assert row["event_date"] is None
    assert row["event_date_precision"] == EventDatePrecision.UNKNOWN.value
    assert row["delist_date"] is None
    assert row["status"] == ListingStatus.UNKNOWN.value
    assert row["price_adjustments"] == []


def test_listing_table_excludes_parking_and_missing_discipline() -> None:
    recs = [
        _parking_record(community="示例小区220二期", raw_line=0),
        _listing_record(
            community="瑾雅苑", listing_price=None, listing_period=None, raw_line=1
        ),
        _listing_record(
            community="澜庭泊府(住宅)", listing_price="460", listing_period=None, raw_line=2
        ),
    ]
    table = listing_event_table(recs, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW)
    # 车位与无挂牌证据者不产生挂牌事件；仅最后一条入表
    assert table.num_rows == 1
    assert table.column("community").to_pylist() == ["澜庭泊府(住宅)"]
    # 缺失数值保持 None（不为 0）
    assert table.column("listing_days").to_pylist()[0] is None
    # 枚举显式值
    assert table.column("verification_status").to_pylist()[0] == "UNVERIFIED"


def test_listing_events_not_entries_in_sale_event() -> None:
    # 挂牌派生表里不出现任何 sale_price/成交价格字段 → 成交与挂牌分离
    table = listing_event_table(
        [_listing_record()], source_id="SRC-007", snapshot_id="s", fetched_at=_NOW
    )
    names = set(table.column_names)
    assert "price_yuan" in names  # 挂牌口径
    assert "total_price_yuan" not in names
    assert "unit_price" not in names
    assert "deal_date" not in names


# ---------------------------------------------------------------------------
# atomic staged write + DerivedManifest (可复现/溯源)
# ---------------------------------------------------------------------------


def test_write_listing_event_stage_atomic_manifest(tmp_path: Path) -> None:
    recs = [
        _listing_record(
            community="瑾雅苑", deal_date=date(2026, 7, 19), listing_price="380",
            listing_period=44, raw_line=4,
        ),
        _parking_record(community="示例小区220二期", raw_line=5),
    ]
    table = listing_event_table(recs, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW)
    final = write_listing_event_stage(
        table,
        data_dir=tmp_path,
        inputs=[InputRef(dataset="chengjiao_list", fetched_at="2026-08-21T03:14:00")],
        notes="WP4-D self-check",
    )
    assert final.exists()
    # 无残留 .incomplete 中间态
    assert not (tmp_path / "staged" / "listing_event.parquet.incomplete").exists()
    # manifest 溯源正确
    manifest = DerivedManifest.model_validate_json(
        (tmp_path / "staged" / "listing_event.manifest.json").read_text(encoding="utf-8")
    )
    assert manifest.layer == "staged"
    assert manifest.table == "listing_event"
    assert manifest.row_count == 1  # 车位不产生挂牌事件
    assert manifest.inputs[0].dataset == "chengjiao_list"
    assert manifest.package_version  # 非空
    # 读回 parquet 行数一致
    assert pa.parquet.read_table(final).num_rows == 1