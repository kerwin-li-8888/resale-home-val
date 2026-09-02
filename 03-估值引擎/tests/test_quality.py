"""Unit tests for WP4-E data-quality report generation (技术方案 §8.4).

Covers the WP4-E acceptance criteria on the quality-report side: the report
carries every §8.4 item, the 3/6/12-month per-community distribution agrees with
the sample's own dates (as_of = 数据截点), the provisional supportability is
explicitly labelled "正式门槛待 BT-001", and the Markdown and JSON describe the
same frozen numbers.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa

from compsval.contract.models import EventDatePrecision
from compsval.ingest.clean import (
    CleanedSale,
    CleaningSummary,
    clean_sales,
    sale_event_table,
)
from compsval.ingest.listing import listing_event_table
from compsval.ingest.parsers.lianjia import LianjiaRecord
from compsval.ingest.quality import (
    build_quality_report,
    key_field_coverage,
    per_community_deals,
    report_to_dict,
    report_to_markdown,
)

_FETCHED = datetime(2026, 8, 21, 3, 14, 0)


def _rec(
    *,
    community: str,
    layout: str = "3室2厅",
    raw_line: int = 1,
    deal: str | None = "2026.07.19",
    price: str | None = "300",
    area: str | None = "100",
    listing_period: int | None = 30,
    unparsed: tuple[str, ...] = (),
) -> LianjiaRecord:
    rec = LianjiaRecord(
        community=community,
        layout=layout,
        raw_start_line=raw_line,
        source_record_id=f"rec-{raw_line}",
        orientation="南",
        unit_price_observed=Decimal("30000"),
    )
    if area is not None and price is not None:
        rec.area_sqm = Decimal(area)
        rec.total_price_yuan = (Decimal(price) * 10000).to_integral_value()
        rec.original_price_text = f"{price}万"
        rec.unit_price_derived = (
            rec.total_price_yuan / rec.area_sqm
        ).to_integral_value()
    if deal is not None:
        rec.deal_date = date(*map(int, deal.split(".")))
        rec.deal_date_precision = EventDatePrecision.DAY
    rec.listing_price_yuan = (Decimal("380") * 10000).to_integral_value()
    rec.listing_period_days = listing_period
    rec.unparsed_lines = unparsed
    return rec


def _parking(*, community: str = "示例小区220", raw_line: int = 0) -> LianjiaRecord:
    return LianjiaRecord(community=community, layout="车位", raw_start_line=raw_line)


def _build(
    records: list[LianjiaRecord],
) -> tuple[list[CleanedSale], CleaningSummary, pa.Table, pa.Table]:
    """Clean + stage a list of records into (cleaned, summary, sale, listing)."""
    cleaned, summary = clean_sales(records)
    sale = sale_event_table(
        cleaned, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_FETCHED
    )
    listing = listing_event_table(
        records, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_FETCHED
    )
    return cleaned, summary, sale, listing


# ---------------------------------------------------------------------------
# 验收②：报告覆盖 §8.4 全部必需项，数值与清洗一致
# ---------------------------------------------------------------------------


def test_report_counts_match_cleaning_summary() -> None:
    records = [
        # 春晖：两录重复（保留首条），1 formal
        _rec(community="春晖", raw_line=1, deal="2025.10.01", price="120", area="40"),
        _rec(community="春晖", raw_line=2, deal="2025.10.01", price="120", area="40"),
        # 示例小区220：车位排除
        _parking(community="示例小区220", raw_line=3),
        # 解析失败记录（不同成交日，避免与下一条被当作重复）
        _rec(community="万科", raw_line=4, deal="2026.06.25", unparsed=("某无法识别行",)),
        _rec(community="万科", raw_line=5, deal="2026.07.01"),
    ]
    cleaned, summary, sale, listing = _build(records)
    report = build_quality_report(
        records,
        cleaned,
        summary,
        sale,
        listing,
        snapshot_id="snap-1",
        source_id="SRC-007",
        fetched_at=_FETCHED,
    )
    # 输入/输出记录数与清洗摘要逐项一致
    assert report.input_records == len(records) == 5
    assert report.parse_failures == 1
    assert report.output_sale_events == summary.total == 5
    assert report.parking_excluded == summary.parking_flagged == 1
    assert report.duplicates == summary.duplicate_flagged == 1
    assert report.abnormal_unit_price == summary.abnormal_unit_price_flagged == 0
    assert report.formal_pool == summary.formal_pool == 3
    # 挂牌事件：车位与解析失败无挂牌证据者不计；两条春晖 + 一条万科有挂牌证据
    assert report.output_listing_events == listing.num_rows > 0


# ---------------------------------------------------------------------------
# 验收②③：关键字段覆盖率（缺失不写 0，覆盖率为已知/总数）
# ---------------------------------------------------------------------------


def test_key_field_coverage_reflects_missing_values() -> None:
    records = [
        _rec(community="雅居乐", raw_line=1, deal="2026.07.10", price="245", area="89.2"),
        # 缺成交价（也缺面积）→ 该字段 coverage 应 < 100%
        _rec(community="雅居乐", raw_line=2, deal="2026.07.15", price=None, area=None),
    ]
    cleaned, summary, sale, listing = _build(records)
    cover = {c.field: c for c in key_field_coverage(sale)}
    # 两行均有 layout；面积/总价各已知 1 / 2
    assert cover["layout"].known == 2 and cover["layout"].total == 2
    assert cover["area_sqm"].known == 1 and cover["area_sqm"].total == 2
    assert cover["total_price_yuan"].known == 1 and cover["total_price_yuan"].total == 2


def test_key_field_coverage_empty_table_is_zero() -> None:
    assert key_field_coverage(pa.table({"a": []})) == []


# ---------------------------------------------------------------------------
# 验收③：各小区 3/6/12 月分布与样本口径一致 + 可支撑等级初判
# ---------------------------------------------------------------------------


def test_per_community_3_6_12_month_buckets_and_support() -> None:
    records = [
        # 雅居乐：3 笔均在过去 3 个月内 → 正式
        *_make_community("雅居乐", ["2026.07.10", "2026.07.15", "2026.08.01"], lines=range(1, 4)),
        # 万科：1 笔落在 6 个月内、3 个月外 → 参考
        *_make_community("万科", ["2026.04.10"], lines=range(4, 5)),
        # 春晖：1 笔落在 12 个月内、6 个月外 → 参考
        *_make_community("春晖", ["2025.10.01"], lines=range(5, 6)),
        # 老区：2 笔但都在最近 12 个月之前 → 无法估值
        *_make_community("老区", ["2024.01.01", "2024.03.01"], lines=range(6, 8)),
    ]
    cleaned, summary, sale, listing = _build(records)
    as_of = _FETCHED.date()
    stats = {s.community: s for s in per_community_deals(sale, as_of)}
    assert stats["雅居乐"].deals_3m == 3
    assert stats["雅居乐"].deals_6m == 3 and stats["雅居乐"].deals_12m == 3
    assert stats["雅居乐"].support == "正式"
    assert stats["万科"].deals_3m == 0
    assert stats["万科"].deals_6m == 1 and stats["万科"].deals_12m == 1
    assert stats["万科"].support == "参考"
    assert stats["春晖"].deals_3m == 0
    assert stats["春晖"].deals_6m == 0 and stats["春晖"].deals_12m == 1
    assert stats["春晖"].support == "参考"
    assert stats["老区"].deals_12m == 0 and stats["老区"].support == "无法估值"


def _make_community(
    community: str, deals: list[str], lines: Iterable[int]
) -> list[LianjiaRecord]:
    return [
        _rec(community=community, raw_line=line, deal=deal)
        for line, deal in zip(lines, deals, strict=True)
    ]


# ---------------------------------------------------------------------------
# 验收②⑦：未来日期与时序矛盾被如实报告
# ---------------------------------------------------------------------------


def test_future_dated_sales_flagged_not_silenced() -> None:
    records = [
        _rec(community="未来", raw_line=1, deal="2026.09.05"),  # 晚于数据截点
        _rec(community="雅居乐", raw_line=2, deal="2026.07.10"),
    ]
    cleaned, summary, sale, listing = _build(records)
    report = build_quality_report(
        records, cleaned, summary, sale, listing,
        snapshot_id="s", source_id="SRC", fetched_at=_FETCHED,
    )
    assert report.future_dated_sales == 1
    assert report.temporal_contradictions == 0


def test_future_dated_formal_sale_excluded_from_monthly_buckets() -> None:
    # RV-WP4-E-01#F1：晚于数据截点的成交不得计入 3/6/12 月分布或支撑初判。
    records = [
        _rec(community="未来广场", raw_line=1, deal="2026.09.05"),  # 晚于 as_of
        _rec(community="过去花园", raw_line=2, deal="2026.07.10"),  # 早于 as_of
    ]
    cleaned, summary, sale, listing = _build(records)
    report = build_quality_report(
        records, cleaned, summary, sale, listing,
        snapshot_id="s", source_id="SRC", fetched_at=_FETCHED,
    )
    stats = {s.community: s for s in report.community_deals}
    assert "未来广场" not in stats  # 纯未来成交小区不再进入分布
    past = stats["过去花园"]
    assert past.deals_12m == 1 and past.support == "参考"


# ---------------------------------------------------------------------------
# 验收②④：Markdown 与 JSON 描述同一冻结报告，且标注正式门槛待 BT-001
# ---------------------------------------------------------------------------


def test_markdown_and_json_cover_all_sections_and_provision_note() -> None:
    records = [_rec(community="雅居乐", raw_line=1, deal="2026.07.10")]
    cleaned, summary, sale, listing = _build(records)
    report = build_quality_report(
        records, cleaned, summary, sale, listing,
        snapshot_id="s", source_id="SRC", fetched_at=_FETCHED,
    )
    md = report_to_markdown(report)
    for heading in (
        "## 1. 输入与输出记录数",
        "## 2. 重复、排除与解析失败数",
        "## 3. 关键字段覆盖率",
        "## 4. 各小区 3/6/12 个月成交分布（正式池）",
        "## 5. 价格、面积、户型异常",
        "## 6. 小区未匹配与冲突清单",
        "## 7. 未来日期与时序矛盾",
        "## 8. 来源新旧与连续性",
        "## 9. 可支撑等级初判",
    ):
        assert heading in md
    assert "正式门槛待 BT-001 核定" in md
    assert "雅居乐" in md

    d = report_to_dict(report)
    assert d["snapshot_id"] == "s"
    counts = d["counts"]
    assert isinstance(counts, dict)
    assert counts["formal_pool"] == report.formal_pool
    assert d["provision_note"] == report.provision_note
    # JSON 与 Markdown 引用同一 formal_pool 数字
    assert f"{report.formal_pool}" in md
    assert d["as_of"] == "2026-08-21"


def test_unmatched_conflicts_empty_for_single_source() -> None:
    cleaned, summary, sale, listing = _build(
        [_rec(community="雅居乐", raw_line=1, deal="2026.07.10")]
    )
    report = build_quality_report(
        [_rec(community="雅居乐", raw_line=1, deal="2026.07.10")],
        cleaned, summary, sale, listing,
        snapshot_id="s", source_id="SRC", fetched_at=_FETCHED,
    )
    assert report.unmatched_conflicts == []

def test_month_cutoff_clamps_day_to_month_length() -> None:
    """ext-sale-ingest-scope-v1-2：as_of 为 29-31 日时月份回滚收敛到月末。"""
    from datetime import date as _date

    from compsval.ingest.quality import _month_cutoff

    assert _month_cutoff(_date(2026, 8, 31), 6) == _date(2026, 2, 28)
    assert _month_cutoff(_date(2026, 8, 31), 12) == _date(2025, 8, 31)
    assert _month_cutoff(_date(2026, 3, 31), 1) == _date(2026, 2, 28)
    assert _month_cutoff(_date(2026, 7, 23), 1) == _date(2026, 6, 23)
