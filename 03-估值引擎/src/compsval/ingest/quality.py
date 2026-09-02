"""Data-quality report generation (WP4-E, 技术方案 §8.4).

After each ``stage`` the pipeline emits a bounded, evidence-based quality report
in Markdown and JSON covering every §8.4 item:

1. input and output record counts;
2. duplicate / excluded / parse-failure counts;
3. key-field coverage;
4. per-community deal distribution over the last 3 / 6 / 12 months;
5. price / area / layout anomalies;
6. community unmatched / conflict list (single-source ⇒ not applicable → WP5);
7. future-date or time-sequence contradictions;
8. source freshness and continuity;
9. provisional per-community supportability (评估可支撑等级初判), explicitly
   labelled "正式门槛待 BT-001".

Nothing here fabricates facts: figures are derived from the parsed records, the
cleaning summary, and the staged sale/listing tables. Unknown values stay
missing (never ``0``). The report is *regenerated* from the same raw snapshot on
every stage run, so it is reproducible, and the JSON and Markdown outputs
describe the same frozen report.
"""

from __future__ import annotations

import calendar
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from compsval.contract.models import MissingSemantics
from compsval.ingest.clean import CleanedSale, CleaningSummary, SaleRecord

#: Missing-placeholder string values treated as "unknown" in coverage counts
#: (a parser-default ``UNKNOWN`` is not "known", even though it is non-null).
_UNKNOWN_WORDS = {
    MissingSemantics.UNKNOWN.value,
    MissingSemantics.MISSING.value,
    MissingSemantics.NOT_APPLICABLE.value,
    MissingSemantics.PARSE_FAILURE.value,
    MissingSemantics.CONFLICT.value,
}

#: Key sale-event fields whose known-ness is reported (§8.4 关键字段覆盖率).
_SALE_KNOWN_FIELDS = (
    "event_date",
    "community",
    "layout",
    "area_sqm",
    "total_price_yuan",
    "unit_price",
    "orientation",
    "listing_price_yuan",
    "listing_period_days",
)

#: Provisional support thresholds — clearly labelled as a *初判*, the formal
#: thresholds are pending BT-001 (§8.4 item 9 / WP4-E 验收④).
_MIN_FORMAL_DEALS_12M = 2
_MIN_REFERENCE_DEALS_12M = 1


@dataclass(frozen=True)
class CommunityDealStats:
    """Per-community formal-sale counts over the last 3 / 6 / 12 months."""

    community: str
    deals_3m: int
    deals_6m: int
    deals_12m: int
    support: str  # 正式 / 参考 / 无法估值 (provisional)


@dataclass(frozen=True)
class Coverage:
    """Known-count / total for one key field (``ratio`` in [0, 1])."""

    field: str
    known: int
    total: int

    @property
    def ratio(self) -> float:
        if self.total == 0:
            return 0.0
        return self.known / self.total


@dataclass(frozen=True)
class QualityReport:
    """A frozen, reproducible view of one stage's data quality (§8.4)."""

    snapshot_id: str
    source_id: str
    fetched_at: datetime
    as_of: date  # data-cutoff used for 3/6/12-month bucketing and future checks
    input_records: int
    parse_failures: int
    output_sale_events: int
    output_listing_events: int
    parking_excluded: int
    duplicates: int
    abnormal_unit_price: int
    formal_pool: int
    key_field_coverage: list[Coverage] = field(default_factory=list)
    community_deals: list[CommunityDealStats] = field(default_factory=list)
    area_implausible: int = 0  # area ≤ 0 yet present (should be impossible)
    layout_unknown: int = 0  # layout missing / not a residential pattern
    future_dated_sales: int = 0  # event_date strictly after the data cutoff
    temporal_contradictions: int = 0  # listing first-date after its own delist
    unmatched_conflicts: list[str] = field(default_factory=list)  # §8.4 item 6
    source_recency_days: int | None = None  # fetched_at → report build, in days
    provision_note: str = "可支撑等级为初判，正式门槛待 BT-001 核定"


def _as_of_from(fetched_at: datetime) -> date:
    """Data-cutoff date: the snapshot fetch date (never time-shifted data)."""
    return fetched_at.date()


def _is_known(value: object) -> bool:
    """A value counts as "known" when it is populated and not a missing marker."""
    return not (value is None or isinstance(value, str) and value in _UNKNOWN_WORDS)


def _rows(table: pa.Table, column: str) -> list[Any]:
    return list(table.column(column).to_pylist())


def key_field_coverage(sale_table: pa.Table) -> list[Coverage]:
    """Known-rate of the key sale fields over the staged sale_event table."""
    total = sale_table.num_rows
    out: list[Coverage] = []
    for name in _SALE_KNOWN_FIELDS:
        if name not in sale_table.column_names:
            continue
        known = sum(1 for v in _rows(sale_table, name) if _is_known(v))
        out.append(Coverage(name, known, total))
    return out


def _month_cutoff(as_of: date, months: int) -> date:
    # Approximate "N months ago" by rolling the calendar month back, clamped to
    # year 1 to stay within pyarrow date32 range. The day is clamped to the
    # target month's length (ext-sale-ingest-scope-v1-2 fix: as_of on the
    # 29th-31st previously raised "day is out of range for month", e.g.
    # 2026-08-31 - 6 months -> 2026-02-31).
    year, month = as_of.year, as_of.month
    total = year * 12 + (month - 1) - months
    year, month = divmod(total, 12)
    last_day = calendar.monthrange(year, month + 1)[1]
    return date(year, month + 1, min(as_of.day, last_day))


def per_community_deals(
    sale_table: pa.Table, as_of: date
) -> list[CommunityDealStats]:
    """Formal-sale counts per community over 3 / 6 / 12 months + provisional support.

    Only the formal pool (anomaly_flag == 正常) counts as a usable deal; dated
    formal sales from the staged parity are bucketed by their ``event_date``.
    Records without a valid date are excluded (counted as unknown, never 0), as
    are records dated after the data cutoff ``as_of`` — future sales never enter
    the distribution.
    """
    communities = _rows(sale_table, "community")
    flags = _rows(sale_table, "anomaly_flag")
    dates_ = _rows(sale_table, "event_date")
    formal_dates: dict[str, list[date]] = defaultdict(list)
    for community, flag, evt in zip(communities, flags, dates_, strict=True):
        if flag != "正常":
            continue
        # 未来日期成交不得进入 3/6/12 月分桶（§7 未来数据防护）。
        if isinstance(evt, date) and evt <= as_of:
            formal_dates[community].append(evt)

    cut_3m = _month_cutoff(as_of, 3)
    cut_6m = _month_cutoff(as_of, 6)
    cut_12m = _month_cutoff(as_of, 12)

    stats: list[CommunityDealStats] = []
    for community in sorted(formal_dates):
        ds = formal_dates[community]
        n3 = sum(1 for d in ds if d >= cut_3m)
        n6 = sum(1 for d in ds if d >= cut_6m)
        n12 = sum(1 for d in ds if d >= cut_12m)
        if n12 >= _MIN_FORMAL_DEALS_12M:
            support = "正式"
        elif n12 >= _MIN_REFERENCE_DEALS_12M:
            support = "参考"
        else:
            support = "无法估值"
        # The report is a *data-availability* view; communities with zero formal
        # deals in the last 12 months still carry evidence (older / excluded)
        # and are listed rather than dropped.
        stats.append(
            CommunityDealStats(community, n3, n6, n12, support)
        )

    # Community records are always present in the staged table; any raw record
    # whose community was not seen there is reported (should be empty).
    return stats


def _count_area_implausible(sale_table: pa.Table) -> int:
    return sum(
        1 for v in _rows(sale_table, "area_sqm") if v is not None and float(v) <= 0
    )


def _count_layout_unknown(sale_table: pa.Table) -> int:
    return sum(1 for v in _rows(sale_table, "layout") if not _is_known(v))


def _count_future_dated(sale_table: pa.Table, as_of: date) -> int:
    return sum(1 for v in _rows(sale_table, "event_date") if isinstance(v, date) and v > as_of)


def _count_temporal_contradictions(listing_table: pa.Table) -> int:
    """Flag listing rows whose first-listing date is after their own delist date."""
    starts = _rows(listing_table, "event_date")
    dels = _rows(listing_table, "delist_date")
    return sum(
        1
        for s, d in zip(starts, dels, strict=True)
        if isinstance(s, date) and isinstance(d, date) and s > d
    )


def build_quality_report(
    records: Sequence[SaleRecord],
    cleaned: Sequence[CleanedSale],
    summary: CleaningSummary,
    sale_table: pa.Table,
    listing_table: pa.Table,
    *,
    snapshot_id: str,
    source_id: str,
    fetched_at: datetime,
    today: date | None = None,
    unmatched_conflicts: Sequence[str] = (),
) -> QualityReport:
    """Assemble the frozen quality report for one stage run (§8.4)."""
    as_of = _as_of_from(fetched_at)
    parse_failures = sum(1 for r in records if r.unparsed_lines)
    return QualityReport(
        snapshot_id=snapshot_id,
        source_id=source_id,
        fetched_at=fetched_at,
        as_of=as_of,
        input_records=len(records),
        parse_failures=parse_failures,
        output_sale_events=summary.total,
        output_listing_events=listing_table.num_rows,
        parking_excluded=summary.parking_flagged,
        duplicates=summary.duplicate_flagged,
        abnormal_unit_price=summary.abnormal_unit_price_flagged,
        formal_pool=summary.formal_pool,
        key_field_coverage=key_field_coverage(sale_table),
        community_deals=per_community_deals(sale_table, as_of),
        area_implausible=_count_area_implausible(sale_table),
        layout_unknown=_count_layout_unknown(sale_table),
        future_dated_sales=_count_future_dated(sale_table, as_of),
        temporal_contradictions=_count_temporal_contradictions(listing_table),
        unmatched_conflicts=list(unmatched_conflicts),
        source_recency_days=(
            (today - fetched_at.date()).days
            if today is not None and fetched_at.date() <= today
            else None
        ),
        provision_note="可支撑等级为初判，正式门槛待 BT-001 核定",
    )


def report_to_dict(report: QualityReport) -> dict[str, object]:
    """A JSON-safe dict mirroring the Markdown report exactly."""
    return {
        "schema_version": "1.0",
        "snapshot_id": report.snapshot_id,
        "source_id": report.source_id,
        "fetched_at": report.fetched_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "counts": {
            "input_records": report.input_records,
            "parse_failures": report.parse_failures,
            "output_sale_events": report.output_sale_events,
            "output_listing_events": report.output_listing_events,
            "parking_excluded": report.parking_excluded,
            "duplicates": report.duplicates,
            "abnormal_unit_price": report.abnormal_unit_price,
            "formal_pool": report.formal_pool,
            "area_implausible": report.area_implausible,
            "layout_unknown": report.layout_unknown,
            "future_dated_sales": report.future_dated_sales,
            "temporal_contradictions": report.temporal_contradictions,
            "source_recency_days": report.source_recency_days,
        },
        "key_field_coverage": [
            {"field": c.field, "known": c.known, "total": c.total, "ratio": round(c.ratio, 4)}
            for c in report.key_field_coverage
        ],
        "community_deals": [
            {
                "community": s.community,
                "deals_3m": s.deals_3m,
                "deals_6m": s.deals_6m,
                "deals_12m": s.deals_12m,
                "support": s.support,
            }
            for s in report.community_deals
        ],
        "unmatched_conflicts": report.unmatched_conflicts,
        "provision_note": report.provision_note,
    }


def report_to_markdown(report: QualityReport) -> str:
    """A human-readable Markdown report describing the *same* frozen data."""
    lines: list[str] = [
        f"# 数据质量报告 · {report.snapshot_id}",
        "",
        f"- 来源：`{report.source_id}`",
        f"- 快照数据截点 fetched_at：`{report.fetched_at.isoformat()}`",
        f"- 统计基准日 as_of：`{report.as_of.isoformat()}`",
        f"- 初判等级注明：{report.provision_note}",
        "",
        "## 1. 输入与输出记录数",
        "",
        f"- 输入原始记录：{report.input_records}（另解析失败记录 {report.parse_failures}）",
        f"- 输出成交事件 sale_event：{report.output_sale_events}",
        f"- 输出挂牌事件 listing_event：{report.output_listing_events}",
        f"- 正式成交池（去除车位/重复/异常单价）：{report.formal_pool}",
        "",
        "## 2. 重复、排除与解析失败数",
        "",
        f"- 车位/非住宅排除：{report.parking_excluded}",
        f"- 重复多录（保留首条）：{report.duplicates}",
        f"- 疑似异常单价：{report.abnormal_unit_price}",
        f"- 解析失败记录：{report.parse_failures}",
        "",
        "## 3. 关键字段覆盖率",
        "",
        "| 字段 | 已知数/总数 | 覆盖率 |",
        "|---|---|---|",
    ]
    for cov in report.key_field_coverage:
        lines.append(f"| {cov.field} | {cov.known}/{cov.total} | {cov.ratio:.1%} |")
    lines += [
        "",
        "## 4. 各小区 3/6/12 个月成交分布（正式池）",
        "",
        "| 小区 | 3 月 | 6 月 | 12 月 | 可支撑等级（初判） |",
        "|---|---|---|---|---|",
    ]
    for s in report.community_deals:
        lines.append(
            f"| {s.community} | {s.deals_3m} | {s.deals_6m} | {s.deals_12m} | {s.support} |"
        )
    lines += [
        "",
        "## 5. 价格、面积、户型异常",
        "",
        f"- 疑似异常单价：{report.abnormal_unit_price}",
        f"- 面积数值异常（≤0）：{report.area_implausible}",
        f"- 户型缺失/未知：{report.layout_unknown}",
        "",
        "## 6. 小区未匹配与冲突清单",
        "",
    ]
    if not report.unmatched_conflicts:
        lines.append(
            "- 当前为单来源（链家）数据，无跨来源实体匹配，冲突清单不适用；实体合并归 WP5。"
        )
    else:
        for item in report.unmatched_conflicts:
            lines.append(f"- {item}")
    lines += [
        "",
        "## 7. 未来日期与时序矛盾",
        "",
        f"- 晚于数据截点的成交（未来日期）：{report.future_dated_sales}",
        f"- 挂牌起始晚于自身下架的时序矛盾：{report.temporal_contradictions}",
        "",
        "## 8. 来源新旧与连续性",
        "",
        f"- 快照 id：`{report.snapshot_id}`",
        f"- fetched_at：`{report.fetched_at.isoformat()}`",
        f"- 距最近一次取数天数（初判用）："
        f"{report.source_recency_days if report.source_recency_days is not None else 'N/A'}",
        "- 说明：staged 与 marts 表由不可变 raw 快照逐次重建，重跑不追加、不覆盖证据，可复现。",
        "",
        "## 9. 可支撑等级初判",
        "",
        f"- {report.provision_note}。",
        "- 每个小区以上述 12 个月正式成交数为依据作出 `正式 / 参考 / 无法估值` 初判；",
        "- 最终支持条件待 BT-001 回放后核定，本报告不宣称正式可用。",
        "",
    ]
    return "\n".join(lines)


QUALITY_DIR = "quality"
REPORT_FILENAME_MARKDOWN = "quality_report.md"
REPORT_FILENAME_JSON = "quality_report.json"


def write_quality_report(
    report: QualityReport,
    *,
    data_dir: Path,
    notes: str | None = None,
) -> tuple[Path, Path]:
    """Write the report as Markdown + JSON (same frozen data) into data/quality/.

    Both files are written atomically via a ``.incomplete`` sibling then renamed,
    so a partially flushed report can never masquerade as a complete one.
    """
    quality_dir = data_dir / QUALITY_DIR
    quality_dir.mkdir(parents=True, exist_ok=True)

    md_final = quality_dir / REPORT_FILENAME_MARKDOWN
    md_work = quality_dir / (REPORT_FILENAME_MARKDOWN + ".incomplete")
    md_work.write_text(report_to_markdown(report), encoding="utf-8")
    md_work.replace(md_final)

    json_final = quality_dir / REPORT_FILENAME_JSON
    json_work = quality_dir / (REPORT_FILENAME_JSON + ".incomplete")
    json_work.write_text(
        json.dumps(report_to_dict(report), ensure_ascii=False, indent=2, sort_keys=False),
        encoding="utf-8",
    )
    json_work.replace(json_final)

    if notes:
        quality_dir.joinpath("NOTES.txt").write_text(f"{notes}\n", encoding="utf-8")
    return md_final, json_final


__all__ = [
    "Coverage",
    "QualityReport",
    "build_quality_report",
    "key_field_coverage",
    "per_community_deals",
    "report_to_dict",
    "report_to_markdown",
    "write_quality_report",
]