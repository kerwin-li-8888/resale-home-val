"""Unit tests for WP4-C sale cleaning, dedup, and anomaly flagging (DATA-006).

Covers parking exclusion, transaction-identity dedup, community-median abnormal
unit-price flagging, the RV-WP4-B-01 F1 rounding tolerance, provenance
traceability to the original ``raw_start_line``, and atomic staged
``sale_event`` parquet + DerivedManifest writes. There is no darker raw-snapshot
mutation here.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from compsval.contract.models import AnomalyFlag
from compsval.ingest.clean import (
    ROUNDING_TOLERANCE_YUAN,
    clean_sales,
    sale_event_table,
    transaction_key,
    within_rounding_tolerance,
    write_sale_event_stage,
)
from compsval.ingest.manifests import DerivedManifest, InputRef
from compsval.ingest.parsers.lianjia import LianjiaRecord, parse_lianjia_txt

_NOW = datetime(2026, 8, 21, 3, 14, 0)


def _residential(
    *,
    community: str,
    area: str,
    total_wan: str,
    unit_derived: str | None = None,
    listing_wan: str | None = None,
    raw_line: int | None = None,
) -> LianjiaRecord:
    """A minimal residential record with just enough identity fields filled."""
    total = (Decimal(total_wan) * 10000).to_integral_value()
    return LianjiaRecord(
        community=community,
        layout="2室1厅",
        area_sqm=Decimal(area),
        deal_date=date(2026, 7, 19),
        total_price_yuan=total,
        original_price_text=f"{total_wan}万",
        unit_price_derived=Decimal(unit_derived) if unit_derived else None,
        unit_price_observed=Decimal(unit_derived) if unit_derived else None,
        unit_price_formula=(
            "total_price_yuan / area_sqm, rounded to integer" if unit_derived else ""
        ),
        listing_price_yuan=(Decimal(listing_wan) * 10000).to_integral_value()
        if listing_wan
        else None,
        raw_start_line=raw_line,
    )


def _parking(*, community: str, raw_line: int | None = None) -> LianjiaRecord:
    return LianjiaRecord(
        community=community,
        layout="车位",
        area_sqm=None,
        raw_start_line=raw_line,
    )


# ---------------------------------------------------------------------------
# parking / non-residential exclusion (验收①、样本§4 示例小区220/澜庭泊府)
# ---------------------------------------------------------------------------


def test_parking_flagged_and_excluded_from_formal_pool() -> None:
    recs = [
        _parking(community="示例小区220二期", raw_line=1),
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived="28200",
            listing_wan="380",
            raw_line=2,
        ),
        _parking(community="澜庭泊府(住宅)", raw_line=3),
    ]
    cleaned, summary = clean_sales(recs)
    # exactly the two parking records flagged; the residential one is normal
    parking = [s for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_PARKING]
    normal = [s for s in cleaned if s.anomaly_flag == AnomalyFlag.NORMAL]
    assert len(parking) == 2
    assert {p.record.community for p in parking} == {"示例小区220二期", "澜庭泊府(住宅)"}
    assert len(normal) == 1
    assert summary.parking_flagged == 2
    assert summary.formal_pool == 1


def test_parking_real_record_from_txt() -> None:
    # 真实车位块：无面积 → clean 直接标记，不进入正式池
    park = [
        "示例小区220二期 车位",
        "南 | 其他2026.07.2127万",
        "地下室(共32层) 塔楼22595元/平",
        "房屋满五年近地铁",
        "挂牌30万成交周期115天",
        "申霄汉免费咨询",
    ]
    recs = parse_lianjia_txt(park)
    cleaned, summary = clean_sales(recs)
    assert len(recs) == 1
    assert cleaned[0].anomaly_flag == AnomalyFlag.SUSPECT_PARKING
    assert summary.parking_flagged == 1
    assert summary.formal_pool == 0


# ---------------------------------------------------------------------------
# duplicate detection (验收②、样本§4 瑾雅苑两录)
# ---------------------------------------------------------------------------


def _duplicate_blocks(second_listing_days: str) -> list[str]:
    first = [
        "瑾雅苑 3室2厅 109.6平米",
        "南 | 精装2026.07.19 309万",
        "高楼层(共25层) 2008年塔楼28300元/平",
        "挂牌380万成交周期44天",
    ]
    # 同一笔交易二次上架：layout（3室1厅）与周期（47天）不同，属性其余一致
    second = [
        "瑾雅苑 3室1厅 109.6平米",
        "南 | 精装2026.07.19 309万",
        "高楼层(共25层) 2008年塔楼28300元/平",
        f"挂牌380万成交周期{second_listing_days}天",
    ]
    return first + second


def test_transaction_key_ignores_layout_and_listing_period() -> None:
    # 瑾雅苑两条：户型/周期不同、其余成交身份一致 → 去重键相同
    a, b = parse_lianjia_txt(_duplicate_blocks("47"))
    assert a.layout != b.layout  # 3室2厅 vs 3室1厅
    assert transaction_key(a) == transaction_key(b)


def test_true_duplicate_flagged_keeping_first() -> None:
    a, b = parse_lianjia_txt(_duplicate_blocks("47"))
    assert a.raw_start_line != b.raw_start_line
    cleaned, summary = clean_sales([a, b])
    assert cleaned[0].anomaly_flag == AnomalyFlag.NORMAL  # 首条保留
    assert cleaned[1].anomaly_flag == AnomalyFlag.SUSPECT_DUPLICATE
    assert cleaned[1].dedup_key == transaction_key(a)
    assert summary.duplicate_flagged == 1
    assert summary.formal_pool == 1
    # 标记可追溯到首条原记录
    assert f"@{a.raw_start_line}" in cleaned[1].flag_note


def test_fingerprint_duplicates_also_captured() -> None:
    # 完全相同的两块（同指纹）也按内容视为重复 → 仅保留一条
    block = _duplicate_blocks("44")
    recs = parse_lianjia_txt(block + block)
    assert len(recs) == 4
    cleaned, summary = clean_sales(recs)
    assert summary.duplicate_flagged == 3
    assert summary.formal_pool == 1


def test_duplicate_with_missing_identity_field_never_collapsed() -> None:
    # 反例：缺挂牌价或总价时无法认定重复 → 不静默合并
    recs = [
        _residential(community="某小区", area="80", total_wan="150", listing_wan=None, raw_line=1),
        _residential(community="某小区", area="80", total_wan="150", listing_wan=None, raw_line=2),
    ]
    cleaned, summary = clean_sales(recs)
    assert all(s.anomaly_flag == AnomalyFlag.NORMAL for s in cleaned)
    assert summary.duplicate_flagged == 0
    assert summary.formal_pool == 2


# ---------------------------------------------------------------------------
# abnormal unit price (验收③、样本§4 示例小区154 65%)
# ---------------------------------------------------------------------------


def test_abnormal_unit_price_flagged_not_silently_adopted() -> None:
    # 同小区 5 条住宅，其中一条单价偏离中位约 65% → 应被标记而非采纳
    recs = [
        _residential(
            community="示例小区154",
            area="76.42",
            total_wan="110",
            unit_derived="14395",
            listing_wan="110",
            raw_line=1,
        ),
        _residential(
            community="示例小区154",
            area="103.70",
            total_wan="150",
            unit_derived="14465",
            listing_wan="150",
            raw_line=2,
        ),
        _residential(
            community="示例小区154",
            area="67.25",
            total_wan="97",
            unit_derived="14424",
            listing_wan="97",
            raw_line=3,
        ),
        _residential(
            community="示例小区154",
            area="52.11",
            total_wan="198",
            unit_derived="38000",
            listing_wan="200",
            raw_line=4,
        ),
        _residential(
            community="示例小区154",
            area="88.34",
            total_wan="128",
            unit_derived="14490",
            listing_wan="128",
            raw_line=5,
        ),
    ]
    cleaned, summary = clean_sales(recs)
    flagged = [c for c in cleaned if c.anomaly_flag == AnomalyFlag.SUSPECT_ABNORMAL_UNIT_PRICE]
    assert len(flagged) == 1
    assert flagged[0].record.raw_start_line == 4  # 单价 38000 的那条
    assert "38000" in flagged[0].flag_note
    assert summary.abnormal_unit_price_flagged == 1
    assert summary.formal_pool == 4


def test_no_abnormal_flag_without_community_baseline() -> None:
    # 反例：小区仅 1 条住宅（或清一色缺失）→ 无双记录基线，不标记
    recs = [
        _residential(
            community="孤例小区", area="60", total_wan="300", unit_derived="50000", raw_line=1
        )
    ]
    cleaned, summary = clean_sales(recs)
    assert cleaned[0].anomaly_flag == AnomalyFlag.NORMAL
    assert summary.abnormal_unit_price_flagged == 0


def test_parking_and_duplicate_never_also_flagged_abnormal() -> None:
    # 反例：车位无面积、重复已被标记 → 不应再被打上异常单价
    park = _parking(community="瑾雅苑", raw_line=0)
    a, b = (
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived="28200",
            listing_wan="380",
            raw_line=1,
        ),
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived="28200",
            listing_wan="380",
            raw_line=2,
        ),
    )
    cleaned, summary = clean_sales([park, a, b])
    assert summary.abnormal_unit_price_flagged == 0
    parking_flags = {
        s.record.raw_start_line for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_PARKING
    }
    dup_flags = {
        s.record.raw_start_line for s in cleaned if s.anomaly_flag == AnomalyFlag.SUSPECT_DUPLICATE
    }
    assert parking_flags == {0}
    assert dup_flags == {2}


# ---------------------------------------------------------------------------
# F1 rounding tolerance (RV-WP4-B-01 F1)
# ---------------------------------------------------------------------------


def test_within_rounding_tolerance_absorbs_1_yuan_gap() -> None:
    # 派生按 ROUND_HALF_EVEN、平台按 ceil，系统性差 1 元/㎡ → 判为同一观察价
    assert within_rounding_tolerance(Decimal("30700"), Decimal("30701")) is True
    assert within_rounding_tolerance(Decimal("30700"), Decimal("30700")) is True


def test_within_rounding_tolerance_not_masking_real_deviation() -> None:
    # 反例：真实偏差远超舍入噪声 → 不落入容忍
    assert within_rounding_tolerance(Decimal("30700"), Decimal("38000")) is False
    assert Decimal("2") == ROUNDING_TOLERANCE_YUAN


def test_within_rounding_tolerance_unknown_side() -> None:
    # 反例：缺失一侧无法比较 → False（由派生横截面判断兜底）
    assert within_rounding_tolerance(Decimal("30700"), None) is False
    assert within_rounding_tolerance(None, Decimal("30700")) is False


# ---------------------------------------------------------------------------
# staged sale_event derivation + provenance
# ---------------------------------------------------------------------------


def test_sale_event_table_schema_and_missing_discipline() -> None:
    recs = [
        _parking(community="示例小区220二期", raw_line=1),
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived=None,
            listing_wan="380",
            raw_line=2,
        ),
    ]
    cleaned, _ = clean_sales(recs)
    table = sale_event_table(cleaned, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW)
    assert table.num_rows == 2
    names = table.column_names
    assert "unit_price_observed" in names
    # 缺失数值保持 None（不为 0）
    col = table.column("area_sqm").to_pylist()
    assert col[0] is None and col[1] == 109.6
    assert table.column("unit_price").to_pylist()[1] is None
    # 枚举写入显式值
    assert table.column("anomaly_flag").to_pylist()[0] == "疑似车位"
    # 溯源链
    assert table.column("raw_locator").to_pylist() == ["1", "2"]
    assert table.column("community").to_pylist() == ["示例小区220二期", "瑾雅苑"]


def test_sale_event_table_event_id_uses_raw_line() -> None:
    recs = [
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived="28200",
            listing_wan="380",
            raw_line=7,
        )
    ]
    cleaned, _ = clean_sales(recs)
    table = sale_event_table(cleaned, source_id="SRC-007", snapshot_id="s", fetched_at=_NOW)
    assert table.column("sale_event_id").to_pylist() == ["SRC-007-line7"]


def test_write_sale_event_stage_atomic_manifest(tmp_path: Path) -> None:
    recs = [
        _residential(
            community="瑾雅苑",
            area="109.6",
            total_wan="309",
            unit_derived="28200",
            listing_wan="380",
            raw_line=4,
        ),
        _parking(community="示例小区220二期", raw_line=5),
    ]
    cleaned, _ = clean_sales(recs)
    table = sale_event_table(cleaned, source_id="SRC-007", snapshot_id="snap-1", fetched_at=_NOW)
    final = write_sale_event_stage(
        table,
        data_dir=tmp_path,
        inputs=[InputRef(dataset="chengjiao_list", fetched_at="2026-08-21T03:14:00")],
        notes="WP4-C self-check",
    )
    assert final.exists()
    # 无残留 .incomplete 中间态
    assert not (tmp_path / "staged" / "sale_event.parquet.incomplete").exists()
    # manifest 溯源正确
    manifest = DerivedManifest.model_validate_json(
        (tmp_path / "staged" / "sale_event.manifest.json").read_text(encoding="utf-8")
    )
    assert manifest.layer == "staged"
    assert manifest.table == "sale_event"
    assert manifest.row_count == 2
    assert manifest.inputs[0].dataset == "chengjiao_list"
    assert manifest.package_version  # 非空
    # 读回的 parquet 行数一致
    assert pa.parquet.read_table(final).num_rows == 2
