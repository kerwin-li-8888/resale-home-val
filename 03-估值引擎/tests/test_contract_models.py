"""Data-contract models (WP3-D): validation, missing semantics, JSON Schema."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from compsval.contract import models
from compsval.contract.models import (
    ListingEvent,
    ListingPriceBenchmark,
    MissingSemantics,
    PriceBenchmark,
    RawSnapshot,
    SaleEvent,
    SnapshotFormat,
    SnapshotParseStatus,
)
from compsval.contract.sample_data import (
    sample_listings,
    sample_sales,
    sample_snapshots,
    sample_sources,
)

_SHA = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
_FETCHED = datetime(2026, 8, 21, 9, 0, 0, tzinfo=UTC)


def _sale(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "sale_event_id": "SALE-T",
        "source_id": "SRC-005",
        "snapshot_id": "fang_esf-chengjiao-20260821-fang_kangtai_chengjiao_20260821",
        "fetched_at": _FETCHED,
        "parser_version": "parser-0.1.0",
        "content_hash": _SHA,
        "community_id": "C-XXXX0027",
        "original_price_text": "150万",
        "unit_price_formula": "total_price_yuan / area_sqm",
        "price_benchmark": PriceBenchmark.PLATFORM_DISCLOSED,
    }
    base.update(overrides)
    return base


def test_missing_semantics_codes_are_distinct() -> None:
    codes = [member.value for member in MissingSemantics]
    assert codes == [
        "UNKNOWN",
        "MISSING",
        "NOT_APPLICABLE",
        "PARSE_FAILURE",
        "CONFLICT",
    ]
    assert len(set(codes)) == len(codes)


def test_sale_event_normal_construction() -> None:
    sale = SaleEvent(**_sale(total_price_yuan=Decimal("1500000")))
    assert sale.sale_event_id == "SALE-T"
    assert sale.total_price_yuan == Decimal("1500000")
    assert sale.original_price_text == "150万"
    assert sale.anomaly_flag.value == "正常"


def test_sale_event_unknown_price_is_none_not_zero() -> None:
    sale = SaleEvent(**_sale())
    assert sale.total_price_yuan is None
    assert sale.area_sqm is None
    assert sale.layout == "UNKNOWN"
    assert sale.event_date_precision.value == "UNKNOWN"


@pytest.mark.parametrize(
    "field",
    ["total_price_yuan", "area_sqm", "unit_price", "listing_price_yuan"],
)
def test_sale_event_rejects_zero_amount_or_area(field: str) -> None:
    # CXWP3-001：未知金额/面积必须为 None，不得写成 0
    with pytest.raises(ValidationError):
        SaleEvent(**_sale(**{field: Decimal("0")}))


def _listing(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "listing_event_id": "LIST-T",
        "source_id": "SRC-007",
        "snapshot_id": "lianjia-chengjiao_list-20260821-lianjia_targetdistrict_chengjiao_list_20260821",
        "fetched_at": _FETCHED,
        "parser_version": "parser-0.1.0",
        "content_hash": _SHA,
        "community_id": "C-XXXX0027",
        "price_benchmark": ListingPriceBenchmark.PLATFORM_LISTING,
    }
    base.update(overrides)
    return base


def test_listing_event_rejects_zero_price() -> None:
    # CXWP3-001：挂牌总价未知必须为 None，不得写成 0
    with pytest.raises(ValidationError):
        ListingEvent(**_listing(price_yuan=Decimal("0")))


def test_sale_event_parses_decimal_date() -> None:
    sale = SaleEvent(
        **_sale(
            sale_date=date(2026, 8, 10),
            total_price_yuan=Decimal("1500000"),
            area_sqm=Decimal("73.6"),
        )
    )
    assert sale.sale_date == date(2026, 8, 10)
    assert sale.area_sqm == Decimal("73.6")


def test_sale_event_rejects_bad_content_hash() -> None:
    with pytest.raises(ValidationError):
        SaleEvent(**_sale(content_hash="not-a-sha256"))


def test_raw_snapshot_rejects_zero_file_count() -> None:
    with pytest.raises(ValidationError):
        RawSnapshot(
            snapshot_id="s1",
            source_id="SRC-005",
            dataset="chengjiao",
            fetched_at=_FETCHED,
            content_hash=_SHA,
            file_count=0,
            format=SnapshotFormat.PNG,
            parse_status=SnapshotParseStatus.NOT_PARSED,
        )


def test_raw_snapshot_rejects_negative_record_count() -> None:
    with pytest.raises(ValidationError):
        RawSnapshot(
            snapshot_id="s1",
            source_id="SRC-005",
            dataset="chengjiao",
            fetched_at=_FETCHED,
            content_hash=_SHA,
            file_count=1,
            record_count=-1,
            format=SnapshotFormat.PNG,
        )


def test_json_schema_covers_registered_models() -> None:
    for name in models.CONTRACT_MODELS:
        schema = models.json_schema(name)
        assert schema["title"]
        assert schema["properties"]
    # 含枚举字段的模型应产出 $defs 定义；building 全为标量字段，合法无 $defs
    for name in (
        "source_registry",
        "raw_snapshot",
        "sale_event",
        "listing_event",
        "community",
        "community_alias",
        "market_series",
    ):
        assert "$defs" in models.json_schema(name)
    sale_schema = models.json_schema("sale_event")
    assert "sale_event_id" in sale_schema["properties"]
    assert "sale_date" in sale_schema["properties"]
    raw_schema = models.json_schema("raw_snapshot")
    assert raw_schema["properties"]["content_hash"]["pattern"]


def test_json_schema_unknown_model_raises() -> None:
    with pytest.raises(KeyError):
        models.json_schema("not_a_model")


def test_sample_data_is_reproducible() -> None:
    assert sample_sources() == sample_sources()
    assert sample_snapshots() == sample_snapshots()
    assert sample_sales() == sample_sales()
    assert sample_listings() == sample_listings()


def test_sample_sales_cover_missing_semantics() -> None:
    unknown_sale = sample_sales()[1]
    assert unknown_sale.total_price_yuan is None
    assert unknown_sale.area_sqm is None
    assert unknown_sale.unit_price_formula == "UNKNOWN"
    assert unknown_sale.layout == "UNKNOWN"


# ---- EXTFP0-B：数据合同向后兼容扩展（技术方案 §5.2） ----
def test_snapshot_format_extends_backward_compatible() -> None:
    """新格式成员存在，且旧成员与其值完全不变（向后兼容）。"""
    values = {member.name: member.value for member in SnapshotFormat}
    new = {"XLSX", "JPEG", "WEBP", "TIFF", "BINARY"}
    legacy = {k: v for k, v in values.items() if k not in new}
    assert legacy == {
        "PARQUET": "parquet",
        "PNG": "png",
        "TXT": "txt",
        "HTML": "html",
        "JSON": "json",
        "OTHER": "其他",
    }
    assert values["XLSX"] == "xlsx"
    assert values["JPEG"] == "jpeg"
    assert values["WEBP"] == "webp"
    assert values["TIFF"] == "tiff"
    assert values["BINARY"] == "binary"


def test_raw_snapshot_old_json_without_mime_type_deserializes() -> None:
    """旧序列化 JSON（无 mime_type）仍可反序列化，mime_type 默认 None（向后兼容）。"""
    legacy_json = (
        '{"snapshot_id":"s1","source_id":"SRC-005","dataset":"chengjiao",'
        '"fetched_at":"2026-08-21T09:00:00Z","content_hash":"' + _SHA + '",'
        '"file_count":1,"record_count":0,"format":"png",'
        '"parse_status":"未解析","failure_info":"NOT_APPLICABLE",'
        '"prev_snapshot_id":"UNKNOWN"}'
    )
    restored = RawSnapshot.model_validate_json(legacy_json)
    assert restored.format is SnapshotFormat.PNG
    assert restored.mime_type is None


def test_raw_snapshot_mime_type_roundtrip() -> None:
    snap = RawSnapshot(
        snapshot_id="s1",
        source_id="SRC-011",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED,
        content_hash=_SHA,
        file_count=1,
        format=SnapshotFormat.XLSX,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    dumped = snap.model_dump(mode="json")
    assert dumped["mime_type"] == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    restored = RawSnapshot.model_validate(dumped)
    assert restored.mime_type == snap.mime_type
    assert restored.format is SnapshotFormat.XLSX


def test_raw_snapshot_accepts_new_formats() -> None:
    for fmt in (SnapshotFormat.XLSX, SnapshotFormat.JPEG,
                SnapshotFormat.WEBP, SnapshotFormat.TIFF, SnapshotFormat.BINARY):
        snap = RawSnapshot(
            snapshot_id="s1",
            source_id="SRC-011",
            dataset="floorplan_image",
            fetched_at=_FETCHED,
            content_hash=_SHA,
            file_count=1,
            format=fmt,
        )
        assert snap.format is fmt


def test_raw_snapshot_json_schema_includes_mime_type_and_new_formats() -> None:
    raw_schema = models.json_schema("raw_snapshot")
    assert "mime_type" in raw_schema["properties"]
    assert "string" in str(raw_schema["properties"]["mime_type"]["anyOf"])
    format_def = raw_schema["$defs"]["SnapshotFormat"]
    enum_values = set(format_def["enum"])
    assert {"xlsx", "jpeg", "webp", "tiff", "binary"} <= enum_values
    # 旧枚举值仍保留
    assert {"parquet", "png", "txt", "html", "json", "其他"} <= enum_values
