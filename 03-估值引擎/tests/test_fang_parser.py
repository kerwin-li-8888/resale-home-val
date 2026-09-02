"""G3R-A 房天下成交 CSV 解析器测试。"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from compsval.contract.models import EventDatePrecision, MissingSemantics
from compsval.ingest.parsers.fang_esf import (
    FANG_COMMUNITY_REGISTRY,
    parse_fang_esf_csv,
    resolve_fang_community,
)

REPO_ROOT = Path(__file__).resolve().parents[1].parent
EVIDENCE_DIR = (
    REPO_ROOT
    / "01-数据"
    / "raw"
    / "source=fang_esf"
    / "dataset=chengjiao"
    / "fetched_at=20260822"
)


def _table(
    *,
    areas: list[object],
    dates: list[object],
    totals: list[object],
    units: list[object],
    notes: list[object],
) -> pa.Table:
    return pa.table(
        {
            "area_m2": pa.array(areas, type=pa.float64()),
            "deal_date": pa.array(dates, type=pa.date32()),
            "total_price_wan": pa.array(totals, type=pa.int64()),
            "unit_price_yuan_m2": pa.array(units, type=pa.int64()),
            "source_note": pa.array(notes, type=pa.string()),
        }
    )


def test_standardization_known_row() -> None:
    """标准化已知答案：万→元整数、日期日精度、派生单价公式、披露单价保留。"""
    table = _table(
        areas=[54.25],
        dates=[dt.date(2026, 5, 22)],
        totals=[140],
        units=[25807],
        notes=["市场信息"],
    )
    (rec,) = parse_fang_esf_csv(table, "示例小区166")
    assert rec.community == "示例小区166"
    assert rec.deal_date == dt.date(2026, 5, 22)
    assert rec.deal_date_precision == EventDatePrecision.DAY
    assert rec.total_price_yuan == 1_400_000
    assert rec.original_price_text == "140万"
    # 派生单价 = 1400000 / 54.25 = 25806.45… → 取整 25806
    assert rec.unit_price_derived == 25806
    assert rec.unit_price_observed == 25807
    assert rec.unit_price_formula == "total_price_yuan / area_sqm, rounded to integer"
    assert rec.source_note == "市场信息"
    assert rec.layout == MissingSemantics.UNKNOWN.value
    assert rec.orientation == MissingSemantics.UNKNOWN.value
    assert rec.listing_price_yuan is None
    assert rec.raw_start_line == 2  # 表头第 1 行，数据行从第 2 行起


def test_fingerprint_deterministic() -> None:
    """同输入同指纹（确定性）。"""
    table = _table(
        areas=[54.25, 54.25],
        dates=[dt.date(2026, 5, 22), dt.date(2026, 5, 22)],
        totals=[140, 140],
        units=[25807, 25807],
        notes=["市场信息", "市场信息"],
    )
    recs = parse_fang_esf_csv(table, "示例小区166")
    assert len(recs) == 2
    assert recs[0].source_record_id == recs[1].source_record_id


def test_missing_semantics_no_zero() -> None:
    """缺失数值 → None + 对应语义，绝不写 0；日期缺失 → UNKNOWN 精度。"""
    table = _table(
        areas=[None, 54.25, 54.25],
        dates=[None, dt.date(2026, 1, 1), dt.date(2026, 1, 1)],
        totals=[None, None, 100],
        units=[None, None, 10000],
        notes=["", "市场信息", "市场信息"],
    )
    recs = parse_fang_esf_csv(table, "示例小区136")
    assert recs[0].area_sqm is None
    assert recs[0].area_state == MissingSemantics.MISSING
    assert recs[0].deal_date is None
    assert recs[0].deal_date_precision == EventDatePrecision.UNKNOWN
    assert recs[0].total_price_yuan is None
    assert recs[0].unit_price_observed is None
    assert recs[0].source_note == MissingSemantics.UNKNOWN.value

    assert recs[1].total_price_yuan is None  # 总价缺失不写 0
    assert recs[1].original_price_text == MissingSemantics.UNKNOWN.value
    assert recs[1].unit_price_derived is None

    assert recs[2].total_price_yuan == 1_000_000
    assert recs[2].unit_price_derived == 18433  # 1000000 / 54.25 = 18433.18… → 取整
    assert recs[2].unit_price_observed == 10000


def test_nonpositive_values_parse_failure() -> None:
    """面积/总价 <= 0 → 面积 PARSE_FAILURE、总价不设值（不臆测）。"""
    table = _table(
        areas=[0.0, -1.0, 54.25],
        dates=[dt.date(2026, 1, 1), dt.date(2026, 1, 1), dt.date(2026, 1, 1)],
        totals=[100, 100, 0],
        units=[10000, 10000, 0],
        notes=["市场信息", "市场信息", "市场信息"],
    )
    recs = parse_fang_esf_csv(table, "示例小区121")
    assert recs[0].area_state == MissingSemantics.PARSE_FAILURE
    assert recs[0].area_sqm is None
    assert recs[1].area_state == MissingSemantics.PARSE_FAILURE
    assert recs[2].total_price_yuan is None  # 总价 0 → 不设值
    assert recs[2].original_price_text == "0万"  # 原文保留
    assert recs[2].unit_price_observed is None  # 单价 0 → 不设 observed


def test_bad_date_string_precision_unknown() -> None:
    """坏日期字符串 → deal_date None、精度 UNKNOWN（防御字符串列）。"""
    table = pa.table(
        {
            "area_m2": pa.array([54.25], type=pa.float64()),
            "deal_date": pa.array(["2026/05/22"], type=pa.string()),
            "total_price_wan": pa.array([140], type=pa.int64()),
            "unit_price_yuan_m2": pa.array([25807], type=pa.int64()),
            "source_note": pa.array(["市场信息"], type=pa.string()),
        }
    )
    (rec,) = parse_fang_esf_csv(table, "示例小区166")
    assert rec.deal_date is None
    assert rec.deal_date_precision == EventDatePrecision.UNKNOWN


def test_iso_string_date_parsed() -> None:
    """ISO 字符串日期列 → 正常解析为 DAY。"""
    table = pa.table(
        {
            "area_m2": pa.array([54.25], type=pa.float64()),
            "deal_date": pa.array(["2026-05-22"], type=pa.string()),
            "total_price_wan": pa.array([140], type=pa.int64()),
            "unit_price_yuan_m2": pa.array([25807], type=pa.int64()),
            "source_note": pa.array(["市场信息"], type=pa.string()),
        }
    )
    (rec,) = parse_fang_esf_csv(table, "示例小区166")
    assert rec.deal_date == dt.date(2026, 5, 22)
    assert rec.deal_date_precision == EventDatePrecision.DAY


def test_missing_column_raises() -> None:
    """缺列（schema 漂移）→ 明确报错，不静默错读。"""
    table = pa.table({"area_m2": pa.array([54.25], type=pa.float64())})
    with pytest.raises(ValueError, match="缺少必要列"):
        parse_fang_esf_csv(table, "示例小区166")


def test_community_registry_full_coverage() -> None:
    """快照→小区映射注册表覆盖 7 个补数小区且可溯源。"""
    expected = {
        "2811405748": "示例小区166",
        "2811007476": "示例小区136",
        "2811021647": "拾光里",
        "2811019201": "示例小区121",
        "2811052010": "示例小区132",
        "2811007655": "示例小区203",
        "2811034445": "示例小区130",
    }
    assert expected == FANG_COMMUNITY_REGISTRY


def test_resolve_fang_community_from_url() -> None:
    """从来源 URL 解析小区标准名；未登记/无 ID → None。"""
    assert resolve_fang_community(
        "https://esf.fang.com.example/loupan/2811405748/chengjiao/"
    ) == "示例小区166"
    assert (
        resolve_fang_community("https://esf.fang.com.example/loupan/9999999999/chengjiao/")
        is None
    )
    assert resolve_fang_community("local file import: /tmp/x.csv") is None
    assert resolve_fang_community(None) is None


# ---------------------------------------------------------------------------
# 真实证据文件集成（数据在仓库内；文件缺失时跳过，不阻塞离线条）
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("filename", "expected_rows", "community"),
    [
        ("fang_nanbei_chengjiao_20260822.csv", 60, "示例小区166"),
        ("fang_hongyu_chengjiao_20260822.csv", 34, "示例小区136"),
        ("fang_luomajiari_chengjiao_20260822.csv", 88, "拾光里"),
        ("fang_cuicheng_chengjiao_20260822.csv", 160, "示例小区121"),
        ("fang_guangda_chengjiao_20260822.csv", 167, "示例小区132"),
        ("fang_zhonglv_chengjiao_20260822.csv", 44, "示例小区203"),
        ("fang_fuji_chengjiao_20260822.csv", 196, "示例小区130"),
    ],
)
def test_real_evidence_csv_counts(
    filename: str, expected_rows: int, community: str
) -> None:
    """真实补数 CSV 全量解析，条数与补数快照索引 §3 一致（60/34/88/160/167/44/196）。"""
    path = EVIDENCE_DIR / filename
    if not path.is_file():
        pytest.skip(f"证据文件缺失（跳过集成测试）：{path}")
    table = pacsv.read_csv(path)
    recs = parse_fang_esf_csv(table, community)
    assert len(recs) == expected_rows
    # 全量标准化抽查：日期/总价/面积均非缺失
    assert all(r.deal_date is not None for r in recs)
    assert all(r.deal_date_precision == EventDatePrecision.DAY for r in recs)
    assert all(r.total_price_yuan is not None and r.total_price_yuan > 0 for r in recs)
    assert all(r.area_sqm is not None and r.area_sqm > 0 for r in recs)
    assert all(r.source_record_id and r.raw_start_line == 2 + i for i, r in enumerate(recs))


def test_real_evidence_derived_price_is_total_over_area() -> None:
    """真实数据：派生单价严格等于 总价/面积 取整（公式契约）；披露单价如实保留。

    平台披露单价可能不等于 总价/面积（房天下实测存在 ~0.3% 偏差），解析器
    职责是同时保留派生价（估值用）与披露价（观察值），不强行对齐。
    """
    path = EVIDENCE_DIR / "fang_nanbei_chengjiao_20260822.csv"
    if not path.is_file():
        pytest.skip("证据文件缺失（跳过集成测试）")
    recs = parse_fang_esf_csv(pacsv.read_csv(path), "示例小区166")
    for rec in recs:
        if rec.area_sqm is None or rec.total_price_yuan is None:
            continue
        expected = (rec.total_price_yuan / rec.area_sqm).to_integral_value()
        assert rec.unit_price_derived == expected
        assert rec.unit_price_observed is not None
