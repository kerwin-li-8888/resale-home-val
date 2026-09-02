"""LJ-C 链家成交 HTML 解析器与交叉校验测试。"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from compsval.contract.models import (
    EventDatePrecision,
    MissingSemantics,
)
from compsval.ingest.parsers.lianjia import parse_lianjia_txt
from compsval.ingest.parsers.lianjia_html import (
    CSV_COLUMNS,
    crosscheck_html_vs_log,
    extract_li_blocks,
    lianjia_records_to_csv_rows,
    parse_lianjia_csv_table,
    parse_lianjia_html,
    write_lianjia_csv,
)

REPO_ROOT = Path(__file__).resolve().parents[1].parent
EVIDENCE_DIR = (
    REPO_ROOT
    / "01-数据"
    / "raw"
    / "source=lianjia"
    / "dataset=chengjiao"
    / "fetched_at=20260823"
)

# 一个典型的链家成交 li（从真实 HTML 抽象：title/houseInfo/dealDate/totalPrice/
# positionInfo/unitPrice/dealHouseInfo/dealCycleeInfo/agentInfoList 字段）。
_RESIDENTIAL_LI = """
<li>
  <div class="info">
    <div class="title"><a href="https://lianjia.com.example/chengjiao/108407728347.html">示例小区166 2室1厅 54.25平米</a></div>
    <div class="address">
      <div class="houseInfo"><span class="houseIcon"></span>西南 | 简装</div>
      <div class="dealDate">2026.05.22</div>
      <div class="totalPrice"><span class="number">140</span>万</div>
    </div>
    <div class="flood">
      <div class="positionInfo"><span class="positionIcon"></span>低楼层(共29层) 2003年塔楼</div>
      <div class="unitPrice"><span class="number">25807</span>元/平</div>
    </div>
    <div class="dealHouseInfo"><span class="dealHouseIcon"></span><span class="dealHouseTxt"><span>房屋满五年</span><span>近地铁</span></span></div>
    <div class="dealCycleeInfo"><span class="dealCycleIcon"></span><span class="dealCycleTxt"><span>挂牌145万</span><span>成交周期271天</span></span></div>
    <div class="agentInfoList"><span class="agentIcon"></span><a href="#" class="agent_name">黄水钦</a><span>免费咨询</span></div>
  </div>
</li>
"""  # noqa: E501

_PARKING_LI = """
<li>
  <div class="info">
    <div class="title"><a href="#">示例小区166 车位</a></div>
    <div class="address">
      <div class="houseInfo">东 | 毛坯</div>
      <div class="dealDate">2025.11.14</div>
      <div class="totalPrice"><span class="number">22</span>万</div>
    </div>
    <div class="flood">
      <div class="positionInfo">地下室 塔楼</div>
      <div class="unitPrice"><span class="number">17461</span>元/平</div>
    </div>
    <div class="dealHouseInfo"><span>近地铁</span></div>
    <div class="dealCycleeInfo"><span>挂牌27.5万</span><span>成交周期850天</span></div>
    <div class="agentInfoList">周晓磊<span>免费咨询</span></div>
  </div>
</li>
"""  # noqa: E501


def _wrap_li(*lis: str) -> str:
    """把 li 包进 ul.listContent 容器（模拟真实成交页）。"""
    return '<div class="content"><ul class="listContent">' + "".join(lis) + "</ul></div>"


def test_extract_li_blocks() -> None:
    html = _wrap_li(_RESIDENTIAL_LI, _PARKING_LI)
    blocks = extract_li_blocks(html)
    assert len(blocks) == 2


def test_extract_li_blocks_missing_ul_raises() -> None:
    with pytest.raises(ValueError, match="listContent"):
        extract_li_blocks("<div>no list</div>")


def test_parse_lianjia_html_residential() -> None:
    recs = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI))
    assert len(recs) == 1
    r = recs[0]
    assert r.community == "示例小区166"
    assert r.layout == "2室1厅"
    assert r.area_sqm == Decimal("54.25")
    assert r.orientation == "西南"
    assert r.decoration == "简装"
    assert r.deal_date == dt.date(2026, 5, 22)
    assert r.deal_date_precision == EventDatePrecision.DAY
    assert r.total_price_yuan == Decimal("1400000")
    assert r.original_price_text == "140万"
    assert r.floor == "低楼层"
    assert r.total_floors == 29
    assert r.year_built == 2003
    assert r.building_type == "塔楼"
    assert "满5年" in r.features
    assert r.unit_price_observed == Decimal("25807")
    assert r.listing_price_yuan == Decimal("1450000")
    assert r.listing_period_days == 271
    assert r.agent == "黄水钦"


def test_parse_lianjia_html_parking() -> None:
    recs = parse_lianjia_html(_wrap_li(_PARKING_LI))
    assert len(recs) == 1
    r = recs[0]
    assert r.layout == "车位"
    assert r.area_sqm is None
    assert r.area_state == MissingSemantics.NOT_APPLICABLE
    assert r.unit_price_derived is None
    assert r.community == "示例小区166"


def test_parse_lianjia_html_community_override() -> None:
    recs = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI), community="自定义小区")
    assert recs[0].community == "自定义小区"


def test_parse_lianjia_html_matches_txt_parser() -> None:
    """HTML 解析结果与 fetch_log innerText（TXT 行）解析结果一致。"""
    html_rec = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI))[0]
    # 用与 HTML 等价的 TXT 行块（WP4-B 格式）直接解析
    txt_lines = [
        "示例小区166 2室1厅 54.25平米",
        "西南 | 简装2026.05.22140万",
        "低楼层(共29层) 2003年塔楼25807元/平",
        "房屋满五年近地铁",
        "挂牌145万成交周期271天",
        "黄水钦免费咨询",
    ]
    txt_rec = parse_lianjia_txt(txt_lines)[0]
    assert html_rec.community == txt_rec.community
    assert html_rec.layout == txt_rec.layout
    assert html_rec.area_sqm == txt_rec.area_sqm
    assert html_rec.deal_date == txt_rec.deal_date
    assert html_rec.total_price_yuan == txt_rec.total_price_yuan
    assert html_rec.unit_price_observed == txt_rec.unit_price_observed
    assert html_rec.listing_price_yuan == txt_rec.listing_price_yuan
    assert html_rec.listing_period_days == txt_rec.listing_period_days


def test_csv_roundtrip() -> None:
    """CSV 写入 → 读表 → parse_lianjia_csv_table 回读一致。"""
    import tempfile

    recs = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI, _PARKING_LI))
    with tempfile.TemporaryDirectory() as tmp:
        out = write_lianjia_csv(recs, out_path=Path(tmp) / "nanbei.csv")
        table = pacsv.read_csv(out)
        assert list(table.column_names) == list(CSV_COLUMNS)
        assert table.num_rows == 2
        back = parse_lianjia_csv_table(table, community="示例小区166")
        assert len(back) == 2
        r0, r1 = back
        assert r0.layout == "2室1厅"
        assert r0.area_sqm == Decimal("54.25")
        assert r0.deal_date == dt.date(2026, 5, 22)
        assert r0.total_price_yuan == Decimal("1400000")
        assert r0.listing_price_yuan == Decimal("1450000")
        assert r0.listing_period_days == 271
        assert r0.unit_price_observed == Decimal("25807")
        assert r1.layout == "车位"
        assert r1.area_sqm is None
        assert r1.area_state == MissingSemantics.NOT_APPLICABLE


def test_csv_rows_missing_not_zero() -> None:
    """CSV 行缺失字段留空串，不用 0。"""
    recs = parse_lianjia_html(_wrap_li(_PARKING_LI))
    (row,) = lianjia_records_to_csv_rows(recs)
    assert row["area_m2"] == ""
    assert row["deal_date"] == "2025-11-14"  # 车位也有成交日
    assert row["total_price_wan"] == "22"


def test_crosscheck_matching() -> None:
    html_recs = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI))
    log_recs = parse_lianjia_txt(
        [
            "示例小区166 2室1厅 54.25平米",
            "西南 | 简装2026.05.22140万",
            "低楼层(共29层) 2003年塔楼25807元/平",
            "房屋满五年近地铁",
            "挂牌145万成交周期271天",
            "黄水钦免费咨询",
        ]
    )
    result = crosscheck_html_vs_log(html_recs, log_recs)
    assert result.ok
    assert result.missing_from_log == 0
    assert result.missing_from_html == 0


def test_crosscheck_mismatch_detected() -> None:
    html_recs = parse_lianjia_html(_wrap_li(_RESIDENTIAL_LI))
    log_recs = parse_lianjia_txt(
        [
            "示例小区166 2室1厅 55.00平米",  # 面积不同
            "西南 | 简装2026.05.22140万",
            "低楼层(共29层) 2003年塔楼25807元/平",
            "房屋满五年近地铁",
            "挂牌145万成交周期271天",
            "黄水钦免费咨询",
        ]
    )
    result = crosscheck_html_vs_log(html_recs, log_recs)
    assert not result.ok
    assert result.missing_from_log == 1
    assert result.missing_from_html == 1


def test_parse_lianjia_csv_table_missing_columns_raises() -> None:
    table = pa.table({"community": ["x"]})
    with pytest.raises(ValueError, match="缺少必要列"):
        parse_lianjia_csv_table(table, community="x")
