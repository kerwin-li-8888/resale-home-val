"""Unit tests for the Lianjia Hecheng-list TXT parser (WP4-B).

Covers normal residential records, parking records, missing-value semantics,
unit-price derivation, Chinese/Arabic year parsing, header/anti-pattern
lines, fingerprint stability, and raw-start-line tracking. No cleaning or
deduplication is expected here (that is WP4-C); parsing is pure and does not
touch any snapshot.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from compsval.contract.models import (
    EventDatePrecision,
    MissingSemantics,
)
from compsval.ingest.parsers.lianjia import (
    parse_lianjia_txt,
)

# A realistic single residential block, one element per raw line (no header here).
_RESIDENTIAL = [
    "示例小区126 2室1厅 84.04平米",
    "南 | 精装2026.04.12 258万",
    "中楼层(共33层) 2005年板楼30700元/平",
    "房屋满五年近地铁",
    "挂牌258万成交周期89天",
    "王梅免费咨询",
]

_HEADER = [
    "示例城市目标区二手房成交记录",
    "链家网·家和置业",
    "共184条",
    "https://lianjia.com.example/chengjiao/targetdistrict/",
]


def test_normal_residential_record_full() -> None:
    recs = parse_lianjia_txt(_HEADER + _RESIDENTIAL)
    assert len(recs) == 1
    r = recs[0]
    assert r.community == "示例小区126"
    assert r.layout == "2室1厅"
    assert r.area_sqm == Decimal("84.04")
    assert r.orientation == "南"
    assert r.decoration == "精装"
    assert r.deal_date == date(2026, 4, 12)
    assert r.deal_date_precision == EventDatePrecision.DAY
    assert r.total_price_yuan == Decimal("2580000")
    assert r.original_price_text == "258万"
    assert r.unit_price_derived == Decimal("30700")
    assert r.unit_price_formula == "total_price_yuan / area_sqm, rounded to integer"
    assert r.floor == "中楼层"
    assert r.total_floors == 33
    assert r.year_built == 2005
    assert r.year_state is None  # 年份有值=已知，不设缺失标记
    assert r.building_type == "板楼"
    assert "满5年" in r.features
    assert r.listing_price_yuan == Decimal("2580000")
    assert r.listing_period_days == 89
    assert r.agent == "王梅"


def test_derived_unit_price_matches_formula() -> None:
    r = parse_lianjia_txt(_RESIDENTIAL)[0]
    assert r.total_price_yuan is not None and r.area_sqm is not None
    expected = (r.total_price_yuan / r.area_sqm).to_integral_value()
    assert r.unit_price_derived == expected
    assert r.unit_price_derived == r.unit_price_observed  # 链家页面两者并列，通常一致


def test_parking_record_area_not_applicable() -> None:
    park = [
        "星河湾 车位",
        "北 | 毛坯2026.06.01 45万",
        "地下车位(共1层) 2020年 15000元/平",
    ]
    recs = parse_lianjia_txt(park)
    assert len(recs) == 1
    r = recs[0]
    assert r.layout == "车位"
    assert r.area_sqm is None
    assert r.area_state == MissingSemantics.NOT_APPLICABLE  # 车位无平米，非缺失
    assert r.unit_price_derived is None  # 无面积 → 不派生单价
    assert r.unit_price_formula == ""  # 车位不派生，也不写公式


def test_pingfang_building_type() -> None:
    """链家"平房"楼型（LJ-C 发现：示例小区166 42.85㎡ 2003年平房）可解析。"""
    block = [
        "示例小区166 1室1厅 42.85平米",
        "西南 | 其他2026.01.03 128万",
        "中楼层(共29层) 2003年平房29872元/平",
        "房屋满五年近地铁",
        "挂牌135万成交周期11天",
        "叶志超免费咨询",
    ]
    recs = parse_lianjia_txt(block)
    assert len(recs) == 1
    r = recs[0]
    assert r.building_type == "平房"
    assert r.year_built == 2003
    assert r.total_floors == 29
    assert r.floor == "中楼层"
    assert r.unit_price_observed == Decimal("29872")


def test_year_missing_flag() -> None:
    lines = [
        "康乐小区 3室2厅 98.00平米",
        "东 | 简装2025.12.30 300万",
        "高楼层(共9层) 暂无数据板楼45000元/平",
    ]
    r = parse_lianjia_txt(lines)[0]
    assert r.year_built is None
    assert r.year_state == MissingSemantics.MISSING


def test_multiple_records_split_by_start_lines() -> None:
    lines = [
        "小区甲 2室1厅 70.00平米",
        "南 | 精装2026.01.01 150万",
        "中楼层 2020年 21000元/平",
        "小区乙 1室1厅 55.00平米",
        "西 | 毛坯2026.02.01 99万",
        "低楼层(共6层) 18000元/平",
    ]
    recs = parse_lianjia_txt(lines)
    assert len(recs) == 2
    assert recs[0].community == "小区甲"
    assert recs[1].community == "小区乙"
    assert recs[0].raw_start_line == 1
    assert recs[1].raw_start_line == 4


def test_header_and_garbage_lines_are_ignored() -> None:
    # 头部行 + 记录完成后的一行乱码，均不产生新记录也不报错（反例）
    lines = _HEADER + _RESIDENTIAL + ["这是无法解析的杂质行not-a-record"]
    recs = parse_lianjia_txt(lines)
    assert len(recs) == 1
    assert recs[0].community == "示例小区126"


def test_malformed_start_line_does_not_open_record() -> None:
    # 面积非法（非数字）→ 不匹配起始行，前一条记录不受影响（反例）
    lines = _RESIDENTIAL + ["某小区 2室1厅 十二平米"]
    recs = parse_lianjia_txt(lines)
    assert len(recs) == 1
    assert recs[0].community == "示例小区126"


def test_near_metro_feature_only() -> None:
    lines = [
        "近地铁小区 2室2厅 80.00平米",
        "南 | 精装2026.03.10 200万",
        "中楼层 2018年 25000元/平",
        "近地铁",
    ]
    r = parse_lianjia_txt(lines)[0]
    assert "近地铁" in r.features


def test_chinese_and_arabic_years() -> None:
    # "满十年"→10，"满三年"→3，"满6年"→6
    lines = [
        "小区C 2室1厅 60.00平米",
        "南 | 精装2026.04.04 120万",
        "中楼层 2016年 20000元/平",
        "房屋满十年近地铁",
    ]
    r = parse_lianjia_txt(lines)[0]
    assert "满10年" in r.features

    lines2 = [
        "小区D 2室1厅 65.00平米",
        "南 | 精装2026.05.05 130万",
        "中楼层 2015年 20000元/平",
        "房屋满三年",
    ]
    assert any("满3年" in f for f in parse_lianjia_txt(lines2)[0].features)

    lines3 = [
        "小区E 2室1厅 70.00平米",
        "南 | 精装2026.06.06 140万",
        "中楼层 2014年 20000元/平",
        "房屋满6年",
    ]
    assert any("满6年" in f for f in parse_lianjia_txt(lines3)[0].features)


def test_fingerprint_stable_and_unique() -> None:
    a = parse_lianjia_txt(_RESIDENTIAL)[0]
    b = parse_lianjia_txt(_RESIDENTIAL)[0]
    assert a.source_record_id == b.source_record_id
    assert len(a.source_record_id) == 64
    assert int(a.source_record_id, 16) >= 0  # 是合法十六进制
    # 不同面积/价格 → 指纹不同
    other = parse_lianjia_txt(
        ["示例小区126 2室1厅 90.00平米", "南 | 精装2026.04.12 300万", "中楼层 2005年 33000元/平"]
    )[0]
    assert other.source_record_id != a.source_record_id


def test_default_missing_semantics() -> None:
    # 仅起始行，无任何属性行 → 可选字段保持默认缺省（不填0）
    r = parse_lianjia_txt(["某小区 2室2厅 88.00平米"])[0]
    assert r.area_sqm == Decimal("88.00")
    assert r.orientation == MissingSemantics.UNKNOWN.value
    assert r.decoration == MissingSemantics.UNKNOWN.value
    assert r.deal_date is None
    assert r.total_price_yuan is None
    assert r.unit_price_observed is None
    assert r.unit_price_formula == ""
    assert r.raw_start_line == 1


def test_community_trailing_residential_suffix_stripped() -> None:
    lines = ["示例小区126(住宅) 2室1厅 84.04平米", "南 | 精装2026.04.12 258万"]
    r = parse_lianjia_txt(lines)[0]
    assert r.community == "示例小区126"


def test_blank_lines_ignored() -> None:
    recs = parse_lianjia_txt(["", " ", _RESIDENTIAL[0], "", _RESIDENTIAL[1]])
    assert len(recs) == 1
    assert recs[0].community == "示例小区126"


# --- 真实链家 TXT 格式特征（2026-08-21 快照实测） ---
def test_real_format_no_space_between_date_and_price() -> None:
    # 真实行：日期与价格之间无空格（"2026.07.21245万"）；派生单价须与平台披露一致
    block = [
        "示例小区126 2室1厅 84.04平米",
        "北 | 简装2026.07.21245万",
        "高楼层(共31层) 2009年塔楼29153元/平",
        "房屋满五年近地铁",
        "挂牌258万成交周期89天",
        "罗俊杰免费咨询",
    ]
    r = parse_lianjia_txt(block)[0]
    assert r.deal_date == date(2026, 7, 21)
    assert r.total_price_yuan == Decimal("2450000")
    assert r.unit_price_derived is not None
    assert r.unit_price_derived == r.unit_price_observed  # 回算与披露一致
    assert r.year_built == 2009
    assert r.unit_price_formula == "total_price_yuan / area_sqm, rounded to integer"


def test_real_format_dual_orientation_and_decimal_price() -> None:
    # 双朝向（"东 西"）与小数总价（"220.8万"）
    block = [
        "桐福中路 4室1厅 88平米",
        "东 东南 | 毛坯2026.07.21220.8万",
        "低楼层(共4层) 暂无数据25091元/平",
        "近地铁",
        "挂牌238万成交周期168天",
        "陈淑谊免费咨询",
    ]
    r = parse_lianjia_txt(block)[0]
    assert r.orientation == "东 东南"
    assert r.total_price_yuan == Decimal("2208000")
    assert r.layout == "4室1厅"
    assert r.area_sqm == Decimal("88")
    assert r.deal_date == date(2026, 7, 21)
    assert r.year_built is None
    assert r.year_state == MissingSemantics.MISSING  # 暂无数据→缺失
    assert r.agent == "陈淑谊"


def test_real_parking_record() -> None:
    # 真实车位记录：无面积、无派生单价，面积 NOT_APPLICABLE
    block = [
        "示例小区220二期 车位",
        "南 | 其他2026.07.2127万",
        "地下室(共32层) 塔楼22595元/平",
        "房屋满五年近地铁",
        "挂牌30万成交周期115天",
        "申霄汉免费咨询",
    ]
    recs = parse_lianjia_txt(block)
    assert len(recs) == 1
    r = recs[0]
    assert r.layout == "车位"
    assert r.area_sqm is None
    assert r.area_state == MissingSemantics.NOT_APPLICABLE
    assert r.total_price_yuan == Decimal("270000")
    assert r.unit_price_derived is None
    assert r.unit_price_formula == ""
    assert r.unit_price_observed == Decimal("22595")  # 平台披露单价原样保留


def test_duplicate_records_keep_stable_fingerprint() -> None:
    # 反例（重复记录）：两条完全相同的记录块解析为两条记录，且 source_record_id
    # 指纹一致 → 供 WP4-C 去重键使用（去重动作归 C，不在 B 实现）
    block = [
        "示例小区126 2室1厅 84.04平米",
        "北 | 简装2026.07.21245万",
        "高楼层(共31层) 2009年塔楼29153元/平",
        "房屋满五年近地铁",
        "挂牌258万成交周期89天",
        "罗俊杰免费咨询",
    ]
    recs = parse_lianjia_txt(block + block)
    assert len(recs) == 2
    assert recs[0].source_record_id == recs[1].source_record_id


def test_unmatched_attribute_line_is_recorded_not_dropped() -> None:
    # 反例：记录块内出现无法归类的行 → 记入 unparsed_lines，不静默丢弃也不臆造字段
    block = [
        "某小区 2室1厅 60.00平米",
        "南 | 简装2026.01.010100万",
        "财报异常量级别未知口径",
        "中楼层 2016年 20000元/平",
    ]
    r = parse_lianjia_txt(block)[0]
    assert "财报异常量级别未知口径" in r.unparsed_lines
    assert r.deal_date == date(2026, 1, 1)
    assert r.total_price_yuan == Decimal("1000000")