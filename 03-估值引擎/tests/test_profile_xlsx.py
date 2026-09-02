"""EXTFP0-D 普通住宅画像与字段映射的离线测试。

用小而确定的 XLSX fixture（用 openpyxl 在 tmp_path 生成，保证真实可解析结构）
验证画像统计正确，绝不触碰真实外部数据文件，也不访问网络。
"""

from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook
from pytest import raises

from compsval.ingest.profile_xlsx import (
    AREA_TARGETS,
    PROFILE_RULE_VERSION,
    profile_xlsx,
)

HEADERS = [
    "省份", "城市", "区县", "板块", "房屋ID", "房源标题", "房源描述", "成交日期",
    "户型图", "小区名字", "小区ID", "成交总价", "成交均价", "楼层", "客厅数量",
    "卧室数量", "房屋面积", "朝向", "房源描述.1", "房屋类型", "是否有电梯",
    "装修情况", "建成时间", "房屋用途", "房屋权属", "房屋位置", "纬度", "经度",
    "位置描述", "挂牌价格", "成交天数", "价格调整次数", "带看", "关注", "浏览",
    "经纪人", "品牌", "户型", "房屋面积.1", "建筑面积", "结构", "梯户比例",
    "高度", "供暖方式", "房屋年龄", "产权",
]


def _row(
    *,
    use: str | None,
    area17: object,
    area39: object,
    area40: object,
    desc7: object | None,
    desc19: object | None,
) -> list[object]:
    """按表头位置构造一列与 HEADERS 对齐的行（未列出的列留空）。"""
    vals: list[object] = [None] * len(HEADERS)
    by_name: dict[str, object] = {
        "房屋用途": use,
        "房屋面积": area17,
        "房屋面积.1": area39,
        "建筑面积": area40,
        "房源描述": desc7,
        "房源描述.1": desc19,
    }
    for name, value in by_name.items():
        if value is not None:
            vals[HEADERS.index(name)] = value
    return vals


def _make_fixture(tmp_path: Path) -> Path:
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    rows = [
        _row(use="普通住宅", area17=100, area39=100, area40=105, desc7="朝向好", desc19="采光佳"),
        _row(use="普通住宅", area17=None, area39=None, area40=90, desc7=None, desc19=None),
        _row(use="普通住宅", area17="88.5", area39=88.5, area40=88.5, desc7="通风", desc19="通风"),
        _row(use="普通住宅", area17="-", area39="-", area40=70, desc7="老房", desc19="满五"),
        _row(use="商住两用", area17=50, area39=50, area40=50, desc7=None, desc19="商住"),
        _row(use="车库", area17=12, area39=12, area40=12, desc7="", desc19=""),
        _row(use="普通住宅", area17=120, area39=120, area40=130, desc7="江景", desc19="江景"),
        _row(use="暂无", area17=95, area39=95, area40=95, desc7=None, desc19=None),
        _row(use="别墅", area17=250, area39=250, area40=250, desc7="独栋", desc19="独栋"),
        _row(use="普通住宅", area17=101, area39=101.5, area40=110, desc7="xx", desc19="yy"),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "fixture.xlsx"
    wb.save(path)
    return path


def test_profile_usage_distribution(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_fixture(tmp_path)).sheets[0]
    assert sheet.sheet_name == "Sheet1"
    assert sheet.data_rows_total == 10
    assert sheet.ordinary_residential_count == 6  # 第0,1,2,3,6,9 行
    assert sheet.property_use_asserted_excluded == 3  # 商住两用 + 车库 + 别墅
    assert sheet.property_use_unknown == 1  # 暂无
    dist = sheet.property_use_distribution
    assert dist["普通住宅"] == 6
    assert dist["商住两用"] == 1
    assert dist["车库"] == 1
    assert dist["别墅"] == 1
    assert dist["暂无"] == 1


def test_profile_area_column_mapping(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_fixture(tmp_path)).sheets[0]
    by_header = {a.header: a for a in sheet.area_columns}
    assert set(by_header) == {t for t, _ in AREA_TARGETS}

    q17 = by_header["房屋面积"]
    assert q17.column_letter == "Q"  # 第17列
    assert q17.parseable_count == 8  # 100,88.5,50,12,120,95,250,101
    assert q17.missing_count == 1  # None 行
    assert q17.parse_failure_count == 1  # "-"
    assert q17.min_value == 12.0
    assert q17.max_value == 250.0
    assert q17.count_gt_zero == 8

    am39 = by_header["房屋面积.1"]
    assert am39.column_letter == "AM"  # 第39列
    assert am39.equal_to_17_count == 7  # 除 101.5 vs 101 外其余一致
    assert am39.differs_from_17_count == 1
    assert am39.both_parseable_count == 8

    an40 = by_header["建筑面积"]
    assert an40.column_letter == "AN"  # 第40列
    assert an40.parseable_count == 10  # 全部 10 行均有数值
    assert an40.min_value == 12.0
    assert an40.max_value == 250.0


def test_profile_description_pair(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_fixture(tmp_path)).sheets[0]
    pair = sheet.description_pair
    assert pair is not None
    assert pair.source_header == "房源描述"
    assert pair.tags_header == "房源描述.1"
    assert pair.source_present_count == 6  # 第0,2,3,6,8,9 行有值
    assert pair.tags_present_count == 7  # 额外含第4行 desc19="商住"
    assert pair.both_present_differ_count == 3  # R0,R3,R9
    assert pair.both_present_same_count == 3  # R2,R6,R8


def test_profile_missing_semantics(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_fixture(tmp_path)).sheets[0]
    sem = {m.header: m for m in sheet.missing_semantics}
    assert sem["房屋面积"].missing_count == 1
    assert sem["房屋面积"].column_letter == "Q"
    assert sem["房屋面积.1"].missing_count == 1
    assert sem["房源描述"].missing_count == 4  # None/空 第1,4,5,7 行


def test_profile_report_metadata(tmp_path: Path) -> None:
    report = profile_xlsx(_make_fixture(tmp_path))
    assert report.profile_rule_version == PROFILE_RULE_VERSION
    assert report.source_sha256 is not None
    assert report.source_path.endswith("fixture.xlsx")
    assert len(report.sheets) == 1


def test_profile_writes_json_atomically(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    out = tmp_path / "profile.json"
    profile_xlsx(path, out_json=out)
    assert out.is_file()
    data = out.read_text(encoding="utf-8")
    assert PROFILE_RULE_VERSION in data
    assert "普通住宅" in data
    # 原子写入：无残留 .incomplete
    assert not out.with_name(out.name + ".incomplete").exists()
    reloaded = json.loads(data)
    assert reloaded["sheets"][0]["property_use_distribution"]["普通住宅"] == 6


def test_profile_missing_source_raises(tmp_path: Path) -> None:
    with raises(FileNotFoundError):
        profile_xlsx(tmp_path / "nope.xlsx")


def _make_unit_fixture(tmp_path: Path) -> Path:
    """含面积单位后缀与「暂无数据」占位的真实链家式样板。"""
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)  # 第40列 AN=建筑面积，第17列 Q=房屋面积
    rows = [
        _row(use="普通住宅", area17="88.5", area39="88.5㎡", area40="90.5㎡",
             desc7="", desc19=""),
        _row(use="普通住宅", area17="64", area39="64㎡", area40="暂无数据",
             desc7="", desc19=""),
        _row(use="普通住宅", area17="108.3㎡", area39="108.3㎡", area40="108.3㎡",
             desc7="", desc19=""),
        _row(use="普通住宅", area17="暂无数据", area39="暂无", area40=70,
             desc7="", desc19=""),
        _row(use="普通住宅", area17=50, area39="50㎡", area40=55,
             desc7="", desc19=""),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "units.xlsx"
    wb.save(path)
    return path


def test_profile_area_strips_unit_suffix(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_unit_fixture(tmp_path)).sheets[0]
    by_header = {a.header: a for a in sheet.area_columns}

    # 第17列：108.3㎡ 与 暂无数据 都要正确处理
    q17 = by_header["房屋面积"]
    assert q17.parseable_count == 4  # 88.5, 64, 108.3, 50
    assert q17.missing_count == 1  # "暂无数据" → 来源未披露
    assert q17.parse_failure_count == 0
    assert q17.max_value == 108.3

    # 第39列：单位后缀剥离后全部可解析；暂无 → 缺失
    am39 = by_header["房屋面积.1"]
    assert am39.parseable_count == 4  # 88.5, 64, 108.3, 50
    assert am39.missing_count == 1  # "暂无"
    assert am39.parse_failure_count == 0
    assert am39.equal_to_17_count == 4  # R0,R1,R2,R4 均可解析且相等
    assert am39.differs_from_17_count == 0

    # 第40列：暂无数据 → 缺失；其余剥离后缀可解析
    an40 = by_header["建筑面积"]
    assert an40.parseable_count == 4  # 90.5, 108.3, 70, 55
    assert an40.missing_count == 1  # 暂无数据
    assert an40.parse_failure_count == 0


def test_profile_placeholder_is_not_parse_failure(tmp_path: Path) -> None:
    sheet = profile_xlsx(_make_unit_fixture(tmp_path)).sheets[0]
    sem = {m.header: m for m in sheet.missing_semantics}
    # 「暂无数据」在面积列计入 non_assert_placeholder，而不是 missing_count
    assert sem["房屋面积.1"].non_assert_placeholder_count == 1
    assert sem["房屋面积.1"].missing_count == 0


def test_profile_consistency_requires_both_parseable(tmp_path: Path) -> None:
    """RV-EXTFP0-D-03#F6：一致性统计要求第17列与比较列 *两列均* 可解析才计入。

    _make_unit_fixture 第 3 行 Q=「暂无数据」（不可解析）、AN=70（可解析）：
    该行不得错误计入 `differs_from_17_count`。
    """
    sheet = profile_xlsx(_make_unit_fixture(tmp_path)).sheets[0]
    by_header = {a.header: a for a in sheet.area_columns}

    an40 = by_header["建筑面积"]  # AN
    # R0/R2/R4 两列均可解析 → equal R2(108.3) 一致；diff R0(90.5)/R4(55) → 2
    assert an40.equal_to_17_count == 1
    assert an40.differs_from_17_count == 2
    assert an40.both_parseable_count == 3
    # 第 3 行 Q 不可解析、AN 可解析：不得计入 differs
    assert an40.differs_from_17_count == 2  # 不含第3行

    am39 = by_header["房屋面积.1"]  # AM：R0/R1/R2/R4 可解析，R3 为「暂无」
    # R0/R1/R2/R4 两列均可解析且相等（R1=64, R2=108.3, R3 排除）
    assert am39.equal_to_17_count == 4
    assert am39.differs_from_17_count == 0