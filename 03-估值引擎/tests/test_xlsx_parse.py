"""EXTFP1-C 全量逐行解析的离线测试。

用 openpyxl 在 tmp_path 合成真实 XLSX fixture，验证 §6 字段映射、缺失语义
（MISSING/PARSE_FAILURE）、户型图 URL 安全解析衔接、cell 表转置等价性与
守恒统计。绝不触碰真实外部数据文件，也不访问网络。
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook

from compsval import cli
from compsval.ingest.import_file import read_xlsx_table
from compsval.ingest.xlsx_parse import (
    FieldParseStatus,
    PropertyUseNorm,
    XlsxParsedRecord,
    iter_parse_xlsx,
    summarize,
    transpose_cell_table,
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

FP_URL = "http://ke-image.ljcdn.com/hdic-frame/abc.jpg.1440x1080.jpg?from=ke.com"
PH_URL = "http://ke-image.ljcdn.com/beike/dituFindHouse/xyz.png"


def _row(**kw: object) -> list[object]:
    vals: list[object] = [None] * len(HEADERS)
    by_name = {
        "房屋ID": "108404666013",
        "小区名字": "示例小区121",
        "小区ID": "2811019201",
        "成交日期": datetime(2023, 12, 17),
        "成交总价": "700000",
        "成交均价": "21814",
        "房屋面积": "32.09",
        "建筑面积": "暂无数据",
        "户型": "1室1厅1卫",
        "卧室数量": "1",
        "客厅数量": "1",
        "楼层": "中楼层/31层",
        "朝向": "东",
        "装修情况": "精装",
        "建成时间": "2015年",
        "房屋类型": "塔楼",
        "是否有电梯": "有",
        "房屋用途": "普通住宅",
        "户型图": f"['{FP_URL}']",
        "挂牌价格": "95.0",
        "成交天数": "159",
        "价格调整次数": "1",
        "房源描述": "朝东精装",
        "房源描述.1": "满五",
    }
    by_name.update(kw)
    for name, value in by_name.items():
        if value is not None:
            vals[HEADERS.index(name)] = value
    return vals


def _make_fixture(tmp_path: Path, name: str = "fixture.xlsx") -> Path:
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    rows = [
        _row(),  # 正常普通住宅
        _row(房屋ID="R2", 房屋用途="商住两用", 成交总价="165000", 成交均价="16500",
             房屋面积="10.0", 挂牌价格="25.0", 成交天数="21"),
        _row(房屋ID="R3", 房屋用途="车库", 成交总价="175000", 成交均价="14681",
             房屋面积="11.92", 户型图=f"['{PH_URL}']"),
        _row(房屋ID="R4", 房屋用途="普通住宅", 成交日期="2023-11-13", 成交总价="暂无数据",
             成交均价="-", 房屋面积="88.5㎡", 建筑面积="90.5㎡", 挂牌价格="暂无",
             成交天数="暂无数据", 户型图="['http://a.example.com/1.jpg', 'http://b.example.com/2.jpg']"),
        _row(房屋ID="R5", 房屋用途="暂无", 成交日期=None, 成交总价=None, 成交均价=None,
             房屋面积=None, 建筑面积=None, 户型图=None, 挂牌价格=None, 成交天数=None),
        _row(房屋ID="R6", 房屋用途="普通住宅", 成交日期="not-a-date", 成交总价="abc",
             成交均价="", 房屋面积="-", 建筑面积="120㎡", 户型图="['ftp://x/1.jpg']"),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / name
    wb.save(path)
    return path


def _consume(path: Path) -> list[XlsxParsedRecord]:
    return list(iter_parse_xlsx(path))


def test_parse_normal_ordinary_record(tmp_path: Path) -> None:
    rec = _consume(_make_fixture(tmp_path))[0]
    assert rec.source_record_id == "108404666013"
    assert rec.community_name == "示例小区121"
    assert rec.community_source_id == "2811019201"
    # 成交日期 datetime → ISO DAY
    assert rec.sale_date == "2023-12-17"
    assert rec.sale_date_precision == "DAY"
    # 总价/均价/面积（单位已验证：元/元每平米/㎡）
    assert rec.total_price_yuan == 700000
    assert rec.total_price_status is FieldParseStatus.PARSED
    assert rec.unit_price_observed == 21814
    assert rec.transaction_area_sqm == Decimal("32.09")
    assert rec.area_status is FieldParseStatus.PARSED
    # 挂牌价（万元 → 元）
    assert rec.listing_price_yuan == 950000
    assert rec.listing_price_status is FieldParseStatus.PARSED
    assert rec.listing_days == 159
    # 用途归一
    assert rec.property_use_raw == "普通住宅"
    assert rec.property_use_norm is PropertyUseNorm.ORDINARY_RESIDENTIAL
    # 户型图 URL 安全解析衔接（候选非占位）
    assert rec.floorplan_url_status == "URLS_OK"
    assert rec.floorplan_candidate_count == 1
    # 描述分开保留
    assert rec.source_property_description == "朝东精装"
    assert rec.source_property_tags == "满五"


def test_parse_missing_semantics(tmp_path: Path) -> None:
    records = {r.source_record_id: r for r in _consume(_make_fixture(tmp_path))}
    # R4：暂无数据/暂无/- 归 MISSING；"-" 归 PARSE_FAILURE（原文存在不可解析）
    r4 = records["R4"]
    assert r4.total_price_status is FieldParseStatus.MISSING
    assert r4.unit_price_status is FieldParseStatus.PARSE_FAILURE  # "-"
    assert r4.area_status is FieldParseStatus.PARSED  # 88.5㎡ 剥离后缀
    assert r4.listing_price_status is FieldParseStatus.MISSING  # 暂无
    assert r4.listing_days_status is FieldParseStatus.MISSING  # 暂无数据
    assert r4.building_area_status is FieldParseStatus.PARSED  # 90.5㎡
    # R5：空值全 MISSING，用途 UNKNOWN
    r5 = records["R5"]
    assert r5.total_price_status is FieldParseStatus.MISSING
    assert r5.area_status is FieldParseStatus.MISSING
    assert r5.property_use_norm is PropertyUseNorm.UNKNOWN
    assert r5.floorplan_url_status == "NO_URL"
    assert r5.sale_date_precision == "UNKNOWN"
    # R6：原文存在但不可解析 → PARSE_FAILURE；ftp URL 非候选
    r6 = records["R6"]
    assert r6.total_price_status is FieldParseStatus.PARSE_FAILURE  # "abc"
    assert r6.unit_price_status is FieldParseStatus.MISSING  # "" 空串
    assert r6.area_status is FieldParseStatus.PARSE_FAILURE  # "-"
    assert r6.building_area_status is FieldParseStatus.PARSED  # 120㎡
    assert r6.sale_date_precision == "UNKNOWN"  # not-a-date
    assert r6.floorplan_url_status == "URLS_OK"
    assert r6.floorplan_candidate_count == 0  # ftp:// 非 http(s) 非候选


def test_parse_counterexample_use_and_multi_url(tmp_path: Path) -> None:
    records = {r.source_record_id: r for r in _consume(_make_fixture(tmp_path))}
    # 商住两用/车库 → 非普通住宅（明确排除）
    assert records["R2"].property_use_norm is PropertyUseNorm.ASSERTED_EXCLUDED
    assert records["R3"].property_use_norm is PropertyUseNorm.ASSERTED_EXCLUDED
    # R3 占位 URL → 候选 0
    assert records["R3"].floorplan_candidate_count == 0
    # R4 多 URL → 候选 2（保序不静默取第一条）
    assert records["R4"].floorplan_url_status == "URLS_OK"
    assert records["R4"].floorplan_candidate_count == 2


def test_summarize_conservation_and_distribution(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    records = _consume(path)
    summary = summarize(records)
    assert summary.data_rows_total == 6
    assert summary.parsed_count == 6
    assert summary.ordinary_residential_count == 3  # R0/R4/R6
    dist = summary.property_use_distribution
    assert dist["普通住宅"] == 3
    assert dist["非普通住宅"] == 2  # R2/R3
    assert dist["UNKNOWN"] == 1  # R5
    # 字段缺失统计
    assert summary.field_status_counts["total_price"]["MISSING"] == 2  # R4/R5
    assert summary.field_status_counts["total_price"]["PARSE_FAILURE"] == 1  # R6
    assert summary.field_status_counts["area_17"]["PARSED"] == 4  # R0/R2/R3/R4
    assert summary.field_status_counts["area_17"]["MISSING"] == 1  # R5
    assert summary.field_status_counts["area_17"]["PARSE_FAILURE"] == 1  # R6


def test_transpose_cell_table_equivalence(tmp_path: Path) -> None:
    """cell 表转置后的行与 openpyxl 直读解析结果等价（§13 原始层 cell 表 → 行）。"""
    path = _make_fixture(tmp_path)
    cell_table = read_xlsx_table(path)
    transposed = transpose_cell_table(cell_table)
    assert "Sheet1" in transposed
    header, rows = transposed["Sheet1"]
    assert header == tuple(HEADERS)
    assert [rn for rn, _ in rows] == [2, 3, 4, 5, 6, 7]  # 数据行号（表头=1）

    direct = list(iter_parse_xlsx(path))
    from compsval.ingest.xlsx_parse import _parse_rows

    via_cells = list(_parse_rows(header, (values for _, values in rows)))
    assert len(via_cells) == len(direct)
    for a, b in zip(direct, via_cells, strict=True):
        assert a.model_dump() == b.model_dump(), f"row {a.row_number} 不等价"


def test_iter_parse_closes_workbook_early(tmp_path: Path) -> None:
    """提前 break 后生成器 close 不抛异常（openpyxl workbook 正常关闭）。"""
    it = iter_parse_xlsx(_make_fixture(tmp_path))
    first = next(it)
    assert first.row_number == 1
    it.close()  # 不应抛异常


def test_cli_xlsx_parse_summary(tmp_path: Path, capsys) -> None:
    path = _make_fixture(tmp_path)
    out = tmp_path / "parse-summary.json"
    assert (
        cli.main(
            ["xlsx", "parse", "--input", str(path), "--out", str(out), "--sample", "2"]
        )
        == 0
    )
    capsys.readouterr()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["workpackage"] == "EXTFP1-C"
    assert data["data_rows_total"] == 6
    assert data["ordinary_residential_count"] == 3
    assert data["property_use_distribution"]["普通住宅"] == 3
    assert len(data["sample_records"]) == 2
    assert data["sample_records"][0]["source_record_id"] == "108404666013"
    assert data["source_sha256"] is not None
    # 原子写：无 .incomplete 残留
    assert not out.with_name(out.name + ".incomplete").exists()


def test_cli_xlsx_parse_missing_input_fails(tmp_path: Path, capsys) -> None:
    assert (
        cli.main(
            [
                "xlsx", "parse",
                "--input", str(tmp_path / "missing.xlsx"),
                "--out", str(tmp_path / "out.json"),
            ]
        )
        == 1
    )
    assert "input not found" in capsys.readouterr().out
