"""EXTFP0-E 占位图画像与选择规则的离线测试。

用小而确定的 XLSX fixture（openpyxl 在 tmp_path 生成）验证 URL 列表安全解析、
dituFindHouse 占位识别、选择规则冻结与报告生成；绝不触碰真实外部数据，不访问网络。
"""

from __future__ import annotations

import json
from pathlib import Path

from openpyxl import Workbook
from pytest import raises

from compsval.ingest.floorplan_profile import (
    SELECTION_RULE_TEXT,
    SELECTION_RULE_VERSION,
    UrlClass,
    UrlListStatus,
    classify_url,
    is_placeholder_url,
    parse_url_list,
    profile_floorplan,
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

FP_URL = "http://ke-image.ljcdn.com/hdic-frame/aa4e2842.1440x1080.jpg?from=ke.com"
PH_URL = "http://ke-image.ljcdn.com/beike/dituFindHouse/1590373908380.png.1000x750.jpg?from=ke.com"


def _row(*, use: str, url_cell: object) -> list[object]:
    vals: list[object] = [None] * len(HEADERS)
    vals[HEADERS.index("房屋用途")] = use
    vals[HEADERS.index("户型图")] = url_cell
    return vals


def _make_fixture(tmp_path: Path) -> Path:
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    rows = [
        # 普通住宅：单户型图 / 多URL(1户型图+1占位) / 纯占位 / 空 / 暂无 / 解析失败 / None
        _row(use="普通住宅", url_cell=f"[{FP_URL!r}]"),
        _row(use="普通住宅", url_cell=f"[{FP_URL!r}, {PH_URL!r}]"),
        _row(use="普通住宅", url_cell=f"[{PH_URL!r}]"),
        _row(use="普通住宅", url_cell=""),
        _row(use="普通住宅", url_cell="暂无"),
        _row(use="普通住宅", url_cell="this is not a list"),
        _row(use="普通住宅", url_cell=None),
        _row(use="商住两用", url_cell=f"[{PH_URL!r}]"),
        _row(use="普通住宅", url_cell="[123]"),  # 非字符串元素
        _row(use="普通住宅", url_cell="[]"),  # 空列表
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "fp.xlsx"
    wb.save(path)
    return path


def test_parse_url_list_safe_single() -> None:
    status, items = parse_url_list(f"[{FP_URL!r}]")
    assert status == UrlListStatus.URLS_OK
    assert len(items) == 1
    assert items[0].url_class == UrlClass.FLOORPLAN_CANDIDATE


def test_parse_url_list_no_url_and_non_assert() -> None:
    assert parse_url_list(None)[0] == UrlListStatus.NO_URL
    assert parse_url_list("")[0] == UrlListStatus.NO_URL
    assert parse_url_list("暂无")[0] == UrlListStatus.NO_URL
    assert parse_url_list("暂无数据")[0] == UrlListStatus.NO_URL


def test_parse_url_list_failures() -> None:
    assert parse_url_list("not a literal")[0] == UrlListStatus.URL_PARSE_FAILURE
    assert parse_url_list("[123]")[0] == UrlListStatus.URL_PARSE_FAILURE
    assert parse_url_list("[]")[0] == UrlListStatus.URL_PARSE_FAILURE
    assert parse_url_list("'just-a-string'")[0] == UrlListStatus.URL_PARSE_FAILURE


def test_parse_url_list_multi_preserves_order() -> None:
    status, items = parse_url_list(f"[{FP_URL!r}, {PH_URL!r}]")
    assert status == UrlListStatus.URLS_OK
    assert [it.url for it in items] == [FP_URL, PH_URL]  # 不静默取第一条
    assert items[0].url_class == UrlClass.FLOORPLAN_CANDIDATE
    assert items[1].url_class == UrlClass.PLACEHOLDER


def test_placeholder_detection() -> None:
    assert is_placeholder_url(PH_URL) is True
    assert is_placeholder_url(FP_URL) is False
    assert is_placeholder_url("http://x/dituFindHouse/1.png") is True
    assert classify_url(PH_URL) == UrlClass.PLACEHOLDER
    assert classify_url(FP_URL) == UrlClass.FLOORPLAN_CANDIDATE


def test_classify_url_restricts_non_http_scheme() -> None:
    """RV-EXTFP0-E-03#F2：非 http(s) scheme 的 URL 不是户型图候选，归 PLACEHOLDER。"""
    for bad in (
        "ftp://ke-image.ljcdn.com/hdic-frame/1.jpg",
        "data:image/png;base64,AAAA",
        "file:///C:/tmp/1.jpg",
        "MAILTO:x@y.com",
        "not-a-url",
        "//protocol-relative.example/1.jpg",
    ):
        assert classify_url(bad) == UrlClass.PLACEHOLDER, bad
    # http/https 仍正常分流
    assert classify_url(FP_URL) == UrlClass.FLOORPLAN_CANDIDATE
    assert classify_url(PH_URL) == UrlClass.PLACEHOLDER


def test_profile_floorplan_counts(tmp_path: Path) -> None:
    report = profile_floorplan(_make_fixture(tmp_path))
    url = report.url_list
    assert url.data_rows_total == 10
    assert url.url_list_status_counts[UrlListStatus.NO_URL.value] == 3  # 空/暂无/None
    assert url.url_list_status_counts[UrlListStatus.URL_PARSE_FAILURE.value] == 3  # 非列表/123/[]
    assert url.url_list_status_counts[UrlListStatus.URLS_OK.value] == 4
    assert url.multi_url_records == 1  # 第1行两URL
    assert url.url_total == 5  # 1+2+1+1
    assert url.url_class_counts[UrlClass.FLOORPLAN_CANDIDATE.value] == 2
    assert url.url_class_counts[UrlClass.PLACEHOLDER.value] == 3  # 第1,2,7行各1个占位


def test_profile_ordinary_selection(tmp_path: Path) -> None:
    report = profile_floorplan(_make_fixture(tmp_path))
    o = report.ordinary
    assert o.ordinary_residential_count == 9  # 商住两用不算
    assert o.no_url_count == 3
    assert o.parse_failure_count == 3  # 非列表 / [123] / []
    assert o.placeholder_only_count == 1  # 纯占位（第2行）
    assert o.floorplan_candidate_count == 2  # 第0行单户型图、第1行含户型图
    assert o.multi_url_candidate_count == 1
    assert o.placeholder_url_count == 2
    assert o.floorplan_url_count == 2  # 第0行、第1行各1个户型图URL


def test_profile_metadata_and_rule_freeze(tmp_path: Path) -> None:
    report = profile_floorplan(_make_fixture(tmp_path))
    assert report.selection_rule_version == SELECTION_RULE_VERSION
    assert report.selection_rule_text == SELECTION_RULE_TEXT
    assert report.source_sha256 is not None
    assert report.column_letter == "I"  # 户型图 第9列
    assert report.use_column_letter == "X"  # 房屋用途 第24列


def test_profile_writes_json_atomically(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    out = tmp_path / "fp.json"
    profile_floorplan(path, out_json=out)
    assert out.is_file()
    assert not out.with_name(out.name + ".incomplete").exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["selection_rule_version"] == SELECTION_RULE_VERSION
    assert data["ordinary"]["floorplan_candidate_count"] == 2


def test_profile_missing_source_raises(tmp_path: Path) -> None:
    with raises(FileNotFoundError):
        profile_floorplan(tmp_path / "nope.xlsx")