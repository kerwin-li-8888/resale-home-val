"""外部链家 staged 属性标准化（excel-attribute-enrichment）离线测试。

覆盖：五列解析函数（正常/暂无/空/乱文本/边界）、normalize_attributes_table
（原文列不变 + 标准化列 + 质量分布）、stage_attributes_run（不可变新 run、
指针切换、源 run 字节不变、质量 JSON、血缘 parser_version）。不触网。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from compsval.ingest.manifests import read_derived_manifest
from compsval.ingest.xlsx_attributes import (
    ATTRIBUTE_COLUMNS,
    ATTRIBUTES_RULE_VERSION,
    norm_decoration,
    normalize_attributes_table,
    parse_elevator,
    parse_floor,
    parse_year,
)
from compsval.ingest.xlsx_parse import FieldParseStatus
from compsval.ingest.xlsx_stage import (
    ORDINARY_FILENAME,
    SALE_RECORD_FILENAME,
    read_current_run,
    stage_attributes_run,
    stage_xlsx,
)

# ---------------------------------------------------------------------------
# 解析纯函数
# ---------------------------------------------------------------------------


def test_parse_floor_standard_buckets() -> None:
    assert parse_floor("高楼层/9层") == ("高楼层", 9, FieldParseStatus.PARSED)
    assert parse_floor("中楼层/共18层") == ("中楼层", 18, FieldParseStatus.PARSED)
    assert parse_floor("地下室/2层") == ("地下室", 2, FieldParseStatus.PARSED)
    assert parse_floor("顶层/共3层") == ("顶层", 3, FieldParseStatus.PARSED)


def test_parse_floor_missing_and_failures() -> None:
    assert parse_floor("暂无数据")[2] is FieldParseStatus.MISSING
    assert parse_floor("")[2] is FieldParseStatus.MISSING
    # 无「/N层」部分：段位保留、总层数缺失、整体 PARSE_FAILURE
    bucket, total, status = parse_floor("高楼层")
    assert bucket == "高楼层" and total is None
    assert status is FieldParseStatus.PARSE_FAILURE
    # 词表外段位：不臆造归类
    bucket2, total2, status2 = parse_floor("X楼层/9层")
    assert bucket2 is None and total2 == 9
    assert status2 is FieldParseStatus.PARSE_FAILURE
    # 非法总层数
    _b, _t, status3 = parse_floor("中楼层/abc层")
    assert status3 is FieldParseStatus.PARSE_FAILURE
    _b, zero, status4 = parse_floor("中楼层/0层")
    assert zero is None and status4 is FieldParseStatus.PARSE_FAILURE


def test_parse_year() -> None:
    assert parse_year("2015年") == (2015, FieldParseStatus.PARSED)
    assert parse_year("1999") == (1999, FieldParseStatus.PARSED)
    assert parse_year("暂无数据")[1] is FieldParseStatus.MISSING
    assert parse_year("20x5年")[1] is FieldParseStatus.PARSE_FAILURE
    assert parse_year("999年")[1] is FieldParseStatus.PARSE_FAILURE


def test_parse_elevator_disclosed_only() -> None:
    assert parse_elevator("有") == (True, FieldParseStatus.PARSED)
    assert parse_elevator("无") == (False, FieldParseStatus.PARSED)
    assert parse_elevator("暂无数据")[1] is FieldParseStatus.MISSING
    assert parse_elevator("maybe")[1] is FieldParseStatus.PARSE_FAILURE


def test_norm_decoration_vocabulary() -> None:
    assert norm_decoration("精装") == ("精装", FieldParseStatus.PARSED)
    assert norm_decoration("其他") == ("其他", FieldParseStatus.PARSED)
    assert norm_decoration("豪装")[1] is FieldParseStatus.PARSE_FAILURE
    assert norm_decoration("暂无")[1] is FieldParseStatus.MISSING


# ---------------------------------------------------------------------------
# 表级标准化：原文列不变 + 标准化列
# ---------------------------------------------------------------------------


def _raw_table() -> pa.Table:
    return pa.table(
        {
            "row_number": [1, 2, 3],
            "floor_raw": ["高楼层/9层", "暂无数据", "乱文本"],
            "built_year_raw": ["2005年", "暂无数据", "1980年"],
            "has_elevator_raw": ["有", "无", "暂无数据"],
            "decoration": ["精装", "毛坯", "豪装"],
            "orientation": ["南", "", "东南"],
        }
    )


def test_normalize_attributes_table_appends_and_keeps_raw() -> None:
    source = _raw_table()
    out, summary = normalize_attributes_table(source)
    # 原文列逐值不变
    for name in source.column_names:
        assert out.column(name).to_pylist() == source.column(name).to_pylist()
    # 标准化列
    assert out.column("floor_bucket").to_pylist() == ["高楼层", None, None]
    assert out.column("total_floors").to_pylist() == [9, None, None]
    assert out.column("year_built").to_pylist() == [2005, None, 1980]
    assert out.column("has_elevator").to_pylist() == [True, False, None]
    assert out.column("decoration_norm").to_pylist() == ["精装", "毛坯", None]
    # 质量分布：缺失与解析失败可区分，不落 0
    assert summary.row_count == 3
    assert summary.floor_status["MISSING"] == 1
    assert summary.floor_status["PARSE_FAILURE"] == 1
    assert summary.floor_joint_failure == 1
    assert summary.year_built_status["MISSING"] == 1
    assert summary.has_elevator_status["MISSING"] == 1
    assert summary.decoration_status["PARSE_FAILURE"] == 1
    assert summary.orientation_known == 2
    cov = summary.coverage()
    assert abs(cov["floor"] - 1 / 3) < 1e-9
    assert cov["year_built"] == 2 / 3
    payload = summary.to_dict()
    assert payload["rule_version"] == ATTRIBUTES_RULE_VERSION


def test_normalize_is_idempotent_on_v2_table() -> None:
    once, _ = normalize_attributes_table(_raw_table())
    twice, summary = normalize_attributes_table(once)
    assert twice.column_names == once.column_names
    assert summary.row_count == 3


# ---------------------------------------------------------------------------
# stage_attributes_run：不可变新 run + 指针 + 源 run 字节不变
# ---------------------------------------------------------------------------

_HEADERS = [
    "房屋ID", "小区名字", "小区ID", "成交日期", "成交总价", "成交均价",
    "房屋面积", "户型", "朝向", "楼层", "是否有电梯", "装修情况", "建成时间",
    "房屋用途", "挂牌价格", "成交天数", "房源描述", "房源描述.1", "户型图",
]


def _xlsx_row(**kw: object) -> list[object]:
    values: list[object] = [None] * len(_HEADERS)
    base = {
        "房屋ID": "R1",
        "小区名字": "示例小区121",
        "小区ID": "2811019201",
        "成交日期": datetime(2023, 12, 17),
        "成交总价": "700000",
        "成交均价": "21814",
        "房屋面积": "32.09",
        "户型": "1室1厅1卫",
        "朝向": "南",
        "楼层": "高楼层/9层",
        "是否有电梯": "有",
        "装修情况": "精装",
        "建成时间": "2005年",
        "房屋用途": "普通住宅",
    }
    base.update(kw)
    for name, value in base.items():
        if value is not None:
            values[_HEADERS.index(name)] = value
    return values


def _make_source_lake(tmp_path: Path) -> Path:
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.append(_HEADERS)
    ws.append(_xlsx_row())
    ws.append(
        _xlsx_row(
            房屋ID="R2",
            楼层="暂无数据",
            是否有电梯="暂无数据",
            建成时间="暂无数据",
            装修情况="毛坯",
        )
    )
    ws.append(_xlsx_row(房屋ID="R3", 房屋用途="车库", 楼层="乱文本", 装修情况="豪装"))
    xlsx = tmp_path / "src.xlsx"
    wb.save(xlsx)
    lake = tmp_path / "lake"
    result = stage_xlsx(xlsx, data_dir=lake, run_id="20260101T000000Z")
    assert result.run_id == "20260101T000000Z"
    return lake


def _dir_sha256(root: Path) -> dict[str, str]:
    import hashlib

    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            out[str(path.relative_to(root))] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return out


def test_stage_attributes_run_immutable_and_pointer(tmp_path: Path) -> None:
    lake = _make_source_lake(tmp_path)
    source_dir = lake / "staged" / "lianjia_ext" / "runs" / "run_20260101T000000Z"
    before = _dir_sha256(source_dir)

    result = stage_attributes_run(
        "20260101T000000Z", data_dir=lake, target_run_id="20260102T000000Z"
    )
    assert result.run_id == "20260102T000000Z"
    assert result.sale_record_count == 3
    assert result.ordinary_residential_count == 2

    # 源 run 逐字节不变（含 manifest）
    assert _dir_sha256(source_dir) == before

    # 指针切到新 run，且新 run 两表含标准化列
    current = read_current_run(lake)
    assert current is not None and current["run_id"] == "20260102T000000Z"
    ordinary = pq.read_table(result.ordinary_residential_path)
    for name in ATTRIBUTE_COLUMNS:
        assert name in ordinary.column_names
    # 守恒：原文列值保留
    assert ordinary.column("floor_raw").to_pylist() == ["高楼层/9层", "暂无数据"]
    assert ordinary.column("total_floors").to_pylist() == [9, None]
    assert ordinary.column("has_elevator").to_pylist() == [True, None]

    # 质量摘要落盘且覆盖率与行数一致
    import json

    quality = json.loads(result.quality_json.read_text(encoding="utf-8"))
    assert quality["source_run_id"] == "20260101T000000Z"
    assert quality["ordinary_residential"]["row_count"] == 2

    # 血缘：parser_version 记录属性规则版本，inputs 指向源 run（含 hash）
    manifest = read_derived_manifest(result.ordinary_residential_path)
    assert manifest.parser_version == ATTRIBUTES_RULE_VERSION
    assert manifest.inputs[0].fetched_at == "20260101T000000Z"
    assert manifest.inputs[0].content_hash is not None

    # run 不可变：同目标 run_id 再次执行 → FileExistsError
    with pytest.raises(FileExistsError):
        stage_attributes_run(
            "20260101T000000Z", data_dir=lake, target_run_id="20260102T000000Z"
        )


def test_stage_attributes_run_missing_source(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    with pytest.raises(FileNotFoundError):
        stage_attributes_run("no-such-run", data_dir=lake)
    assert SALE_RECORD_FILENAME  # 保持导入被使用
    assert ORDINARY_FILENAME
