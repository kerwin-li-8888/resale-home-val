"""EXTFP1-D 普通住宅 staged 表的离线测试。

用 openpyxl 合成 XLSX fixture 验证：全量 sale_record 表构建、普通住宅过滤
守恒、血缘 manifest、原子写与 CLI。绝不触碰真实外部数据文件，也不访问网络。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from compsval import cli
from compsval.ingest.manifests import InputRef, read_derived_manifest
from compsval.ingest.xlsx_parse import iter_parse_xlsx
from compsval.ingest.xlsx_stage import (
    ORDINARY_FILENAME,
    ORDINARY_RESIDENTIAL_TABLE,
    SALE_RECORD_FILENAME,
    SALE_RECORD_TABLE,
    ordinary_residential_table,
    read_current_run,
    sale_record_table,
    stage_xlsx,
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
        "建筑面积": "26.06",
        "户型": "1室1厅1卫",
        "房屋用途": "普通住宅",
        "户型图": "['http://ke-image.ljcdn.com/hdic-frame/abc.jpg']",
        "挂牌价格": "95.0",
        "成交天数": "159",
        "房源描述": "朝东",
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
        _row(),  # 普通住宅
        _row(房屋ID="R2", 房屋用途="商住两用", 成交总价="165000", 成交均价="16500",
             房屋面积="10.0", 建筑面积=None),
        _row(房屋ID="R3", 房屋用途="车库", 成交总价="175000", 成交均价="14681",
             房屋面积="11.92"),
        _row(房屋ID="R4", 房屋用途="普通住宅", 成交日期="2023-11-13", 成交总价="暂无数据",
             成交均价="-", 房屋面积="88.5㎡", 建筑面积="90.5㎡", 挂牌价格="暂无",
             成交天数="暂无数据"),
        _row(房屋ID="R5", 房屋用途="暂无", 成交日期=None, 成交总价=None, 成交均价=None,
             房屋面积=None, 建筑面积=None, 户型图=None, 挂牌价格=None, 成交天数=None),
        _row(房屋ID="R6", 房屋用途="普通住宅", 成交总价="250000", 成交均价="12345",
             房屋面积="20.25", 建筑面积=None),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / name
    wb.save(path)
    return path


def test_sale_record_table_full_build(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    table = sale_record_table(iter_parse_xlsx(path))
    assert table.num_rows == 6
    assert table.schema.field("total_price_yuan").type == __import__("pyarrow").decimal128(18, 2)
    assert table.schema.field("property_use_norm").type == __import__("pyarrow").string()
    names = set(table.column_names)
    assert "extra_fields_json" in names  # 其余列原文全量保留为 JSON 字符串
    # 守恒：全量 6 行含全部用途
    norms = table.column("property_use_norm").to_pylist()
    assert norms.count("普通住宅") == 3


def test_ordinary_residential_filter_conservation(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    full = sale_record_table(iter_parse_xlsx(path))
    ordinary = ordinary_residential_table(full)
    assert ordinary.num_rows == 3  # R0/R4/R6
    assert all(n == "普通住宅" for n in ordinary.column("property_use_norm").to_pylist())
    assert full.num_rows == ordinary.num_rows + 3  # 排除商住两用/车库/UNKNOWN


def test_stage_xlsx_writes_immutable_run_and_lineage(tmp_path: Path) -> None:
    """CX-EXTFP1-001/-002 修复：run 目录不可变 + current 指针 + 结构化 inputs。"""
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    result = stage_xlsx(
        path, data_dir=lake, run_id="20260825T000000Z",
        inputs=[InputRef(dataset="chengjiao_xlsx", fetched_at="20260824T000000Z",
                         content_hash="abc123")],
    )

    assert result.run_id == "20260825T000000Z"
    assert result.sale_record_count == 6
    assert result.ordinary_residential_count == 3
    assert result.excluded_count == 3

    # 不可变 run 目录布局
    run_dir = lake / "staged" / "lianjia_ext" / "runs" / "run_20260825T000000Z"
    assert result.run_dir == run_dir
    sale_path = run_dir / SALE_RECORD_FILENAME
    ordinary_path = run_dir / ORDINARY_FILENAME
    assert result.sale_record_path == sale_path
    assert result.ordinary_residential_path == ordinary_path
    assert pq.read_table(sale_path).num_rows == 6
    assert pq.read_table(ordinary_path).num_rows == 3

    # 当前指针指向该 run
    current = read_current_run(lake)
    assert current == {
        "run_id": "20260825T000000Z",
        "sale_record": "runs/run_20260825T000000Z/lianjia_ext_sale_record.parquet",
        "ordinary_residential": (
            "runs/run_20260825T000000Z/lianjia_ext_ordinary_residential.parquet"
        ),
    }

    # 血缘 manifest：结构化 inputs 指向实际快照（含 content_hash）+ parser_version
    sale_manifest = read_derived_manifest(sale_path)
    assert sale_manifest.layer == "staged"
    assert sale_manifest.table == SALE_RECORD_TABLE
    assert sale_manifest.row_count == 6
    assert sale_manifest.parser_version == "EXTFP1-C-1.0"
    (inp,) = sale_manifest.inputs
    assert inp.dataset == "chengjiao_xlsx"
    assert inp.fetched_at == "20260824T000000Z"
    assert inp.content_hash == "abc123"
    ordinary_manifest = read_derived_manifest(ordinary_path)
    assert ordinary_manifest.table == ORDINARY_RESIDENTIAL_TABLE
    assert ordinary_manifest.row_count == 3
    assert ordinary_manifest.parser_version == "EXTFP1-C-1.0"
    assert ordinary_manifest.inputs[0].content_hash == "abc123"

    # 原子写：无 .incomplete 残留（run 目录与指针）
    assert not run_dir.with_name(run_dir.name + ".incomplete").exists()
    assert not result.current_pointer.with_name("current.json.incomplete").exists()


def test_stage_immutable_runs_counterexample(tmp_path: Path) -> None:
    """CX-EXTFP1-001 反例：不同输入两次 stage，旧 run 产物永久保留、指针切换。"""
    lake = tmp_path / "lake"
    path_a = _make_fixture(tmp_path, name="a.xlsx")
    path_b = tmp_path / "b.xlsx"
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    ws.append(_row(房屋ID="B-ONLY", 房屋用途="普通住宅", 成交总价="111111",
                   成交均价="11111", 房屋面积="10.0", 建筑面积=None))
    wb.save(path_b)

    r1 = stage_xlsx(path_a, data_dir=lake, run_id="20260825T000000Z")
    r2 = stage_xlsx(path_b, data_dir=lake, run_id="20260825T010000Z")

    # 两个 run 产物都存在且可读
    t1 = pq.read_table(r1.sale_record_path).column("source_record_id").to_pylist()
    t2 = pq.read_table(r2.sale_record_path).column("source_record_id").to_pylist()
    assert "108404666013" in t1 and "B-ONLY" not in t1  # run1 = 输入 A
    assert t2 == ["B-ONLY"]  # run2 = 输入 B
    # 指针切换为最新 run；run1 仍保留（未被覆盖）
    current = read_current_run(lake)
    assert current["run_id"] == "20260825T010000Z"
    assert r1.sale_record_path.is_file()

    # 同 run_id 重跑拒绝（run 不可变）
    with pytest.raises(FileExistsError):
        stage_xlsx(path_a, data_dir=lake, run_id="20260825T000000Z")


def test_stage_repeatable_same_input(tmp_path: Path) -> None:
    """同一输入两次 stage（不同 run）业务列一致（可复现）。"""
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    r1 = stage_xlsx(path, data_dir=lake, run_id="20260825T000000Z")
    r2 = stage_xlsx(path, data_dir=lake, run_id="20260825T010000Z")
    first = pq.read_table(r1.sale_record_path).to_pydict()
    second = pq.read_table(r2.sale_record_path).to_pydict()
    assert first == second
    assert r1.run_id != r2.run_id


def test_cli_xlsx_stage_success(tmp_path: Path, capsys) -> None:
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    assert (
        cli.main(
            ["xlsx", "stage", "--input", str(path), "--data-dir", str(lake)]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "sale_record=6" in out
    assert "普通住宅=3" in out
    assert "排除=3" in out
    assert "守恒=True" in out
    assert "run_id=" in out
    current = read_current_run(lake)
    assert current is not None
    assert (lake / "staged" / "lianjia_ext" / current["sale_record"]).is_file()
    assert (lake / "staged" / "lianjia_ext" / current["ordinary_residential"]).is_file()


def test_cli_xlsx_stage_missing_input_fails(tmp_path: Path, capsys) -> None:
    assert (
        cli.main(
            [
                "xlsx", "stage",
                "--input", str(tmp_path / "missing.xlsx"),
                "--data-dir", str(tmp_path / "lake"),
            ]
        )
        == 1
    )
    assert "input not found" in capsys.readouterr().out
