"""EXTFP1-E 质量报告与回滚点的离线测试。

用合成 XLSX fixture 走 stage → quality 链路，验证质量报告数字、守恒校验、
MD+JSON 一致性、回滚点字段与 CLI。绝不触碰真实外部数据文件，也不访问网络。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from openpyxl import Workbook

from compsval import cli
from compsval.ingest.xlsx_quality import (
    build_xlsx_quality_report,
    load_staged_tables,
    write_xlsx_quality,
)
from compsval.ingest.xlsx_stage import stage_xlsx

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


def _make_fixture(tmp_path: Path) -> Path:
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
             房屋面积="11.92", 建筑面积=None),
        _row(房屋ID="R4", 房屋用途="普通住宅", 成交日期="2023-11-13", 成交总价="暂无数据",
             成交均价="-", 房屋面积="88.5㎡", 建筑面积="90.5㎡", 挂牌价格="暂无",
             成交天数="暂无数据", 户型图="['http://a.example.com/1.jpg', 'http://b.example.com/2.jpg']"),
        _row(房屋ID="R5", 房屋用途="暂无", 成交日期=None, 成交总价=None, 成交均价=None,
             房屋面积=None, 建筑面积=None, 户型图=None, 挂牌价格=None, 成交天数=None),
        _row(房屋ID="R6", 房屋用途="普通住宅", 成交总价="250000", 成交均价="12345",
             房屋面积="20.25", 建筑面积=None),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "fixture.xlsx"
    wb.save(path)
    return path


def _stage_tables(
    tmp_path: Path,
) -> tuple[pa.Table, pa.Table, Path, list[dict[str, object]], str]:
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    stage_xlsx(path, data_dir=lake, run_id="20260825T000000Z")
    sale, ordinary, manifests, run_id = load_staged_tables(lake)
    return sale, ordinary, lake, manifests, run_id


def test_build_quality_report_conservation(tmp_path: Path) -> None:
    sale, ordinary, _, manifests, run_id = _stage_tables(tmp_path)
    report = build_xlsx_quality_report(
        sale, ordinary, source_sha256="abc", git_baseline="c1",
        staged_manifests=manifests,
    )
    c = report.counts
    assert c["sale_record_rows"] == 6
    assert c["ordinary_residential_rows"] == 3
    assert c["excluded_rows"] == 3
    assert c["conserved"] == 1
    assert report.source_sha256 == "abc"
    assert report.rollback["git_baseline"] == "c1"
    assert "staged/lianjia_ext/runs" in str(report.rollback["data_artifacts"])
    # CX-EXTFP1-003：staged_tables 呈现真实 manifest 摘要
    assert len(report.staged_tables) == 2
    assert report.staged_tables[0]["table"] == "lianjia_ext_sale_record"
    assert report.staged_tables[0]["row_count"] == 6
    assert report.staged_tables[1]["table"] == "lianjia_ext_ordinary_residential"
    assert report.staged_tables[1]["row_count"] == 3
    assert run_id == "20260825T000000Z"


def test_quality_fail_closed_on_manifest_mismatch(tmp_path: Path) -> None:
    """CX-EXTFP1-003：manifest row_count 与表不符 → 失败关闭。"""
    sale, ordinary, _, manifests, _ = _stage_tables(tmp_path)
    bad = dict(manifests[0])
    bad["row_count"] = 999
    with pytest.raises(ValueError, match="row_count mismatch"):
        build_xlsx_quality_report(
            sale, ordinary, staged_manifests=[bad, manifests[1]]
        )


def test_quality_field_status_and_url_distribution(tmp_path: Path) -> None:
    sale, ordinary, _, manifests, _ = _stage_tables(tmp_path)
    report = build_xlsx_quality_report(sale, ordinary, staged_manifests=manifests)
    # 字段状态：R4 总价 MISSING、R5 全 MISSING、R6 均价 PARSE_FAILURE（"-" 在均价列）
    assert report.field_status["total_price_status"].PARSED == 4
    assert report.field_status["total_price_status"].MISSING == 2  # R4/R5
    assert report.field_status["area_status"].PARSED == 5  # R0/R1/R2/R4/R6
    assert report.field_status["area_status"].MISSING == 1  # R5
    assert report.field_status["area_status"].PARSE_FAILURE == 0
    assert report.field_status["unit_price_status"].PARSE_FAILURE == 1  # R4 "-"
    # URL 分布：R5 无 URL；R4 多 URL 候选 2
    url = report.floorplan_url_status
    assert url["NO_URL"] == 1
    assert url["URLS_OK"] == 5
    assert url["FLOORPLAN_CANDIDATE_RECORDS"] == 5
    assert url["FLOORPLAN_CANDIDATE_URLS"] == 6  # R0/R2/R3/R6 各1 + R4 各2
    # 面积一致性：R0/R4 两列均可解析且不等；R1/R2/R3 仅第17列
    a = report.area_consistency
    assert a["both_parseable_differ"] == 2
    assert a["only_area_17"] == 3
    assert a["both_parseable_equal"] == 0


def test_write_quality_md_and_json_same_data(tmp_path: Path) -> None:
    sale, ordinary, lake, manifests, _ = _stage_tables(tmp_path)
    report = build_xlsx_quality_report(
        sale, ordinary, git_baseline="c1", staged_manifests=manifests
    )
    out = tmp_path / "quality.json"
    md_path, json_path = write_xlsx_quality(report, data_dir=lake, out_json=out)
    assert md_path.is_file()
    assert json_path.is_file()
    md = md_path.read_text(encoding="utf-8")
    data = json.loads(json_path.read_text(encoding="utf-8"))
    # MD 与 JSON 描述同一冻结数据
    assert str(data["counts"]["sale_record_rows"]) in md
    assert "普通住宅" in md
    assert data["rollback"]["git_baseline"] == "c1"
    assert data["report_version"] == "EXTFP1-E-1.0"
    # CX-EXTFP1-003：staged_tables 在 MD 中可呈现
    assert "lianjia_ext_sale_record" in md
    assert data["staged_tables"][0]["table"] == "lianjia_ext_sale_record"
    # 原子写：无 .incomplete 残留
    assert not md_path.with_name(md_path.name + ".incomplete").exists()
    assert not json_path.with_name(json_path.name + ".incomplete").exists()


def test_cli_xlsx_quality(tmp_path: Path, capsys) -> None:
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    stage_xlsx(path, data_dir=lake, run_id="20260825T000000Z")
    out = tmp_path / "q.json"
    assert (
        cli.main(
            ["xlsx", "quality", "--data-dir", str(lake), "--out", str(out),
             "--git-baseline", "c1"]
        )
        == 0
    )
    captured = capsys.readouterr().out
    assert "守恒=通过" in captured
    assert "run_id=20260825T000000Z" in captured
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["counts"]["sale_record_rows"] == 6
    assert data["rollback"]["git_baseline"] == "c1"
    # CX-EXTFP1-003：staged_tables 呈现真实 manifest（含 inputs 血缘）
    assert len(data["staged_tables"]) == 2
    assert data["staged_tables"][0]["inputs"] != [] or data["staged_tables"][0].get("inputs")
    assert data["source_sha256"] is not None


def test_cli_xlsx_quality_without_staged_fails(tmp_path: Path, capsys) -> None:
    assert (
        cli.main(
            [
                "xlsx", "quality",
                "--data-dir", str(tmp_path / "empty-lake"),
                "--out", str(tmp_path / "q.json"),
            ]
        )
        == 1
    )
    assert "先运行 compsval xlsx stage" in capsys.readouterr().out
