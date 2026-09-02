"""EXTFP1 工作包离线测试集成（EXTFP1-F，技术方案 §10.4 四类测试）。

用合成 XLSX fixture 把 EXTFP1 完整链路串接为一条数据管道：二进制快照
（write_binary_snapshot）→ 全量解析（iter_parse_xlsx）→ 普通住宅 staged 表
（stage_xlsx）→ 质量报告（build_xlsx_quality_report），覆盖正常/边界/缺失/
反例四类场景；并验证 schema 向后兼容（SnapshotManifest.mime_type 旧 JSON 可
反序列化）与重复运行一致。全部离线（openpyxl 在 tmp_path 合成），不触碰真实
外部数据文件，不访问网络。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from openpyxl import Workbook

from compsval.ingest.binary_snapshot import write_binary_snapshot
from compsval.ingest.manifests import SnapshotManifest, read_derived_manifest
from compsval.ingest.xlsx_parse import iter_parse_xlsx, summarize
from compsval.ingest.xlsx_quality import (
    build_xlsx_quality_report,
    report_to_dict,
)
from compsval.ingest.xlsx_stage import (
    ORDINARY_FILENAME,
    SALE_RECORD_FILENAME,
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
        "建筑面积": "26.06",
        "户型": "1室1厅1卫",
        "房屋用途": "普通住宅",
        "户型图": f"['{FP_URL}']",
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
    """12 行合成 XLSX，覆盖四类场景（正常/边界/缺失/反例）。"""
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    rows = [
        # 正常：普通住宅完整字段 + 单候选 URL
        _row(),
        _row(房屋ID="R2", 房屋用途="普通住宅", 成交日期="2023-11-13",
             成交总价="250000", 成交均价="12345", 房屋面积="20.25",
             建筑面积=None, 户型="2室1厅1卫"),
        # 边界：面积带单位后缀、多 URL（候选+占位共存）、挂牌价万元
        _row(房屋ID="R3", 房屋用途="普通住宅", 成交总价="880000",
             成交均价="14681", 房屋面积="11.92㎡", 建筑面积="10.5㎡",
             户型图=f"['{FP_URL}', '{PH_URL}']", 挂牌价格="118.0", 成交天数="21"),
        # 缺失：空 URL、纯占位、用途暂无、无面积、无日期
        _row(房屋ID="R4", 房屋用途="普通住宅", 成交日期=None, 成交总价="暂无数据",
             成交均价="-", 房屋面积="88.5㎡", 建筑面积="暂无数据",
             户型图=f"['{PH_URL}']", 挂牌价格="暂无", 成交天数="暂无数据"),
        _row(房屋ID="R5", 房屋用途="暂无", 成交日期=None, 成交总价=None, 成交均价=None,
             房屋面积=None, 建筑面积=None, 户型图=None, 挂牌价格=None, 成交天数=None),
        _row(房屋ID="R6", 房屋用途="普通住宅", 成交总价="175000", 成交均价="14681",
             房屋面积="11.92", 建筑面积=None, 户型图=""),
        # 反例：商住两用/车库/别墅混入、非字符串 URL 数组、ftp URL
        _row(房屋ID="R7", 房屋用途="商住两用", 成交总价="165000", 成交均价="16500",
             房屋面积="10.0", 建筑面积=None),
        _row(房屋ID="R8", 房屋用途="车库", 成交总价="175000", 成交均价="14681",
             房屋面积="11.92", 建筑面积=None, 户型图=f"['{PH_URL}']"),
        _row(房屋ID="R9", 房屋用途="别墅", 成交总价="8000000", 成交均价="50000",
             房屋面积="160.0", 建筑面积="200.0", 户型图=f"['{FP_URL}']"),
        _row(房屋ID="R10", 房屋用途="普通住宅", 成交总价="600000", 成交均价="25000",
             房屋面积="24.0", 建筑面积=None, 户型图="[12345]"),
        _row(房屋ID="R11", 房屋用途="平房", 成交总价="300000", 成交均价="20000",
             房屋面积="15.0", 建筑面积=None),
        _row(房屋ID="R12", 房屋用途="普通住宅", 成交总价="520000", 成交均价="26000",
             房屋面积="20.0", 建筑面积="19.0", 户型图="['ftp://x/1.jpg']"),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "fixture.xlsx"
    wb.save(path)
    return path


def _xlsx_bytes(path: Path) -> bytes:
    return path.read_bytes()


def test_full_chain_binary_to_quality(tmp_path: Path) -> None:
    """完整链路：二进制快照 → 解析 → staged 两表 → 质量报告，数字守恒闭合。"""
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"

    # 1. 二进制快照（原始字节不可变）
    bs = write_binary_snapshot(
        path, source="lianjia_ext", dataset="chengjiao_xlsx",
        fetched_at=datetime(2026, 8, 24, 0, 0, 0, tzinfo=UTC), query="q", root=lake,
    )
    assert bs.data_path.read_bytes() == _xlsx_bytes(path)  # 字节守恒
    assert bs.raw_snapshot.mime_type is not None  # mime_type 补填

    # 2. 全量解析（守恒）
    records = list(iter_parse_xlsx(path))
    summary = summarize(records)
    assert summary.data_rows_total == 12
    # 普通住宅：R0,R2,R3,R4,R6,R10,R12 = 7 行；R5 UNKNOWN、R7/R8/R9/R11 排除
    assert summary.ordinary_residential_count == 7

    # 3. staged 两表（守恒，不可变 run + current 指针）
    staged = stage_xlsx(path, data_dir=lake, run_id="20260825T000000Z")
    assert staged.sale_record_count == 12
    assert staged.ordinary_residential_count == 7
    assert staged.excluded_count == 5
    assert pq.read_table(staged.sale_record_path).num_rows == 12
    assert pq.read_table(staged.ordinary_residential_path).num_rows == 7
    assert staged.run_id == "20260825T000000Z"
    # 结构化血缘：inputs 指向二进制快照（含 content_hash）

    sale_manifest = read_derived_manifest(staged.sale_record_path)
    (inp,) = sale_manifest.inputs
    assert inp.dataset == "chengjiao_xlsx"
    assert inp.content_hash == bs.manifest.files[0].sha256

    # 4. 质量报告（守恒 + 统计，真实 manifest 呈现）
    from compsval.ingest.xlsx_quality import load_staged_tables

    sale, ordinary, manifests, run_id = load_staged_tables(lake)
    quality = build_xlsx_quality_report(
        sale, ordinary, staged_manifests=manifests, git_baseline="c1"
    )
    assert quality.counts["sale_record_rows"] == 12
    assert quality.counts["ordinary_residential_rows"] == 7
    assert quality.counts["conserved"] == 1
    assert len(quality.staged_tables) == 2  # CX-EXTFP1-003
    assert run_id == "20260825T000000Z"


def test_normal_ordinary_residential_full_fields(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    records = list(iter_parse_xlsx(path))
    r0 = records[0]
    assert r0.property_use_norm.value == "普通住宅"
    assert r0.sale_date == "2023-12-17"
    assert r0.total_price_yuan == 700000
    assert r0.unit_price_observed == 21814
    assert r0.transaction_area_sqm == Decimal("32.09")
    assert r0.listing_price_yuan == 950000  # 挂牌万元 → 元
    assert r0.floorplan_candidate_count == 1


def test_boundary_area_suffix_and_multi_url(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    records = {r.source_record_id: r for r in iter_parse_xlsx(path)}
    r3 = records["R3"]
    assert r3.transaction_area_sqm == Decimal("11.92")  # "11.92㎡" 剥离后缀
    assert r3.building_area_detail_sqm == Decimal("10.5")
    assert r3.floorplan_candidate_count == 1  # 候选+占位共存，只计候选
    assert r3.listing_price_yuan == 1180000  # 118.0 万 → 元


def test_missing_placeholder_and_unknown(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    records = {r.source_record_id: r for r in iter_parse_xlsx(path)}
    r4 = records["R4"]
    assert r4.total_price_status.value == "MISSING"  # 暂无数据
    assert r4.unit_price_status.value == "PARSE_FAILURE"  # "-"
    assert r4.listing_price_status.value == "MISSING"
    assert r4.floorplan_url_status == "URLS_OK"
    assert r4.floorplan_candidate_count == 0  # 纯占位
    r5 = records["R5"]
    assert r5.property_use_norm.value == "UNKNOWN"
    assert r5.floorplan_url_status == "NO_URL"
    assert r5.area_status.value == "MISSING"
    r6 = records["R6"]
    assert r6.floorplan_url_status == "NO_URL"  # 空串 URL


def test_counterexample_excluded_uses_and_bad_urls(tmp_path: Path) -> None:
    path = _make_fixture(tmp_path)
    records = {r.source_record_id: r for r in iter_parse_xlsx(path)}
    for rid in ("R7", "R8", "R9", "R11"):
        assert records[rid].property_use_norm.value == "非普通住宅"
    r10 = records["R10"]
    assert r10.floorplan_url_status == "URL_PARSE_FAILURE"  # [12345] 非字符串
    r12 = records["R12"]
    assert r12.floorplan_candidate_count == 0  # ftp:// 非候选


def test_schema_backward_compatible_manifest(tmp_path: Path) -> None:
    """EXTFP1-B 合同扩展不破坏旧 manifest：无 mime_type 仍反序列化且默认 None。"""
    path = _make_fixture(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        path, source="lianjia_ext", dataset="chengjiao_xlsx",
        fetched_at=datetime(2026, 8, 24, 0, 0, 0, tzinfo=UTC), query="q", root=lake,
    )
    manifest_path = next((lake / "raw").rglob("manifest.json"))
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data.pop("mime_type", None)  # 模拟旧 manifest
    manifest_path.write_text(json.dumps(data), encoding="utf-8")
    reloaded = SnapshotManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    assert reloaded.mime_type is None


def test_repeatable_stage_and_quality(tmp_path: Path) -> None:
    """同一输入两次 stage+quality 业务数字一致（可复现）；两个 run 都保留。"""
    path = _make_fixture(tmp_path)
    lake = tmp_path / "l1"
    r1 = stage_xlsx(path, data_dir=lake, run_id="20260825T000000Z")
    r2 = stage_xlsx(path, data_dir=lake, run_id="20260825T010000Z")

    def _business(run_dir: Path) -> tuple[dict, dict]:
        sale = pq.read_table(run_dir / SALE_RECORD_FILENAME)
        ordinary = pq.read_table(run_dir / ORDINARY_FILENAME)
        rep = build_xlsx_quality_report(sale, ordinary)
        d = report_to_dict(rep)
        d.pop("built_at", None)  # 执行时刻非业务字段
        return d["counts"], d["area_consistency"]

    assert _business(r1.run_dir) == _business(r2.run_dir)
    # 两个 run 产物均保留（不覆盖）
    assert r1.sale_record_path.is_file()
    assert r2.sale_record_path.is_file()
    # 血缘 manifest 行数一致 + parser_version

    m1 = read_derived_manifest(r1.sale_record_path)
    m2 = read_derived_manifest(r2.sale_record_path)
    assert m1.row_count == 12 and m2.row_count == 12
    assert m1.parser_version == "EXTFP1-C-1.0"
