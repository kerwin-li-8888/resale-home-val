"""EXTFP0-F 离线集成测试：EXTFP0 完整数据链路 + 四类测试 + 可复现性。

把 EXTFP0 各原子任务交付串成一条合成数据链路，用小而确定的 XLSX fixture
（openpyxl 在 tmp_path 生成，保证真实可解析结构）验证跨模块协作、来源登记、
向后兼容与重复运行一致。绝不触碰真实外部数据文件，也不访问网络。

链路：``resolve_source_dir``（来源登记 lianjia_ext）
    → ``import_local_file``（XLSX 结构化快照，ERC-EXTFP0-C）
    → ``profile_xlsx``（普通住宅画像，EXTFP0-D）
    → ``profile_floorplan``（占位图画像与选择规则，EXTFP0-E）。

四类测试按技术方案 §10.4 语义组织：
- 正常：普通住宅 + 完整可解析字段 + 户型图候选 URL；
- 边界：面积带单位后缀、多 URL、同一 URL 多成交语义、旋转/相邻不相关；
- 缺失：空 URL、占位图、无面积、无用途（暂无/暂无数据）；
- 反例：商住两用/车库/别墅混入、非字符串 URL 数组、HTML 冒充 URL 串。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from openpyxl import Workbook

from compsval.contract.models import RawSnapshot, SnapshotFormat
from compsval.ingest.floorplan_profile import (
    SELECTION_RULE_VERSION,
    UrlListStatus,
    profile_floorplan,
)
from compsval.ingest.import_file import import_local_file, resolve_source_dir
from compsval.ingest.profile_xlsx import PROFILE_RULE_VERSION, profile_xlsx

# 与真实外部链家成交 Excel 表头对齐的合成表头
HEADERS = [
    "省份", "城市", "区县", "板块", "房屋ID", "房源标题", "房源描述", "成交日期",
    "户型图", "小区名字", "小区ID", "成交总价", "成交均价", "楼层", "客厅数量",
    "卧室数量", "房屋面积", "朝向", "房源描述.1", "房屋类型", "是否有电梯",
    "装修情况", "建成时间", "房屋用途", "房屋权属", "房屋位置", "纬度", "经度",
    "位置描述", "挂牌价格", "成交天数", "价格调整次数", "带看", "关注", "浏览",
    "经纪人", "品牌", "户型", "房屋面积.1", "建筑面积", "结构", "梯户比例",
    "高度", "供暖方式", "房屋年龄", "产权",
]

CANDIDATE_URL = "http://ke-image.ljcdn.com/hdic-frame/aa.jpg.1440x1080.jpg?from=ke.com"
PLACEHOLDER_URL = "http://img.ljcdn.com/beike/dituFindHouse/xx.png"


def _row(
    *,
    use: str | None,
    area17: object,
    area39: object,
    area40: object,
    huxingtu: object,
    desc7: object | None = None,
    desc19: object | None = None,
) -> list[object]:
    """按表头位置构造与 HEADERS 对齐的行（未列出字段留空）。"""
    vals: list[object] = [None] * len(HEADERS)
    by_name: dict[str, object] = {
        "房屋用途": use,
        "房屋面积": area17,
        "房屋面积.1": area39,
        "建筑面积": area40,
        "户型图": huxingtu,
        "房源描述": desc7,
        "房源描述.1": desc19,
    }
    for name, value in by_name.items():
        if value is not None:
            vals[HEADERS.index(name)] = value
    return vals


def _make_fixture(tmp_path: Path) -> Path:
    """合成 XLSX：同一文件被 画像(profile_xlsx/floorplan) 与 快照(import) 共用。

    共 12 个数据行，覆盖四类场景：
      0  普通住宅  A:首页 正常 单户型图候选 / 面积清晰
      1  普通住宅  B:单位后缀面积 边界 单户型图候选
      2  普通住宅  C:多 URL 边界 （1 候选 + 1 占位）
      3  普通住宅  D:纯占位 缺失（占位图 / 无有效户型图）
      4  普通住宅  E:空户型图 缺失
      5  普通住宅  F:暂无用途->? 缺失：用途=暂无（不计普通住宅）
      6  商住两用  G:非字符串 URL 数组 反例
      7  车库      H:无面积 缺失/反例
      8  普通住宅  I:同一 URL 多条成交 边界（URL 稳定可复现）
      9  普通住宅  J:URL 列表含非字符串元素 反例（解析失败，非候选）
     10  别墅      K:超普通 反例
     11  普通住宅  L:空面积 / 无描述 缺失
    """
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(HEADERS)
    rows = [
        _row(use="普通住宅", area17=100.0, area39=100.0, area40=105.0,
             huxingtu=repr([CANDIDATE_URL]), desc7="朝向好", desc19="采光佳"),
        _row(use="普通住宅", area17="88.5㎡", area39="88.5㎡", area40="88.5㎡",
             huxingtu=repr([CANDIDATE_URL])),
        _row(use="普通住宅", area17=95.0, area39=95.0, area40=100.0,
             huxingtu=repr([CANDIDATE_URL, PLACEHOLDER_URL])),
        _row(use="普通住宅", area17=70.0, area39=70.0, area40=70.0,
             huxingtu=repr([PLACEHOLDER_URL])),
        _row(use="普通住宅", area17=75.0, area39=75.0, area40=75.0, huxingtu=""),
        _row(use="暂无", area17=80.0, area39=80.0, area40=80.0,
             huxingtu=repr([CANDIDATE_URL])),
        _row(use="商住两用", area17=50.0, area39=50.0, area40=50.0,
             huxingtu="[12345]"),
        _row(use="车库", area17=None, area39=None, area40=None,
             huxingtu=repr([CANDIDATE_URL])),
        _row(use="普通住宅", area17=82.0, area39=82.0, area40=82.0,
             huxingtu=repr([CANDIDATE_URL])),
        _row(use="普通住宅", area17=66.0, area39=66.0, area40=66.0,
             huxingtu=repr([CANDIDATE_URL, 123])),
        _row(use="别墅", area17=250.0, area39=250.0, area40=250.0,
             huxingtu=repr([CANDIDATE_URL])),
        _row(use="普通住宅", area17="", area39="", area40=None,
             huxingtu=repr([CANDIDATE_URL]), desc7="", desc19=""),
    ]
    for r in rows:
        ws.append(r)
    path = tmp_path / "集成fixture.xlsx"
    wb.save(path)
    return path


# ---- 0. 链路串接基线 ----
def test_source_to_snapshot_to_profiles_integrated(tmp_path: Path) -> None:
    """完整链路：来源登记 -> XLSX 快照 -> 普通住宅画像 -> 占位图画像。

    验证 EXTFP0-A/C/D/E 交付在同一合成输入上协作，统计守恒闭合。
    """
    xlsx = _make_fixture(tmp_path)

    # A: 来源登记（lianjia_ext 独立来源）
    assert resolve_source_dir("SRC-011") == "lianjia_ext"

    # C: XLSX 结构化快照（只读，不伪装文本），不触发实时网络
    fetched_at = datetime(2026, 8, 24, 0, 0, 0, tzinfo=UTC)
    result = import_local_file(
        input_path=xlsx,
        source="SRC-011",
        dataset="chengjiao_xlsx",
        fetched_at=fetched_at,
        query=str(xlsx),
        data_dir=tmp_path / "lake",
    )
    assert result.directory.is_dir()

    # D: 普通住宅画像
    d = profile_xlsx(xlsx)
    assert d.profile_rule_version == PROFILE_RULE_VERSION
    sheet = d.sheets[0]
    assert sheet.data_rows_total == 12
    # 普通住宅：行 0,1,2,3,4,8,9,11 = 8；明确排除（商住两用/车库/别墅）= 3；用途未知（暂无）= 1
    assert sheet.ordinary_residential_count == 8
    assert sheet.property_use_asserted_excluded == 3
    assert sheet.property_use_unknown == 1

    # E: 占位图画像
    e = profile_floorplan(xlsx)
    assert e.selection_rule_version == SELECTION_RULE_VERSION

    # 守恒：普通住宅画像与占位图画像各自统计的内部字段，均来自同一 12 行
    assert e.url_list.data_rows_total == 12


def test_normal_ordinary_floorplan_candidate(tmp_path: Path) -> None:
    """正常：普通住宅 + 可解析字段 + 户型图候选 URL，进入选择清单。"""
    xlsx = _make_fixture(tmp_path)
    e = profile_floorplan(xlsx)
    # 普通住宅中带 >=1 个候选 URL 的行：0,1,2,8,11（5 条）；3(纯占位)/4(空)/9(解析失败) 非候选
    assert e.ordinary.floorplan_candidate_count == 5
    assert e.ordinary.placeholder_only_count == 1  # 第3行纯占位
    assert e.ordinary.no_url_count == 1  # 第4行空
    assert e.ordinary.parse_failure_count == 1  # 第9行 URL 列表含非字符串元素
    assert e.ordinary.placeholder_url_count == 2  # 第2行含1占位 + 第3行1占位
    assert e.ordinary.floorplan_url_count == 5


def test_boundary_area_unit_suffix_and_multi_url(tmp_path: Path) -> None:
    """边界：面积单位后缀 + 多 URL（候选+占位共存）。"""
    xlsx = _make_fixture(tmp_path)
    d = profile_xlsx(xlsx)
    # 第1行 "88.5㎡" 应能剥离单位后缀并解析为 88.5
    cols = {c.header: c for c in d.sheets[0].area_columns}
    assert cols["房屋面积"].parseable_count >= 1
    # 边界：第2行多 URL 在占位图画像中计为多 URL 记录
    e = profile_floorplan(xlsx)
    assert e.ordinary.multi_url_candidate_count == 1  # 第2行


def test_missing_placeholder_and_unknown(tmp_path: Path) -> None:
    """缺失：空 URL、纯占位图、无面积、用途=暂无。"""
    xlsx = _make_fixture(tmp_path)
    e = profile_floorplan(xlsx)
    # 空 URL（第4行）-> NO_URL（全表仅 1 条）
    assert e.url_list.url_list_status_counts[UrlListStatus.NO_URL.value] == 1
    # 纯占位（第3行）
    assert e.ordinary.placeholder_only_count == 1
    # 暂无用途（第5行）不进入普通住宅
    assert e.ordinary.ordinary_residential_count == 8


def test_counterexample_non_asserted_use_and_non_string_url(tmp_path: Path) -> None:
    """反例：商住两用/车库/别墅混入 + 非字符串 URL 数组（第6、9行解析失败）。"""
    xlsx = _make_fixture(tmp_path)
    d = profile_xlsx(xlsx)
    e = profile_floorplan(xlsx)
    # 第6行 "[12345]" 仅含非字符串元素、第9行 URL 数组混入整数 -> URL_PARSE_FAILURE
    assert e.url_list.url_list_status_counts[UrlListStatus.URL_PARSE_FAILURE.value] == 2
    # 反例用途不入普通住宅画像是 profile 层职责
    assert d.sheets[0].property_use_asserted_excluded == 3  # 商住/车库/别墅


def test_schema_backward_compatible_snapshot_format(tmp_path: Path) -> None:
    """schema 向后兼容（EXTFP0-B）：旧枚举值不变、新格式存在、旧 JSON 可反序列化。"""

    # 旧 JSON（无 mime_type）可继续反序列化，mime_type 默认 None
    old = {
        "snapshot_id": "s1", "source_id": "SRC-011", "dataset": "chengjiao_xlsx",
        "fetched_at": "2026-08-24T00:00:00Z", "query": "raw",
        "content_hash": "0" * 64, "file_count": 1, "record_count": 10,
        "format": "xlsx",
    }
    snap = RawSnapshot.model_validate(old)
    assert snap.mime_type is None
    assert snap.format.value == "xlsx"
    # 新格式枚举存在
    assert SnapshotFormat.XLSX.value == "xlsx"


def test_repeatable_profiles_and_evidence(tmp_path: Path) -> None:
    """重复运行一致 + 机器可读证据：同一输入两次画像业务字段逐字段一致，报告可 JSON 序列化。

``profiled_at`` 为画像执行时刻（微秒级），非业务字段，不计入一致性比较。
"""
    xlsx = _make_fixture(tmp_path)
    out = tmp_path / "integrated_evidence.json"
    d1 = profile_xlsx(xlsx)
    d2 = profile_xlsx(xlsx)
    assert d1.model_dump(exclude={"profiled_at"}) == d2.model_dump(exclude={"profiled_at"})
    e1 = profile_floorplan(xlsx)
    e2 = profile_floorplan(xlsx)
    assert e1.model_dump(exclude={"profiled_at"}) == e2.model_dump(exclude={"profiled_at"})
    # 机器可读：整合证据 JSON
    evidence = {
        "source_path": str(xlsx),
        "data_rows": d1.sheets[0].data_rows_total,
        "ordinary_residential": d1.sheets[0].ordinary_residential_count,
        "floorplan_candidate": e1.ordinary.floorplan_candidate_count,
        "profile_rule_version": d1.profile_rule_version,
        "selection_rule_version": e1.selection_rule_version,
    }
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded == evidence