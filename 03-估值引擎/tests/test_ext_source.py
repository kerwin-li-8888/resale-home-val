"""ext 第四合并源测试（ext-sale-ingest-scope-v1-2）。

覆盖：P0 注册表 5 键溯源与相似名隔离、staged 普通住宅表 → LianjiaRecord
适配（缺失语义/精度/PARSED 门槛）、可解析分流守恒、指针读取失败显式报错、
build_combined_marts 端到端接入（可解析入池/未解析不入池/跨源去重保留序/
属性扩列回接/重跑逐字节一致）。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from compsval.entities.backfill import (
    CommunityIdLookup,
    load_community_lookup,
)
from compsval.ingest.ext_source import (
    EXT_SOURCE_ID,
    ext_rows_to_records,
    normalize_layout,
    read_current_ext_run,
    run_fetched_at,
    split_resolvable,
)
from compsval.ingest.marts_build import (
    LIANJIA_COMMUNITY_REGISTRY,
    build_combined_marts,
)
from compsval.ingest.stage import valid_sale_table

# ---------------------------------------------------------------------------
# P0 注册表（任务 1.1）
# ---------------------------------------------------------------------------


def test_registry_p0_five_mappings_match_evidence() -> None:
    """5 条已确认映射逐条入表，目标 ID 与证据包 §0 一致。"""
    expected = {
        "示例小区144": "C-XXXX0116",
        "示例小区202": "C-XXXX0052",
        "示例小区041": "C-XXXX0053",
        "示例小区219": "C-XXXX0105",
        "示例小区031": "C-XXXX0009",
    }
    for name, cid in expected.items():
        assert LIANJIA_COMMUNITY_REGISTRY.get(name) == cid


def test_registry_similar_names_not_registered() -> None:
    """证据包未确认的相似命名不进入注册表（不误吸）。"""
    for name in (
        "示例小区145",
        "示例小区217",
        "示例小区050",
        "江南花苑",
        "江南新苑",
        "红棉苑南区",
        "红棉苑北区",
        "广信红棉阁",
        "示例小区009",
    ):
        assert name not in LIANJIA_COMMUNITY_REGISTRY


def test_registry_lookup_resolves_p0_names() -> None:
    """注册表经 lianjia_extended_lookup 对空查找表补条目（可解析）。"""
    from compsval.ingest.marts_build import lianjia_extended_lookup

    empty = CommunityIdLookup(canonical={}, alias_consistent={}, blocked={})
    lookup = lianjia_extended_lookup(empty)
    assert split_resolvable.__doc__ is not None  # 语义锚
    kept, unmatched = split_resolvable(
        ext_rows_to_records(_ext_table([("示例小区144", "2026-03-15")])), lookup
    )
    assert len(kept) == 1 and not unmatched


# ---------------------------------------------------------------------------
# 适配器（任务 1.2）
# ---------------------------------------------------------------------------


def _ext_table(
    rows: list[tuple[str, str]],
    *,
    unit_status: str = "PARSED",
    total_status: str = "PARSED",
    area_status: str = "PARSED",
    precision: str = "DAY",
    attributes: bool = True,
    with_listing: bool = True,
    areas: list[str] | None = None,
    totals: list[str] | None = None,
    units: list[str] | None = None,
) -> pa.Table:
    """合成 staged 普通住宅表（行 = (community_name, sale_date)）。

    ``areas/totals/units`` 可逐行覆盖（Decimal 字符串），用于构造跨源同身份行。
    """
    n = len(rows)
    areas = areas or ["88.50"] * n
    totals = totals or ["3000000.00"] * n
    units = units or ["33898.00"] * n
    columns: dict[str, pa.Array] = {
        "row_number": pa.array(list(range(1, n + 1)), type=pa.int64()),
        "source_record_id": pa.array([f"10{9000 + i}" for i in range(n)], type=pa.string()),
        "community_name": pa.array([r[0] for r in rows], type=pa.string()),
        "sale_date": pa.array([r[1] for r in rows], type=pa.string()),
        "sale_date_precision": pa.array([precision] * n, type=pa.string()),
        "total_price_yuan": pa.array([Decimal(v) for v in totals], type=pa.decimal128(18, 2)),
        "total_price_status": pa.array([total_status] * n, type=pa.string()),
        "unit_price_observed": pa.array([Decimal(v) for v in units], type=pa.decimal128(18, 2)),
        "unit_price_status": pa.array([unit_status] * n, type=pa.string()),
        "transaction_area_sqm": pa.array([Decimal(v) for v in areas], type=pa.decimal128(18, 2)),
        "area_status": pa.array([area_status] * n, type=pa.string()),
        "layout_raw": pa.array(["2室1厅1卫"] * n, type=pa.string()),
        "floor_raw": pa.array(["高楼层"] * n, type=pa.string()),
        "orientation": pa.array(["南"] * n, type=pa.string()),
        "decoration": pa.array(["精装"] * n, type=pa.string()),
        "listing_price_yuan": pa.array(
            [Decimal("3200000.00")] * n if with_listing else [None] * n,
            type=pa.decimal128(18, 2),
        ),
        "listing_days": pa.array([30] * n if with_listing else [None] * n, type=pa.int64()),
    }
    if attributes:
        columns.update(
            {
                "total_floors": pa.array([30] * n, type=pa.int64()),
                "year_built": pa.array([2005] * n, type=pa.int64()),
                "has_elevator": pa.array([True] * n, type=pa.bool_()),
                "decoration_norm": pa.array(["精装"] * n, type=pa.string()),
            }
        )
    return pa.table(columns)


def test_ext_rows_to_records_full_mapping() -> None:
    """字段完整行的映射：日期精度/价格/派生单价/挂牌/溯源列。"""
    records = ext_rows_to_records(_ext_table([("示例小区144", "2026-03-15")]))
    assert len(records) == 1
    rec = records[0]
    assert rec.community == "示例小区144"
    assert rec.layout == "2室1厅"  # 数据字典口径（去卫数；原文 2室1厅1卫 留 staged）
    assert rec.deal_date == date(2026, 3, 15)
    assert rec.deal_date_precision.value == "DAY"
    assert rec.area_sqm == Decimal("88.50")
    assert rec.total_price_yuan == 3000000
    assert rec.unit_price_observed == 33898
    assert rec.unit_price_derived == 33898  # 3000000 / 88.5 取整
    assert "total_price_yuan / area_sqm" in rec.unit_price_formula
    assert rec.orientation == "南"
    assert rec.decoration == "精装"
    assert rec.floor == "高楼层"
    assert rec.listing_price_yuan == 3200000
    assert rec.listing_period_days == 30
    assert rec.raw_start_line == 1
    assert rec.source_record_id == "109000"


def test_normalize_layout_matches_dictionary_convention() -> None:
    """户型归一：带卫数 → N室N厅（字典口径）；不匹配写法原样携带。"""
    assert normalize_layout("2室1厅1卫") == "2室1厅"
    assert normalize_layout("3室2厅2卫") == "3室2厅"
    assert normalize_layout("2室1厅0卫") == "2室1厅"
    assert normalize_layout("2室1厅") == "2室1厅"  # 已是字典口径
    assert normalize_layout("") == "UNKNOWN"
    assert normalize_layout("车位") == "车位"  # 非标准写法原样携带（清洗链处理）


def test_ext_rows_missing_semantics_preserved() -> None:
    """缺失语义：空/非法 → None/UNKNOWN，不写 0；精度 MONTH 如实保留。"""
    table = _ext_table(
        [("示例小区144", "")],
        unit_status="PARSE_FAILURE",
        total_status="PARSE_FAILURE",
        area_status="PARSE_FAILURE",
        precision="MONTH",
        with_listing=False,
    )
    null_columns = {"layout_raw", "floor_raw", "orientation", "decoration"}
    table = pa.table(
        {
            name: (
                pa.array([None], type=table.schema.field(name).type)
                if name in null_columns
                else table.column(name)
            )
            for name in table.column_names
        },
        schema=table.schema,
    )
    rec = ext_rows_to_records(table)[0]
    assert rec.layout == "UNKNOWN"
    assert rec.floor == "UNKNOWN"
    assert rec.orientation == "UNKNOWN"
    assert rec.decoration == "UNKNOWN"
    assert rec.deal_date is None
    assert rec.deal_date_precision.value == "UNKNOWN"
    assert rec.total_price_yuan is None
    assert rec.unit_price_observed is None
    assert rec.unit_price_derived is None
    assert rec.area_state is not None and rec.area_state.value == "PARSE_FAILURE"
    assert rec.listing_price_yuan is None


def test_ext_rows_month_precision_carried() -> None:
    rec = ext_rows_to_records(_ext_table([("示例小区144", "2026-03-01")], precision="MONTH"))[0]
    assert rec.deal_date == date(2026, 3, 1)
    assert rec.deal_date_precision.value == "MONTH"


def test_ext_rows_unit_price_requires_parsed_status() -> None:
    """PARSED 门槛：状态非 PARSED 的披露单价不携带（不臆测）。"""
    rec = ext_rows_to_records(_ext_table([("示例小区144", "2026-03-15")], unit_status="MISSING"))[0]
    assert rec.unit_price_observed is None
    assert rec.unit_price_derived is not None  # 派生价仍按公式可得


def test_split_resolvable_conservation_and_blocked() -> None:
    """守恒：输入 = 可解析 + 未解析；blocked 别名（泰沙路）不入可解析。"""
    lookup = CommunityIdLookup(
        canonical={"示例小区144": ("C-XXXX0116", "标准名命中")},
        alias_consistent={},
        blocked={"泰沙路": "待定"},
    )
    records = ext_rows_to_records(
        _ext_table(
            [("示例小区144", "2026-03-15"), ("泰沙路", "2026-03-16"), ("社区X", "2026-03-17")]
        )
    )
    kept, unmatched = split_resolvable(records, lookup)
    assert len(kept) == 1
    assert kept[0].community == "示例小区144"
    assert unmatched == Counter({"泰沙路": 1, "社区X": 1})
    assert len(records) == len(kept) + sum(unmatched.values())  # 守恒


def test_read_current_ext_run_pointer_missing_returns_none(tmp_path: Path) -> None:
    assert read_current_ext_run(tmp_path) is None


def test_read_current_ext_run_missing_file_fails_loud(tmp_path: Path) -> None:
    staged = tmp_path / "staged" / "lianjia_ext"
    staged.mkdir(parents=True)
    (staged / "current.json").write_text(
        json.dumps({"run_id": "R1", "ordinary_residential": "runs/run_R1/t.parquet"}),
        encoding="utf-8",
    )
    with pytest.raises(FileNotFoundError):
        read_current_ext_run(tmp_path)


def test_read_current_ext_run_reads_pointer_run(tmp_path: Path) -> None:
    run_dir = tmp_path / "staged" / "lianjia_ext" / "runs" / "run_R1"
    run_dir.mkdir(parents=True)
    pq_path = run_dir / "lianjia_ext_ordinary_residential.parquet"
    import pyarrow.parquet as pq

    pq.write_table(_ext_table([("示例小区144", "2026-03-15")]), pq_path)
    (tmp_path / "staged" / "lianjia_ext" / "current.json").write_text(
        json.dumps(
            {
                "run_id": "R1",
                "ordinary_residential": (
                    "runs/run_R1/lianjia_ext_ordinary_residential.parquet"
                ),
            }
        ),
        encoding="utf-8",
    )
    run = read_current_ext_run(tmp_path)
    assert run is not None
    assert run.run_id == "R1"
    assert run.ordinary_sha256
    assert run.table.num_rows == 1


def test_run_fetched_at_parses_run_id() -> None:
    assert run_fetched_at("20260831T041648Z") == datetime(2026, 8, 31, 4, 16, 48, tzinfo=UTC)


# ---------------------------------------------------------------------------
# 端到端：build_combined_marts 接入 ext 第四来源
# ---------------------------------------------------------------------------

_LIANJIA_TS = datetime(2026, 8, 21, tzinfo=UTC)

_LIANJIA_TXT = [
    "示例城市目标区二手房成交记录",
    "链家网·家和置业",
    "共2条",
    "https://lianjia.com.example/chengjiao/targetdistrict/",
    "示例小区132 2室1厅 78.15平米",
    "南 | 精装2026.07.19 313万",
    "高楼层(共25层) 2005年板楼40051元/平",
    "挂牌330万成交周期60天",
    "王梅免费咨询",
    "星河湾 车位",
    "北 | 毛坯2026.06.01 45万",
    "地下车位(共1层) 2020年 15000元/平",
]


def _seed_entities(lake: Path) -> None:
    """最小实体表：canonical（示例小区132/示例小区144）+ blocked 别名（泰沙路）。"""
    entities = lake / "entities"
    entities.mkdir(parents=True)
    pa.table(
        {
            "community_id": pa.array(["C-XXXX0069", "C-XXXX0116"], type=pa.string()),
            "standard_name": pa.array(["示例小区132", "示例小区144"], type=pa.string()),
        }
    )
    community = pa.table(
        {
            "community_id": pa.array(["C-XXXX0069", "C-XXXX0116"], type=pa.string()),
            "standard_name": pa.array(["示例小区132", "示例小区144"], type=pa.string()),
        }
    )
    import pyarrow.parquet as pq

    pq.write_table(community, entities / "community.parquet")
    alias = pa.table(
        {
            "alias_id": pa.array(["AC-72"], type=pa.string()),
            "community_id": pa.array(["C-XXXX0089"], type=pa.string()),
            "source_alias": pa.array(["泰沙路"], type=pa.string()),
            "source_id": pa.array(["SRC-007"], type=pa.string()),
            "source_ref": pa.array(["测试：道路级命名"], type=pa.string()),
            "conflict_status": pa.array(["待定"], type=pa.string()),
        }
    )
    pq.write_table(alias, entities / "community_alias.parquet")


def _seed_ext_staged(lake: Path, table: pa.Table, run_id: str = "20260831T041648Z") -> None:
    import pyarrow.parquet as pq

    run_dir = lake / "staged" / "lianjia_ext" / "runs" / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table, run_dir / "lianjia_ext_ordinary_residential.parquet", compression="zstd"
    )
    pointer = lake / "staged" / "lianjia_ext" / "current.json"
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "ordinary_residential": (
                    f"runs/run_{run_id}/lianjia_ext_ordinary_residential.parquet"
                ),
            }
        ),
        encoding="utf-8",
    )


def _seed_lianjia_raw(lake: Path) -> None:
    from compsval.ingest.import_file import import_local_file

    raw = lake / "evidence" / "lianjia.txt"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("\n".join(_LIANJIA_TXT) + "\n", encoding="utf-8")
    import_local_file(
        input_path=raw,
        source="lianjia",
        dataset="chengjiao_list",
        fetched_at=_LIANJIA_TS,
        query="https://lianjia.com.example/chengjiao/targetdistrict/",
        data_dir=lake,
    )


def test_build_combined_marts_with_ext_source(tmp_path: Path) -> None:
    """ext 接入：可解析入池、未解析/blocked 不入池、跨源去重保留序、扩列回接。"""
    lake = tmp_path / "lake"
    _seed_lianjia_raw(lake)
    _seed_entities(lake)
    # ext：示例小区144唯一成交（扩列命中）+ 示例小区132与链家同交易（78.15/313万/
    # 2026-07-19，无挂牌价 → 富长度低于链家行，链家保留）+ 泰沙路（blocked）
    # + 社区X（未匹配）
    ext = _ext_table(
        [
            ("示例小区144", "2026-03-15"),
            ("示例小区132", "2026-07-19"),
            ("泰沙路", "2026-03-16"),
            ("社区X", "2026-03-17"),
        ],
        with_listing=False,
        areas=["88.50", "78.15", "88.50", "88.50"],
        totals=["3000000.00", "3130000.00", "3000000.00", "3000000.00"],
        units=["33898.00", "40051.00", "33898.00", "33898.00"],
    )
    _seed_ext_staged(lake, ext)

    result = build_combined_marts(data_dir=lake)
    assert result.ext_run_id == "20260831T041648Z"
    assert result.ext_input_rows == 4
    assert result.ext_kept_rows == 2
    assert result.ext_unmatched_rows == 2
    assert result.ext_unmatched_names == 2
    # 守恒：输入 = 入池 + 未解析
    assert result.ext_input_rows == result.ext_kept_rows + result.ext_unmatched_rows

    import pyarrow.parquet as pq

    vs = pq.read_table(result.valid_sale_path)  # 落盘表含扩列回接
    assert "total_floors" in vs.column_names  # 扩列回接（不回退属性基线）
    df = vs.to_pydict()
    communities = df["community"]
    assert "泰沙路" not in communities and "社区X" not in communities  # 不入正式池
    assert communities.count("示例小区144") == 1
    src = dict(zip(df["sale_event_id"], df["source_id"], strict=True))
    # 跨源去重：示例小区132同身份 ext 行被标疑似重复（不进 valid_sale，链家行保留）
    assert result.cross_source_duplicates == 1
    ext_ids = [k for k, v in src.items() if v == EXT_SOURCE_ID]
    assert len(ext_ids) == 1  # 仅示例小区144 ext 行
    flags = dict(zip(df["sale_event_id"], df["anomaly_flag"], strict=True))
    kept_by_src = [src[k] for k, v in flags.items() if v == "正常"]
    assert kept_by_src.count(EXT_SOURCE_ID) == 1  # 仅示例小区144 ext 行入正式池
    assert kept_by_src.count("SRC-007") == 1  # 示例小区132链家行保留

    # 属性扩列：示例小区144 ext 行命中自身 staged 属性
    jn = [
        (src[k], flags[k])
        for k, c in zip(df["sale_event_id"], communities, strict=True)
        if c == "示例小区144"
    ]
    assert jn and jn[0][1] == "正常"
    total_floors = dict(zip(df["sale_event_id"], df["total_floors"], strict=True))
    refs = dict(zip(df["sale_event_id"], df["attribute_enrich_ref"], strict=True))
    jn_id = [k for k, c in zip(df["sale_event_id"], communities, strict=True) if c == "示例小区144"][0]
    assert total_floors[jn_id] == 30
    assert refs[jn_id] is not None and refs[jn_id].startswith(
        f"lianjia_ext@{result.ext_run_id}|"
    )
    assert result.attribute_matched_rows >= 2

    # 未解析 + blocked 源名进质量报告登记（不静默丢弃）
    quality_json = json.loads(result.quality_json.read_text(encoding="utf-8"))
    unmatched = quality_json["unmatched_conflicts"]
    assert "社区X" in unmatched and "泰沙路" in unmatched


def test_build_combined_marts_ext_reproducible(tmp_path: Path) -> None:
    """同输入重跑逐字节一致（含 ext 与扩列）。"""
    lake = tmp_path / "lake"
    _seed_lianjia_raw(lake)
    _seed_entities(lake)
    _seed_ext_staged(lake, _ext_table([("示例小区144", "2026-03-15")]))
    first = build_combined_marts(data_dir=lake)
    second = build_combined_marts(data_dir=lake)
    assert first.valid_sale_path.read_bytes() == second.valid_sale_path.read_bytes()


def test_build_combined_marts_without_pointer_unchanged(tmp_path: Path) -> None:
    """无 ext 指针（旧环境）：合并行为与既有三来源一致（无 ext 字段）。"""
    lake = tmp_path / "lake"
    _seed_lianjia_raw(lake)
    result = build_combined_marts(data_dir=lake)
    assert result.ext_run_id is None
    assert result.ext_input_rows == 0
    vs = valid_sale_table(result.sale_table)
    assert "total_floors" not in vs.column_names  # 无 ext 不做扩列（保持原状）


def test_split_resolvable_with_real_entities(tmp_path: Path) -> None:
    """真实实体表读取链路：canonical 命中 + blocked 别名分流。"""
    _seed_entities(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)
    records = ext_rows_to_records(
        _ext_table([("示例小区144", "2026-03-15"), ("泰沙路", "2026-03-16")])
    )
    kept, unmatched = split_resolvable(records, lookup)
    assert [r.community for r in kept] == ["示例小区144"]
    assert unmatched == Counter({"泰沙路": 1})
