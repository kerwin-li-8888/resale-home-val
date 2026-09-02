"""G3R-C 多源 marts 合并构建测试（跨源去重 + 唯一事件 ID + 端到端 + CLI）。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from compsval import cli
from compsval.ingest.import_file import import_local_file
from compsval.ingest.marts_build import (
    LIANJIA_COMMUNITY_REGISTRY,
    backfill_lianjia_layouts,
    build_combined_marts,
    cross_source_dedup,
    lianjia_extended_lookup,
    merge_snapshots,
    reconstruct_records,
)
from compsval.ingest.parsers.lianjia import LianjiaRecord, parse_lianjia_txt
from compsval.ingest.snapshots import write_raw_snapshot
from compsval.ingest.stage import valid_sale_table

_LIANJIA_TS = datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC)
_LJ_CHENGJIAO_TS = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
_FANG_TS = datetime(2026, 8, 22, 15, 24, 43, tzinfo=UTC)
_FANG_TS_2 = datetime(2026, 8, 22, 15, 30, 0, tzinfo=UTC)

# 链家 TXT：1 条示例小区132住宅（字段丰富）+ 1 条车位（排除）
_LIANJIA_TXT = [
    "示例城市目标区二手房成交记录",
    "链家网·家和置业",
    "共3条",
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


def _seed_lianjia(lake: Path) -> None:
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


def _fang_table(
    areas: list[float], dates: list[date], totals: list[int], units: list[int]
) -> pa.Table:
    return pa.table(
        {
            "area_m2": pa.array(areas, type=pa.float64()),
            "deal_date": pa.array(dates, type=pa.date32()),
            "total_price_wan": pa.array(totals, type=pa.int64()),
            "unit_price_yuan_m2": pa.array(units, type=pa.int64()),
            "source_note": pa.array(["市场信息"] * len(areas), type=pa.string()),
        }
    )


def _seed_fang(lake: Path) -> None:
    """房天下 示例小区132：与链家 78.15/313万 同一交易（跨源重复）+ 一条唯一成交。"""
    write_raw_snapshot(
        _fang_table(
            [78.15, 60.0],
            [date(2026, 7, 19), date(2026, 6, 1)],
            [313, 150],
            [40050, 25000],
        ),
        root=lake,
        source="fang_esf",
        dataset="chengjiao",
        fetched_at=_FANG_TS,
        query="https://esf.fang.com.example/loupan/2811052010/chengjiao/",
    )


#: 链家成交 CSV（LJ-D 快照形状）：示例小区132 78.15/313万（与房天下/链家列表同一
#: 交易，layout 已知）+ 示例小区132 80㎡/320万（链家唯一成交）。
_LJ_CHENGJIAO_TXT = [
    "示例小区132 2室1厅 78.15平米",
    "南 | 精装2026.07.19 313万",
    "高楼层(共25层) 2005年板楼40051元/平",
    "挂牌330万成交周期60天",
    "王梅免费咨询",
    "示例小区132 3室2厅 80.00平米",
    "南 | 精装2026.05.10 320万",
    "中楼层(共25层) 2005年板楼40000元/平",
    "挂牌340万成交周期30天",
    "李四免费咨询",
]


def _seed_lianjia_chengjiao(lake: Path) -> None:
    """链家成交 CSV 快照（LJ-D 形状：write_lianjia_csv → import_local_file）。"""
    from compsval.ingest.parsers.lianjia_html import write_lianjia_csv

    csv_path = lake / "evidence" / "lianjia_chengjiao.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_lianjia_csv(parse_lianjia_txt(_LJ_CHENGJIAO_TXT), out_path=csv_path)
    import_local_file(
        input_path=csv_path,
        source="lianjia",
        dataset="chengjiao",
        fetched_at=_LJ_CHENGJIAO_TS,
        query="LJ-C 结构化 CSV（小区 key=guangda）",
        data_dir=lake,
    )


# ---------------------------------------------------------------------------
# 单元：跨源去重
# ---------------------------------------------------------------------------


def _sale_rows_table(rows: list[dict[str, object]]) -> pa.Table:
    """用 sale_event_table 完整链构造表：先经解析→清洗→建表（真实字段面）。"""
    blocks: list[str] = []
    for r in rows:
        if r["layout"] == "车位":
            blocks.append(f"{r['community']} 车位")
        else:
            blocks.append(
                f"{r['community']} {r['layout']} {r['area']}平米"
            )
        blocks.append(
            f"{r['orientation']} | 精装{r['date']} {r['total_wan']}万"
        )
        if r.get("listing_wan"):
            blocks.append(f"挂牌{r['listing_wan']}万成交周期{r.get('days', 60)}天")
        blocks.append("测试免费咨询")
    records = parse_lianjia_txt(blocks)
    from compsval.ingest.clean import clean_sales, sale_event_table

    cleaned, _ = clean_sales(records)
    return sale_event_table(
        cleaned,
        source_id="SRC-007",
        snapshot_id="lianjia-test",
        fetched_at=_LIANJIA_TS,
    )


def test_cross_source_dedup_keeps_richer_flags_other() -> None:
    """同一交易身份：保留字段更丰富来源，其余标跨源重复。"""
    table = _sale_rows_table(
        [
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "南",
                "listing_wan": 330,
            },  # 字段丰富（有户型/朝向/挂牌）
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "北",
            },  # 同交易身份，字段更少
        ]
    )
    deduped, flagged = cross_source_dedup(table)
    assert flagged == 1
    flags = deduped.column("anomaly_flag").to_pylist()
    notes = deduped.column("flag_note").to_pylist()
    assert flags.count("正常") == 1
    assert flags.count("疑似重复") == 1
    dup_note = notes[flags.index("疑似重复")]
    assert "跨源/多录同一交易身份" in dup_note
    # 保留的是字段更丰富（含挂牌价）那条
    kept_community_ids = deduped.filter(
        pa.array([f == "正常" for f in flags])
    ).column("listing_price_yuan").to_pylist()
    assert kept_community_ids == [3300000]


def test_cross_source_dedup_ignores_non_normal_canonical() -> None:
    """非 NORMAL 记录不作为保留对象（本身不进正式池，不参与去重）。"""
    table = _sale_rows_table(
        [
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "南",
            },
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "北",
            },
        ]
    )
    # 第一条标记为异常单价（非 NORMAL），则第二条自动成为保留对象、不再被去重
    rows = table.to_pylist()
    rows[0]["anomaly_flag"] = "疑似异常单价"
    from pyarrow import RecordBatch

    altered = pa.Table.from_batches([RecordBatch.from_pylist(rows, schema=table.schema)])
    deduped, flagged = cross_source_dedup(altered)
    assert flagged == 0
    assert deduped.num_rows == 2


def test_cross_source_dedup_missing_identity_untouched() -> None:
    """身份字段缺失（无成交日/总价）→ 不做跨源去重（不臆测合并）。"""
    table = _sale_rows_table(
        [
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "南",
            },
            {
                "community": "示例小区132",
                "layout": "2室1厅",
                "area": 78.15,
                "date": "2026.07.19",
                "total_wan": 313,
                "orientation": "北",
            },
        ]
    )
    rows = table.to_pylist()
    rows[1]["total_price_yuan"] = None  # 第二条身份键缺失
    from pyarrow import RecordBatch

    altered = pa.Table.from_batches([RecordBatch.from_pylist(rows, schema=table.schema)])
    deduped, flagged = cross_source_dedup(altered)
    assert flagged == 0
    assert deduped.num_rows == 2


def test_unique_sale_event_ids_across_snapshots() -> None:
    """合并后 sale_event_id 全局唯一（跨快照相同行号撞号防护）。"""
    from compsval.ingest.marts_build import _unique_sale_event_ids

    table = pa.table(
        {
            "snapshot_id": ["fang_esf-a", "fang_esf-b"],
            "raw_locator": ["2", "2"],
            "sale_event_id": ["SRC-005-line2", "SRC-005-line2"],  # 撞号
        }
    )
    out = _unique_sale_event_ids(table)
    ids = out.column("sale_event_id").to_pylist()
    assert ids[0] != ids[1]
    assert ids[0] == "fang_esf-a-line2"
    assert ids[1] == "fang_esf-b-line2"


def test_backfill_lianjia_layouts_unknown_backfilled() -> None:
    """被跨源去重标记的房天下 UNKNOWN 行经链家同身份行回填 layout。"""
    table = pa.table(
        {
            "anomaly_flag": ["正常", "疑似重复"],
            "layout": ["2室1厅", "UNKNOWN"],
            "community_id": ["C-XXXX0069", "C-XXXX0069"],
            "community": ["示例小区132", "示例小区132"],
            "area_sqm": [78.15, 78.15],
            "event_date": [date(2026, 7, 19), date(2026, 7, 19)],
            "total_price_yuan": [3130000, 3130000],
            "flag_note": ["", "跨源/多录同一交易身份，去除本条"],
        }
    )
    out, n = backfill_lianjia_layouts(table)
    assert n == 1
    assert out.column("layout").to_pylist() == ["2室1厅", "2室1厅"]
    assert "跨源回填" in out.column("flag_note").to_pylist()[1]


def test_backfill_lianjia_layouts_no_match_untouched() -> None:
    """身份键不匹配或无保留行时 UNKNOWN 不回填（不臆测）。"""
    table = pa.table(
        {
            "anomaly_flag": ["疑似重复"],
            "layout": ["UNKNOWN"],
            "community_id": ["C-XXXX0069"],
            "community": ["示例小区132"],
            "area_sqm": [999.0],
            "event_date": [date(2026, 7, 19)],
            "total_price_yuan": [3130000],
            "flag_note": [""],
        }
    )
    out, n = backfill_lianjia_layouts(table)
    assert n == 0
    assert out.column("layout").to_pylist() == ["UNKNOWN"]


def test_lianjia_extended_lookup_registry() -> None:
    """链家成交社区注册表并入回填查找表；未注册小区仍 UNMATCHED。"""
    from compsval.entities.backfill import (
        CommunityIdLookup,
        resolve_community_id,
    )

    empty = CommunityIdLookup(canonical={}, alias_consistent={}, blocked={})
    lookup = lianjia_extended_lookup(empty)
    cid, _outcome, reason = resolve_community_id("示例小区166", lookup)
    assert cid == "C-XXXX0122"
    assert "链家成交社区注册表" in reason
    # 名录外/待核小区（楹隆天悦）不入注册表 → 未匹配
    assert resolve_community_id("楹隆天悦", lookup)[0] is None
    assert "示例小区136拾光里" in LIANJIA_COMMUNITY_REGISTRY
    assert "楹隆花园" not in LIANJIA_COMMUNITY_REGISTRY  # 探测报告待核


# ---------------------------------------------------------------------------
# 端到端：build_combined_marts（临时湖）
# ---------------------------------------------------------------------------


def test_build_combined_marts_end_to_end(tmp_path: Path) -> None:
    """链家 + 房天下合并：跨源重复去除、车位排除、正式池、唯一事件 ID、质量报告。"""
    lake = tmp_path / "lake"
    _seed_lianjia(lake)
    _seed_fang(lake)

    result = build_combined_marts(data_dir=lake)
    assert len(result.snapshot_ids) == 2

    # 跨源去重：78.15/313万 的房天下记录被标记，链家（字段更丰富）保留
    assert result.cross_source_duplicates == 1
    assert result.summary.duplicate_flagged == 1
    assert result.summary.parking_flagged == 1  # 星河湾车位
    # 正式池 = 链家清晏 + 房天下 60.00 + 房天下 78.15 去重后保留 1 条 = 3
    # （链家清晏 1 + 房天下 60.00 1 = 2 正常；78.15 跨源重复被标记）
    assert result.summary.formal_pool == 2

    vs = valid_sale_table(result.sale_table)
    vs_ids = vs.column("sale_event_id").to_pylist()
    assert len(vs_ids) == len(set(vs_ids))  # 全局唯一
    communities = vs.column("community").to_pylist()
    assert set(communities) == {"示例小区132"}

    # 产物文件 + 溯源
    assert result.valid_sale_path.is_file()
    assert result.valid_listing_path.is_file()
    assert result.quality_md.is_file()
    assert result.quality_json.is_file()
    manifest = result.valid_sale_path.with_suffix(".manifest.json")
    assert manifest.is_file()

    # 可复现：同输入重跑产出同内容
    again = build_combined_marts(data_dir=lake)
    assert again.summary.formal_pool == result.summary.formal_pool
    assert again.cross_source_duplicates == result.cross_source_duplicates
    assert (
        (result.valid_sale_path).read_bytes() == (again.valid_sale_path).read_bytes()
    )


def test_merge_snapshots_and_reconstruct_fang_community(tmp_path: Path) -> None:
    """merge_snapshots 只取参与来源；reconstruct_records 从 query 解析小区。"""
    lake = tmp_path / "lake"
    _seed_lianjia(lake)
    _seed_fang(lake)
    refs = merge_snapshots(lake)
    assert [(r.source, r.dataset) for r in refs] == [
        ("fang_esf", "chengjiao"),
        ("lianjia", "chengjiao_list"),
    ]
    fang_ref = [r for r in refs if r.source == "fang_esf"][0]
    records = reconstruct_records(fang_ref)
    assert records
    assert records[0].community == "示例小区132"
    assert len(records) == 2
    lianjia_ref = [r for r in refs if r.source == "lianjia"][0]
    lianjia_records = reconstruct_records(lianjia_ref)
    assert all(isinstance(r, LianjiaRecord) for r in lianjia_records)


def test_reconstruct_lianjia_chengjiao_csv_snapshot(tmp_path: Path) -> None:
    """LJ-D 链家成交 CSV 快照按列回读为规范化记录（LJ-E 分派解析）。"""
    lake = tmp_path / "lake"
    _seed_lianjia_chengjiao(lake)
    refs = merge_snapshots(lake)
    ref = [r for r in refs if (r.source, r.dataset) == ("lianjia", "chengjiao")][0]
    records = reconstruct_records(ref)
    assert all(isinstance(r, LianjiaRecord) for r in records)
    assert len(records) == 2
    assert records[0].community == "示例小区132"
    assert records[0].layout == "2室1厅"
    assert records[0].total_price_yuan == 3130000
    assert records[0].listing_price_yuan == 3300000


def test_build_combined_marts_three_sources(tmp_path: Path) -> None:
    """链家成交 + 链家列表 + 房天下 三来源：跨源去重、户型回填、正式池。"""
    lake = tmp_path / "lake"
    _seed_lianjia(lake)
    _seed_lianjia_chengjiao(lake)
    _seed_fang(lake)

    result = build_combined_marts(data_dir=lake)
    assert len(result.snapshot_ids) == 3

    # 78.15/313万 同一交易三录（链家列表 + 链家成交 + 房天下）→ 2 条被跨源标记
    assert result.cross_source_duplicates == 2
    # 被标记的房天下 78.15 行 layout=UNKNOWN → 经链家同身份回填
    assert result.layout_backfilled == 1
    # 正式池 = 链家列表 78.15 + 链家成交 80 + 房天下 60 = 3（车位 1 条排除）
    assert result.summary.formal_pool == 3

    vs = valid_sale_table(result.sale_table)
    vs_ids = vs.column("sale_event_id").to_pylist()
    assert len(vs_ids) == len(set(vs_ids))  # 全局唯一
    layouts = vs.column("layout").to_pylist()
    # 正式池 3 条：链家列表 78.15 + 链家成交 80 有户型；房天下 60（无链家同身份
    # 匹配）UNKNOWN 如实保留，不回填不臆测
    assert layouts.count("UNKNOWN") == 1
    assert layouts.count("2室1厅") == 1
    assert layouts.count("3室2厅") == 1
    # 可复现
    again = build_combined_marts(data_dir=lake)
    assert again.cross_source_duplicates == result.cross_source_duplicates
    assert again.layout_backfilled == result.layout_backfilled
    assert result.valid_sale_path.read_bytes() == again.valid_sale_path.read_bytes()


def test_build_combined_marts_no_sources_fails(tmp_path: Path) -> None:
    """无可合并快照 → 明确失败（不产出半成品）。"""
    with pytest.raises(ValueError, match="无可合并快照"):
        build_combined_marts(data_dir=tmp_path)


# ---------------------------------------------------------------------------
# CLI：compsval data marts-build
# ---------------------------------------------------------------------------


def test_cli_data_marts_build(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lake = tmp_path / "lake"
    _seed_lianjia(lake)
    _seed_fang(lake)
    assert cli.main(["data", "marts-build", "--data-dir", str(lake)]) == 0
    out = capsys.readouterr().out
    assert "snapshots=2" in out
    assert "cross_source_duplicates=1" in out
    assert "formal_pool=2" in out
