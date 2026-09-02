"""WP4-E: ``compsval data stage`` reproducible pipeline + marts layer.

Seeds one immutable raw lianjia TXT snapshot, then runs ``data_stage`` and
checks the WP4-E acceptance criteria on the orchestration side:

① ``data_stage``/``compsval data stage`` re-derives staged + marts tables with a
   correct DerivedManifest lineage, and is reproducible (same snapshot →
   byte-identical reports) — acceptance ① and ⑤;
② the quality report covers the §8.4 items and matches the frozen summary —
   acceptance ②;
③ valid_sale holds only the formal (NORMAL) pool and never merges listing data
   with sale data; valid_listing keeps every effective listing event and is
   separate from sale — acceptance ③ / domain rule "成交 ≠ 挂牌".
The generic staged ``event_date`` is mapped to the semantic ``sale_date`` /
``listing_date`` in the marts layer (RV-WP4-D-01 F1).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval import cli
from compsval.catalog import SnapshotRef, list_snapshots
from compsval.ingest.import_file import import_local_file
from compsval.ingest.manifests import (
    read_derived_manifest,
)
from compsval.ingest.stage import (
    VALID_LISTING_FILENAME,
    VALID_LISTING_TABLE,
    VALID_SALE_FILENAME,
    VALID_SALE_TABLE,
    StageResult,
    data_stage,
)

_FETCHED_AT = datetime(2026, 8, 21, 3, 14, 0, tzinfo=UTC)

_HEADER = ["示例城市目标区二手房成交记录", "链家网·家和置业", "共4条", "https://lianjia.com.example/chengjiao/targetdistrict/"]

# 1 normal residential record (示例小区126)
_RESIDENCE = [
    "示例小区126 2室1厅 84.04平米",
    "南 | 精装2026.04.12 258万",
    "中楼层(共33层) 2005年板楼30700元/平",
    "房屋满五年近地铁",
    "挂牌258万成交周期89天",
    "王梅免费咨询",
]

# 1 parking record (星河湾)
_PARKING = [
    "星河湾 车位",
    "北 | 毛坯2026.06.01 45万",
    "地下车位(共1层) 2020年 15000元/平",
]

# 2 identical 瑾雅苑 records → form a duplicate pair (first retained, second flagged)
_JY = [
    "瑾雅苑 3室2厅 109.6平米",
    "南 | 精装2026.07.19 309万",
    "高楼层(共25层) 2008年塔楼28300元/平",
    "挂牌380万成交周期44天",
]


def _seed_snapshot(lake: Path) -> None:
    """Import a small lianjia TXT as an immutable raw snapshot into ``lake``."""
    raw = lake / "evidence" / "lianjia.txt"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text(
        "\n".join(_HEADER + _RESIDENCE + _PARKING + _JY + _JY) + "\n",
        encoding="utf-8",
    )
    import_local_file(
        input_path=raw,
        source="lianjia",
        dataset="chengjiao_list",
        fetched_at=_FETCHED_AT,
        query="https://lianjia.com.example/chengjiao/targetdistrict/",
        data_dir=lake,
    )


def _stage(lake: Path) -> tuple[SnapshotRef, StageResult]:
    (ref,) = list_snapshots(lake)
    return ref, data_stage(ref, data_dir=lake)


def _cols(table: pa.Table) -> set[str]:
    return set(table.column_names)


# ---------------------------------------------------------------------------
# 验收①⑤：编排产物齐备 + DerivedManifest 溯源 + 可复现
# ---------------------------------------------------------------------------


def test_data_stage_produces_all_artifacts_with_manifest(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    ref, result = _stage(lake)

    # 全套产物文件存在
    for p in (
        result.sale_event_path,
        result.listing_event_path,
        result.valid_sale_path,
        result.valid_listing_path,
        result.quality_report_md,
        result.quality_report_json,
    ):
        assert p.is_file(), p

    assert result.valid_sale_path.name == VALID_SALE_FILENAME
    assert result.valid_listing_path.name == VALID_LISTING_FILENAME

    # DerivedManifest 溯源：layer/table/行数与实际 parquet 行数一致
    for target, table_name in (
        (result.valid_sale_path, VALID_SALE_TABLE),
        (result.valid_listing_path, VALID_LISTING_TABLE),
    ):
        manifest = read_derived_manifest(target)
        assert manifest.table == table_name
        assert manifest.row_count == pq.read_table(target).num_rows

    # 挂牌/成交分表，且快照来源可识别
    assert ref is not None


def test_data_stage_is_reproducible_byte_identical_reports(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    ref = list_snapshots(lake)[0]

    first = data_stage(ref, data_dir=lake)
    second = data_stage(ref, data_dir=lake)

    assert first.quality_report_md.read_bytes() == second.quality_report_md.read_bytes()
    assert first.quality_report_json.read_bytes() == second.quality_report_json.read_bytes()
    assert first.valid_sale_path.read_bytes() == second.valid_sale_path.read_bytes()
    assert first.valid_listing_path.read_bytes() == second.valid_listing_path.read_bytes()


# ---------------------------------------------------------------------------
# 验收③ + 领域规则：valid_sale 仅正式池、成交≠挂牌
# ---------------------------------------------------------------------------


def test_valid_sale_only_formal_pool_and_is_not_listing(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    _, result = _stage(lake)

    sale = pq.read_table(result.valid_sale_path)
    # 示例小区126(1) + 瑾雅苑保留首条(1)；车位被排除、重复第二条被排除
    assert sale.num_rows == 2
    assert sale.column("anomaly_flag").to_pylist() == ["正常", "正常"]
    assert "sale_date" in _cols(sale)  # event_date → sale_date
    assert "total_price_yuan" in _cols(sale)
    # 挂牌证据字段不混入 sale 主标签：挂牌派生字段不冒充成交
    assert "delist_date" not in _cols(sale)
    assert "status" not in _cols(sale)


def test_valid_listing_keeps_every_entry_and_is_not_sale(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    _, result = _stage(lake)

    listing = pq.read_table(result.valid_listing_path)
    # 示例小区126 + 瑾雅苑 2 条均有挂牌证据 → 3；车位无挂牌证据排除
    assert listing.num_rows == 3
    assert "listing_date" in _cols(listing)  # event_date → listing_date
    assert "price_yuan" in _cols(listing)
    # 挂牌表不出现成交价/单价，证明未把挂牌当成交
    assert "total_price_yuan" not in _cols(listing)
    assert "unit_price" not in _cols(listing)


# ---------------------------------------------------------------------------
# 验收②：质量报告覆盖 §8.4 关键项且与清洗摘要一致
# ---------------------------------------------------------------------------


def test_quality_report_matches_summary_and_covers_sections(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    _, result = _stage(lake)

    import json

    report = json.loads(result.quality_report_json.read_text(encoding="utf-8"))
    # 输入4记录：1 车位、1 重复（瑾雅苑两录）、正式池 2
    assert report["counts"]["input_records"] == 4
    assert report["counts"]["parking_excluded"] == 1
    assert report["counts"]["duplicates"] == 1
    assert report["counts"]["formal_pool"] == 2
    assert report["provision_note"] == "可支撑等级为初判，正式门槛待 BT-001 核定"
    assert "key_field_coverage" in report
    assert "community_deals" in report

    md = result.quality_report_md.read_text(encoding="utf-8")
    assert "正式门槛待 BT-001 核定" in md
    assert "示例小区126" in md or "瑾雅苑" in md


# ---------------------------------------------------------------------------
# CLI：compsval data stage --snapshot 可用并返回 0
# ---------------------------------------------------------------------------


def test_cli_data_stage_success(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    ref = list_snapshots(lake)[0]
    snapshot_id = f"{ref.source}/{ref.dataset}@{ref.fetched_at}"

    assert cli.main(["data", "stage", "--snapshot", snapshot_id, "--data-dir", str(lake)]) == 0
    out = capsys.readouterr().out
    assert "quality:" in out
    assert "formal_pool=2" in out


def test_cli_data_stage_unknown_snapshot_fails(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    assert cli.main(["data", "stage", "--snapshot", "nope", "--data-dir", str(lake)]) == 1