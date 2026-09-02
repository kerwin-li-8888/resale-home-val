"""WP5-D: market_series 市场序列登记（时间修正候选证据，本包不做修正计算）。

对照 WP5-D 验收标准：
① 每行可追溯到来源（source_id/source_key/source_ref 指向 58 名录转录）；
② source_strength 按 官方/平台/第三方 标注（58=平台，不冒充官方）；
③ revision_flag 标注是否会修订历史值（58 平台月度更新会修订 → True）；
④ 与成交/挂牌事件表严格分离（entities 层 vs staged 层、无事件主键列）；
⑤ ruff/mypy/pytest 通过。

数据来自候选小区名录 §1.1 的 58 板块均价（SRC-008，2026-08）：8 个有值板块
落表；"待补"板块无数值不落表；price_change 名录未转录 → None（不得用 0）。
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from compsval.contract.models import SourceStrength
from compsval.entities.community import ENTITIES_LAYER
from compsval.entities.market_series import (
    F58_MONTH,
    F58_SOURCE_ID,
    MARKET_FILENAME,
    MARKET_TABLE,
    MarketSeriesRow,
    build_market_series_entity,
    market_series_schema,
    market_series_table,
    series_id_of,
    write_market_series_entity,
)
from compsval.ingest.clean import SALE_EVENT_TABLE, STAGED_LAYER
from compsval.ingest.listing import LISTING_EVENT_TABLE

#: 名录 §1.1 真实 58 板块均价（2026-08）——8 个有值板块（板块名, 均价 元/㎡）。
_REAL_BLOCKS: tuple[tuple[str, int], ...] = (
    ("工业大道中", 27623),
    ("工业大道南", 34672),
    ("汐园", 44987),
    ("南洲", 30823),
    ("前进路", 21974),
    ("滨江西", 49308),
    ("滨江中", 50399),
    ("新港西", 7977),
)

#: 名录 §1.1 "待补"板块（无数值 → 不落表，不得虚构）。
_PENDING_BLOCKS: tuple[str, ...] = (
    "工业大道北",
    "江南西",
    "宝岗",
    "昌岗路",
    "江燕路",
    "东泊南",
)


def _real_rows() -> list[MarketSeriesRow]:
    rows: list[MarketSeriesRow] = []
    for seq, (region, price) in enumerate(_REAL_BLOCKS, start=1):
        rows.append(
            MarketSeriesRow(
                series_id=series_id_of(seq, F58_MONTH),
                region=region,
                month=F58_MONTH,
                price=Decimal(price),
                price_change=None,
                source_strength=SourceStrength.PLATFORM,
                revision_flag=True,
                source_id=F58_SOURCE_ID,
                source_key=f"{seq:03d}",
                source_ref=(
                    f"候选小区名录-V0.1.md §1.1 边界表 板块<{region}>行"
                    f"；58 快照 source=58/dataset=ban_kkuai_price/fetched_at=20260820"
                    f"（页面 https://gz.58.com/fangjia/1657/）"
                ),
            )
        )
    return rows


# ---- ① 每行可追溯到来源 ----
def test_every_row_traceable_to_source() -> None:
    rows = _real_rows()
    for row in rows:
        assert row.source_id == "SRC-008"
        assert row.source_key != ""
        assert "候选小区名录" in row.source_ref
        assert "ban_kkuai_price" in row.source_ref
        assert "gz.58.com" in row.source_ref


def test_source_key_is_block_seq() -> None:
    rows = _real_rows()
    assert [r.source_key for r in rows] == [f"{i:03d}" for i in range(1, len(rows) + 1)]


# ---- ② source_strength 按 官方/平台/第三方 标注 ----
def test_source_strength_enum_three_values() -> None:
    values = {v.value for v in SourceStrength}
    assert values == {"官方", "平台", "第三方"}


def test_58_is_platform_not_official() -> None:
    rows = _real_rows()
    assert all(r.source_strength is SourceStrength.PLATFORM for r in rows)
    # 官方聚合无真实数值样本不落表 → 无 OFFICIAL 行（不把平台当官方）
    assert not any(r.source_strength is SourceStrength.OFFICIAL for r in rows)
    assert not any(r.source_strength is SourceStrength.THIRD_PARTY for r in rows)


# ---- ③ revision_flag 标注是否会修订历史值 ----
def test_revision_flag_true_for_58() -> None:
    rows = _real_rows()
    assert all(r.revision_flag is True for r in rows)


def test_unknown_price_change_is_none_not_zero() -> None:
    rows = _real_rows()
    assert all(r.price_change is None for r in rows)
    for r in rows:
        assert r.price is not None and r.price > 0


# ---- ④ 与成交/挂牌事件表严格分离 ----
def test_market_series_is_entities_layer_not_staged() -> None:
    assert ENTITIES_LAYER == "entities"
    assert ENTITIES_LAYER != STAGED_LAYER
    assert MARKET_TABLE != SALE_EVENT_TABLE
    assert MARKET_TABLE != LISTING_EVENT_TABLE


def test_market_series_has_no_event_primary_keys() -> None:
    names = set(market_series_schema().names)
    event_keys = {"sale_event_id", "listing_event_id", "community_id", "unit_price", "sale_date"}
    assert not (names & event_keys)


def test_build_writes_only_entities_not_staged(
    tmp_path: Path,
) -> None:
    build_market_series_entity(data_dir=tmp_path)
    staged_dir = tmp_path / STAGED_LAYER
    assert not staged_dir.exists() or not any(staged_dir.iterdir())
    entities_dir = tmp_path / ENTITIES_LAYER
    assert (entities_dir / MARKET_FILENAME).is_file()


# ---- 模型/模式 ----
def test_series_id_format() -> None:
    assert series_id_of(1, date(2026, 8, 1)) == "MS-001-202608"
    assert series_id_of(8, date(2026, 8, 1)) == "MS-008-202608"


def test_schema_covers_model_fields_and_traceability() -> None:
    names = set(market_series_schema().names)
    required = {
        "series_id",
        "region",
        "month",
        "price",
        "price_change",
        "source_strength",
        "revision_flag",
        # 溯源扩展（验收①）
        "source_id",
        "source_key",
        "source_ref",
    }
    assert required <= names
    # price/price_change 可为空（未知）；revision_flag 可为空（未知）
    field = {f.name: f for f in market_series_schema()}
    assert field["price"].nullable
    assert field["price_change"].nullable
    assert field["revision_flag"].nullable


# ---- 真实数据转录 ----
def test_eight_blocks_with_real_prices() -> None:
    rows = _real_rows()
    assert len(rows) == 8
    actual: list[tuple[str, int]] = []
    for r in rows:
        assert r.price is not None
        actual.append((r.region, int(r.price)))
    assert actual == list(_REAL_BLOCKS)


def test_pending_blocks_not_included() -> None:
    regions = {r.region for r in _real_rows()}
    for pending in _PENDING_BLOCKS:
        assert pending not in regions


def test_build_entity_matches_real_transcription(tmp_path: Path) -> None:
    path = build_market_series_entity(data_dir=tmp_path)
    table = pq.read_table(path)
    assert table.num_rows == 8
    assert table.column("source_strength").to_pylist() == ["平台"] * 8


# ---- 写盘与 manifest ----
def test_write_entity_atomic(tmp_path: Path) -> None:
    table = market_series_table(_real_rows())
    path = write_market_series_entity(
        table,
        data_dir=tmp_path,
        inputs=[],
        notes="测试",
    )
    assert path.name == MARKET_FILENAME
    assert not path.with_name(MARKET_FILENAME + ".incomplete").exists()
    # manifest 兄弟文件可追溯（<table>.manifest.json）
    manifest_path = path.with_suffix(".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["layer"] == ENTITIES_LAYER
    assert manifest["table"] == MARKET_TABLE
    assert manifest["row_count"] == 8
    assert manifest["notes"] == "测试"


def test_rebuild_is_reproducible(tmp_path: Path) -> None:
    first = build_market_series_entity(data_dir=tmp_path)
    second = build_market_series_entity(data_dir=tmp_path)
    assert pq.read_table(first).equals(pq.read_table(second))
