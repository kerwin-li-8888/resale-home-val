"""WP5-E: staged 事件 community_id 回填（provisional 小区名 → 标准小区 ID）。

对照 WP5-E 验收标准：
① 成交/挂牌事件 community_id 全部回填为标准 ID；
② 回填经 alias 映射可溯源（映射理由 = alias 的 source_ref）；
③ 未匹配 / 低置信（PENDING/CONFLICT）小区不静默归并、回填为 None；
④ 重跑可复现（同快照 + 同实体表 → 派生 staged 表字节一致）；
⑤ ruff/mypy/pytest 通过。

实体表缺失时回填退化为保留 provisional 值（行为不变，兼容无 entities 环境）。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.catalog import list_snapshots
from compsval.contract.models import AliasConflictStatus
from compsval.entities.alias import ALIAS_FILENAME, alias_schema
from compsval.entities.backfill import (
    BackfillOutcome,
    CommunityIdLookup,
    collect_unmatched_conflicts,
    load_community_lookup,
    resolve_community_id,
)
from compsval.entities.community import (
    COMMUNITY_FILENAME,
    ENTITIES_LAYER,
    community_schema,
)
from compsval.ingest.clean import clean_sales, sale_event_table
from compsval.ingest.import_file import import_local_file
from compsval.ingest.listing import listing_event_table
from compsval.ingest.parsers.lianjia import LianjiaRecord
from compsval.ingest.stage import data_stage

_FETCHED = datetime(2026, 8, 21, 3, 14, 0, tzinfo=UTC)
_SOURCE_ID = "SRC-X"
_SNAPSHOT_ID = "snap-1"


def _sale(
    *,
    community: str,
    total_wan: str = "258",
    area_sqm: str = "84.04",
    raw_line: int = 1,
) -> LianjiaRecord:
    return LianjiaRecord(
        community=community,
        layout="2室1厅",
        area_sqm=Decimal(area_sqm),
        deal_date=date(2026, 7, 19),
        total_price_yuan=(Decimal(total_wan) * 10000).to_integral_value(),
        original_price_text=f"{total_wan}万",
        listing_price_yuan=(Decimal(total_wan) * 10000).to_integral_value(),
        raw_start_line=raw_line,
    )


def _write_entities(data_dir: Path) -> None:
    """写一张合成 community + community_alias 实体表（直接落到 data/entities/）。"""
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)

    community = pa.table(
        {
            "community_id": ["C-1", "C-2"],
            "standard_name": ["绿洲花园", "越秀花园"],
            "block": ["A区", "B区"],
            "address": ["示例路1号", "示例路2号"],
            "latitude": [None, None],
            "longitude": [None, None],
            "coordinate_system": ["UNKNOWN", "UNKNOWN"],
            "boundary_status": ["机器确认", "机器确认"],
            "source_id": ["SRC-005", "SRC-005"],
            "source_key": ["1", "2"],
            "source_ref": ["名录 §3", "名录 §3"],
            "notes": [None, None],
        },
        schema=community_schema(),
    )
    pq.write_table(community, entities_dir / COMMUNITY_FILENAME, compression="zstd")

    alias = pa.table(
        {
            "alias_id": ["A-9-1", "A-9-2", "A-9-3"],
            "community_id": ["C-1", "C-2", "C-1"],
            "source_alias": ["绿洲家园", "越秀紫苑", "相似命名X"],
            "source_id": ["SRC-006", "SRC-005", "SRC-005"],
            "source_ref": [
                "候选小区名录-V0.1.md §3 #9",
                "候选小区名录-V0.1.md §3 #9",
                "候选小区名录-V0.1.md §3 #9",
            ],
            "conflict_status": [
                AliasConflictStatus.CONSISTENT.value,
                AliasConflictStatus.PENDING.value,
                AliasConflictStatus.CONFLICT.value,
            ],
        },
        schema=alias_schema(),
    )
    pq.write_table(alias, entities_dir / ALIAS_FILENAME, compression="zstd")


# ---------------------------------------------------------------------------
# load_community_lookup：从实体表构建查找表（实体缺失 → 空查找）
# ---------------------------------------------------------------------------


def test_load_lookup_builds_from_entity_tables(tmp_path: Path) -> None:
    _write_entities(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)

    # 标准名 → C-id
    assert lookup.canonical["绿洲花园"] == ("C-1", "社区权威表 standard_name 命中")
    # CONSISTENT 别名 → C-id（溯源理由 = alias source_ref）
    assert lookup.alias_consistent["绿洲家园"] == ("C-1", "候选小区名录-V0.1.md §3 #9")
    # PENDING/CONFLICT 别名 → 进 blocked，不带出可映射 id
    assert lookup.blocked["越秀紫苑"] == AliasConflictStatus.PENDING.value
    assert lookup.blocked["相似命名X"] == AliasConflictStatus.CONFLICT.value
    assert not lookup.empty


def test_load_lookup_is_empty_when_entities_missing(tmp_path: Path) -> None:
    lookup = load_community_lookup(data_dir=tmp_path)
    assert lookup.empty


# ---------------------------------------------------------------------------
# 验收①：标准名 / 一致别名正确回填为标准 ID
# ---------------------------------------------------------------------------


def test_resolve_canonical_name_hits() -> None:
    lookup = _sample_lookup()
    community_id, outcome, reason = resolve_community_id("绿洲花园", lookup)
    assert community_id == "C-1"
    assert outcome is BackfillOutcome.HIT_CANONICAL
    assert "standard_name" in reason


def _sample_lookup() -> CommunityIdLookup:
    lookup = CommunityIdLookup(
        canonical={
            "绿洲花园": ("C-1", "社区权威表 standard_name 命中"),
            "越秀花园": ("C-2", "社区权威表 standard_name 命中"),
        },
        alias_consistent={"绿洲家园": ("C-1", "候选小区名录 §3 #9")},
        blocked={
            "越秀紫苑": AliasConflictStatus.PENDING.value,
            "相似命名X": AliasConflictStatus.CONFLICT.value,
        },
    )
    return lookup


def test_resolve_consistent_alias_hits_with_traceable_reason() -> None:
    lookup = _sample_lookup()
    community_id, outcome, reason = resolve_community_id("绿洲家园", lookup)
    assert community_id == "C-1"
    assert outcome is BackfillOutcome.HIT_ALIAS
    # 验收②：经 alias 映射可溯源（理由 = 该 alias 的 source_ref/出处）
    assert "§3 #9" in reason


# ---------------------------------------------------------------------------
# 验收③：未匹配 / 低置信（PENDING/CONFLICT）不静默归并 → None
# ---------------------------------------------------------------------------


def test_resolve_blocked_pending_is_not_merged() -> None:
    lookup = _sample_lookup()
    community_id, outcome, reason = resolve_community_id("越秀紫苑", lookup)
    assert community_id is None
    assert outcome is BackfillOutcome.BLOCKED
    assert "不静默合并" in reason


def test_resolve_blocked_conflict_is_not_merged() -> None:
    lookup = _sample_lookup()
    community_id, outcome, _reason = resolve_community_id("相似命名X", lookup)
    assert community_id is None
    assert outcome is BackfillOutcome.BLOCKED


def test_resolve_unmatched_returns_none() -> None:
    lookup = _sample_lookup()
    community_id, outcome, reason = resolve_community_id("不存在的小区", lookup)
    assert community_id is None
    assert outcome is BackfillOutcome.UNMATCHED
    assert "均未命中" in reason


def test_collect_unmatched_conflicts_aggregates_blocked_and_unmatched() -> None:
    lookup = _sample_lookup()
    names = ["绿洲花园", "绿洲家园", "越秀紫苑", "相似命名X", "神秘乡村", "", "  "]
    conflicts = collect_unmatched_conflicts(names, lookup)
    # 命中（标准名/一致别名）不进清单；PENDING/CONFLICT 与未匹配去重后进清单
    assert conflicts == ["相似命名X", "神秘乡村", "越秀紫苑"]


def test_collect_unmatched_conflicts_empty_lookup_returns_empty(tmp_path: Path) -> None:
    lookup = load_community_lookup(data_dir=tmp_path)  # 无实体表 → 空查找
    assert collect_unmatched_conflicts(["绿洲花园"], lookup) == []


def test_resolve_none_or_blank_never_matches() -> None:
    lookup = _sample_lookup()
    for bad in (None, "", "  "):
        community_id, outcome, _reason = resolve_community_id(bad, lookup)
        assert community_id is None
        assert outcome is BackfillOutcome.UNMATCHED


# ---------------------------------------------------------------------------
# 验收①集成：sale_event_table / listing_event_table 用查找表回填 community_id
# ---------------------------------------------------------------------------


def test_sale_event_table_backfills_community_id(tmp_path: Path) -> None:
    _write_entities(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)
    cleaned, _summary = clean_sales([_sale(community="绿洲花园")])
    table = sale_event_table(
        cleaned,
        source_id=_SOURCE_ID,
        snapshot_id=_SNAPSHOT_ID,
        fetched_at=_FETCHED,
        community_lookup=lookup,
    )
    assert table.column("community_id").to_pylist() == ["C-1"]


def test_sale_event_table_backfills_via_alias_and_leaves_unmatched_none(
    tmp_path: Path,
) -> None:
    _write_entities(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)
    cleaned, _summary = clean_sales(
        [_sale(community="绿洲家园", raw_line=1), _sale(community="神秘乡村", raw_line=2)]
    )
    table = sale_event_table(
        cleaned,
        source_id=_SOURCE_ID,
        snapshot_id=_SNAPSHOT_ID,
        fetched_at=_FETCHED,
        community_lookup=lookup,
    )
    assert table.column("community").to_pylist() == ["绿洲家园", "神秘乡村"]
    # 一致别名回填为 C-1；未匹配小区回填为 None（不静默归并）
    assert table.column("community_id").to_pylist() == ["C-1", None]


def test_listing_event_table_backfills_community_id(tmp_path: Path) -> None:
    _write_entities(tmp_path)
    lookup = load_community_lookup(data_dir=tmp_path)
    table = listing_event_table(
        [_sale(community="越秀花园")],
        source_id=_SOURCE_ID,
        snapshot_id=_SNAPSHOT_ID,
        fetched_at=_FETCHED,
        community_lookup=lookup,
    )
    assert table.column("community_id").to_pylist() == ["C-2"]


def test_sale_event_table_preserves_provisional_without_lookup(tmp_path: Path) -> None:
    # 实体表缺失 / 空查找 → 回填退化为保留 provisional 小区名（行为不变）
    cleaned, _summary = clean_sales([_sale(community="绿洲花园")])
    table = sale_event_table(
        cleaned,
        source_id=_SOURCE_ID,
        snapshot_id=_SNAPSHOT_ID,
        fetched_at=_FETCHED,
    )
    assert table.column("community_id").to_pylist() == ["绿洲花园"]


# ---------------------------------------------------------------------------
# 验收④：同快照 + 同实体表重跑 → staged 派生表字节一致（可复现）
# ---------------------------------------------------------------------------


def test_data_stage_with_entities_is_reproducible(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    _seed_snapshot(lake)
    _write_entities(lake)
    ref = list_snapshots(lake)[0]

    first = data_stage(ref, data_dir=lake)
    second = data_stage(ref, data_dir=lake)

    assert first.sale_event_path.read_bytes() == second.sale_event_path.read_bytes()
    assert first.listing_event_path.read_bytes() == second.listing_event_path.read_bytes()
    # 回填确实生效：绿洲花园 → C-1；神秘乡村未匹配 → None（不静默归并）
    assert pq.read_table(first.sale_event_path).column("community_id").to_pylist() == ["C-1", None]
    # 验收③：未匹配清单真实落地到数据质量报告
    report = json.loads(first.quality_report_json.read_text(encoding="utf-8"))
    assert "神秘乡村" in report["unmatched_conflicts"]


def _seed_snapshot(lake: Path) -> None:
    lake.mkdir(parents=True, exist_ok=True)
    raw = lake / "evidence" / "lianjia.txt"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text(
        "\n".join(
            ["示例城市目标区二手房成交记录", "链家网·家和置业", "共2条", "https://lianjia.com.example/chengjiao/targetdistrict/"]
            + [
                "绿洲花园 2室1厅 84.04平米",
                "南 | 精装2026.04.12 258万",
                "中楼层(共33层) 2005年板楼30700元/平",
                "挂牌258万成交周期89天",
                "",
                "神秘乡村 3室1厅 100.50平米",
                "南 | 简装2026.05.01 350万",
                "中楼层(共20层) 2010年塔楼34800元/平",
                "挂牌350万成交周期40天",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    import_local_file(
        input_path=raw,
        source="lianjia",
        dataset="chengjiao_list",
        fetched_at=_FETCHED,
        query="https://lianjia.com.example/chengjiao/targetdistrict/",
        data_dir=lake,
    )