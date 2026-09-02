"""WP5-D market_series 市场序列登记（时间修正候选证据，本包不做修正计算）。

从 58 同城目标区板块均价（SRC-008；候选小区名录-V0.1 §1.1 边界表 2026-08
采集转录）构建 ``market_series`` 市场序列实体表，写入 ``data/entities/
market_series.parquet`` 并附 DerivedManifest（可复现 + 溯源）：

- ``series_id = MS-<板块序号>-<YYYYMM>``（稳定标识，数据字典 §3.8 主键）；
- ``region`` 取 58 板块名（目标区西部板块，边界已由用户确认 2026-08-21）；
- ``month`` = 2026-08（精度到月）；``price`` 为板块月度聚合均价（元/㎡）；
- 名录中"待补"板块无数值 → **不落表**（无数据不虚构）；``price_change`` 名录
  未转录同比/环比 → ``None``（不得用 0）；
- ``source_strength`` = 平台（58 为平台聚合口径，不冒充官方，验收②）；
- ``revision_flag`` = True（58 平台月度更新、聚合口径不透明、走势页可切换
  近3月/1年/10年，历史值会被平台修订，需求 §3.4）；
- ``source_id = SRC-008``、``source_key = 板块序号``、``source_ref`` 指向
  候选小区名录 §1.1 边界表行 + 58 快照引用（每行可溯源，验收①）。

市场序列为**独立实体表**：entities 层（非 staged 事件层）、板块聚合粒度（非
逐套成交），与成交（sale_event）/挂牌（listing_event）事件表严格分离（验收
④），构建过程不读取、不改写事件表。本包**不做时间修正计算**（归 WP6
VAL1-004，禁止改动）。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import SourceStrength
from compsval.entities.community import ENTITIES_LAYER
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)

MARKET_TABLE = "market_series"
MARKET_FILENAME = f"{MARKET_TABLE}.parquet"

#: 市场序列来源：58 同城·示例城市目标区·板块均价（registered SRC-008）。
F58_SOURCE_ID = "SRC-008"
#: 58 快照引用（样本快照索引-V0.1：source=58/dataset=ban_kkuai_price/fetched_at=20260820）。
F58_INPUT = InputRef(dataset="ban_kkuai_price", fetched_at="2026-08-20")
#: 58 板块均价统计月份（2026-08，精度到月）。
F58_MONTH = date(2026, 8, 1)


@dataclass(frozen=True)
class F58Block:
    """一条 58 板块均价转录（候选小区名录 §1.1 边界表，有值板块）。"""

    seq: int
    region: str
    price: int
    remark: str | None = None


#: 名录 §1.1 中具真实 58 均价数值的板块（8 个；"待补"板块无数值不落表）。
_F58_BLOCKS: tuple[F58Block, ...] = (
    F58Block(1, "工业大道中", 27623, "58 单列（房天下未单列）"),
    F58Block(2, "工业大道南", 34672, None),
    F58Block(3, "汐园", 44987, "房天下归入工业大道北"),
    F58Block(4, "南洲", 30823, None),
    F58Block(5, "前进路", 21974, "已确认纳入（2026-08-21）"),
    F58Block(6, "滨江西", 49308, None),
    F58Block(7, "滨江中", 50399, "已确认纳入（2026-08-21）"),
    F58Block(8, "新港西", 7977, "58 全板块口径（非西段单独）"),
)


@dataclass(frozen=True)
class MarketSeriesRow:
    """一条 market_series 实体行（含来源溯源，写表前组装）。"""

    series_id: str
    region: str
    month: date
    price: Decimal | None
    price_change: Decimal | None
    source_strength: SourceStrength
    revision_flag: bool | None
    source_id: str
    source_key: str
    source_ref: str


def series_id_of(seq: int, month: date) -> str:
    """市场序列唯一 ID：``MS-<板块序号>-<YYYYMM>``（数据字典 §3.8 主键）。"""
    return f"MS-{seq:03d}-{month:%Y%m}"


def _f58_source_ref(block: F58Block) -> str:
    """58 板块均价的来源定位（每行可追溯，验收①）。"""
    remark = f"；{block.remark}" if block.remark else ""
    return (
        f"候选小区名录-V0.1.md §1.1 边界表 板块<{block.region}>行{remark}"
        f"；58 快照 source=58/dataset=ban_kkuai_price/fetched_at=20260820"
        f"（页面 https://gz.58.com/fangjia/1657/）"
    )


def _f58_rows() -> list[MarketSeriesRow]:
    """58 板块均价转录 → market_series 行（平台强度，revision_flag=True）。"""
    rows: list[MarketSeriesRow] = []
    for block in _F58_BLOCKS:
        rows.append(
            MarketSeriesRow(
                series_id=series_id_of(block.seq, F58_MONTH),
                region=block.region,
                month=F58_MONTH,
                price=Decimal(block.price),
                # 名录未转录同比/环比 → None（未知不用 0）
                price_change=None,
                source_strength=SourceStrength.PLATFORM,
                # 58 平台月度更新会修订历史值（需求 §3.4）
                revision_flag=True,
                source_id=F58_SOURCE_ID,
                source_key=f"{block.seq:03d}",
                source_ref=_f58_source_ref(block),
            )
        )
    return rows


def market_series_schema() -> pa.Schema:
    """``market_series`` 实体表 PyArrow 模式（数据字典 §3.8 字段 + 溯源扩展）。"""
    return pa.schema(
        [
            pa.field("series_id", pa.string(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("month", pa.date32(), nullable=False),
            pa.field("price", pa.decimal128(12, 2), nullable=True),
            pa.field("price_change", pa.decimal128(8, 2), nullable=True),
            pa.field("source_strength", pa.string(), nullable=False),
            pa.field("revision_flag", pa.bool_(), nullable=True),
            pa.field("source_id", pa.string(), nullable=False),
            pa.field("source_key", pa.string(), nullable=False),
            pa.field("source_ref", pa.string(), nullable=False),
        ]
    )


def market_series_table(rows: Sequence[MarketSeriesRow]) -> pa.Table:
    """把 market_series 实体序列构造成与 :func:`market_series_schema` 一致的表。"""
    cols: dict[str, list[object]] = {name: [] for name in market_series_schema().names}
    for row in rows:
        row_values: dict[str, object] = {
            "series_id": row.series_id,
            "region": row.region,
            "month": row.month,
            "price": row.price,
            "price_change": row.price_change,
            "source_strength": row.source_strength.value,
            "revision_flag": row.revision_flag,
            "source_id": row.source_id,
            "source_key": row.source_key,
            "source_ref": row.source_ref,
        }
        if list(row_values) != list(cols):
            raise AssertionError("row column order diverged from schema")  # pragma: no cover
        for name in cols:
            cols[name].append(row_values[name])
    return pa.table(cols, schema=market_series_schema())


def write_market_series_entity(
    table: pa.Table,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef],
    notes: str | None = None,
) -> Path:
    """把 market_series 实体表及其 DerivedManifest 原子写入 ``data/entities/``。

    与 community/alias/building 写盘纪律一致：先写 ``.incomplete`` 兄弟文件再
    重命名，避免半写表冒充完整派生表。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    entities_dir.mkdir(parents=True, exist_ok=True)
    final_path = entities_dir / MARKET_FILENAME
    work_path = entities_dir / (MARKET_FILENAME + ".incomplete")

    pq.write_table(table, work_path, compression="zstd")
    manifest = DerivedManifest(
        layer=ENTITIES_LAYER,
        table=MARKET_TABLE,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=list(inputs),
        package_version=__version__,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def build_market_series_entity(
    *,
    data_dir: Path,
    notes: str | None = None,
) -> Path:
    """从 58 板块均价转录构建并写入 market_series 实体表（WP5-D 主入口）。

    返回写入的 parquet 路径；行为 58 名录有值板块数（8）一致。数据来自候选
    小区名录 §1.1 转录（非 raw 快照导入），构建过程不读取、不改写成交/挂牌
    事件表（验收④）。
    """
    rows = _f58_rows()
    table = market_series_table(rows)
    return write_market_series_entity(
        table,
        data_dir=data_dir,
        inputs=[F58_INPUT],
        notes=notes,
    )
