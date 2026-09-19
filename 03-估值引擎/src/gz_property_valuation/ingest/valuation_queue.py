"""交换实现: 103 待估值清单专用读取器（接口约定 V1 §2.2/§2.3，design D1）。

103 导出 CSV 为 utf-8-sig（带 BOM）且首行为 ``#`` 声明行，既有
``pyarrow.csv`` 直读路径不处理这两类偏差，也不做列血缘映射；本读取器：

- 显式剥离 UTF-8 BOM（首列名不得带 ``\\ufeff`` 前缀）；
- 跳过 ``#`` 开头声明行（声明行不得混入数据行）；
- 16 列结构校验（列名与顺序与契约 §2.2 完全一致，不符即拒绝——坏文件
  不得以「残缺快照」形态污染证据链）；
- 血缘映射 house_id→source_record_id、community→community_name，
  其余 14 列原样保留（全部按字符串读入，缺失即空串，不做数值解释）。

分区口径：契约 §2.3 规定快照落 ``fetched_at=<YYYYMMDD>``（天级），与
``write_raw_snapshot`` 默认秒级戳（``%Y%m%dT%H%M%SZ``）不同；
:class:`DayStampDateTime` 只把该分区格式重定向为天级，其余行为不变，
不改共享快照原语。
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from gz_property_valuation.ingest.snapshots import FETCHED_AT_FORMAT

#: valuation_queue 数据集名（103 待估值清单）。
VALUATION_QUEUE_DATASET = "valuation_queue"

#: 契约 §2.2 的 16 列（列名与顺序即结构校验基准）。
VALUATION_QUEUE_COLUMNS: tuple[str, ...] = (
    "house_id",
    "community",
    "board_code",
    "title",
    "layout",
    "area_sqm",
    "orientation",
    "decoration",
    "floor",
    "year_built",
    "total_price_yuan",
    "unit_price",
    "follow_count",
    "published_at",
    "first_seen_at",
    "last_seen_at",
)

#: 声明行前缀（CSV 镜像风格：首行 ``#`` 开头，先于表头与数据行）。
DECLARATION_PREFIX = "#"

#: 血缘映射（契约 §2.3）：house_id→source_record_id、community→community_name。
LINEAGE_RENAME: dict[str, str] = {
    "house_id": "source_record_id",
    "community": "community_name",
}


class DayStampDateTime(datetime):
    """天级 fetched_at：仅把 write_raw_snapshot 的分区格式重定向为 %Y%m%d。"""

    def strftime(self, format: str) -> str:
        if format == FETCHED_AT_FORMAT:
            return datetime.strftime(self, "%Y%m%d")
        return datetime.strftime(self, format)


def day_stamp(value: datetime) -> DayStampDateTime:
    """把 aware datetime 转为天级戳（时分秒截断，时区保持不变）。"""
    return DayStampDateTime(value.year, value.month, value.day, tzinfo=value.tzinfo)


def snapshot_columns() -> list[str]:
    """快照列序：契约 16 列顺序，其中两列按血缘映射改名。"""
    return [LINEAGE_RENAME.get(name, name) for name in VALUATION_QUEUE_COLUMNS]


def read_valuation_queue_csv(path: Path) -> pa.Table:
    """读取 103 待估值清单 CSV → 结构校验后的全字符串 PyArrow 表。

    列名集合或顺序与契约 §2.2 不符、或数据行字段数不为 16 →
    ``ValueError`` 拒绝摄入。原文件只读，绝不改写。
    """
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    text = raw.decode("utf-8")
    data_lines = [
        line for line in text.splitlines() if not line.startswith(DECLARATION_PREFIX)
    ]
    reader = csv.reader(io.StringIO("\n".join(data_lines)))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValueError("待估值清单为空：缺少表头行") from exc
    if tuple(header) != VALUATION_QUEUE_COLUMNS:
        raise ValueError(
            "待估值清单 16 列结构不符（契约 §2.2）："
            f"期望 {list(VALUATION_QUEUE_COLUMNS)}，实际 {header}"
        )
    columns: dict[str, list[str]] = {name: [] for name in snapshot_columns()}
    for line_no, row in enumerate(reader, start=2):
        if not row:
            continue
        if len(row) != len(VALUATION_QUEUE_COLUMNS):
            raise ValueError(
                f"待估值清单第 {line_no} 行字段数 {len(row)} ≠ 16，拒绝摄入"
            )
        for name, value in zip(VALUATION_QUEUE_COLUMNS, row, strict=True):
            columns[LINEAGE_RENAME.get(name, name)].append(value)
    return pa.table(
        {name: pa.array(values, type=pa.string()) for name, values in columns.items()}
    )
