"""staged 链家 ext 普通住宅表 → marts 合并源适配器（ext-sale-ingest-scope-v1-2）。

把 ``staged/lianjia_ext`` current 指针指向 run 的 ``lianjia_ext_ordinary_residential``
作为 marts 合并的第四输入源接入 ``build_combined_marts``：

- **只读 staged**：run 产物不可变，指针只决定"当前 run"；本模块不写任何
  staged/raw 资产，文件指纹（SHA256）由调用方登记进产物 manifest；
- **普通住宅口径**：输入即 ``ordinary_residential`` 表（README §3.3 排除类
  在结构化口径先分流，不经清洗兜底）；
- **字段映射（design D3）**：与 LJ-C CSV 解析（``parse_lianjia_csv_table``）
  同一套缺失语义——空/非法 → ``None``/``UNKNOWN``，日期精度如实保留
  （DAY/MONTH），``unit_price_observed`` 仅在 ``unit_price_status=PARSED``
  时携带，派生单价与公式与既有链路一致；``source_record_id`` 直接使用
  ext 平台记录 ID（可回溯 staged 行），``raw_start_line`` = staged
  ``row_number``；
- **名称解析分流**：``split_resolvable`` 用扩展查找表（标准名 > 一致别名 >
  注册表）把可解析行与未解析行分开；未解析行不进入合并（不静默归并），
  行数与代表性源名由调用方如实登记进质量报告（LJ-E「名录外如实标记」）。

本模块**不改写**任何权威表与快照；同名解析语义与 backfill 完全一致
（待定/冲突/排除别名一律 blocked，不映射）。
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.contract.models import EventDatePrecision, MissingSemantics
from compsval.entities.backfill import (
    CommunityIdLookup,
    resolve_community_id,
)
from compsval.ingest.parsers.lianjia import LianjiaRecord

EXT_SOURCE_ID = "SRC-011"
EXT_POINTER_RELPATH = ("staged", "lianjia_ext", "current.json")
EXT_DATASET = "lianjia_ext_ordinary_residential"

# 适配器消费的 staged 列（缺失 → 显式报错，不静默降级）。
_REQUIRED_COLUMNS = (
    "row_number",
    "source_record_id",
    "community_name",
    "sale_date",
    "sale_date_precision",
    "total_price_yuan",
    "total_price_status",
    "unit_price_observed",
    "unit_price_status",
    "transaction_area_sqm",
    "area_status",
    "layout_raw",
    "floor_raw",
    "orientation",
    "decoration",
    "listing_price_yuan",
    "listing_days",
)

_PRECISION_BY_STATUS = {
    "DAY": EventDatePrecision.DAY,
    "MONTH": EventDatePrecision.MONTH,
}

#: 数据字典户型口径 = "N室N厅"（链家 TXT 解析与 subject 输入均无卫数）。
#: ext staged layout_raw 形如 "2室1厅1卫"（多出卫数），入表时归一到字典口径，
#: 原始文本（含卫数）保留在 staged 供溯源。无法归一的写法原样携带。
_LAYOUT_NORM_RE = re.compile(r"^(\d+室\d+厅)\d*卫$")


def normalize_layout(raw: object) -> str:
    """staged layout_raw → 数据字典户型口径（"N室N厅"）；不匹配则原样携带。"""
    text = str(raw).strip() if raw is not None else ""
    if not text:
        return MissingSemantics.UNKNOWN.value
    m = _LAYOUT_NORM_RE.match(text)
    return m.group(1) if m else text


@dataclass(frozen=True)
class ExtRunInput:
    """current 指针指向的 ext run（只读输入，含文件指纹）。"""

    run_id: str
    ordinary_path: Path
    ordinary_sha256: str
    table: pa.Table


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_current_ext_run(data_dir: Path) -> ExtRunInput | None:
    """读 current 指针指向 run 的普通住宅表；指针缺失 → ``None``（跳过 ext 源）。

    指针存在但 run 文件缺失 → ``FileNotFoundError``（显式失败，不静默退化为
    无属性/无 ext 的半成品重建）。
    """
    pointer_path = data_dir.joinpath(*EXT_POINTER_RELPATH)
    if not pointer_path.is_file():
        return None
    current = json.loads(pointer_path.read_text(encoding="utf-8"))
    run_id = str(current.get("run_id") or "").strip()
    rel = str(current.get("ordinary_residential") or "").strip()
    if not run_id or not rel:
        raise ValueError(
            f"ext current 指针不完整（缺 run_id/ordinary_residential）：{pointer_path}"
        )
    ordinary_path = data_dir / "staged" / "lianjia_ext" / rel
    if not ordinary_path.is_file():
        raise FileNotFoundError(f"ext current 指针指向的 run 表不存在：{ordinary_path}")
    return ExtRunInput(
        run_id=run_id,
        ordinary_path=ordinary_path,
        ordinary_sha256=_sha256_of(ordinary_path),
        table=pq.read_table(ordinary_path),
    )


def run_fetched_at(run_id: str) -> datetime:
    """run ID（``YYYYMMDDTHHMMSSZ``）→ UTC 时间（manifest/质量报告数据截点）。"""
    return datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > 0 else None


def _int_or_none(value: object) -> int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _text_or_unknown(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    return text or MissingSemantics.UNKNOWN.value


def ext_rows_to_records(table: pa.Table) -> list[LianjiaRecord]:
    """staged 普通住宅表逐行 → ``LianjiaRecord``（缺失语义与 LJ-C 一致）。"""
    missing = [name for name in _REQUIRED_COLUMNS if name not in table.column_names]
    if missing:
        raise ValueError(f"ext staged 表缺少必要列: {', '.join(missing)}")

    cols = {name: table.column(name).to_pylist() for name in _REQUIRED_COLUMNS}
    records: list[LianjiaRecord] = []
    for idx in range(table.num_rows):
        rec = LianjiaRecord(
            community=_text_or_unknown(cols["community_name"][idx]),
            layout=normalize_layout(cols["layout_raw"][idx]),
            raw_start_line=_int_or_none(cols["row_number"][idx]),
        )
        rec.source_record_id = _text_or_unknown(cols["source_record_id"][idx])

        if str(cols["area_status"][idx] or "") == "PARSE_FAILURE":
            rec.area_state = MissingSemantics.PARSE_FAILURE
        area = _decimal_or_none(cols["transaction_area_sqm"][idx])
        if area is not None:
            rec.area_sqm = area

        date_text = str(cols["sale_date"][idx] or "").strip()
        if date_text:
            try:
                rec.deal_date = datetime.fromisoformat(date_text).date()
                rec.deal_date_precision = _PRECISION_BY_STATUS.get(
                    str(cols["sale_date_precision"][idx] or ""),
                    EventDatePrecision.UNKNOWN,
                )
            except ValueError:
                rec.deal_date_precision = EventDatePrecision.UNKNOWN

        if str(cols["total_price_status"][idx] or "") == "PARSED":
            total = _decimal_or_none(cols["total_price_yuan"][idx])
            if total is not None:
                rec.total_price_yuan = total.to_integral_value()

        if str(cols["unit_price_status"][idx] or "") == "PARSED":
            unit = _decimal_or_none(cols["unit_price_observed"][idx])
            if unit is not None:
                rec.unit_price_observed = unit.to_integral_value()

        if rec.area_sqm and rec.total_price_yuan:
            rec.unit_price_derived = (
                rec.total_price_yuan / rec.area_sqm
            ).to_integral_value()
            rec.unit_price_formula = "total_price_yuan / area_sqm, rounded to integer"

        rec.orientation = _text_or_unknown(cols["orientation"][idx])
        rec.decoration = _text_or_unknown(cols["decoration"][idx])
        rec.floor = _text_or_unknown(cols["floor_raw"][idx])

        listing = _decimal_or_none(cols["listing_price_yuan"][idx])
        if listing is not None:
            rec.listing_price_yuan = listing.to_integral_value()
        rec.listing_period_days = _int_or_none(cols["listing_days"][idx])

        records.append(rec)
    return records


def split_resolvable(
    records: list[LianjiaRecord],
    lookup: CommunityIdLookup,
) -> tuple[list[LianjiaRecord], Counter[str]]:
    """按扩展查找表分流：可解析行入合并，未解析行计数留痕（不静默丢弃）。"""
    kept: list[LianjiaRecord] = []
    unmatched: Counter[str] = Counter()
    for rec in records:
        community_id, _outcome, _reason = resolve_community_id(rec.community, lookup)
        if community_id is None:
            unmatched[rec.community] += 1
        else:
            kept.append(rec)
    return kept, unmatched
