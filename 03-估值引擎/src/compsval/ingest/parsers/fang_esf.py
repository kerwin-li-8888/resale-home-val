"""房天下（fang.com，SRC-005）成交列表 CSV 解析器（G3R-A）。

解析 WP3-A / 2026-08-22 补数采集的房天下成交列表结构化 CSV（原始证据=
41 个 HTML 页，补数快照索引-20260822 登记），列为：:

    area_m2, deal_date, total_price_wan, unit_price_yuan_m2, source_note

- ``deal_date`` 房天下精确到日 → ``date``（DAY 精度）；
- ``total_price_wan``（万）→ 整数人民币元（``231万`` = 2310000）；
- ``unit_price_yuan_m2`` 为平台披露单价（元/㎡），保留为 ``unit_price_observed``；
- 派生单价 = ``total_price_yuan / area_sqm`` 取整，并记录公式
  （与链家解析器同口径，RV-WP4-B-01 F1 舍入容忍见 clean.py）；
- 列表无户型/楼层/朝向/挂牌 → 对应字段 UNKNOWN / None，不虚构。

原始 CSV 按小区分文件、**不含小区名列**；快照→小区名由
:data:`FANG_COMMUNITY_REGISTRY`（loupan ID → 标准名，来源=补数快照索引
§1/候选小区名录）解析，经 :func:`resolve_fang_community` 从快照 query（来源
URL 含 ``loupan/<ID>/``）得到。

缺失纪律与链家解析器一致（数据字典 §1/§5）：缺失用 UNKNOWN/MISSING，
数值用 None，**不得用 0**；解析失败用 PARSE_FAILURE 标记。本模块不做清洗/
去重（归 G3R-C marts 合并）。
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Final

import pyarrow as pa

from compsval.contract.models import (
    EventDatePrecision,
    MissingSemantics,
)

PARSER_VERSION: Final = "fang-esf-csv.v1"

#: 快照 query/URL 中提取 loupan ID 的模式（如 https://esf.fang.com.example/loupan/2811405748/chengjiao/）
_LOUPAN_ID_RE = re.compile(r"loupan/(?P<id>\d+)/")

#: 快照→小区名注册表（loupan ID → 标准名）。权威来源：补数快照索引
#: -20260822-V0.1.md §1（小区ID/名称）与候选小区名录-V0.1.md（板块归属）。
#: 键为字符串 ID（URL 中即字符串，避免 int 前导零语义问题）。
FANG_COMMUNITY_REGISTRY: Final[dict[str, str]] = {
    "2811405748": "示例小区166",  # 补数快照索引 §1.1（宝岗）
    "2811007476": "示例小区136",  # §1.2（宝岗）
    "2811021647": "拾光里",  # §1.3（宝岗；候选名录 §2.4 行1 补数新增）
    "2811019201": "示例小区121",  # §1.4（昌岗路）
    "2811052010": "示例小区132",  # §1.5（工业大道北）
    "2811007655": "示例小区203",  # §1.6（昌岗路）
    "2811034445": "示例小区130",  # §1.7（滨江西；候选名录 §2.9 行1 补数新增）
}


def resolve_fang_community(query_or_url: str | None) -> str | None:
    """从快照 query（来源 URL）解析小区标准名。

    提取 ``loupan/<ID>/`` 中的 ID 并在 :data:`FANG_COMMUNITY_REGISTRY`
    查表；URL 不含 ID 或 ID 未登记 → ``None``（不臆测，由调用方标记）。
    """
    if not query_or_url:
        return None
    match = _LOUPAN_ID_RE.search(query_or_url)
    if match is None:
        return None
    return FANG_COMMUNITY_REGISTRY.get(match.group("id"))


def _fingerprint(record: FangEsfRecord) -> str:
    """确定性指纹：同一交易身份（小区/面积/成交日/总价/披露单价）同指纹。"""
    payload = "|".join(
        [
            record.community,
            str(record.area_sqm or ""),
            record.deal_date.isoformat() if record.deal_date else "",
            str(record.total_price_yuan or ""),
            str(record.unit_price_observed or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass
class FangEsfRecord:
    """一条房天下成交列表记录（G3R-A 输出；字段面与 LianjiaRecord 对齐）。"""

    community: str
    area_sqm: Decimal | None = None
    area_state: MissingSemantics | None = None
    deal_date: date | None = None
    deal_date_precision: EventDatePrecision = EventDatePrecision.UNKNOWN
    total_price_yuan: Decimal | None = None
    original_price_text: str = ""
    unit_price_observed: Decimal | None = None
    unit_price_derived: Decimal | None = None
    unit_price_formula: str = ""
    source_note: str = MissingSemantics.UNKNOWN.value
    source_record_id: str = MissingSemantics.UNKNOWN.value
    raw_start_line: int | None = None
    # 以下字段与 clean.py / sale_event_table 消费接口对齐；房天下列表无 → UNKNOWN/None。
    layout: str = MissingSemantics.UNKNOWN.value
    orientation: str = MissingSemantics.UNKNOWN.value
    listing_price_yuan: Decimal | None = None
    listing_period_days: int | None = None
    # CSV 结构化解析无"未解析行"概念 → 恒为空（质量报告接口对齐，FangEsf 缺失语义
    # 经 area_state/deal_date_precision 等显式表达）。
    unparsed_lines: tuple[str, ...] = ()


#: CSV 必读列（缺列 = 快照 schema 漂移，直接报错防止静默错读）。
_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    "area_m2",
    "deal_date",
    "total_price_wan",
    "unit_price_yuan_m2",
    "source_note",
)


def _parse_deal_date(value: object) -> tuple[date | None, EventDatePrecision]:
    """``date32`` 标量或 ISO 字符串 → (date, DAY)；缺失/坏值 → (None, UNKNOWN)。"""
    if value is None:
        return None, EventDatePrecision.UNKNOWN
    if isinstance(value, date):
        return value, EventDatePrecision.DAY
    text = str(value).strip()
    if not text:
        return None, EventDatePrecision.UNKNOWN
    try:
        return date.fromisoformat(text), EventDatePrecision.DAY
    except ValueError:
        return None, EventDatePrecision.UNKNOWN


def _to_decimal(value: object) -> Decimal | None:
    """pyarrow 数值/字符串 → Decimal；空/非法 → None。"""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_fang_esf_csv(table: pa.Table, community: str) -> list[FangEsfRecord]:
    """把房天下成交 CSV 的结构化快照表解析为规范化记录列表。

    ``table`` 为 ``compsval ingest file`` 导入后的原始快照表（pyarrow CSV 推断：
    area_m2=double / deal_date=date32 / total_price_wan=int64 /
    unit_price_yuan_m2=int64 / source_note=string）。``community`` 为
    :func:`resolve_fang_community` 解析出的小区标准名（调用方保证非空）。

    缺失/非法值语义：面积/总价/单价空 → ``None`` + 对应 MissingSemantics；
    面积/总价 <= 0 → ``PARSE_FAILURE``；日期不可解析 → 精度 UNKNOWN。
    ``raw_start_line`` = CSV 物理行号（表头第 1 行，数据行从第 2 行起）。
    """
    missing = [c for c in _REQUIRED_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"房天下成交快照缺少必要列: {', '.join(missing)}")

    areas = table.column("area_m2").to_pylist()
    dates = table.column("deal_date").to_pylist()
    totals = table.column("total_price_wan").to_pylist()
    units = table.column("unit_price_yuan_m2").to_pylist()
    notes = table.column("source_note").to_pylist()

    records: list[FangEsfRecord] = []
    for idx, (area_raw, date_raw, total_raw, unit_raw, note_raw) in enumerate(
        zip(areas, dates, totals, units, notes, strict=True)
    ):
        rec = FangEsfRecord(community=community, raw_start_line=2 + idx)

        area = _to_decimal(area_raw)
        if area is None:
            rec.area_state = MissingSemantics.MISSING
        elif area <= 0:
            rec.area_state = MissingSemantics.PARSE_FAILURE
        else:
            rec.area_sqm = area

        rec.deal_date, rec.deal_date_precision = _parse_deal_date(date_raw)

        total_wan = _to_decimal(total_raw)
        if total_wan is None:
            # 总价缺失：保留原文占位不臆测
            rec.original_price_text = MissingSemantics.UNKNOWN.value
        else:
            # 原文保留：万为单位、十进制普通格式（不输出科学计数法）
            rec.original_price_text = f"{format(total_wan.normalize(), 'f')}万"
            if total_wan > 0:
                rec.total_price_yuan = (total_wan * Decimal("10000")).to_integral_value()

        observed = _to_decimal(unit_raw)
        if observed is not None and observed > 0:
            rec.unit_price_observed = observed.to_integral_value()

        if rec.area_sqm and rec.total_price_yuan:
            rec.unit_price_derived = (
                rec.total_price_yuan / rec.area_sqm
            ).to_integral_value()
            rec.unit_price_formula = "total_price_yuan / area_sqm, rounded to integer"

        note = str(note_raw).strip() if note_raw is not None else ""
        rec.source_note = note or MissingSemantics.UNKNOWN.value

        rec.source_record_id = _fingerprint(rec)
        records.append(rec)
    return records


__all__ = [
    "FANG_COMMUNITY_REGISTRY",
    "FangEsfRecord",
    "PARSER_VERSION",
    "parse_fang_esf_csv",
    "resolve_fang_community",
]
