"""链家（SRC-007）成交列表 HTML 解析器与交叉校验（LJ-C）。

LJ-B 采集的链家成交页为 HTML 原样（``.listContent`` 内每条成交一个
``<li>``），同时 fetch_log JSONL 保存了浏览器提取的 ``.listContent
innerText``。本模块实现**独立的** HTML 解析路径（不依赖 fetch_log），
用于：

1. 从 HTML 直接解析为 :class:`LianjiaRecord`（字段提取自 li 的 div 结构，
   拼接为与 WP4-B TXT 一致的"行块"格式后复用 :func:`parse_lianjia_txt`
   的标准化纪律——日期/面积/总价/单价/缺失语义完全一致）；
2. 生成结构化 CSV（每小区一个文件，供 LJ-D ``compsval ingest file`` 导入为
   不可变 raw 快照）；
3. 解析 LJ-D 导入后的 CSV 快照表为规范化记录（供 LJ-E marts 合并）；
4. **交叉校验**：HTML 解析记录集合 vs fetch_log rows 解析记录集合双向
   比对（面积/成交日/总价/户型/小区名 五元组），保证采集证据一致
   （房天下补数经验 E5：CSV↔HTML 双向校验）。

缺失纪律与 WP4-B 一致：缺失用 UNKNOWN/MISSING，数值 None，**不用 0**；
车位记录（``layout == "车位"``）保留原样（清洗归 LJ-E/WP4-C）。
本模块不做清洗/去重/回填。
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.csv  # noqa: F401  # 顶层不暴露 pa.csv，显式导入保证可用

from compsval.contract.models import (
    EventDatePrecision,
    MissingSemantics,
)
from compsval.ingest.parsers.lianjia import (
    LianjiaRecord,
    parse_lianjia_txt,
)

PARSER_VERSION: Final = "lianjia-html.v1"

#: HTML 中 `<ul class="listContent">` 成交列表容器。
_LIST_UL_RE = re.compile(r'<ul class="listContent"[\s\S]*?</ul>')
#: 列表内每条成交的 li。
_LI_RE = re.compile(r"<li[^>]*>([\s\S]*?)</li>")
#: li 内按 div class 提取文本。
_DIV_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _div_re(cls: str) -> re.Pattern[str]:
    """按 div class 提取其内文本（去标签、压缩空白）的正则（带缓存）。"""
    if cls not in _DIV_RE_CACHE:
        _DIV_RE_CACHE[cls] = re.compile(
            r'<div class="' + re.escape(cls) + r'"[^>]*>([\s\S]*?)</div>'
        )
    return _DIV_RE_CACHE[cls]


def _text_of(li_html: str, cls: str) -> str:
    """li 中指定 div class 的纯文本（去标签、空白压缩为单空格）。找不到 → 空串。"""
    m = _div_re(cls).search(li_html)
    if m is None:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(1))).strip()


def extract_li_blocks(html: str) -> list[str]:
    """从链家成交页 HTML 提取每条成交的 li 原始块。

    找不到 ``ul.listContent`` → ``ValueError``（页面结构漂移，防静默空采）。
    """
    m = _LIST_UL_RE.search(html)
    if m is None:
        raise ValueError("链家成交页 HTML 缺少 ul.listContent（页面结构漂移或非成交页）")
    return _LI_RE.findall(m.group(0))


def _li_to_txt_lines(li_html: str) -> list[str]:
    """把 li 的 div 字段拼接为 WP4-B TXT 行格式（parse_lianjia_txt 可解析）。

    拼接规则与 WP4-B 记录块一致（见 test_parsers.py 的 _RESIDENTIAL）：
    - 行1 title：``小区名 户型 面积平米``（或 ``小区名 车位``）；
    - 行2：``朝向 | 装修 + 成交日 + 总价万``；
    - 行3：``楼层(共N层) 年代年 楼型 单价元/平``；
    - 行4：``满N年近地铁``（特征，可为空）；
    - 行5：``挂牌X万成交周期N天``；
    - 行6：``经纪人免费咨询``。
    """
    title = _text_of(li_html, "title")
    house_info = _text_of(li_html, "houseInfo")  # 朝向 | 装修
    deal_date = _text_of(li_html, "dealDate")  # 2026.05.22
    total_price = _text_of(li_html, "totalPrice")  # 140万
    position_info = _text_of(li_html, "positionInfo")  # 低楼层(共29层) 2003年塔楼
    unit_price = _text_of(li_html, "unitPrice")  # 25807元/平
    deal_house = _text_of(li_html, "dealHouseInfo")  # 房屋满五年近地铁
    deal_cycle = _text_of(li_html, "dealCycleeInfo")  # 挂牌145万成交周期271天
    agent = _text_of(li_html, "agentInfoList")  # 黄水钦免费咨询

    if not title:
        raise ValueError("li 缺少 title（无法识别小区/户型起始行）")

    # 车位记录：houseInfo 可能为"朝向 | 装修"，其余字段照常
    lines = [title]
    if house_info:
        lines.append(f"{house_info}{deal_date}{total_price}")
    else:
        # 无 houseInfo（极少数异常行）→ 保留空行占位，由 parse_lianjia_txt 记 unparsed
        lines.append(f"{deal_date}{total_price}")
    if position_info:
        lines.append(f"{position_info}{unit_price}")
    elif unit_price:
        lines.append(unit_price)
    if deal_house:
        lines.append(deal_house)
    if deal_cycle:
        lines.append(deal_cycle)
    if agent:
        lines.append(agent)
    return lines


def parse_lianjia_html(html: str, community: str | None = None) -> list[LianjiaRecord]:
    """从链家成交页 HTML 解析为规范化记录（独立于 fetch_log 的解析路径）。

    ``community`` 若给出，作为源小区名；否则记录中的 community 取自 li
    title 首词（链家页面 title 即 ``小区名 户型 面积平米``）。
    """
    records: list[LianjiaRecord] = []
    for li_html in extract_li_blocks(html):
        txt_lines = _li_to_txt_lines(li_html)
        parsed = parse_lianjia_txt(txt_lines)
        if not parsed:
            continue
        rec = parsed[0]
        if community is not None:
            rec.community = community
        records.append(rec)
    return records


# --------------------------------------------------------------------------
# 结构化 CSV（LJ-D 导入源）
# --------------------------------------------------------------------------

#: 链家成交 CSV 列（LianjiaRecord 字段面；面积/总价/单价数值化，缺失留空）。
CSV_COLUMNS: Final[tuple[str, ...]] = (
    "community",
    "layout",
    "area_m2",
    "deal_date",
    "total_price_wan",
    "unit_price_yuan_m2",
    "orientation",
    "decoration",
    "floor",
    "total_floors",
    "year_built",
    "building_type",
    "features",
    "listing_price_wan",
    "listing_period_days",
    "agent",
)

_MISSING_WORDS = {"UNKNOWN", "MISSING", "NOT_APPLICABLE", "PARSE_FAILURE", "CONFLICT"}


def _wan(yuan: object) -> str:
    """元整数 → 万文本（CSV 保留万口径与房天下一致）；空/None → 空串。"""
    if yuan is None:
        return ""
    try:
        d = Decimal(str(yuan)) / Decimal("10000")
        # 普通十进制格式（不输出科学计数法）；仅小数尾随 0 才裁剪
        text = format(d.normalize(), "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    except Exception:  # noqa: BLE001 - CSV 导出宽容处理非法值
        return ""


def _features_text(features: tuple[str, ...] | None) -> str:
    return "|".join(features) if features else ""


def lianjia_records_to_csv_rows(
    records: list[LianjiaRecord],
) -> list[dict[str, str]]:
    """规范化记录 → CSV 行（每行一个 dict，缺失字段留空串，不用 0）。"""
    rows: list[dict[str, str]] = []
    for r in records:
        rows.append(
            {
                "community": r.community or "",
                "layout": r.layout if r.layout not in _MISSING_WORDS else "",
                "area_m2": str(r.area_sqm) if r.area_sqm is not None else "",
                "deal_date": r.deal_date.isoformat() if r.deal_date else "",
                "total_price_wan": _wan(r.total_price_yuan),
                "unit_price_yuan_m2": (
                    str(r.unit_price_observed)
                    if r.unit_price_observed is not None
                    else ""
                ),
                "orientation": r.orientation if r.orientation not in _MISSING_WORDS else "",
                "decoration": r.decoration if r.decoration not in _MISSING_WORDS else "",
                "floor": r.floor if r.floor not in _MISSING_WORDS else "",
                "total_floors": str(r.total_floors) if r.total_floors is not None else "",
                "year_built": str(r.year_built) if r.year_built is not None else "",
                "building_type": r.building_type or "",
                "features": _features_text(r.features),
                "listing_price_wan": _wan(r.listing_price_yuan),
                "listing_period_days": (
                    str(r.listing_period_days)
                    if r.listing_period_days is not None
                    else ""
                ),
                "agent": r.agent if r.agent not in _MISSING_WORDS else "",
            }
        )
    return rows


def write_lianjia_csv(
    records: list[LianjiaRecord],
    *,
    out_path: Path,
) -> Path:
    """写链家成交结构化 CSV（LJ-D 导入源）。返回输出路径。"""
    import csv

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(lianjia_records_to_csv_rows(records))
    return out_path


def parse_lianjia_csv_table(
    table: pa.Table,
    community: str,
) -> list[LianjiaRecord]:
    """解析 LJ-D 导入后的链家成交 CSV 快照表为规范化记录（供 LJ-E 合并）。

    缺失/非法值语义与 WP4-B 一致：面积/总价/单价空 → None；日期不可解析
    → 精度 UNKNOWN；``raw_start_line`` = CSV 物理行号（表头第 1 行，数据从
    第 2 行起）。
    """
    from datetime import date as _date

    required = [c for c in CSV_COLUMNS if c not in table.column_names]
    if required:
        raise ValueError(f"链家成交快照缺少必要列: {', '.join(required)}")

    records: list[LianjiaRecord] = []
    cols = {name: table.column(name).to_pylist() for name in CSV_COLUMNS}
    n = table.num_rows
    for idx in range(n):
        layout = str(cols["layout"][idx] or "").strip()
        rec = LianjiaRecord(
            community=community or str(cols["community"][idx] or "").strip(),
            layout=layout or MissingSemantics.UNKNOWN.value,
            raw_start_line=2 + idx,
        )
        # 车位语义与 HTML 解析路径对齐（_make_record）：layout=="车位" → 面积不适用
        if rec.layout == "车位":
            rec.area_state = MissingSemantics.NOT_APPLICABLE

        area_raw = cols["area_m2"][idx]
        if area_raw not in (None, ""):
            try:
                area = Decimal(str(area_raw))
                if area > 0:
                    rec.area_sqm = area
            except (InvalidOperation, ValueError):
                rec.area_state = MissingSemantics.PARSE_FAILURE

        date_raw = cols["deal_date"][idx]
        if date_raw not in (None, ""):
            text = str(date_raw).strip()
            try:
                rec.deal_date = _date.fromisoformat(text)
                rec.deal_date_precision = EventDatePrecision.DAY
            except ValueError:
                rec.deal_date_precision = EventDatePrecision.UNKNOWN

        total_wan = cols["total_price_wan"][idx]
        if total_wan not in (None, ""):
            try:
                total = Decimal(str(total_wan)) * Decimal("10000")
                if total > 0:
                    rec.total_price_yuan = total.to_integral_value()
                    rec.original_price_text = f"{Decimal(str(total_wan)).normalize():f}万"
            except (InvalidOperation, ValueError):
                pass

        unit_raw = cols["unit_price_yuan_m2"][idx]
        if unit_raw not in (None, ""):
            try:
                u = Decimal(str(unit_raw))
                if u > 0:
                    rec.unit_price_observed = u.to_integral_value()
            except (InvalidOperation, ValueError):
                pass

        if rec.area_sqm and rec.total_price_yuan:
            rec.unit_price_derived = (
                rec.total_price_yuan / rec.area_sqm
            ).to_integral_value()
            rec.unit_price_formula = "total_price_yuan / area_sqm, rounded to integer"

        for field, _default in (
            ("orientation", MissingSemantics.UNKNOWN.value),
            ("decoration", MissingSemantics.UNKNOWN.value),
            ("floor", MissingSemantics.UNKNOWN.value),
            ("building_type", None),
            ("agent", MissingSemantics.UNKNOWN.value),
        ):
            val = str(cols[field][idx] or "").strip()
            if val:
                setattr(rec, field, val)

        tf = cols["total_floors"][idx]
        if tf not in (None, ""):
            with suppress(TypeError, ValueError):
                rec.total_floors = int(tf)
        yb = cols["year_built"][idx]
        if yb not in (None, ""):
            try:
                rec.year_built = int(yb)
            except (TypeError, ValueError):
                rec.year_state = MissingSemantics.MISSING
        lp = cols["listing_price_wan"][idx]
        if lp not in (None, ""):
            try:
                v = Decimal(str(lp)) * Decimal("10000")
                if v > 0:
                    rec.listing_price_yuan = v.to_integral_value()
            except (InvalidOperation, ValueError):
                pass
        lpd = cols["listing_period_days"][idx]
        if lpd not in (None, ""):
            with suppress(TypeError, ValueError):
                rec.listing_period_days = int(lpd)
        feats = str(cols["features"][idx] or "").strip()
        rec.features = tuple(f for f in feats.split("|") if f)

        rec.source_record_id = _fingerprint(rec)
        records.append(rec)
    return records


def _fingerprint(record: LianjiaRecord) -> str:
    """确定性指纹：同一交易身份（小区/面积/成交日/总价/户型）同指纹。"""
    import hashlib

    payload = "|".join(
        [
            record.community,
            record.layout,
            str(record.area_sqm or ""),
            record.deal_date.isoformat() if record.deal_date else "",
            str(record.total_price_yuan or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# 交叉校验（HTML ↔ fetch_log rows）
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CrosscheckResult:
    """HTML 解析记录与 fetch_log rows 解析记录的比对结果。"""

    html_records: int
    log_records: int
    missing_from_log: int  # 在 HTML 但不在 fetch_log
    missing_from_html: int  # 在 fetch_log 但不在 HTML
    ok: bool


def _record_key(rec: LianjiaRecord) -> tuple[str, str, str, str, str]:
    """五元组身份键：小区/户型/面积/成交日/总价。任一缺失 → 用 '' 占位。"""
    return (
        rec.community,
        rec.layout,
        str(rec.area_sqm) if rec.area_sqm is not None else "",
        rec.deal_date.isoformat() if rec.deal_date else "",
        str(rec.total_price_yuan) if rec.total_price_yuan is not None else "",
    )


def crosscheck_html_vs_log(
    html_records: list[LianjiaRecord],
    log_records: list[LianjiaRecord],
) -> CrosscheckResult:
    """HTML 解析记录 vs fetch_log rows 解析记录双向比对。

    一致标准：两边五元组集合完全相等（``missing_from_log == 0`` 且
    ``missing_from_html == 0``）。两边数量可因重复记录去重集合后不同，
    以**集合**比对（同一交易多录仅计一次身份，避免重复噪声）。
    """
    html_set = {_record_key(r) for r in html_records}
    log_set = {_record_key(r) for r in log_records}
    return CrosscheckResult(
        html_records=len(html_records),
        log_records=len(log_records),
        missing_from_log=len(html_set - log_set),
        missing_from_html=len(log_set - html_set),
        ok=(html_set == log_set),
    )


__all__ = [
    "CSV_COLUMNS",
    "CrosscheckResult",
    "PARSER_VERSION",
    "crosscheck_html_vs_log",
    "extract_li_blocks",
    "lianjia_records_to_csv_rows",
    "parse_lianjia_csv_table",
    "parse_lianjia_html",
    "write_lianjia_csv",
]
