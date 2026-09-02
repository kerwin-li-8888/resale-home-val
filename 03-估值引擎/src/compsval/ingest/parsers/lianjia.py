"""Chain-house (链家) Hecheng-list TXT parser (WP4-B).

Parses the raw text saved by WP3-A/WP2 as ``lianjia.com.example/chengjiao/targetdistrict/``
``.listContent`` ``innerText`` (``source=lianjia/dataset=chengjiao_list``). The
file is a header (4 lines) followed by variable-line record blocks:

- record start: ``<community> <layout> <area>平米`` or ``<community> 车位``
- orientation/date/price: ``<orientation> | <decoration><YYYY.MM.DD><price>万``
- floor/age/type/unit: ``<floor>(共<N>层)? <cycles>: (NNNN年)?(塔楼|板楼)?<unit>元/平``
- features: ``房屋满<N>年近地铁`` / ``近地铁``
- listing: ``挂牌<price>万成交周期<N>天``
- agent: ``<name>免费咨询``

Standardization rules (数据字典 §1 / §5, missing discipline):
- dates ``2026.07.21`` -> ``date`` with DAY precision;
- area "平米" and money "万" -> :class:`decimal.Decimal`; ``245万`` = 2450000 元;
- derived unit price = ``total_price_yuan / area_sqm`` (rounded), recorded with
  its formula; the observed ``元/平`` is kept separately;
- missing semantics are explicit: parking layout -> area NOT_APPLICABLE,
  ``暂无数据`` year -> MISSING, absent optional fields -> None / UNKNOWN. ``0``
  is never used for a missing value.

No cleaning/dedup/flagging happens here (that is WP4-C); every raw line is
preserved via ``raw_start_line`` so deletions are always traceable.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from compsval.contract.models import EventDatePrecision, MissingSemantics

PARSER_VERSION = "lianjia-tx.v1"

# --- record-start: "<community> <layout> <area>平米"  (住宅) --------------
_CYCLE_RES = (
    re.compile(r"^(?P<community>.+?)\s+(?P<layout>\d+室\d+厅)\s+(?P<area>[\d.]+)平米$"),
    re.compile(r"^(?P<community>.+?)\s+(?P<layout>车位)$"),
)
_ORIENTATION_RE = re.compile(
    r"^(?P<orientation>[东南西北\s]+?)\s*\|\s*"
    r"(?P<decoration>[\U00004e00-\U00009fff]+)"
    r"(?P<date>\d{4}\.\d{2}\.\d{2})"
    r"\s*(?P<price>[\d.]+)万$"
)
_FLOOR_RE = re.compile(
    r"^(?P<floor>[^\s()]+)(?:\(共(?P<total>\d+)层\))?\s+"
    r"(?:(?P<year>\d{4})年)?"
    r"(?P<year_flag>暂无数据)?"
    r"(?P<building>塔楼|板楼|平房)?"
    r"(?P<unit>[\d.]+)元/平$"
)
_FEATURE_RE = re.compile(
    r"^(?:房屋满(?P<years>[0-9一二两三四五六七八九十百]+)年)?(?P<metro>近地铁)?$"
)
_LISTING_RE = re.compile(r"^挂牌(?P<price>[\d.]+)万成交周期(?P<days>\d+)天$")
_AGENT_RE = re.compile(r"^(?P<agent>.*?)免费咨询$")

_CN_NUMERALS = {
    "一": 1, "两": 2, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
}
_NUMERIC_NUMERALS = {str(n): n for n in range(10)}


def _chinese_years(text: str) -> int | None:
    """Parse "满五年"-style Chinese or Arabic numerals to an int."""
    if not text:
        return None
    if any(ch.isdigit() for ch in text):
        try:
            return int(text)
        except ValueError:
            return None
    total = 0
    for ch in text:
        if ch in _CN_NUMERALS:
            total = total * 10 + _CN_NUMERALS[ch]
        elif ch in _NUMERIC_NUMERALS:
            total = total * 10 + _NUMERIC_NUMERALS[ch]
        else:
            return None
    return total or None


def _to_yuan(text: str) -> Decimal:
    """``245`` / ``48.5`` (''万'' already stripped) -> integer yuan."""
    return (Decimal(text) * 10000).to_integral_value()


_EN_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")


def _parse_date(text: str) -> date:
    return date(*map(int, text.split(".")))


@dataclass
class LianjiaRecord:
    """One normalized chain-house listing/transaction record (WP4-B output)."""

    community: str
    layout: str
    area_sqm: Decimal | None = None
    area_state: MissingSemantics | None = None
    orientation: str = MissingSemantics.UNKNOWN.value
    decoration: str = MissingSemantics.UNKNOWN.value
    deal_date: date | None = None
    deal_date_precision: EventDatePrecision = EventDatePrecision.UNKNOWN
    total_price_yuan: Decimal | None = None
    original_price_text: str = ""
    unit_price_observed: Decimal | None = None
    unit_price_derived: Decimal | None = None
    unit_price_formula: str = ""
    floor: str = MissingSemantics.UNKNOWN.value
    total_floors: int | None = None
    year_built: int | None = None
    year_state: MissingSemantics | None = None
    building_type: str | None = None
    features: tuple[str, ...] = ()
    listing_price_yuan: Decimal | None = None
    listing_period_days: int | None = None
    agent: str = MissingSemantics.UNKNOWN.value
    unparsed_lines: tuple[str, ...] = ()
    raw_start_line: int | None = None
    source_record_id: str = MissingSemantics.UNKNOWN.value


def _fingerprint(record: LianjiaRecord) -> str:
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


def _make_record(start: re.Match[str], start_line: int) -> LianjiaRecord:
    community = start.group("community").strip()
    layout = start.group("layout")
    rec = LianjiaRecord(community=community, layout=layout, raw_start_line=start_line)
    if layout == "车位":
        rec.area_state = MissingSemantics.NOT_APPLICABLE
    else:
        rec.area_sqm = Decimal(start.group("area"))
    if community.endswith("(住宅)"):
        community = community[: -len("(住宅)")].rstrip()
        rec.community = community
    return rec


def parse_line(record: LianjiaRecord, line: str) -> None:
    """Apply one attribute line to the open record (assumes line already matched)."""
    if (m := _ORIENTATION_RE.match(line)) is not None:
        record.orientation = m.group("orientation").strip() or MissingSemantics.UNKNOWN.value
        record.decoration = m.group("decoration")
        record.deal_date = _parse_date(m.group("date"))
        record.deal_date_precision = EventDatePrecision.DAY
        price_text = m.group("price")
        record.original_price_text = f"{price_text}万"
        record.total_price_yuan = _to_yuan(price_text)
        if record.area_sqm and record.total_price_yuan:
            record.unit_price_derived = (
                record.total_price_yuan / record.area_sqm
            ).to_integral_value()
            record.unit_price_formula = "total_price_yuan / area_sqm, rounded to integer"
    elif (m := _FLOOR_RE.match(line)) is not None:
        record.floor = m.group("floor")
        if m.group("total") is not None:
            record.total_floors = int(m.group("total"))
        if m.group("year"):
            record.year_built = int(m.group("year"))
            # 年份有值 = 已登记；无显式 MissingSemantics 标记（None 表示已知）
        else:
            record.year_state = MissingSemantics.MISSING
        record.building_type = m.group("building")
        record.unit_price_observed = Decimal(m.group("unit"))
        if record.layout != "车位" and not record.unit_price_formula:
            record.unit_price_formula = "total_price_yuan / area_sqm, rounded to integer"
    elif _LISTING_RE.match(line) is not None:
        lm = _LISTING_RE.match(line)
        assert lm is not None
        record.listing_price_yuan = _to_yuan(lm.group("price"))
        record.listing_period_days = int(lm.group("days"))
    elif _AGENT_RE.match(line) is not None:
        am = _AGENT_RE.match(line)
        assert am is not None
        record.agent = am.group("agent").strip() or MissingSemantics.UNKNOWN.value
    elif _FEATURE_RE.match(line) is not None:
        fm = _FEATURE_RE.match(line)
        assert fm is not None
        if fm.group("years"):
            years = _chinese_years(fm.group("years"))
            if years:
                record.features = (*record.features, f"满{years}年")
        if fm.group("metro"):
            record.features = (*record.features, "近地铁")
    else:
        # 无法归入任何已知字段行 → 记录原行供质量报告计数（解析失败可区分，
        # 不静默丢弃也不臆造字段，见验收标准③⑤）
        record.unparsed_lines = (*record.unparsed_lines, line)


def _finalize(record: LianjiaRecord) -> LianjiaRecord:
    if record.unit_price_observed is None:
        record.unit_price_formula = ""
    record.source_record_id = _fingerprint(record)
    return record


def parse_lianjia_txt(lines: list[str]) -> list[LianjiaRecord]:
    """Parse the chain-house Hecheng-list TXT (one element per raw line) into records.

    Attribute lines are matched in order and folded into the current open record;
    a new ``<community> <layout> <area>平米`` / ``<community> 车位`` line closes the
    previous record. Unmatched lines are ignored silently (header / agent spacing),
    but any line that looks like a record start opens a new record.
    """
    records: list[LianjiaRecord] = []
    current: LianjiaRecord | None = None
    for idx, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue
        start = None
        for res in _CYCLE_RES:
            match = res.match(line)
            if match is not None:
                start = match
                break
        if start is not None:
            if current is not None:
                records.append(_finalize(current))
            current = _make_record(start, idx)
            continue
        if current is None:
            continue
        parse_line(current, line)
    if current is not None:
        records.append(_finalize(current))
    return records