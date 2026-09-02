"""外部链家成交 staged 表属性列标准化（excel-attribute-enrichment 任务①）。

从既有 staged 两表（``lianjia_ext_sale_record`` / ``lianjia_ext_ordinary_residential``）
的原文列（``floor_raw`` / ``decoration`` / ``built_year_raw`` / ``has_elevator_raw``）
只读派生标准化属性列：

- ``floor_bucket``（楼层段位）+ ``total_floors``（总层数 int）：楼层原文为
  「段位/N层」合并文本（探针实测，无精确所在楼层），只拆分不臆造精确楼层
  （数据字典 §1：原文无法归位 → PARSE_FAILURE，不用 0/估算值充当）；
- ``year_built``（建成年代 int）：「YYYY年」→ 年份，范围合理性校验；
- ``has_elevator``（bool）：仅取来源披露值（有→True / 无→False）；
  字典 §3.5 的「总层数>7 推断」口径**不**在本模块使用（披露值优先，不混用）；
- ``decoration_norm``（精装/简装/毛坯/其他）：枚举归一，词表外 → PARSE_FAILURE。

缺失语义（数据字典 §1）：空/「暂无/暂无数据」→ ``MISSING``（落表 None）；
原文存在但无法解析 → ``PARSE_FAILURE``（入质量报告分布）；绝不补造、绝不以 0
充当缺失。原文列逐字节保留，标准化列与其并存可互相核对。

朝向不新增标准化列：原文即 raw 值，估值侧按字典 §5 两值面分工做归组
（``valuation.comparable.orientation_group``）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from compsval.ingest.xlsx_parse import FieldParseStatus

#: 属性标准化规则版本（随 DerivedManifest.parser_version 落盘，可追溯）。
ATTRIBUTES_RULE_VERSION = "XLSX-ATTR-1.0"

#: 「来源未披露/暂无」占位（与 xlsx_parse.NON_ASSERTED 一致 + 空串）。
_PLACEHOLDERS = {"", "暂无", "暂无数据", "None", "UNKNOWN"}

#: 链家披露的楼层段位词表；词表外原文按 PARSE_FAILURE 如实失败，不臆造归类。
FLOOR_BUCKETS = ("低楼层", "中楼层", "高楼层", "顶层", "底层", "地下室")

#: 建成年代合理范围（四位年，越界按 PARSE_FAILURE）。
_YEAR_MIN = 1800
_YEAR_MAX = 2999

#: 装修枚举词表（探针实测 恰为四值；词表外如实失败）。
DECORATION_VALUES = ("精装", "简装", "毛坯", "其他")

#: 标准化列名（追加在原文列之后；原文列不动）。
ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "floor_bucket",
    "total_floors",
    "year_built",
    "has_elevator",
    "decoration_norm",
    "floor_status",
    "year_built_status",
    "has_elevator_status",
    "decoration_status",
)

_ATTRIBUTE_INT_COLUMNS = frozenset({"total_floors", "year_built"})
_ATTRIBUTE_BOOL_COLUMNS = frozenset({"has_elevator"})


def _clean(text: str) -> str:
    return text.strip()


def _is_missing(text: str) -> bool:
    return _clean(text) in _PLACEHOLDERS


def parse_floor(raw: str) -> tuple[str | None, int | None, FieldParseStatus]:
    """楼层原文「段位/N层」→ (floor_bucket, total_floors, status)。

    - 空/暂无 → ``(None, None, MISSING)``；
    - 段位或总层数任一无法解析 → 可解析部分保留、整体 ``PARSE_FAILURE``
      （逐字段如实：不臆造缺失侧的值）；
    - 段位必须在 :data:`FLOOR_BUCKETS` 词表内（词表外如实失败，不归类）。
    """
    if _is_missing(raw):
        return None, None, FieldParseStatus.MISSING
    text = _clean(raw)
    left, sep, right = text.partition("/")
    bucket: str | None = None
    total: int | None = None
    failed = False
    bucket_text = _clean(left)
    if bucket_text in FLOOR_BUCKETS:
        bucket = bucket_text
    else:
        failed = True
    total_text = _clean(right)
    if sep and total_text.endswith("层"):
        total_text = total_text[:-1].strip()
    if total_text.startswith("共"):
        total_text = total_text[1:].strip()
    if total_text.isdigit():
        value = int(total_text)
        if value > 0:
            total = value
        else:
            failed = True
    else:
        failed = True
    if failed:
        return bucket, total, FieldParseStatus.PARSE_FAILURE
    return bucket, total, FieldParseStatus.PARSED


def parse_year(raw: str) -> tuple[int | None, FieldParseStatus]:
    """建成时间原文「YYYY年」→ (year_built, status)；越界/非数字 → PARSE_FAILURE。"""
    if _is_missing(raw):
        return None, FieldParseStatus.MISSING
    text = _clean(raw)
    if text.endswith("年"):
        text = text[:-1].strip()
    if not text.isdigit():
        return None, FieldParseStatus.PARSE_FAILURE
    value = int(text)
    if not _YEAR_MIN <= value <= _YEAR_MAX:
        return None, FieldParseStatus.PARSE_FAILURE
    return value, FieldParseStatus.PARSED


def parse_elevator(raw: str) -> tuple[bool | None, FieldParseStatus]:
    """是否有电梯原文 → (has_elevator, status)；仅认披露值 有/无，不推断。"""
    if _is_missing(raw):
        return None, FieldParseStatus.MISSING
    text = _clean(raw)
    if text == "有":
        return True, FieldParseStatus.PARSED
    if text == "无":
        return False, FieldParseStatus.PARSED
    return None, FieldParseStatus.PARSE_FAILURE


def norm_decoration(raw: str) -> tuple[str | None, FieldParseStatus]:
    """装修情况原文归一（精装/简装/毛坯/其他）；词表外 → PARSE_FAILURE。"""
    if _is_missing(raw):
        return None, FieldParseStatus.MISSING
    text = _clean(raw)
    if text in DECORATION_VALUES:
        return text, FieldParseStatus.PARSED
    return None, FieldParseStatus.PARSE_FAILURE


def _status_counts() -> dict[str, int]:
    return {status.value: 0 for status in FieldParseStatus}


@dataclass
class AttributesSummary:
    """属性标准化质量摘要（覆盖率/缺失/解析失败分布，入 v2 质量报告）。

    可变累加器：:func:`normalize_attributes_table` 逐行计数后返回。
    """

    row_count: int
    floor_status: dict[str, int] = field(default_factory=_status_counts)
    year_built_status: dict[str, int] = field(default_factory=_status_counts)
    has_elevator_status: dict[str, int] = field(default_factory=_status_counts)
    decoration_status: dict[str, int] = field(default_factory=_status_counts)
    #: 楼层联合解析失败行数（段位或总层数任一 PARSE_FAILURE）。
    floor_joint_failure: int = 0
    #: 朝向原文非缺失行数（朝向不新增标准化列，只报覆盖）。
    orientation_known: int = 0

    def coverage(self) -> dict[str, float]:
        """各属性列 PARSED 覆盖率（0-1；row_count=0 时全 0.0）。"""
        if self.row_count == 0:
            return {name: 0.0 for name in _COVERAGE_FIELDS}
        counts = {
            "floor": self.floor_status,
            "year_built": self.year_built_status,
            "has_elevator": self.has_elevator_status,
            "decoration": self.decoration_status,
        }
        return {
            name: bucket.get(FieldParseStatus.PARSED.value, 0) / self.row_count
            for name, bucket in counts.items()
        }

    def to_dict(self) -> dict[str, object]:
        """JSON 可序列化形态（质量报告落盘用）。"""
        return {
            "rule_version": ATTRIBUTES_RULE_VERSION,
            "row_count": self.row_count,
            "floor_status": dict(self.floor_status),
            "year_built_status": dict(self.year_built_status),
            "has_elevator_status": dict(self.has_elevator_status),
            "decoration_status": dict(self.decoration_status),
            "floor_joint_failure": self.floor_joint_failure,
            "orientation_known": self.orientation_known,
            "coverage": self.coverage(),
        }


_COVERAGE_FIELDS = ("floor", "year_built", "has_elevator", "decoration")


def _attribute_field_type(name: str) -> pa.DataType:
    if name in _ATTRIBUTE_INT_COLUMNS:
        return pa.int64()
    if name in _ATTRIBUTE_BOOL_COLUMNS:
        return pa.bool_()
    return pa.string()


def _drop_attribute_columns(table: pa.Table) -> pa.Table:
    """防御性去掉已有标准化列（源应为 v1 表；重复派生不叠加列）。"""
    drop = [name for name in ATTRIBUTE_COLUMNS if name in table.column_names]
    return table.drop_columns(drop) if drop else table


def normalize_attributes_table(table: pa.Table) -> tuple[pa.Table, AttributesSummary]:
    """从 staged 表只读派生标准化属性列（原文列逐字节不变）。

    返回 ``(扩列表, AttributesSummary)``；输入表不被修改。原文列缺失时抛
    ``KeyError``（调用方应保证源为 staged v1 两表）。
    """
    work = _drop_attribute_columns(table)
    floors = [parse_floor(str(v)) for v in work.column("floor_raw").to_pylist()]
    years = [parse_year(str(v)) for v in work.column("built_year_raw").to_pylist()]
    elevators = [parse_elevator(str(v)) for v in work.column("has_elevator_raw").to_pylist()]
    decorations = [norm_decoration(str(v)) for v in work.column("decoration").to_pylist()]
    orientations = [str(v) for v in work.column("orientation").to_pylist()]

    columns: dict[str, list[object]] = {
        "floor_bucket": [item[0] for item in floors],
        "total_floors": [item[1] for item in floors],
        "year_built": [item[0] for item in years],
        "has_elevator": [item[0] for item in elevators],
        "decoration_norm": [item[0] for item in decorations],
        "floor_status": [item[2].value for item in floors],
        "year_built_status": [item[1].value for item in years],
        "has_elevator_status": [item[1].value for item in elevators],
        "decoration_status": [item[1].value for item in decorations],
    }
    out = work
    for name in ATTRIBUTE_COLUMNS:
        out = out.append_column(
            pa.field(name, _attribute_field_type(name), nullable=True),
            pa.array(columns[name], type=_attribute_field_type(name)),
        )

    summary = AttributesSummary(row_count=out.num_rows)
    for floor_item in floors:
        summary.floor_status[floor_item[2].value] += 1
        if floor_item[2] is FieldParseStatus.PARSE_FAILURE:
            summary.floor_joint_failure += 1
    for year_item in years:
        summary.year_built_status[year_item[1].value] += 1
    for elevator_item in elevators:
        summary.has_elevator_status[elevator_item[1].value] += 1
    for decoration_item in decorations:
        summary.decoration_status[decoration_item[1].value] += 1
    for text in orientations:
        if not _is_missing(text):
            summary.orientation_known += 1
    return out, summary


__all__ = [
    "ATTRIBUTES_RULE_VERSION",
    "ATTRIBUTE_COLUMNS",
    "AttributesSummary",
    "DECORATION_VALUES",
    "FLOOR_BUCKETS",
    "normalize_attributes_table",
    "norm_decoration",
    "parse_elevator",
    "parse_floor",
    "parse_year",
]
