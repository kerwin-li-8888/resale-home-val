"""Minimum data-contract models (数据字典-V0.1 §1-§3).

Implements the four WP3-D entities — ``source_registry``, ``raw_snapshot``,
``sale_event``, ``listing_event`` — together with the common evidence fields
(§2) and the missing-semantics enum (§1). WP5-A adds the four domain entity
models the dictionary records: ``community``, ``community_alias``, ``building``
and ``market_series`` (§3.3/§3.4/§3.5/§3.8); the remaining WP6 entities
(``subject_property`` … ``outcome_event``) keep their dictionary calibre but are
implemented in WP6.

Missing-value discipline (§7.3): text/enum fields use the explicit
``UNKNOWN``/``NOT_APPLICABLE`` codes when a value is absent; numeric fields
(amounts, areas, floors, coordinates, year) use ``None`` for unknown and a real
non-zero value for known values — unknown is never written as ``0``.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

SHA256_PATTERN = r"^[0-9a-fA-F]{64}$"


# ---- §1 缺失语义（未知/缺失/不适用/解析失败/冲突 与 0 严格分开） ----
class MissingSemantics(StrEnum):
    UNKNOWN = "UNKNOWN"
    MISSING = "MISSING"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    PARSE_FAILURE = "PARSE_FAILURE"
    CONFLICT = "CONFLICT"


# ---- §2 通用证据字段枚举 ----
class EventDatePrecision(StrEnum):
    DAY = "DAY"
    MONTH = "MONTH"
    UNKNOWN = "UNKNOWN"


class VerificationStatus(StrEnum):
    UNVERIFIED = "UNVERIFIED"
    VERIFIED = "VERIFIED"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


# ---- §3.1 source_registry 枚举 ----
class SourceRole(StrEnum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


class SourceGranularity(StrEnum):
    SALE_UNIT = "逐套成交"
    AGGREGATE_SALE = "聚合成交"
    LISTING = "挂牌"
    COMMUNITY = "小区"
    OTHER = "其他"


class SourceUpdateFrequency(StrEnum):
    CONTINUOUS = "持续"
    MONTHLY = "月度"
    ON_LISTING = "随放盘"
    UNKNOWN = "未知"


class SourceAccessCondition(StrEnum):
    PUBLIC = "公开"
    LOGIN = "登录"
    CAPTCHA = "验证码"
    APP = "App"
    PAID = "付费"
    UNKNOWN = "未知"


class SourceAcquisitionMethod(StrEnum):
    BROWSE = "浏览"
    DOWNLOAD = "下载"
    QUERY_API = "查询接口"
    MANUAL = "人工配合"
    UNKNOWN = "未知"


class SourceRepeatability(StrEnum):
    AUTOMATIC = "可重复取得"
    CONDITIONAL = "有条件"
    UNSUSTAINABLE = "不可持续"
    UNKNOWN = "未知"


class SourceStatus(StrEnum):
    CANDIDATE = "候选"
    REGISTERED = "已登记"
    ACTIVE = "启用"
    DISABLED = "停用"


# ---- §3.2 raw_snapshot 枚举 ----
class SnapshotFormat(StrEnum):
    """快照格式。

    EXTFP0-B（2026-08-24）以向后兼容方式扩展：新增 XLSX/JPEG/WEBP/TIFF/BINARY，
    旧成员与值保持不变（技术方案 §5.2）。
    """

    PARQUET = "parquet"
    PNG = "png"
    TXT = "txt"
    HTML = "html"
    JSON = "json"
    XLSX = "xlsx"
    JPEG = "jpeg"
    WEBP = "webp"
    TIFF = "tiff"
    BINARY = "binary"
    OTHER = "其他"


class SnapshotParseStatus(StrEnum):
    NOT_PARSED = "未解析"
    PARSED = "已解析"
    PARSE_FAILED = "解析失败"
    PARTIAL_FAILURE = "部分失败"


# ---- §3.6 / §3.7 成交与挂牌枚举（严格分表） ----
class PriceBenchmark(StrEnum):
    ACTUAL_REGISTERED = "实际登记"
    PLATFORM_DISCLOSED = "平台披露"
    CONTRACT_FILED = "网签"
    INFERRED = "推断"
    UNKNOWN = "未知"


class ListingPriceBenchmark(StrEnum):
    OFFICIAL_LISTING = "官方挂牌"
    PLATFORM_LISTING = "平台挂牌"
    UNKNOWN = "未知"


class ListingStatus(StrEnum):
    ON_SALE = "在售"
    DELISTED = "下架"
    SOLD = "已成交"
    UNKNOWN = "未知"


class AnomalyFlag(StrEnum):
    NORMAL = "正常"
    SUSPECT_PARKING = "疑似车位"
    SUSPECT_DUPLICATE = "疑似重复"
    SUSPECT_ABNORMAL_UNIT_PRICE = "疑似异常单价"
    OTHER = "其他"


# ---- §2 通用证据字段（sale_event / listing_event 公共基类） ----
class EvidenceRecord(BaseModel):
    """通用证据字段（数据字典 §2）：来源解析事实记录的公共基类。

    仅用于 sale_event / listing_event 等解析事实记录；raw_snapshot 为快照
    登记实体，按其 §3.2 独立字段集实现，不继承本基类（RV-WP3-C-01 F2）。
    """

    source_id: str = Field(
        ...,
        description="来源登记ID，对应 source_registry.source_id（如 SRC-005）",
    )
    source_record_id: str = Field(
        default="UNKNOWN", description="来源内部稳定ID；来源无稳定ID时=UNKNOWN"
    )
    snapshot_id: str = Field(
        ...,
        description="来源记录所属原始快照ID，对应 raw_snapshot.snapshot_id",
    )
    raw_locator: str = Field(
        default="UNKNOWN", description="快照内记录定位（行号/节点路径/URL）；缺失=UNKNOWN"
    )
    fetched_at: datetime = Field(..., description="系统取得时间（取自快照）")
    published_at: datetime | None = Field(default=None, description="来源公布时间；无则=UNKNOWN")
    event_date: date | None = Field(default=None, description="事件发生日（成交/挂牌/调价）")
    event_date_precision: EventDatePrecision = Field(
        default=EventDatePrecision.UNKNOWN, description="事件日期精度（中原仅到月→MONTH）"
    )
    verification_status: VerificationStatus = Field(
        default=VerificationStatus.UNVERIFIED,
        description="未验证/已核验/有冲突/已拒绝",
    )
    parser_version: str = Field(..., description="生成该记录所用解析器版本")
    content_hash: str = Field(
        ..., pattern=SHA256_PATTERN, description="原始内容或记录指纹（sha256 十六进制）"
    )


# ---- §3.1 source_registry ----
class SourceRegistry(BaseModel):
    """来源登记（数据字典 §3.1）。"""

    source_id: str = Field(..., description="唯一来源ID（如 SRC-005），契约主键")
    name: str = Field(..., description="来源名称（如 房天下·示例城市目标区·小区成交记录）")
    publisher: str = Field(default="UNKNOWN", description="发布主体")
    role: SourceRole = Field(..., description="第一阶段角色（逐套/市场序列/后续）")
    granularity: SourceGranularity = Field(..., description="数据粒度")
    entry_url: str = Field(default="UNKNOWN", description="入口地址；无则=UNKNOWN")
    price_benchmark: str = Field(
        default="UNKNOWN", description="价格口径（平台披露/官方挂牌/聚合/未知）"
    )
    update_frequency: SourceUpdateFrequency = Field(
        default=SourceUpdateFrequency.UNKNOWN, description="更新频率"
    )
    access_condition: SourceAccessCondition = Field(..., description="访问条件")
    acquisition_method: SourceAcquisitionMethod = Field(
        default=SourceAcquisitionMethod.UNKNOWN, description="获取方式"
    )
    repeatability: SourceRepeatability = Field(..., description="可重复性结论（DATA-002 输出）")
    usage_terms: str = Field(
        default="UNKNOWN", description="使用条款/限制链接；无则=UNKNOWN"
    )
    status: SourceStatus = Field(..., description="登记状态")
    registered_at: datetime = Field(..., description="登记时间")


# ---- §3.2 raw_snapshot（独立字段集，不继承通用证据字段） ----
class RawSnapshot(BaseModel):
    """不可变原始快照登记（数据字典 §3.2）。"""

    snapshot_id: str = Field(
        ...,
        description="唯一快照ID（如 {source}-{dataset}-{fetched_at}），契约主键",
    )
    source_id: str = Field(..., description="来源，FK→source_registry")
    dataset: str = Field(
        ..., description="数据集名（chengjiao/community_list/listing…）"
    )
    fetched_at: datetime = Field(..., description="系统取得时间")
    query: str = Field(
        default="UNKNOWN", description="查询条件或原始文件名（URL/关键词）；无则=UNKNOWN"
    )
    content_hash: str = Field(
        ..., pattern=SHA256_PATTERN, description="原始内容指纹（sha256）"
    )
    file_count: int = Field(..., ge=1, description="文件数")
    record_count: int = Field(default=0, ge=0, description="记录数；非表格文件=0（不适用）")
    format: SnapshotFormat = Field(..., description="快照格式")
    mime_type: str | None = Field(
        default=None,
        description=(
            "实际数据 MIME 类型（如 XLSX/图片）；未知或未记录=None"
            "（EXTFP0-B 新增，向后兼容）"
        ),
    )
    parse_status: SnapshotParseStatus = Field(
        default=SnapshotParseStatus.NOT_PARSED, description="解析状态"
    )
    failure_info: str = Field(
        default="NOT_APPLICABLE", description="失败信息；无则=NOT_APPLICABLE"
    )
    prev_snapshot_id: str = Field(
        default="UNKNOWN", description="上一快照引用（增量链）；无则=UNKNOWN"
    )


# ---- §3.6 sale_event（成交事件；与挂牌严格分表） ----
class SaleEvent(EvidenceRecord):
    """成交事件（数据字典 §3.6）。成交与挂牌分属不同模型，绝不合并。"""

    sale_event_id: str = Field(..., description="成交事件主键")
    community_id: str = Field(..., description="关联小区，FK→community")
    sale_date: date | None = Field(
        default=None, description="成交日期（精度在 event_date_precision）"
    )
    total_price_yuan: Decimal | None = Field(
        default=None, gt=0, description="成交总价，统一人民币元整数；未知=UNKNOWN（不得用0）"
    )
    original_price_text: str = Field(
        ..., description="原始成交价文本（如 150万）原样保留"
    )
    area_sqm: Decimal | None = Field(
        default=None, gt=0, description="建筑面积（平方米）；未知不能用0"
    )
    unit_price: Decimal | None = Field(
        default=None, gt=0, description="派生值：单价（元/㎡），记录计算公式与舍入"
    )
    unit_price_formula: str = Field(..., description="单价计算公式与舍入规则")
    layout: str = Field(default="UNKNOWN", description="户型；列表无→UNKNOWN")
    floor: int | None = Field(default=None, description="所在楼层；未知不得用0")
    total_floors: int | None = Field(default=None, description="总楼层；未知不得用0")
    orientation: str = Field(default="UNKNOWN", description="朝向；列表无→UNKNOWN")
    has_elevator: bool | None = Field(default=None, description="电梯；未知=UNKNOWN")
    price_benchmark: PriceBenchmark = Field(..., description="价格口径")
    listing_price_yuan: Decimal | None = Field(
        default=None, gt=0, description="成交时点关联挂牌价（链家）；与挂牌事件分离"
    )
    listing_period_days: int | None = Field(
        default=None, description="成交周期（链家）；其他源无→UNKNOWN"
    )
    anomaly_flag: AnomalyFlag = Field(
        default=AnomalyFlag.NORMAL, description="清洗阶段标记，原始记录不标"
    )


# ---- §3.7 listing_event（挂牌事件；与成交严格分离） ----
class ListingEvent(EvidenceRecord):
    """挂牌事件（数据字典 §3.7）。挂牌价不得静默作为成交价。"""

    listing_event_id: str = Field(..., description="挂牌事件主键")
    community_id: str = Field(..., description="关联小区，FK→community")
    listing_id: str = Field(
        default="UNKNOWN", description="来源房源ID；重复挂牌识别线索"
    )
    listing_date: date | None = Field(default=None, description="首次挂牌日期")
    price_yuan: Decimal | None = Field(
        default=None, gt=0, description="挂牌总价（官方=拟转让价）；未知不得用0"
    )
    price_adjustments: list[Decimal] = Field(
        default_factory=list, description="历次调价序列；无则=空表/UNKNOWN"
    )
    delist_date: date | None = Field(default=None, description="下架/成交状态日期")
    status: ListingStatus = Field(default=ListingStatus.UNKNOWN, description="挂牌状态")
    listing_days: int | None = Field(default=None, description="挂牌天数")
    price_benchmark: ListingPriceBenchmark = Field(
        ..., description="价格口径（官方挂牌/平台挂牌）"
    )


# ---- §3.3-§3.8 WP5 领域实体枚举 ----
class CoordinateSystem(StrEnum):
    WGS84 = "WGS84"
    GCJ02 = "GCJ02"
    BD09 = "BD09"
    UNKNOWN = "UNKNOWN"


class BoundaryStatus(StrEnum):
    """范围状态（DATA-001-C 边界三分，§3.3）。"""

    MACHINE_CONFIRMED = "机器确认"
    BOUNDARY_PENDING = "边界待定"
    OUT_OF_SCOPE = "正式范围外"


class AliasConflictStatus(StrEnum):
    """别名匹配冲突状态（§3.4）；冲突不静默合并，进复核。

    ``EXCLUDED``（ext-sale-ingest-scope-v1-2）：道路级命名等无法唯一解析的
    源名经用户裁决后的**终态**——永久退出自动映射候选，仅保留登记与溯源；
    语义上与待定同为 blocked（仅一致别名参与自动映射），但不再进入待复核队列。
    """

    CONSISTENT = "一致"
    CONFLICT = "冲突"
    PENDING = "待定"
    EXCLUDED = "排除"


class SourceStrength(StrEnum):
    """市场序列来源强度（§3.8）：官方/平台/第三方。"""

    OFFICIAL = "官方"
    PLATFORM = "平台"
    THIRD_PARTY = "第三方"


# ---- §3.3 community（小区实体权威表，WP5-A 构建骨架） ----
class Community(BaseModel):
    """小区实体权威表（数据字典 §3.3）。

    骨架期（WP5-A）以候选小区名录（房天下）为权威骨架，community_id 采用
    ``C-<房天下loupanID>`` 稳定标识；链家全量权威数据后续回填时经
    community_alias 映射到同一 community_id（WP5-B/E）。
    """

    community_id: str = Field(..., description="标准小区ID，实体权威表主键（DATA-005）")
    standard_name: str = Field(..., description="标准名（骨架期取候选名录标准名）")
    block: str = Field(..., description="来源板块名（各源口径不同；骨架期为房天下板块）")
    address: str = Field(
        default="UNKNOWN", description="至少到道路/门牌；未知=UNKNOWN（不得用0）"
    )
    latitude: float | None = Field(
        default=None, ge=-90, le=90, description="纬度（十进制度）；未知=None（不得用0）"
    )
    longitude: float | None = Field(
        default=None, ge=-180, le=180, description="经度（十进制度）；未知=None（不得用0）"
    )
    coordinate_system: CoordinateSystem = Field(
        default=CoordinateSystem.UNKNOWN,
        description="坐标系；记录坐标时必须声明，不做无声明转换（§7.3）",
    )
    boundary_status: BoundaryStatus = Field(
        ..., description="范围状态（DATA-001-C 边界三分）"
    )
    source_id: str = Field(
        ..., description="来源登记ID，FK→source_registry（骨架期=SRC-005 房天下）"
    )
    source_key: str = Field(
        default="UNKNOWN", description="来源内部稳定ID（房天下 loupan ID）；无则=UNKNOWN"
    )
    source_ref: str = Field(
        ..., description="来源定位（名录节号/行号），每行可追溯到来源（验收②）"
    )
    notes: str | None = Field(default=None, description="名录备注")

    @model_validator(mode="after")
    def _coordinate_system_required_with_coordinates(self) -> Self:
        if (self.latitude is not None or self.longitude is not None) and (
            self.coordinate_system is CoordinateSystem.UNKNOWN
        ):
            raise ValueError(
                "记录坐标必须声明 coordinate_system，不得为 UNKNOWN（§7.3 无声明不转换）"
            )
        return self


# ---- §3.4 community_alias（小区别名映射；不静默合并） ----
class CommunityAlias(BaseModel):
    """小区别名映射（数据字典 §3.4）。"""

    alias_id: str = Field(..., description="唯一ID，主键")
    community_id: str = Field(..., description="FK→community，标准小区ID")
    source_alias: str = Field(..., description="来源别名（曾用名/平台差异/分期名）")
    source_id: str = Field(..., description="FK→source_registry，别名来源")
    source_ref: str = Field(
        ..., description="别名出处定位（名录 §3 冲突清单项/来源页），每行可溯源"
    )
    conflict_status: AliasConflictStatus = Field(
        ..., description="与标准名的匹配冲突状态；冲突不静默合并，进复核"
    )


# ---- §3.5 building（楼栋弱实体；信息不足允许未知，不得用0） ----
class Building(BaseModel):
    """楼栋弱实体（数据字典 §3.5）。"""

    building_id: str = Field(..., description="唯一ID，主键")
    community_id: str = Field(..., description="FK→community，所属小区")
    building_name: str = Field(
        default="UNKNOWN", description="楼栋名/编号；未知=UNKNOWN"
    )
    year_built: int | None = Field(
        default=None, ge=1, description="建成年代；未知=None（不得用0）"
    )
    total_floors: int | None = Field(
        default=None, ge=1, description="总层数；未知=None（不得用0）"
    )
    has_elevator: bool | None = Field(
        default=None, description="是否电梯楼；未知=None"
    )


# ---- §3.8 market_series（市场序列；时间修正候选证据登记，本包不做修正） ----
class MarketSeries(BaseModel):
    """市场序列（数据字典 §3.8）。"""

    series_id: str = Field(..., description="唯一ID，主键")
    region: str = Field(..., description="区域/板块（目标区/板块/小区）")
    month: date = Field(..., description="统计月份（精度到月）")
    price: Decimal | None = Field(
        default=None, gt=0, description="聚合价（元/㎡）；未知=None（不得用0）"
    )
    price_change: Decimal | None = Field(
        default=None, description="同比/环比（%）；未知=None（不得用0）"
    )
    source_strength: SourceStrength = Field(
        ..., description="来源强度（官方/平台/第三方）"
    )
    revision_flag: bool | None = Field(
        default=None, description="是否会修订历史值（需求§3.4）；未知=None"
    )


# ---- §3.9-3.11 估值领域模型（WP6，VAL1-002 候选池起逐步实现） ----
class SubjectProperty(BaseModel):
    """目标房源，一次估值的输入快照（数据字典 §3.9）。

    ``valuation_date`` 为估值时点（必需）；之后的数据不得进入（§3.1/§9.2
    无未来泄漏）。P1 属性（楼栋/楼层/总层数/电梯/朝向/年代）未知允许
    （None/UNKNOWN，不得用 0）；``site_observations`` 为现场观察项，
    公开不可得时保持 UNKNOWN（扩大区间）。
    """

    subject_id: str = Field(..., description="唯一ID，主键")
    community_id: str = Field(..., description="FK→community，小区（必需）")
    area_sqm: Decimal = Field(..., gt=0, description="建筑面积（㎡，必需）")
    layout: str = Field(..., description="户型（必需，如 2室1厅）")
    valuation_date: date = Field(..., description="估值时点（必需）；之后数据不得进入")
    building_name: str = Field(default="UNKNOWN", description="楼栋名/编号；未知=UNKNOWN")
    floor: int | None = Field(default=None, ge=1, description="所在楼层；未知=None")
    total_floors: int | None = Field(default=None, ge=1, description="总层数；未知=None")
    has_elevator: bool | None = Field(default=None, description="电梯；未知=None")
    orientation: str = Field(default="UNKNOWN", description="朝向；未知=UNKNOWN")
    year_built: int | None = Field(default=None, ge=1, description="建成年代；未知=None")
    site_observations: str | None = Field(
        default=None,
        description="现场观察项（装修/采光/噪音/景观/维护）；公开不可得→UNKNOWN（扩大区间）",
    )


class ValuationRun(BaseModel):
    """一次运行总清单（数据字典 §3.10）；估值时点与数据截点固定（README §3.1）。"""

    run_id: str = Field(..., description="唯一ID，主键")
    subject_id: str = Field(..., description="FK→subject_property，目标房源")
    valuation_date: date = Field(..., description="估值时点（必需）")
    data_cutoff: date = Field(..., description="数据截点（只使用当日及之前数据）")
    data_version: str = Field(..., description="数据版本（输入表来源标识，可复现）")
    rule_version: str = Field(..., description="规则版本（策略 rule_version）")
    code_version: str = Field(..., description="代码版本（包版本）")
    parameters: dict[str, Any] = Field(..., description="运行参数（可复现）")
    run_at: datetime = Field(..., description="运行时间")


class CompCandidate(BaseModel):
    """可比案例候选，全量留痕（数据字典 §3.11）。

    ``selected`` 区分入选/排除；``tier`` 为层级（放宽有序，A 层=1，逐级放宽
    递增）；候选池阶段（WP6-A）只做检索不做层级，未分层=None（缺失用 None
    不用 0），由 WP6-B 逐级放宽填充；``similarity`` 由 WP6-B 相似度分项填充
    （未知=None）；``reason`` 为入选/排除理由（必填，可溯源）。
    """

    candidate_id: str = Field(..., description="唯一ID，主键")
    run_id: str = Field(..., description="FK→valuation_run")
    sale_event_id: str = Field(..., description="FK→sale_event，成交事件")
    community_id: str = Field(..., description="FK→community，小区")
    selected: bool = Field(..., description="入选/排除")
    tier: int | None = Field(
        default=None, ge=1, description="层级（放宽有序；A 层=1）；候选池阶段=None，WP6-B 填充"
    )
    similarity: Decimal | None = Field(
        default=None, description="相似度；未知=None（不得用 0）"
    )
    reason: str = Field(..., description="入选/排除理由（必填）")


# ---- §3.13 valuation_result（估值结果，不可覆盖）枚举与模型（WP6，VAL1-006） ----
class ConfidenceLevel(StrEnum):
    """可信度四级（技术方案 §9.7 / 数据字典 §3.13，未经校准不输出精确分数/概率）。"""

    HIGH = "高"
    MEDIUM = "中"
    LOW = "低"
    INSUFFICIENT = "不足"


class OutputStatus(StrEnum):
    """输出状态（数据字典 §3.13 三态）。

    用户 2026-08-22 判定：status 落盘采用 §3.13 三态。技术方案 §9.8 的
    ``insufficient_data``/``not_applicable`` 归入 ``候选``，由 reason/补数要求列
    区分"未形成可靠结果"的具体原因；``正式`` 由发布状态门禁控制，不因单次
    数据充分而由单次运行自宣（§9.8 反例）。
    """

    CANDIDATE = "候选"
    REFERENCE = "参考"
    FORMAL = "正式"


class ValuationResult(BaseModel):
    """估值结果，不可覆盖（数据字典 §3.13）。center/range/confidence/status 必填。

    仅报告"估值范围"，非已校准统计置信区间（§9.6）；range 以
    ``range_lower``/``range_upper`` 表达；``evidence`` 为分项证据（§9.7），
    ``reason`` 记录状态原因/补数要求（§9.8 反例与单调性）。
    """

    result_id: str = Field(..., description="唯一ID，主键")
    run_id: str = Field(..., description="FK→valuation_run")
    subject_id: str = Field(..., description="FK→subject_property，目标房源")
    center: Decimal = Field(..., gt=0, description="当前市场价值中心（元/㎡）")
    range_lower: Decimal | None = Field(
        default=None, ge=0, description="合理估值区间下界（元/㎡）；候选/不可靠=None"
    )
    range_upper: Decimal | None = Field(
        default=None, gt=0, description="合理估值区间上界（元/㎡）；候选/不可靠=None"
    )
    confidence: ConfidenceLevel = Field(..., description="可信度（四级）")
    status: OutputStatus = Field(..., description="输出状态（候选/参考/正式）")
    valuation_date: date = Field(..., description="估值时点")
    rule_version: str = Field(..., description="规则版本（策略 rule_version）")
    evidence: dict[str, str] = Field(..., description="分项证据（§9.7）")
    reason: str = Field(..., description="状态原因/补数要求（必填，可溯源）")


class ReviewAction(StrEnum):
    """复核动作（技术方案 §11.1 / README §6.9）。

    允许删除/恢复/更换案例、修改已知属性与现场观察；自动结果本身只可确认，
    不被静默改写（§6.9：复核不得静默修改自动结果）。
    """

    CONFIRM = "确认"
    DELETE_CASE = "删除案例"
    RESTORE_CASE = "恢复案例"
    SWAP_CASE = "更换案例"
    MODIFY_ATTRIBUTE = "修改属性"
    MODIFY_OBSERVATION = "修改现场观察"
    CORRECT_DATA = "纠正数据"


class ReviewJudgment(StrEnum):
    """复核判断类型（§6.9 第6条 / 技术方案 §11.1）。

    严格区分“纠正错误数据”与“主观判断调整”：前者针对数据/解析错误、可复现
    依据充分；后者是复核人对估值口径的主观取舍。两者证据标准不同，必须显式
    标记不得混淆（验收③）。
    """

    CORRECT_ERROR = "纠正错误数据"
    SUBJECTIVE_ADJUSTMENT = "主观判断调整"


class ReviewEvent(BaseModel):
    """复核事件，只追加（数据字典 §3.14 / 技术方案 §11.1）。

    review_event 是 append-only 留痕：一次复核产生一条事件，不修改既有事件，
    也不静默改写它指向的 ``valuation_result`` 自动结果（§6.9/验收①）。
    新估值版本作为后续事件表达，原自动版本永久保留（§11.1）。
    """

    review_id: str = Field(..., description="唯一ID，主键")
    result_id: str = Field(..., description="FK→valuation_result")
    action: ReviewAction = Field(..., description="复核动作")
    judgment: ReviewJudgment = Field(..., description="纠正错误数据/主观判断调整")
    subject: str = Field(..., description="复核对象描述（案例/属性/调整项/估值结论）")
    before: dict[str, Any] = Field(default_factory=dict, description="修改前值")
    after: dict[str, Any] = Field(default_factory=dict, description="修改后值")
    reason: str = Field(..., description="理由（必填）")
    evidence: str = Field(..., description="证据或现场观察（必填）")
    reviewed_at: datetime = Field(..., description="复核时间")
    reviewer: str = Field(..., description="复核人（第一阶段唯一：用户）")
    rule_version: str = Field(..., description="规则版本（策略 rule_version）")


CONTRACT_MODELS: dict[str, type[BaseModel]] = {
    "source_registry": SourceRegistry,
    "raw_snapshot": RawSnapshot,
    "sale_event": SaleEvent,
    "listing_event": ListingEvent,
    "community": Community,
    "community_alias": CommunityAlias,
    "building": Building,
    "market_series": MarketSeries,
    "subject_property": SubjectProperty,
    "valuation_run": ValuationRun,
    "comp_candidate": CompCandidate,
    "valuation_result": ValuationResult,
    "review_event": ReviewEvent,
}


def json_schema(model: str) -> dict[str, Any]:
    """JSON Schema for one registered contract model (验收标准③)."""
    try:
        model_type = CONTRACT_MODELS[model]
    except KeyError as exc:
        choices = ", ".join(sorted(CONTRACT_MODELS))
        raise KeyError(f"unknown contract model {model!r}; choose from {choices}") from exc
    return model_type.model_json_schema()
