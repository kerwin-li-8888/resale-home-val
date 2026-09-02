"""WP9-A 影子运行基础设施（SHADOW-001 前置）：影子登记/追踪/误差监控/新鲜度。

技术方案 §14 G4（影子运行通过）要求：冻结真实估值并持续追踪后续结果、
人工复核完整留痕、没有无法解释的系统性偏差、近期误差和数据新鲜度可以检测。
本模块落地 G4 的机制层（WP9-A）：

1. **影子标的登记（验收①）**：``register_subject`` 复用 WP7-B ``run_estimate``
   冻结估值（包络完整、写 05-估值报告 冻结 JSON），并把 frozen 结果登记到
   影子追踪表 ``data/shadow/shadow_track.parquet``；
2. **追踪表只读不改写（验收②）**：``shadow_track`` 登记后不可改写——同一
   run_id 重复登记幂等返回既有行；backfill 只写独立的 ``shadow_followup``
   表，绝不触碰 track 表的 frozen 列；
3. **后续成交回填与误差计算（验收③）**：``backfill_followups`` 对每个影子
   标的，只使用 ``估值时点 < sale_date <= tracking_cutoff`` 且同小区的
   ``valid_sale`` 成交（时间外校验，不挑选样本），计算 APE 与区间命中，
   全量重建可复现；
4. **近期误差滚动窗口 + 数据新鲜度（验收④）**：``monitor`` 输出近期窗口
   （默认 30 天）的误差指标（APE 中位数/高分位、区间命中率、有符号误差、
   高估率）与数据新鲜度（valid_sale 最新快照取得时间 vs 估值/监测时点），
   并按 README §7.2 触发条件（误差扩大/区间失准/数据中断）给出预警；
   ``compsval report build`` 第 12 节「后续结果区」的数据源即本模块的
   ``shadow_followup``（WP9-C/E 在此之上联动报告，本包不改 WP7 报告逻辑）。

禁止项（工作包合同）：不改 WP7 冻结估值/报告/复核逻辑；不覆盖冻结估值
JSON；不把估值时点后数据纳入估值本身；不改 WP6 策略。登记与回填均只追加
或重建派生表，不动 raw/staged/marts/entities/valuation 既有表。
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    read_derived_manifest,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import MissingDependencyError
from compsval.valuation.estimate import run_estimate

#: 影子派生层（data/shadow/；与 catalog 注册的既有层独立，不改 catalog.py）。
SHADOW_LAYER: Final = "shadow"
TRACK_TABLE: Final = "shadow_track"
FOLLOWUP_TABLE: Final = "shadow_followup"
TRACK_FILENAME: Final = f"{TRACK_TABLE}.parquet"
FOLLOWUP_FILENAME: Final = f"{FOLLOWUP_TABLE}.parquet"

#: G3 复审证据 V1.2 §6 确认的误差参考基线（GATE1-001，2026-08-23 用户接受）。
#: 影子期（SHADOW-001）监控的对比基准，非发布门槛。
DEFAULT_BASELINE_APE_MEDIAN: Final = 0.078
DEFAULT_BASELINE_RANGE_COVERAGE: Final = 0.80
#: 近期误差滚动窗口天数（合同 §5：滚动窗口口径在 WP9-A 落地，如 30 天）。
DEFAULT_WINDOW_DAYS: Final = 30
#: 数据新鲜度阈值：快照取得时间距监测时点超过该天数 → 数据中断预警。
DEFAULT_STALE_DAYS: Final = 30
#: 触发条件判定的最小近期样本数；不足则不判定（避免小样本误报）。
MIN_TRIGGER_SAMPLES: Final = 5

#: 触发条件标签（README §7.2：误差扩大/区间失准/数据中断；样本不足如实标注）。
TRIGGER_ERROR_EXPANSION: Final = "error_expansion"
TRIGGER_RANGE_MISS: Final = "range_miss"
TRIGGER_DATA_STALE: Final = "data_stale"
TRIGGER_INSUFFICIENT_SAMPLE: Final = "insufficient_window_sample"

_TRIGGER_LABELS: Final[dict[str, str]] = {
    TRIGGER_ERROR_EXPANSION: "近期误差扩大（窗口 APE 中位数高于基线）",
    TRIGGER_RANGE_MISS: "区间失准（近期区间命中率低于目标）",
    TRIGGER_DATA_STALE: "数据中断（最新快照取得时间距监测时点过久）",
    TRIGGER_INSUFFICIENT_SAMPLE: "近期窗口样本不足，触发条件不判定",
}


def _text(value: object) -> str:
    """pyarrow 标量 → str；None 保持空串（用于判定 community/layout）。"""
    if value is None:
        return ""
    text = str(value).strip()
    return "UNKNOWN" if text == "" else text


def _quantile(values: list[float], q: float) -> float:
    """确定性分位（线性索引取整），空序列禁止调用。"""
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def _latest_fetched_at(valid_sale_path: Path) -> datetime | None:
    """valid_sale 溯源中最新快照取得时间（数据新鲜度依据）；缺失 → None 不虚构。"""
    manifest_path = valid_sale_path.with_suffix(".manifest.json")
    if not manifest_path.is_file():
        return None
    try:
        inputs = list(read_derived_manifest(valid_sale_path).inputs)
    except Exception:  # noqa: BLE001 - manifest 损坏不虚构新鲜度
        return None
    stamps = [
        datetime.fromisoformat(item.fetched_at)
        for item in inputs
        if item.fetched_at and item.fetched_at != "UNKNOWN"
    ]
    return max(stamps) if stamps else None


def _data_version_of(valid_sale_path: Path) -> str:
    """valid_sale 数据版本（优先取 manifest 溯源；缺失 → UNKNOWN，不虚构）。"""
    manifest_path = valid_sale_path.with_suffix(".manifest.json")
    if not manifest_path.is_file():
        return "UNKNOWN"
    try:
        inputs = list(read_derived_manifest(valid_sale_path).inputs)
    except Exception:  # noqa: BLE001 - manifest 损坏不虚构版本
        return "UNKNOWN"
    return ";".join(f"{item.dataset}@{item.fetched_at}" for item in inputs) or "UNKNOWN"


def _read_table_or_empty(path: Path) -> pa.Table:
    """读派生表；不存在 → 空表（调用方决定语义）。"""
    if not path.is_file():
        names = shadow_track_schema().names
        return pa.table({name: [] for name in names}, schema=shadow_track_schema())
    return pq.read_table(path)


# ---------------------------------------------------------------------------
# 影子追踪表（frozen 结果，登记后只读不改写）
# ---------------------------------------------------------------------------


def shadow_track_schema() -> pa.Schema:
    """影子追踪表模式：每影子标的一行 frozen 结果（登记后不改写）。"""
    return pa.schema(
        [
            pa.field("subject_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("valuation_date", pa.date32(), nullable=False),
            pa.field("area_sqm", pa.float64(), nullable=True),
            pa.field("layout", pa.string(), nullable=False),
            pa.field("frozen_center", pa.float64(), nullable=True),
            pa.field("frozen_range_lower", pa.float64(), nullable=True),
            pa.field("frozen_range_upper", pa.float64(), nullable=True),
            pa.field("frozen_confidence", pa.string(), nullable=True),
            pa.field("frozen_business_status", pa.string(), nullable=True),
            pa.field("data_version", pa.string(), nullable=True),
            pa.field("rule_version", pa.string(), nullable=False),
            pa.field("estimate_path", pa.string(), nullable=False),
            pa.field("registered_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("notes", pa.string(), nullable=True),
        ]
    )


def _rows_to_track(rows: Sequence[dict[str, Any]]) -> pa.Table:
    names = shadow_track_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    return pa.table(columns, schema=shadow_track_schema())


@dataclass(frozen=True)
class RegisterOutcome:
    """一次影子登记的结果（供 CLI 打印与测试断言）。"""

    subject_id: str
    run_id: str
    estimate_path: Path
    track_path: Path
    data_version: str | None
    rule_version: str
    frozen_business_status: str | None
    duplicated: bool  # True = run_id 已登记，返回既有行（未改写）


def register_subject(
    *,
    subject: SubjectProperty,
    data_dir: Path,
    out_root: Path | None = None,
    rule_version: str = "1.0",
    notes: str | None = None,
) -> RegisterOutcome:
    """登记一个影子标的：复用 WP7-B ``run_estimate`` 冻结估值并登记追踪行。

    - 冻结估值复用 ``run_estimate``（包络完整、写冻结 JSON），不重写 WP7 逻辑；
    - 追踪表只读不改写（验收②）：同 run_id 已登记 → 幂等返回既有行，不覆盖；
    - 必要数据表缺失 → ``MissingDependencyError``（退出码 3，由 run_estimate 抛出）。
    """
    outcome = run_estimate(
        subject=subject,
        data_dir=data_dir,
        out_root=out_root,
        rule_version=rule_version,
    )
    result = outcome.result
    frozen_center = float(result.center) if result is not None else None
    frozen_lower = (
        float(result.range_lower) if result is not None and result.range_lower is not None else None
    )
    frozen_upper = (
        float(result.range_upper) if result is not None and result.range_upper is not None else None
    )
    frozen_confidence = result.confidence.value if result is not None else None
    business_status = outcome.envelope.business_status or "信息不足"

    track_path = data_dir / SHADOW_LAYER / TRACK_FILENAME
    existing = _read_table_or_empty(track_path)
    rows = existing.to_pylist()
    prior = next((r for r in rows if str(r.get("run_id")) == outcome.run_id), None)
    if prior is not None:
        # 只读不改写：已登记 run 返回既有行，不覆盖 frozen 结果（验收②）
        return RegisterOutcome(
            subject_id=str(prior["subject_id"]),
            run_id=outcome.run_id,
            estimate_path=Path(str(prior["estimate_path"])),
            track_path=track_path,
            data_version=str(prior.get("data_version")) or None,
            rule_version=str(prior["rule_version"]),
            frozen_business_status=str(prior.get("frozen_business_status")) or None,
            duplicated=True,
        )

    now = datetime.now(UTC)
    rows.append(
        {
            "subject_id": subject.subject_id,
            "run_id": outcome.run_id,
            "community_id": subject.community_id,
            "valuation_date": subject.valuation_date,
            "area_sqm": float(subject.area_sqm),
            "layout": _text(subject.layout) or "UNKNOWN",
            "frozen_center": frozen_center,
            "frozen_range_lower": frozen_lower,
            "frozen_range_upper": frozen_upper,
            "frozen_confidence": frozen_confidence,
            "frozen_business_status": business_status,
            "data_version": outcome.envelope.data_version,
            "rule_version": rule_version,
            "estimate_path": str(outcome.estimate_path),
            "registered_at": now,
            "notes": notes,
        }
    )
    table = _rows_to_track(rows)
    track_path.parent.mkdir(parents=True, exist_ok=True)
    work = track_path.with_name(TRACK_FILENAME + ".incomplete")
    pq.write_table(table, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=SHADOW_LAYER,
            table=TRACK_TABLE,
            built_at=now,
            row_count=table.num_rows,
            inputs=[],
            package_version=__version__,
            notes=f"WP9-A 影子登记（SHADOW-001；run {outcome.run_id}；只追加不改写）",
        ),
        track_path,
    )
    work.replace(track_path)
    return RegisterOutcome(
        subject_id=subject.subject_id,
        run_id=outcome.run_id,
        estimate_path=outcome.estimate_path,
        track_path=track_path,
        data_version=outcome.envelope.data_version,
        rule_version=rule_version,
        frozen_business_status=business_status,
        duplicated=False,
    )


# ---------------------------------------------------------------------------
# 后续成交回填与误差计算（时间外，全量重建可复现）
# ---------------------------------------------------------------------------


def shadow_followup_schema() -> pa.Schema:
    """影子后续成交表模式：每影子标的 × 每笔后续成交一行（可重算）。"""
    return pa.schema(
        [
            pa.field("subject_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("sale_event_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("actual_sale_date", pa.date32(), nullable=False),
            pa.field("actual_unit_price", pa.float64(), nullable=False),
            pa.field("area_sqm", pa.float64(), nullable=True),
            pa.field("layout", pa.string(), nullable=True),
            pa.field("ape", pa.float64(), nullable=True),
            pa.field("range_hit", pa.bool_(), nullable=True),
            pa.field("data_version", pa.string(), nullable=False),
            pa.field("matched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def _rows_to_followup(rows: list[dict[str, Any]]) -> pa.Table:
    names = shadow_followup_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    return pa.table(columns, schema=shadow_followup_schema())


def followup_error(
    *,
    frozen_center: float | None,
    frozen_lower: float | None,
    frozen_upper: float | None,
    actual_unit_price: float,
) -> tuple[float | None, bool | None]:
    """单笔后续成交 vs 冻结结果的误差（APE + 区间命中；无冻结值 → None）。

    APE = |center - actual| / actual；区间命中 = lower <= actual <= upper。
    冻结中心缺失（信息不足）→ APE None；区间不完整 → 命中 None，不虚构。
    """
    ape: float | None = None
    if frozen_center is not None and actual_unit_price > 0:
        ape = abs(frozen_center - actual_unit_price) / actual_unit_price
    range_hit: bool | None = None
    if frozen_lower is not None and frozen_upper is not None:
        range_hit = frozen_lower <= actual_unit_price <= frozen_upper
    return ape, range_hit


@dataclass(frozen=True)
class BackfillOutcome:
    """一次后续成交回填的结果（供 CLI 打印与测试断言）。"""

    followup_path: Path
    track_path: Path
    n_subjects: int
    n_followup: int
    tracking_cutoff: date
    data_version: str
    followup: pa.Table


def backfill_followups(
    *,
    data_dir: Path,
    tracking_cutoff: date | None = None,
) -> BackfillOutcome:
    """回填影子标的后续实际成交并计算误差（验收③，时间外、不挑选样本）。

    - 追踪源：``valid_sale``（链家/房天下成交，已入池）；只使用
      ``估值时点 < sale_date <= tracking_cutoff`` 且同小区（community_id 一致）
      的成交（时间外校验，估值时点前的成交绝不进入）；
    - tracking_cutoff 默认 = valid_sale 最大成交日（当前可得数据的全部后续成交）；
    - 全量重建 ``shadow_followup``（同输入同输出，可复现）；不触碰 track 表。
    """
    track_path = data_dir / SHADOW_LAYER / TRACK_FILENAME
    if not track_path.is_file():
        raise MissingDependencyError(
            f"影子追踪表缺失：{track_path}（先运行 compsval shadow register）"
        )
    valid_sale_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    if not valid_sale_path.is_file():
        raise MissingDependencyError(f"valid_sale 表缺失：{valid_sale_path}")
    track = pq.read_table(track_path)
    valid_sale = pq.read_table(valid_sale_path)
    data_version = _data_version_of(valid_sale_path)

    sale_dates = [
        value for value in valid_sale.column("sale_date").to_pylist() if isinstance(value, date)
    ]
    cutoff = tracking_cutoff or (max(sale_dates) if sale_dates else date.min)
    if tracking_cutoff is None and not sale_dates:
        # valid_sale 无成交 → 无追踪样本，如实返回空 followup（不虚构）
        cutoff = date.min

    # 同小区后续成交按 community_id 索引（匹配口径：同小区全部成交，不挑选样本）
    sales_by_community: dict[str, list[dict[str, Any]]] = {}
    for row in valid_sale.to_pylist():
        cid = _text(row.get("community_id"))
        if not cid or cid == "UNKNOWN":
            continue
        sale_date = row.get("sale_date")
        if not isinstance(sale_date, date):
            continue
        sales_by_community.setdefault(cid, []).append(row)

    now = datetime.now(UTC)
    followup_rows: list[dict[str, Any]] = []
    for tr in track.to_pylist():
        valuation_date = tr.get("valuation_date")
        if not isinstance(valuation_date, date):
            continue
        community_id = _text(tr.get("community_id"))
        frozen_center = tr.get("frozen_center")
        frozen_lower = tr.get("frozen_range_lower")
        frozen_upper = tr.get("frozen_range_upper")
        for sale in sales_by_community.get(community_id, []):
            sale_date = sale.get("sale_date")
            if not isinstance(sale_date, date):
                continue
            if not (valuation_date < sale_date <= cutoff):
                continue  # 时间外：估值时点之前或超出追踪截点的成交不入
            actual = sale.get("unit_price")
            if actual is None or float(actual) <= 0:
                continue
            area = sale.get("area_sqm")
            actual_price = float(actual)
            ape, range_hit = followup_error(
                frozen_center=float(frozen_center) if frozen_center is not None else None,
                frozen_lower=float(frozen_lower) if frozen_lower is not None else None,
                frozen_upper=float(frozen_upper) if frozen_upper is not None else None,
                actual_unit_price=actual_price,
            )
            followup_rows.append(
                {
                    "subject_id": _text(tr.get("subject_id")),
                    "run_id": _text(tr.get("run_id")),
                    "sale_event_id": _text(sale.get("sale_event_id")),
                    "source_id": _text(sale.get("source_id")) or None,
                    "community_id": community_id,
                    "actual_sale_date": sale_date,
                    "actual_unit_price": actual_price,
                    "area_sqm": float(area) if area is not None else None,
                    "layout": _text(sale.get("layout")) or None,
                    "ape": ape,
                    "range_hit": range_hit,
                    "data_version": data_version,
                    "matched_at": now,
                }
            )

    followup = _rows_to_followup(followup_rows)
    followup_path = data_dir / SHADOW_LAYER / FOLLOWUP_FILENAME
    followup_path.parent.mkdir(parents=True, exist_ok=True)
    work = followup_path.with_name(FOLLOWUP_FILENAME + ".incomplete")
    pq.write_table(followup, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=SHADOW_LAYER,
            table=FOLLOWUP_TABLE,
            built_at=now,
            row_count=followup.num_rows,
            inputs=[],
            package_version=__version__,
            notes=f"WP9-A 影子后续成交（时间外；tracking_cutoff={cutoff.isoformat()}）",
        ),
        followup_path,
    )
    work.replace(followup_path)
    return BackfillOutcome(
        followup_path=followup_path,
        track_path=track_path,
        n_subjects=track.num_rows,
        n_followup=followup.num_rows,
        tracking_cutoff=cutoff,
        data_version=data_version,
        followup=followup,
    )


# ---------------------------------------------------------------------------
# 误差监控：近期滚动窗口 + 数据新鲜度 + 触发条件（README §7.2）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ShadowMonitorConfig:
    """影子误差监控配置（近期窗口/基线/新鲜度阈值；数值待定项用已确认候选）。"""

    #: 近期误差滚动窗口天数（合同 §5；默认 30 天）。
    window_days: int = DEFAULT_WINDOW_DAYS
    #: 误差基线：窗口 APE 中位数高于此值 → 误差扩大预警（G3 §6：APE 中位 7.8%）。
    baseline_ape_median: float = DEFAULT_BASELINE_APE_MEDIAN
    #: 区间目标：窗口区间命中率低于此值 → 区间失准预警（G3 §6：区间目标 80-90%）。
    baseline_range_coverage: float = DEFAULT_BASELINE_RANGE_COVERAGE
    #: 数据新鲜度阈值：最新快照距监测时点超过该天数 → 数据中断预警。
    stale_days: int = DEFAULT_STALE_DAYS
    #: 监测时点（默认今天；测试可注入固定日期保证可复现）。
    as_of: date | None = None


def compute_window_metrics(
    followup: pa.Table,
    *,
    window_days: int,
    as_of: date,
) -> dict[str, Any]:
    """近期滚动窗口误差指标（默认 30 天；无样本如实 None/0，不虚构）。

    APE 与区间命中在 ``backfill_followups`` 已按冻结结果计算并落盘；此处只做
    窗口过滤与汇总。有符号误差/高估率需 frozen_center，见 ``compute_signed_metrics``。
    """
    window_start = as_of - timedelta(days=window_days)
    apes: list[float] = []
    hit_total = 0
    hit_count = 0
    n_window = 0
    for row in followup.to_pylist():
        sale_date = row.get("actual_sale_date")
        if not isinstance(sale_date, date):
            continue
        if not (window_start <= sale_date <= as_of):
            continue
        n_window += 1
        ape = row.get("ape")
        if ape is not None:
            apes.append(float(ape))
        range_hit = row.get("range_hit")
        if range_hit is not None:
            hit_total += 1
            if bool(range_hit):
                hit_count += 1
    return {
        "n_window_sales": n_window,
        "n_window_estimated": len(apes),
        "window_ape_median": statistics.median(apes) if apes else None,
        "window_ape_high_quantile": _quantile(apes, 0.90) if apes else None,
        "n_window_range": hit_total,
        "window_range_coverage_rate": (hit_count / hit_total) if hit_total else None,
    }


def compute_signed_metrics(
    followup: pa.Table,
    track: pa.Table,
    *,
    window_days: int,
    as_of: date,
) -> dict[str, Any]:
    """近期窗口有符号误差与高估率（G4 无系统性偏差的方向证据）。

    followup 未存 frozen_center，从 track 表按 run_id 还原后再算方向；无
    center（信息不足）的样本不计入方向。无样本如实 None。
    """
    center_by_run: dict[str, float] = {}
    for tr in track.to_pylist():
        run_id = _text(tr.get("run_id"))
        center = tr.get("frozen_center")
        if run_id and center is not None:
            center_by_run[run_id] = float(center)
    window_start = as_of - timedelta(days=window_days)
    signed: list[float] = []
    for row in followup.to_pylist():
        sale_date = row.get("actual_sale_date")
        if not isinstance(sale_date, date):
            continue
        if not (window_start <= sale_date <= as_of):
            continue
        run_id = _text(row.get("run_id"))
        center = center_by_run.get(run_id)
        actual = row.get("actual_unit_price")
        if center is None or actual is None or float(actual) <= 0:
            continue
        signed.append((center - float(actual)) / float(actual))
    overvaluation_rate = (
        sum(1 for s in signed if s > 0) / len(signed) if signed else None
    )
    return {
        "n_window_signed": len(signed),
        "window_signed_error_median": statistics.median(signed) if signed else None,
        "window_overvaluation_rate": overvaluation_rate,
    }


@dataclass(frozen=True)
class MonitorReport:
    """影子误差监控报告（可读 dict 由 ``monitor`` 产出，供 CLI/测试断言）。"""

    as_of: date
    window_metrics: dict[str, Any]
    signed_metrics: dict[str, Any]
    freshness: dict[str, Any]
    triggers: list[dict[str, Any]]
    subjects: list[dict[str, Any]]


def monitor(
    *,
    data_dir: Path,
    config: ShadowMonitorConfig,
) -> MonitorReport:
    """影子误差监控：近期滚动窗口 + 数据新鲜度 + README §7.2 触发条件。

    触发条件（小样本不判定，样本 < MIN_TRIGGER_SAMPLES 如实标注）：
    - 误差扩大：窗口 APE 中位数 > baseline_ape_median（G3 §6：7.8%）；
    - 区间失准：窗口区间命中率 < baseline_range_coverage（G3 §6：80%）；
    - 数据中断：最新快照取得时间距 as_of 超过 stale_days（默认 30 天）。
    """
    track_path = data_dir / SHADOW_LAYER / TRACK_FILENAME
    followup_path = data_dir / SHADOW_LAYER / FOLLOWUP_FILENAME
    valid_sale_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    if not track_path.is_file():
        raise MissingDependencyError(
            f"影子追踪表缺失：{track_path}（先运行 compsval shadow register）"
        )
    if not valid_sale_path.is_file():
        raise MissingDependencyError(f"valid_sale 表缺失：{valid_sale_path}")
    track = pq.read_table(track_path)
    followup = (
        pq.read_table(followup_path) if followup_path.is_file() else _empty_followup()
    )

    as_of = config.as_of or date.today()
    window_metrics = compute_window_metrics(
        followup, window_days=config.window_days, as_of=as_of
    )
    signed_metrics = compute_signed_metrics(
        followup, track, window_days=config.window_days, as_of=as_of
    )

    latest_fetched = _latest_fetched_at(valid_sale_path)
    days_since: int | None = None
    if latest_fetched is not None:
        days_since = (as_of - latest_fetched.date()).days
    freshness = {
        "latest_fetched_at": latest_fetched.isoformat() if latest_fetched else None,
        "days_since_latest_fetch": days_since,
        "fresh": days_since is not None and days_since <= config.stale_days,
    }

    triggers: list[dict[str, Any]] = []
    n_window_est = int(window_metrics.get("n_window_estimated") or 0)
    if n_window_est < MIN_TRIGGER_SAMPLES:
        triggers.append(
            {
                "trigger": TRIGGER_INSUFFICIENT_SAMPLE,
                "label": _TRIGGER_LABELS[TRIGGER_INSUFFICIENT_SAMPLE],
                "n_window_estimated": n_window_est,
            }
        )
    else:
        ape_median = window_metrics.get("window_ape_median")
        if ape_median is not None and float(ape_median) > config.baseline_ape_median:
            triggers.append(
                {
                    "trigger": TRIGGER_ERROR_EXPANSION,
                    "label": _TRIGGER_LABELS[TRIGGER_ERROR_EXPANSION],
                    "window_ape_median": float(ape_median),
                    "baseline_ape_median": config.baseline_ape_median,
                }
            )
        coverage = window_metrics.get("window_range_coverage_rate")
        n_range = int(window_metrics.get("n_window_range") or 0)
        if (
            n_range >= MIN_TRIGGER_SAMPLES
            and coverage is not None
            and float(coverage) < config.baseline_range_coverage
        ):
            triggers.append(
                {
                    "trigger": TRIGGER_RANGE_MISS,
                    "label": _TRIGGER_LABELS[TRIGGER_RANGE_MISS],
                    "window_range_coverage_rate": float(coverage),
                    "baseline_range_coverage": config.baseline_range_coverage,
                }
            )
    if days_since is not None and days_since > config.stale_days:
        triggers.append(
            {
                "trigger": TRIGGER_DATA_STALE,
                "label": _TRIGGER_LABELS[TRIGGER_DATA_STALE],
                "days_since_latest_fetch": days_since,
                "stale_days": config.stale_days,
            }
        )

    followup_by_run: dict[str, list[dict[str, Any]]] = {}
    for row in followup.to_pylist():
        run_id = _text(row.get("run_id"))
        followup_by_run.setdefault(run_id, []).append(row)
    subjects: list[dict[str, Any]] = []
    window_start = as_of - timedelta(days=config.window_days)
    for tr in track.to_pylist():
        run_id = _text(tr.get("run_id"))
        rows = followup_by_run.get(run_id, [])
        n_window = sum(
            1
            for r in rows
            if isinstance(r.get("actual_sale_date"), date)
            and window_start <= r["actual_sale_date"] <= as_of
        )
        subjects.append(
            {
                "subject_id": _text(tr.get("subject_id")),
                "run_id": run_id,
                "community_id": _text(tr.get("community_id")),
                "frozen_business_status": _text(tr.get("frozen_business_status")) or None,
                "frozen_center": (
                    float(tr["frozen_center"]) if tr.get("frozen_center") is not None else None
                ),
                "valuation_date": (
                    tr.get("valuation_date").isoformat()
                    if isinstance(tr.get("valuation_date"), date)
                    else None
                ),
                "n_followup_sales": len(rows),
                "n_window_sales": n_window,
            }
        )

    return MonitorReport(
        as_of=as_of,
        window_metrics=window_metrics,
        signed_metrics=signed_metrics,
        freshness=freshness,
        triggers=triggers,
        subjects=subjects,
    )


def _empty_followup() -> pa.Table:
    names = shadow_followup_schema().names
    return pa.table({name: [] for name in names}, schema=shadow_followup_schema())


def followup_markdown(
    *,
    run_id: str,
    data_dir: Path,
    reports_root: Path | None = None,
) -> str:
    """后续结果区 Markdown 片段（``compsval report build`` 第 12 节的数据源）。

    只读：从 ``shadow_followup`` 读取指定 run 的后续成交，生成可嵌入报告的
    「后续结果区」文本；无追踪样本如实标注。不修改任何冻结产物（WP9-A 不
    改 WP7 报告逻辑；本函数仅作为报告联动的前置数据/文本源）。
    """
    followup_path = data_dir / SHADOW_LAYER / FOLLOWUP_FILENAME
    if not followup_path.is_file():
        return (
            "- （暂无影子追踪记录；`compsval shadow register` 登记标的并 "
            "`compsval shadow backfill` 回填后生成）"
        )
    followup = pq.read_table(followup_path)
    rows = [r for r in followup.to_pylist() if _text(r.get("run_id")) == run_id]
    if not rows:
        return "- （该估值 run 暂无后续成交可追踪，或尚未回填）"
    lines = ["| 成交事件 | 成交日 | 实际单价(元/㎡) | APE | 区间命中 |", "|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda x: str(x.get("actual_sale_date"))):
        sale_date = r.get("actual_sale_date")
        sale_date_text = sale_date.isoformat() if isinstance(sale_date, date) else "UNKNOWN"
        ape = r.get("ape")
        ape_text = f"{float(ape):.1%}" if ape is not None else "—"
        hit = r.get("range_hit")
        hit_text = "命中" if hit is True else ("未命中" if hit is False else "—")
        lines.append(
            f"| {_text(r.get('sale_event_id'))} | {sale_date_text} | "
            f"{float(r['actual_unit_price']):.0f} | {ape_text} | {hit_text} |"
        )
    return "\n".join(lines)
