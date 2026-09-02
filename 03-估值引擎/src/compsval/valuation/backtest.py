"""WP8-A 滚动历史回放（BT-001）：时间外切分 + 简单基准 + §13.3 指标。

技术方案 §13.3/§10.2/§10.3/§10.4/§12.2：每个回放估值点 D 只用当时可得数据
（``sale_date <= D`` 且排除目标成交自身），对之后的可信成交进行校验；基准
至少为"同小区近期成交简单中位数"（README §7.1）；输出 §13.3 指标清单
（APE 中位数、高分位绝对误差、有符号误差与高估率、区间覆盖率、区间相对
宽度、状态覆盖）。命令形式 ``compsval backtest run --config <yaml>``。

设计要点（对应 WP8-A 验收标准）：

- **时间外切分（验收①）**：回放点 D 的可用池 = valid_sale 中 ``sale_date
  <= D`` 且 ``sale_event_id != 目标成交`` 的记录（目标成交价格不得进入自身
  估值的比较池）。合成多期数据反例测试验证无 ``sale_date > D`` 记录入池。
- **简单基准（验收②）**：同小区（community_id 相同）近期
  （``[D-window, D]``）成交 ``unit_price`` 的简单中位数；无同小区历史成交
  或中位数不可计算 → ``baseline=None``，不虚构。
- **指标（验收③）**：APE 中位数/高分位、有符号误差与高估率、区间覆盖率、
  区间相对宽度、状态覆盖，合成已知答案对拍。
- **命令（验收④）**：``compsval backtest run --config <yaml>`` 非交互、stdout 只
  写 §10.3 包络、退出码 0/2/3/4/5（缺配置/坏配置 → 2；必要表缺失 → 3）。
- **可重复（验收⑤）**：同一输入/数据/规则重复运行产出同一 run_id 与同一
  明细/指标表（run_at 为执行元数据，不参与内容判定）。
- **只读复用（合同禁止项）**：复用 WP7 ``run_estimate`` 链路但**不写**真实
  data_dir 与 05-估值报告；每次回放估值在临时目录内执行（过滤后 valid_sale
  + 实体表副本），结束后清理。

真实数据限制（工作包合同数据边界）：当前 valid_sale 27 条全部落在
2026-07-19~21 三个日期窗口、快照 2026-08-21 抓取，滚动回放历史深度严重
受限。本模块以合成多期数据验证正确性；真实回放结果如实报告覆盖与限制，
不假装校准（WP8-C 出具诚实报告）。
"""

from __future__ import annotations

import hashlib
import json
import shutil
import statistics
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from compsval import __version__
from compsval.contract.models import SubjectProperty
from compsval.entities import (
    building as entities_building,
)
from compsval.entities import (
    community as entities_community,
)
from compsval.entities import (
    market_series as entities_market_series,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    read_derived_manifest,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
    VersionMismatchError,
)
from compsval.valuation.aggregation import (
    AGGREGATION_POLICIES,
    DEFAULT_AGGREGATION_POLICY,
)
from compsval.valuation.candidate import DEFAULT_RULE_VERSION
from compsval.valuation.estimate import EstimateOutcome, run_estimate
from compsval.valuation.interval_calibration import (
    LEGACY_RULE_VERSION,
    calibration_config_path,
)

#: 回放产物层（新层，catalog 注册 ``bt_`` 前缀视图）。
BACKTEST_LAYER: Final = "backtest"
DETAIL_TABLE: Final = "backtest_detail"
METRICS_TABLE: Final = "backtest_metrics"
GROUPED_TABLE: Final = "backtest_grouped"
DETAIL_FILENAME: Final = f"{DETAIL_TABLE}.parquet"
METRICS_FILENAME: Final = f"{METRICS_TABLE}.parquet"
GROUPED_FILENAME: Final = f"{GROUPED_TABLE}.parquet"
RUN_MANIFEST_FILENAME: Final = "run_manifest.json"

#: 目标成交被跳过的理由（可溯源，不虚构）。
REASON_TARGET_UNMATCHED = "目标成交未匹配小区（community_id 缺失），无法构造 subject，跳过"
REASON_TARGET_MISSING_FIELDS = "目标成交缺少必要字段（面积/总价/单价），无法构造 subject，跳过"

#: 回放使用的 valid_sale 必读列（与候选检索所需列一致，缺失列按整列过滤处理）。
_POOL_COLUMNS = ("sale_date", "sale_event_id", "community_id", "unit_price")

#: 近似月天数（回放窗口按自然月折算；精确边界在 WP8-C 报告说明）。
_DAYS_PER_MONTH: Final = 30


def _text(value: object) -> str:
    """pyarrow 标量 → str；None 保持空串（用于判定 community 等）。"""
    if value is None:
        return ""
    text = str(value).strip()
    return "UNKNOWN" if text == "" else text


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BacktestConfig:
    """回放配置（YAML ``--config`` 对应字段；数值待定项不填假默认）。"""

    #: 规则版本（回放与 estimate 链路一致；默认同 WP6 候选版本 1.0）。
    rule_version: str = DEFAULT_RULE_VERSION
    #: 回放估值点集合；空元组 → auto = valid_sale 去重成交日（升序）。
    replay_dates: tuple[date, ...] = ()
    #: 简单基准"近期"窗口（自然月数）。
    baseline_window_months: int = 12
    #: 高分位绝对误差分位（默认 0.90）。
    high_quantile: float = 0.90
    #: 期望数据版本（可选）：指定且与 valid_sale manifest 数据版本不一致 →
    #: ``VersionMismatchError``（退出码 4，§10.4）；None 不校验。
    expected_data_version: str | None = None
    #: 汇总中心构造策略（add-aggregator-policy-experiment）：默认 = 现行为。
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY
    #: 数据湖根（None → 运行时 data_dir）。
    data_dir: Path | None = None
    #: 产物输出根（None → data_dir/backtest）。
    out_dir: Path | None = None


def load_backtest_config(path: Path) -> BacktestConfig:
    """读取并校验回放配置 YAML；非法输入 → ``InvalidInputError``（退出码 2）。"""
    if not path.is_file():
        raise InvalidInputError(f"回放配置文件不存在：{path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - 配置不可解析属输入不合法
        raise InvalidInputError(f"回放配置解析失败：{path}：{exc}") from exc
    if not isinstance(raw, dict):
        raise InvalidInputError(f"回放配置必须为 YAML 映射：{path}")
    try:
        rule_version = str(raw.get("rule_version", DEFAULT_RULE_VERSION))
        if not rule_version:
            raise ValueError("rule_version 不能为空")
        window = int(raw.get("baseline_window_months", 12))
        if window <= 0:
            raise ValueError("baseline_window_months 必须为正整数")
        quantile = float(raw.get("high_quantile", 0.90))
        if not 0.0 < quantile < 1.0:
            raise ValueError("high_quantile 必须在 (0, 1) 区间")
        dates_raw = raw.get("replay_dates", [])
        if not isinstance(dates_raw, list):
            raise ValueError("replay_dates 必须为日期字符串列表（YYYY-MM-DD）")
        replay_dates = tuple(
            sorted({date.fromisoformat(str(item)) for item in dates_raw})
        )
        expected_version = raw.get("expected_data_version")
        if expected_version is not None and not str(expected_version):
            raise ValueError("expected_data_version 不能为空字符串")
        policy = str(raw.get("aggregation_policy", DEFAULT_AGGREGATION_POLICY))
        if policy not in AGGREGATION_POLICIES:
            raise ValueError(
                "aggregation_policy 必须为 " + "/".join(AGGREGATION_POLICIES) + " 之一"
            )
        data_dir = Path(str(raw["data_dir"])) if raw.get("data_dir") else None
        out_dir = Path(str(raw["out_dir"])) if raw.get("out_dir") else None
    except Exception as exc:  # noqa: BLE001 - 字段类型/取值非法属输入不合法
        raise InvalidInputError(f"回放配置非法：{path}：{exc}") from exc
    return BacktestConfig(
        rule_version=rule_version,
        replay_dates=replay_dates,
        baseline_window_months=window,
        high_quantile=quantile,
        expected_data_version=str(expected_version) if expected_version else None,
        aggregation_policy=policy,
        data_dir=data_dir,
        out_dir=out_dir,
    )


def _replay_dates(config: BacktestConfig, valid_sale: pa.Table) -> list[date]:
    """回放估值点：config 指定优先；auto = valid_sale 去重成交日（升序）。"""
    if config.replay_dates:
        return list(config.replay_dates)
    dates = {
        value
        for value in valid_sale.column("sale_date").to_pylist()
        if isinstance(value, date)
    }
    return sorted(dates)


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


def _run_id(
    data_version: str,
    rule_version: str,
    dates: Sequence[date],
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY,
) -> str:
    """回放 run_id：同一 数据版本+规则版本+回放点集合 产出同一 ID（可复现）。

    非默认汇总策略追加策略后缀以区分候选运行；默认策略构成不变（既有
    BT-1.0 run_id 保持可复现）。
    """
    suffix = (
        f"-{aggregation_policy}"
        if aggregation_policy != DEFAULT_AGGREGATION_POLICY
        else ""
    )
    if not dates:
        return f"BT-{rule_version}-{data_version}-empty{suffix}"
    first = dates[0].strftime("%Y%m%d")
    last = dates[-1].strftime("%Y%m%d")
    return f"BT-{rule_version}-{data_version}-{first}-{last}-{len(dates)}p{suffix}"


# ---------------------------------------------------------------------------
# 时间外切分与简单基准
# ---------------------------------------------------------------------------


def filter_pool(valid_sale: pa.Table, cutoff: date, exclude_event_id: str) -> pa.Table:
    """回放点 D 的可用池：``sale_date <= D`` 且排除目标成交自身（验收①）。

    未来成交（``sale_date > D``）与目标成交价格一律不得进入比较池；返回与
    valid_sale 同 schema 的过滤子表。
    """
    missing = [c for c in _POOL_COLUMNS if c not in valid_sale.column_names]
    if missing:
        raise ValueError(f"valid_sale 缺少必要列: {', '.join(missing)}")
    keep: list[bool] = []
    for sale_date, event_id in zip(
        valid_sale.column("sale_date").to_pylist(),
        valid_sale.column("sale_event_id").to_pylist(),
        strict=True,
    ):
        is_prior = isinstance(sale_date, date) and sale_date <= cutoff
        keep.append(bool(is_prior and event_id != exclude_event_id))
    return valid_sale.filter(pa.array(keep))


def simple_baseline(
    pool: pa.Table,
    community_id: str,
    cutoff: date,
    window_days: int,
) -> tuple[float | None, int]:
    """同小区近期成交简单中位数（README §7.1 基准，验收②）。

    口径：``community_id == 目标小区``、``sale_date ∈ [cutoff-window, cutoff]``
    的 ``unit_price`` 中位数。无有效值 → ``(None, 0)``（不虚构基准）。
    """
    missing = [c for c in _POOL_COLUMNS if c not in pool.column_names]
    if missing:
        raise ValueError(f"pool 缺少必要列: {', '.join(missing)}")
    window_start = cutoff - timedelta(days=window_days)
    values: list[float] = []
    for cid, sale_date, price in zip(
        pool.column("community_id").to_pylist(),
        pool.column("sale_date").to_pylist(),
        pool.column("unit_price").to_pylist(),
        strict=True,
    ):
        if not cid or cid == "UNKNOWN" or cid != community_id:
            continue
        # 时间边界纵深防御：只取 [cutoff-window, cutoff] 内成交（验收①）
        if not isinstance(sale_date, date):
            continue
        if sale_date < window_start or sale_date > cutoff:
            continue
        if price is None or price <= 0:
            continue
        values.append(float(price))
    if not values:
        return None, 0
    return statistics.median(values), len(values)


# ---------------------------------------------------------------------------
# 明细构造（每个 回放点 × 目标成交 一行）
# ---------------------------------------------------------------------------


def backtest_detail_schema() -> pa.Schema:
    """回放明细表 PyArrow 模式（每行 = 一个回放估值点上的一个校验目标）。"""
    return pa.schema(
        [
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("replay_date", pa.date32(), nullable=False),
            pa.field("target_sale_event_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("area_sqm", pa.float64(), nullable=True),
            pa.field("layout", pa.string(), nullable=True),
            pa.field("actual_unit_price", pa.float64(), nullable=True),
            pa.field("estimate_center", pa.float64(), nullable=True),
            pa.field("range_lower", pa.float64(), nullable=True),
            pa.field("range_upper", pa.float64(), nullable=True),
            pa.field("confidence", pa.string(), nullable=True),
            pa.field("business_status", pa.string(), nullable=True),
            pa.field("baseline_median", pa.float64(), nullable=True),
            pa.field("baseline_count", pa.int32(), nullable=False),
            pa.field("pool_size", pa.int32(), nullable=False),
            pa.field("pool_matched", pa.int32(), nullable=False),
            pa.field("skip_reason", pa.string(), nullable=True),
            pa.field("rule_version", pa.string(), nullable=False),
            # 中心构造诊断列（add-aggregator-policy-experiment；可空、向后兼容）
            pa.field("n_comps", pa.int32(), nullable=True),
            pa.field("effective_samples", pa.float64(), nullable=True),
            pa.field("max_weight_share", pa.float64(), nullable=True),
            pa.field("dominant_flag", pa.bool_(), nullable=True),
            pa.field("aggregation_policy", pa.string(), nullable=True),
            pa.field("fallback_flag", pa.bool_(), nullable=True),
        ]
    )


def _empty_detail() -> pa.Table:
    """零目标回放 → 空明细表（保持 schema，指标如实为 0/None）。"""
    names = backtest_detail_schema().names
    return pa.table({name: [] for name in names}, schema=backtest_detail_schema())


def _rows_to_detail(rows: Sequence[dict[str, Any]]) -> pa.Table:
    """明细行（dict 列表）→ 按固定 schema 的明细表（列缺失/多余即报错，防漂移）。"""
    names = backtest_detail_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    return pa.table(columns, schema=backtest_detail_schema())


def _subject_attributes_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """目标成交行 → subject P1 属性 kwargs（excel-attribute-enrichment 任务③）。

    属性为目标房产自身静态事实（电梯/朝向/年代/总层数），取自目标行自身记录，
    不引入估值时点之后的市场信息（无未来泄漏）；非法/越界值如实留 None，
    不因属性缺失跳过目标（跳过理由仍只保留既有两条）。
    """
    kwargs: dict[str, Any] = {}
    orientation = row.get("orientation")
    if orientation is not None and str(orientation).strip() not in ("", "UNKNOWN"):
        kwargs["orientation"] = str(orientation).strip()
    for key in ("total_floors", "year_built"):
        value = row.get(key)
        if value is None:
            continue
        try:
            number = int(str(value))
        except (TypeError, ValueError):
            continue
        if number >= 1:
            kwargs[key] = number
    elevator = row.get("has_elevator")
    if elevator is not None:
        kwargs["has_elevator"] = bool(elevator)
    return kwargs


def _build_subject(
    row: dict[str, Any], replay_date: date
) -> tuple[SubjectProperty | None, str | None]:
    """目标成交 → 回放 subject；缺必要字段 → (None, 跳过理由)。"""
    community_id = _text(row.get("community_id"))
    if not community_id or community_id == "UNKNOWN":
        return None, REASON_TARGET_UNMATCHED
    area = row.get("area_sqm")
    unit_price = row.get("unit_price")
    if area is None or unit_price is None:
        return None, REASON_TARGET_MISSING_FIELDS
    try:
        area_decimal = Decimal(str(area))
        if area_decimal <= 0:
            return None, REASON_TARGET_MISSING_FIELDS
    except (TypeError, ValueError):
        return None, REASON_TARGET_MISSING_FIELDS
    sale_event_id = _text(row.get("sale_event_id"))
    subject = SubjectProperty(
        subject_id=f"BT-{sale_event_id}",
        community_id=community_id,
        area_sqm=area_decimal,
        layout=_text(row.get("layout")) or "UNKNOWN",
        valuation_date=replay_date,
        **_subject_attributes_from_row(row),
    )
    return subject, None


def _count_matched(pool: pa.Table) -> int:
    """池内已匹配小区（community_id 非空）的成交条数。"""
    count = 0
    for cid in pool.column("community_id").to_pylist():
        if cid and cid != "UNKNOWN":
            count += 1
    return count


def _run_isolated_estimate(
    *,
    subject: SubjectProperty,
    pool: pa.Table,
    communities: pa.Table,
    buildings: pa.Table,
    market_series: pa.Table,
    rule_version: str,
    calibration_config: Path | None = None,
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY,
) -> EstimateOutcome:
    """在临时数据目录内执行一次回放估值（只读复用 WP7 链路，不污染真实湖）。

    过滤后的 valid_sale（排除未来成交与目标自身）+ 实体表副本写入临时目录，
    ``run_estimate`` 在其上执行并把中间/冻结产物留在临时目录，随上下文清理。
    校准版本（1.1 起）须把校准配置一并注入临时目录（与真实湖同一份资产）。
    ``aggregation_policy`` 只切换汇总中心构造（默认 = 现行为）。
    """
    with tempfile.TemporaryDirectory(prefix="compsval-backtest-") as td:
        tmp = Path(td)
        marts_dir = tmp / MARTS_LAYER
        marts_dir.mkdir(parents=True)
        pq.write_table(pool, marts_dir / VALID_SALE_FILENAME)
        entities_dir = tmp / entities_community.ENTITIES_LAYER
        entities_dir.mkdir(parents=True)
        pq.write_table(communities, entities_dir / entities_community.COMMUNITY_FILENAME)
        pq.write_table(buildings, entities_dir / entities_building.BUILDING_FILENAME)
        pq.write_table(
            market_series,
            entities_dir / f"{entities_market_series.MARKET_TABLE}.parquet",
        )
        if calibration_config is not None:
            rules_dir = tmp / calibration_config.parent.name
            rules_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(calibration_config, rules_dir / calibration_config.name)
        return run_estimate(
            subject=subject,
            data_dir=tmp,
            out_root=tmp,
            rule_version=rule_version,
            aggregation_policy=aggregation_policy,
        )


def _replay_one(
    *,
    run_id: str,
    row: dict[str, Any],
    replay_date: date,
    valid_sale: pa.Table,
    communities: pa.Table,
    buildings: pa.Table,
    market_series: pa.Table,
    window_days: int,
    rule_version: str,
    calibration_config: Path | None = None,
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY,
) -> dict[str, Any]:
    """单个目标成交的回放：构造 subject → 切分池 → 基准 → 估值 → 明细行。"""
    sale_event_id = _text(row.get("sale_event_id"))
    community_id = _text(row.get("community_id"))
    area = row.get("area_sqm")
    actual_unit_price = row.get("unit_price")

    pool = filter_pool(valid_sale, replay_date, sale_event_id)
    pool_size = pool.num_rows
    pool_matched = _count_matched(pool)
    baseline, baseline_count = simple_baseline(
        pool, community_id, replay_date, window_days
    )

    detail: dict[str, Any] = {
        "run_id": run_id,
        "replay_date": replay_date,
        "target_sale_event_id": sale_event_id,
        "community_id": community_id or "UNKNOWN",
        "source_id": _text(row.get("source_id")) or None,
        "area_sqm": float(area) if area is not None else None,
        "layout": _text(row.get("layout")) or None,
        "actual_unit_price": float(actual_unit_price)
        if actual_unit_price is not None
        else None,
        "estimate_center": None,
        "range_lower": None,
        "range_upper": None,
        "confidence": None,
        "business_status": None,
        "baseline_median": baseline,
        "baseline_count": baseline_count,
        "pool_size": pool_size,
        "pool_matched": pool_matched,
        "skip_reason": None,
        "rule_version": rule_version,
        "n_comps": None,
        "effective_samples": None,
        "max_weight_share": None,
        "dominant_flag": None,
        "aggregation_policy": aggregation_policy,
        "fallback_flag": None,
    }

    subject, skip_reason = _build_subject(row, replay_date)
    if subject is None:
        detail["skip_reason"] = skip_reason
        return detail

    outcome = _run_isolated_estimate(
        subject=subject,
        pool=pool,
        communities=communities,
        buildings=buildings,
        market_series=market_series,
        rule_version=rule_version,
        calibration_config=calibration_config,
        aggregation_policy=aggregation_policy,
    )
    detail["business_status"] = outcome.envelope.business_status
    result = outcome.result
    if result is not None:
        detail["estimate_center"] = float(result.center)
        detail["range_lower"] = (
            float(result.range_lower) if result.range_lower is not None else None
        )
        detail["range_upper"] = (
            float(result.range_upper) if result.range_upper is not None else None
        )
        detail["confidence"] = result.confidence.value
    diagnostics = outcome.diagnostics
    if diagnostics is not None:
        detail["n_comps"] = diagnostics.n_comps
        detail["effective_samples"] = diagnostics.effective_samples
        detail["max_weight_share"] = (
            float(diagnostics.max_weight_share)
            if diagnostics.max_weight_share is not None
            else None
        )
        detail["dominant_flag"] = diagnostics.dominant_flag
        detail["fallback_flag"] = diagnostics.center_fallback
    return detail


# ---------------------------------------------------------------------------
# 指标（§13.3）
# ---------------------------------------------------------------------------


def _quantile(values: Sequence[float], q: float) -> float:
    """确定性分位（线性索引取整），空序列禁止调用。"""
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


def compute_metrics(detail: pa.Table, high_quantile: float = 0.90) -> pa.Table:
    """§13.3 指标：APE 中位数/高分位、有符号误差与高估率、区间覆盖率、
    区间相对宽度、状态覆盖、基准可用性与基准误差。无数据 → 如实 0/None。

    返回 ``(metric, value, n)`` 长表：``metric`` 为指标名，``value`` 为数值
    （计数为浮点、比率/中位数为浮点或 None），``n`` 为该指标的有效样本数。
    """
    n = detail.num_rows
    if n == 0:
        return pa.table(
            {
                "metric": [
                    "n_targets",
                    "n_skipped",
                    "n_estimated",
                    "n_range",
                    "n_with_baseline",
                    "ape_median",
                    "ape_high_quantile",
                    "signed_error_median",
                    "overvaluation_rate",
                    "range_coverage_rate",
                    "range_relative_width_median",
                    "n_candidate",
                    "n_reference",
                    "n_formal",
                    "n_insufficient",
                    "baseline_ape_median",
                    "baseline_ape_high_quantile",
                ],
                "value": [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    None,
                    None,
                ],
                "n": [0] * 17,
            }
        )

    centers = detail.column("estimate_center").to_pylist()
    actuals = detail.column("actual_unit_price").to_pylist()
    lowers = detail.column("range_lower").to_pylist()
    uppers = detail.column("range_upper").to_pylist()
    statuses = detail.column("business_status").to_pylist()
    baselines = detail.column("baseline_median").to_pylist()
    skip_reasons = detail.column("skip_reason").to_pylist()

    n_skipped = sum(1 for s in skip_reasons if s)
    n_estimated = sum(1 for c in centers if c is not None)

    apes: list[float] = []
    signed_errors: list[float] = []
    for center, actual in zip(centers, actuals, strict=True):
        if center is None or actual is None or actual <= 0:
            continue
        ratio = (float(center) - float(actual)) / float(actual)
        apes.append(abs(ratio))
        signed_errors.append(ratio)

    overvaluation_rate = (
        sum(1 for s in signed_errors if s > 0) / len(signed_errors)
        if signed_errors
        else None
    )

    widths: list[float] = []
    covered = 0
    n_range = 0
    for center, lower, upper, actual in zip(
        centers, lowers, uppers, actuals, strict=True
    ):
        if center is None or lower is None or upper is None or actual is None:
            continue
        n_range += 1
        if lower <= actual <= upper:
            covered += 1
        if center > 0:
            widths.append((float(upper) - float(lower)) / float(center))

    range_coverage_rate = covered / n_range if n_range else None
    range_width_median = statistics.median(widths) if widths else None

    baseline_apes: list[float] = []
    n_with_baseline = 0
    for baseline, actual in zip(baselines, actuals, strict=True):
        if baseline is None or actual is None or actual <= 0:
            continue
        n_with_baseline += 1
        baseline_apes.append(abs(float(baseline) - float(actual)) / float(actual))

    counts: dict[str, int] = {}
    for status in statuses:
        counts[str(status)] = counts.get(str(status), 0) + 1

    return pa.table(
        {
            "metric": [
                "n_targets",
                "n_skipped",
                "n_estimated",
                "n_range",
                "n_with_baseline",
                "ape_median",
                "ape_high_quantile",
                "signed_error_median",
                "overvaluation_rate",
                "range_coverage_rate",
                "range_relative_width_median",
                "n_candidate",
                "n_reference",
                "n_formal",
                "n_insufficient",
                "baseline_ape_median",
                "baseline_ape_high_quantile",
            ],
            "value": [
                float(n),
                float(n_skipped),
                float(n_estimated),
                float(n_range),
                float(n_with_baseline),
                statistics.median(apes) if apes else None,
                _quantile(apes, high_quantile) if apes else None,
                statistics.median(signed_errors) if signed_errors else None,
                overvaluation_rate,
                range_coverage_rate,
                range_width_median,
                float(counts.get("候选", 0)),
                float(counts.get("参考", 0)),
                float(counts.get("正式", 0)),
                float(counts.get("信息不足", 0)),
                statistics.median(baseline_apes) if baseline_apes else None,
                _quantile(baseline_apes, high_quantile) if baseline_apes else None,
            ],
            "n": [
                n,
                n_skipped,
                n_estimated,
                n_range,
                n_with_baseline,
                len(apes),
                len(apes),
                len(signed_errors),
                len(signed_errors),
                n_range,
                len(widths),
                n,
                n,
                n,
                n,
                len(baseline_apes),
                len(baseline_apes),
            ],
        }
    )


# ---------------------------------------------------------------------------
# 分组指标（§13.3 分组口径：小区/户型/面积段/来源/可信度）
# ---------------------------------------------------------------------------


def area_band(area: float | None) -> str | None:
    """面积段（㎡）：<50 / 50-70 / 70-90 / 90-110 / 110-130 / >=130。"""
    if area is None:
        return None
    if area < 50:
        return "<50"
    if area < 70:
        return "50-70"
    if area < 90:
        return "70-90"
    if area < 110:
        return "90-110"
    if area < 130:
        return "110-130"
    return ">=130"


def grouped_metrics_schema() -> pa.Schema:
    """分组指标长表模式（每行 = 一个分组的指标，与整体指标同口径）。"""
    return pa.schema(
        [
            pa.field("group_dimension", pa.string(), nullable=False),
            pa.field("group_value", pa.string(), nullable=False),
            pa.field("metric", pa.string(), nullable=False),
            pa.field("value", pa.float64(), nullable=True),
            pa.field("n", pa.int32(), nullable=False),
        ]
    )


def _grouped_rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    """分组行（dict 列表）→ 按固定 schema 的分组表（列缺失/多余即报错）。"""
    names = grouped_metrics_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    return pa.table(columns, schema=grouped_metrics_schema())


def compute_grouped_metrics(detail: pa.Table, high_quantile: float = 0.90) -> pa.Table:
    """按小区/户型/面积段/来源/可信度分组计算 §13.3 指标（不隐藏子组失败）。

    每个分组复用与整体相同的指标口径（``compute_metrics``）；分组维度值缺失
    （None）不单列分组。空明细 → 空分组表（保持 schema，不虚构分组）。
    """
    rows: list[dict[str, Any]] = []
    if detail.num_rows == 0:
        names = grouped_metrics_schema().names
        return pa.table({name: [] for name in names}, schema=grouped_metrics_schema())

    community_ids = detail.column("community_id").to_pylist()
    layouts = detail.column("layout").to_pylist()
    areas = detail.column("area_sqm").to_pylist()
    source_ids = (
        detail.column("source_id").to_pylist()
        if "source_id" in detail.column_names
        else [None] * detail.num_rows
    )
    confidences = detail.column("confidence").to_pylist()
    dims: list[tuple[str, list[str | None]]] = [
        ("community_id", community_ids),
        ("layout", layouts),
        ("area_band", [area_band(float(a)) if a is not None else None for a in areas]),
        ("source_id", source_ids),
        ("confidence", confidences),
    ]
    for dimension, values in dims:
        for value in sorted({v for v in values if v is not None}):
            keep = [i for i, v in enumerate(values) if v == value]
            subset = detail.take(keep)
            group_metrics = compute_metrics(subset, high_quantile)
            for metric, val, count in zip(
                group_metrics.column("metric").to_pylist(),
                group_metrics.column("value").to_pylist(),
                group_metrics.column("n").to_pylist(),
                strict=True,
            ):
                rows.append(
                    {
                        "group_dimension": dimension,
                        "group_value": str(value),
                        "metric": str(metric),
                        "value": val,
                        "n": int(count),
                    }
                )
    return _grouped_rows_to_table(rows)


def over_performance_groups(
    grouped: pa.Table, overall_metrics: pa.Table
) -> list[tuple[str, str, float, float]]:
    """材料性失败线索：APE 中位数高于整体的分组（如实报告，非校准阈值）。

    返回 ``(dimension, value, group_ape_median, overall_ape_median)``；整体无
    APE 中位数或分组样本不足（n_estimated < 1）时不标注。仅作"需关注子组"
    提示，不作已校准门槛（WP8-C 门槛由用户确认）。
    """
    overall_ape: float | None = None
    for metric, value in zip(
        overall_metrics.column("metric").to_pylist(),
        overall_metrics.column("value").to_pylist(),
        strict=True,
    ):
        if metric == "ape_median" and value is not None:
            overall_ape = float(value)
    if overall_ape is None:
        return []

    flags: list[tuple[str, str, float, float]] = []
    rows = grouped.to_pylist()
    by_group: dict[tuple[str, str], dict[str, float]] = {}
    for row in rows:
        by_group.setdefault(
            (str(row["group_dimension"]), str(row["group_value"])), {}
        )[str(row["metric"])] = row["value"]
    for (dimension, value), metrics in sorted(by_group.items()):
        ape = metrics.get("ape_median")
        n_est = int(metrics.get("n_estimated", 0) or 0)
        if ape is None or n_est < 1:
            continue
        if float(ape) > overall_ape:
            flags.append((dimension, value, float(ape), overall_ape))
    return flags


# ---------------------------------------------------------------------------
# 写盘（原子写入 + DerivedManifest + §12.2 运行清单）
# ---------------------------------------------------------------------------


def _sha256_of(path: Path) -> str:
    """产物内容哈希（§12.2 产物清单哈希；可复现性证据）。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_table(table: pa.Table, path: Path, manifest: DerivedManifest) -> Path:
    final = path
    work = final.with_name(final.name + ".incomplete")
    pq.write_table(table, work, compression="zstd")
    write_derived_manifest(manifest, final)
    work.replace(final)
    return final


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BacktestOutcome:
    """一次 ``compsval backtest run`` 的结果（供 CLI 打印与测试断言）。"""

    run_id: str
    detail_path: Path
    metrics_path: Path
    grouped_path: Path
    run_manifest_path: Path
    detail: pa.Table
    metrics: pa.Table
    grouped: pa.Table
    data_version: str


def run_backtest(config: BacktestConfig, data_dir: Path) -> BacktestOutcome:
    """滚动历史回放主入口：切分 → 逐点估值 + 基准 → 指标 → 写盘（含运行清单）。

    必要数据表缺失 → ``MissingDependencyError``（退出码 3）。回放产物写
    ``out_dir``（默认 ``data_dir/backtest``），不触碰 raw/staged/marts/
    entities/valuation 既有表与 05-估值报告。
    """
    valid_sale_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    community_path = (
        data_dir / entities_community.ENTITIES_LAYER / entities_community.COMMUNITY_FILENAME
    )
    building_path = (
        data_dir / entities_community.ENTITIES_LAYER / entities_building.BUILDING_FILENAME
    )
    market_path = (
        data_dir
        / entities_community.ENTITIES_LAYER
        / f"{entities_market_series.MARKET_TABLE}.parquet"
    )
    for path, what in (
        (valid_sale_path, "valid_sale"),
        (community_path, "community"),
        (building_path, "building"),
        (market_path, "market_series"),
    ):
        if not path.is_file():
            raise MissingDependencyError(f"{what} 表缺失：{path}")

    valid_sale = pq.read_table(valid_sale_path)
    communities = pq.read_table(community_path)
    buildings = pq.read_table(building_path)
    market_series = pq.read_table(market_path)

    data_version = _data_version_of(valid_sale_path)
    # §10.4 版本一致性预检（验收⑤：数据版本不一致 → 退出码 4 拒绝继续）
    if (
        config.expected_data_version is not None
        and data_version != config.expected_data_version
    ):
        raise VersionMismatchError(
            f"数据版本 {data_version} 与配置期望 {config.expected_data_version} 不一致"
        )
    dates = _replay_dates(config, valid_sale)
    run_id = _run_id(data_version, config.rule_version, dates, config.aggregation_policy)
    window_days = config.baseline_window_months * _DAYS_PER_MONTH

    # 校准版本（1.1 起）预检并定位校准配置（缺失 → 退出码 3，不静默降级）。
    calibration_config: Path | None = None
    if config.rule_version != LEGACY_RULE_VERSION:
        cfg_path = calibration_config_path(data_dir, config.rule_version)
        if not cfg_path.is_file():
            raise MissingDependencyError(
                f"区间校准配置缺失（规则版本 {config.rule_version} 需要校准资产）：{cfg_path}"
            )
        calibration_config = cfg_path

    sales = valid_sale.to_pylist()
    by_date: dict[date, list[dict[str, Any]]] = {}
    for row in sales:
        sale_date = row.get("sale_date")
        if isinstance(sale_date, date):
            by_date.setdefault(sale_date, []).append(row)

    rows: list[dict[str, Any]] = []
    for replay_date in dates:
        for row in by_date.get(replay_date, []):
            rows.append(
                _replay_one(
                    run_id=run_id,
                    row=row,
                    replay_date=replay_date,
                    valid_sale=valid_sale,
                    communities=communities,
                    buildings=buildings,
                    market_series=market_series,
                    window_days=window_days,
                    rule_version=config.rule_version,
                    calibration_config=calibration_config,
                    aggregation_policy=config.aggregation_policy,
                )
            )
    detail = (
        _rows_to_detail(rows) if rows else _empty_detail()
    )
    metrics = compute_metrics(detail, config.high_quantile)
    grouped = compute_grouped_metrics(detail, config.high_quantile)

    out_dir = config.out_dir or (data_dir / BACKTEST_LAYER)
    out_dir.mkdir(parents=True, exist_ok=True)
    detail_path = out_dir / DETAIL_FILENAME
    metrics_path = out_dir / METRICS_FILENAME
    grouped_path = out_dir / GROUPED_FILENAME

    inputs: list[InputRef] = []
    manifest_path = valid_sale_path.with_suffix(".manifest.json")
    if manifest_path.is_file():
        try:
            inputs = list(read_derived_manifest(valid_sale_path).inputs)
        except Exception:  # noqa: BLE001 - 溯源缺失不虚构
            inputs = []

    _write_table(
        detail,
        detail_path,
        DerivedManifest(
            layer=BACKTEST_LAYER,
            table=DETAIL_TABLE,
            built_at=datetime.now(UTC),
            row_count=detail.num_rows,
            inputs=list(inputs),
            package_version=__version__,
            notes=f"WP8-A 回放明细（BT-001；run {run_id}）",
        ),
    )
    _write_table(
        metrics,
        metrics_path,
        DerivedManifest(
            layer=BACKTEST_LAYER,
            table=METRICS_TABLE,
            built_at=datetime.now(UTC),
            row_count=metrics.num_rows,
            inputs=list(inputs),
            package_version=__version__,
            notes=f"WP8-A 回放指标（BT-001；run {run_id}）",
        ),
    )
    _write_table(
        grouped,
        grouped_path,
        DerivedManifest(
            layer=BACKTEST_LAYER,
            table=GROUPED_TABLE,
            built_at=datetime.now(UTC),
            row_count=grouped.num_rows,
            inputs=list(inputs),
            package_version=__version__,
            notes=f"WP8-B 回放分组指标（§13.3；run {run_id}）",
        ),
    )

    warnings: list[str] = []
    metrics_by_name = {
        str(metric): value
        for metric, value in zip(
            metrics.column("metric").to_pylist(),
            metrics.column("value").to_pylist(),
            strict=True,
        )
    }
    n_estimated = int(metrics_by_name.get("n_estimated") or 0)
    n_skipped = int(metrics_by_name.get("n_skipped") or 0)
    n_targets = int(metrics_by_name.get("n_targets") or 0)
    if n_estimated == 0:
        warnings.append("无任何回放校验样本，回放覆盖为 0，不得据此校准数值门槛")
    if n_skipped > 0:
        warnings.append(f"{n_skipped} 个目标成交因未匹配小区/缺必要字段被跳过（如实报告）")
    if n_targets == 0:
        warnings.append("回放目标为空（无成交），回放为空运行")

    over_groups = over_performance_groups(grouped, metrics)

    run_manifest_path = out_dir / RUN_MANIFEST_FILENAME
    run_manifest: dict[str, Any] = {
        "run_id": run_id,
        "command": "backtest run",
        "code_version": __version__,
        "data_version": data_version,
        "rule_version": config.rule_version,
        "parameters": {
            "replay_dates": [d.isoformat() for d in dates],
            "replay_dates_auto": not bool(config.replay_dates),
            "baseline_window_months": config.baseline_window_months,
            "high_quantile": config.high_quantile,
            "expected_data_version": config.expected_data_version,
            "aggregation_policy": config.aggregation_policy,
        },
        "run_at": datetime.now(UTC).isoformat(),
        "warnings": warnings,
        "over_performance_groups": [
            {"group_dimension": d, "group_value": v, "group_ape_median": g, "overall_ape_median": o}
            for d, v, g, o in over_groups
        ],
        "artifacts": [
            {
                "path": str(detail_path),
                "sha256": _sha256_of(detail_path),
            },
            {
                "path": str(metrics_path),
                "sha256": _sha256_of(metrics_path),
            },
            {
                "path": str(grouped_path),
                "sha256": _sha256_of(grouped_path),
            },
        ],
    }
    work = run_manifest_path.with_name(RUN_MANIFEST_FILENAME + ".incomplete")
    work.write_text(
        json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    work.replace(run_manifest_path)

    return BacktestOutcome(
        run_id=run_id,
        detail_path=detail_path,
        metrics_path=metrics_path,
        grouped_path=grouped_path,
        run_manifest_path=run_manifest_path,
        detail=detail,
        metrics=metrics,
        grouped=grouped,
        data_version=data_version,
    )
