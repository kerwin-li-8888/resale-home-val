"""WP7-B 端到端估值命令（REP-001 执行）：``compsval estimate`` 编排器。

技术方案 §10.2/§10.3/§10.4 + §9 估值执行顺序：校验 subject → 固定
valuation_date/数据截点（``--as-of`` 一致性）→ 串联 WP6 全链路
（候选 → 层级/相似度 → 时间 → 差异 → 汇总/可信度/状态）→ 输出 §10.3
统一包络 → 冻结估值 JSON 落盘。非交互、禁止网络。

设计要点（对应 WP7-B 验收标准）：

- **一次命令端到端（验收①）**：从 subject JSON 直接产出冻结估值 JSON
  （§10.3 包络全字段），stdout 只写机器可解析 JSON；
- **--as-of 一致性（验收②）**：与 subject ``valuation_date`` 不一致 →
  ``InvalidInputError``（退出码 2），不写任何产物；
- **依赖分级（验收③）**：必要数据表缺失 → ``MissingDependencyError``
  （退出码 3）；汇总结果规则版本与当前不一致 → ``VersionMismatchError``
  （退出码 4）；
- **可重复（验收④）**：复用 WP6 确定性 ``run_id_of`` 与原子写盘，同一
  输入/数据/规则重复运行产出同一 run 与同一结果；
- **冻结一致性（验收⑤）**：冻结 JSON 的估值字段直接来自
  ``valuation_result`` 表（同一冻结），report build（WP7-C）据此还原；
- **业务降级不静默吞（§10.3）**：无入选可比时输出
  ``business_status=信息不足`` 的 success 包络并冻结该判定，不冒充失败
  也不虚构估值。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

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
from compsval.ingest.manifests import InputRef, read_derived_manifest
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
    OutputEnvelope,
    VersionMismatchError,
)
from compsval.valuation.aggregation import (
    DEFAULT_AGGREGATION_POLICY,
    ValuationResultTabled,
    apply_aggregation,
)
from compsval.valuation.candidate import (
    DEFAULT_RULE_VERSION,
    build_valuation,
)
from compsval.valuation.comparable import apply_tiers_to_candidate_table
from compsval.valuation.difference import apply_difference_adjustments
from compsval.valuation.scope import (
    ACTIVE_SCOPE_POLICY_VERSION,
    scope_policy_filename,
)
from compsval.valuation.time_adjustment import apply_time_adjustments

#: 冻结估值输出根目录：README §11 建议的项目级 ``05-估值报告``。
DEFAULT_REPORTS_ROOT = Path(__file__).resolve().parents[4] / "05-估值报告"

#: 冻结估值 JSON 文件名。
ESTIMATE_FILENAME = "estimate.json"

#: 发布决定记录（受控 formal 启用路径，CX-WP9-02 修复）：记录 RELEASE1-001
#: 用户发布决定的运行开关文件，位于 ``<data_dir>/release/release_decision.json``。
#: 治理记录（用户决策证据）在 ``04-校验/G5-发布门禁证据-V0.1.md``；本文件只
#: 是其运行时载体。缺失/无效/未发布一律视为未启用（保守：保持候选/参考）。
RELEASE_LAYER = "release"
RELEASE_DECISION_FILENAME = "release_decision.json"

#: 发布决定记录必需字段（名称 → 类型；str 字段还须非空白）。
_RELEASE_DECISION_FIELDS: tuple[tuple[str, type], ...] = (
    ("decision_id", str),
    ("released", bool),
    ("decided_at", str),
    ("decided_by", str),
    ("gate_evidence", str),
)


@dataclass(frozen=True)
class EstimateDiagnostics:
    """中心构造诊断（add-aggregator-policy-experiment；供回放明细列填充）。

    只随内存结果对象传递，SHALL NOT 进入冻结估值 JSON（默认路径产物逐字节不变）。
    """

    n_comps: int
    effective_samples: float
    max_weight_share: Decimal | None
    dominant_flag: bool
    center_fallback: bool
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY


@dataclass(frozen=True)
class EstimateOutcome:
    """一次 ``compsval estimate`` 的结果（供 CLI 打印与测试断言）。"""

    envelope: OutputEnvelope
    estimate_path: Path
    run_id: str
    result: ValuationResultTabled | None  # None 表示无数值结果（信息不足）
    diagnostics: EstimateDiagnostics | None = None  # None = 无数值结果或未启用


def _read_or_raise(path: Path, what: str) -> pa.Table:
    """读必需数据表；缺失 → MissingDependencyError（退出码 3）。"""
    if not path.is_file():
        raise MissingDependencyError(f"{what} 缺失：{path}")
    return pq.read_table(path)


def _result_payload(
    result: ValuationResultTabled, n_comps: int, effective_samples: float
) -> dict[str, Any]:
    """估值结果 → 冻结 JSON 的机器可解析对象（与 valuation_result 表一致）。"""
    return {
        "result_id": result.result_id,
        "run_id": result.run_id,
        "subject_id": result.subject_id,
        "center": float(result.center),
        "range": [
            float(result.range_lower) if result.range_lower is not None else None,
            float(result.range_upper) if result.range_upper is not None else None,
        ],
        "confidence": result.confidence.value,
        "status": result.status.value,
        "valuation_date": result.valuation_date.isoformat(),
        "rule_version": result.rule_version,
        "reason": result.reason,
        "n_comps": n_comps,
        "effective_samples": effective_samples,
    }


def _in_formal_scope(data_dir: Path, community_id: str) -> bool:
    """目标小区是否在适用范围（ScopePolicy 纳入名单）内（G5 formal 双闸）。

    - ScopePolicy 表缺失 → False（保守：范围未固定不输出正式）；
    - 小区不在名单 → False（名录外不纳入 formal，README §3.2/§3.3）；
    - 仅在 ``scope_decision == 纳入`` 时返回 True。

    读取版本 = ``ACTIVE_SCOPE_POLICY_VERSION``（v1.2，2026-09-01 由 user
    正式基线确认启用；此前维持 v1.1，见 scope-policy-rebaseline spec）。
    """
    path = data_dir / entities_community.ENTITIES_LAYER / scope_policy_filename(
        ACTIVE_SCOPE_POLICY_VERSION
    )
    if not path.is_file():
        return False
    try:
        rows = pq.read_table(path).to_pylist()
    except Exception:  # noqa: BLE001 - 范围表损坏按未固定处理（保守）
        return False
    for row in rows:
        if str(row.get("community_id")) == community_id:
            return str(row.get("scope_decision")) == "纳入"
    return False


@dataclass(frozen=True)
class ReleaseDecision:
    """受控 formal 启用判定结果（``load_release_decision`` 输出）。

    - ``recorded``：发布决定记录文件是否存在（存在但无效时告警可见）；
    - ``enabled``：仅在记录存在、字段完整、``released=true`` 且
      ``decided_at`` 为合法 ISO 日期时为 True；
    - ``detail``：判定依据（进包络 warning，可审计）。
    """

    recorded: bool
    enabled: bool
    detail: str


def release_decision_path(data_dir: Path) -> Path:
    """发布决定记录路径：``<data_dir>/release/release_decision.json``。"""
    return data_dir / RELEASE_LAYER / RELEASE_DECISION_FILENAME


def load_release_decision(data_dir: Path) -> ReleaseDecision:
    """读发布决定记录 → formal 启用判定（CX-WP9-02 受控启用路径）。

    保守原则：任何缺失/损坏/未授权情形都返回 ``enabled=False``，官方 CLI
    入口保持候选/参考输出；只有完整的已发布记录（RELEASE1-001 用户明确
    决定，治理证据见 04-校验/G5-发布门禁证据）才允许输出 formal。CLI 不
    提供 ``--formal`` 之类的自由开关——启用只能经由该记录文件。
    """
    path = release_decision_path(data_dir)
    if not path.is_file():
        return ReleaseDecision(False, False, f"发布决定记录缺失（{path}），保持候选/参考")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        return ReleaseDecision(True, False, f"发布决定记录不可解析（{exc}），保持候选/参考")
    if not isinstance(payload, dict):
        return ReleaseDecision(True, False, "发布决定记录必须为 JSON 对象，保持候选/参考")
    for name, expected in _RELEASE_DECISION_FIELDS:
        value = payload.get(name)
        if not isinstance(value, expected):
            return ReleaseDecision(
                True,
                False,
                f"发布决定记录字段 {name} 缺失或类型不符（{value!r}），保持候选/参考",
            )
        if isinstance(value, str) and not value.strip():
            return ReleaseDecision(
                True, False, f"发布决定记录字段 {name} 为空白，保持候选/参考"
            )
    decided_at = payload["decided_at"]
    try:
        date.fromisoformat(str(decided_at))
    except ValueError:
        return ReleaseDecision(
            True, False, f"发布决定记录 decided_at 非 ISO 日期（{decided_at!r}），保持候选/参考"
        )
    if payload["released"] is not True:
        return ReleaseDecision(
            True, False, f"发布决定记录 released={payload['released']!r}，保持候选/参考"
        )
    return ReleaseDecision(
        True,
        True,
        "发布决定记录生效："
        f"{payload['decision_id']}（{payload['decided_at']}，{payload['decided_by']}；"
        f"{payload['gate_evidence']}）",
    )


def _write_frozen_estimate(envelope: OutputEnvelope, run_id: str, out_root: Path) -> Path:
    """原子写冻结估值 JSON（UTF-8；同 run 再生覆盖，不同 run 各自留档）。"""
    out_dir = out_root / f"valuation_id={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / ESTIMATE_FILENAME
    work = out_dir / (ESTIMATE_FILENAME + ".incomplete")
    work.write_text(envelope.model_dump_json(indent=2), encoding="utf-8")
    work.replace(path)
    return path


def _insufficient_outcome(
    run_id: str,
    data_version: str,
    rule_version: str,
    out_root: Path | None,
) -> EstimateOutcome:
    """无入选可比时的业务降级结果：信息不足（success 包络，冻结留档）。"""
    envelope = OutputEnvelope(
        command="estimate",
        business_status="信息不足",
        run_id=run_id,
        data_version=data_version,
        rule_version=rule_version,
        warnings=["无入选可比案例，无法进行汇总（拒绝虚构估值）"],
    )
    estimate_path = _write_frozen_estimate(envelope, run_id, out_root or DEFAULT_REPORTS_ROOT)
    envelope.add_artifact(str(estimate_path))
    return EstimateOutcome(envelope, estimate_path, run_id, None)


def run_estimate(
    *,
    subject: SubjectProperty,
    data_dir: Path,
    as_of: date | None = None,
    out_root: Path | None = None,
    rule_version: str = DEFAULT_RULE_VERSION,
    formal_release_enabled: bool = False,
    aggregation_policy: str = DEFAULT_AGGREGATION_POLICY,
) -> EstimateOutcome:
    """端到端估值：subject JSON → 冻结估值 JSON（WP6 全链路编排）。

    不改写 WP6 策略模块逻辑，只按技术方案 §9.2 执行顺序编排；数据截点固定为
    subject ``valuation_date``（``--as-of`` 已一致性校验）。formal 输出受发布
    门禁双闸控制：``formal_release_enabled``（G5 通过前为 False，只输出候选/
    参考）+ 目标小区在 ScopePolicy 纳入范围（``_in_formal_scope``）。
    ``aggregation_policy``（默认 = 现行为）只切换汇总中心构造，默认路径产物
    与既有实现逐值等价；未登记策略由聚合层拒绝（不静默回退）。
    """
    # 验收②：--as-of 与 subject valuation_date 一致性
    if as_of is not None and as_of != subject.valuation_date:
        raise InvalidInputError(
            f"--as-of {as_of.isoformat()} 与 subject.valuation_date "
            f"{subject.valuation_date.isoformat()} 不一致"
        )

    # 验收③：必需数据表预检（缺失 → 退出码 3）
    valid_sale = _read_or_raise(data_dir / MARTS_LAYER / VALID_SALE_FILENAME, "valid_sale 表")
    communities = _read_or_raise(
        data_dir / entities_community.ENTITIES_LAYER / entities_community.COMMUNITY_FILENAME,
        "community 表",
    )
    market_series = _read_or_raise(
        data_dir
        / entities_community.ENTITIES_LAYER
        / f"{entities_market_series.MARKET_TABLE}.parquet",
        "market_series 表",
    )
    buildings = _read_or_raise(
        data_dir / entities_community.ENTITIES_LAYER / entities_building.BUILDING_FILENAME,
        "building 表",
    )

    # 溯源 input_refs（valid_sale manifest；缺失则不注入，不虚构）
    inputs: list[InputRef] = []
    manifest_path = (data_dir / MARTS_LAYER / VALID_SALE_FILENAME).with_suffix(".manifest.json")
    if manifest_path.is_file():
        inputs = list(read_derived_manifest(data_dir / MARTS_LAYER / VALID_SALE_FILENAME).inputs)

    # §9.2 顺序：候选 → 层级/相似度 → 时间 → 差异 → 汇总
    build = build_valuation(
        subject,
        data_dir=data_dir,
        rule_version=rule_version,
        data_cutoff=subject.valuation_date,
        notes=f"WP7-B: compsval estimate 端到端（{__version__}）",
    )
    tier = apply_tiers_to_candidate_table(
        data_dir=data_dir,
        subject=subject,
        valid_sale=valid_sale,
        communities=communities,
        input_refs=inputs,
        rule_version=rule_version,
    )

    # 无入选可比：业务降级为“信息不足”，不冒充失败也不虚构估值（§10.3）；
    # 提前短路跳过 时间/差异/汇总（对空候选无意义）。
    if not any(c.selected for c in tier.candidates):
        return _insufficient_outcome(
            build.run.run_id, build.run.data_version, rule_version, out_root
        )

    apply_time_adjustments(
        data_dir=data_dir,
        subject=subject,
        valid_sale=valid_sale,
        market_series=market_series,
        communities=communities,
        input_refs=inputs,
        rule_version=rule_version,
    )
    apply_difference_adjustments(
        data_dir=data_dir,
        subject=subject,
        valid_sale=valid_sale,
        buildings=buildings,
        input_refs=inputs,
        rule_version=rule_version,
    )
    try:
        outcome = apply_aggregation(
            data_dir=data_dir,
            subject=subject,
            valid_sale=valid_sale,
            input_refs=inputs,
            rule_version=rule_version,
            formal_release_enabled=formal_release_enabled,
            in_formal_scope=_in_formal_scope(data_dir, subject.community_id),
            aggregation_policy=aggregation_policy,
        )
    except ValueError as exc:
        # 兜底：聚合仍无可比 → 信息不足
        envelope = OutputEnvelope(
            command="estimate",
            business_status="信息不足",
            run_id=build.run.run_id,
            data_version=build.run.data_version,
            rule_version=rule_version,
            warnings=[f"无入选可比案例：{exc}"],
        )
        estimate_path = _write_frozen_estimate(
            envelope, build.run.run_id, out_root or DEFAULT_REPORTS_ROOT
        )
        envelope.add_artifact(str(estimate_path))
        return EstimateOutcome(envelope, estimate_path, build.run.run_id, None)

    # 验收③：规则版本不一致 → 退出码 4（拒绝继续）
    if outcome.result.rule_version != rule_version:
        raise VersionMismatchError(
            f"汇总结果规则版本 {outcome.result.rule_version} 与当前 {rule_version} 不一致"
        )

    envelope = OutputEnvelope(
        command="estimate",
        business_status=outcome.result.status.value,
        run_id=outcome.result.run_id,
        data_version=build.run.data_version,
        rule_version=outcome.result.rule_version,
        result=_result_payload(outcome.result, outcome.n_comps, outcome.effective_samples),
    )
    estimate_path = _write_frozen_estimate(
        envelope, outcome.result.run_id, out_root or DEFAULT_REPORTS_ROOT
    )
    envelope.add_artifact(str(estimate_path))
    diagnostics = EstimateDiagnostics(
        n_comps=outcome.n_comps,
        effective_samples=outcome.effective_samples,
        max_weight_share=outcome.max_weight_share,
        dominant_flag=outcome.dominant_flag,
        center_fallback=outcome.center_fallback,
        aggregation_policy=outcome.policy,
    )
    return EstimateOutcome(
        envelope, estimate_path, outcome.result.run_id, outcome.result, diagnostics
    )
