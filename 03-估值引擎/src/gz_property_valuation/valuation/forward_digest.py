"""转发候选消化（digest-forwarded-candidates；rule 1.4）。

两个机制（design D1/D4）：

- **机制一 价格层次条件化消化**：v2 账本中经 merged 转发并入迁入方的案例
  （冻结清单，sale_event_id 级）在估计时点入选池内存在价位位势。以
  ``r = median(清单内入选案例单价) / median(清单外入选案例单价)`` 估计位势；
  清单内入选数 ≥ ``min_marked_cases`` 且 ``|1/r−1| ≥ trigger_gap`` 时，对
  清单内入选案例追加 ``comp_adjustment`` 行（``adjustment_type="转发折算"``、
  ``factor=1/r``，乘法口径叠加于时间/差异因子，与子区修正同模式）；
  ``|1/r−1| > max_gap`` 降级不折算并留痕。
- **机制二 转发源候选补偿**：目标小区命中冻结映射的转发源且同小区候选不足
  （< 现行最低有效案例数）时，层级判定改以迁入方小区身份进行（视同合并后
  成员）；补偿来源与理由经包络 warning 与 stage meta 留痕。

资产（design D2）：``<data_dir>/rules/forward_digest.v1.json`` + 同名
``.sha256`` sidecar（内容哈希强校验）。显式失败（不静默回退）：资产缺失、
哈希不符、参数未配置（占位）。账本语义门（design D2）：活动池中转发源
小区行数 = 0（v2 语义，转发已生效）→ 启用；> 0（v1 语义，转发未生效）→
禁用（no-op 留痕）——这是「rule 1.4 在 v1 账本上与 rule 1.3 逐位一致」的
机制保证，且对时间外过滤池/隔离 workdir 同样成立。资产内的全量账本哈希
为血缘元数据。

确定性：中位数与冻结查表，无随机源；同输入重跑逐位一致。全程只读
marts/entities 与 valuation 中间表，不改写冻结估值。
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.parquet as pq

from gz_property_valuation import __version__
from gz_property_valuation.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from gz_property_valuation.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from gz_property_valuation.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from gz_property_valuation.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
    CompCandidate,
)
from gz_property_valuation.valuation.time_adjustment import (
    COMP_ADJUSTMENT_FILENAME,
    comp_adjustment_schema,
)

#: 转发消化规则版本。
DIGEST_RULE_VERSION: Final = "1.4"

#: 消化资产文件名（``<data_dir>/rules/`` 下；哈希登记在同名 ``.sha256``）。
ASSET_FILENAME: Final = "forward_digest.v1.json"

#: comp_adjustment 的 adjustment_type 标识（与子区修正 "子区" 并列）。
ADJUSTMENT_TYPE: Final = "转发折算"

#: 阶段元数据文件名（``data/valuation/`` 下，复核可见）。
STAGE_META_FILENAME: Final = "forward_digest_stage_meta.json"

#: 现行最低有效案例数（补偿触发门槛，与 aggregation/subarea 同口径）。
MIN_EFFECTIVE_SAMPLES: Final = 3

#: 判定结论（留痕与测试断言用）。
DECISION_APPLIED: Final = "折算"
DECISION_OVER_CAP: Final = "超上限降级"
DECISION_BELOW_TRIGGER: Final = "低于触发阈值"
DECISION_BELOW_MIN: Final = "清单内案例不足"
DECISION_DISABLED: Final = "禁用"


def _file_sha256(path: Path) -> str:
    """文件内容 sha256（按 (路径, mtime_ns, size) 缓存；同输入重复调用零开销）。"""
    stat = path.stat()
    key = (str(path), stat.st_mtime_ns, stat.st_size)
    cached = _FILE_SHA_CACHE.get(key)
    if cached is None:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        cached = digest.hexdigest()
        _FILE_SHA_CACHE[key] = cached
    return cached


_FILE_SHA_CACHE: dict[tuple[str, int, int], str] = {}


@dataclass(frozen=True)
class ForwardDigestState:
    """消化资产加载结果（含账本哈希门判定）。"""

    enabled: bool
    disabled_reason: str | None
    parameters: Mapping[str, Any]
    event_map: Mapping[str, tuple[str, str]]  # sale_event_id -> (src_cid, dst_cid)
    src_to_dst: Mapping[str, str]
    asset_sha256: str


@dataclass(frozen=True)
class ForwardDigestResult:
    """一次价格消化应用的结论（供包络 warning 与测试断言）。"""

    applied: bool
    decision: str
    detail: str
    r: float | None = None
    n_marked: int = 0
    n_unmarked: int = 0
    warnings: tuple[str, ...] = ()


def _parse_parameters(payload: dict[str, Any]) -> Mapping[str, Any]:
    """参数段解析：占位（null/缺 key）→ InvalidInputError（显式失败）。"""
    raw = payload.get("parameters")
    if not isinstance(raw, dict):
        raise InvalidInputError("转发消化资产 parameters 缺失（参数未配置）")
    for key in ("min_marked_cases", "trigger_gap", "max_gap"):
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise InvalidInputError(
                f"转发消化参数未配置：{key}（占位状态不可运行，需沙箱证据与用户确认后写入）"
            )
    if raw.get("status") not in ("trial", "production"):
        raise InvalidInputError(
            f"转发消化参数 status 非法（须为 trial/production）：{raw.get('status')!r}"
        )
    return raw


def load_forward_digest(
    data_dir: Path,
    *,
    rule_version: str,
    valid_sale: pa.Table | None = None,
) -> ForwardDigestState:
    """加载消化资产并做账本语义门判定。

    - ``rule_version != 1.4`` → 禁用（no-op，零开销路径）；
    - 资产缺失 / sidecar 哈希不符 / 参数占位 → 显式失败
      （``MissingDependencyError`` / ``InvalidInputError``）；
    - 账本语义门（design D2：转发源存在性）：活动池中转发源小区行数 = 0
      → v2 语义，启用；> 0 → v1 语义，禁用（no-op 留痕，这是「rule 1.4 在
      v1 账本上与 rule 1.3 逐位一致」的机制保证）。资产内的全量账本哈希
      登记为血缘元数据，不作运行时门禁（时间外过滤池/隔离 workdir 的池
      文件哈希天然不同于全量账本）。
    """
    if rule_version != DIGEST_RULE_VERSION:
        return ForwardDigestState(
            enabled=False,
            disabled_reason=f"规则版本 {rule_version} 非转发消化版本（{DIGEST_RULE_VERSION}）",
            parameters={},
            event_map={},
            src_to_dst={},
            asset_sha256="",
        )
    asset_path = data_dir / "rules" / ASSET_FILENAME
    if not asset_path.is_file():
        raise MissingDependencyError(f"转发消化资产缺失（rule 1.4 需要）：{asset_path}")
    payload = asset_path.read_bytes()
    asset_sha = hashlib.sha256(payload).hexdigest()
    sidecar = asset_path.with_suffix(".json.sha256")
    if not sidecar.is_file():
        raise MissingDependencyError(f"转发消化资产哈希登记缺失：{sidecar}")
    expected = sidecar.read_text(encoding="utf-8").strip().split()[0]
    if asset_sha != expected:
        raise MissingDependencyError(
            f"转发消化资产哈希不符（资产 {asset_sha[:12]}… != 登记 {expected[:12]}…）"
        )
    try:
        data = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidInputError(f"转发消化资产不可解析：{exc}") from exc
    if not isinstance(data, dict) or data.get("rule_version") != DIGEST_RULE_VERSION:
        raise InvalidInputError(
            f"转发消化资产 rule_version 非法：{data.get('rule_version')!r}"
        )
    parameters = _parse_parameters(data)
    event_map: dict[str, tuple[str, str]] = {}
    for row in data.get("forwarded") or []:
        event_map[str(row["sale_event_id"])] = (str(row["src_cid"]), str(row["dst_cid"]))
    if not event_map:
        raise InvalidInputError("转发消化资产转发清单为空")
    src_to_dst = {src: dst for src, dst in event_map.values()}

    # 账本语义门：活动池中转发源小区的行数（0 = v2 语义启用；>0 = v1 语义禁用）
    table = valid_sale
    if table is None:
        ledger_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
        if not ledger_path.is_file():
            raise MissingDependencyError(f"活动账本缺失：{ledger_path}")
        table = pq.read_table(ledger_path, columns=["community_id"])
    src_set = set(src_to_dst)
    src_rows = 0
    if "community_id" in table.column_names:
        for cid in table.column("community_id").to_pylist():
            if cid is not None and str(cid) in src_set:
                src_rows += 1
    if src_rows == 0:
        return ForwardDigestState(
            enabled=True,
            disabled_reason=None,
            parameters=parameters,
            event_map=event_map,
            src_to_dst=src_to_dst,
            asset_sha256=asset_sha,
        )
    return ForwardDigestState(
        enabled=False,
        disabled_reason=(
            f"活动池仍含转发源小区行（{src_rows} 行 > 0），判定为 v1 语义，"
            "消化禁用（no-op）"
        ),
        parameters=parameters,
        event_map=event_map,
        src_to_dst=src_to_dst,
        asset_sha256=asset_sha,
    )


def compensated_subject_community(
    state: ForwardDigestState,
    subject: Any,
    candidates: Sequence[CompCandidate],
    *,
    min_effective_samples: int = MIN_EFFECTIVE_SAMPLES,
) -> tuple[str | None, dict[str, Any]]:
    """机制二：转发源候选补偿判定。

    目标小区命中冻结映射的转发源、且同小区候选数 < 最低有效案例数时，
    返回迁入方小区 id（层级判定以该身份进行，视同合并后成员）；否则返回
    ``None`` 与留痕 trace。
    """
    trace: dict[str, Any] = {
        "mechanism": "转发源候选补偿",
        "subject_community": str(subject.community_id),
        "applied": False,
    }
    if not state.enabled:
        trace["reason"] = state.disabled_reason or "消化禁用"
        return None, trace
    src = str(subject.community_id)
    dst = state.src_to_dst.get(src)
    if dst is None:
        trace["reason"] = "目标小区不在冻结转发映射的转发源内"
        return None, trace
    same = sum(
        1
        for cand in candidates
        if cand.selected and cand.community_id == src
    )
    trace["same_community_candidates"] = same
    trace["min_effective_samples"] = min_effective_samples
    if same >= min_effective_samples:
        trace["reason"] = f"同小区候选 {same} ≥ {min_effective_samples}，无需补偿"
        return None, trace
    trace.update(
        {
            "dst_cid": dst,
            "reason": "转发源同小区候选不足，视同合并后成员（design D4）",
        }
    )
    return dst, trace


def _comp_prices_of(
    valid_sale: pa.Table, rows: list[dict[str, Any]]
) -> list[Decimal]:
    """入选案例行的 valid_sale 原单价（缺失行不参与 r 估计，留痕由调用方计）。"""
    price_by_event = {
        str(eid): (Decimal(str(p)) if p is not None else None)
        for eid, p in zip(
            valid_sale.column("sale_event_id").to_pylist(),
            valid_sale.column("unit_price").to_pylist(),
            strict=True,
        )
    }
    prices: list[Decimal] = []
    for row in rows:
        price = price_by_event.get(str(row["sale_event_id"]))
        if price is not None:
            prices.append(price)
    return prices


def _write_stage_meta(
    valuation_dir: Path, payload: dict[str, Any]
) -> None:
    meta_path = valuation_dir / STAGE_META_FILENAME
    work = valuation_dir / (STAGE_META_FILENAME + ".incomplete")
    work.write_text(
        json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )
    work.replace(meta_path)


def apply_forward_digest(
    *,
    data_dir: Path,
    subject: Any,
    valid_sale: pa.Table,
    input_refs: Sequence[InputRef],
    rule_version: str,
    compensation_trace: Mapping[str, Any] | None = None,
) -> ForwardDigestResult:
    """机制一主入口：入选池的价格层次位势估计与 ×(1/r) 折算应用。

    - ``rule_version != 1.4`` 或账本门禁禁用 → 不启用（零改写）；
    - 清单内入选案例不足 / 清单外无基准 → 不折算（留痕）；
    - ``|1/r−1| < trigger_gap`` → 低于阈值不折算（示例小区132型自动豁免）；
    - ``|1/r−1| > max_gap`` → 超上限降级不折算并留痕；
    - 触发 → 清单内入选案例逐条追加 comp_adjustment 行（factor=1/r），
      幂等重建「转发折算」部分（保留时间/差异/子区行）。
    """
    state = load_forward_digest(data_dir, rule_version=rule_version)
    if not state.enabled:
        return ForwardDigestResult(
            applied=False,
            decision=DECISION_DISABLED,
            detail=state.disabled_reason or "转发消化禁用",
        )

    candidate_path = data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME
    if not candidate_path.is_file():
        return ForwardDigestResult(
            applied=False, decision="skipped", detail="comp_candidate 缺失（未运行候选/层级阶段）"
        )
    source = pq.read_table(candidate_path)
    rows = source.to_pylist()
    selected = [row for row in rows if row.get("selected")]
    marked_rows = [
        row for row in selected if str(row["sale_event_id"]) in state.event_map
    ]
    unmarked_rows = [
        row for row in selected if str(row["sale_event_id"]) not in state.event_map
    ]
    min_marked = int(state.parameters["min_marked_cases"])
    trigger_gap = Decimal(str(state.parameters["trigger_gap"]))
    max_gap = Decimal(str(state.parameters["max_gap"]))

    def _meta(decision: str, r_value: float | None = None) -> dict[str, Any]:
        return {
            "mechanism": "转发候选消化（价格层次条件化）",
            "decision": decision,
            "r": r_value,
            "n_selected_marked": len(marked_rows),
            "n_selected_unmarked": len(unmarked_rows),
            "min_marked_cases": min_marked,
            "trigger_gap": float(trigger_gap),
            "max_gap": float(max_gap),
            "asset_version": "v1",
            "asset_sha256": state.asset_sha256,
            "rule_version": rule_version,
            "subject_community": str(subject.community_id),
            "compensation": dict(compensation_trace) if compensation_trace else None,
            "built_at": datetime.now(UTC).isoformat(),
        }

    def _finish(
        result: ForwardDigestResult, decision: str, r_value: float | None
    ) -> ForwardDigestResult:
        _write_stage_meta(data_dir / VALUATION_LAYER, _meta(decision, r_value))
        return result

    if len(marked_rows) < min_marked or not unmarked_rows:
        decision = DECISION_BELOW_MIN if len(marked_rows) < min_marked else "清单外基准不足"
        return _finish(
            ForwardDigestResult(
                applied=False,
                decision=decision,
                detail=(
                    f"清单内入选 {len(marked_rows)} < {min_marked} 或清单外基准缺失，"
                    "不折算（与 rule 1.3 逐位一致）"
                ),
                n_marked=len(marked_rows),
                n_unmarked=len(unmarked_rows),
            ),
            decision,
            None,
        )

    marked_prices = _comp_prices_of(valid_sale, marked_rows)
    unmarked_prices = _comp_prices_of(valid_sale, unmarked_rows)
    if not marked_prices or not unmarked_prices:
        return _finish(
            ForwardDigestResult(
                applied=False,
                decision="价格缺失",
                detail="入选案例单价缺失（理论分支；不虚构 r）",
                n_marked=len(marked_rows),
                n_unmarked=len(unmarked_rows),
            ),
            "价格缺失",
            None,
        )
    try:
        r = statistics.median(marked_prices) / statistics.median(unmarked_prices)
        gap = abs(Decimal("1") - r)
    except (InvalidOperation, ZeroDivisionError) as exc:
        raise InvalidInputError(f"转发消化位势估计非法：{exc}") from exc

    if gap > max_gap:
        return _finish(
            ForwardDigestResult(
                applied=False,
                decision=DECISION_OVER_CAP,
                detail=f"|1/r−1|={float(gap):.4f} 超上限 {float(max_gap)}，降级不折算并留痕",
                r=float(r),
                n_marked=len(marked_rows),
                n_unmarked=len(unmarked_rows),
                warnings=(
                    f"转发折算降级：|1/r−1|={float(gap):.4f} 超上限 {float(max_gap)}",
                ),
            ),
            DECISION_OVER_CAP,
            float(r),
        )
    if gap < trigger_gap:
        return _finish(
            ForwardDigestResult(
                applied=False,
                decision=DECISION_BELOW_TRIGGER,
                detail=(
                    f"|1/r−1|={float(gap):.4f} 低于触发阈值 {float(trigger_gap)}，"
                    "保持现行为（与 rule 1.3 逐位一致）"
                ),
                r=float(r),
                n_marked=len(marked_rows),
                n_unmarked=len(unmarked_rows),
            ),
            DECISION_BELOW_TRIGGER,
            float(r),
        )

    factor = (Decimal("1") / r).quantize(Decimal("0.0001"))
    basis = {
        "判定": DECISION_APPLIED,
        "r": float(r),
        "字段口径": {
            "r": "清单内入选案例单价中位 / 清单外入选案例单价中位（估计时点池，raw 单价）",
            "折算因子": "实际应用乘数（=1/r，清单内案例折算至留守存量价位）",
        },
        "折算因子": float(factor),
        "样本量": {"清单内": len(marked_prices), "清单外": len(unmarked_prices)},
        "参数": {
            "min_marked_cases": min_marked,
            "trigger_gap": float(trigger_gap),
            "max_gap": float(max_gap),
            "status": state.parameters.get("status"),
        },
        "资产": {"version": "v1", "sha256": state.asset_sha256},
    }

    adjustment_path = data_dir / VALUATION_LAYER / COMP_ADJUSTMENT_FILENAME
    if adjustment_path.is_file():
        merged = pq.read_table(adjustment_path)
        merged = merged.filter(
            pa.array(
                [
                    str(t) != ADJUSTMENT_TYPE
                    for t in merged.column("adjustment_type").to_pylist()
                ]
            )
        )
    else:
        merged = pa.table(
            {name: [] for name in comp_adjustment_schema().names},
            schema=comp_adjustment_schema(),
        )
    schema = comp_adjustment_schema()
    extra_cols: dict[str, list[Any]] = {name: [] for name in schema.names}
    for row in marked_rows:
        cid = str(row["candidate_id"])
        payload = {
            "adjustment_id": f"{cid}-FD",
            "candidate_id": cid,
            "adjustment_type": ADJUSTMENT_TYPE,
            "amount": None,
            "sale_date": None,
            "valuation_date": subject.valuation_date,
            "basis": json.dumps(basis, ensure_ascii=False),
            "evidence_strength": "强",
            "source_series": "无（转发清单冻结资产价位折算）",
            "warning": None,
            "rule_version": rule_version,
            "direction": "下" if factor < 1 else "上" if factor > 1 else None,
            "factor": factor,
            "feature": "转发价位势",
            "formula": "调整后单价=单价×时间系数×(1/r)",
            "subject_side": str(subject.community_id),
            "comparable_side": "转发并入（清单内）",
        }
        for name in schema.names:
            extra_cols[name].append(payload[name])
    merged = pa.concat_tables([merged, pa.table(extra_cols, schema=schema)])

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    work = valuation_dir / (COMP_ADJUSTMENT_FILENAME + ".incomplete")
    pq.write_table(merged, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table="comp_adjustment",
            built_at=datetime.now(UTC),
            row_count=merged.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes="转发候选消化: 追加转发折算行(adjustment_type=转发折算)",
        ),
        adjustment_path,
    )
    work.replace(adjustment_path)

    warning = (
        f"转发折算生效：r={float(r):.4f}，factor={float(factor):.4f}，"
        f"清单内入选 {len(marked_rows)} 条（|1/r−1|={float(gap):.4f}）"
    )
    return _finish(
        ForwardDigestResult(
            applied=True,
            decision=DECISION_APPLIED,
            detail=warning,
            r=float(r),
            n_marked=len(marked_rows),
            n_unmarked=len(unmarked_rows),
            warnings=(warning,),
        ),
        DECISION_APPLIED,
        float(r),
    )
