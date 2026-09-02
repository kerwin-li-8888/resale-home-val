"""WP7-C Markdown 估值报告生成（技术方案 §11.2 十二节）。

从冻结估值 JSON（``estimate.json``，WP7-B 产出）与估值中间结果表
（valuation_run/comp_candidate/comp_adjustment/valuation_result/review_event）
生成可读 Markdown 报告。报告固定包含 §11.2 十二节；每个关键价格回到机器
结果（冻结 JSON / valid_sale / 中间结果表），报告只读生成、不修改冻结 JSON。

设计要点（对应 WP7-C 验收标准）：

- **十二节齐全（验收①）**：估值对象和时点/输出状态/中心值、区间和可信度/
  关键限制与补数/入选可比案例/被排除案例及原因/时间和差异处理/可信度分项/
  自动结果和人工复核/数据、代码、规则和运行版本/来源证据定位/后续结果区；
- **价格可追溯（验收②）**：中心值/区间/可信度来自冻结 JSON 的 result（与
  valuation_result 表同一冻结），可比单价从 valid_sale 溯源，调整明细来自
  comp_adjustment；
- **只读不覆盖（验收④）**：报告可重复生成，绝不改写冻结 JSON 与中间表；
- **缺失安全**：无复核事件/后续结果时对应节明确标注“无”，不虚构。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from compsval.valuation.aggregation import VALUATION_RESULT_FILENAME
from compsval.valuation.candidate import (
    COMP_CANDIDATE_FILENAME,
    VALUATION_LAYER,
    VALUATION_RUN_FILENAME,
)
from compsval.valuation.review import REVIEW_EVENT_FILENAME
from compsval.valuation.time_adjustment import COMP_ADJUSTMENT_FILENAME

#: 冻结估值 JSON 文件名（WP7-B estimate.py 定义保持一致）。
ESTIMATE_FILENAME = "estimate.json"
#: 报告文件名。
REPORT_FILENAME = "report.md"

#: 冻结产物目录前缀（05-估值报告/valuation_id=<run_id>/）。
FROZEN_DIR_PREFIX = "valuation_id="


@dataclass(frozen=True)
class ReportBuildResult:
    """一次 ``compsval report build`` 的结果（Markdown 文本 + 业务状态）。"""

    markdown: str
    business_status: str | None


def _read_table_or_none(path: Path) -> list[dict[str, Any]] | None:
    """读可选中间表（缺失返回 None，不报错）。"""
    if not path.is_file():
        return None
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())


def _fmt(value: object) -> str:
    """标量 → 展示文本（None → 空串，避免输出 'None'）。"""
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _fmt_price(value: Any) -> str:
    """价格/单价展示：数值带两位小数；None 显示为“未知”。"""
    if value is None:
        return "未知"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:,.2f}"


def _section(title: str, body: str) -> str:
    return f"## {title}\n\n{body.strip()}\n\n"


def build_report_markdown(
    *,
    run_id: str,
    data_dir: Path,
    reports_root: Path,
) -> ReportBuildResult:
    """从冻结估值 JSON + 中间结果表生成 §11.2 十二节 Markdown 报告。"""
    frozen_dir = reports_root / f"{FROZEN_DIR_PREFIX}{run_id}"
    frozen_path = frozen_dir / ESTIMATE_FILENAME
    if not frozen_path.is_file():
        raise MissingDependencyError(f"冻结估值 JSON 缺失：{frozen_path}（先运行 compsval estimate）")
    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))
    result = frozen.get("result") or {}
    warnings = frozen.get("warnings") or []

    run_rows = _read_table_or_none(data_dir / VALUATION_LAYER / VALUATION_RUN_FILENAME)
    run = next((r for r in (run_rows or []) if str(r.get("run_id")) == run_id), None)
    if run is None:
        raise InvalidInputError(
            f"运行清单中不存在 run_id：{run_id}（data/valuation/valuation_run.parquet）"
        )

    candidates = _read_table_or_none(data_dir / VALUATION_LAYER / COMP_CANDIDATE_FILENAME)
    run_candidates = [c for c in (candidates or []) if str(c.get("run_id")) == run_id]
    selected = [c for c in run_candidates if c.get("selected")]
    excluded = [c for c in run_candidates if not c.get("selected")]

    adjustments = _read_table_or_none(data_dir / VALUATION_LAYER / COMP_ADJUSTMENT_FILENAME)
    time_adj = [a for a in (adjustments or []) if a.get("adjustment_type") == "时间"]
    diff_adj = [a for a in (adjustments or []) if a.get("adjustment_type") == "差异"]

    result_rows = _read_table_or_none(data_dir / VALUATION_LAYER / VALUATION_RESULT_FILENAME)
    result_row = next(
        (r for r in (result_rows or []) if str(r.get("run_id")) == run_id), None
    )

    review_rows = _read_table_or_none(data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME)
    result_id = result.get("result_id") or (result_row or {}).get("result_id")
    reviews = (
        [r for r in (review_rows or []) if str(r.get("result_id")) == str(result_id)]
        if result_id is not None
        else []
    )

    # 可比单价从 valid_sale 溯源（§11.2 第 5 节价格可追溯）
    unit_prices: dict[str, float | None] = {}
    valid_sale_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    if valid_sale_path.is_file():
        for row in pq.read_table(valid_sale_path).to_pylist():
            unit_prices[str(row.get("sale_event_id"))] = row.get("unit_price")

    subject_id = run.get("subject_id", "")
    valuation_date = result.get("valuation_date") or _fmt(run.get("valuation_date"))

    parts: list[str] = []
    parts.append(f"# 估值报告 {run_id}\n")
    parts.append(
        f"> 生成时间：{_fmt(run.get('run_at'))}；本报告由冻结估值 JSON 只读生成，"
        "不修改自动结果。\n"
    )

    # 1. 估值对象和估值时点
    parts.append(_section(
        "1. 估值对象和估值时点",
        f"- 目标房源（subject_id）：{_fmt(subject_id)}\n"
        f"- 估值时点：{_fmt(valuation_date)}\n"
        f"- 数据截点：{_fmt(run.get('data_cutoff'))}",
    ))

    # 2. 输出状态
    parts.append(_section(
        "2. 输出状态",
        f"- 业务状态：{_fmt(frozen.get('business_status'))}\n"
        f"- 输出状态：{_fmt(result.get('status'))}\n"
        f"- 理由：{_fmt(result.get('reason'))}",
    ))

    # 3. 中心值、估值范围和可信度
    center = result.get("center")
    rng = result.get("range") or []
    parts.append(_section(
        "3. 中心值、估值范围和可信度",
        f"- 中心值（元/㎡）：{_fmt_price(center)}\n"
        f"- 估值区间（元/㎡）：[{_fmt_price(rng[0] if len(rng) > 0 else None)}, "
        f"{_fmt_price(rng[1] if len(rng) > 1 else None)}]\n"
        f"- 可信度：{_fmt(result.get('confidence'))}",
    ))

    # 4. 关键限制与补数建议
    limit_lines = [f"- {_fmt(w)}" for w in warnings] or [
        f"- {_fmt(result.get('reason'))}"
    ]
    parts.append(_section(
        "4. 关键限制与补数建议",
        "\n".join(limit_lines) if limit_lines else "- 无",
    ))

    # 5. 入选可比案例
    if selected:
        rows = []
        for c in selected:
            price = unit_prices.get(str(c.get("sale_event_id")))
            rows.append(
                f"- {_fmt(c.get('candidate_id'))}：成交日 {_fmt(c.get('sale_date'))}，"
                f"小区 {_fmt(c.get('community_id'))}，层级 {_fmt(c.get('tier'))}，"
                f"相似度 {_fmt(c.get('similarity'))}，单价（元/㎡）{_fmt_price(price)}"
            )
        body = "\n".join(rows)
    else:
        body = "- 无入选可比案例"
    parts.append(_section("5. 入选可比案例", body))

    # 6. 被排除案例及原因
    if excluded:
        rows = [f"- {_fmt(c.get('candidate_id'))}：{_fmt(c.get('reason'))}" for c in excluded]
        body = "\n".join(rows)
    else:
        body = "- 无排除案例"
    parts.append(_section("6. 被排除案例及原因", body))

    # 7. 时间和差异处理
    adj_lines: list[str] = []
    for a in time_adj:
        adj_lines.append(
            f"- 时间（{_fmt(a.get('candidate_id'))}）：依据 {_fmt(a.get('basis'))}，"
            f"数值 {_fmt(a.get('amount'))}，证据强度 {_fmt(a.get('evidence_strength'))}"
            f"{f"，警告：{_fmt(a.get('warning'))}" if a.get("warning") else ''}"
        )
    for a in diff_adj:
        adj_lines.append(
            f"- 差异（{_fmt(a.get('candidate_id'))}，{_fmt(a.get('feature'))}）："
            f"{_fmt(a.get('direction'))}，系数 {_fmt(a.get('factor'))}，"
            f"依据 {_fmt(a.get('basis'))}"
        )
    parts.append(_section(
        "7. 时间和差异处理",
        "\n".join(adj_lines) if adj_lines else "- 无调整记录",
    ))

    # 8. 可信度分项（valuation_result 表字段名为 evidence，aggregation.py schema）
    evidence = {}
    if result_row is not None and result_row.get("evidence"):
        try:
            evidence = json.loads(str(result_row["evidence"]))
        except (json.JSONDecodeError, TypeError):
            evidence = {}
    if evidence:
        ev_lines = [f"- {k}：{_fmt(v)}" for k, v in sorted(evidence.items())]
        body = "\n".join(ev_lines)
    else:
        body = f"- 可信度（整体）：{_fmt(result.get('confidence'))}（分项证据未落表）"
    parts.append(_section("8. 可信度分项", body))

    # 9. 自动结果和人工复核
    auto_lines = (
        f"- 自动结果：中心 {_fmt_price(center)} 元/㎡，区间 "
        f"[{_fmt_price(rng[0] if len(rng) > 0 else None)}, "
        f"{_fmt_price(rng[1] if len(rng) > 1 else None)}] 元/㎡，"
        f"可信度 {_fmt(result.get('confidence'))}，状态 {_fmt(result.get('status'))}"
    )
    review_lines = []
    for r in reviews:
        review_lines.append(
            f"- {_fmt(r.get('review_id'))}：动作 {_fmt(r.get('action'))}，"
            f"判定 {_fmt(r.get('judgment'))}，理由 {_fmt(r.get('reason'))}，"
            f"时间 {_fmt(r.get('reviewed_at'))}"
        )
    body = auto_lines + ("\n" + "\n".join(review_lines) if review_lines else "\n- 无人工复核事件")
    parts.append(_section("9. 自动结果和人工复核", body))

    # 10. 数据、代码、规则和运行版本
    parts.append(_section(
        "10. 数据、代码、规则和运行版本",
        f"- 数据版本：{_fmt(run.get('data_version'))}\n"
        f"- 代码版本：{_fmt(run.get('code_version'))}\n"
        f"- 规则版本：{_fmt(run.get('rule_version'))}\n"
        f"- 运行参数：{json.dumps(run.get('parameters'), ensure_ascii=False)}",
    ))

    # 11. 来源证据定位
    src_lines = [
        f"- {_fmt(c.get('candidate_id'))}：原始定位 {_fmt(c.get('raw_locator'))}"
        for c in selected
    ]
    parts.append(_section(
        "11. 来源证据定位",
        "\n".join(src_lines) if src_lines else "- 无入选案例",
    ))

    # 12. 后续结果区（初始为空）
    parts.append(_section("12. 后续结果区", "- （初始为空；后续成交结果落 outcome_event 后回填）"))

    return ReportBuildResult(
        markdown="".join(parts),
        business_status=frozen.get("business_status"),
    )


def report_path_for(run_id: str, reports_root: Path) -> Path:
    """报告落盘路径（与冻结 JSON 同目录）。"""
    return reports_root / f"{FROZEN_DIR_PREFIX}{run_id}" / REPORT_FILENAME
