"""WP8-B 可复现回放报告（BT-001）：从回放产物只读生成 Markdown + JSON。

技术方案 §13.3 时间外回放 + §12.2 运行清单 + §14 G3 证据：报告固定包含运行
清单、整体指标、与简单基准对比、分组指标（§13.3 分组口径）、需关注子组
（APE 中位数高于整体的分组，如实提示非校准门槛）、数据深度限制与警告
（诚实声明，不假装校准）、产物清单与哈希。

设计要点（对应 WP8-B 验收标准）：

- **可复现（验收②）**：报告完全由回放产物（detail/metrics/grouped/
  run_manifest）只读生成，同一产物重复生成同一报告（内容一致）；
- **指标一致（验收②）**：报告中的每个关键指标直接来自机器产物表，不重新
  计算、不虚构；
- **诚实声明（验收②）**：回放覆盖（可回放点数/校验样本数）与数据深度限制
  显式写入报告与警告；
- **落盘（合同）**：报告写 ``04-校验/backtest_reports/<run_id>/``（README
  §11 建议目录），不触碰 05-估值报告冻结产物。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from compsval.valuation.backtest import (
    BACKTEST_LAYER,
    DETAIL_FILENAME,
    GROUPED_FILENAME,
    METRICS_FILENAME,
    RUN_MANIFEST_FILENAME,
)

#: 回放报告根：项目级 ``04-校验/backtest_reports``（README §11 建议目录）。
DEFAULT_BACKTEST_REPORTS_ROOT = (
    Path(__file__).resolve().parents[4] / "04-校验" / "backtest_reports"
)

#: 报告文件名。
REPORT_MARKDOWN_FILENAME = "replay_report.md"
REPORT_JSON_FILENAME = "replay_report.json"

#: 报告数据深度限制说明（覆盖/样本不足时如实声明，不假装校准）。
_LIMITED_DEPTH_NOTE = (
    "数据深度限制：回放估值点与校验样本仅来自当前数据湖内的历史成交。当前"
    "成交数据时间跨度有限（快照取得时间晚于成交日期、成交日窗口窄），回放"
    "覆盖可能远小于真实市场跨度；在覆盖不足时不得依据本报告校准数值门槛，"
    "须先补充多期数据再行回放（技术方案 §13.3/G3）。"
)


def _safe_dir_name(run_id: str) -> str:
    """把 run_id 消毒为合法目录名（Windows 禁 `:`/`@` 等；确定性替换）。

    多源合并数据版本会生成超长 run_id（G3R 实测 371 字符），超过 Windows
    目录名单项 255 字符上限 → 超长时截断到安全长度并附确定性 SHA-256 前缀
    （同 run_id 恒同目录名，可复现；截断信息不丢失，run_id 全文在报告内）。
    """
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", run_id)
    if len(safe) <= 120:
        return safe
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    return f"{safe[:96]}-{digest}"


def report_paths_for(run_id: str, out_root: Path) -> tuple[Path, Path]:
    """报告输出路径（markdown, json）：``<out_root>/<消毒 run_id>/``。"""
    out_dir = out_root / f"backtest_run_id={_safe_dir_name(run_id)}"
    return out_dir / REPORT_MARKDOWN_FILENAME, out_dir / REPORT_JSON_FILENAME


def _metrics_map(metrics: pa.Table) -> dict[str, float | None]:
    return {
        str(metric): value
        for metric, value in zip(
            metrics.column("metric").to_pylist(),
            metrics.column("value").to_pylist(),
            strict=True,
        )
    }


def _md_table(headers: list[str], rows: list[list[object]]) -> str:
    """Markdown 表格（简单转义管道符）。"""
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        cells = [str(cell) if cell is not None else "-" for cell in row]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _fmt(value: object, digits: int = 4) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.{digits}f}"
    return str(value)


def _overall_markdown(metrics: pa.Table) -> str:
    """整体指标（§13.3 指标清单 → Markdown 表，值直接来自机器指标表）。"""
    headers = ["指标", "值", "有效样本 n"]
    rows = [
        [str(m), _fmt(v), int(n)]
        for m, v, n in zip(
            metrics.column("metric").to_pylist(),
            metrics.column("value").to_pylist(),
            metrics.column("n").to_pylist(),
            strict=True,
        )
    ]
    return _md_table(headers, rows)


def _baseline_markdown(metrics_map: dict[str, float | None]) -> str:
    """与简单基准（同小区近期成交简单中位数）对比（§13.3/README §7.1）。

    只报告基准可用点上的基准误差与系统误差；基准不可用/样本不足如实说明。
    """
    n_with_baseline = int(metrics_map.get("n_with_baseline") or 0)
    if n_with_baseline == 0:
        return (
            "无同小区近期成交基准可用点（n_with_baseline=0），"
            "无法与简单基准对比（不虚构基准误差）。"
        )
    rows: list[list[object]] = [
        ["n_with_baseline", str(n_with_baseline)],
        ["baseline_ape_median", _fmt(metrics_map.get("baseline_ape_median"))],
        [
            "baseline_ape_high_quantile",
            _fmt(metrics_map.get("baseline_ape_high_quantile")),
        ],
        ["n_estimated（系统）", _fmt(metrics_map.get("n_estimated"))],
        ["ape_median（系统）", _fmt(metrics_map.get("ape_median"))],
    ]
    return _md_table(["对比项", "值"], rows)


def _grouped_markdown(grouped: pa.Table) -> str:
    """分组指标（§13.3：小区/户型/面积段/来源/可信度）→ 分节子表。

    子组校验样本为 0 时如实标注（不隐藏失败、不虚构结论）。
    """
    if grouped.num_rows == 0:
        return "无回放目标，分组指标为空（不虚构分组结论）。"
    sections: list[str] = []
    rows = grouped.to_pylist()
    dims = [
        ("community_id", "小区"),
        ("layout", "户型"),
        ("area_band", "面积段"),
        ("source_id", "来源"),
        ("confidence", "可信度"),
    ]
    by_dim: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_dim.setdefault(str(row["group_dimension"]), []).append(row)
    for dim_key, dim_label in dims:
        group_rows = by_dim.get(dim_key, [])
        if not group_rows:
            continue
        by_value: dict[str, dict[str, Any]] = {}
        for row in group_rows:
            by_value.setdefault(str(row["group_value"]), {})[
                str(row["metric"])
            ] = row["value"]
        header = ["分组值", "目标数", "估计数", "APE 中位数", "区间覆盖率", "基准中位数误差"]
        table_rows: list[list[object]] = []
        for value in sorted(by_value):
            m = by_value[value]
            n_est = int(m.get("n_estimated", 0) or 0)
            baseline_text = _fmt(m.get("baseline_ape_median"))
            if n_est == 0:
                baseline_text = f"{baseline_text}（无校验样本）"
            table_rows.append(
                [
                    value,
                    int(m.get("n_targets", 0) or 0),
                    n_est,
                    _fmt(m.get("ape_median")),
                    _fmt(m.get("range_coverage_rate")),
                    baseline_text,
                ]
            )
        sections.append(f"### 按{dim_label}分组\n\n" + _md_table(header, table_rows))
    return "\n\n".join(sections)


def _warnings_markdown(warnings: list[str]) -> str:
    if not warnings:
        return "无警告。"
    return "\n".join(f"- {w}" for w in warnings)


def _artifacts_markdown(artifacts: list[dict[str, str]]) -> str:
    headers = ["产物", "SHA256"]
    rows: list[list[object]] = [[a["path"], a.get("sha256", "-")] for a in artifacts]
    return _md_table(headers, rows)


def build_backtest_report(
    *,
    run_id: str,
    data_dir: Path,
    out_root: Path,
    backtest_dir: Path | None = None,
) -> BacktestReportResult:
    """从回放产物只读生成回放报告（Markdown + JSON）并原子写盘。

    回放产物缺失 → ``MissingDependencyError``（退出码 3）；run_id 与回放运行
    清单不一致 → ``InvalidInputError``（退出码 2，拒绝生成错误归属的报告）。
    ``backtest_dir``（可选）显式指定回放产物目录（如汇总器对照实验的
    ``data/backtest-exp/<候选>/``）；缺省 = ``<data_dir>/backtest``（既有行为）。
    """
    backtest_dir_resolved = (
        backtest_dir if backtest_dir is not None else data_dir / BACKTEST_LAYER
    )
    detail_path = backtest_dir_resolved / DETAIL_FILENAME
    metrics_path = backtest_dir_resolved / METRICS_FILENAME
    grouped_path = backtest_dir_resolved / GROUPED_FILENAME
    run_manifest_path = backtest_dir_resolved / RUN_MANIFEST_FILENAME
    for path, what in (
        (detail_path, "回放明细表"),
        (metrics_path, "回放指标表"),
        (grouped_path, "回放分组指标表"),
        (run_manifest_path, "回放运行清单"),
    ):
        if not path.is_file():
            raise MissingDependencyError(f"{what} 缺失：{path}")

    try:
        run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise MissingDependencyError(f"回放运行清单不可解析：{run_manifest_path}：{exc}") from exc
    if str(run_manifest.get("run_id")) != run_id:
        raise InvalidInputError(
            f"回放运行清单 run_id={run_manifest.get('run_id')} 与请求 {run_id} 不一致"
        )

    detail = pq.read_table(detail_path)
    metrics = pq.read_table(metrics_path)
    grouped = pq.read_table(grouped_path)

    metrics_map = _metrics_map(metrics)
    warnings = [str(w) for w in run_manifest.get("warnings", [])]
    artifacts = list(run_manifest.get("artifacts", []))
    over_groups = list(run_manifest.get("over_performance_groups", []))

    n_targets = int(metrics_map.get("n_targets") or 0)
    n_estimated = int(metrics_map.get("n_estimated") or 0)
    depth_lines = [
        f"- 回放估值点：{len({d for d in detail.column('replay_date').to_pylist() if d})} 个",
        f"- 校验目标：{n_targets} 个；其中可估值样本：{n_estimated} 个",
        f"- {_LIMITED_DEPTH_NOTE}",
    ]

    sections: list[str] = [
        f"# 历史回放报告（BT-001）— run {run_id}",
        "## 1. 运行清单（§12.2）",
        _md_table(
            ["项", "值"],
            [
                ["run_id", run_id],
                ["code_version", str(run_manifest.get("code_version"))],
                ["data_version", str(run_manifest.get("data_version"))],
                ["rule_version", str(run_manifest.get("rule_version"))],
                ["parameters", json.dumps(run_manifest.get("parameters"), ensure_ascii=False)],
                ["run_at", str(run_manifest.get("run_at"))],
            ],
        ),
        "## 2. 整体指标（§13.3）",
        _overall_markdown(metrics),
        "## 3. 与简单基准对比",
        _baseline_markdown(metrics_map),
        "## 4. 分组指标（§13.3）",
        _grouped_markdown(grouped),
        "## 5. 需关注子组（APE 中位数高于整体，非校准门槛）",
        (
            "\n".join(
                f"- {g['group_dimension']} = {g['group_value']}："
                f"组内 APE 中位数 {_fmt(g['group_ape_median'])} "
                f"> 整体 {_fmt(g['overall_ape_median'])}"
                for g in over_groups
            )
            if over_groups
            else "无（或整体无 APE 中位数可对比）。"
        ),
        "## 6. 数据深度限制与警告（诚实声明）",
        "\n".join(depth_lines),
        "## 7. 警告",
        _warnings_markdown(warnings),
        "## 8. 产物清单与哈希",
        _artifacts_markdown(artifacts),
    ]
    markdown = "\n\n".join(sections) + "\n"

    report_json: dict[str, Any] = {
        "schema_version": "1.0",
        "run_id": run_id,
        "data_version": run_manifest.get("data_version"),
        "rule_version": run_manifest.get("rule_version"),
        "code_version": run_manifest.get("code_version"),
        "parameters": run_manifest.get("parameters"),
        "run_at": run_manifest.get("run_at"),
        "warnings": warnings,
        "over_performance_groups": over_groups,
        "metrics": metrics_map,
        "data_depth_limits": {
            "n_replay_dates": len(
                {d for d in detail.column("replay_date").to_pylist() if d}
            ),
            "n_targets": n_targets,
            "n_estimated": n_estimated,
            "note": _LIMITED_DEPTH_NOTE,
        },
        "grouped": grouped.to_pylist(),
        "artifacts": artifacts,
    }

    markdown_path, json_path = report_paths_for(run_id, out_root)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    md_work = markdown_path.with_name(REPORT_MARKDOWN_FILENAME + ".incomplete")
    md_work.write_text(markdown, encoding="utf-8")
    md_work.replace(markdown_path)
    json_work = json_path.with_name(REPORT_JSON_FILENAME + ".incomplete")
    json_work.write_text(
        json.dumps(report_json, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    json_work.replace(json_path)

    return BacktestReportResult(
        run_id=run_id,
        data_version=run_manifest.get("data_version"),
        rule_version=run_manifest.get("rule_version"),
        markdown=markdown,
        markdown_path=markdown_path,
        json_path=json_path,
    )


@dataclass(frozen=True)
class BacktestReportResult:
    """一次 ``compsval backtest report`` 的结果（供 CLI 打印与测试断言）。"""

    run_id: str
    data_version: str | None
    rule_version: str | None
    markdown: str
    markdown_path: Path
    json_path: Path
