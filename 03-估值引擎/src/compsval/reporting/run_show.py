"""WP7-C 运行清单查询：``compsval run show``（技术方案 §10.2）。

从 ``data/valuation/valuation_run.parquet`` 按 run_id 查询一次运行的总清单
（data_version/rule_version/code_version/parameters/run_at）并列出产物路径
（冻结估值 JSON、Markdown 报告，若存在）。输出 §10.3 统一包络。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
    OutputEnvelope,
)
from compsval.reporting.markdown import (
    ESTIMATE_FILENAME,
    FROZEN_DIR_PREFIX,
    REPORT_FILENAME,
)
from compsval.valuation.candidate import VALUATION_LAYER, VALUATION_RUN_FILENAME

#: 产物目录前缀（05-估值报告/valuation_id=<run_id>/）。


def _list_product_paths(run_id: str, reports_root: Path) -> dict[str, str]:
    """列出该 run 的冻结产物路径（不存在则返回空串）。"""
    frozen_dir = reports_root / f"{FROZEN_DIR_PREFIX}{run_id}"
    products: dict[str, str] = {}
    estimate_path = frozen_dir / ESTIMATE_FILENAME
    if estimate_path.is_file():
        products["estimate"] = str(estimate_path)
    report_path = frozen_dir / REPORT_FILENAME
    if report_path.is_file():
        products["report"] = str(report_path)
    return products


def run_versions(*, run_id: str, data_dir: Path) -> tuple[str | None, str | None]:
    """从运行清单表读取 run 的 (data_version, rule_version)；缺失 → (None, None)。

    供 report build / review apply 包络版本回填（RV-WP7-C-02 F3 /
    RV-WP7-D-01 F4，并入 WP8 版本治理）：版本缺失时如实填 None，不虚构。
    """
    run_path = data_dir / VALUATION_LAYER / VALUATION_RUN_FILENAME
    if not run_path.is_file():
        return None, None
    rows = pq.read_table(run_path).to_pylist()
    run = next((r for r in rows if str(r.get("run_id")) == run_id), None)
    if run is None:
        return None, None
    return run.get("data_version"), run.get("rule_version")


def show_run(
    *,
    run_id: str,
    data_dir: Path,
    reports_root: Path,
) -> OutputEnvelope:
    """查询运行清单并输出 §10.3 包络（result 为运行清单 + 产物路径）。"""
    run_path = data_dir / VALUATION_LAYER / VALUATION_RUN_FILENAME
    if not run_path.is_file():
        raise MissingDependencyError(f"运行清单表缺失：{run_path}")
    rows = pq.read_table(run_path).to_pylist()
    run: dict[str, Any] | None = next(
        (r for r in rows if str(r.get("run_id")) == run_id), None
    )
    if run is None:
        raise InvalidInputError(f"运行清单中不存在 run_id：{run_id}")

    products = _list_product_paths(run_id, reports_root)
    result: dict[str, Any] = {
        "run_id": run.get("run_id"),
        "subject_id": run.get("subject_id"),
        "valuation_date": str(run.get("valuation_date")),
        "data_cutoff": str(run.get("data_cutoff")),
        "data_version": run.get("data_version"),
        "rule_version": run.get("rule_version"),
        "code_version": run.get("code_version"),
        "parameters": run.get("parameters"),
        "run_at": str(run.get("run_at")),
        "products": products,
    }
    envelope = OutputEnvelope(
        command="run show",
        run_id=run_id,
        data_version=run.get("data_version"),
        rule_version=run.get("rule_version"),
        result=result,
    )
    for product in products.values():
        envelope.add_artifact(product)
    return envelope
