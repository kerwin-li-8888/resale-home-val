"""WP7-D ``compsval review apply`` 复核命令（技术方案 §10.2/§11.1）。

对齐技术方案合同：``compsval review apply --valuation <run_id> --input <json>``。
校验估值结果存在（不存在 → ``MissingDependencyError`` 退出码 3）→ 复用
WP6-F ``review.py`` 只追加 ``review_event`` → 输出新版本引用（review_id +
声明不覆盖自动结果）。本模块只做薄封装，不改写 review.py 逻辑。

设计要点（对应 WP7-D 验收标准）：

- **校验估值存在（验收①）**：按 run_id 在 ``valuation_result`` 表解析
  result_id；run 或表缺失 → 退出码 3，不写任何复核事件；
- **只追加不覆盖（验收②）**：委托 WP6-F ``append_review_events``（只追加，
  自动结果不可被复核改写）；用户输入不得携带与 run 不一致的 result_id
  （不一致 → 退出码 2）；
- **留痕可查（验收③）**：复核事件落 ``review_event.parquet`` 后，report
  build（WP7-C 第 9 节）可见复核记录；自动结果（valuation_result）原样。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from compsval.contract.models import ReviewEvent
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from compsval.valuation import review as review_core
from compsval.valuation.aggregation import VALUATION_RESULT_FILENAME
from compsval.valuation.candidate import VALUATION_LAYER


def result_id_of_run(*, run_id: str, data_dir: Path) -> str:
    """从 valuation_result 表解析 run_id → result_id（不存在 → 退出码 3）。"""
    path = data_dir / VALUATION_LAYER / VALUATION_RESULT_FILENAME
    if not path.is_file():
        raise MissingDependencyError(
            f"估值结果表缺失：{path}（先运行 compsval estimate）"
        )
    rows = pq.read_table(path).to_pylist()
    match = next((r for r in rows if str(r.get("run_id")) == run_id), None)
    if match is None:
        raise MissingDependencyError(
            f"估值结果中不存在 run_id：{run_id}（先运行 compsval estimate）"
        )
    result_id = match.get("result_id")
    if not result_id:
        raise MissingDependencyError(f"run {run_id} 的估值结果缺少 result_id")
    return str(result_id)


def apply_review_for_run(
    *,
    run_id: str,
    data_dir: Path,
    input_payload: dict[str, Any],
) -> tuple[Path, list[ReviewEvent]]:
    """校验估值存在 + 注入 result_id + 只追加 review_event（WP6-F 复用）。"""
    result_id = result_id_of_run(run_id=run_id, data_dir=data_dir)

    # 用户输入不得自带与 run 不一致的 result_id（以 --valuation 为准）
    provided = input_payload.get("result_id")
    if provided is not None and str(provided) != result_id:
        raise InvalidInputError(
            f"输入 result_id {provided} 与 run {run_id} 的 {result_id} 不一致"
        )

    payload = dict(input_payload)
    payload["result_id"] = result_id
    event_input = review_core.ReviewEventInput(**payload)
    return review_core.append_review_events(
        data_dir=data_dir,
        events=[event_input.to_review_event()],
        notes=f"WP7-D: compsval review apply（run {run_id}）",
    )
