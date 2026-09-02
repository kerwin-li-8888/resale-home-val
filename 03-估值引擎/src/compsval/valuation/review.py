"""WP6-F 人工复核留痕（VAL1-007）：review_event 只追加。

README §6.9 / 技术方案 §11.1 / 数据字典 §3.14：人工复核通过 JSON 合同提交，
完成后以 ``review_event`` 追加留痕；自动结果（``valuation_result``）不被静默
覆盖，复核产生新版本作为后续事件表达，原自动版本永久保留。

设计要点（对应 WP6-F 验收标准）：

- **只追加不覆盖（验收①）**：每次复核只往 ``review_event.parquet`` 追加一行，
  既有事件永不被修改/删除；写入前对该 ``result_id`` 做存在性校验（FK），不
  写的目标仍是其指向的自动结果——本模块绝不改写 ``valuation_result``；
- **记录完整（验收②）**：每条事件含修改前后值（``before``/``after``）、
  理由（``reason``）、证据/现场观察（``evidence``）、复核时间（``reviewed_at``）
  与规则版本（``rule_version``），均必填可溯源；
- **区分纠正/主观（验收③）**：``judgment`` 显式区分“纠正错误数据”与
  “主观判断调整”，两者证据标准不同，不得混淆；
- **不利用未来信息（§6.9 第7条）**：复核只审查估值时点当时可得信息，不因
  截点之后信息倒改历史估值（本模块仅留痕，不做任何数值改写）。
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from compsval import __version__
from compsval.contract.models import (
    ReviewAction,
    ReviewEvent,
    ReviewJudgment,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.valuation.aggregation import VALUATION_RESULT_FILENAME
from compsval.valuation.candidate import (
    DEFAULT_RULE_VERSION,
    VALUATION_LAYER,
)

REVIEW_EVENT_TABLE = "review_event"
REVIEW_EVENT_FILENAME = f"{REVIEW_EVENT_TABLE}.parquet"

#: 唯一复核人（README §6.9：用户是第一阶段的唯一人工复核人）。
DEFAULT_REVIEWER = "user"

#: review_id 前缀（``REV-<result_id>-<seq>``，seq 为该 result 事件序号）。
REVIEW_ID_PREFIX = "REV-"


def review_event_schema() -> pa.Schema:
    """``review_event`` 只追加表模式（模型字段 + 溯源 + 版本）。"""
    return pa.schema(
        [
            pa.field("review_id", pa.string(), nullable=False),
            pa.field("result_id", pa.string(), nullable=False),
            pa.field("action", pa.string(), nullable=False),
            pa.field("judgment", pa.string(), nullable=False),
            pa.field("subject", pa.string(), nullable=False),
            pa.field("before", pa.string(), nullable=False),
            pa.field("after", pa.string(), nullable=False),
            pa.field("reason", pa.string(), nullable=False),
            pa.field("evidence", pa.string(), nullable=False),
            pa.field("reviewed_at", pa.timestamp("us"), nullable=False),
            pa.field("reviewer", pa.string(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
        ]
    )


def _json_dump(payload: dict[str, Any]) -> str:
    """before/after 以 JSON 字符串落盘（保证可解析、可回读）。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def review_event_table(events: Sequence[ReviewEvent]) -> pa.Table:
    """复核事件序列 → 只追加表。"""
    rows: dict[str, list[Any]] = {name: [] for name in review_event_schema().names}
    for event in events:
        rows["review_id"].append(event.review_id)
        rows["result_id"].append(event.result_id)
        rows["action"].append(event.action.value)
        rows["judgment"].append(event.judgment.value)
        rows["subject"].append(event.subject)
        rows["before"].append(_json_dump(event.before))
        rows["after"].append(_json_dump(event.after))
        rows["reason"].append(event.reason)
        rows["evidence"].append(event.evidence)
        rows["reviewed_at"].append(event.reviewed_at.astimezone(UTC).replace(tzinfo=None))
        rows["reviewer"].append(event.reviewer)
        rows["rule_version"].append(event.rule_version)
    return pa.table(rows, schema=review_event_schema())


def _event_seq(existing: pa.Table, result_id: str) -> int:
    """该 result 的既有事件序号基数：REV-<result_id>-<seq> 的最大 seq+1。

    在新表（无既有事件）时返回 1；每次只追加，永不复用/覆盖既有 review_id。
    """
    seq = 0
    prefix = f"{REVIEW_ID_PREFIX}{result_id}-"
    for rid in existing.column("review_id").to_pylist():
        if isinstance(rid, str) and rid.startswith(prefix):
            tail = rid[len(prefix) :]
            if tail.isdigit():
                seq = max(seq, int(tail))
    return seq + 1


def _read_existing(data_dir: Path) -> tuple[pa.Table, Path | None]:
    """读取既有 review_event 表；无则返回空表（schema 一致）。"""
    path = data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME
    if not path.is_file():
        empty: dict[str, list[Any]] = {
            name: [] for name in review_event_schema().names
        }
        return pa.table(empty, schema=review_event_schema()), None
    return pq.read_table(path), path


def _result_exists(data_dir: Path, result_id: str) -> bool:
    """FK 校验：复核事件必须指向存在的 valuation_result（拒绝悬空留痕）。"""
    path = data_dir / VALUATION_LAYER / VALUATION_RESULT_FILENAME
    if not path.is_file():
        return False
    result_ids = pq.read_table(path).column("result_id").to_pylist()
    return result_id in result_ids


class ReviewError(ValueError):
    """复核输入/校验错误（拒绝写入；只追加表不允许损坏）。"""


class ReviewEventInput(BaseModel):
    """CLI JSON 复核合同（技术方案 §11.1）。

    未显式给出 ``reviewed_at``/``reviewer``/``rule_version``/``review_id`` 时
    使用合理默认值；``review_id`` 由 append 自动生成，忽略调用方传入值。
    """

    result_id: str = Field(..., description="FK→valuation_result")
    action: ReviewAction = Field(..., description="复核动作")
    judgment: ReviewJudgment = Field(..., description="纠正错误数据/主观判断调整")
    subject: str = Field(..., description="复核对象描述")
    before: dict[str, Any] = Field(default_factory=dict, description="修改前值")
    after: dict[str, Any] = Field(default_factory=dict, description="修改后值")
    reason: str = Field(..., description="理由")
    evidence: str = Field(..., description="证据或现场观察")
    reviewed_at: datetime | None = Field(default=None, description="复核时间（默认当前）")
    reviewer: str = Field(default=DEFAULT_REVIEWER, description="复核人")
    rule_version: str = Field(default=DEFAULT_RULE_VERSION, description="规则版本")

    def to_review_event(self) -> ReviewEvent:
        return ReviewEvent(
            review_id="",  # append 自动分配
            result_id=self.result_id,
            action=self.action,
            judgment=self.judgment,
            subject=self.subject,
            before=self.before,
            after=self.after,
            reason=self.reason,
            evidence=self.evidence,
            reviewed_at=self.reviewed_at or datetime.now(UTC),
            reviewer=self.reviewer,
            rule_version=self.rule_version,
        )


def append_review_events(
    *,
    data_dir: Path,
    events: Sequence[ReviewEvent],
    input_refs: Sequence[InputRef] = (),
    notes: str | None = None,
) -> tuple[Path, list[ReviewEvent]]:
    """追加复核事件到 ``review_event``（只追加，验收①-③）。

    - 校验每条事件必填项（reason/evidence）与 FK（result 存在）；
    - 自动生成 ``review_id``（REV-<result_id>-<seq>，递增不复用）；
    - 读既有表 + ``pq.concat`` 追加 → 原子写回 + DerivedManifest；
    - 绝不改写 ``valuation_result``（只读校验）；
    - 返回 ``(写入路径, 已分配 review_id 的事件)``，供调用方溯源复核编号。
    """
    if not events:
        raise ReviewError("无复核事件可追加")

    result_ids = {e.result_id for e in events}
    for result_id in result_ids:
        if not _result_exists(data_dir, result_id):
            raise ReviewError(f"复核事件指向不存在或未构建的估值结果: {result_id}")

    for event in events:
        if not event.reason.strip():
            raise ReviewError(f"{event.review_id or '(待分配)'} 缺少理由(reason)")
        if not event.evidence.strip():
            raise ReviewError(f"{event.review_id or '(待分配)'} 缺少证据(evidence)")

    existing, path = _read_existing(data_dir)
    # 为每条事件分配 over-the-wire review_id（按 result_id 分别递增）。
    seq_by_result: dict[str, int] = {}
    typed: list[ReviewEvent] = []
    for event in events:
        seq = seq_by_result.get(event.result_id, _event_seq(existing, event.result_id))
        seq_by_result[event.result_id] = seq + 1
        typed.append(
            ReviewEvent(
                review_id=f"{REVIEW_ID_PREFIX}{event.result_id}-{seq}",
                **event.model_dump(exclude={"review_id"}),
            )
        )

    appended = review_event_table(typed)
    combined = pa.concat_tables([existing, appended]) if existing.num_rows else appended

    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)
    write_path = valuation_dir / REVIEW_EVENT_FILENAME
    work = valuation_dir / (REVIEW_EVENT_FILENAME + ".incomplete")
    pq.write_table(combined, work, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=REVIEW_EVENT_TABLE,
            built_at=datetime.now(UTC),
            row_count=combined.num_rows,
            inputs=list(input_refs),
            package_version=__version__,
            notes=notes or f"WP6-F: 只追加复核留痕，追加 {len(typed)} 条",
        ),
        write_path,
    )
    work.replace(write_path)
    return write_path, typed
