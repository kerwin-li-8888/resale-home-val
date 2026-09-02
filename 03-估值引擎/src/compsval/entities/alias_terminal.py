"""道路级命名待定行终态清零（ext-sale-ingest-scope-v1-2，P1）。

把 ``community_alias.parquet`` 中名录 §3 冲突清单 #10 对应的道路级命名待定行
（AC-63~70 工业大道/工业大道南、AC-72/73 泰沙路）按冻结 overrides 一次性置为
``EXCLUDED``（排除）终态：

- **原地改状态**：行本体保留（不物理删除、不新增行、``alias_id`` 不变）；
- **追加溯源**：``source_ref`` 保留原批次溯源并追加统一裁决标记
  （裁决日期、裁决口径、原状态）；标记幂等——重跑遇已裁决行跳过，
  不重复追加；
- **blocked 语义不变**：``排除`` 与待定/冲突同为非一致状态，自动映射仍仅取
  ``一致`` 别名（backfill 消费代码无需修改），但排除行不再属于待复核队列；
- **守卫**：行数不变、``alias_id`` 集合不变、应用后目标行无残留待定；
- **留痕**：产物 DerivedManifest 登记冻结 overrides 文件指纹与既有表指纹。

本模块不改写 ``community.parquet`` 与 ``scope_policy`` 各版本文件。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.contract.models import AliasConflictStatus
from compsval.entities.alias import (
    ALIAS_TABLE,
    write_alias_entity,
)
from compsval.ingest.manifests import InputRef

STATUS_PENDING = AliasConflictStatus.PENDING.value
STATUS_EXCLUDED = AliasConflictStatus.EXCLUDED.value


@dataclass(frozen=True)
class TerminalOverrides:
    """冻结的道路级命名终态裁决（含溯源标记模板）。"""

    change: str
    adjudicated_at: str
    adjudicated_by: str
    basis: str
    marker: str
    alias_ids: tuple[str, ...]
    sha256: str

    @property
    def dataset(self) -> str:
        return "alias_terminal_overrides"


def load_terminal_overrides(path: Path) -> TerminalOverrides:
    """读取并校验冻结 overrides 文件（SHA256 随文件内容确定）。"""
    raw = path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    for key in (
        "change",
        "adjudicated_at",
        "adjudicated_by",
        "basis",
        "marker_template",
        "overrides",
    ):
        if key not in payload:
            raise ValueError(f"终态 overrides 缺少字段 {key}：{path}")
    alias_ids = tuple(
        str(item["alias_id"]) for item in payload["overrides"]
    )
    if not alias_ids or len(set(alias_ids)) != len(alias_ids):
        raise ValueError(f"终态 overrides alias_id 为空或重复：{path}")
    marker = str(payload["marker_template"]).format(
        adjudicated_at=payload["adjudicated_at"],
        change=payload["change"],
    )
    return TerminalOverrides(
        change=str(payload["change"]),
        adjudicated_at=str(payload["adjudicated_at"]),
        adjudicated_by=str(payload["adjudicated_by"]),
        basis=str(payload["basis"]),
        marker=marker,
        alias_ids=alias_ids,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def _already_adjudicated(source_ref: object, marker: str) -> bool:
    return isinstance(source_ref, str) and marker in source_ref


def _sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def apply_alias_terminal_overrides(
    *,
    data_dir: Path,
    overrides_path: Path,
    notes: str | None = None,
) -> Path:
    """把冻结终态 overrides 应用于别名表（幂等），返回写入路径。

    状态机：待定 → 排除（追加裁决标记）；排除且已带标记 → 跳过（幂等）；
    其余状态 → 显式报错（防误改一致/冲突行）。
    """
    overrides = load_terminal_overrides(overrides_path)
    alias_path = data_dir / "entities" / "community_alias.parquet"
    if not alias_path.is_file():
        raise FileNotFoundError(f"别名表不存在：{alias_path}")
    table = pq.read_table(alias_path)

    columns = {name: table.column(name).to_pylist() for name in table.column_names}
    rows_before = table.num_rows
    targets = set(overrides.alias_ids)
    applied = 0
    skipped = 0
    for i, alias_id in enumerate(columns["alias_id"]):
        if alias_id not in targets:
            continue
        status = columns["conflict_status"][i]
        source_ref = columns["source_ref"][i]
        if status == STATUS_PENDING:
            columns["conflict_status"][i] = STATUS_EXCLUDED
            columns["source_ref"][i] = f"{source_ref}{overrides.marker}"
            applied += 1
        elif status == STATUS_EXCLUDED and _already_adjudicated(source_ref, overrides.marker):
            skipped += 1  # 幂等护栏：重复应用不追加溯源
        else:
            raise ValueError(
                f"别名行 {alias_id} 状态为 {status!r}，与终态裁决前置（待定）不符，拒绝应用"
            )

    missing = targets - set(columns["alias_id"])
    if missing:
        raise ValueError(f"终态裁决目标行缺失：{sorted(missing)}")
    if applied + skipped != len(targets):
        raise AssertionError("终态裁决应用计数不一致")  # pragma: no cover

    out = pa.table(columns, schema=table.schema)
    if out.num_rows != rows_before:
        raise AssertionError("终态裁决不得改变行数")  # pragma: no cover

    inputs = [
        InputRef(
            dataset=overrides.dataset,
            fetched_at=overrides.adjudicated_at,
            content_hash=overrides.sha256,
        ),
        InputRef(
            dataset=f"{ALIAS_TABLE}_before",
            fetched_at=overrides.adjudicated_at,
            content_hash=_sha256_of(alias_path),
        ),
    ]
    default_notes = (
        f"道路级命名终态清零（{overrides.change}）：待定→排除 {applied} 行"
        f"（幂等跳过 {skipped}），全表 {out.num_rows} 行不变；"
        f"裁决口径={overrides.basis}"
    )
    return write_alias_entity(
        out,
        data_dir=data_dir,
        inputs=inputs,
        notes=notes or default_notes,
    )


__all__ = [
    "TerminalOverrides",
    "apply_alias_terminal_overrides",
    "load_terminal_overrides",
]
