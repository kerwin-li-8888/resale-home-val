"""fangcollect 批次别名登记（close-fangcollect-exchange-loop，2026-09-10 用户裁决）。

fangcollect 平台清单的链家叫法小区名按冻结裁决表（默认路径
``examples/phase2_demo/adjudication/fangcollect-裁决表-20260910.csv``，可用
``build_alias_fangcollect_batch(adjudication_csv=...)`` 显式覆盖；SHA256 旁车
留痕，裁决记录与裁决表同目录）登记进 community_alias 构建链：

- 处置 = ``待补实体_名录外不入表``：不产生别名行（本批 10 名全部如此——经核验均为
  真实独立实体但不在现行 239 实体内；新实体入口规则属 backlog 待决策 2，不扩权
  建实体），名字进「待补实体清单」留痕，解析层维持失败（信息不足）语义；
- 处置 = 一致/待定/排除：按状态落表；当前冻结裁决表格式未携带目标 ``community_id``
  列，出现此类处置时显式报错，防止静默丢弃用户裁决；
- 批次登记留痕：冻结裁决表以 ``InputRef(dataset=fangcollect_adjudication_table)``
  登记进 DerivedManifest inputs，notes 记录批次名字数与落表行数（本批 = 0）；
- 幂等：既有非 ``FC-`` 行原序原样保留，同输入重跑产出逐字节一致的表文件；
- 匹配消费语义不变：自动小区映射仍仅取 ``conflict_status=一致`` 别名。

由 ``tests/test_alias_fangcollect.py`` 保障对拍。

lianjia 对照批次（expand-lianjia-alias-rebaseline，2026-09-11 用户裁决）：

- 冻结裁决表 = ``examples/phase2_demo/adjudication/lianjia-对照裁决表-20260911.csv``
  （列：源名/处置/目标 community_id/目标标准名/裁决依据/备注；SHA256 旁车；
  裁决记录与裁决表同目录）；
- 处置 = ``一致``：落一致别名行（目标 community_id 必填，源名 = 链家叫法，
  source_id = SRC-011 链家 ext 平台）；
- 处置 = ``排除``：落排除终态行（道路级命名等不参与自动映射；链家源名无
  实体锚，community_id 记 UNKNOWN，溯源靠 source_ref）；
- 处置 = ``待定``/``维持既有``：不落新行（挂起待二轮 / blocked 既有行不动）；
- 幂等：既有非 ``LJ-`` 行（含 fangcollect 批次行）原序原样保留，同输入重跑
  逐字节一致。

由 ``tests/test_alias_lianjia.py`` 保障对拍。
"""

from __future__ import annotations

import csv
import hashlib
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path

import pyarrow.parquet as pq

from gz_property_valuation.contract.models import AliasConflictStatus, CommunityAlias
from gz_property_valuation.entities.alias import (
    ALIAS_FILENAME,
    alias_table,
    write_alias_entity,
)
from gz_property_valuation.entities.community import ENTITIES_LAYER
from gz_property_valuation.ingest.manifests import InputRef

FANGCOLLECT_SOURCE_ID = "SRC-007"
FANGCOLLECT_BATCH_PREFIX = "FC-"
ADJUDICATED_AT = "2026-09-10"
DISPOSITION_OFF_TABLE = "待补实体_名录外不入表"
ADJUDICATION_FILENAME = "fangcollect-裁决表-20260910.csv"

LIANJIA_SOURCE_ID = "SRC-011"
LIANJIA_BATCH_PREFIX = "LJ-"
LIANJIA_ADJUDICATED_AT = "2026-09-11"
LIANJIA_ADJUDICATION_FILENAME = "lianjia-对照裁决表-20260911.csv"
LIANJIA_ADJUDICATION_RECORD = "execution/裁决记录-20260911.md"
LIANJIA_NAME_COLUMN = "源名"
LIANJIA_TARGET_COLUMN = "目标community_id"
#: 落新行的处置（一致 = 参与自动映射；排除 = 终态不映射，仅登记留痕）
LIANJIA_ROW_DISPOSITIONS = ("一致", "排除")
#: 不落新行的处置（待定 = 挂起待二轮；维持既有 = blocked 既有行不动）
LIANJIA_NO_ROW_DISPOSITIONS = ("待定", "维持既有")
LIANJIA_DISPOSITIONS = LIANJIA_ROW_DISPOSITIONS + LIANJIA_NO_ROW_DISPOSITIONS
#: 排除行无实体锚（链家道路级源名）时的 community_id 占位（缺失语义 = UNKNOWN）
LIANJIA_UNANCHORED_COMMUNITY_ID = "UNKNOWN"

_REPO_ROOT = Path(__file__).resolve().parents[4]
#: 冻结裁决表默认路径（3.2 参数化口径）：examples 约定目录，显式路径经
#: ``build_alias_*_batch(adjudication_csv=...)`` 覆盖；changes 归档目录回落保留。
_EXAMPLES_ADJUDICATION_DIR = _REPO_ROOT / "examples" / "phase2_demo" / "adjudication"
_ADJUDICATION_CANDIDATES = (
    _EXAMPLES_ADJUDICATION_DIR / ADJUDICATION_FILENAME,
)
_LIANJIA_ADJUDICATION_CANDIDATES = (
    _EXAMPLES_ADJUDICATION_DIR / LIANJIA_ADJUDICATION_FILENAME,
)


def _archive_candidates() -> tuple[Path, ...]:
    """归档回落（archive/<date>-close-fangcollect-exchange-loop；移动非改写）。"""
    archive = _REPO_ROOT / "openspec" / "changes" / "archive"
    if not archive.is_dir():
        return ()
    return tuple(
        sorted(archive.glob(f"*-close-fangcollect-exchange-loop/execution/{ADJUDICATION_FILENAME}"))
    )


def _lianjia_archive_candidates() -> tuple[Path, ...]:
    """lianjia 批次归档回落（archive/<date>-expand-lianjia-alias-rebaseline）。"""
    archive = _REPO_ROOT / "openspec" / "changes" / "archive"
    if not archive.is_dir():
        return ()
    return tuple(
        sorted(
            archive.glob(
                f"*-expand-lianjia-alias-rebaseline/execution/{LIANJIA_ADJUDICATION_FILENAME}"
            )
        )
    )


def default_adjudication_csv() -> Path:
    """冻结裁决表默认路径：examples 约定目录优先，changes 归档目录回落。"""
    candidates = _ADJUDICATION_CANDIDATES + _archive_candidates()
    if not candidates:
        raise FileNotFoundError(
            f"冻结裁决表候选为空：{_ADJUDICATION_CANDIDATES[0]}"
        )
    return next((p for p in candidates if p.is_file()), candidates[0])


def default_lianjia_adjudication_csv() -> Path:
    """lianjia 冻结裁决表默认路径：examples 约定目录优先，changes 归档目录回落。"""
    candidates = _LIANJIA_ADJUDICATION_CANDIDATES + _lianjia_archive_candidates()
    if not candidates:
        raise FileNotFoundError(
            f"冻结裁决表候选为空：{_LIANJIA_ADJUDICATION_CANDIDATES[0]}"
        )
    return next((p for p in candidates if p.is_file()), candidates[0])


def parse_adjudication_rows(
    csv_path: Path,
    *,
    name_column: str = "小区名",
    target_column: str | None = None,
    no_row_dispositions: Sequence[str] = (DISPOSITION_OFF_TABLE,),
    require_target_dispositions: Sequence[str] | None = None,
    allowed_dispositions: Sequence[str] | None = None,
) -> tuple[dict[str, str], ...]:
    """读取冻结裁决表（utf-8-sig），按行序返回字典序列（含守卫校验）。

    守卫：必要列齐全、名字不重复、处置合法；不落行处置 = ``no_row_dispositions``
    （fangcollect 默认 = 名录外不入表）。表未携带目标 ``community_id`` 列时出现
    落表处置 → 显式报错，防止静默丢弃用户裁决；携带目标列时 ``一致`` 处置
    （或 ``require_target_dispositions`` 指定的处置）目标为空 → 显式报错
    （排除行可无实体锚，由批次行构造器落占位值）。
    """
    no_row = frozenset(no_row_dispositions)
    required = {name_column, "处置"}
    if target_column is not None:
        required.add(target_column)
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"冻结裁决表缺少必要列 {sorted(required)}：{csv_path}")
        rows = [
            {k: (v or "").strip() for k, v in row.items() if k is not None}
            for row in reader
        ]
    if not rows:
        raise ValueError(f"冻结裁决表无数据行：{csv_path}")
    names = [r[name_column] for r in rows]
    if len(set(names)) != len(names):
        raise ValueError(f"冻结裁决表小区名重复：{names}")
    if allowed_dispositions is not None:
        allowed = frozenset(allowed_dispositions)
        bad = sorted({r["处置"] for r in rows} - allowed)
        if bad:
            raise ValueError(
                f"冻结裁决表存在未授权处置 {bad}（合法值 = "
                f"{sorted(allowed)}）：{csv_path}"
            )
    if target_column is None:
        off_table = [r[name_column] for r in rows if r["处置"] in no_row]
        on_table = sorted(set(names) - set(off_table))
        if on_table:
            statuses = sorted({r["处置"] for r in rows if r["处置"] not in no_row})
            raise ValueError(
                f"冻结裁决表存在落表处置 {statuses}，当前表格式未携带目标 community_id 列，"
                f"拒绝静默丢弃裁决：{on_table}"
            )
    else:
        need_target = (
            frozenset(require_target_dispositions)
            if require_target_dispositions is not None
            else None
        )
        missing = [
            r[name_column]
            for r in rows
            if r["处置"] not in no_row
            and r[target_column] == ""
            and (need_target is None or r["处置"] in need_target)
        ]
        if missing:
            raise ValueError(
                f"冻结裁决表落表处置缺少目标 community_id，拒绝静默丢弃裁决：{missing}"
            )
    return tuple(rows)


def fangcollect_alias_rows(
    adjudicated: tuple[dict[str, str], ...],
) -> tuple[CommunityAlias, ...]:
    """按冻结裁决解析本批次别名行。

    处置 = 待补实体_名录外不入表 → 不落行（名录外不建实体，见模块 docstring）；
    其余处置已在解析层守卫拒绝。当前裁决下恒为空元组。
    """
    return tuple(
        CommunityAlias(
            alias_id=f"{FANGCOLLECT_BATCH_PREFIX}{seq}",
            community_id=r["目标community_id"],
            source_alias=r["小区名"],
            source_id=FANGCOLLECT_SOURCE_ID,
            source_ref=(
                f"close-fangcollect-exchange-loop {ADJUDICATION_FILENAME}"
                f"（{ADJUDICATED_AT} 用户裁决）：{r['小区名']}"
            ),
            conflict_status=AliasConflictStatus(r["处置"]),
        )
        for seq, r in enumerate(
            (row for row in adjudicated if row["处置"] != DISPOSITION_OFF_TABLE),
            start=1,
        )
    )


def lianjia_alias_rows(
    adjudicated: tuple[dict[str, str], ...],
) -> tuple[CommunityAlias, ...]:
    """按 lianjia 冻结裁决解析本批次别名行（源名 = 链家叫法）。

    处置 = ``一致`` → 一致别名行（目标 community_id 必填，解析守卫已校验）；
    处置 = ``排除`` → 排除终态行（道路级命名等不参与自动映射；链家源名无
    实体锚，community_id 落 UNKNOWN 占位，溯源靠 source_ref）；
    处置 = ``待定``/``维持既有`` → 不落行。
    """
    return tuple(
        CommunityAlias(
            alias_id=f"{LIANJIA_BATCH_PREFIX}{seq}",
            community_id=(
                r[LIANJIA_TARGET_COLUMN] or LIANJIA_UNANCHORED_COMMUNITY_ID
            ),
            source_alias=r[LIANJIA_NAME_COLUMN],
            source_id=LIANJIA_SOURCE_ID,
            source_ref=(
                f"expand-lianjia-alias-rebaseline {LIANJIA_ADJUDICATION_RECORD}"
                f"（{LIANJIA_ADJUDICATED_AT} 用户裁决，冻结表 "
                f"{LIANJIA_ADJUDICATION_FILENAME}）：{r[LIANJIA_NAME_COLUMN]}"
            ),
            conflict_status=AliasConflictStatus(r["处置"]),
        )
        for seq, r in enumerate(
            (row for row in adjudicated if row["处置"] in LIANJIA_ROW_DISPOSITIONS),
            start=1,
        )
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_existing_aliases(
    alias_path: Path,
    batch_prefixes: Sequence[str] = (FANGCOLLECT_BATCH_PREFIX,),
) -> list[CommunityAlias]:
    """读取既有别名行（剔除指定批次前缀行，幂等重建）。"""
    table = pq.read_table(alias_path)
    kept: list[CommunityAlias] = []
    for aid, cid, name, sid, ref, status in zip(
        table.column("alias_id").to_pylist(),
        table.column("community_id").to_pylist(),
        table.column("source_alias").to_pylist(),
        table.column("source_id").to_pylist(),
        table.column("source_ref").to_pylist(),
        table.column("conflict_status").to_pylist(),
        strict=True,
    ):
        if any(aid.startswith(prefix) for prefix in batch_prefixes):
            continue
        kept.append(
            CommunityAlias(
                alias_id=aid,
                community_id=cid,
                source_alias=name,
                source_id=sid,
                source_ref=ref,
                conflict_status=AliasConflictStatus(status),
            )
        )
    return kept


def _build_alias_batch(
    *,
    data_dir: Path,
    adjudication_csv: Path,
    new_rows: tuple[CommunityAlias, ...],
    batch_prefixes: Sequence[str],
    dataset_label: str,
    fetched_at: str,
    default_notes_of: Callable[[int], str],
    notes: str | None,
) -> Path:
    """批次登记公共管线：幂等重写 ``community_alias.parquet`` + manifest。

    ``default_notes_of`` 接收合并后全表行数（notes 留痕用）。
    """
    entities_dir = data_dir / ENTITIES_LAYER
    alias_path = entities_dir / ALIAS_FILENAME
    if not alias_path.is_file():
        raise FileNotFoundError(f"别名表不存在，请先构建骨架：{alias_path}")
    kept = _read_existing_aliases(alias_path, batch_prefixes)

    merged = kept + list(new_rows)
    ids = [r.alias_id for r in merged]
    if len(ids) != len(set(ids)):
        raise AssertionError("alias_id 重复")

    inputs = [
        InputRef(
            dataset=dataset_label,
            fetched_at=fetched_at,
            content_hash=_sha256_file(adjudication_csv),
        ),
        InputRef(
            dataset="community_alias_previous",
            fetched_at=fetched_at,
            content_hash=_sha256_file(alias_path),
        ),
    ]
    return write_alias_entity(
        alias_table(merged),
        data_dir=data_dir,
        inputs=inputs,
        notes=notes or default_notes_of(len(merged)),
    )


def build_alias_fangcollect_batch(
    *,
    data_dir: Path,
    adjudication_csv: Path | None = None,
    notes: str | None = None,
) -> Path:
    """登记 fangcollect 冻结裁决批次并幂等重写 ``community_alias.parquet`` + manifest。

    返回写入路径。本批 10 名处置全部为「待补实体_名录外不入表」→ 落表行 0，
    全表行数不变（87 = 87 + 0）；批次以 manifest inputs 与 notes 留痕。
    """
    csv_path = adjudication_csv or default_adjudication_csv()
    if not csv_path.is_file():
        raise FileNotFoundError(f"冻结裁决表缺失：{csv_path}")
    adjudicated = parse_adjudication_rows(csv_path)

    new_rows = fangcollect_alias_rows(adjudicated)
    return _build_alias_batch(
        data_dir=data_dir,
        adjudication_csv=csv_path,
        new_rows=new_rows,
        batch_prefixes=(FANGCOLLECT_BATCH_PREFIX,),
        dataset_label="fangcollect_adjudication_table",
        fetched_at=ADJUDICATED_AT,
        default_notes_of=lambda total: (
            f"close-fangcollect-exchange-loop fangcollect 批次：登记 {len(adjudicated)} 名"
            f"（处置={DISPOSITION_OFF_TABLE}，另见待补实体清单），落表行 {len(new_rows)}，"
            f"全表 {total} 行不变（{ADJUDICATED_AT} 用户裁决）"
        ),
        notes=notes,
    )


def build_alias_lianjia_batch(
    *,
    data_dir: Path,
    adjudication_csv: Path | None = None,
    notes: str | None = None,
) -> Path:
    """登记 lianjia 对照冻结裁决批次并幂等重写 ``community_alias.parquet`` + manifest。

    以 ``lianjia-对照裁决表-20260911.csv`` 为唯一输入：处置 = 一致（9 名）/ 排除
    （道路级命名终态行）落新行，待定/维持既有不落新行；既有非 ``LJ-`` 行
    （含 fangcollect 批次行）原序原样保留；批次以 manifest inputs 与 notes 留痕。
    """
    csv_path = adjudication_csv or default_lianjia_adjudication_csv()
    if not csv_path.is_file():
        raise FileNotFoundError(f"冻结裁决表缺失：{csv_path}")
    adjudicated = parse_adjudication_rows(
        csv_path,
        name_column=LIANJIA_NAME_COLUMN,
        target_column=LIANJIA_TARGET_COLUMN,
        no_row_dispositions=LIANJIA_NO_ROW_DISPOSITIONS,
        require_target_dispositions=("一致",),
        allowed_dispositions=LIANJIA_DISPOSITIONS,
    )

    by_disposition = Counter(r["处置"] for r in adjudicated)
    new_rows = lianjia_alias_rows(adjudicated)
    return _build_alias_batch(
        data_dir=data_dir,
        adjudication_csv=csv_path,
        new_rows=new_rows,
        batch_prefixes=(LIANJIA_BATCH_PREFIX,),
        dataset_label="lianjia_adjudication_table",
        fetched_at=LIANJIA_ADJUDICATED_AT,
        default_notes_of=lambda total: (
            f"expand-lianjia-alias-rebaseline lianjia 对照批次：登记 {len(adjudicated)} 名"
            f"（一致 {by_disposition['一致']} / 排除 {by_disposition['排除']} / "
            f"待定 {by_disposition['待定']} / 维持既有 {by_disposition['维持既有']}，"
            f"见 execution/裁决记录-20260911.md），落表行 {len(new_rows)}，"
            f"全表 {total} 行（{LIANJIA_ADJUDICATED_AT} 用户裁决）"
        ),
        notes=notes,
    )


__all__ = [
    "ADJUDICATED_AT",
    "ADJUDICATION_FILENAME",
    "DISPOSITION_OFF_TABLE",
    "FANGCOLLECT_BATCH_PREFIX",
    "FANGCOLLECT_SOURCE_ID",
    "LIANJIA_ADJUDICATION_FILENAME",
    "LIANJIA_ADJUDICATED_AT",
    "LIANJIA_BATCH_PREFIX",
    "LIANJIA_DISPOSITIONS",
    "LIANJIA_NAME_COLUMN",
    "LIANJIA_NO_ROW_DISPOSITIONS",
    "LIANJIA_ROW_DISPOSITIONS",
    "LIANJIA_SOURCE_ID",
    "LIANJIA_TARGET_COLUMN",
    "LIANJIA_UNANCHORED_COMMUNITY_ID",
    "build_alias_fangcollect_batch",
    "build_alias_lianjia_batch",
    "default_adjudication_csv",
    "default_lianjia_adjudication_csv",
    "fangcollect_alias_rows",
    "lianjia_alias_rows",
    "parse_adjudication_rows",
]
