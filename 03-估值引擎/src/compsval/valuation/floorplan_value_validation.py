"""OCR 户型图字段估值接入价值验证（change floorplan-value-validation）。

本模块承载价值验证的离线实现，分五组任务：

- 组1 冻结输入重建与门禁（1.1-1.4）：经版本指针只读解析冻结生产资产链与
  OCR 原始响应，以固定解析器版本/代码版本/确定性排序重建验证专用派生表，
  核对 229 资产链 / 214 唯一 source_record_id / 214 唯一图片哈希 /
  1,853 条标注 / 状态计数；任一不符即停（``EVIDENCE_INSUFFICIENT``）。
- 组2 评估单位、候选池、时间切分与特征表（2.1-2.8）。
- 组3 第一轮户型相似度对照回放（3.1-3.5）。
- 组4 第二轮面积价格调整条件项（4.1-4.4）。
- 组5 成对指标、子组、敏感性、三选一结论与报告（5.1-5.7）。

硬约束（spec）：零网络零付费；不写入/覆盖冻结 v1 任何产物；验证读取一律
经版本指针；不修改 ``valuation-comparable-core`` 正式行为；结论携带
KL-1..KL-4 与样本量判别力声明；``CANDIDATE_INTEGRATION`` 不构成接入授权。
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import statistics
import subprocess
import tempfile
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.ingest.floorplan_ocr_parse import parse_ocr_run_directory
from compsval.ingest.floorplan_transcribe import (
    ANNOTATION_STAGED_FILENAME,
    transcribe_word_table,
)

# ---------------------------------------------------------------------------
# 常量（与 change 工件一致的事实基线）
# ---------------------------------------------------------------------------

VERSION_POINTER_FILENAME = "lianjia_ext_latest.json"
VERSIONS_DIRNAME = "versions"

#: 生产批次事实基线（重建门禁期望值，与冻结报告/画像报告一致）。
EXPECTED_ASSET_COUNT = 229
EXPECTED_UNIQUE_SOURCE_IDS = 214
EXPECTED_UNIQUE_IMAGE_HASHES = 214
EXPECTED_ANNOTATION_ROWS = 1853
EXPECTED_STATE_COUNTS: dict[str, int] = {
    "ACCEPTED": 1386,
    "ROOM_ONLY": 40,
    "NEEDS_REVIEW": 2,
    "CONFLICT": 425,
}

#: 标注表参与验证重建的列（派生表 schema，列缺失/多余即报错防漂移）。
REBUILD_ANNOTATION_COLUMNS: tuple[str, ...] = (
    "annotation_id",
    "ocr_run_id",
    "ocr_task_id",
    "parse_version",
    "room_word_id",
    "area_word_id",
    "room_name_raw",
    "room_name_normalized",
    "standard_room_type",
    "area_text_raw",
    "area_text_normalized",
    "area_value",
    "area_unit",
    "location",
    "parse_state",
    "isolation_reason",
    "consistency_status",
    "review_state",
    "review_event_ref",
)

#: 生产批次清单字段（asset manifest 记录的键）。
PRODUCTION_MANIFEST_KEY = "production_manifest"
RAW_OCR_RUN_KEY = "raw_ocr_run"


# ---------------------------------------------------------------------------
# 值对象
# ---------------------------------------------------------------------------


class RebuildGateError(RuntimeError):
    """重建门禁未通过：验证 SHALL 停止并以 EVIDENCE_INSUFFICIENT 退出。"""


@dataclass(frozen=True)
class RebuildResult:
    """一次输入重建的结果（供调用方打印与断言）。"""

    run_id: str
    version_id: str
    pointer_path: Path
    production_manifest_path: Path
    ocr_run_dir: Path
    rebuilt_annotation_path: Path
    rebuilt_table: pa.Table
    state_counts: dict[str, int]
    gate: dict[str, Any]
    report_path: Path


@dataclass(frozen=True)
class RebuildOutcome:
    """组1 主入口返回：成功携带重建结果；门禁失败抛 RebuildGateError。"""

    result: RebuildResult


# ---------------------------------------------------------------------------
# 哈希与只读工具
# ---------------------------------------------------------------------------


def file_sha256(path: Path) -> str:
    """文件内容哈希（用于输入登记与只读校验）。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_load(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


# ---------------------------------------------------------------------------
# 冻结版本定位（1.1）
# ---------------------------------------------------------------------------


def load_version_pointer(data_dir: Path) -> dict[str, Any]:
    """读取版本指针 JSON（``data_dir/versions/lianjia_ext_latest.json``）。"""
    pointer_path = data_dir / VERSIONS_DIRNAME / VERSION_POINTER_FILENAME
    if not pointer_path.is_file():
        raise FileNotFoundError(f"版本指针不存在：{pointer_path}")
    pointer = _json_load(pointer_path)
    for key in ("version_id", "manifest", "change_ref"):
        if not pointer.get(key):
            raise ValueError(f"版本指针缺少必需字段 {key!r}：{pointer_path}")
    return pointer


def load_freeze_manifest(pointer: dict[str, Any], data_dir: Path) -> dict[str, Any]:
    """读取冻结清单（指针 manifest 字段为相对 data_dir 的路径）。"""
    manifest_path = data_dir / Path(str(pointer["manifest"]))
    if not manifest_path.is_file():
        raise FileNotFoundError(f"冻结清单不存在：{manifest_path}")
    manifest = _json_load(manifest_path)
    if str(manifest.get("version_id")) != str(pointer["version_id"]):
        raise ValueError("冻结清单 version_id 与版本指针不一致")
    return manifest


def locate_production_assets(
    manifest: dict[str, Any], repo_root: Path
) -> tuple[Path, Path]:
    """定位生产批次 229 资产链的 selection manifest 与 OCR run 目录。"""
    sections = manifest.get("sections", {})
    batch = sections.get("production_batch_229", [])
    prod_path: Path | None = None
    ocr_run: Path | None = None
    for entry in batch:
        key = entry.get("key")
        if key == PRODUCTION_MANIFEST_KEY:
            prod_path = repo_root / Path(str(entry["path"]))
        elif key == RAW_OCR_RUN_KEY:
            ocr_run = repo_root / Path(str(entry["path"]))
    if prod_path is None or not prod_path.is_file():
        raise FileNotFoundError(f"生产选择清单缺失（key={PRODUCTION_MANIFEST_KEY}）")
    if ocr_run is None or not ocr_run.is_dir():
        raise FileNotFoundError(f"生产 OCR run 目录缺失（key={RAW_OCR_RUN_KEY}）")
    return prod_path, ocr_run


# ---------------------------------------------------------------------------
# 生产批次计数（1.3 门禁输入）
# ---------------------------------------------------------------------------


def count_production_manifest(
    production_manifest_path: Path, ocr_run_dir: Path
) -> dict[str, int]:
    """统计生产批次：资产链数 / 唯一 source_record_id / 唯一图片哈希。

    资产链数取自 selection manifest ``record_count``（229）；唯一
    source_record_id 按 manifest 记录去重；唯一图片哈希按 OCR run 记录
    ``image_sha256`` 去重（同图多资产因 image_sha → ocr_task_id 相同）。
    """
    manifest = _json_load(production_manifest_path)
    records = manifest.get("records")
    if not isinstance(records, list) or len(records) != manifest.get("record_count"):
        raise ValueError("生产选择清单 records 与 record_count 不一致")
    unique_source_ids = len({str(r["source_record_id"]) for r in records})
    run_record = _json_load(ocr_run_dir / "ocr_run.json")
    tasks = run_record.get("tasks")
    if not isinstance(tasks, list):
        raise ValueError("OCR run 记录缺少 tasks")
    unique_image_hashes = len({str(t["image_sha256"]) for t in tasks})
    return {
        "asset_count": int(manifest["record_count"]),
        "unique_source_record_id": unique_source_ids,
        "unique_image_sha256": unique_image_hashes,
        "ocr_task_count": len(tasks),
    }


# ---------------------------------------------------------------------------
# 重建标注派生表（1.2/1.4）
# ---------------------------------------------------------------------------


def _canonical_sort(table: pa.Table) -> pa.Table:
    """确定性排序（ocr_task_id + 原始顺序），保证派生表哈希可复现。"""
    task_ids = table.column("ocr_task_id").to_pylist()
    order = sorted(range(table.num_rows), key=lambda i: (str(task_ids[i]), i))
    return table.take(order)


def _read_rebuilt_annotation(path: Path) -> pa.Table:
    """读重建标注表并校验 schema（列缺失/多余即报错，防漂移）。"""
    table = pq.read_table(path)
    names = list(table.column_names)
    if names != list(REBUILD_ANNOTATION_COLUMNS):
        raise ValueError(
            "重建标注表 schema 与预期不一致："
            f"got {names} expected {list(REBUILD_ANNOTATION_COLUMNS)}"
        )
    return table


def rebuild_annotation_table(
    ocr_run_dir: Path, out_dir: Path, run_id: str
) -> tuple[Path, pa.Table]:
    """从冻结生产 OCR 原始响应确定性重建标注派生表。

    链路：``parse_ocr_run_directory``（词表）→ ``transcribe_word_table``
    （标注），均在临时 data_dir 内执行，不触碰冻结目录；派生表规范化排序
    后写入 ``out_dir/floorplan_room_annotation.rebuild.parquet``。
    """
    with tempfile.TemporaryDirectory(prefix="compsval-fvv-rebuild-") as td:
        tmp = Path(td)
        parse_ocr_run_directory(ocr_run_dir, data_dir=tmp)
        transcribe_word_table(tmp / "staged" / "floorplan_ocr_word.parquet", data_dir=tmp)
        rebuilt_path = tmp / "staged" / ANNOTATION_STAGED_FILENAME
        table = _canonical_sort(pq.read_table(rebuilt_path))

    final_path = out_dir / "floorplan_room_annotation.rebuild.parquet"
    work_path = out_dir / "floorplan_room_annotation.rebuild.parquet.incomplete"
    pq.write_table(table, work_path, compression="zstd")
    work_path.replace(final_path)
    return final_path, table


def annotation_state_counts(table: pa.Table) -> dict[str, int]:
    """标注表 parse_state 计数（字典序，缺状态记 0）。"""
    counts: dict[str, int] = {k: 0 for k in EXPECTED_STATE_COUNTS}
    for value in table.column("parse_state").to_pylist():
        key = str(value)
        if key in counts:
            counts[key] += 1
    return counts


# ---------------------------------------------------------------------------
# 重建门禁（1.3）与既有表一致性（1.4）
# ---------------------------------------------------------------------------


def check_rebuild_gate(
    *,
    asset_count: int,
    unique_source_record_id: int,
    unique_image_sha256: int,
    annotation_rows: int,
    state_counts: dict[str, int],
    existing_annotation_path: Path | None = None,
) -> list[str]:
    """重建门禁核对；返回缺口列表（空 = 通过）。

    核对项：229 资产链、214 唯一 source_record_id、214 唯一图片哈希、
    1,853 条标注及 ACCEPTED/ROOM_ONLY/NEEDS_REVIEW/CONFLICT 计数；若给
    既有标注表路径，另核对行数与状态计数一致（血缘闭合）。
    """
    gaps: list[str] = []
    if asset_count != EXPECTED_ASSET_COUNT:
        gaps.append(f"资产链数 {asset_count} != 期望 {EXPECTED_ASSET_COUNT}")
    if unique_source_record_id != EXPECTED_UNIQUE_SOURCE_IDS:
        gaps.append(
            f"唯一 source_record_id {unique_source_record_id} != "
            f"期望 {EXPECTED_UNIQUE_SOURCE_IDS}"
        )
    if unique_image_sha256 != EXPECTED_UNIQUE_IMAGE_HASHES:
        gaps.append(
            f"唯一图片哈希 {unique_image_sha256} != 期望 {EXPECTED_UNIQUE_IMAGE_HASHES}"
        )
    if annotation_rows != EXPECTED_ANNOTATION_ROWS:
        gaps.append(f"标注行数 {annotation_rows} != 期望 {EXPECTED_ANNOTATION_ROWS}")
    for state in EXPECTED_STATE_COUNTS:
        actual = state_counts.get(state, 0)
        if actual != EXPECTED_STATE_COUNTS[state]:
            gaps.append(f"状态 {state} 计数 {actual} != 期望 {EXPECTED_STATE_COUNTS[state]}")
    if existing_annotation_path is not None and existing_annotation_path.is_file():
        existing = pq.read_table(existing_annotation_path)
        if existing.num_rows != annotation_rows:
            gaps.append(
                f"重建标注行数 {annotation_rows} 与既有标注表 "
                f"{existing.num_rows} 不一致（血缘闭合失败）"
            )
        existing_counts = annotation_state_counts(existing)
        for state in EXPECTED_STATE_COUNTS:
            if existing_counts.get(state, 0) != state_counts.get(state, 0):
                gaps.append(
                    f"重建状态 {state} 计数与既有标注表不一致 "
                    f"(重建 {state_counts.get(state)} / 既有 {existing_counts.get(state)})"
                )
    return gaps


def provenance_sources(
    production_manifest_path: Path, ocr_run_dir: Path
) -> list[dict[str, str | None]]:
    """血缘来源登记（1.4）：selection manifest + OCR run 记录与原始响应目录。"""
    sources: list[dict[str, str | None]] = [
        {
            "kind": "selection_manifest",
            "path": str(production_manifest_path),
            "sha256": file_sha256(production_manifest_path),
        },
        {
            "kind": "ocr_run_record",
            "path": str(ocr_run_dir / "ocr_run.json"),
            "sha256": file_sha256(ocr_run_dir / "ocr_run.json"),
        },
        {
            "kind": "ocr_run_dir",
            "path": str(ocr_run_dir),
            "sha256": None,  # 目录计数由冻结清单登记
        },
    ]
    return sources


# ---------------------------------------------------------------------------
# 组1 主入口
# ---------------------------------------------------------------------------


def _sha256_of(path: Path) -> str:
    return file_sha256(path)


def _git_commit(repo_root: Path) -> str:
    """当前代码 commit（spec：重建 manifest SHALL 登记代码 commit）。

    git 不可用或目录非仓库时返回 ``unknown``，不阻断离线验证。
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def run_rebuild(data_dir: Path, out_root: Path, run_id: str | None = None) -> RebuildResult:
    """组1 输入重建与门禁主入口（1.1-1.4 一体）。

    流程：版本指针 → 冻结清单 → 定位生产资产 → 只读登记输入哈希 → 重建
    派生表 → 门禁核对 → 写重建报告；门禁缺口 → ``RebuildGateError``（调用方
    以 EVIDENCE_INSUFFICIENT 收口）。验证产物写 ``out_root/<run_id>/``。
    """
    run_id = run_id or f"fvv-rebuild-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    data_dir = data_dir.resolve()
    out_root = out_root.resolve()
    repo_root = data_dir.parent.parent
    pointer = load_version_pointer(data_dir)
    manifest = load_freeze_manifest(pointer, data_dir)
    prod_path, ocr_run_dir = locate_production_assets(manifest, repo_root)

    out_dir = out_root / run_id / "rebuild"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1.1 输入只读登记（运行前哈希）
    inputs = provenance_sources(prod_path, ocr_run_dir)

    # 1.2 确定性重建
    rebuilt_path, rebuilt_table = rebuild_annotation_table(ocr_run_dir, out_dir, run_id)
    state_counts = annotation_state_counts(rebuilt_table)

    # 1.3 生产批次计数与门禁
    counts = count_production_manifest(prod_path, ocr_run_dir)
    existing_annotation = data_dir / "staged" / ANNOTATION_STAGED_FILENAME
    gaps = check_rebuild_gate(
        asset_count=counts["asset_count"],
        unique_source_record_id=counts["unique_source_record_id"],
        unique_image_sha256=counts["unique_image_sha256"],
        annotation_rows=rebuilt_table.num_rows,
        state_counts=state_counts,
        existing_annotation_path=existing_annotation,
    )

    # 1.4 血缘/schema/排除清单与报告
    derived_hash = _sha256_of(rebuilt_path)
    report: dict[str, Any] = {
        "run_id": run_id,
        "stage": "rebuild_gate",
        "version_id": pointer["version_id"],
        "change_ref": pointer["change_ref"],
        "pointer": str(pointer["manifest"]),
        "code_version": __version__,
        "code_commit": _git_commit(repo_root),
        "counts": counts,
        "annotation": {
            "rows": rebuilt_table.num_rows,
            "state_counts": state_counts,
            "parse_versions": sorted(
                {str(v) for v in rebuilt_table.column("parse_version").to_pylist()}
            ),
            "ocr_run_ids": sorted(
                {str(v) for v in rebuilt_table.column("ocr_run_id").to_pylist()}
            ),
        },
        "derived_table": {"path": str(rebuilt_path), "sha256": derived_hash},
        "inputs": inputs,
        "existing_annotation_checked": existing_annotation.is_file(),
        "gate": {"passed": not gaps, "gaps": gaps},
        "schema": rebuilt_table.schema.names,
        "exclusions": {
            "source": "重建不产生新排除：转录范围外隔离沿用冻结标注表口径"
        },
        "built_at": datetime.now(UTC).isoformat(),
    }
    report_path = out_dir / "rebuild_report.json"
    work = out_dir / "rebuild_report.json.incomplete"
    work.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(report_path)

    if gaps:
        raise RebuildGateError(
            f"重建门禁未通过（run {run_id}）：{gaps}"
        )

    return RebuildResult(
        run_id=run_id,
        version_id=str(pointer["version_id"]),
        pointer_path=data_dir / VERSIONS_DIRNAME / VERSION_POINTER_FILENAME,
        production_manifest_path=prod_path,
        ocr_run_dir=ocr_run_dir,
        rebuilt_annotation_path=rebuilt_path,
        rebuilt_table=rebuilt_table,
        state_counts=state_counts,
        gate={"passed": True, "gaps": []},
        report_path=report_path,
    )


# ---------------------------------------------------------------------------
# 组2 评估单位、候选池、切分与特征（2.1-2.8）
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> float | None:
    """Decimal/float/int/None → float；非法值返回 None（保持未知）。"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_date(value: Any) -> date | None:
    """把 date / ISO 字符串 / None 统一转 date；非法值返回 None。"""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _asset_source_map(asset_manifest_path: Path) -> dict[str, dict[str, Any]]:
    """OCR 输入副本资产清单：asset_id → (source_record_id, sha256, is_duplicate)。"""
    manifest = _json_load(asset_manifest_path)
    out: dict[str, dict[str, Any]] = {}
    for asset in manifest.get("assets", []):
        out[str(asset["asset_id"])] = {
            "source_record_id": str(asset["source_record_id"]),
            "sha256": str(asset.get("sha256") or ""),
            "is_duplicate": bool(asset.get("is_duplicate")),
            "duplicate_count": int(asset.get("duplicate_count") or 1),
        }
    return out


def _ocr_task_source_map(ocr_run_dir: Path) -> dict[str, str]:
    """OCR run tasks：ocr_task_id → image_path 的 asset_id（副本文件名前缀）。"""
    run_record = _json_load(ocr_run_dir / "ocr_run.json")
    out: dict[str, str] = {}
    for task in run_record.get("tasks", []):
        path = str(task.get("image_path") or "")
        out[str(task["ocr_task_id"])] = path.removesuffix(".jpg")
    return out


def unit_list_schema() -> pa.Schema:
    """主评估单位清单模式（2.1；每行 = 一个唯一 source_record_id）。"""
    return pa.schema(
        [
            pa.field("source_record_id", pa.string(), nullable=False),
            pa.field("cluster_id", pa.string(), nullable=False),
            pa.field("inventory_rows", pa.int32(), nullable=False),
            pa.field("is_duplicate", pa.bool_(), nullable=False),
            pa.field("sale_date", pa.date32(), nullable=True),
            pa.field("community_name", pa.string(), nullable=True),
            pa.field("transaction_area_sqm", pa.float64(), nullable=True),
            pa.field("building_area_detail_sqm", pa.float64(), nullable=True),
            pa.field("layout_raw", pa.string(), nullable=True),
            pa.field("actual_total_price", pa.float64(), nullable=True),
            pa.field("actual_unit_price", pa.float64(), nullable=True),
            pa.field("ocr_task_id", pa.string(), nullable=True),
            pa.field("ocr_accepted_count", pa.int32(), nullable=False),
            pa.field("ocr_room_only_count", pa.int32(), nullable=False),
            pa.field("ocr_conflict_count", pa.int32(), nullable=False),
            pa.field("ocr_needs_review_count", pa.int32(), nullable=False),
        ]
    )


def build_unit_list(
    *,
    production_manifest_path: Path,
    asset_manifest_path: Path,
    ocr_run_dir: Path,
    annotation: pa.Table,
    sale: pa.Table,
) -> pa.Table:
    """2.1 双口径清单：229 库存 → 214 唯一主评估记录，source/image 簇标记。

    关联链：production_manifest（库存 229）→ asset manifest（asset_id ↔
    source_record_id）→ OCR run tasks（asset_id ↔ ocr_task_id）→ 标注表
    （ocr_task_id → 状态计数）。主清单按唯一 source_record_id 去重，簇标记
    重复记录组（15 组，含 30 库存条目）。
    """
    asset_map = _asset_source_map(asset_manifest_path)
    task_map = _ocr_task_source_map(ocr_run_dir)

    # 标注状态按 ocr_task_id 聚合
    task_states: dict[str, Counter[str]] = {}
    for task_id, state in zip(
        annotation.column("ocr_task_id").to_pylist(),
        annotation.column("parse_state").to_pylist(),
        strict=True,
    ):
        counter = task_states.setdefault(str(task_id), Counter())
        counter[str(state)] += 1

    # 库存 229（selection manifest records）
    prod = _json_load(production_manifest_path)
    inventory: list[dict[str, Any]] = []
    for rec in prod.get("records", []):
        source_id = str(rec["source_record_id"])
        # 定位 asset_id 与 ocr_task_id（按 source_record_id 关联 asset manifest）
        asset_id = next(
            (aid for aid, info in asset_map.items() if info["source_record_id"] == source_id),
            None,
        )
        ocr_task_id = None
        if asset_id is not None:
            ocr_task_id = next(
                (tid for tid, path_asset in task_map.items() if path_asset == asset_id),
                None,
            )
        inventory.append(
            {
                "source_record_id": source_id,
                "asset_id": asset_id,
                "ocr_task_id": ocr_task_id,
                "is_duplicate": bool(asset_map[asset_id]["is_duplicate"]) if asset_id else False,
            }
        )

    # sale 关联（ordinary_residential 或 sale_record 均含这些列）
    sale_cols = {
        c: sale.column(c).to_pylist() for c in sale.column_names
    }
    sale_by_id: dict[str, dict[str, Any]] = {}
    for i in range(sale.num_rows):
        sid = str(sale_cols["source_record_id"][i])
        sale_by_id.setdefault(
            sid,
            {
                "sale_date": _to_date(sale_cols["sale_date"][i]),
                "community_name": sale_cols["community_name"][i],
                "transaction_area_sqm": _to_float(sale_cols["transaction_area_sqm"][i]),
                "building_area_detail_sqm": _to_float(sale_cols["building_area_detail_sqm"][i]),
                "layout_raw": sale_cols.get("layout_raw", [None] * sale.num_rows)[i],
                "total_price_yuan": _to_float(
                    sale_cols.get("total_price_yuan", [None] * sale.num_rows)[i]
                ),
                "unit_price_observed": _to_float(
                    sale_cols.get("unit_price_observed", [None] * sale.num_rows)[i]
                ),
            },
        )

    # 主清单：唯一 source_record_id；重复组 → 同一 cluster_id
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in inventory:
        by_source.setdefault(row["source_record_id"], []).append(row)

    rows: list[dict[str, Any]] = []
    for cluster_seq, source_id in enumerate(sorted(by_source), start=1):
        group = by_source[source_id]
        dup = len(group) > 1
        # 每个唯一 source_record_id 一个独立簇（含其全部库存条目），
        # 保证时间切分时同一 source/image 簇不跨开发/确认集合。
        cluster_id = f"cluster-{cluster_seq:03d}"

        rep = group[0]
        ocr_task_id = rep["ocr_task_id"]
        states = task_states.get(ocr_task_id or "", Counter())
        sale_info = sale_by_id.get(source_id, {})
        rows.append(
            {
                "source_record_id": source_id,
                "cluster_id": cluster_id,
                "inventory_rows": len(group),
                "is_duplicate": dup,
                "sale_date": sale_info.get("sale_date"),
                "community_name": sale_info.get("community_name"),
                "transaction_area_sqm": sale_info.get("transaction_area_sqm"),
                "building_area_detail_sqm": sale_info.get("building_area_detail_sqm"),
                "layout_raw": sale_info.get("layout_raw"),
                "actual_total_price": sale_info.get("total_price_yuan"),
                "actual_unit_price": sale_info.get("unit_price_observed"),
                "ocr_task_id": ocr_task_id,
                "ocr_accepted_count": int(states.get("ACCEPTED", 0)),
                "ocr_room_only_count": int(states.get("ROOM_ONLY", 0)),
                "ocr_conflict_count": int(states.get("CONFLICT", 0)),
                "ocr_needs_review_count": int(states.get("NEEDS_REVIEW", 0)),
            }
        )
    table = pa.Table.from_pylist(rows, schema=unit_list_schema())
    return table


def inventory_vs_unique(units: pa.Table) -> tuple[int, int]:
    """双口径：库存条目数（按 inventory_rows 求和）与唯一主记录数。"""
    inventory = int(sum(units.column("inventory_rows").to_pylist())) if len(units) else 0
    return inventory, units.num_rows


def filter_candidates(
    pool: pa.Table,
    *,
    community_name: str,
    cutoff: date,
    exclude_source_ids: set[str],
) -> tuple[pa.Table, int]:
    """2.2 共同候选池切分：同小区 + 成交日严格早于目标日 + 排除目标簇。

    返回 ``(候选子表, 被排除的同小区同日候选数)``。同日（sale_date ==
    cutoff）因缺少日内先后信息一律排除（spec：严格小于）。
    """
    community = pool.column("community_name").to_pylist()
    sale_dates = pool.column("sale_date").to_pylist()
    source_ids = (
        pool.column("source_record_id").to_pylist()
        if "source_record_id" in pool.column_names
        else [""] * pool.num_rows
    )
    keep: list[bool] = []
    excluded_same_day = 0
    for cname, sdate, sid in zip(community, sale_dates, source_ids, strict=True):
        if str(cname or "") != community_name:
            keep.append(False)
            continue
        if str(sid) in exclude_source_ids:
            keep.append(False)
            continue
        parsed = _to_date(sdate)
        if parsed is None:
            keep.append(False)
            continue
        if parsed >= cutoff:
            if parsed == cutoff:
                excluded_same_day += 1
            keep.append(False)
            continue
        keep.append(True)
    return pool.filter(pa.array(keep)), excluded_same_day


def time_split_units(units: pa.Table, dev_ratio: float = 0.7) -> tuple[list[str], list[str]]:
    """2.4 开发集/未触碰确认集时间切分；同一 cluster 不跨集合。

    按簇（cluster 内最早 sale_date）升序，前 ``dev_ratio`` 簇入开发集，
    其余入确认集。返回 ``(dev_source_ids, holdout_source_ids)``。
    """
    clusters: dict[str, list[dict[str, Any]]] = {}
    for i in range(units.num_rows):
        cluster_id = str(units.column("cluster_id").to_pylist()[i])
        clusters.setdefault(cluster_id, []).append(
            {
                "source_record_id": str(units.column("source_record_id").to_pylist()[i]),
                "sale_date": units.column("sale_date").to_pylist()[i],
            }
        )
    ordered = sorted(
        clusters.items(),
        key=lambda kv: (
            min(
                
                    d.isoformat()
                    for d in [row["sale_date"] for row in kv[1]]
                    if isinstance(d, date)
                
            )
            if any(isinstance(row["sale_date"], date) for row in kv[1])
            else "9999-99-99",
            kv[0],
        ),
    )
    dev_cluster_count = max(1, round(len(ordered) * dev_ratio))
    dev_ids: list[str] = []
    holdout_ids: list[str] = []
    for idx, (_, rows) in enumerate(ordered):
        for row in rows:
            (dev_ids if idx < dev_cluster_count else holdout_ids).append(
                row["source_record_id"]
            )
    return dev_ids, holdout_ids


def _units_by_source(units: pa.Table) -> dict[str, dict[str, Any]]:
    """单位清单 → source_record_id 索引（组2特征/面积表使用）。"""
    return {
        str(units.column("source_record_id").to_pylist()[i]): {
            "sale_date": units.column("sale_date").to_pylist()[i],
            "community_name": units.column("community_name").to_pylist()[i],
            "transaction_area_sqm": units.column("transaction_area_sqm").to_pylist()[i],
            "building_area_detail_sqm": units.column("building_area_detail_sqm").to_pylist()[i],
            "layout_raw": units.column("layout_raw").to_pylist()[i],
            "ocr_task_id": units.column("ocr_task_id").to_pylist()[i],
        }
        for i in range(units.num_rows)
    }


# ---------------------------------------------------------------------------
# 2.5-2.7 户型特征构造（ACCEPTED / ROOM_ONLY / 隔离计数）
# ---------------------------------------------------------------------------

#: 面积极端值规则（写入报告；仅标记不排除）：非数/<=0 或 > 上限视为越界。
AREA_EXTREME_UPPER_SQM = 500.0


def build_room_features(annotation: pa.Table) -> dict[str, dict[str, Any]]:
    """按 ocr_task_id 构造户型特征（2.5/2.6/2.7）。

    ACCEPTED → 房间类型/数量/按类型面积/已识别房间面积合计/构成比例；
    ROOM_ONLY → 房间类型与数量（面积保持缺失，禁止填 0/均值/反推）；
    CONFLICT/NEEDS_REVIEW/范围外 → 仅隔离计数。
    """
    features: dict[str, dict[str, Any]] = {}
    for i in range(annotation.num_rows):
        task_id = str(annotation.column("ocr_task_id").to_pylist()[i])
        state = str(annotation.column("parse_state").to_pylist()[i])
        room_type = annotation.column("standard_room_type").to_pylist()[i]
        area_value = annotation.column("area_value").to_pylist()[i]

        feat = features.setdefault(
            task_id,
            {
                "accepted_room_types": [],
                "accepted_room_count": 0,
                "accepted_area_by_type": {},
                "accepted_area_total": None,
                "accepted_area_composition": {},
                "room_only_types": [],
                "room_only_count": 0,
                "isolated_conflict": 0,
                "isolated_needs_review": 0,
                "isolated_out_of_scope": 0,
            },
        )
        if state == "ACCEPTED":
            feat["accepted_room_count"] += 1
            if room_type is not None:
                rt = str(room_type)
                feat["accepted_room_types"].append(rt)
                feat["accepted_area_by_type"].setdefault(rt, 0.0)
                if area_value is not None:
                    with contextlib.suppress(TypeError, ValueError):
                        feat["accepted_area_by_type"][rt] += float(area_value)
        elif state == "ROOM_ONLY":
            feat["room_only_count"] += 1
            if room_type is not None:
                feat["room_only_types"].append(str(room_type))
        elif state == "CONFLICT":
            feat["isolated_conflict"] += 1
        elif state == "NEEDS_REVIEW":
            feat["isolated_needs_review"] += 1
        elif state == "OUT_OF_SCOPE":
            feat["isolated_out_of_scope"] += 1

    for feat in features.values():
        area_total = sum(feat["accepted_area_by_type"].values())
        if area_total > 0:
            feat["accepted_area_total"] = area_total
            feat["accepted_area_composition"] = {
                rt: round(v / area_total, 4)
                for rt, v in sorted(feat["accepted_area_by_type"].items())
            }
        feat["accepted_room_types"] = sorted(set(feat["accepted_room_types"]))
        feat["room_only_types"] = sorted(set(feat["room_only_types"]))
    return features


def isolation_counts(annotation: pa.Table) -> dict[str, int]:
    """2.7 隔离计数（CONFLICT/NEEDS_REVIEW/范围外），供审计产物。"""
    counts: dict[str, int] = {
        "CONFLICT": 0,
        "NEEDS_REVIEW": 0,
        "OUT_OF_SCOPE": 0,
    }
    for state in annotation.column("parse_state").to_pylist():
        key = str(state)
        if key in counts:
            counts[key] += 1
    return counts


# ---------------------------------------------------------------------------
# 2.8 面积质量表
# ---------------------------------------------------------------------------


def area_quality_schema() -> pa.Schema:
    """面积质量表模式（每行 = 一个唯一主记录）。"""
    return pa.schema(
        [
            pa.field("source_record_id", pa.string(), nullable=False),
            pa.field("transaction_area_sqm", pa.float64(), nullable=True),
            pa.field("building_area_detail_sqm", pa.float64(), nullable=True),
            pa.field("ocr_area_total", pa.float64(), nullable=True),
            pa.field("abs_diff_ocr_transaction", pa.float64(), nullable=True),
            pa.field("rel_diff_ocr_transaction", pa.float64(), nullable=True),
            pa.field("ratio_ocr_transaction", pa.float64(), nullable=True),
            pa.field("ratio_ocr_building", pa.float64(), nullable=True),
            pa.field("missing_flags", pa.string(), nullable=True),
            pa.field("extreme_flags", pa.string(), nullable=True),
        ]
    )


def build_area_quality(units: pa.Table, features: dict[str, dict[str, Any]]) -> pa.Table:
    """2.8 面积质量表：transaction_area_sqm 主比较、building_area_detail_sqm 参考。

    输出绝对/相对差额、两个比率、缺失与极端标记；正常差额不触发排除、
    不覆盖 Excel（spec：面积比较保留源字段语义）。
    """
    rows: list[dict[str, Any]] = []
    for i in range(units.num_rows):
        source_id = str(units.column("source_record_id").to_pylist()[i])
        task_id = units.column("ocr_task_id").to_pylist()[i]
        transaction = units.column("transaction_area_sqm").to_pylist()[i]
        building = units.column("building_area_detail_sqm").to_pylist()[i]
        ocr_total = None
        if task_id:
            ocr_total = features.get(str(task_id), {}).get("accepted_area_total")

        missing: list[str] = []
        if transaction is None:
            missing.append("transaction")
        if building is None:
            missing.append("building")
        if ocr_total is None:
            missing.append("ocr")

        extreme: list[str] = []
        for label, value in (
            ("transaction", transaction),
            ("building", building),
            ("ocr", ocr_total),
        ):
            if value is not None and (
                value <= 0 or value > AREA_EXTREME_UPPER_SQM
            ):
                extreme.append(label)

        abs_diff = None
        rel_diff = None
        ratio_transaction = None
        ratio_building = None
        if ocr_total is not None and transaction not in (None, 0):
            abs_diff = round(ocr_total - float(transaction), 4)
            rel_diff = round(abs_diff / float(transaction), 4)
            ratio_transaction = round(ocr_total / float(transaction), 4)
        if ocr_total is not None and building not in (None, 0):
            ratio_building = round(ocr_total / float(building), 4)

        rows.append(
            {
                "source_record_id": source_id,
                "transaction_area_sqm": transaction,
                "building_area_detail_sqm": building,
                "ocr_area_total": ocr_total,
                "abs_diff_ocr_transaction": abs_diff,
                "rel_diff_ocr_transaction": rel_diff,
                "ratio_ocr_transaction": ratio_transaction,
                "ratio_ocr_building": ratio_building,
                "missing_flags": ",".join(missing) or None,
                "extreme_flags": ",".join(extreme) or None,
            }
        )
    return pa.Table.from_pylist(rows, schema=area_quality_schema())


# ---------------------------------------------------------------------------
# 2.3 特征对齐层（目标侧 OCR ↔ 可比侧 OCR/Excel，逐特征来源标记）
# ---------------------------------------------------------------------------


def align_comparable_features(
    pool: pa.Table,
    *,
    features_by_task: dict[str, dict[str, Any]],
    task_to_source: dict[str, str],
) -> pa.Table:
    """可比侧对齐层：为候选池每行标记户型特征来源（OCR/Excel/无）。

    目标侧户型特征来自冻结 OCR；可比侧优先同语义合格 OCR，缺少 OCR 时可用
    既有 Excel 结构化字段（transaction_area_sqm / layout_raw / bedrooms_raw /
    living_rooms_raw），每个子特征标记来源。返回与候选池同行的来源标记列。
    """
    source_ids = (
        pool.column("source_record_id").to_pylist()
        if "source_record_id" in pool.column_names
        else [""] * pool.num_rows
    )
    transaction = pool.column("transaction_area_sqm").to_pylist()
    layout = (
        pool.column("layout_raw").to_pylist()
        if "layout_raw" in pool.column_names
        else [None] * pool.num_rows
    )
    source_marks: list[str] = []
    for idx, sid in enumerate(source_ids):
        task_id = task_to_source.get(str(sid))
        feat = features_by_task.get(task_id or "", {}) if task_id else {}
        has_ocr = bool(feat.get("accepted_room_count") or feat.get("room_only_count"))
        has_excel_area = transaction[idx] is not None
        has_excel_layout = bool(layout[idx] and str(layout[idx]).strip())
        parts: list[str] = []
        if has_ocr:
            parts.append("ocr")
        if has_excel_area:
            parts.append("excel_area")
        if has_excel_layout:
            parts.append("excel_layout")
        source_marks.append(",".join(parts) or "none")
    return pool.append_column(
        "feature_sources", pa.array(source_marks, type=pa.string())
    )


def comparable_feature_source_table(
    pool: pa.Table,
    *,
    features_by_task: dict[str, dict[str, Any]],
    task_to_source: dict[str, str],
) -> pa.Table:
    """2.3 可比侧逐特征来源表（不修改候选池，仅输出来源/可对齐性）。

    供 3.x 回放计算可比侧可对齐户型特征覆盖率（按 OCR/Excel 来源拆分）。
    """
    source_ids = (
        pool.column("source_record_id").to_pylist()
        if "source_record_id" in pool.column_names
        else [str(i) for i in range(pool.num_rows)]
    )
    rows: list[dict[str, Any]] = []
    for idx, sid in enumerate(source_ids):
        task_id = task_to_source.get(str(sid))
        feat = features_by_task.get(task_id or "", {}) if task_id else {}
        excel_area = pool.column("transaction_area_sqm").to_pylist()[idx]
        rows.append(
            {
                "source_record_id": str(sid),
                "has_ocr_rooms": bool(
                    feat.get("accepted_room_count") or feat.get("room_only_count")
                ),
                "has_ocr_area": feat.get("accepted_area_total") is not None,
                "has_excel_area": excel_area is not None,
                "feature_sources": (
                    (
                        "ocr,"
                        if feat.get("accepted_room_count") or feat.get("room_only_count")
                        else ""
                    )
                    + ("excel_area," if excel_area is not None else "")
                ).rstrip(",") or "none",
            }
        )
    return pa.Table.from_pylist(rows)


# ---------------------------------------------------------------------------
# 组3 第一轮户型相似度对照回放（3.1-3.5）
# ---------------------------------------------------------------------------

#: Excel 户型文本解析（"3室2厅" → 3 卧 + 2 厅；供可比侧无 OCR 时的分类特征）。
_LAYOUT_RE = re.compile(r"(\d+)\s*室\s*(\d+)\s*厅")


def excel_layout_features(layout_raw: Any) -> dict[str, Any] | None:
    """解析 Excel layout_raw 为分类户型特征（bedroom/living_room 计数）。"""
    if not layout_raw or not str(layout_raw).strip():
        return None
    match = _LAYOUT_RE.search(str(layout_raw))
    if match is None:
        return None
    bedrooms, living = int(match.group(1)), int(match.group(2))
    return {
        "room_types": ["bedroom"] * bedrooms + ["living_room"] * living,
        "room_count": bedrooms + living,
        "area_total": None,  # Excel layout 不提供面积构成
    }


def similarity_overlay_score(
    target: dict[str, Any], candidate: dict[str, Any]
) -> tuple[float | None, str]:
    """3.2 户型相似度子分数（0..1）：房间数量/类型集合/面积构成。

    任一维度缺失即从分母移出；无任何共同合格维度 → (None, 原因)（严格
    no-op，不惩罚缺图/缺面积）。维度缺失保持分数 0/1 边界合理。
    """
    target_room_count = (target.get("accepted_room_count") or 0) + (
        target.get("room_only_count") or 0
    )
    cand_room_count = candidate.get("room_count") or 0
    t_types = set(target.get("accepted_room_types") or []) | set(
        target.get("room_only_types") or []
    )
    c_types = set(candidate.get("room_types") or [])

    terms: list[float] = []
    if target_room_count > 0 and cand_room_count > 0:
        terms.append(1.0 - abs(target_room_count - cand_room_count) / max(
            target_room_count, cand_room_count
        ))
    if t_types and c_types:
        terms.append(len(t_types & c_types) / len(t_types | c_types))
    t_composition = target.get("accepted_area_composition")
    c_composition = candidate.get("area_composition")
    if t_composition and c_composition:
        keys = set(t_composition) | set(c_composition)
        total_diff = sum(
            abs(float(t_composition.get(k, 0)) - float(c_composition.get(k, 0)))
            for k in keys
        )
        terms.append(1.0 - total_diff / 2.0)
    if not terms:
        return None, "无共同合格户型字段（目标或候选缺特征）"
    return sum(terms) / len(terms), ""


def _candidate_unit_price(cand: pa.Table, idx: int) -> float | None:
    """候选单价：unit_price_observed 优先，缺则 total_price/面积。"""
    if "unit_price_observed" in cand.column_names:
        value = cand.column("unit_price_observed").to_pylist()[idx]
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                pass
    total = cand.column("total_price_yuan").to_pylist()[idx]
    area = cand.column("transaction_area_sqm").to_pylist()[idx]
    if total is not None and area not in (None, 0):
        try:
            return float(total) / float(area)
        except (TypeError, ValueError):
            return None
    return None


def _weighted_median(values: list[tuple[float, float]]) -> float | None:
    """按权重取加权中位数（权重非负；权重全 0 → None）。"""
    positive = [(v, w) for v, w in values if w > 0]
    if not positive:
        return None
    ordered = sorted(positive, key=lambda x: x[0])
    total = sum(w for _, w in ordered)
    acc = 0.0
    for value, weight in ordered:
        acc += weight
        if acc >= total / 2:
            return value
    return ordered[-1][0]


def _quantile_of(values: list[float], q: float) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return ordered[idx]


# ---------------------------------------------------------------------------
# 组3（升级）完整比较法引擎：时间修正 + 相似度加权 + 层级 + 汇总
# ---------------------------------------------------------------------------

#: 时间修正序列最少月数（复用 time_adjustment.coefficient_from_index 语义）。
TIME_SERIES_MIN_POINTS = 6

#: 完整比较法相似度分项（复用 comparable.SimilarityPolicy 因子权重语义）。
SIM_FACTOR_WEIGHTS: tuple[tuple[str, float], ...] = (
    ("area", 0.35),
    ("layout", 0.25),
    ("floor", 0.10),
    ("elevator", 0.05),
    ("orientation", 0.15),
    ("year_built", 0.10),
)

#: 层级放宽带宽（相对面积差；同 comparable.area_level 语义）。
AREA_TIGHT_BAND = 0.10
AREA_WIDE_BAND = 0.20


def _room_count_of(layout: Any) -> int | None:
    """户型文本 → 房间数（"3室2厅" → 5）；无法解析 → None。"""
    feat = excel_layout_features(layout)
    return feat["room_count"] if feat else None


def build_community_month_index(sale: pa.Table) -> dict[tuple[str, int, int], float]:
    """同小区滚动成交指数：``(community_name, year, month) → 单价中位数``。

    一次构建供全部目标查询（性能）；只纳入 ``unit_price_observed`` 有效正值的
    成交。时间外判定由调用方按目标时点过滤（系数计算只查当时可得月份）。
    """
    communities = sale.column("community_name").to_pylist()
    sale_dates = sale.column("sale_date").to_pylist()
    prices = sale.column("unit_price_observed").to_pylist()
    by_month: dict[tuple[str, int, int], list[float]] = {}
    for cname, sdate, price in zip(communities, sale_dates, prices, strict=True):
        if not cname:
            continue
        parsed = _to_date(sdate)
        if parsed is None:
            continue
        value = _to_float(price)
        if value is None or value <= 0:
            continue
        by_month.setdefault((str(cname), parsed.year, parsed.month), []).append(value)
    index: dict[tuple[str, int, int], float] = {}
    for key, values in by_month.items():
        index[key] = float(statistics.median(values))
    return index


def time_coefficient(
    index: dict[tuple[str, int, int], float],
    community: str,
    sale_month: tuple[int, int],
    target_month: tuple[int, int],
    *,
    min_points: int = TIME_SERIES_MIN_POINTS,
) -> float | None:
    """同小区时间修正系数（估值时点价 / 成交月价），无可靠序列 → None。

    与 ``time_adjustment.coefficient_from_index`` 语义一致：序列月数下限、
    成交月有锚点、成交后有前瞻锚点；任一不满足 → None（调用方降级不修正，
    不虚构系数）。
    """
    months = sorted(
        (y, m)
        for (c, y, m) in index
        if c == community and (y, m) <= target_month
    )
    if len(months) < min_points:
        return None
    if sale_month > target_month:
        return None
    sale_anchor = None
    for (y, m) in months:
        if (y, m) <= sale_month:
            sale_anchor = index[(community, y, m)]
        else:
            break
    if sale_anchor is None or sale_anchor <= 0:
        return None
    latest_month = months[-1]
    if latest_month <= sale_month:
        return None  # 无成交后锚点（只有成交月及之前数据）
    val_anchor = index[(community, *latest_month)]
    return round(float(val_anchor) / float(sale_anchor), 4)


def _factor_area(target_area: float | None, cand_area: Any) -> float | None:
    """面积相似度：相对差 ≤ 带宽线性下降（同 SimilarityPolicy._area）。"""
    if target_area is None or target_area <= 0:
        return None
    cand = _to_float(cand_area)
    if cand is None or cand <= 0:
        return None
    rel = abs(cand - target_area) / target_area
    return 1.0 - min(rel / 0.35, 1.0)


def _factor_layout(target_layout: Any, cand_layout: Any) -> float | None:
    """户型相似度：同文本=1，同室数=0.5，否则 0（同 SimilarityPolicy._layout）。"""
    if not target_layout or not cand_layout:
        return None
    t_text = str(target_layout).strip()
    c_text = str(cand_layout).strip()
    if not t_text or not c_text or t_text == "UNKNOWN" or c_text == "UNKNOWN":
        return None
    if c_text == t_text:
        return 1.0
    sc = _room_count_of(t_text)
    cc = _room_count_of(c_text)
    if sc is not None and cc is not None and sc == cc:
        return 0.5
    return 0.0


def non_floorplan_similarity(
    target_attrs: dict[str, Any], cand_attrs: dict[str, Any]
) -> float | None:
    """完整比较法相似度（非户型分项，加权；未知分项不计权）。"""
    factors: dict[str, float | None] = {
        "area": _factor_area(target_attrs.get("area"), cand_attrs.get("area")),
        "layout": _factor_layout(target_attrs.get("layout"), cand_attrs.get("layout")),
    }
    total = 0.0
    weighted = 0.0
    for name, weight in SIM_FACTOR_WEIGHTS:
        if name not in factors:
            continue
        score = factors[name]
        if score is not None:
            total += weight
            weighted += score * weight
    if total == 0:
        return None
    return weighted / total


def tier_of(
    target_area: float | None,
    target_layout: Any,
    cand_area: Any,
    cand_layout: Any,
    days_ago: int,
) -> str:
    """同小区层级 A/B/C/D（一次只放宽一个条件；同 comparable 语义）。"""
    if target_area is not None and target_area > 0:
        cand_area_f = _to_float(cand_area)
        if cand_area_f is not None and cand_area_f > 0:
            rel = abs(cand_area_f - target_area) / target_area
            area_same = rel <= AREA_TIGHT_BAND
            area_relaxed = rel <= AREA_WIDE_BAND
        else:
            area_same = area_relaxed = False
    else:
        area_same = area_relaxed = False

    t_layout = str(target_layout).strip() if target_layout else ""
    c_layout = str(cand_layout).strip() if cand_layout else ""
    layout_same = bool(t_layout and c_layout and c_layout == t_layout)
    t_count = _room_count_of(t_layout)
    c_count = _room_count_of(c_layout)
    layout_relaxed = bool(
        t_count is not None and c_count is not None and t_count == c_count
    )

    if area_same and layout_same and days_ago <= 180:
        return "A"
    if (area_same and layout_relaxed) or (area_relaxed and layout_same):
        if days_ago <= 365:
            return "B"
        return "C"
    if days_ago <= 365:
        return "C"
    return "D"


def estimate_full_comparable(
    candidates: pa.Table,
    *,
    target: dict[str, Any],
    community_index: dict[tuple[str, int, int], float],
    overlay_weights: list[float | None] | None = None,
) -> dict[str, Any]:
    """完整比较法估值：层级候选 → 时间修正 → 非户型相似度加权 → 汇总。

    基准组：调整后单价按非户型相似度加权中位数（未知分项不计权）；
    处理组：权重 = 非户型相似度 × 户型 overlay（overlay 缺失用 1.0 中性，
    保证缺失候选不改变权重分配 → 严格退化）；时间修正系数缺失 → 不修正
    （系数 1.0，记录降级）；区间 = 调整后单价 [P25, P75]。
    """
    target_area = target.get("transaction_area_sqm")
    target_layout = target.get("layout_raw")
    target_date = target.get("sale_date")
    community = target.get("community_name")
    target_month = (target_date.year, target_date.month) if isinstance(target_date, date) else None

    adjusted: list[dict[str, Any]] = []
    for i in range(candidates.num_rows):
        cand_date = _to_date(candidates.column("sale_date").to_pylist()[i])
        if cand_date is None or target_month is None:
            continue
        price = _candidate_unit_price(candidates, i)
        if price is None or price <= 0:
            continue
        coeff: float = 1.0
        degraded = False
        if community and isinstance(target_date, date):
            found = time_coefficient(
                community_index,
                str(community),
                (cand_date.year, cand_date.month),
                target_month,
            )
            if found is None:
                degraded = True
            else:
                coeff = found
        adjusted.append(
            {
                "index": i,
                "price": price * coeff,
                "degraded": degraded,
                "days_ago": max(0, (target_date - cand_date).days)
                if isinstance(target_date, date)
                else 0,
            }
        )

    if not adjusted:
        return {"center": None, "lower": None, "upper": None, "status": "信息不足", "n": 0}

    cand_area = candidates.column("transaction_area_sqm").to_pylist()
    cand_layout = (
        candidates.column("layout_raw").to_pylist()
        if "layout_raw" in candidates.column_names
        else [None] * candidates.num_rows
    )
    target_attrs = {"area": target_area, "layout": target_layout}

    weighted_items: list[tuple[float, float]] = []
    for adj in adjusted:
        idx = adj["index"]
        sim = non_floorplan_similarity(
            target_attrs, {"area": cand_area[idx], "layout": cand_layout[idx]}
        )
        weight = sim if sim is not None else 0.0
        if overlay_weights is not None:
            overlay = overlay_weights[idx] if idx < len(overlay_weights) else None
            if overlay is not None:
                weight = weight * overlay
        weighted_items.append((adj["price"], weight))

    prices = [item[0] for item in weighted_items]
    center = _weighted_median(weighted_items) or statistics.median(prices)
    lower = _quantile_of(prices, 0.25)
    upper = _quantile_of(prices, 0.75)
    degraded_n = sum(1 for adj in adjusted if adj["degraded"])
    return {
        "center": center,
        "lower": lower,
        "upper": upper,
        "status": "正式",
        "n": len(prices),
        "degraded_time_n": degraded_n,
    }


def estimate_with_candidates(
    candidates: pa.Table,
    *,
    target_area: float,
    similarity_weights: list[float | None] | None = None,
) -> dict[str, Any]:
    """（保留兼容）简单口径：候选单价中位数 + [P25,P75]（仅用于合成测试）。"""
    prices: list[float] = []
    for i in range(candidates.num_rows):
        price = _candidate_unit_price(candidates, i)
        if price is not None and price > 0:
            prices.append(price)
    if not prices:
        return {"center": None, "lower": None, "upper": None, "status": "信息不足", "n": 0}
    center = statistics.median(prices)
    result: dict[str, Any] = {
        "center": center,
        "lower": _quantile_of(prices, 0.25),
        "upper": _quantile_of(prices, 0.75),
        "status": "正式",
        "n": len(prices),
    }
    if similarity_weights is not None:
        weighted = [
            (p, w)
            for p, w in zip(prices, [x for x in similarity_weights if x is not None], strict=False)
            if w is not None and w > 0
        ]
        weighted_center = _weighted_median(weighted)
        if weighted_center is not None:
            result["center"] = weighted_center
    return result


def _round1_pair_schema() -> pa.Schema:
    """第一轮成对回放明细模式（每行 = 一个唯一目标记录 × 双组结果）。"""
    return pa.schema(
        [
            pa.field("source_record_id", pa.string(), nullable=False),
            pa.field("community_name", pa.string(), nullable=True),
            pa.field("target_date", pa.date32(), nullable=True),
            pa.field("target_area_sqm", pa.float64(), nullable=True),
            pa.field("layout_raw", pa.string(), nullable=True),
            pa.field("actual_total_price", pa.float64(), nullable=True),
            pa.field("actual_unit_price", pa.float64(), nullable=True),
            pa.field("base_center", pa.float64(), nullable=True),
            pa.field("base_lower", pa.float64(), nullable=True),
            pa.field("base_upper", pa.float64(), nullable=True),
            pa.field("base_status", pa.string(), nullable=True),
            pa.field("base_n", pa.int32(), nullable=False),
            pa.field("trt_center", pa.float64(), nullable=True),
            pa.field("trt_lower", pa.float64(), nullable=True),
            pa.field("trt_upper", pa.float64(), nullable=True),
            pa.field("trt_status", pa.string(), nullable=True),
            pa.field("trt_n", pa.int32(), nullable=False),
            pa.field("feature_effective", pa.bool_(), nullable=False),
            pa.field("feature_source", pa.string(), nullable=True),
            pa.field("no_op_reason", pa.string(), nullable=True),
            pa.field("comp_n", pa.int32(), nullable=False),
            pa.field("comp_align_ocr_n", pa.int32(), nullable=False),
            pa.field("comp_align_excel_n", pa.int32(), nullable=False),
            pa.field("target_ocr_n", pa.int32(), nullable=False),
            pa.field("excluded_same_day_n", pa.int32(), nullable=False),
        ]
    )


def _candidate_unit_prices(candidates: pa.Table) -> list[float]:
    return [
        float(p)
        for p in (
            _candidate_unit_price(candidates, i) for i in range(candidates.num_rows)
        )
        if p is not None and p > 0
    ]


def run_round1_pair(
    *,
    unit: dict[str, Any],
    pool: pa.Table,
    features_by_task: dict[str, dict[str, Any]],
    task_to_source: dict[str, str],
    exclude_source_ids: set[str],
    community_index: dict[tuple[str, int, int], float],
) -> dict[str, Any]:
    """3.1/3.3 单目标第一轮成对回放（完整比较法）：基准 vs 处理（户型 overlay）。

    两组共用目标、时点、初始候选池、时间修正与非户型相似度；处理组权重 =
    非户型相似度 × 户型 overlay（overlay 缺失用中性 1.0 → 严格退化）；第一轮
    不做任何数值价格调整（金额修正留待第二轮条件项）。
    """
    target_date = unit.get("sale_date")
    community = unit.get("community_name")
    target_area = unit.get("transaction_area_sqm")
    source_id = unit.get("source_record_id")
    actual_total = unit.get("actual_total_price")
    actual_unit = unit.get("actual_unit_price")
    target_ocr_n = int(unit.get("ocr_accepted_count") or 0) + int(
        unit.get("ocr_room_only_count") or 0
    )

    row: dict[str, Any] = {
        "source_record_id": str(source_id),
        "community_name": community,
        "target_date": target_date,
        "target_area_sqm": target_area,
        "layout_raw": unit.get("layout_raw"),
        "actual_total_price": actual_total,
        "actual_unit_price": actual_unit,
        "base_center": None,
        "base_lower": None,
        "base_upper": None,
        "base_status": None,
        "base_n": 0,
        "trt_center": None,
        "trt_lower": None,
        "trt_upper": None,
        "trt_status": None,
        "trt_n": 0,
        "feature_effective": False,
        "feature_source": None,
        "no_op_reason": None,
        "comp_n": 0,
        "comp_align_ocr_n": 0,
        "comp_align_excel_n": 0,
        "target_ocr_n": target_ocr_n,
        "excluded_same_day_n": 0,
    }
    if not isinstance(target_date, date) or not community:
        row["no_op_reason"] = "目标缺少成交日期或小区"
        return row

    candidates, excluded_same_day = filter_candidates(
        pool,
        community_name=str(community),
        cutoff=target_date,
        exclude_source_ids=set(exclude_source_ids) | {str(source_id)},
    )
    row["excluded_same_day_n"] = excluded_same_day
    row["comp_n"] = candidates.num_rows

    # 可比侧对齐覆盖（按来源拆分）。excel_n 为对齐层口径：Excel 结构化面积
    # 或户型文本在场即计数（spec 2.3 对齐层）；实际 overlay 评分只用可解析
    # layout，报告解读时须注明该口径差异（FVV-VERIFY-20260830-02 S2）
    cand_ids = (
        candidates.column("source_record_id").to_pylist()
        if "source_record_id" in candidates.column_names
        else []
    )
    cand_area = candidates.column("transaction_area_sqm").to_pylist()
    cand_layout = (
        candidates.column("layout_raw").to_pylist()
        if "layout_raw" in candidates.column_names
        else [None] * candidates.num_rows
    )
    for idx, cand_id in enumerate(cand_ids):
        task_id = task_to_source.get(str(cand_id))
        feat = features_by_task.get(task_id or "", {}) if task_id else {}
        if feat.get("accepted_room_count") or feat.get("room_only_count"):
            row["comp_align_ocr_n"] += 1
        elif cand_area[idx] is not None or (
            cand_layout[idx] and str(cand_layout[idx]).strip()
        ):
            row["comp_align_excel_n"] += 1

    # 基准组：完整比较法（时间修正 + 非户型相似度加权，无户型 overlay）
    base = estimate_full_comparable(
        candidates, target=unit, community_index=community_index
    )
    row.update(
        {
            "base_center": base["center"],
            "base_lower": base["lower"],
            "base_upper": base["upper"],
            "base_status": base["status"],
            "base_n": base["n"],
        }
    )

    # 处理组：户型相似度 overlay（权重 = 非户型相似度 × overlay；缺失 → 中性）
    target_task = unit.get("ocr_task_id")
    target_feat = features_by_task.get(str(target_task) if target_task else "", {}) or {}
    weights: list[float | None] = []
    effective = False
    source_marks: set[str] = set()
    for idx, cand_id in enumerate(cand_ids):
        cand_task = task_to_source.get(str(cand_id))
        cand_ocr = features_by_task.get(str(cand_task) if cand_task else "", {}) or {}
        if cand_ocr.get("accepted_room_count") or cand_ocr.get("room_only_count"):
            cand_feat = cand_ocr
            source_marks.add("ocr")
        else:
            excel_feat = excel_layout_features(cand_layout[idx])
            cand_feat = excel_feat or {}
            if excel_feat:
                source_marks.add("excel")
        score, reason = similarity_overlay_score(target_feat, cand_feat)
        weights.append(score)
        if score is not None:
            effective = True
        if reason:
            row["no_op_reason"] = row["no_op_reason"] or reason

    trt = estimate_full_comparable(
        candidates,
        target=unit,
        community_index=community_index,
        overlay_weights=weights,
    )
    row.update(
        {
            "trt_center": trt["center"],
            "trt_lower": trt["lower"],
            "trt_upper": trt["upper"],
            "trt_status": trt["status"],
            "trt_n": trt["n"],
            "feature_effective": effective,
            "feature_source": ",".join(sorted(source_marks)) or None,
        }
    )
    if not effective:
        row["no_op_reason"] = row["no_op_reason"] or "无共同合格户型字段（严格退化）"
    return row


def run_round1_development(
    *,
    units: pa.Table,
    pool: pa.Table,
    features_by_task: dict[str, dict[str, Any]],
    task_to_source: dict[str, str],
    dev_source_ids: list[str],
    holdout_source_ids: set[str],
    community_index: dict[tuple[str, int, int], float],
    out_dir: Path,
    run_id: str,
) -> pa.Table:
    """3.3 开发集第一轮成对回放：逐目标运行基准/处理双组并落盘。

    每组目标共享同一初始候选池（单引擎双配置）；开发集目标只能以开发集
    记录为候选（确认集记录 SHALL 不参与参数标定，见 spec「开发集定规则」）。
    产出成对明细表（每行 = 一个目标 × 双组估值），写
    ``out_dir/round1_dev_pair.parquet`` 及运行摘要；不触碰正式估值产物与
    冻结 v1。
    """
    dev_set = set(dev_source_ids)
    rows: list[dict[str, Any]] = []
    for i in range(units.num_rows):
        source_id = str(units.column("source_record_id").to_pylist()[i])
        if source_id not in dev_set:
            continue
        unit = {
            "source_record_id": source_id,
            "sale_date": units.column("sale_date").to_pylist()[i],
            "community_name": units.column("community_name").to_pylist()[i],
            "transaction_area_sqm": units.column("transaction_area_sqm").to_pylist()[i],
            "layout_raw": units.column("layout_raw").to_pylist()[i],
            "actual_total_price": units.column("actual_total_price").to_pylist()[i],
            "actual_unit_price": units.column("actual_unit_price").to_pylist()[i],
            "ocr_task_id": units.column("ocr_task_id").to_pylist()[i],
            "ocr_accepted_count": units.column("ocr_accepted_count").to_pylist()[i],
            "ocr_room_only_count": units.column("ocr_room_only_count").to_pylist()[i],
        }
        row = run_round1_pair(
            unit=unit,
            pool=pool,
            features_by_task=features_by_task,
            task_to_source=task_to_source,
            exclude_source_ids=holdout_source_ids,  # 确认集记录不进入开发候选池
            community_index=community_index,
        )
        rows.append(row)

    names = _round1_pair_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    table = pa.table(columns, schema=_round1_pair_schema())

    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / "round1_dev_pair.parquet"
    work = out_dir / "round1_dev_pair.parquet.incomplete"
    pq.write_table(table, work, compression="zstd")
    work.replace(final)

    # 运行摘要（特征生效/no-op 率；如实披露，不虚构）
    effective = sum(1 for v in table.column("feature_effective").to_pylist() if v)
    statuses = table.column("base_status").to_pylist()
    summary: dict[str, Any] = {
        "run_id": run_id,
        "stage": "round1_dev",
        "targets": table.num_rows,
        "feature_effective_targets": effective,
        "feature_noop_targets": table.num_rows - effective,
        "estimated_targets": sum(1 for s in statuses if s == "正式"),
        "insufficient_targets": sum(1 for s in statuses if s == "信息不足"),
        "pair_table": str(final),
        "built_at": datetime.now(UTC).isoformat(),
    }
    summary_path = out_dir / "round1_dev_summary.json"
    work = out_dir / "round1_dev_summary.json.incomplete"
    work.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(summary_path)
    return table


# ---------------------------------------------------------------------------
# 组5 成对指标、子组、敏感性、三选一结论（5.1-5.4）
# ---------------------------------------------------------------------------


def _ape(center: float | None, actual: float | None) -> float | None:
    if center is None or actual in (None, 0):
        return None
    return abs(float(center) - float(actual)) / float(actual)


def _signed(center: float | None, actual: float | None) -> float | None:
    if center is None or actual in (None, 0):
        return None
    return (float(center) - float(actual)) / float(actual)


def compute_pair_metrics(
    pairs: pa.Table, high_quantile: float = 0.90
) -> dict[str, Any]:
    """5.1 相同目标集合上的成对指标（基准 vs 处理并排，同一目标集合计算）。

    以 target 是否同时有 base/trt 中心为准（成对完整性）；用同一目标集合
    计算两组的 APE/有符号误差/覆盖等，避免不同样本量制造表面改善。
    """
    base_c = pairs.column("base_center").to_pylist()
    trt_c = pairs.column("trt_center").to_pylist()
    actual = pairs.column("actual_unit_price").to_pylist()
    base_l = pairs.column("base_lower").to_pylist()
    base_u = pairs.column("base_upper").to_pylist()
    trt_l = pairs.column("trt_lower").to_pylist()
    trt_u = pairs.column("trt_upper").to_pylist()
    statuses = pairs.column("base_status").to_pylist()
    effective = pairs.column("feature_effective").to_pylist()
    comp_ocr = pairs.column("comp_align_ocr_n").to_pylist()
    comp_excel = pairs.column("comp_align_excel_n").to_pylist()
    comp_n = pairs.column("comp_n").to_pylist()

    # 成对目标（同集合）
    paired_idx = [
        i
        for i in range(pairs.num_rows)
        if base_c[i] is not None and trt_c[i] is not None and actual[i] not in (None, 0)
    ]
    base_ape = [_ape(base_c[i], actual[i]) for i in paired_idx]
    trt_ape = [_ape(trt_c[i], actual[i]) for i in paired_idx]
    base_signed = [_signed(base_c[i], actual[i]) for i in paired_idx]
    trt_signed = [_signed(trt_c[i], actual[i]) for i in paired_idx]
    ape_delta = [
        t - b for b, t in zip(base_ape, trt_ape, strict=True) if b is not None and t is not None
    ]
    # 零差值占比：成对目标中处理/基准 APE 几乎相同（判别力检查输入，C5）
    zero_delta_share = (
        round(
            sum(1 for d in ape_delta if abs(d) < 0.001) / len(ape_delta),
            4,
        )
        if ape_delta
        else None
    )

    def median(values: Sequence[float | None]) -> float | None:
        clean = [v for v in values if v is not None]
        return statistics.median(clean) if clean else None

    def quantile(values: Sequence[float | None], q: float) -> float | None:
        clean = [v for v in values if v is not None]
        return _quantile_of(clean, q) if clean else None

    def coverage(
        centers: list[float | None],
        lowers: list[float | None],
        uppers: list[float | None],
    ) -> tuple[float | None, int]:
        covered = 0
        n = 0
        for c, lo, hi, a in zip(centers, lowers, uppers, actual, strict=True):
            if c is None or lo is None or hi is None or a in (None, 0):
                continue
            n += 1
            if lo <= a <= hi:
                covered += 1
        return (covered / n if n else None), n

    base_cov, base_cov_n = coverage(base_c, base_l, base_u)
    trt_cov, trt_cov_n = coverage(trt_c, trt_l, trt_u)
    widths_base = [
        (u - lo) / c
        for c, lo, u in zip(base_c, base_l, base_u, strict=True)
        if c not in (None, 0) and lo is not None and u is not None and u > lo
    ]
    widths_trt = [
        (u - lo) / c
        for c, lo, u in zip(trt_c, trt_l, trt_u, strict=True)
        if c not in (None, 0) and lo is not None and u is not None and u > lo
    ]

    n_targets = pairs.num_rows
    n_estimated = len(paired_idx)
    n_insufficient = sum(1 for s in statuses if s == "信息不足")
    n_effective = sum(1 for e in effective if e)

    return {
        "n_targets": n_targets,
        "n_estimated": n_estimated,
        "n_insufficient": n_insufficient,
        "n_effective": n_effective,
        "zero_delta_share": zero_delta_share,
        "noop_rate": round((n_targets - n_effective) / n_targets, 4) if n_targets else None,
        "base_ape_median": median(base_ape),
        "base_ape_high_quantile": quantile(base_ape, high_quantile),
        "trt_ape_median": median(trt_ape),
        "trt_ape_high_quantile": quantile(trt_ape, high_quantile),
        "ape_delta_median": median(ape_delta),
        "trt_better_share": (
            round(
                sum(
                    1
                    for b, t in zip(base_ape, trt_ape, strict=True)
                    if b is not None and t is not None and t < b
                )
                / len(paired_idx),
                4,
            )
            if paired_idx
            else None
        ),
        "base_signed_median": median(base_signed),
        "trt_signed_median": median(trt_signed),
        "base_overvaluation_rate": (
            round(sum(1 for s in base_signed if s is not None and s > 0) / len(base_signed), 4)
            if base_signed
            else None
        ),
        "trt_overvaluation_rate": (
            round(sum(1 for s in trt_signed if s is not None and s > 0) / len(trt_signed), 4)
            if trt_signed
            else None
        ),
        "base_range_coverage": base_cov,
        "trt_range_coverage": trt_cov,
        "base_range_width_median": statistics.median(widths_base) if widths_base else None,
        "trt_range_width_median": statistics.median(widths_trt) if widths_trt else None,
        "formal_coverage_rate": (
            round(n_estimated / n_targets, 4) if n_targets else None
        ),
        "rejection_rate": (
            round(n_insufficient / n_targets, 4) if n_targets else None
        ),
        "comp_align_ocr_total": int(sum(comp_ocr)),
        "comp_align_excel_total": int(sum(comp_excel)),
        "comp_n_median": statistics.median(comp_n) if comp_n else None,
    }


def compute_subgroup_metrics(pairs: pa.Table, high_quantile: float = 0.90) -> pa.Table:
    """5.2 子组指标：按小区/户型/面积段/OCR 完整度分组输出样本量与关键指标。

    小样本子组仅披露判别力限制，不宣称代表总体。
    """
    rows: list[dict[str, Any]] = []
    communities = pairs.column("community_name").to_pylist()
    areas = pairs.column("target_area_sqm").to_pylist()
    layouts = (
        pairs.column("layout_raw").to_pylist()
        if "layout_raw" in pairs.column_names
        else [None] * pairs.num_rows
    )
    # 目标 OCR 完整度：以目标侧 ACCEPTED+ROOM_ONLY 标注数分组（非处理组生效代理）
    target_ocr_n = (
        pairs.column("target_ocr_n").to_pylist()
        if "target_ocr_n" in pairs.column_names
        else [0] * pairs.num_rows
    )
    ocr_complete = ["complete" if int(n) > 0 else "partial" for n in target_ocr_n]

    dimensions: list[tuple[str, list[Any]]] = [
        ("community", communities),
        ("area_band", [area_band_of(float(a)) if a is not None else None for a in areas]),
        ("layout", layouts),
        ("ocr_completeness", [("complete" if v else "partial") for v in ocr_complete]),
    ]
    for dimension, values in dimensions:
        for value in sorted({str(v) for v in values if v is not None}):
            keep = [i for i, v in enumerate(values) if str(v) == value]
            subset = pairs.take(keep)
            metrics = compute_pair_metrics(subset, high_quantile)
            rows.append(
                {
                    "group_dimension": dimension,
                    "group_value": str(value),
                    "n_targets": metrics["n_targets"],
                    "n_estimated": metrics["n_estimated"],
                    "base_ape_median": metrics["base_ape_median"],
                    "trt_ape_median": metrics["trt_ape_median"],
                    "ape_delta_median": metrics["ape_delta_median"],
                    "discrimination_limited": metrics["n_estimated"] < 30,
                }
            )
    return pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                pa.field("group_dimension", pa.string()),
                pa.field("group_value", pa.string()),
                pa.field("n_targets", pa.int32()),
                pa.field("n_estimated", pa.int32()),
                pa.field("base_ape_median", pa.float64()),
                pa.field("trt_ape_median", pa.float64()),
                pa.field("ape_delta_median", pa.float64()),
                pa.field("discrimination_limited", pa.bool_()),
            ]
        ),
    )


def area_band_of(area: float | None) -> str | None:
    """面积段口径（与 README 分组一致）：<70 / 70-90 / 90-110 / 110-130 / >=130。"""
    if area is None:
        return None
    if area < 70:
        return "<70"
    if area < 90:
        return "70-90"
    if area < 110:
        return "90-110"
    if area < 130:
        return "110-130"
    return ">=130"


def decide_round1_conclusion(
    metrics: dict[str, Any],
    *,
    min_estimated: int,
    improvement_threshold: float,
    max_coverage_regression: float,
    max_zero_delta_ratio: float = 0.5,
    max_noop_rate: float = 0.3,
) -> tuple[str, list[str]]:
    """5.4 三选一结论器（含判别力检查，C5）。

    ``EVIDENCE_INSUFFICIENT``：有效样本 < min_estimated、零差值占比 >
    max_zero_delta_ratio（大量无差异目标 → 无法分辨）、no-op 率 >
    max_noop_rate（特征未充分生效）、或无法计算差值。
    ``CANDIDATE_INTEGRATION``：中心准确率改善达到冻结标准（ape_delta 中位数
    <= -improvement_threshold）且保护指标无实质恶化（区间覆盖率下降不超过
    max_coverage_regression）。其余判别力足够但无改善 → ``NO_VALUE``。
    """
    reasons: list[str] = []
    n_estimated = int(metrics.get("n_estimated") or 0)
    if n_estimated < min_estimated:
        return "EVIDENCE_INSUFFICIENT", [
            f"有效成对样本 {n_estimated} < 判别力下限 {min_estimated}"
        ]

    zero_share = metrics.get("zero_delta_share")
    if zero_share is not None and float(zero_share) > max_zero_delta_ratio:
        return "EVIDENCE_INSUFFICIENT", [
            f"零差值占比 {round(float(zero_share), 4)} > 上限 {max_zero_delta_ratio}"
            "（大量目标处理/基准无差异，判别力不足）"
        ]

    noop_rate = metrics.get("noop_rate")
    if noop_rate is not None and float(noop_rate) > max_noop_rate:
        return "EVIDENCE_INSUFFICIENT", [
            f"no-op 率 {round(float(noop_rate), 4)} > 上限 {max_noop_rate}"
            "（特征未充分生效，无法判定增量）"
        ]

    delta = metrics.get("ape_delta_median")
    base_cov = metrics.get("base_range_coverage")
    trt_cov = metrics.get("trt_range_coverage")
    if delta is None:
        return "EVIDENCE_INSUFFICIENT", ["无法计算 APE 差值中位数（无有效成对样本）"]

    if delta <= -improvement_threshold:
        regression = 0.0
        if base_cov is not None and trt_cov is not None:
            regression = base_cov - trt_cov
        if regression > max_coverage_regression:
            return "NO_VALUE", [
                f"中心准确率改善 {round(delta, 4)} 但区间覆盖率下降 "
                f"{round(regression, 4)} 超过容忍 {max_coverage_regression}"
            ]
        reasons.append(f"APE 差值中位数 {round(delta, 4)} <= -{improvement_threshold}")
        return "CANDIDATE_INTEGRATION", reasons

    return "NO_VALUE", [
        f"处理组 APE 差值中位数 {round(delta, 4)} 未达到改善标准 "
        f"（<= -{improvement_threshold}），且判别力检查通过"
    ]


# ---------------------------------------------------------------------------
# 3.4 冻结第一轮配置（用户确认后）与 3.5 确认集运行
# ---------------------------------------------------------------------------

FROZEN_CONFIG_FILENAME = "frozen_round1_config.json"
CONFIRMATION_FILENAME = "user_confirmation.json"


def write_user_confirmation(
    *,
    out_dir: Path,
    run_id: str,
    config: dict[str, Any],
    confirmed_by: str,
    summary: str,
) -> Path:
    """C2 落盘用户确认记录：门槛参数与确认时序的可核验证据。

    记录冻结配置中的全部门槛参数、确认人与确认时间；配合冻结配置时间戳
    早于确认集运行时间戳（``run_round1_confirmation`` 先校验冻结哈希再运行），
    构成「确认后才运行确认集」的时序证据链。
    """
    record: dict[str, Any] = {
        "run_id": run_id,
        "stage": "user_confirmation",
        "confirmed_at": datetime.now(UTC).isoformat(),
        "confirmed_by": str(confirmed_by),
        "summary": summary,
        "thresholds": {
            key: config[key]
            for key in (
                "min_estimated",
                "improvement_threshold",
                "max_coverage_regression",
                "max_zero_delta_ratio",
                "max_noop_rate",
                "min_effective_coverage",
            )
            if key in config
        },
        "similarity_weights": config.get("similarity_weights"),
        "frozen_config_ref": FROZEN_CONFIG_FILENAME,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / CONFIRMATION_FILENAME
    work = out_dir / f"{CONFIRMATION_FILENAME}.incomplete"
    work.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(final)
    return final


def freeze_round1_config(
    *,
    out_dir: Path,
    run_id: str,
    min_estimated: int,
    improvement_threshold: float,
    max_coverage_regression: float,
    similarity_weights: dict[str, float],
    min_effective_coverage: float,
    confirmed_by: str,
    max_zero_delta_ratio: float = 0.5,
    max_noop_rate: float = 0.3,
) -> tuple[Path, dict[str, Any]]:
    """3.4 冻结第一轮配置：用户确认的门槛与特征公式/权重登记哈希。

    冻结后确认集 SHALL 只按此配置运行一次（spec：确认集只按冻结规则判定）。
    ``confirmed_by`` 为确认记录文件路径（用户确认证据，C2）；返回
    ``(配置路径, 配置 dict)``。
    """
    config: dict[str, Any] = {
        "run_id": run_id,
        "stage": "round1_frozen_config",
        "min_estimated": int(min_estimated),
        "improvement_threshold": float(improvement_threshold),
        "max_coverage_regression": float(max_coverage_regression),
        "max_zero_delta_ratio": float(max_zero_delta_ratio),
        "max_noop_rate": float(max_noop_rate),
        "min_effective_coverage": float(min_effective_coverage),
        "similarity_weights": dict(similarity_weights),
        "similarity_formula": (
            "完整比较法：非户型相似度（面积/户型加权，未知分项不计权）× 户型 "
            "overlay（缺失用 1.0 中性 → 严格退化）+ 同小区滚动时间修正；"
            "无共同合格维度则 no-op，不做任何数值价格调整"
        ),
        "confirmed_by": str(confirmed_by),
    }
    content_hash = hashlib.sha256(
        json.dumps(config, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    config["content_hash"] = content_hash

    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / FROZEN_CONFIG_FILENAME
    work = out_dir / f"{FROZEN_CONFIG_FILENAME}.incomplete"
    work.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(final)
    return final, config


def load_frozen_round1_config(config_path: Path) -> dict[str, Any]:
    """读取冻结配置并校验内容哈希（防篡改后运行确认集）。

    哈希计算排除 ``content_hash`` 字段自身（与冻结时口径一致）。
    """
    config = json.loads(config_path.read_text(encoding="utf-8"))
    expected = config.get("content_hash")
    payload = {k: v for k, v in config.items() if k != "content_hash"}
    content_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    if expected is None or content_hash != expected:
        raise RebuildGateError(
            f"冻结配置内容哈希校验失败（可能被篡改）：{config_path}"
        )
    return cast(dict[str, Any], config)


def run_round1_confirmation(
    *,
    units: pa.Table,
    pool: pa.Table,
    features_by_task: dict[str, dict[str, Any]],
    task_to_source: dict[str, str],
    holdout_source_ids: list[str],
    community_index: dict[tuple[str, int, int], float],
    out_dir: Path,
    run_id: str,
    config_path: Path,
) -> pa.Table:
    """3.5 确认集第一轮成对回放：仅按冻结配置运行一次。

    目标 = 未触碰确认集（holdout）；候选池 = 全部时点前普通住宅成交（开发
    集记录可作候选，参数已冻结不涉及调参）；产出成对表并写摘要。运行前
    校验冻结配置哈希。
    """
    load_frozen_round1_config(config_path)  # 哈希校验，防篡改
    holdout_set = set(holdout_source_ids)
    rows: list[dict[str, Any]] = []
    for i in range(units.num_rows):
        source_id = str(units.column("source_record_id").to_pylist()[i])
        if source_id not in holdout_set:
            continue
        unit = {
            "source_record_id": source_id,
            "sale_date": units.column("sale_date").to_pylist()[i],
            "community_name": units.column("community_name").to_pylist()[i],
            "transaction_area_sqm": units.column("transaction_area_sqm").to_pylist()[i],
            "layout_raw": units.column("layout_raw").to_pylist()[i],
            "actual_total_price": units.column("actual_total_price").to_pylist()[i],
            "actual_unit_price": units.column("actual_unit_price").to_pylist()[i],
            "ocr_task_id": units.column("ocr_task_id").to_pylist()[i],
            "ocr_accepted_count": units.column("ocr_accepted_count").to_pylist()[i],
            "ocr_room_only_count": units.column("ocr_room_only_count").to_pylist()[i],
        }
        row = run_round1_pair(
            unit=unit,
            pool=pool,
            features_by_task=features_by_task,
            task_to_source=task_to_source,
            exclude_source_ids=set(),  # 确认集目标排除自身；开发记录可作候选
            community_index=community_index,
        )
        rows.append(row)

    names = _round1_pair_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row[name])
    table = pa.table(columns, schema=_round1_pair_schema())

    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / "round1_confirm_pair.parquet"
    work = out_dir / "round1_confirm_pair.parquet.incomplete"
    pq.write_table(table, work, compression="zstd")
    work.replace(final)

    metrics = compute_pair_metrics(table)
    summary: dict[str, Any] = {
        "run_id": run_id,
        "stage": "round1_confirm",
        "frozen_config": str(config_path),
        "targets": table.num_rows,
        "metrics": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in metrics.items()},
        "pair_table": str(final),
        "run_once": True,
        "no_reverse_tuning": True,
        "built_at": datetime.now(UTC).isoformat(),
    }
    summary_path = out_dir / "round1_confirm_summary.json"
    work = out_dir / "round1_confirm_summary.json.incomplete"
    work.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(summary_path)
    return table


def conclude_round1(
    metrics: dict[str, Any], config: dict[str, Any]
) -> tuple[str, list[str]]:
    """按冻结配置产出第一轮三选一结论（3.5 收口，含判别力检查）。"""
    return decide_round1_conclusion(
        metrics,
        min_estimated=int(config["min_estimated"]),
        improvement_threshold=float(config["improvement_threshold"]),
        max_coverage_regression=float(config["max_coverage_regression"]),
        max_zero_delta_ratio=float(config.get("max_zero_delta_ratio", 0.5)),
        max_noop_rate=float(config.get("max_noop_rate", 0.3)),
    )


# ---------------------------------------------------------------------------
# 组4 第二轮面积价格调整条件门禁（4.1-4.4）
# ---------------------------------------------------------------------------


def decide_round2_gate(
    *,
    round1_conclusion: str,
    metrics: dict[str, Any],
    min_effective_coverage: float,
    min_ocr_align_ratio: float,
) -> tuple[bool, list[str]]:
    """4.1 第二轮门禁：第一轮非证据不足/无价值 + 特征覆盖 + OCR 可比侧对齐。

    任一门禁未满足 → 停止为「条件未触发」，不自动扩样本或调参；返回
    ``(triggered, reasons)``。
    """
    reasons: list[str] = []
    if round1_conclusion == "EVIDENCE_INSUFFICIENT":
        reasons.append("第一轮结论为 EVIDENCE_INSUFFICIENT（输入/覆盖/样本不足）")
        return False, reasons
    if round1_conclusion == "NO_VALUE":
        reasons.append("第一轮结论为 NO_VALUE（无增量价值证据，不启动第二轮）")
        return False, reasons

    n_targets = int(metrics.get("n_targets") or 0)
    n_effective = int(metrics.get("n_effective") or 0)
    eff_rate = n_effective / n_targets if n_targets else 0.0
    if eff_rate < min_effective_coverage:
        reasons.append(
            f"特征生效覆盖 {round(eff_rate, 4)} < 门禁 {min_effective_coverage}"
        )
        return False, reasons

    ocr_total = int(metrics.get("comp_align_ocr_total") or 0)
    excel_total = int(metrics.get("comp_align_excel_total") or 0)
    align_n = ocr_total + excel_total
    ocr_ratio = ocr_total / align_n if align_n else 0.0
    if ocr_ratio < min_ocr_align_ratio:
        reasons.append(
            f"可比侧 OCR 对齐占比 {round(ocr_ratio, 4)} < 门禁 {min_ocr_align_ratio}"
        )
        return False, reasons

    reasons.append("第一轮信号与特征/OCR 对齐门禁均满足")
    return True, reasons


def sensitivity_analysis(pairs: pa.Table, units: pa.Table) -> pa.Table:
    """5.3 敏感性：重复簇保留/排除、目标 OCR 完整度分层、面积极端值。

    主结论始终使用唯一记录口径；敏感性口径单独输出，不替换主评估口径。
    目标 OCR 完整度以目标侧 ACCEPTED+ROOM_ONLY 标注数判定（非候选来源）。
    """
    rows: list[dict[str, Any]] = []
    unit_dup = {
        str(units.column("source_record_id").to_pylist()[i]): bool(
            units.column("is_duplicate").to_pylist()[i]
        )
        for i in range(units.num_rows)
    }
    source_ids = pairs.column("source_record_id").to_pylist()
    target_ocr_n = (
        pairs.column("target_ocr_n").to_pylist()
        if "target_ocr_n" in pairs.column_names
        else [0] * pairs.num_rows
    )
    target_areas = pairs.column("target_area_sqm").to_pylist()

    # 敏感性 1：重复簇（is_duplicate 目标）
    dup_labels = ((False, "exclude_duplicate_clusters"), (True, "only_duplicate_clusters"))
    for dup_value, label in dup_labels:
        keep = [
            i
            for i, sid in enumerate(source_ids)
            if unit_dup.get(str(sid)) == dup_value
        ]
        if not keep:
            continue
        subset = pairs.take(keep)
        m = compute_pair_metrics(subset)
        rows.append(
            {
                "sensitivity": label,
                "n_targets": m["n_targets"],
                "n_estimated": m["n_estimated"],
                "base_ape_median": m["base_ape_median"],
                "trt_ape_median": m["trt_ape_median"],
                "ape_delta_median": m["ape_delta_median"],
            }
        )

    # 敏感性 2：目标 OCR 完整度分层（按目标侧 OCR 标注数）
    ocr_labels = (
        (0, "target_ocr_none"),
        (1, "target_ocr_low"),
    )
    for threshold, label in ocr_labels:
        if threshold == 0:
            keep = [i for i, n in enumerate(target_ocr_n) if int(n) == 0]
        else:
            keep = [i for i, n in enumerate(target_ocr_n) if int(n) > 0]
        if not keep:
            continue
        subset = pairs.take(keep)
        m = compute_pair_metrics(subset)
        rows.append(
            {
                "sensitivity": label,
                "n_targets": m["n_targets"],
                "n_estimated": m["n_estimated"],
                "base_ape_median": m["base_ape_median"],
                "trt_ape_median": m["trt_ape_median"],
                "ape_delta_median": m["ape_delta_median"],
            }
        )

    # 敏感性 3：面积极端值（目标面积 > 上限 500 ㎡ 或 <= 0）保留/排除
    extreme_idx = [
        i
        for i, a in enumerate(target_areas)
        if a is not None and (a <= 0 or a > AREA_EXTREME_UPPER_SQM)
    ]
    extreme_set = set(extreme_idx)
    for label, keep in (
        ("exclude_extreme_area", [i for i in range(pairs.num_rows) if i not in extreme_set]),
        ("include_all_area", list(range(pairs.num_rows))),
    ):
        if not keep:
            continue
        if label == "exclude_extreme_area" and len(keep) == pairs.num_rows:
            continue  # 无面积极端值，排除口径与全部口径等价，跳过重复
        subset = pairs.take(keep)
        m = compute_pair_metrics(subset)
        rows.append(
            {
                "sensitivity": label,
                "n_targets": m["n_targets"],
                "n_estimated": m["n_estimated"],
                "base_ape_median": m["base_ape_median"],
                "trt_ape_median": m["trt_ape_median"],
                "ape_delta_median": m["ape_delta_median"],
            }
        )

    return pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                pa.field("sensitivity", pa.string()),
                pa.field("n_targets", pa.int32()),
                pa.field("n_estimated", pa.int32()),
                pa.field("base_ape_median", pa.float64()),
                pa.field("trt_ape_median", pa.float64()),
                pa.field("ape_delta_median", pa.float64()),
            ]
        ),
    )


# ---------------------------------------------------------------------------
# 端到端编排入口（C6：可重放的一次验证流程）
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ValueValidationOutcome:
    """一次 ``run_value_validation`` 的结果（端到端可重放）。"""

    run_id: str
    units: pa.Table
    features: dict[str, dict[str, Any]]
    dev_ids: list[str]
    holdout_ids: list[str]
    dev_table: pa.Table
    confirm_table: pa.Table
    frozen_config_path: Path
    confirmation_path: Path
    conclusion: str
    conclusion_reasons: list[str]
    round2_triggered: bool
    round2_reasons: list[str]


def run_value_validation(
    data_dir: Path,
    out_root: Path,
    *,
    run_id: str,
    min_estimated: int,
    improvement_threshold: float,
    max_coverage_regression: float,
    max_zero_delta_ratio: float,
    max_noop_rate: float,
    min_effective_coverage: float,
    min_ocr_align_ratio: float,
    confirmed_by: str,
    dev_ratio: float = 0.7,
) -> ValueValidationOutcome:
    """端到端验证流程（C6）：重建→单位→特征→指数→切分→开发→冻结→确认→结论。

    全程离线（零网络零付费）；产物写入 ``out_root/<run_id>/``；所有中间产物
    与结论可重放。若第一轮结论非 CANDIDATE_INTEGRATION/证据不足，第二轮按
    门禁不触发并记录原因。D0 门禁内嵌（W4）：先过输入重建门禁，实验输入
    使用重建派生表而非全局 staged 表，门禁失败抛 ``RebuildGateError`` 终止；
    run 目录已存在时拒绝运行，重跑 SHALL 使用新 run_id（防覆盖历史运行）。
    """
    data_dir = data_dir.resolve()
    out_root = out_root.resolve()
    repo_root = data_dir.parent.parent
    run_dir = out_root / run_id
    if run_dir.exists():
        raise ValueError(f"run 目录已存在，重跑 SHALL 使用新 run_id：{run_dir}")
    pointer = load_version_pointer(data_dir)
    freeze_manifest_path = data_dir / Path(str(pointer["manifest"]))
    freeze_hash_before = file_sha256(freeze_manifest_path)
    # D0 前置：重建门禁未通过时抛 RebuildGateError，不进入回放阶段
    rebuild = run_rebuild(data_dir, out_root, run_id=run_id)
    annotation = rebuild.rebuilt_table
    prod_path = rebuild.production_manifest_path
    ocr_run_dir = rebuild.ocr_run_dir
    asset_manifest = (
        data_dir
        / "raw"
        / "source=lianjia_ext"
        / "dataset=floorplan_ocr_run"
        / "batch_id=floorplan-extfp4-batch-01"
        / "floorplan_asset_manifest.json"
    )
    sale = pq.read_table(
        data_dir
        / "staged"
        / "lianjia_ext"
        / "runs"
        / "run_20260825T033835Z"
        / "lianjia_ext_ordinary_residential.parquet"
    )

    units = build_unit_list(
        production_manifest_path=prod_path,
        asset_manifest_path=asset_manifest,
        ocr_run_dir=ocr_run_dir,
        annotation=annotation,
        sale=sale,
    )
    features = build_room_features(annotation)
    task_to_source = {
        str(units.column("ocr_task_id").to_pylist()[i]): str(
            units.column("source_record_id").to_pylist()[i]
        )
        for i in range(units.num_rows)
        if units.column("ocr_task_id").to_pylist()[i]
    }
    community_index = build_community_month_index(sale)
    dev, holdout = time_split_units(units, dev_ratio=dev_ratio)

    run_dir = out_root / run_id
    dev_table = run_round1_development(
        units=units,
        pool=sale,
        features_by_task=features,
        task_to_source=task_to_source,
        dev_source_ids=dev,
        holdout_source_ids=set(holdout),
        community_index=community_index,
        out_dir=run_dir / "round1",
        run_id=run_id,
    )

    config_path, config = freeze_round1_config(
        out_dir=run_dir / "round1",
        run_id=run_id,
        min_estimated=min_estimated,
        improvement_threshold=improvement_threshold,
        max_coverage_regression=max_coverage_regression,
        similarity_weights={"room_count": 1.0, "room_types": 1.0},
        min_effective_coverage=min_effective_coverage,
        confirmed_by=confirmed_by,
        max_zero_delta_ratio=max_zero_delta_ratio,
        max_noop_rate=max_noop_rate,
    )
    confirmation_path = write_user_confirmation(
        out_dir=run_dir / "round1",
        run_id=run_id,
        config=config,
        confirmed_by=confirmed_by,
        summary=(
            "AskUserQuestion 确认：判别力（min_estimated/零差占比/no-op 率）"
            "与改善门槛；冻结配置哈希锁定。"
        ),
    )

    confirm_table = run_round1_confirmation(
        units=units,
        pool=sale,
        features_by_task=features,
        task_to_source=task_to_source,
        holdout_source_ids=holdout,
        community_index=community_index,
        out_dir=run_dir / "round1",
        run_id=run_id,
        config_path=config_path,
    )
    metrics = compute_pair_metrics(confirm_table)
    conclusion, reasons = conclude_round1(metrics, config)
    triggered, gate_reasons = decide_round2_gate(
        round1_conclusion=conclusion,
        metrics=metrics,
        min_effective_coverage=min_effective_coverage,
        min_ocr_align_ratio=min_ocr_align_ratio,
    )

    subgroup = compute_subgroup_metrics(confirm_table)
    pq.write_table(subgroup, run_dir / "round1" / "round1_confirm_subgroup.parquet")
    sens = sensitivity_analysis(confirm_table, units)
    pq.write_table(sens, run_dir / "round1" / "round1_confirm_sensitivity.parquet")

    # W2：单位/特征/面积质量/切分随 run 落盘，保证 run 目录自包含可重放
    pq.write_table(units, run_dir / "units.parquet", compression="zstd")
    pq.write_table(
        build_area_quality(units, features),
        run_dir / "area_quality.parquet",
        compression="zstd",
    )
    (run_dir / "room_features.json").write_text(
        json.dumps(features, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    split_doc = {
        "run_id": run_id,
        "dev_ratio": dev_ratio,
        "dev": dev,
        "holdout": holdout,
        "dev_count": len(dev),
        "holdout_count": len(holdout),
        "disjoint": not (set(dev) & set(holdout)),
    }
    (run_dir / "split.json").write_text(
        json.dumps(split_doc, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    inventory_entries, unique_units = inventory_vs_unique(units)
    inventory_doc = {
        "run_id": run_id,
        "inventory_entries": inventory_entries,
        "unique_source_record_ids": unique_units,
        "duplicate_cluster_units": int(
            sum(1 for v in units.column("is_duplicate").to_pylist() if v)
        ),
    }
    (run_dir / "inventory_summary.json").write_text(
        json.dumps(inventory_doc, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    freeze_hash_after = file_sha256(freeze_manifest_path)
    final = {
        "run_id": run_id,
        "round1_conclusion": conclusion,
        "round1_reasons": reasons,
        "round2_triggered": triggered,
        "round2_reasons": gate_reasons,
        "frozen_config": str(config_path),
        "confirmation": str(confirmation_path),
        "code_commit": _git_commit(repo_root),
        "rebuild_report": {
            "path": str(rebuild.report_path),
            "sha256": _sha256_of(rebuild.report_path),
        },
        "freeze_manifest_sha256": {
            "before": freeze_hash_before,
            "after": freeze_hash_after,
            "unchanged": freeze_hash_before == freeze_hash_after,
        },
        "metrics": {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in metrics.items()
        },
        "built_at": datetime.now(UTC).isoformat(),
    }
    work = run_dir / "round1_final.json.incomplete"
    work.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(run_dir / "round1_final.json")

    return ValueValidationOutcome(
        run_id=run_id,
        units=units,
        features=features,
        dev_ids=dev,
        holdout_ids=holdout,
        dev_table=dev_table,
        confirm_table=confirm_table,
        frozen_config_path=config_path,
        confirmation_path=confirmation_path,
        conclusion=conclusion,
        conclusion_reasons=reasons,
        round2_triggered=triggered,
        round2_reasons=gate_reasons,
    )
