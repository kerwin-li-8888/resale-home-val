"""原始二进制文件不可变快照（EXTFP1-B，技术方案 §5.2）。

对外部链家成交原始 Excel（或任何二进制证据文件）建立字节级不可变快照：
原文件字节原样复制到数据湖（`03-估值引擎/data/raw/...`，不入 git），
manifest 记录文件名、字节数、SHA256 与 MIME 类型，并登记一条 ``RawSnapshot``
（``format=BINARY`` + ``mime_type``，补 EXTFP0-B 新增字段的登记缺口
RV-EXTFP0-F-01#F2）。源文件始终只读，绝不修改。

与 ``ingest/snapshots.write_raw_snapshot`` 的区别：后者把结构化表写为
parquet 快照；本模块把任意原始字节写为不可变快照（``data.bin`` +
``manifest.json``），两者共用 ``SnapshotManifest`` 结构与
``raw/source=<s>/dataset=<d>/fetched_at=<stamp>/`` 布局，可被
``catalog.list_snapshots`` 一并列出（数据血缘：原始字节与解析后 parquet 同湖）。
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from compsval import __version__, config
from compsval.contract.models import (
    RawSnapshot,
    SnapshotFormat,
    SnapshotParseStatus,
)
from compsval.contract.registry import SOURCE_ID_BY_DIR
from compsval.ingest.manifests import (
    FileInfo,
    SnapshotManifest,
    write_manifest,
)
from compsval.ingest.snapshots import FETCHED_AT_FORMAT

# 二进制快照内的原始字节文件（与 parquet 快照的 data.parquet 对称）
BINARY_FILENAME = "data.bin"
PROVENANCE_FILENAME = "provenance.json"

# 已知后缀 → MIME 类型（技术方案 §5.2/§8.5；未知后缀 → None 保持未知）
MIME_BY_SUFFIX: dict[str, str] = {
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".tiff": "image/tiff",
    ".txt": "text/plain",
    ".csv": "text/csv",
    ".html": "text/html",
    ".json": "application/json",
}


def infer_mime_type(path: Path) -> str | None:
    """按扩展名推断 MIME；未知后缀返回 None（保持未知，不猜测）。"""
    return MIME_BY_SUFFIX.get(path.suffix.lower())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class BinarySnapshotResult:
    """二进制快照写入结果：目录、manifest 与登记的快照记录。"""

    directory: Path
    data_path: Path
    manifest: SnapshotManifest
    raw_snapshot: RawSnapshot


def read_binary_provenance(snapshot_dir: Path) -> dict[str, Any] | None:
    """读附加 provenance 记录（无则 None；与 manifest 分开存储，不改写原证据）。"""
    from typing import cast

    path = snapshot_dir / PROVENANCE_FILENAME
    if not path.is_file():
        return None
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def attach_binary_provenance(
    snapshot_dir: Path,
    *,
    original_filename: str,
    sheet_names: list[str],
    sheet_metadata: list[dict[str, Any]],
    source_row_count: int,
    column_count: int,
    data_date_min: str | None = None,
    data_date_max: str | None = None,
    verification_ref: str | None = None,
    parser_version: str | None = None,
    fetched_at_source: str | None = None,
) -> Path:
    """给既有二进制快照**附加**不可变 provenance 记录（CX-EXTFP1-008 修复）。

    不改写 ``manifest.json`` 或 ``data.bin``（原始证据不可变）；在快照目录新增
    ``provenance.json`` 记录 §5.2 的完整 provenance（含取得时间来源）。已存在时
    ``FileExistsError``（附加记录不可变，一次写入）。
    """
    final_path = snapshot_dir / PROVENANCE_FILENAME
    if final_path.exists():
        raise FileExistsError(f"provenance already attached: {final_path}")
    payload = {
        "original_filename": original_filename,
        "sheet_names": sheet_names,
        "sheet_metadata": sheet_metadata,
        "source_row_count": source_row_count,
        "column_count": column_count,
        "data_date_min": data_date_min,
        "data_date_max": data_date_max,
        "verification_ref": verification_ref,
        "parser_version": parser_version,
        "fetched_at_source": fetched_at_source,
        "attached_at": datetime.now(UTC).isoformat(),
    }
    work = final_path.with_name(PROVENANCE_FILENAME + ".incomplete")
    work.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    work.replace(final_path)
    return final_path


def _snapshot_stamp(fetched_at: datetime) -> str:
    return fetched_at.astimezone(UTC).strftime(FETCHED_AT_FORMAT)


def write_binary_snapshot(
    path: Path,
    *,
    root: Path | None = None,
    source: str,
    dataset: str,
    fetched_at: datetime,
    query: str,
    mime_type: str | None = None,
    completed_at: datetime | None = None,
    original_filename: str | None = None,
    sheet_names: list[str] | None = None,
    sheet_metadata: list[dict[str, Any]] | None = None,
    source_row_count: int | None = None,
    column_count: int | None = None,
    data_date_min: str | None = None,
    data_date_max: str | None = None,
    verification_ref: str | None = None,
    parser_version: str | None = None,
    prev_snapshot_id: str | None = None,
) -> BinarySnapshotResult:
    """把本地原始字节写为不可变二进制快照（data.bin + manifest + RawSnapshot）。

    单快照语义：同 ``source/dataset/fetched_at`` 已存在时抛 ``FileExistsError``，
    重跑绝不覆盖既有证据。原子写：先写 ``.incomplete`` 目录再改名，崩溃残留不会
    伪装成有效快照。**final 目录存在性在创建 work_dir 之前检查**
    （CX-EXTFP1-004 修复：重复导入不得留下无效 ``.incomplete`` 目录）。
    源文件只读，绝不被修改。
    """
    if not path.is_file():
        raise FileNotFoundError(f"source file not found: {path}")
    if mime_type is None:
        mime_type = infer_mime_type(path)

    completed_at = completed_at or datetime.now(UTC)
    # 时序一致性（CX-EXTFP1-008 修复，技术方案 §5.2）：取得时间不得晚于登记/完成时间。
    # fetched_at 是真实取得时间，不得为造唯一目录而伪造；唯一性由目录 stamp 精确到秒承担。
    if fetched_at > completed_at:
        raise ValueError(
            f"fetched_at ({fetched_at.isoformat()}) must not be later than "
            f"completed_at ({completed_at.isoformat()})"
        )
    data_root = root if root is not None else config.data_dir()
    stamp = _snapshot_stamp(fetched_at)
    final_dir = (
        data_root / "raw" / f"source={source}" / f"dataset={dataset}" / f"fetched_at={stamp}"
    )
    if final_dir.exists():
        raise FileExistsError(
            f"snapshot already exists: {final_dir} (single-snapshot semantics; "
            f"re-run never overwrites evidence)"
        )
    work_dir = final_dir.with_name(final_dir.name + ".incomplete")
    work_dir.mkdir(parents=True, exist_ok=False)
    data_path = work_dir / BINARY_FILENAME
    shutil.copyfile(path, data_path)

    manifest = SnapshotManifest(
        source=source,
        dataset=dataset,
        endpoint="",
        query=query,
        fetched_at=fetched_at,
        completed_at=completed_at,
        duration_seconds=0.0,
        source_row_count=source_row_count,
        column_count=column_count,
        row_count=0,
        page_size=1,
        num_pages=1,
        order_key=None,
        columns=[],
        files=[
            FileInfo(
                path=BINARY_FILENAME,
                rows=0,
                size_bytes=data_path.stat().st_size,
                sha256=_sha256(data_path),
            )
        ],
        package_version=__version__,
        mime_type=mime_type,
        original_filename=original_filename,
        sheet_names=sheet_names,
        sheet_metadata=sheet_metadata,
        data_date_min=data_date_min,
        data_date_max=data_date_max,
        verification_ref=verification_ref,
        parser_version=parser_version,
        prev_snapshot_id=prev_snapshot_id,
    )
    write_manifest(manifest, work_dir)
    work_dir.rename(final_dir)

    raw_snapshot = raw_snapshot_from_binary(
        final_dir,
        source_id=SOURCE_ID_BY_DIR.get(source, "UNKNOWN"),
    )
    return BinarySnapshotResult(
        directory=final_dir,
        data_path=final_dir / BINARY_FILENAME,
        manifest=manifest,
        raw_snapshot=raw_snapshot,
    )


def raw_snapshot_from_binary(
    directory: Path,
    *,
    source_id: str,
) -> RawSnapshot:
    """从二进制快照目录（manifest）生成一条 RawSnapshot 登记记录。

    ``mime_type`` 取自 manifest（EXTFP1-B 补填）；旧 manifest 无此字段时为 None
    （保持未知）。``record_count=0``：二进制字节未逐行解析，非表格语义。
    ``snapshot_id`` = ``{source}-{dataset}-{fetched_at stamp}``，与
    ``ingest/stage.snapshot_id_of`` 命名一致，便于与结构化快照对齐。
    """
    manifest = SnapshotManifest.model_validate_json(
        (directory / "manifest.json").read_text(encoding="utf-8")
    )
    files = manifest.files
    (binary_file,) = [f for f in files if f.path == BINARY_FILENAME]
    stamp = directory.name.split("=", 1)[1]
    return RawSnapshot(
        snapshot_id=f"{manifest.source}-{manifest.dataset}-{stamp}",
        source_id=source_id,
        dataset=manifest.dataset,
        fetched_at=manifest.fetched_at,
        query=manifest.query,
        content_hash=binary_file.sha256,
        file_count=1,
        record_count=0,
        format=SnapshotFormat.BINARY,
        mime_type=manifest.mime_type,
        parse_status=SnapshotParseStatus.NOT_PARSED,
    )


def list_binary_snapshots(root: Path | None = None) -> list[RawSnapshot]:
    """扫描数据湖中的二进制快照，逐目录生成 RawSnapshot 登记（实时扫描，不持久化）。

    与 ``registry.list_evidence_snapshots`` 同模式：指纹/属性从实际文件读取，
    重跑稳定，始终反映存储字节。
    """
    data_root = root if root is not None else config.data_dir()
    snapshots: list[RawSnapshot] = []
    for snapshot_dir in sorted(
        (data_root / "raw").glob("source=*/dataset=*/fetched_at=*")
    ):
        if snapshot_dir.name.endswith(".incomplete"):
            continue
        if not (snapshot_dir / BINARY_FILENAME).is_file():
            continue
        if not (snapshot_dir / "manifest.json").is_file():
            continue
        source = snapshot_dir.parent.parent.name.split("=", 1)[1]
        snapshots.append(
            raw_snapshot_from_binary(
                snapshot_dir,
                source_id=SOURCE_ID_BY_DIR.get(source, "UNKNOWN"),
            )
        )
    return snapshots


__all__ = [
    "BINARY_FILENAME",
    "PROVENANCE_FILENAME",
    "BinarySnapshotResult",
    "attach_binary_provenance",
    "infer_mime_type",
    "list_binary_snapshots",
    "raw_snapshot_from_binary",
    "read_binary_provenance",
    "write_binary_snapshot",
]
