"""Snapshot manifest schema: fetch provenance recorded alongside every raw snapshot.

Ported and renamed from Philly Fair Measure (fixed SHA e163eba6); the manifest
schema and its write/read helpers are kept as the provenance skeleton for the
immutable raw snapshot guarantee (DATA-004). Field names (e.g. ``carto_type``)
retain the upstream spelling so existing manifests remain readable; the source
adapter defines the source that populates them.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

MANIFEST_FILENAME = "manifest.json"


class ColumnInfo(BaseModel):
    name: str
    carto_type: str | None = None
    pg_type: str | None = None
    arrow_type: str


class FileInfo(BaseModel):
    path: str
    rows: int
    size_bytes: int
    sha256: str


class SnapshotManifest(BaseModel):
    manifest_version: int = 1
    source: str
    dataset: str
    endpoint: str
    query: str
    fetched_at: datetime
    completed_at: datetime
    duration_seconds: float
    source_row_count: int | None
    row_count: int
    page_size: int
    num_pages: int
    order_key: str | None
    row_limit: int | None = None
    excluded_columns: list[str] = Field(default_factory=list)
    columns: list[ColumnInfo]
    files: list[FileInfo]
    package_version: str
    mime_type: str | None = Field(
        default=None,
        description=(
            "实际数据 MIME 类型（如 XLSX/图片）；未知或未记录=None"
            "（EXTFP1-B 新增，向后兼容：旧 manifest 无此字段仍可反序列化）"
        ),
    )
    original_filename: str | None = Field(
        default=None,
        description=(
            "原始文件名（技术方案 §5.2 provenance；CX-EXTFP1-002 修复，"
            "向后兼容：旧 manifest 无此字段仍可反序列化）"
        ),
    )
    sheet_names: list[str] | None = Field(
        default=None, description="XLSX 工作表名（§5.2；无表概念=None）"
    )
    sheet_metadata: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "每个工作表的行列元数据（§5.2「工作表名、行列数」；每项含 "
            "sheet/rows/columns/headers；CX-EXTFP1-006-R 落盘，向后兼容："
            "旧 manifest 无此字段仍可反序列化）"
        ),
    )
    column_count: int | None = Field(
        default=None,
        description=(
            "列数（§5.2 行列数；CX-EXTFP1-006 落盘，向后兼容：旧 manifest "
            "无此字段仍可反序列化）"
        ),
    )
    data_date_min: str | None = Field(
        default=None, description="数据日期范围下限（ISO；§5.2）"
    )
    data_date_max: str | None = Field(
        default=None, description="数据日期范围上限（ISO；§5.2）"
    )
    verification_ref: str | None = Field(
        default=None, description="来源验证报告引用（§5.2；如画像报告路径）"
    )
    parser_version: str | None = Field(
        default=None, description="解析器版本（§5.2；本快照的解析规则版本）"
    )
    prev_snapshot_id: str | None = Field(
        default=None, description="上一快照 ID（§5.2；None=首个快照）"
    )
    notes: str | None = None


def write_manifest(manifest: SnapshotManifest, directory: Path) -> Path:
    path = directory / MANIFEST_FILENAME
    path.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return path


def read_manifest(directory: Path) -> SnapshotManifest:
    path = directory / MANIFEST_FILENAME
    return SnapshotManifest.model_validate_json(path.read_text(encoding="utf-8"))


class InputRef(BaseModel):
    """A pointer to the exact upstream data a derived table was built from."""

    dataset: str
    fetched_at: str
    content_hash: str | None = Field(
        default=None,
        description=(
            "上游内容 SHA256（CX-EXTFP1-002 修复：staged 血缘结构化指向原始"
            "快照内容；向后兼容：旧 manifest 无此字段仍可反序列化）"
        ),
    )


class DerivedManifest(BaseModel):
    """Provenance sidecar for staged/mart tables (stored as <table>.manifest.json)."""

    manifest_version: int = 1
    layer: str
    table: str
    built_at: datetime
    row_count: int
    inputs: list[InputRef]
    package_version: str
    parser_version: str | None = Field(
        default=None,
        description=(
            "解析规则版本（CX-EXTFP1-002 修复：staged 血缘记录解析器版本；"
            "向后兼容：旧 manifest 无此字段仍可反序列化）"
        ),
    )
    notes: str | None = None


def derived_manifest_path(table_path: Path) -> Path:
    return table_path.with_suffix(".manifest.json")


def write_derived_manifest(manifest: DerivedManifest, table_path: Path) -> Path:
    path = derived_manifest_path(table_path)
    path.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return path


def read_derived_manifest(table_path: Path) -> DerivedManifest:
    return DerivedManifest.model_validate_json(
        derived_manifest_path(table_path).read_text(encoding="utf-8")
    )