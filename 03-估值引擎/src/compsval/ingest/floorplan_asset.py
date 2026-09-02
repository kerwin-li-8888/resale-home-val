"""户型图原图资产与校验（EXTFP2-D，技术方案 §7.1/§7.5/§8.1 步骤 8-10/§11.4/§13）。

对 EXTFP2-C 下载运行（``download_state.json`` + ``{download_task_id}.img`` 原始字节）
执行字节级校验：魔数/MIME/尺寸（Pillow）、扩展名**由实际字节联合确定**（绝不直取
URL 后缀，如 ``*.png.1440x1080.jpg`` 的后缀可能是假象）、SHA256 复核、原始字节落盘
到数据湖 ``dataset=floorplan_image/batch_id=<id>/``，并生成不可变资产 manifest。
重复（同内容 SHA256）只标记关系、不删除不合并（技术方案 §5.3）。

范围边界（EXTFP2-D 合同）：本模块只管**原图资产化与字节校验**。不做感知哈希去重
（perceptual_hash 仅占位，供后续质量报告，不做业务去重）、不做像素级占位图识别
（``NON_FLOORPLAN_PLACEHOLDER`` 由选择层在下游负责）、不做任何真实网络下载
（真实 10 张试跑属 EXTFP2-E）、不写密钥/敏感信息。

状态机衔接（§7.5）：下载层 ``DOWNLOADED`` 的字节经校验为有效图片 → ``DOWNLOADED``
（终态）；字节无法解码/非图片/复核 SHA256 不符 → ``IMAGE_INVALID``（终态，不进资产，
不留存副本）。``DOWNLOAD_FAILED`` 任务无字节，不进资产，仅在 manifest 计数中呈现。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_download import (
    STATE_FILENAME,
    DownloadRunRecord,
    DownloadState,
    DownloadTask,
)

try:  # Pillow 性能路径：不阻塞纯解析场景（EXTFP2-A 已锁定依赖，离线可用）
    from PIL import Image

    _HAS_PIL = True
except Exception:  # pragma: no cover - 依赖缺失退化（compsval floorplan ... 正常路径不会走到）
    _HAS_PIL = False

# 资产化规则版本：随 manifest 落盘，供复核与下游引用（EXTFP2-D-ASSET-1.0）
ASSET_RULES_VERSION = "EXTFP2-D-ASSET-1.0"

# 资产 manifest 文件名（位于 batch 目录）
ASSET_MANIFEST_FILENAME = "floorplan_asset_manifest.json"

# 检测到的图片格式 → (MIME, 规范扩展名)
FORMAT_META: dict[str, tuple[str, str]] = {
    "JPEG": ("image/jpeg", ".jpg"),
    "PNG": ("image/png", ".png"),
    "WEBP": ("image/webp", ".webp"),
    "TIFF": ("image/tiff", ".tiff"),
}

# URL 后缀规范化（jpeg→jpg、tif→tiff），其余小写原样
_URL_EXT_ALIAS = {"jpeg": "jpg", "tif": "tiff"}


# 与 §7.5 一致的资产状态（下载层 DOWNLOADED / IMAGE_INVALID 为本模块产出终态）
class AssetStatus(StrEnum):
    DOWNLOADED = "DOWNLOADED"  # 字节已取得且校验为有效图片（进资产、留副本）
    IMAGE_INVALID = "IMAGE_INVALID"  # 字节非有效图片 / SHA256 复核不符（高压不进资产）

    NOT_AVAILABLE = "NOT_AVAILABLE"  # 下载未成功，无字节（不进资产，仅计数）


@dataclass(frozen=True)
class DetectedImage:
    """对原始字节的检测结果（魔数/MIME/尺寸/扩展名联合确定）。"""

    format_name: str
    mime_type: str
    extension: str  # 以 '.' 开头，如 .jpg
    width: int
    height: int
    url_suggested_extension: str | None  # URL 后缀推断（仅供比对，不用于定名）


def url_suggested_extension(url: str) -> str | None:
    """由 URL 路径末段推断的扩展名（技术方案 §8.1 步骤 9 '不直取 URL 后缀'）。

    取路径最后一段的末位点号 token（如 ``a.jpg.1440x1080.jpg`` → ``.jpg``）做规范化，
    仅用于与检测结果比对（extension_mismatch_url），**不**以它决定文件名/扩展名。
    """
    try:
        path = urlsplit(url).path or ""
    except ValueError:
        return None
    base = path.rsplit("/", 1)[-1] if "/" in path else path
    match = re.search(r"\.([A-Za-z0-9]{1,5})$", base)
    if not match:
        return None
    ext = match.group(1).lower()
    return _URL_EXT_ALIAS.get(ext, ext)


def sniff_image(data: bytes) -> DetectedImage | None:
    """魔数/MIME/尺寸联合识别（Pillow）。无法解码/非图片返回 None。"""
    if not _HAS_PIL:
        return None
    try:
        with Image.open(BytesIO(data)) as im:
            fmt = (im.format or "").upper()
            if fmt not in FORMAT_META:
                return None
            width, height = im.size
    except Exception:  # noqa: BLE001 - Pillow 解码失败一律视为非有效图片
        return None
    mime, ext = FORMAT_META[fmt]
    return DetectedImage(
        format_name=fmt,
        mime_type=mime,
        extension=ext,
        width=width,
        height=height,
        url_suggested_extension=None,
    )


class FloorplanAsset(BaseModel):
    """单张户型图原图资产（技术方案 §7.1 字段，EXTFP2-D-ASSET-1.0）。

    asset_id 与下载层一致（由成交记录+URL 序号确定性生成），保证血缘可回溯。
    只标记重复，不删除/不合并业务关系（§5.3）。
    """

    asset_id: str
    download_task_id: str
    source_record_id: str
    source_row_number: int
    url_ordinal: int
    source_url_raw: str = Field(description="原始候选 URL，不改写")
    download_url: str = Field(description="实际使用 URL（HTTPS 优先规范化后）")
    downloader_version: str
    asset_status: AssetStatus
    downloaded_at: str | None = None
    http_status: int | None = None
    final_url: str | None = None
    mime_type: str | None = None
    file_extension: str | None = Field(
        default=None, description="由实际字节确定的扩展名（带前导点）"
    )
    width: int | None = None
    height: int | None = None
    byte_size: int | None = None
    sha256: str | None = None
    perceptual_hash: str | None = Field(
        default=None,
        description="感知哈希占位（仅用于后续质量报告，不做业务去重；本阶段不计算）",
    )
    storage_path: str | None = Field(
        default=None, description="数据湖内相对 batch 目录的存储路径（如 <asset_id>.jpg）"
    )
    download_attempts: int = 0
    last_error: str | None = None
    extension_mismatch_url: bool = Field(
        default=False, description="URL 后缀与字节决定扩展名不一致（证明未直取 URL 后缀）"
    )
    is_duplicate: bool = Field(default=False, description="与其它资产同内容 SHA256（只标记不合并）")
    duplicate_count: int = Field(default=0, description="共享同内容 SHA256 的资产总数（含自身）")


class FloorplanAssetRun(BaseModel):
    """一次资产化的总记录与不可变 manifest（EXTFP2-D 退出证据）。"""

    batch_id: str
    rules_version: str = ASSET_RULES_VERSION
    download_run_id: str
    download_run_dir: str
    manifest_ref: str | None = Field(default=None, description="下载清单 record_ids_hash 或路径")
    sourced: bool
    created_at: str
    assets: list[FloorplanAsset] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _load_run(start_path: Path) -> DownloadRunRecord:
    """读取下载运行记录（读取本目录 download_state.json，兼容旧名 download_run.json）。"""
    state_path = start_path / STATE_FILENAME
    if not state_path.is_file():
        state_path = start_path / "download_run.json"
    if not state_path.is_file():
        raise FileNotFoundError(f"下载运行状态缺失: {start_path}")
    return DownloadRunRecord.model_validate(json.loads(state_path.read_text(encoding="utf-8")))


def _batch_dir_for(out_raw_dir: Path, batch_id: str) -> Path:
    return out_raw_dir / f"batch_id={batch_id}"


def build_asset_manifest(
    run_dir: Path,
    out_raw_dir: Path,
    *,
    rules_version: str = ASSET_RULES_VERSION,
    batch_id: str | None = None,
) -> FloorplanAssetRun:
    """校验下载运行的原始字节并生成不可变资产 manifest（EXTFP2-D）。

    - 仅对 ``DOWNLOADED`` 且 ``{download_task_id}.img`` 存在的任务构建资产；
    - 魔数/MIME/尺寸校验（Pillow）；扩展名由字节决定，URL 后缀仅比对不采用；
    - 复核 SHA256 = 下载时记录，不符标记 IMAGE_INVALID 不进资产；
    - 有效图片原始字节落盘到 ``<out_raw_dir>/batch_id=<id>/<asset_id><ext>``；
    - 同内容 SHA256 只标记 is_duplicate，不删除/不合并；
    - 不可变 manifest 原子写盘。

    返回完整 ``FloorplanAssetRun``。本函数不触网。
    """
    record = _load_run(run_dir)
    resolved_batch = batch_id or f"floorplan-{record.run_id}"
    batch_dir = _batch_dir_for(out_raw_dir, resolved_batch)

    assets: list[FloorplanAsset] = []
    counts = {
        AssetStatus.DOWNLOADED.value: 0,
        AssetStatus.IMAGE_INVALID.value: 0,
        AssetStatus.NOT_AVAILABLE.value: 0,
    }

    # 只处理下载成功且有字节的任务
    candidates: list[DownloadTask] = []
    for task in record.tasks:
        if task.state is not DownloadState.DOWNLOADED:
            counts[AssetStatus.NOT_AVAILABLE.value] += 1
            continue
        if not (run_dir / f"{task.download_task_id}.img").is_file():
            counts[AssetStatus.NOT_AVAILABLE.value] += 1
            continue
        candidates.append(task)

    # 先读取全部字节并检测，采集有效资产用于重复标记
    staged: list[tuple[DownloadTask, bytes, DetectedImage | None, str, str | None]] = []
    for task in candidates:
        data = (run_dir / f"{task.download_task_id}.img").read_bytes()
        content_sha = _sha256_hex(data)
        detected = sniff_image(data)
        stage_error: str | None = None
        if task.content_sha256 and content_sha != task.content_sha256:
            stage_error = "sha256-mismatch-reverify"
        elif detected is None:
            stage_error = "not-decodable-image"
        staged.append((task, data, detected, content_sha, stage_error))

    # 同内容 SHA256 分组（只标记不合并，§5.3）
    sha_groups: dict[str, int] = {}
    for _, _, _, content_sha, stage_error in staged:
        if stage_error is None:
            sha_groups[content_sha] = sha_groups.get(content_sha, 0) + 1

    for task, data, detected, content_sha, stage_error in staged:
        base = FloorplanAsset(
            asset_id=task.asset_id,
            download_task_id=task.download_task_id,
            source_record_id=task.source_record_id,
            source_row_number=task.row_number,
            url_ordinal=task.url_ordinal,
            source_url_raw=task.url,
            download_url=task.canonical_url,
            downloader_version=record.downloader_version,
            asset_status=(AssetStatus.IMAGE_INVALID if stage_error else AssetStatus.DOWNLOADED),
            downloaded_at=task.downloaded_at,
            http_status=None,
            final_url=task.final_url,
            byte_size=len(data),
            sha256=content_sha,
            download_attempts=task.attempts,
            last_error=stage_error or task.last_error,
        )
        if stage_error is not None:
            counts[AssetStatus.IMAGE_INVALID.value] += 1
            assets.append(base)
            continue

        # 有效图片：扩展名由字节决定，URL 后缀仅比对
        assert detected is not None
        url_ext = url_suggested_extension(task.canonical_url)
        mismatch = url_ext is not None and url_ext.lstrip(".") != detected.extension.lstrip(".")
        dup_count = sha_groups[content_sha]
        base.mime_type = detected.mime_type
        base.file_extension = detected.extension
        base.width = detected.width
        base.height = detected.height
        base.extension_mismatch_url = mismatch
        base.is_duplicate = dup_count > 1
        base.duplicate_count = dup_count

        filename = f"{task.asset_id}{detected.extension}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        dest = batch_dir / filename
        dest.write_bytes(data)  # 原始字节落盘，不转码不重压缩
        base.storage_path = filename

        counts[AssetStatus.DOWNLOADED.value] += 1
        assets.append(base)

    assets.sort(key=lambda a: a.asset_id)
    run = FloorplanAssetRun(
        batch_id=resolved_batch,
        rules_version=rules_version,
        download_run_id=record.run_id,
        download_run_dir=run_dir.as_posix(),
        manifest_ref=record.manifest_ref,
        sourced=record.sourced,
        created_at=datetime.now(UTC).isoformat(),
        assets=assets,
        counts=counts,
    )

    batch_dir.mkdir(parents=True, exist_ok=True)
    work = batch_dir / (ASSET_MANIFEST_FILENAME + ".incomplete")
    work.write_text(run.model_dump_json(indent=2), encoding="utf-8")
    work.replace(batch_dir / ASSET_MANIFEST_FILENAME)

    return run


#: staged 资产表文件名（技术方案 §5 数据边界；沿用 `data_dir/staged/*.parquet` 约定）
ASSET_STAGED_FILENAME = "floorplan_asset.parquet"


def write_staged_asset_table(
    run: FloorplanAssetRun,
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """把资产化结果落盘为 staged/floorplan_asset.parquet（EXTFP2-F，RV-EXTFP2-D-01#F3）。

    将不可变资产 manifest 的行级数据按 schema 平铺为 Parquet（zstd 压缩），原子写入
    ``.incomplete`` 兄弟文件后 rename，避免半写表冒充完整派生表（与 clean.py 的
    sale_event 表同一约定）。本函数不触网、不改写原始 raw 字节。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict[str, object]] = []
    for a in run.assets:
        rows.append(
            {
                "batch_id": run.batch_id,
                "asset_rules_version": run.rules_version,
                "asset_id": a.asset_id,
                "download_task_id": a.download_task_id,
                "source_record_id": a.source_record_id,
                "source_row_number": a.source_row_number,
                "url_ordinal": a.url_ordinal,
                "asset_status": a.asset_status.value,
                "downloaded_at": a.downloaded_at,
                "http_status": a.http_status,
                "final_url": a.final_url,
                "mime_type": a.mime_type,
                "file_extension": a.file_extension,
                "width": a.width,
                "height": a.height,
                "byte_size": a.byte_size,
                "sha256": a.sha256,
                "is_duplicate": a.is_duplicate,
                "duplicate_count": a.duplicate_count,
                "extension_mismatch_url": a.extension_mismatch_url,
                "storage_path": a.storage_path,
                "download_attempts": a.download_attempts,
                "last_error": a.last_error,
            }
        )

    table = pa.Table.from_pylist(rows)
    staged_dir = data_dir / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / ASSET_STAGED_FILENAME
    work_path = staged_dir / (ASSET_STAGED_FILENAME + ".incomplete")
    pq.write_table(table, work_path, compression=compression)
    work_path.replace(final_path)
    return final_path


def write_cumulative_staged_asset_table(
    manifests: list[dict[str, Any]],
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """把多个批量资产 manifest 合并为累计行级表写入 staged/floorplan_asset.parquet。

    CX-EXTFP2-01 §5：输出累计 10 张的资产证据。跨全部 batch_id 平铺每行并保留各自
    batch_id 血缘；原子写盘切换 current 指针（与 write_staged_asset_table 同一约定）。
    本函数不触网、不改写原始 raw 字节。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict[str, object]] = []
    for manifest in manifests:
        batch_id = str(manifest.get("batch_id", ""))
        rules_version = str(manifest.get("rules_version", ""))
        for a in manifest.get("assets", []) or []:
            rows.append(
                {
                    "batch_id": batch_id,
                    "asset_rules_version": rules_version,
                    "asset_id": a.get("asset_id"),
                    "download_task_id": a.get("download_task_id"),
                    "source_record_id": a.get("source_record_id"),
                    "source_row_number": a.get("source_row_number"),
                    "url_ordinal": a.get("url_ordinal"),
                    "asset_status": a.get("asset_status"),
                    "downloaded_at": a.get("downloaded_at"),
                    "http_status": a.get("http_status"),
                    "final_url": a.get("final_url"),
                    "mime_type": a.get("mime_type"),
                    "file_extension": a.get("file_extension"),
                    "width": a.get("width"),
                    "height": a.get("height"),
                    "byte_size": a.get("byte_size"),
                    "sha256": a.get("sha256"),
                    "is_duplicate": a.get("is_duplicate"),
                    "duplicate_count": a.get("duplicate_count"),
                    "extension_mismatch_url": a.get("extension_mismatch_url"),
                    "storage_path": a.get("storage_path"),
                    "download_attempts": a.get("download_attempts"),
                    "last_error": a.get("last_error"),
                }
            )

    table = pa.Table.from_pylist(rows)
    staged_dir = data_dir / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / ASSET_STAGED_FILENAME
    work_path = staged_dir / (ASSET_STAGED_FILENAME + ".incomplete")
    pq.write_table(table, work_path, compression=compression)
    work_path.replace(final_path)
    return final_path


__all__ = [
    "ASSET_MANIFEST_FILENAME",
    "ASSET_RULES_VERSION",
    "ASSET_STAGED_FILENAME",
    "AssetStatus",
    "DetectedImage",
    "FloorplanAsset",
    "FloorplanAssetRun",
    "FORMAT_META",
    "build_asset_manifest",
    "write_cumulative_staged_asset_table",
    "write_staged_asset_table",
    "sniff_image",
    "url_suggested_extension",
]
