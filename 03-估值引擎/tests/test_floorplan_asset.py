"""EXTFP2-D 户型图原图资产与校验（floorplan_asset）的离线测试。

全部用例离线（Pillow 本地解码 + httpx.MockTransport 生成下载，绝不触网）。
覆盖：魔数/MIME/尺寸识别、扩展名由字节决定（URL 后缀仅比对不采用）、有效资产落盘、
无效字节/SHA256 复核不符标记 IMAGE_INVALID、同内容重复只标记不合并、下载失败不进资产、
schema 与 manifest JSON 往返兼容（资产主键与下载层一致）。
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import httpx
from PIL import Image

from compsval.ingest.floorplan_asset import (
    ASSET_MANIFEST_FILENAME,
    AssetStatus,
    build_asset_manifest,
    sniff_image,
    url_suggested_extension,
)
from compsval.ingest.floorplan_download import (
    DownloadState,
    run_download,
)
from compsval.ingest.floorplan_selection import (
    SelectionEntry,
    SelectionManifest,
)

DOMAIN = "ke-image.ljcdn.com"
# URL 后缀为假象（图片实际是 JPEG/PNG），用于验证「扩展名由字节决定」
JPEG_URL = f"https://{DOMAIN}/hdic-frame/a.png?from=ke.com"
PNG_URL = f"https://{DOMAIN}/hdic-frame/b.jpg?from=ke.com"
PLAIN_URL = f"https://{DOMAIN}/hdic-frame/c.jpg?from=ke.com"
EVIL_URL = "http://evil.example.com/x.jpg?from=ke.com"

MANIFEST_HASH = "recids-hash-EXTFP2D-0123456789abcdef"


def _jpeg_bytes(width: int = 12, height: int = 9) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), (200, 30, 30)).save(buf, format="JPEG")
    return buf.getvalue()


def _png_bytes(width: int = 8, height: int = 6) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), (30, 200, 30)).save(buf, format="PNG")
    return buf.getvalue()


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _entry(rid: str, row: int, seq: int, url: str, domain: str = DOMAIN) -> SelectionEntry:
    return SelectionEntry(
        source_record_id=rid,
        row_number=row,
        url_seq=seq,
        url=url,
        normalized_url=url,
        domain=domain,
    )


def _manifest(entries: list[SelectionEntry]) -> SelectionManifest:
    return SelectionManifest(
        selection_rule_version="EXTFP2-B-SELECT-1.0",
        selection_rule_text="test",
        snapshot_ref="snap-1",
        geoscope="测试",
        filter_condition="test",
        record_count=len({e.source_record_id for e in entries}),
        asset_count=len(entries),
        record_ids_hash=MANIFEST_HASH,
        records=entries,
        domain_whitelist=[DOMAIN],
        estimated_download_bytes=len(entries) * 70 * 1024,
        storage_cap_bytes=len(entries) * 70 * 1024 * 2,
        budget_cap_yuan=float(len(entries)),
        avg_bytes_estimate=70 * 1024,
    )


def _mock_transport(by_url: dict[str, bytes], status: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url not in by_url:
            return httpx.Response(404, content=b"", request=request)
        return httpx.Response(status, content=by_url[url], request=request)

    return httpx.MockTransport(handler)


def _download(entries: list[SelectionEntry], by_url: dict[str, bytes], tmp_path: Path):
    manifest = _manifest(entries)
    return run_download(
        manifest,
        tmp_path,
        transport=_mock_transport(by_url),
        base_backoff=0.0,
    )


# ---------------------------------------------------------------------------
# 1) 魔数/MIME/尺寸识别
# ---------------------------------------------------------------------------


def test_sniff_image_valid_png() -> None:
    d = sniff_image(_png_bytes(8, 6))
    assert d is not None
    assert d.format_name == "PNG"
    assert d.mime_type == "image/png"
    assert d.extension == ".png"
    assert (d.width, d.height) == (8, 6)


def test_sniff_image_valid_jpeg() -> None:
    d = sniff_image(_jpeg_bytes(12, 9))
    assert d is not None
    assert d.format_name == "JPEG"
    assert d.mime_type == "image/jpeg"
    assert d.extension == ".jpg"
    assert (d.width, d.height) == (12, 9)


def test_sniff_image_garbage_is_none() -> None:
    assert sniff_image(b"\x00\x01\x02 not a real image response \xff\xfe" * 8) is None


def test_url_suggested_extension_untangled() -> None:
    # 多后缀假象：只取末段
    assert url_suggested_extension("http://x/a.png.1440x1080.jpg") == "jpg"
    assert url_suggested_extension(f"https://{DOMAIN}/b.jpg") == "jpg"
    assert url_suggested_extension(f"https://{DOMAIN}/c.PNG") == "png"
    assert url_suggested_extension(f"https://{DOMAIN}/d") is None


# ---------------------------------------------------------------------------
# 2) 有效资产：扩展名由字节决定（URL 后缀不一致 → mismatch）、落盘、SHA 复核
# ---------------------------------------------------------------------------


def test_asset_extension_from_bytes_not_url(tmp_path: Path) -> None:
    """URL 后缀 .png 但实际字节是 JPEG → 文件扩展名 .jpg，并标记 extension_mismatch_url。"""
    jpg = _jpeg_bytes()
    _download([_entry("R1", 1, 1, JPEG_URL)], {JPEG_URL: jpg}, tmp_path)
    out_raw = tmp_path / "raw"
    run = build_asset_manifest(tmp_path, out_raw)

    assert run.counts["DOWNLOADED"] == 1
    asset = run.assets[0]
    assert asset.asset_status is AssetStatus.DOWNLOADED
    assert asset.file_extension == ".jpg"  # 由字节决定
    assert asset.mime_type == "image/jpeg"
    assert asset.extension_mismatch_url is True  # URL 说 .png，字节是 .jpg
    assert asset.sha256 == _sha(jpg)
    assert (asset.width, asset.height) == (12, 9)

    # 原始字节按字节扩展名落盘
    stored = out_raw / f"batch_id={run.batch_id}" / f"{asset.asset_id}.jpg"
    assert stored.is_file()
    assert stored.read_bytes() == jpg
    assert asset.storage_path == f"{asset.asset_id}.jpg"

    # 复用下载时的 content_sha256 完全一致（血缘可回溯）
    dl = (tmp_path / "download_state.json").read_text(encoding="utf-8")
    assert str(_sha(jpg)) in dl


def test_asset_duplicate_marked_not_merged(tmp_path: Path) -> None:
    """两条成交记录同内容字节 → 各自独立资产，标记 is_duplicate，文件都保留不合并。"""
    jpg = _jpeg_bytes()
    url = PLAIN_URL
    _download(
        [_entry("R1", 1, 1, url), _entry("R2", 2, 1, url)],
        {url: jpg},
        tmp_path,
    )
    out_raw = tmp_path / "raw"
    run = build_asset_manifest(tmp_path, out_raw)

    # 两条资产都保留（R1/R2 各自独立 sales record）
    assets = run.assets
    assert len(assets) == 2
    assert assets[0].asset_id != assets[1].asset_id
    for a in assets:
        assert a.asset_status is AssetStatus.DOWNLOADED
        assert a.is_duplicate is True  # 同内容 SHA256 → 标记重复
        assert a.duplicate_count == 2
        assert a.sha256 == _sha(jpg)
    # 两个文件都落盘（不合并、不覆盖）
    for a in assets:
        assert (out_raw / f"batch_id={run.batch_id}" / a.storage_path).is_file()


# ---------------------------------------------------------------------------
# 3) 反例：无效字节 / SHA256 复核不符 → IMAGE_INVALID，不进资产；失败任务不计数资产
# ---------------------------------------------------------------------------


def test_invalid_bytes_asset_invalid(tmp_path: Path) -> None:
    garbage = b"\x89PNG\r\n\x1a\ntruncated-garbage-not-full-image-" * 4
    _download([_entry("R1", 1, 1, PLAIN_URL)], {PLAIN_URL: garbage}, tmp_path)
    out_raw = tmp_path / "raw"
    run = build_asset_manifest(tmp_path, out_raw)

    assert run.counts["IMAGE_INVALID"] == 1
    asset = run.assets[0]
    assert asset.asset_status is AssetStatus.IMAGE_INVALID
    assert asset.last_error == "not-decodable-image"
    assert asset.storage_path is None
    # 不进资产：无落盘文件
    batch = out_raw / f"batch_id={run.batch_id}"
    assert not batch.exists() or list(batch.glob("*.jpg")) == []


def test_sha256_reverify_mismatch_asset_invalid(tmp_path: Path) -> None:
    """下载记录 content_sha256 与实际字节不符 → 复核失败标记 IMAGE_INVALID（刷改防御）。"""
    jpg = _jpeg_bytes()
    dl_state = {
        "run_id": "floorplan-dl-sha-mismatch",
        "downloader_version": "EXTFP2-C-DL-1.0",
        "manifest_ref": "recids-hash-x",
        "sourced": True,
        "state_counts": {"DOWNLOADED": 1},
        "created_at": "2026-08-25T00:00:00Z",
        "updated_at": "2026-08-25T00:00:00Z",
        "run_dir": str(tmp_path),
        "tasks": [
            {
                "download_task_id": "td1",
                "asset_id": "a1",
                "sale_record_key": "s1",
                "source_record_id": "R1",
                "row_number": 1,
                "url_ordinal": 1,
                "url": PLAIN_URL,
                "canonical_url": PLAIN_URL,
                "domain": DOMAIN,
                "state": "DOWNLOADED",
                "attempts": 1,
                "content_sha256": "0" * 64,  # 伪造的复核基准
                "downloaded_at": "2026-08-25T00:00:00Z",
            }
        ],
    }
    (tmp_path / "download_state.json").write_bytes(json.dumps(dl_state).encode("utf-8"))
    (tmp_path / "td1.img").write_bytes(jpg)  # 实际字节哈希 != "0"*64

    run = build_asset_manifest(tmp_path, tmp_path / "raw", batch_id="b1")
    assert run.counts["IMAGE_INVALID"] == 1
    asset = run.assets[0]
    assert asset.asset_status is AssetStatus.IMAGE_INVALID
    assert asset.last_error == "sha256-mismatch-reverify"
    assert asset.storage_path is None


def test_download_failed_not_counted_as_asset(tmp_path: Path) -> None:
    """未被下载（DOWNLOAD_FAILED / 缺 .img）的任务不进资产，仅在计数呈现。"""
    jpg = _jpeg_bytes()
    _download(
        [_entry("R1", 1, 1, PLAIN_URL), _entry("R2", 2, 1, EVIL_URL)],
        {PLAIN_URL: jpg},  # EVIL_URL 不为 mock 提供 → 404 → DOWNLOAD_FAILED
        tmp_path,
    )
    out_raw = tmp_path / "raw"
    run = build_asset_manifest(tmp_path, out_raw)

    assert run.counts["NOT_AVAILABLE"] == 1  # R2 下载失败，无字节
    assert run.counts["DOWNLOADED"] == 1
    assert [a.source_record_id for a in run.assets] == ["R1"]
    assert all(a.asset_status is AssetStatus.DOWNLOADED for a in run.assets)


# ---------------------------------------------------------------------------
# 4) schema 兼容：manifest JSON 往返 + 资产主键与下载层血缘一致
# ---------------------------------------------------------------------------


def test_asset_manifest_schema_roundtrip(tmp_path: Path) -> None:
    jpg = _jpeg_bytes()
    rec = _download([_entry("R1", 1, 1, JPEG_URL)], {JPEG_URL: jpg}, tmp_path)
    out_raw = tmp_path / "raw"
    run = build_asset_manifest(tmp_path, out_raw, batch_id="schema-batch")

    manifest_path = out_raw / "batch_id=schema-batch" / ASSET_MANIFEST_FILENAME
    assert manifest_path.is_file()
    loaded = json.loads(manifest_path.read_text(encoding="utf-8"))

    # 往返后字段保持一致（新资产 schema 向后兼容可读）
    assert loaded["batch_id"] == "schema-batch"
    assert loaded["counts"]["DOWNLOADED"] == 1
    assert loaded["rules_version"] == "EXTFP2-D-ASSET-1.0"
    assert loaded["download_run_id"] == rec.run_id

    # 资产主键与下载层血缘一致（同一 download_task_id / asset_id）
    task = rec.tasks[0]
    asset = run.assets[0]
    assert asset.asset_id == task.asset_id
    assert asset.download_task_id == task.download_task_id
    assert asset.source_record_id == task.source_record_id
    # 下载层状态完整（schema 兼容既有下载模型）
    assert task.state is DownloadState.DOWNLOADED


# ---------------------------------------------------------------------------
# 5) staged 资产表 roundtrip（RV-EXTFP2-F-01#F1：读回 parquet 后列集与 manifest 行级一致）
# ---------------------------------------------------------------------------


def test_write_staged_asset_table_roundtrip_columns(tmp_path: Path) -> None:
    """staged floorplan_asset.parquet 读回后：列集 = 预期 schema 23 列，行数与 manifest
    一致且关键字段（asset_id/sha256/asset_status/storage_path 等）逐行对齐。

    这是对「CLI 正常通路写 staged 表」之外的独立 roundtrip 断言，证明 parquet 落库不是
    无约束补写，而是与不可变资产 manifest 保持行级血缘（RV-EXTFP2-D-01#F3）。
    """
    import pyarrow.parquet as pq

    from compsval.ingest.floorplan_asset import (
        ASSET_STAGED_FILENAME,
        write_staged_asset_table,
    )

    jpg = _jpeg_bytes()
    _download([_entry("R1", 1, 1, PLAIN_URL)], {PLAIN_URL: jpg}, tmp_path)
    run = build_asset_manifest(tmp_path, tmp_path / "raw", batch_id="staged-rb")

    path = write_staged_asset_table(run, tmp_path)
    assert path == tmp_path / "staged" / ASSET_STAGED_FILENAME
    assert path.is_file()
    # 无残留 .incomplete 中间态（原子 replace）
    assert not path.with_suffix(path.suffix + ".incomplete").exists()

    table = pq.read_table(path)
    assert table.num_rows == len(run.assets) == 1
    assert set(table.column_names) == {
        "batch_id",
        "asset_rules_version",
        "asset_id",
        "download_task_id",
        "source_record_id",
        "source_row_number",
        "url_ordinal",
        "asset_status",
        "downloaded_at",
        "http_status",
        "final_url",
        "mime_type",
        "file_extension",
        "width",
        "height",
        "byte_size",
        "sha256",
        "is_duplicate",
        "duplicate_count",
        "extension_mismatch_url",
        "storage_path",
        "download_attempts",
        "last_error",
    }

    row = table.to_pylist()[0]
    a = run.assets[0]
    assert row["asset_id"] == a.asset_id
    assert row["sha256"] == a.sha256
    assert row["asset_status"] == a.asset_status.value
    assert row["batch_id"] == run.batch_id
    assert row["asset_rules_version"] == run.rules_version
    # 可空/可空值字段与 manifest 逐行一致（任何列不得错位脱落）
    assert row["storage_path"] == a.storage_path
    assert row["file_extension"] == a.file_extension
    assert row["downloaded_at"] == a.downloaded_at
    assert row["width"] == a.width
    assert row["height"] == a.height
    assert row["is_duplicate"] == a.is_duplicate
    assert row["duplicate_count"] == a.duplicate_count
