"""EXTFP1-B 原始二进制快照的离线测试。

用 tmp_path 合成的小字节文件验证：字节守恒、manifest/MIME/RawSnapshot 登记、
catalog 可见性、单快照语义、原子写、schema 向后兼容与 CLI 行为。绝不触碰真实
外部数据文件，也不访问网络。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from compsval import cli
from compsval.catalog import list_snapshots
from compsval.contract.models import (
    RawSnapshot,
    SnapshotFormat,
    SnapshotParseStatus,
)
from compsval.contract.registry import SOURCE_ID_BY_DIR
from compsval.ingest.binary_snapshot import (
    BINARY_FILENAME,
    PROVENANCE_FILENAME,
    attach_binary_provenance,
    infer_mime_type,
    list_binary_snapshots,
    raw_snapshot_from_binary,
    read_binary_provenance,
    write_binary_snapshot,
)
from compsval.ingest.manifests import SnapshotManifest, read_manifest

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_FETCHED_AT = datetime(2026, 8, 24, 0, 0, 0, tzinfo=UTC)


def _make_raw_file(tmp_path: Path, name: str = "原始数据.xlsx") -> Path:
    """真实 XLSX（含成交日期列）——`compsval ingest binary` 的 XLSX 探针需可解析。"""
    from datetime import datetime as _dt

    from openpyxl import Workbook

    path = tmp_path / name
    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "Sheet1"
    ws.append(["房屋ID", "成交日期", "成交总价"])
    ws.append(["108404666013", _dt(2023, 12, 17), "700000"])
    ws.append(["108404909781", _dt(2023, 12, 18), "175000"])
    ws.append(["108405043374", _dt(2023, 11, 13), "165000"])
    wb.save(path)
    return path


def test_fetched_at_later_than_completed_at_rejected(tmp_path: Path) -> None:
    """CX-EXTFP1-008：取得时间不得晚于登记/完成时间（拒绝时序失真）。"""
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    with pytest.raises(ValueError, match="must not be later"):
        write_binary_snapshot(
            src, source="lianjia_ext", dataset="chengjiao_xlsx",
            fetched_at=datetime(2026, 8, 25, 3, 0, 0, tzinfo=UTC),
            completed_at=datetime(2026, 8, 25, 2, 0, 0, tzinfo=UTC),
            query="q", root=lake,
        )
    # 未创建任何残留目录
    assert not (lake / "raw").exists()


def test_attach_and_read_provenance(tmp_path: Path) -> None:
    """CX-EXTFP1-008：附加不可变 provenance 记录（不改写 manifest/data.bin）。"""
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    result = write_binary_snapshot(
        src, source="lianjia_ext", dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT, query="q", root=lake,
    )
    assert read_binary_provenance(result.directory) is None  # 初始无附加记录

    attached = attach_binary_provenance(
        result.directory,
        original_filename=src.name,
        sheet_names=["Sheet1"],
        sheet_metadata=[{"sheet": "Sheet1", "rows": 3, "columns": 3,
                         "headers": ["房屋ID", "成交日期", "成交总价"]}],
        source_row_count=3,
        column_count=3,
        verification_ref="画像报告.json",
        parser_version="EXTFP1-C-1.0",
        fetched_at_source="user-provided 2026-08-24",
    )
    assert attached.name == PROVENANCE_FILENAME
    provenance = read_binary_provenance(result.directory)
    assert provenance is not None
    assert provenance["original_filename"] == "原始数据.xlsx"
    assert provenance["sheet_metadata"][0]["columns"] == 3
    assert provenance["fetched_at_source"] == "user-provided 2026-08-24"
    # 原 manifest 与 data.bin 未改写
    reloaded = read_manifest(result.directory)
    assert reloaded.files[0].sha256 == result.manifest.files[0].sha256
    # 重复附加拒绝（不可变）
    with pytest.raises(FileExistsError):
        attach_binary_provenance(
            result.directory,
            original_filename=src.name,
            sheet_names=["Sheet1"],
            sheet_metadata=[],
            source_row_count=3,
            column_count=3,
        )
    # 无 .incomplete 残留
    assert not attached.with_name(PROVENANCE_FILENAME + ".incomplete").exists()


def test_duplicate_import_leaves_no_incomplete(tmp_path: Path) -> None:
    """CX-EXTFP1-004 修复：重复导入先查 final，不得留下无效 .incomplete 目录。"""
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        src, source="lianjia_ext", dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT, query="q", root=lake,
    )
    with pytest.raises(FileExistsError):
        write_binary_snapshot(
            src, source="lianjia_ext", dataset="chengjiao_xlsx",
            fetched_at=_FETCHED_AT, query="q", root=lake,
        )
    # 无 .incomplete 残留
    stamp = _FETCHED_AT.strftime("%Y%m%dT%H%M%SZ")
    incomplete = (
        lake / "raw" / "source=lianjia_ext" / "dataset=chengjiao_xlsx"
        / f"fetched_at={stamp}.incomplete"
    )
    assert not incomplete.exists()


def test_binary_manifest_provenance(tmp_path: Path) -> None:
    """CX-EXTFP1-002 修复：原始文件名/工作表/日期范围/解析器版本落盘。"""
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    result = write_binary_snapshot(
        src, source="lianjia_ext", dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT, query="q", root=lake,
        original_filename=src.name,
        sheet_names=["Sheet1"],
        sheet_metadata=[{"sheet": "Sheet1", "rows": 3, "columns": 3,
                         "headers": ["房屋ID", "成交日期", "成交总价"]}],
        source_row_count=3,
        column_count=3,
        data_date_min="2023-11-13",
        data_date_max="2023-12-18",
        verification_ref="画像报告.json",
        parser_version="EXTFP1-C-1.0",
        prev_snapshot_id="lianjia_ext-chengjiao_xlsx-20260824T000000Z",
    )
    reloaded = read_manifest(result.directory)
    assert reloaded.original_filename == "原始数据.xlsx"
    assert reloaded.sheet_names == ["Sheet1"]
    assert reloaded.sheet_metadata == [
        {"sheet": "Sheet1", "rows": 3, "columns": 3,
         "headers": ["房屋ID", "成交日期", "成交总价"]}
    ]  # CX-EXTFP1-006-R：每工作表行列数/表头落盘
    assert reloaded.source_row_count == 3  # CX-EXTFP1-006：行数落盘（§5.2）
    assert reloaded.column_count == 3  # CX-EXTFP1-006：列数落盘（§5.2）
    assert reloaded.prev_snapshot_id == "lianjia_ext-chengjiao_xlsx-20260824T000000Z"
    assert reloaded.data_date_min == "2023-11-13"
    assert reloaded.data_date_max == "2023-12-18"
    assert reloaded.verification_ref == "画像报告.json"
    assert reloaded.parser_version == "EXTFP1-C-1.0"


def test_infer_mime_type() -> None:
    assert infer_mime_type(Path("a.xlsx")) == XLSX_MIME
    assert infer_mime_type(Path("a.XLSX")) == XLSX_MIME  # 大小写不敏感
    assert infer_mime_type(Path("a.png")) == "image/png"
    assert infer_mime_type(Path("a.unknown-ext")) is None  # 未知后缀保持未知


def test_write_binary_snapshot_preserves_bytes_and_manifest(tmp_path: Path) -> None:
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    result = write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query=f"local binary import: {src.resolve()}",
        root=lake,
    )

    # 目录布局：raw/source=.../dataset=.../fetched_at=<stamp>/data.bin + manifest.json
    assert result.directory.name == f"fetched_at={_FETCHED_AT.strftime('%Y%m%dT%H%M%SZ')}"
    assert result.data_path.name == BINARY_FILENAME
    assert result.data_path.read_bytes() == src.read_bytes()  # 字节守恒
    assert result.manifest.files[0].path == BINARY_FILENAME
    assert result.manifest.files[0].size_bytes == src.stat().st_size
    assert result.manifest.files[0].sha256 != ""  # sha256 已计算

    # manifest 可重读，mime_type 已落盘（EXTFP1-B 补填）
    reloaded = read_manifest(result.directory)
    assert reloaded.mime_type == XLSX_MIME
    assert reloaded.row_count == 0  # 二进制未逐行解析

    # 原子写：无 .incomplete 残留
    assert not result.directory.with_name(result.directory.name + ".incomplete").exists()


def test_raw_snapshot_registration_with_mime_type(tmp_path: Path) -> None:
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    result = write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query="q",
        root=lake,
    )
    r: RawSnapshot = result.raw_snapshot
    assert r.snapshot_id == f"lianjia_ext-chengjiao_xlsx-{_FETCHED_AT.strftime('%Y%m%dT%H%M%SZ')}"
    assert r.source_id == SOURCE_ID_BY_DIR["lianjia_ext"] == "SRC-011"
    assert r.dataset == "chengjiao_xlsx"
    assert r.format is SnapshotFormat.BINARY
    assert r.mime_type == XLSX_MIME  # RV-EXTFP0-F-01#F2 补填
    assert r.parse_status is SnapshotParseStatus.NOT_PARSED
    assert r.record_count == 0
    assert r.content_hash == result.manifest.files[0].sha256

    # 从目录可重新生成同一登记（扫描模式，不持久化）
    regenerated = raw_snapshot_from_binary(
        result.directory, source_id=SOURCE_ID_BY_DIR["lianjia_ext"]
    )
    assert regenerated.model_dump() == r.model_dump()


def test_binary_snapshot_visible_in_catalog(tmp_path: Path) -> None:
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query="q",
        root=lake,
    )
    (ref,) = list_snapshots(lake)
    assert ref.source == "lianjia_ext"
    assert ref.dataset == "chengjiao_xlsx"
    assert ref.data_path.name == BINARY_FILENAME  # 无 data.parquet 时指向 data.bin
    assert ref.data_path.is_file()

    (snap,) = list_binary_snapshots(lake)
    assert snap.format is SnapshotFormat.BINARY
    assert snap.mime_type == XLSX_MIME


def test_connect_ignores_binary_snapshot_parquet_read(tmp_path: Path) -> None:
    """RV-EXTFP1-B-01#F1 修复：含二进制快照的湖 connect()/compsval sql 不得把 data.bin 当 parquet 读。"""
    from compsval import catalog

    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query="q",
        root=lake,
    )
    con = catalog.connect(lake)
    try:
        assert con.sql("SELECT 1 AS x").fetchone() == (1,)
    finally:
        con.close()

    # CLI compsval sql 在同一湖上可用（不被 data.bin 破坏）
    assert cli.main(["sql", "SELECT 1 AS x", "--data-dir", str(lake)]) == 0


def test_single_snapshot_semantics(tmp_path: Path) -> None:
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query="q",
        root=lake,
    )
    with pytest.raises(FileExistsError):
        write_binary_snapshot(
            src,
            source="lianjia_ext",
            dataset="chengjiao_xlsx",
            fetched_at=_FETCHED_AT,
            query="q",
            root=lake,
        )


def test_manifest_schema_backward_compatible_no_mime_type(tmp_path: Path) -> None:
    """EXTFP1-B 合同扩展不破坏旧 manifest：无 mime_type 字段可反序列化且默认 None。"""
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    write_binary_snapshot(
        src,
        source="lianjia_ext",
        dataset="chengjiao_xlsx",
        fetched_at=_FETCHED_AT,
        query="q",
        root=lake,
    )
    manifest_path = (
        lake
        / "raw"
        / "source=lianjia_ext"
        / "dataset=chengjiao_xlsx"
        / f"fetched_at={_FETCHED_AT.strftime('%Y%m%dT%H%M%SZ')}"
        / "manifest.json"
    )
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    data.pop("mime_type", None)  # 模拟旧 manifest（EXTFP1-B 之前无此字段）
    manifest_path.write_text(json.dumps(data), encoding="utf-8")

    reloaded = SnapshotManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    assert reloaded.mime_type is None

    r = raw_snapshot_from_binary(manifest_path.parent, source_id="SRC-011")
    assert r.mime_type is None  # 旧 manifest → mime 保持未知，不猜测


def test_cli_ingest_binary_success_and_catalog(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    src = _make_raw_file(tmp_path)
    lake = tmp_path / "lake"
    assert (
        cli.main(
            [
                "ingest", "binary",
                "--input", str(src),
                "--source", "SRC-011",
                "--dataset", "chengjiao_xlsx",
                "--fetched-at", "20260824",
                "--data-dir", str(lake),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "imported" in out and "bytes" in out
    assert "mime_type=" + XLSX_MIME in out  # 推断的 MIME 已打印
    assert "format=binary" in out

    assert cli.main(["catalog", "--data-dir", str(lake)]) == 0
    catalog_out = capsys.readouterr().out
    assert "lianjia_ext/chengjiao_xlsx@" in catalog_out


def test_cli_ingest_binary_explicit_mime_and_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    src = tmp_path / "raw.unknown"
    src.write_bytes(b"abc")
    lake = tmp_path / "lake"
    # 显式 --mime-type 覆盖推断
    assert (
        cli.main(
            [
                "ingest", "binary",
                "--input", str(src),
                "--source", "lianjia_ext",
                "--dataset", "d",
                "--fetched-at", "20260824",
                "--mime-type", "application/octet-stream",
                "--data-dir", str(lake),
            ]
        )
        == 0
    )
    assert "mime_type=application/octet-stream" in capsys.readouterr().out

    # 未知 source 失败
    assert (
        cli.main(
            [
                "ingest", "binary",
                "--input", str(src),
                "--source", "nope",
                "--dataset", "d",
                "--fetched-at", "20260824",
                "--data-dir", str(lake),
            ]
        )
        == 1
    )
    assert "unknown --source" in capsys.readouterr().out

    # 输入不存在失败
    assert (
        cli.main(
            [
                "ingest", "binary",
                "--input", str(tmp_path / "missing.bin"),
                "--source", "lianjia_ext",
                "--dataset", "d",
                "--fetched-at", "20260824",
                "--data-dir", str(lake),
            ]
        )
        == 1
    )
    assert "input not found" in capsys.readouterr().out

    # 重复导入失败（单快照语义）
    assert (
        cli.main(
            [
                "ingest", "binary",
                "--input", str(src),
                "--source", "lianjia_ext",
                "--dataset", "d",
                "--fetched-at", "20260824",
                "--mime-type", "application/octet-stream",
                "--data-dir", str(lake),
            ]
        )
        == 1
    )
    assert "snapshot already exists" in capsys.readouterr().out
