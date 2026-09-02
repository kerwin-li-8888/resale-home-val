"""EXTFP2-E 户型图 10 张试跑链路（floorplan_e2e）的离线测试。

全部用例离线：样本只读登记、子集清单重建、下载质量报告均在本地 mock 完成，
绝不发放真实 HTTP。覆盖：样本清单解析与大小校验、跨 URL 域白名单与全量清单定位、
子集幂等重建（record_ids_hash 重算）、样本 SHA256 双向比对（一致/不一致/未定位）、
JSON 与 Markdown 同一冻结报告。
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from typing import Any

import httpx
from PIL import Image

from compsval.ingest.floorplan_asset import (
    build_asset_manifest,
)
from compsval.ingest.floorplan_download import (
    run_download,
)
from compsval.ingest.floorplan_e2e import (
    SUBSET_FILENAME,
    build_download_quality_report,
    build_subset_manifest,
    parse_sample_list,
    register_samples,
)
from compsval.ingest.floorplan_selection import (
    SelectionEntry,
    SelectionManifest,
)

DOMAIN = "ke-image.ljcdn.com"
S1_URL = f"http://{DOMAIN}/hdic-frame/s1.jpg?from=ke.com"
S2_URL = f"http://{DOMAIN}/hdic-frame/s2.jpg?from=ke.com"

MANIFEST_HASH = "recids-hash-EXTFP2E-0123456789abcdef"


def _jpeg_bytes(width: int = 12, height: int = 9) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), (200, 30, 30)).save(buf, format="JPEG")
    return buf.getvalue()


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _entry(rid: str, row: int, seq: int, http_url: str) -> SelectionEntry:
    from compsval.ingest.floorplan_selection import _normalize_https

    return SelectionEntry(
        source_record_id=rid,
        row_number=row,
        url_seq=seq,
        url=http_url,
        normalized_url=_normalize_https(http_url),
        domain=DOMAIN,
    )


def _manifest(entries: list[SelectionEntry]) -> SelectionManifest:
    return SelectionManifest(
        selection_rule_version="EXTFP2-B-SELECT-1.0",
        selection_rule_text="test",
        snapshot_ref="snap-1",
        run_id="run-extfp1-latest",
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


def _sample_dir(tmp_path: Path, spec: dict[str, bytes]) -> Path:
    """构造样本目录：写 huxingtu_0N.jpg + 样本来源清单.md，返回目录。"""
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)
    rows = ["| 文件 | 大小(bytes) | 来源 URL |", "|---|---|---|"]
    for fname, b in spec.items():
        (d / fname).write_bytes(b)
        rows.append(f"| {fname} | {len(b)} | {S1_URL if fname.endswith('01.jpg') else S2_URL} |")
    (d / "样本来源清单.md").write_text("\n".join(rows) + "\n", encoding="utf-8")
    return d


# ---------------------------------------------------------------------------
# 1) 样本清单解析（只读）与文件校验
# ---------------------------------------------------------------------------


def test_parse_sample_list_computes_sha256(tmp_path: Path) -> None:
    j1, j2 = _jpeg_bytes(), _jpeg_bytes(20, 14)
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1, "huxingtu_02.jpg": j2})
    files = parse_sample_list(d, d / "样本来源清单.md")

    assert [f.filename for f in files] == ["huxingtu_01.jpg", "huxingtu_02.jpg"]
    assert files[0].local_sha256 == _sha(j1)
    assert files[1].local_sha256 == _sha(j2)
    # 只读：样本文件未被改写
    assert (d / "huxingtu_01.jpg").read_bytes() == j1


def test_parse_sample_list_missing_file_fails(tmp_path: Path) -> None:
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": _jpeg_bytes()})
    (d / "huxingtu_01.jpg").unlink()
    import pytest

    with pytest.raises(FileNotFoundError):
        parse_sample_list(d, d / "样本来源清单.md")


def test_parse_sample_list_size_mismatch_fails(tmp_path: Path) -> None:
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": _jpeg_bytes()})
    (d / "huxingtu_01.jpg").write_bytes(b"x")  # 大小与清单不符
    import pytest

    with pytest.raises(ValueError):
        parse_sample_list(d, d / "样本来源清单.md")


# ---------------------------------------------------------------------------
# 2) 样本只读登记：白名单 + 全量清单定位
# ---------------------------------------------------------------------------


def test_register_samples_maps_manifest_location(tmp_path: Path) -> None:
    j1, j2 = _jpeg_bytes(), _jpeg_bytes(20, 14)
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1, "huxingtu_02.jpg": j2})
    files = parse_sample_list(d, d / "样本来源清单.md")
    manifest = _manifest([_entry("RS1", 1, 1, S1_URL), _entry("RS2", 2, 1, S2_URL)])

    reg = register_samples(manifest, files, created_at="2026-08-25T00:00:00Z")

    assert reg.total_files == 2
    assert reg.not_found_in_manifest == []
    by_file = {s["file"]: s for s in reg.samples}
    assert by_file["huxingtu_01.jpg"]["whitelisted"] is True
    assert by_file["huxingtu_01.jpg"]["in_manifest"] is True
    assert by_file["huxingtu_01.jpg"]["source_record_id"] == "RS1"
    assert by_file["huxingtu_01.jpg"]["row_number"] == 1
    assert by_file["huxingtu_01.jpg"]["url_seq"] == 1
    assert by_file["huxingtu_02.jpg"]["source_record_id"] == "RS2"


def test_register_samples_not_found_and_off_whitelist(tmp_path: Path) -> None:
    j1 = _jpeg_bytes()
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)
    (d / "huxingtu_01.jpg").write_bytes(j1)
    (d / "样本来源清单.md").write_text(
        "| 文件 | 大小(bytes) | 来源 URL |\n|---|---|---|\n"
        f"| huxingtu_01.jpg | {len(j1)} | http://evil.example.com/a.jpg?from=ke.com |\n",
        encoding="utf-8",
    )
    files = parse_sample_list(d, d / "样本来源清单.md")
    manifest = _manifest([_entry("RS1", 1, 1, S1_URL)])

    reg = register_samples(manifest, files)

    s = reg.samples[0]
    assert s["domain"] == "evil.example.com"
    assert s["whitelisted"] is False
    assert s["in_manifest"] is False
    assert s["source_record_id"] is None
    assert reg.not_found_in_manifest == ["huxingtu_01.jpg"]


# ---------------------------------------------------------------------------
# 3) 子集清单重建：幂等、重算哈希/计数/估算
# ---------------------------------------------------------------------------


def test_build_subset_manifest_picks_and_rehash(tmp_path: Path) -> None:
    j1, j2, j3 = _jpeg_bytes(), _jpeg_bytes(20, 14), _jpeg_bytes(9, 9)
    d = _sample_dir(
        tmp_path,
        {"huxingtu_01.jpg": j1, "huxingtu_02.jpg": j2, "huxingtu_03.jpg": j3},
    )
    files = parse_sample_list(d, d / "样本来源清单.md")
    # 全量含 4 张（样本 2 张命中 + 2 张未命中）
    full = _manifest(
        [
            _entry("RS1", 1, 1, S1_URL),
            _entry("RS2", 2, 1, S2_URL),
            _entry("RX1", 3, 1, f"http://{DOMAIN}/others/x.jpg?from=ke.com"),
            _entry("RX2", 4, 1, f"http://{DOMAIN}/others/y.jpg?from=ke.com"),
        ]
    )
    # 只登记前 2 张样本对应 URL（第三张样本 URL 只出现一次，无法命中两条同 URL 记录）
    out = tmp_path / "e2e" / SUBSET_FILENAME
    subset = build_subset_manifest(full, files[:2], out, expected_count=2)

    assert subset.asset_count == 2
    assert [e.source_record_id for e in subset.records] == ["RS1", "RS2"]
    assert subset.record_count == 2
    assert subset.estimated_download_bytes == 2 * 70 * 1024
    assert subset.storage_cap_bytes == int(2 * 70 * 1024 * 2.0)
    assert out.is_file()
    # *.incomplete 中间态已原子替换（不残留）
    assert not out.with_suffix(out.suffix + ".incomplete").exists()
    # record_ids_hash 按排序后 source_record_id 重算
    expected = hashlib.sha256("\n".join(sorted(["RS1", "RS2"])).encode("utf-8")).hexdigest()
    assert subset.record_ids_hash == expected


def test_build_subset_manifest_idempotent(tmp_path: Path) -> None:
    j1 = _jpeg_bytes()
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1})
    files = parse_sample_list(d, d / "样本来源清单.md")
    full = _manifest([_entry("RS1", 1, 1, S1_URL)])
    out = tmp_path / "e2e" / SUBSET_FILENAME

    a = build_subset_manifest(full, files, out, expected_count=1)
    b = build_subset_manifest(full, files, out, expected_count=1)  # 重跑

    assert a.record_ids_hash == b.record_ids_hash
    assert [e.source_record_id for e in a.records] == [e.source_record_id for e in b.records]


def test_build_subset_manifest_supplements_to_expected_count(tmp_path: Path) -> None:
    """CX-EXTFP2-01 §5：seed 命中不足 expected_count 时按稳定顺序 (row_number,url_seq) 补足。"""
    j1, j2, j3 = _jpeg_bytes(), _jpeg_bytes(20, 14), _jpeg_bytes(9, 9)
    # 3 个样本：01→S1、02→S2、03→S2（后两者同 URL，只命中一条）
    d = _sample_dir(
        tmp_path,
        {"huxingtu_01.jpg": j1, "huxingtu_02.jpg": j2, "huxingtu_03.jpg": j3},
    )
    files = parse_sample_list(d, d / "样本来源清单.md")
    full = _manifest(
        [_entry("RS1", 1, 1, S1_URL), _entry("RS2", 2, 1, S2_URL)]
        + [
            _entry(f"RX{i}", 2 + i, 1, f"http://{DOMAIN}/others/x{i}.jpg?from=ke.com")
            for i in range(1, 13)
        ]
    )
    out = tmp_path / "e2e" / SUBSET_FILENAME

    subset = build_subset_manifest(full, files, out, expected_count=10)

    # seed 命中 RS1、RS2；其余按 (row_number,url_seq) 补选前 8 条，恰好补足 10
    assert subset.asset_count == 10
    rids = [e.source_record_id for e in subset.records]
    assert rids[:2] == ["RS1", "RS2"]
    assert rids[2:] == [f"RX{i}" for i in range(1, 9)]
    assert subset.record_count == 10
    # 补选不产生越权域名：仍在白名单内
    assert all(e.domain == DOMAIN for e in subset.records)


def test_build_subset_manifest_fail_closed_when_insufficient(tmp_path: Path) -> None:
    """CX-EXTFP2-01 §5 fail-closed：seed 命中 + 补选仍不足 expected_count 时禁止子集清单。"""
    import pytest

    j1 = _jpeg_bytes()
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1})
    files = parse_sample_list(d, d / "样本来源清单.md")
    # 全量仅 3 条：1 seed 命中 + 2 候选，补足也只能到 3 < 10
    full = _manifest(
        [
            _entry("RS1", 1, 1, S1_URL),
            _entry("RX1", 2, 1, f"http://{DOMAIN}/others/x1.jpg?from=ke.com"),
            _entry("RX2", 3, 1, f"http://{DOMAIN}/others/x2.jpg?from=ke.com"),
        ]
    )
    out = tmp_path / "e2e" / SUBSET_FILENAME

    with pytest.raises(ValueError, match="expected_count=10"):
        build_subset_manifest(full, files, out, expected_count=10)
    # 且不得残留半成品清单（fail-closed 不写盘）
    assert not out.exists()
    assert not out.with_suffix(out.suffix + ".incomplete").exists()


# ---------------------------------------------------------------------------
# 4) 下载质量报告：样本 SHA256 双向比对 + JSON/MD 同一冻结报告
# ---------------------------------------------------------------------------


def _mock_transport(by_url: dict[str, bytes], status: int = 200) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url not in by_url:
            return httpx.Response(404, content=b"", request=request)
        return httpx.Response(status, content=by_url[url], request=request)

    return httpx.MockTransport(handler)


def _e2e_bundle(
    full: SelectionManifest,
    files: list[Any],
    tmp_path: Path,
    region: dict[str, bytes],
) -> dict[str, Any]:
    """离线跑完整链路，返回 build_download_quality_report 的 JSON 报告。"""
    subset_path = tmp_path / "e2e" / SUBSET_FILENAME
    subset = build_subset_manifest(full, files, subset_path, expected_count=len(full.records))

    from datetime import UTC, datetime

    dl_dir = tmp_path / "dl"
    dl_record = run_download(
        subset,
        dl_dir,
        transport=_mock_transport(region),
        base_backoff=0.0,
    )
    dl_run_dir = Path(dl_record.run_dir)
    raw_dir = tmp_path / "raw"
    asset_run = build_asset_manifest(dl_run_dir, raw_dir, batch_id="e2e-batch")
    asset_batch_dir = raw_dir / "batch_id=e2e-batch"

    from compsval.ingest.floorplan_e2e import E2eBundle

    bundle = E2eBundle(
        created_at=datetime.now(UTC).isoformat(),
        selection_ref=str(full.selection_rule_version),
        selection_rule_version=full.selection_rule_version,
        subset_path=str(subset_path),
        subset_asset_count=subset.asset_count,
        download_run_id=dl_record.run_id,
        downloader_version="EXTFP2-C-DL-1.0",
        download_state_counts=dict(dl_record.state_counts),
        asset_batch_id=asset_run.batch_id,
        asset_rules_version=asset_run.rules_version,
        asset_counts=dict(asset_run.counts),
        domain_whitelist=[DOMAIN],
    )
    registration = register_samples(full, files, created_at="2026-08-25T00:00:00Z")
    manifest_dict = json.loads(
        (asset_batch_dir / "floorplan_asset_manifest.json").read_text(encoding="utf-8")
    )
    qrep_json_path, qrep_md_path = build_download_quality_report(
        bundle,
        sample_registration=registration,
        asset_manifests=[manifest_dict],
        local_files=files,
        out_dir=tmp_path / "report",
    )
    report = json.loads(qrep_json_path.read_text(encoding="utf-8"))
    assert qrep_md_path.is_file()
    return {"report": report, "json": qrep_json_path, "md": qrep_md_path}


def test_quality_report_sample_sha256_match(tmp_path: Path) -> None:
    from compsval.ingest.floorplan_selection import _normalize_https

    j1 = _jpeg_bytes()
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1})
    files = parse_sample_list(d, d / "样本来源清单.md")
    full = _manifest([_entry("RS1", 1, 1, S1_URL)])

    # mock 返回与本地样本完全一致的字节 → MANCH
    result = _e2e_bundle(full, files, tmp_path, {_normalize_https(S1_URL): j1})["report"]
    sc = result["sample_comparison"]
    assert sc["total_files"] == 1
    assert sc["sha256_match"] == 1
    assert sc["sha256_mismatch"] == 0
    assert sc["not_available"] == 0
    assert sc["unmatched_files"] == []
    assert sc["rows"][0]["match"] == "MATCH"
    assert sc["rows"][0]["local_sha256"] == _sha(j1)[:12]


def test_quality_report_mismatch_detected(tmp_path: Path) -> None:
    """下载字节与本地样本 SHA256 不一致 → MISMATCH（双向验证的失败路径）。"""
    local = _jpeg_bytes(10, 8)
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": local})
    files = parse_sample_list(d, d / "样本来源清单.md")
    full = _manifest([_entry("RS1", 1, 1, S1_URL)])
    # 覆盖 mock 响应，让它返回不同字节 → 下载哈希 != 本地样本哈希
    subset_path = tmp_path / "e2e" / SUBSET_FILENAME
    subset = build_subset_manifest(full, files, subset_path, expected_count=len(full.records))
    dl_dir = tmp_path / "dl"
    dl_record = run_download(
        subset,
        dl_dir,
        transport=_mock_transport({subset.records[0].normalized_url: _jpeg_bytes(5, 5)}),
        base_backoff=0.0,
    )
    dl_run_dir = Path(dl_record.run_dir)
    raw_dir = tmp_path / "raw"
    asset_run = build_asset_manifest(dl_run_dir, raw_dir, batch_id="e2e-mm")
    asset_batch_dir = raw_dir / "batch_id=e2e-mm"

    from datetime import UTC, datetime

    from compsval.ingest.floorplan_e2e import E2eBundle

    bundle = E2eBundle(
        created_at=datetime.now(UTC).isoformat(),
        selection_ref=str(full.selection_rule_version),
        selection_rule_version=full.selection_rule_version,
        subset_path=str(subset_path),
        subset_asset_count=subset.asset_count,
        download_run_id=dl_record.run_id,
        downloader_version="EXTFP2-C-DL-1.0",
        download_state_counts=dict(dl_record.state_counts),
        asset_batch_id=asset_run.batch_id,
        asset_rules_version=asset_run.rules_version,
        asset_counts=dict(asset_run.counts),
        domain_whitelist=[DOMAIN],
    )
    registration = register_samples(full, files, created_at="2026-08-25T00:00:00Z")
    manifest_dict = json.loads(
        (asset_batch_dir / "floorplan_asset_manifest.json").read_text(encoding="utf-8")
    )
    qrep_json_path, _ = build_download_quality_report(
        bundle,
        sample_registration=registration,
        asset_manifests=[manifest_dict],
        local_files=files,
        out_dir=tmp_path / "report-mm",
    )
    report = json.loads(qrep_json_path.read_text(encoding="utf-8"))
    sc = report["sample_comparison"]
    assert sc["sha256_match"] == 0
    assert sc["sha256_mismatch"] == 1
    assert sc["rows"][0]["match"] == "MISMATCH"


def test_quality_report_json_and_md_same_frozen(tmp_path: Path) -> None:
    from compsval.ingest.floorplan_selection import _normalize_https

    j1 = _jpeg_bytes()
    d = _sample_dir(tmp_path, {"huxingtu_01.jpg": j1})
    files = parse_sample_list(d, d / "样本来源清单.md")
    full = _manifest([_entry("RS1", 1, 1, S1_URL)])
    result = _e2e_bundle(full, files, tmp_path, {_normalize_https(S1_URL): j1})

    md_text = result["md"].read_text(encoding="utf-8")
    # 同一规则版本冻结：MD 引用同一 QREP 规则版本与创建时刻
    assert "EXTFP2-E-QREP-1.0" in md_text
    assert "1 张" in md_text  # 总计渲染
    # 资产明细与样本比对都出现在 MD（与 JSON 同一冻结源）
    assert "样本 SHA256 双向比对" in md_text


# ---------------------------------------------------------------------------
# 5) 增量补下（CX-EXTFP2-01 §5）：只补缺失 + 累计计数/多 batch 报告聚合
# ---------------------------------------------------------------------------


def test_resolve_missing_assets_and_download_manifest(tmp_path: Path) -> None:
    from compsval.ingest.floorplan_download import (
        compute_asset_id,
        sale_record_key,
    )
    from compsval.ingest.floorplan_e2e import (
        build_download_manifest,
        resolve_missing_assets,
    )

    full = _manifest(
        [
            _entry("RS1", 1, 1, S1_URL),
            _entry("RS2", 2, 1, S2_URL),
            _entry("RX1", 3, 1, f"http://{DOMAIN}/others/x1.jpg?from=ke.com"),
            _entry("RX2", 4, 1, f"http://{DOMAIN}/others/x2.jpg?from=ke.com"),
        ]
    )
    # 模拟 RS1/RS2 已落盘（snapshot_ref = snap-1 的幂等 asset_id）
    existing = {
        compute_asset_id(sale_record_key("snap-1", row, rid), 1)
        for rid, row in (("RS1", 1), ("RS2", 2))
    }
    missing = resolve_missing_assets(full.records, "snap-1", existing)
    assert [e.source_record_id for e in missing] == ["RX1", "RX2"]

    dl = build_download_manifest(full, missing)
    assert dl.asset_count == 2
    assert [e.source_record_id for e in dl.records] == ["RX1", "RX2"]


def test_collect_existing_downloaded_asset_ids(tmp_path: Path) -> None:
    from compsval.ingest.floorplan_e2e import (
        collect_existing_downloaded_asset_ids,
    )

    raw = tmp_path / "raw"
    b = raw / "batch_id=b1"
    b.mkdir(parents=True)
    (b / "floorplan_asset_manifest.json").write_text(
        json.dumps(
            {
                "batch_id": "b1",
                "assets": [
                    {"asset_id": "a1", "asset_status": "DOWNLOADED"},
                    {"asset_id": "a2", "asset_status": "IMAGE_INVALID"},
                ],
            }
        ),
        encoding="utf-8",
    )
    ids = collect_existing_downloaded_asset_ids(raw)
    assert ids == {"a1"}


def test_aggregate_bundle_counts_cumulative() -> None:
    from compsval.ingest.floorplan_e2e import aggregate_bundle_counts

    m1 = {"assets": [{"asset_status": "DOWNLOADED"}, {"asset_status": "IMAGE_INVALID"}]}
    m2 = {"assets": [{"asset_status": "DOWNLOADED"}]}
    download_counts, asset_counts = aggregate_bundle_counts([m1, m2])
    assert download_counts == {"DOWNLOADED": 2}
    assert asset_counts == {"DOWNLOADED": 2, "IMAGE_INVALID": 1, "NOT_AVAILABLE": 0}


def test_quality_report_aggregates_multiple_batches(tmp_path: Path) -> None:
    """累计质量报告：跨多个 batch manifest 平铺资产、聚合计数，MD/JSON 同源。"""
    from datetime import UTC, datetime

    from compsval.ingest.floorplan_e2e import (
        E2eBundle,
        SampleRegistration,
        build_download_quality_report,
    )

    m1 = {
        "batch_id": "b1",
        "rules_version": "EXTFP2-D-ASSET-1.0",
        "assets": [
            {
                "source_record_id": "RS1",
                "url_ordinal": 1,
                "asset_id": "a1",
                "asset_status": "DOWNLOADED",
                "mime_type": "image/jpeg",
                "file_extension": ".jpg",
                "width": 12,
                "height": 9,
                "byte_size": 100,
                "sha256": "0123456789abcdef",
                "is_duplicate": False,
                "storage_path": "a1.jpg",
            }
        ],
    }
    m2 = {
        "batch_id": "b2",
        "rules_version": "EXTFP2-D-ASSET-1.0",
        "assets": [
            {
                "source_record_id": "RX1",
                "url_ordinal": 1,
                "asset_id": "a2",
                "asset_status": "DOWNLOADED",
                "mime_type": "image/jpeg",
                "file_extension": ".jpg",
                "width": 12,
                "height": 9,
                "byte_size": 120,
                "sha256": "fedcba9876543210",
                "is_duplicate": False,
                "storage_path": "a2.jpg",
            }
        ],
    }
    registration = SampleRegistration(
        created_at="2026-08-25T00:00:00Z",
        source="test",
        domain_whitelist=[DOMAIN],
        total_files=0,
        samples=[],
    )
    bundle = E2eBundle(
        created_at=datetime.now(UTC).isoformat(),
        selection_ref="full-manifest",
        selection_rule_version="EXTFP2-B-SELECT-1.0",
        subset_path="subset.json",
        subset_asset_count=2,
        expected_count=2,
        download_run_id="run-b2",
        downloader_version="EXTFP2-C-DL-1.0",
        download_state_counts={"DOWNLOADED": 2},
        asset_batch_id="b2",
        asset_rules_version="EXTFP2-D-ASSET-1.0",
        asset_counts={"DOWNLOADED": 2, "IMAGE_INVALID": 0, "NOT_AVAILABLE": 0},
        domain_whitelist=[DOMAIN],
        aggregated_download_run_ids=["run-b1", "run-b2"],
        aggregated_batch_ids=["b1", "b2"],
    )
    qrep_json_path, qrep_md_path = build_download_quality_report(
        bundle,
        sample_registration=registration,
        asset_manifests=[m1, m2],
        local_files=[],
        out_dir=tmp_path / "report-cum",
    )
    report = json.loads(qrep_json_path.read_text(encoding="utf-8"))
    assert len(report["assets"]) == 2
    assert {"b1", "b2"} <= {r["batch_id"] for r in report["assets"]}
    assert report["aggregated_batch_ids"] == ["b1", "b2"]
    assert report["download_state_counts"] == {"DOWNLOADED": 2}
    md_text = qrep_md_path.read_text(encoding="utf-8")
    assert "expected_count=2" in md_text
    assert "run-b1" in md_text and "b1" in md_text
