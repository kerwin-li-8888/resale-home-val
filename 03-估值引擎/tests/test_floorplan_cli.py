"""EXTFP2-F：compsval floorplan CLI 异常分支离线测试。

补齐 RV-EXTFP2-B-01#F1（select/download 的 CLI 异常分支）与
RV-EXTFP2-E-01#F3（e2e 的编排分支，含 ``--sample-list`` 缺省回退路径）的
离线覆盖。全部用例走 ``cli.main`` 真实分发，不触网、不访问真实数据：
select 覆盖未知 profile / 缺 current.json / 缺 staged 表 / 异常构建 / 正常产出；
download 覆盖缺 selection / 参数非法 / 坏 manifest / 白名单零请求；
asset 覆盖缺 run 目录 / 缺下载状态 / 正常通路写 staged 表；
e2e 覆盖缺 selection / 缺样本目录 / 缺样本清单(缺省回退路径) / 参数非法 /
坏 manifest / 子集空停止。
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from compsval import cli
from compsval.ingest.floorplan_profile import UrlListStatus
from compsval.ingest.floorplan_selection import (
    SelectionEntry,
    SelectionManifest,
)

DOMAIN = "ke-image.ljcdn.com"
URL_A = f"http://{DOMAIN}/hdic-frame/a.jpg?from=ke.com"
URL_B = f"http://{DOMAIN}/hdic-frame/b.jpg?from=ke.com"
EVIL_URL = "http://evil.example.com/8.jpg?from=ke.com"


def _entry(rid: str, row: int, seq: int, url: str, domain: str = DOMAIN) -> SelectionEntry:
    from compsval.ingest.floorplan_selection import _normalize_https

    return SelectionEntry(
        source_record_id=rid,
        row_number=row,
        url_seq=seq,
        url=url,
        normalized_url=_normalize_https(url),
        domain=domain,
    )


def _manifest(entries: list[SelectionEntry]) -> SelectionManifest:
    return SelectionManifest(
        selection_rule_version="EXTFP2-B-SELECT-1.0",
        selection_rule_text="test",
        snapshot_ref="snap-1",
        run_id="run-test",
        geoscope="测试",
        filter_condition="test",
        record_count=len({e.source_record_id for e in entries}),
        asset_count=len(entries),
        record_ids_hash="cli-test-hash-0123456789abcdef",
        records=entries,
        domain_whitelist=[DOMAIN],
        estimated_download_bytes=len(entries) * 70 * 1024,
        storage_cap_bytes=len(entries) * 70 * 1024 * 2,
        budget_cap_yuan=float(len(entries)),
        avg_bytes_estimate=70 * 1024,
    )


def write_manifest_json(path: Path, manifest: SelectionManifest) -> Path:
    path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return path


def _write_selection_parquet(staged_runs_dir: Path) -> Path:
    """在 staged runs 目录写一张含一条普通住宅候选的 parquet，返回其路径。"""
    parquet_path = staged_runs_dir / "lianjia_ext_ordinary_residential.parquet"
    df = pl.DataFrame(
        {
            "row_number": [1],
            "source_record_id": ["A1"],
            "sale_date": ["2023-01-01"],
            "property_use_norm": ["普通住宅"],
            "floorplan_url_status": [UrlListStatus.URLS_OK.value],
            "floorplan_candidate_count": [1],
            "floorplan_url_list_raw": [f"[{URL_A!r}]"],
        }
    )
    df.write_parquet(parquet_path)
    return parquet_path


def _write_acceptance_parquet(staged_runs_dir: Path) -> Path:
    """EXTFP3-G 验收抽样所需列集：在 select 基础上补 bedrooms_raw / extra_fields_json。"""
    parquet_path = staged_runs_dir / "lianjia_ext_ordinary_residential.parquet"
    df = pl.DataFrame(
        {
            "row_number": [1],
            "source_record_id": ["A1"],
            "sale_date": ["2023-01-01"],
            "bedrooms_raw": ["3"],
            "extra_fields_json": [json.dumps({"区县": "目标区"})],
            "property_use_norm": ["普通住宅"],
            "floorplan_url_status": [UrlListStatus.URLS_OK.value],
            "floorplan_candidate_count": [1],
            "floorplan_url_list_raw": [f"[{URL_A!r}]"],
        }
    )
    df.write_parquet(parquet_path)
    return parquet_path


def _write_current_pointer(data_dir: Path, run_id: str, rel_parquet: str) -> Path:
    staged = data_dir / "staged" / "lianjia_ext"
    staged.mkdir(parents=True, exist_ok=True)
    pointer = staged / "current.json"
    pointer.write_text(
        json.dumps({"run_id": run_id, "ordinary_residential": rel_parquet}),
        encoding="utf-8",
    )
    return pointer


def _staged_runs(data_dir: Path, run_id: str) -> Path:
    d = data_dir / "staged" / "lianjia_ext" / "runs" / f"run_{run_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


# ---------------------------------------------------------------------------
# 1) floorplan select 异常分支（RV-EXTFP2-B-01#F1）
# ---------------------------------------------------------------------------


def test_select_cli_unknown_profile(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["floorplan", "select", "--profile", "bogus", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "unknown profile" in capsys.readouterr().out


def test_select_cli_no_current_pointer(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["floorplan", "select", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "no current run pointer" in capsys.readouterr().out


def test_select_cli_missing_staged_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 有指针但 runs 下缺普通住宅表
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    rc = cli.main(["floorplan", "select", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "staged parquet not found" in capsys.readouterr().out


def test_select_cli_run_id_override_missing_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # --run-id 覆盖，但该 run 的表不存在
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    rc = cli.main(["floorplan", "select", "--run-id", "missing", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "staged parquet not found" in capsys.readouterr().out


def test_select_cli_unreadable_table_fails_gracefully(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # 指针指向的 parquet 是坏文件（非 parquet 字节）→ build_selection 触发异常 → CLI 捕获返回 1
    _staged_runs(tmp_path, "run1")
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    bad = _staged_runs(tmp_path, "run1") / "lianjia_ext_ordinary_residential.parquet"
    bad.write_bytes(b"this is not a parquet file at all")
    rc = cli.main(["floorplan", "select", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "[floorplan select]" in capsys.readouterr().out


def test_select_cli_normal_writes_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_selection_parquet(_staged_runs(tmp_path, "run1"))
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    out = tmp_path / "out" / "selection_manifest.json"
    rc = cli.main(["floorplan", "select", "--data-dir", str(tmp_path), "--out", str(out)])
    assert rc == 0
    assert "选择清单 ->" in capsys.readouterr().out
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["record_count"] == 1
    assert data["asset_count"] == 1


# ---------------------------------------------------------------------------
# 1b) floorplan acceptance 异常/正常分支（EXTFP3-G CLI）
# ---------------------------------------------------------------------------


def test_acceptance_cli_unknown_profile(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["floorplan", "acceptance", "--profile", "bogus", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "unknown profile" in capsys.readouterr().out


def test_acceptance_cli_no_current_pointer(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = cli.main(["floorplan", "acceptance", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "no current run pointer" in capsys.readouterr().out


def test_acceptance_cli_missing_staged_table(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    rc = cli.main(["floorplan", "acceptance", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "staged parquet not found" in capsys.readouterr().out


def test_acceptance_cli_unreadable_table_fails_gracefully(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _staged_runs(tmp_path, "run1")
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    bad = _staged_runs(tmp_path, "run1") / "lianjia_ext_ordinary_residential.parquet"
    bad.write_bytes(b"this is not a parquet file at all")
    rc = cli.main(["floorplan", "acceptance", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "[floorplan acceptance]" in capsys.readouterr().out


def test_acceptance_cli_missing_columns_fails_gracefully(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # select 列集（缺 bedrooms_raw/extra_fields_json）→ build_acceptance_sample 抛异常 → 返回 1
    _write_selection_parquet(_staged_runs(tmp_path, "run1"))
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    rc = cli.main(["floorplan", "acceptance", "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "[floorplan acceptance]" in capsys.readouterr().out


def test_acceptance_cli_normal_writes_manifest_and_golden(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_acceptance_parquet(_staged_runs(tmp_path, "run1"))
    _write_current_pointer(
        tmp_path, "run1", "runs/run_run1/lianjia_ext_ordinary_residential.parquet"
    )
    out = tmp_path / "out" / "acceptance_manifest.json"
    golden = tmp_path / "out" / "golden_template.csv"
    rc = cli.main(
        [
            "floorplan",
            "acceptance",
            "--data-dir",
            str(tmp_path),
            "--target",
            "1",
            "--out",
            str(out),
            "--golden-csv",
            str(golden),
        ]
    )
    assert rc == 0
    captured = capsys.readouterr().out
    assert "抽样清单 ->" in captured
    assert "黄金标签模板 ->" in captured
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["sampling_rule_version"] == "EXTFP3-G-SAMPLE-1.0"
    assert data["record_count"] == 1
    assert data["asset_count"] == 1
    assert data["target_size"] == 1
    assert golden.is_file()
    header = golden.read_text(encoding="utf-8-sig").splitlines()[0]
    assert "图片文字类别" in header and "房间清单" in header


# ---------------------------------------------------------------------------
# 2) floorplan download 异常分支
# ---------------------------------------------------------------------------


def test_download_cli_missing_selection(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(
        [
            "floorplan",
            "download",
            "--selection",
            str(tmp_path / "nope.json"),
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 1
    assert "selection manifest not found" in capsys.readouterr().out


def test_download_cli_invalid_concurrency(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    rc = cli.main(
        [
            "floorplan",
            "download",
            "--selection",
            str(sel),
            "--max-concurrency",
            "0",
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 2
    assert "invalid --max-concurrency" in capsys.readouterr().out


def test_download_cli_invalid_retries(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    rc = cli.main(
        [
            "floorplan",
            "download",
            "--selection",
            str(sel),
            "--retries",
            "0",
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 2
    assert "invalid --retries" in capsys.readouterr().out


def test_download_cli_invalid_timeout(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    rc = cli.main(
        [
            "floorplan",
            "download",
            "--selection",
            str(sel),
            "--timeout",
            "-1",
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 2
    assert "invalid --timeout" in capsys.readouterr().out


def test_download_cli_bad_manifest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sel = tmp_path / "sel.json"
    sel.write_text("{not json", encoding="utf-8")
    rc = cli.main(["floorplan", "download", "--selection", str(sel), "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "invalid selection manifest" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# 3) floorplan asset 异常分支
# ---------------------------------------------------------------------------


def test_asset_cli_missing_run_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(
        ["floorplan", "asset", "--run", str(tmp_path / "nope"), "--data-dir", str(tmp_path)]
    )
    assert rc == 1
    assert "download run dir not found" in capsys.readouterr().out


def test_asset_cli_no_download_state(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = tmp_path / "run1"
    run_dir.mkdir(parents=True, exist_ok=True)
    rc = cli.main(["floorplan", "asset", "--run", str(run_dir), "--data-dir", str(tmp_path)])
    assert rc == 1
    assert "no download state" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# 4) floorplan e2e 异常分支（RV-EXTFP2-E-01#F3 缺省回退路径）
# ---------------------------------------------------------------------------

SAMPLE_LIST_NAME = "样本来源清单.md"


def _sample_dir(tmp_path: Path, rows: list[tuple[str, bytes]]) -> Path:
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)
    lines = ["| 文件 | 大小(bytes) | 来源 URL |", "|---|---|---|"]
    for fname, b in rows:
        (d / fname).write_bytes(b)
        lines.append(f"| {fname} | {len(b)} | {URL_A} |")
    (d / SAMPLE_LIST_NAME).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return d


def test_e2e_cli_missing_selection(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    d = _sample_dir(tmp_path, [])
    rc = cli.main(
        [
            "floorplan",
            "e2e",
            "--selection",
            str(tmp_path / "nope.json"),
            "--sample-dir",
            str(d),
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert "selection manifest not found" in out


def test_e2e_cli_missing_sample_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    rc = cli.main(
        [
            "floorplan",
            "e2e",
            "--selection",
            str(sel),
            "--sample-dir",
            str(tmp_path / "nope"),
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 1
    assert "样本目录 not found" in capsys.readouterr().out


def test_e2e_cli_default_sample_list_fallback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """--sample-list 缺省 → 回退 <sample-dir>/样本来源清单.md；缺失时优雅返回 1 不崩溃。"""
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)  # 存在样本目录但缺清单文件
    rc = cli.main(
        [
            "floorplan",
            "e2e",
            "--selection",
            str(sel),
            "--sample-dir",
            str(d),
            "--data-dir",
            str(tmp_path),
        ]
    )
    assert rc == 1
    assert "样本来源清单 not found" in capsys.readouterr().out


def test_e2e_cli_explicit_sample_list_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """显式 --sample-list 被采纳：文件存在即通过校验；样本 URL 为白名单外 → 子集空 → 不触网停止。"""
    piece = b"\x89PNG\r\n\x1a\n"
    d = tmp_path / "samples"
    d.mkdir(parents=True, exist_ok=True)
    (d / "huxingtu_01.jpg").write_bytes(piece)
    explicit = tmp_path / "custom.md"
    explicit.write_text(
        "| 文件 | 大小(bytes) | 来源 URL |\n"
        "|---|---|---|\n"
        f"| huxingtu_01.jpg | {len(piece)} | {EVIL_URL} |\n",
        encoding="utf-8",
    )
    sel = write_manifest_json(tmp_path / "sel.json", _manifest([_entry("R1", 1, 1, URL_A)]))
    rc = cli.main(
        [
            "floorplan",
            "e2e",
            "--selection",
            str(sel),
            "--sample-dir",
            str(d),
            "--sample-list",
            str(explicit),
            "--data-dir",
            str(tmp_path),
        ]
    )
    # 显式路径被采纳；样本为白名单外且全量清单补不足 10 → fail-closed 停止，不触网
    out = capsys.readouterr().out
    assert "not found" not in out
    assert rc == 2
    assert "expected_count=10" in out
    assert "子集资产不足" in out


# ---------------------------------------------------------------------------
# 5) floorplan 父命令无子命令兜底（RV-EXTFP2-C-01#F1 已含，此处留回归）
# ---------------------------------------------------------------------------


def test_floorplan_no_subcommand_usage(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["floorplan"])
    assert rc == 2
    assert "usage: compsval floorplan" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# 6) 单一集成测试入口（RV-EXTFP2-F-01#F2：正常通路不在各命令单测零散覆盖，而在
# 一条端到端链路集中验证）—— download(mock) → asset CLI 真实分发 → staged parquet 读回
# ---------------------------------------------------------------------------


def test_single_e2e_normal_path_download_to_staged(tmp_path: Path) -> None:
    """一条正常通路的单一集成入口：mock 下载 2 张 → 经 cli.main 分发 floorplan asset 正常
    通路（写 raw + staged 资产表）→ 读回 staged parquet 断言行级列集与 CLI 产物一致。

    与 F1 roundtrip 不同：此处经 CLI 真实分发（含 CLI 级参数/错误码/输出路径），证明
    各命令正常通路在真实编排下可连贯串成链路，而非仅各单测各自绿。
    """
    import hashlib
    import io

    import httpx
    from PIL import Image

    from compsval.ingest.floorplan_asset import ASSET_STAGED_FILENAME
    from compsval.ingest.floorplan_download import run_download
    from compsval.ingest.floorplan_selection import _normalize_https

    def _jpeg(w: int, h: int) -> bytes:
        buf = io.BytesIO()
        Image.new("RGB", (w, h), (40, 90, 200)).save(buf, format="JPEG")
        return buf.getvalue()

    j1, j2 = _jpeg(12, 9), _jpeg(20, 14)
    body = {_normalize_https(URL_A): j1, _normalize_https(URL_B): j2}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url not in body:
            return httpx.Response(404, content=b"", request=request)
        return httpx.Response(200, content=body[url], request=request)

    sel = write_manifest_json(
        tmp_path / "sel.json",
        _manifest([_entry("R1", 1, 1, URL_A), _entry("R2", 2, 1, URL_B)]),
    )
    dl_dir = tmp_path / "dl"
    rec = run_download(
        _manifest_file_obj(sel),
        dl_dir,
        transport=httpx.MockTransport(handler),
        base_backoff=0.0,
    )

    rc = cli.main(
        ["floorplan", "asset", "--run", str(Path(rec.run_dir)), "--data-dir", str(tmp_path)]
    )
    assert rc == 0

    staged = tmp_path / "staged" / ASSET_STAGED_FILENAME
    assert staged.is_file()

    import pyarrow.parquet as pq

    table = pq.read_table(staged)
    assert table.num_rows == 2
    col2sha = {row["asset_id"]: row["sha256"] for row in table.to_pylist()}
    # 与下载字节 SHA 双向一致（血缘贯穿 download→asset→staged，asset_id 与任务清单一一对应）
    assert set(col2sha) == {t.asset_id for t in rec.tasks}
    # staged 与下载 run_dir 中各任务实际字节的 SHA 一致
    for t in rec.tasks:
        raw = (Path(rec.run_dir) / f"{t.download_task_id}.img").read_bytes()
        assert col2sha[t.asset_id] == hashlib.sha256(raw).hexdigest()


def _manifest_file_obj(sel_path: Path) -> SelectionManifest:
    import json

    from compsval.ingest.floorplan_selection import SelectionManifest

    return SelectionManifest.model_validate(json.loads(sel_path.read_text(encoding="utf-8")))
