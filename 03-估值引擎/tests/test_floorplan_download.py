"""EXTFP2-C 户型图下载器与状态机（floorplan_download）的离线测试。

全部用例用 ``httpx.MockTransport`` 离线应答，绝不触网；覆盖需求中的四类：
正常成功落盘 / 边界（429、5xx 重试成功、多 URL url_ordinal）/ 缺失与重试上限（连续 5xx、
404 不重试、白名单外零请求）/ 反例与断点续跑与 force-new-run / 幂等。
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from compsval.ingest.floorplan_download import (
    DOWNLOADER_VERSION,
    DownloadState,
    DownloadTask,
    deterministic_run_id,
    run_download,
    transition,
)
from compsval.ingest.floorplan_selection import (
    SelectionEntry,
    SelectionManifest,
)

URL_A = "https://ke-image.ljcdn.com/hdic-frame/a.jpg?from=ke.com"
URL_B = "https://ke-image.ljcdn.com/hdic-frame/b.jpg?from=ke.com"
URL_C = "https://ke-image.ljcdn.com/hdic-frame/c.jpg?from=ke.com"
EVIL_URL = "http://evil.example.com/8.jpg?from=ke.com"

PAYLOAD_A = b"\x89PNG\r\n\x1a\nfake-floorplan-a-bytes"
PAYLOAD_B = b"\x89PNG\r\n\x1a\nfake-floorplan-b-bytes"

MANIFEST_HASH = "recids-hash-EXTFP2C-XXXX0001abcdef"


def _entry(
    rid: str,
    row: int,
    seq: int,
    url: str,
    domain: str = "ke-image.ljcdn.com",
    normalized_url: str | None = None,
) -> SelectionEntry:
    return SelectionEntry(
        source_record_id=rid,
        row_number=row,
        url_seq=seq,
        url=url,
        normalized_url=normalized_url or url,
        domain=domain,
    )


def _manifest(
    entries: list[SelectionEntry], *, snapshot_ref: str | None = "snap-1"
) -> SelectionManifest:
    return SelectionManifest(
        selection_rule_version="EXTFP2-B-SELECT-1.0",
        selection_rule_text="test",
        snapshot_ref=snapshot_ref,
        geoscope="示例城市西部目标区测试",
        filter_condition="test",
        record_count=len({e.source_record_id for e in entries}),
        asset_count=len(entries),
        record_ids_hash=MANIFEST_HASH,
        records=entries,
        domain_whitelist=["ke-image.ljcdn.com"],
        estimated_download_bytes=len(entries) * 70 * 1024,
        storage_cap_bytes=len(entries) * 70 * 1024 * 2,
        budget_cap_yuan=float(len(entries)),
        avg_bytes_estimate=70 * 1024,
    )


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _mock_transport(
    handler: Callable[[httpx.Request], httpx.Response | httpx.HTTPError],
) -> httpx.MockTransport:
    def _handle(request: httpx.Request) -> httpx.Response:
        result = handler(request)
        if isinstance(result, httpx.HTTPError):
            raise result
        return result

    return httpx.MockTransport(_handle)


def _url_handler(
    counter: dict[str, int],
    *,
    status: int = 200,
    content: bytes = PAYLOAD_A,
) -> Callable[[httpx.Request], httpx.Response]:
    """返回固定状态/响应的 handler；每 URL 记录请求次数。"""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        counter[url] = counter.get(url, 0) + 1
        return httpx.Response(status, content=content, request=request)

    return handler


# ---------------------------------------------------------------------------
# 1) 正常：固定 bytes → DOWNLOADED、字节正确落盘、size/sha256 正确
# ---------------------------------------------------------------------------


def test_normal_download_persists_exact_bytes(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, content=PAYLOAD_A))

    record = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0)

    assert len(record.tasks) == 1
    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOADED
    assert task.attempts == 1
    assert task.size_bytes == len(PAYLOAD_A)
    assert task.content_sha256 == _sha(PAYLOAD_A)
    assert task.downloaded_at is not None
    assert task.final_url == URL_A

    img = tmp_path / f"{task.download_task_id}.img"
    assert img.is_file()
    assert img.read_bytes() == PAYLOAD_A  # 原始字节，不转码不重压缩

    assert str(URL_A) in counter and counter[str(URL_A)] == 1


# ---------------------------------------------------------------------------
# 2) 边界：429 重试成功 / 5xx 重试成功 / 多 URL url_ordinal 正确编号
# ---------------------------------------------------------------------------


def test_retry_429_then_success(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        counter[str(request.url)] = counter.get(str(request.url), 0) + 1
        n = counter[str(request.url)]
        if n < 3:  # 前 2 次 429，第 3 次成功
            return httpx.Response(429, content=b"", request=request)
        return httpx.Response(200, content=PAYLOAD_A, request=request)

    transport = _mock_transport(handler)
    record = run_download(manifest, tmp_path, transport=transport, max_attempts=3, base_backoff=0.0)

    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOADED
    assert task.attempts == 3  # 429 走指数退避重试直至成功
    assert (tmp_path / f"{task.download_task_id}.img").read_bytes() == PAYLOAD_A


def test_retry_5xx_then_success(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        counter[str(request.url)] = counter.get(str(request.url), 0) + 1
        n = counter[str(request.url)]
        if n < 2:
            return httpx.Response(503, content=b"", request=request)
        return httpx.Response(200, content=PAYLOAD_A, request=request)

    transport = _mock_transport(handler)
    record = run_download(manifest, tmp_path, transport=transport, max_attempts=3, base_backoff=0.0)

    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOADED
    assert task.attempts == 2
    assert task.last_error is None


def test_multi_url_ordinal_numbering(tmp_path: Path) -> None:
    """同记录多条资产按记录内 URL 序号正确编号（url_ordinal 1,2 起）。"""
    manifest = _manifest([_entry("R1", 1, 1, URL_A), _entry("R1", 1, 2, URL_B)])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, content=PAYLOAD_A))

    record = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0)

    assert [t.url_ordinal for t in record.tasks] == [1, 2]
    assert record.tasks[0].source_record_id == "R1"
    assert record.tasks[0].asset_id != record.tasks[1].asset_id  # 不同 url_ordinal → 不同 asset_id
    assert all(t.state is DownloadState.DOWNLOADED for t in record.tasks)
    for t in record.tasks:
        assert (tmp_path / f"{t.download_task_id}.img").is_file()


# ---------------------------------------------------------------------------
# 3) 缺失/重试上限：连续 5xx 超上限、404 不重试、白名单外零请求
# ---------------------------------------------------------------------------


def test_5xx_exhausts_retries_then_failed(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, status=500))

    record = run_download(manifest, tmp_path, transport=transport, max_attempts=3, base_backoff=0.0)

    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOAD_FAILED
    assert task.attempts == 3  # 连续 5xx 直到达到含首次共 3 次请求上限
    assert task.last_error == "http-500"
    assert not (tmp_path / f"{task.download_task_id}.img").exists()


def test_404_no_retry_failed_immediately(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, status=404))

    record = run_download(manifest, tmp_path, transport=transport, max_attempts=3, base_backoff=0.0)

    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOAD_FAILED
    assert task.attempts == 1  # 404 非 408/429，不重试
    assert task.last_error == "http-404"


def test_domain_not_in_whitelist_zero_requests(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, EVIL_URL, domain="evil.example.com")])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, content=PAYLOAD_A))

    record = run_download(manifest, tmp_path, transport=transport, max_attempts=3, base_backoff=0.0)

    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOAD_FAILED
    assert task.last_error == "domain-not-allowed"
    assert task.attempts == 0  # 不计入重试：从未发起请求
    assert counter == {}  # 零请求
    assert record.state_counts.get("DOWNLOADED", 0) == 0


# ---------------------------------------------------------------------------
# 4) 反例/断点续跑/force-new-run
# ---------------------------------------------------------------------------


def _mixed_transport(counter: dict[str, int]) -> httpx.MockTransport:
    """URL_A 成功，URL_B 恒 500。"""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        counter[url] = counter.get(url, 0) + 1
        if url == URL_B:
            return httpx.Response(500, content=b"", request=request)
        return httpx.Response(200, content=PAYLOAD_A, request=request)

    return _mock_transport(handler)


def test_resume_only_retries_failed_tasks(tmp_path: Path) -> None:
    """第一次部分成功 + 部分失败；第二次重跑仅请求失败项、成功项跳过。"""
    manifest = _manifest([_entry("R1", 1, 1, URL_A), _entry("R2", 2, 1, URL_B)])
    c1: dict[str, int] = {}
    r1 = run_download(manifest, tmp_path, transport=_mixed_transport(c1), base_backoff=0.0)

    # 第一次：A 成功，B 失败
    by_url = {t.canonical_url: t for t in r1.tasks}
    assert by_url[URL_A].state is DownloadState.DOWNLOADED
    assert by_url[URL_B].state is DownloadState.DOWNLOAD_FAILED

    # 第二次重跑同 out：响应计数里只有失败项 B 被请求，成功项 A 跳过
    c2: dict[str, int] = {}
    r2 = run_download(manifest, tmp_path, transport=_mixed_transport(c2), base_backoff=0.0)

    by_url2 = {t.canonical_url: t for t in r2.tasks}
    assert by_url2[URL_A].state is DownloadState.DOWNLOADED  # 复用成功
    assert by_url2[URL_B].state is DownloadState.DOWNLOAD_FAILED  # 仍失败
    assert URL_A not in c2  # 成功项未被再次请求
    assert c2.get(URL_B, 0) >= 1  # 失败项被再次请求
    assert r2.run_id == r1.run_id  # 同一 run 断点续跑


def test_force_new_run_keeps_old_evidence(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    counter: dict[str, int] = {}
    transport = _mock_transport(_url_handler(counter, content=PAYLOAD_A))

    r1 = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0)
    old_run_dir = Path(r1.run_dir)
    old_img = old_run_dir / f"{r1.tasks[0].download_task_id}.img"
    assert old_img.is_file()

    r2 = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0, force_new_run=True)
    new_run_dir = Path(r2.run_dir)

    assert new_run_dir != old_run_dir  # 新输出目录
    assert r2.run_id != r1.run_id  # 新 run_id
    assert old_img.is_file()  # 旧证据不被覆盖
    assert (new_run_dir / f"{r2.tasks[0].download_task_id}.img").is_file()
    assert r2.run_dir.startswith((tmp_path / "run_").as_posix())


# ---------------------------------------------------------------------------
# 5) 幂等：相同输入两次 run 的资产级状态与落盘一致
# ---------------------------------------------------------------------------


def test_idempotent_same_input_same_result(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A), _entry("R2", 2, 1, URL_C)])

    def ok_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == URL_C:
            return httpx.Response(200, content=PAYLOAD_B, request=request)
        return httpx.Response(200, content=PAYLOAD_A, request=request)

    transport = _mock_transport(ok_handler)
    r1 = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0)
    assert all(t.state is DownloadState.DOWNLOADED for t in r1.tasks)

    # 第二次：全部已 DOWNLOADED → 幂等键命中，零请求也保持一致结果
    second_hits: dict[str, int] = {}

    def crash_handler(request: httpx.Request) -> httpx.Response:
        second_hits[str(request.url)] = second_hits.get(str(request.url), 0) + 1
        raise AssertionError("idempotent rerun should make no network request")

    r2 = run_download(
        manifest, tmp_path, transport=_mock_transport(crash_handler), base_backoff=0.0
    )

    assert second_hits == {}  # 幂等重跑零请求
    assert r2.run_id == r1.run_id
    assert [(t.download_task_id, t.state, t.content_sha256) for t in r2.tasks] == [
        (t.download_task_id, t.state, t.content_sha256) for t in r1.tasks
    ]


def test_manifest_ref_and_version(tmp_path: Path) -> None:
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    transport = _mock_transport(_url_handler({}, content=PAYLOAD_A))
    record = run_download(manifest, tmp_path, transport=transport, base_backoff=0.0)
    assert record.downloader_version == DOWNLOADER_VERSION
    assert record.manifest_ref == manifest.record_ids_hash
    assert record.sourced is True  # snapshot_ref 非空
    assert record.run_id == deterministic_run_id(manifest)


# ---------------------------------------------------------------------------
# 5.1) 状态机直接测试（RV-EXTFP2-C-01#F2，合同退出证据「状态机测试」）
# ---------------------------------------------------------------------------


def test_transition_state_machine_rules(tmp_path: Path) -> None:
    """显式状态按白名单迁移：合法迁移通过，非法迁移与终态回退抛 ValueError。"""

    def _task() -> DownloadTask:
        return DownloadTask(
            download_task_id="t1",
            asset_id="a1",
            sale_record_key="s1",
            source_record_id="R1",
            row_number=1,
            url_ordinal=1,
            url=URL_A,
            canonical_url=URL_A,
            domain="ke-image.ljcdn.com",
            state=DownloadState.READY_TO_DOWNLOAD,
        )

    # 合法：READY -> DOWNLOADING -> DOWNLOADED（终态）
    t = _task()
    transition(t, DownloadState.DOWNLOADING)
    assert t.state is DownloadState.DOWNLOADING
    transition(t, DownloadState.DOWNLOADED)
    assert t.state is DownloadState.DOWNLOADED
    # 终态不可回退：DOWNLOADED 不再接受任何迁移
    with pytest.raises(ValueError):
        transition(t, DownloadState.DOWNLOADING)
    with pytest.raises(ValueError):
        transition(t, DownloadState.DOWNLOAD_FAILED)
    # 非法迁移：READY 不允许直接跳 DOWNLOADED（必经 DOWNLOADING）
    t2 = _task()
    with pytest.raises(ValueError):
        transition(t2, DownloadState.DOWNLOADED)
    # 合法失败分支：READY/DOWNLOADING -> DOWNLOAD_FAILED（终态）
    t3 = _task()
    transition(t3, DownloadState.DOWNLOAD_FAILED)
    assert t3.state is DownloadState.DOWNLOAD_FAILED
    with pytest.raises(ValueError):
        transition(t3, DownloadState.READY_TO_DOWNLOAD)  # 失败终态不可回退


# ---------------------------------------------------------------------------
# 5.2) 域名白名单交集 fail-closed（RV-EXTFP2-C-01#F3）
# ---------------------------------------------------------------------------


def test_manifest_whitelist_intersection_fail_closed(tmp_path: Path) -> None:
    """manifest 声明白名单与模块白名单无交集时 fail-closed：零请求直接 DOWNLOAD_FAILED。"""
    manifest = _manifest([_entry("R1", 1, 1, URL_A)])
    manifest.domain_whitelist = ["unknown-cdn.example.com"]  # 与模块白名单无交集
    counter: dict[str, int] = {}
    record = run_download(
        manifest,
        tmp_path,
        transport=_mock_transport(_url_handler(counter, content=PAYLOAD_A)),
        base_backoff=0.0,
    )
    task = record.tasks[0]
    assert task.state is DownloadState.DOWNLOAD_FAILED
    assert task.last_error == "domain-not-allowed"
    assert task.attempts == 0
    assert counter == {}  # 零请求
