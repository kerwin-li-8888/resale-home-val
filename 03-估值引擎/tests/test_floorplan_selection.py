"""EXTFP2-B 户型图选择清单生成（build_selection）的离线测试。

用 polars 在 tmp_path 合成小 staged parquet，覆盖：正常选择、多 URL 序号、
缺失（NO_URL/空/非普通住宅/解析失败）、白名单外违规域名、占位无候选排除、
幂等性。全程不触网、不访问真实数据。
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from compsval.ingest.floorplan_profile import UrlListStatus
from compsval.ingest.floorplan_selection import (
    DOMAIN_WHITELIST,
    SELECTION_RULE_VERSION,
    SelectionManifest,
    build_selection,
)

GOOD_URL_1 = "http://ke-image.ljcdn.com/hdic-frame/1.jpg?from=ke.com"
GOOD_URL_2 = "http://ke-image.ljcdn.com/hdic-frame/2a.jpg?from=ke.com"
GOOD_URL_3 = "http://ke-image.ljcdn.com/hdic-frame/2b.jpg?from=ke.com"
GOOD_URL_7 = "http://ke-image.ljcdn.com/hdic-frame/7.jpg?from=ke.com"
EVIL_URL = "http://evil.example.com/8.jpg"
PLACEHOLDER_URL = "http://ke-image.ljcdn.com/beike/dituFindHouse/1590373908380.png?from=ke.com"


def _write_fixture(tmp_path: Path) -> Path:
    """合成 staged 普通住宅 parquet（覆盖正常/多URL/缺失/违规/占位场景）。"""
    df = pl.DataFrame(
        {
            "row_number": [1, 2, 3, 4, 5, 6, 7, 8],
            "source_record_id": ["A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"],
            "sale_date": [
                "2023-01-01",
                "2023-02-01",
                "2023-03-01",
                None,
                "2023-04-01",
                "2023-05-01",
                "2023-06-01",
                None,
            ],
            "property_use_norm": [
                "普通住宅",
                "普通住宅",
                "普通住宅",
                "普通住宅",
                "商住两用",
                "普通住宅",
                "普通住宅",
                "普通住宅",
            ],
            "floorplan_url_status": [
                UrlListStatus.URLS_OK.value,  # A1 正常，1 资产
                UrlListStatus.URLS_OK.value,  # A2 多 URL（2 资产）
                UrlListStatus.URLS_OK.value,  # A3 空 raw → candidate<1 排除
                UrlListStatus.NO_URL.value,  # A4 NO_URL 排除
                UrlListStatus.URLS_OK.value,  # A5 非普通住宅 排除
                UrlListStatus.URL_PARSE_FAILURE.value,  # A6 解析失败 排除
                UrlListStatus.URLS_OK.value,  # A7 1 白名单 + 1 违规域名 → 1 资产 + 1 违规
                UrlListStatus.URLS_OK.value,  # A8 纯占位（存储计数>0 但重解析 0 候选）排除
            ],
            "floorplan_candidate_count": [1, 2, 0, 0, 1, 0, 2, 2],
            "floorplan_url_list_raw": [
                f"[{GOOD_URL_1!r}]",
                f"[{GOOD_URL_2!r}, {GOOD_URL_3!r}]",
                "[]",
                None,
                f"[{GOOD_URL_1!r}]",
                "this is not a list",
                f"[{GOOD_URL_7!r}, {EVIL_URL!r}]",
                f"[{PLACEHOLDER_URL!r}]",
            ],
        }
    )
    path = tmp_path / "lianjia_ext_ordinary_residential.parquet"
    df.write_parquet(path)
    return path


def test_select_normal_counts_and_seq(tmp_path: Path) -> None:
    m = build_selection(_write_fixture(tmp_path))
    # A1(1 资产), A2(2 资产), A7(1 资产)  被选入；其余排除
    assert m.record_count == 3
    assert m.asset_count == 4
    assert m.selection_rule_version == SELECTION_RULE_VERSION
    assert isinstance(m, SelectionManifest)

    # 记录级清单：全部资产都在，URL 序号正确（A2 两条连续 1,2）
    urls = [e.url for e in m.records]
    assert GOOD_URL_1 in urls
    assert GOOD_URL_2 in urls
    assert GOOD_URL_3 in urls
    assert GOOD_URL_7 in urls
    a2 = [e for e in m.records if e.source_record_id == "A2"]
    assert [e.url_seq for e in a2] == [1, 2]
    assert a2[0].url == GOOD_URL_2 and a2[1].url == GOOD_URL_3

    # A1 单 URL 序号从 1 起
    a1 = [e for e in m.records if e.source_record_id == "A1"]
    assert [e.url_seq for e in a1] == [1]


def test_normalized_url_and_domain(tmp_path: Path) -> None:
    m = build_selection(_write_fixture(tmp_path))
    a1 = next(e for e in m.records if e.source_record_id == "A1")
    # http → https，规范化域名入白名单
    assert a1.normalized_url.startswith("https://ke-image.ljcdn.com/")
    assert a1.domain == "ke-image.ljcdn.com"
    assert m.domain_whitelist == sorted(DOMAIN_WHITELIST)


def test_missing_and_non_residential_excluded(tmp_path: Path) -> None:
    m = build_selection(_write_fixture(tmp_path))
    ids = {e.source_record_id for e in m.records}
    # A4(NO_URL) / A3(空raw) / A6(解析失败) / A5(商住两用) 均不在清单
    assert "A4" not in ids
    assert "A3" not in ids
    assert "A6" not in ids
    assert "A5" not in ids
    # date 范围取有效记录的 sale_date
    assert m.date_range_min == "2023-01-01"
    assert m.date_range_max == "2023-06-01"


def test_placeholder_only_and_stored_count_mismatch_excluded(tmp_path: Path) -> None:
    """A8 纯占位：存储 candidate_count=2，但重解析 0 候选 → 不信任计数，被排除。"""
    m = build_selection(_write_fixture(tmp_path))
    assert "A8" not in {e.source_record_id for e in m.records}


def test_forbidden_domain_not_counted_as_asset(tmp_path: Path) -> None:
    m = build_selection(_write_fixture(tmp_path))
    # A7 的 EVIL_URL 计入违规，不计入资产
    assert m.forbidden_domain_count == 1
    assert "evil.example.com" in m.forbidden_domains
    assert all(e.url != EVIL_URL for e in m.records)
    # 只有白名单 1 张入资产
    assert sum(1 for e in m.records if e.source_record_id == "A7") == 1


def test_idempotent_same_hash_and_records(tmp_path: Path) -> None:
    path = _write_fixture(tmp_path)
    m1 = build_selection(path)
    m2 = build_selection(path)
    assert m1.record_ids_hash == m2.record_ids_hash
    assert m1.record_count == m2.record_count
    assert [e.model_dump() for e in m1.records] == [e.model_dump() for e in m2.records]


def test_atomic_write_json(tmp_path: Path) -> None:
    out = tmp_path / "sub" / "selection_manifest.json"
    m = build_selection(_write_fixture(tmp_path), out_json=out)
    assert out.is_file()
    assert not out.with_name(out.name + ".incomplete").exists()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["record_count"] == m.record_count
    assert data["asset_count"] == m.asset_count
    assert data["record_ids_hash"] == m.record_ids_hash
    assert data["records"][0]["url_seq"] == 1


# ---------------------------------------------------------------------------
# 记录级去重（EXTFP4-verify-followups，SUGGESTION ②）
# ---------------------------------------------------------------------------


def _write_dup_staged(path: Path) -> None:
    """S1 出现两行（row_number 1/2 均窗口内可解析），S2/S3 各一行。"""
    df = pl.DataFrame(
        {
            "row_number": [1, 2, 3, 4],
            "source_record_id": ["S1", "S1", "S2", "S3"],
            "sale_date": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"],
            "property_use_norm": ["普通住宅", "普通住宅", "普通住宅", "普通住宅"],
            "floorplan_url_status": [UrlListStatus.URLS_OK.value] * 4,
            "floorplan_candidate_count": [1, 1, 1, 1],
            "floorplan_url_list_raw": [
                f"[{GOOD_URL_1!r}]",
                f"[{GOOD_URL_2!r}]",
                f"[{GOOD_URL_3!r}]",
                f"[{GOOD_URL_7!r}]",
            ],
        }
    )
    df.write_parquet(path)


def test_dedupe_disabled_default_matches_original_behavior(tmp_path: Path) -> None:
    """默认 dedupe_record_ids=False 与 EXTFP2-B 行为完全一致（全量锚点口径不变）。"""
    path = _write_fixture(tmp_path)
    m1 = build_selection(path)
    m2 = build_selection(path, dedupe_record_ids=False)
    assert m1.model_dump() == m2.model_dump()
    assert m1.record_ids_hash == m2.record_ids_hash
    assert m1.dedupe_record_count == 0 and m1.dedupe_record_sample == []


def test_dedupe_keeps_one_row_per_record_id(tmp_path: Path) -> None:
    """启用去重：同 source_record_id 多行只保留一行，计数与哈希按去重后口径。"""
    path = tmp_path / "dup.parquet"
    _write_dup_staged(path)
    m = build_selection(path, dedupe_record_ids=True)
    ids = [e.source_record_id for e in m.records]
    assert ids.count("S1") == 1  # S1 两行只保留一行
    assert m.record_count == 3
    assert m.asset_count == 3
    assert m.dedupe_record_count == 1  # 被去重 1 行
    assert m.dedupe_record_sample == ["S1"]
    # 保留行 = row_number 最小者（row_number=1 的 S1 行，URL=GOOD_URL_1）
    s1 = next(e for e in m.records if e.source_record_id == "S1")
    assert s1.url == GOOD_URL_1
    # 哈希与去重前（多集口径）不同，且记录数更少
    m_plain = build_selection(path)
    assert m.record_ids_hash != m_plain.record_ids_hash
    assert m.record_count < m_plain.record_count


def test_dedupe_tie_break_by_raw_lexicographic(tmp_path: Path) -> None:
    """row_number 重复时保留行按 floorplan_url_list_raw 字典序最小者。"""
    df = pl.DataFrame(
        {
            "row_number": [9, 9],
            "source_record_id": ["T1", "T1"],
            "sale_date": ["2023-01-01", "2023-02-01"],
            "property_use_norm": ["普通住宅", "普通住宅"],
            "floorplan_url_status": [UrlListStatus.URLS_OK.value] * 2,
            "floorplan_candidate_count": [1, 1],
            "floorplan_url_list_raw": [
                f"[{GOOD_URL_2!r}]",  # "2a"
                f"[{GOOD_URL_3!r}]",  # "2b" > "2a"
            ],
        }
    )
    path = tmp_path / "tie.parquet"
    df.write_parquet(path)
    m = build_selection(path, dedupe_record_ids=True)
    kept = [e for e in m.records if e.source_record_id == "T1"]
    assert len(kept) == 1
    assert kept[0].url == GOOD_URL_2  # "2a" 字典序小于 "2b"
    assert m.dedupe_record_count == 1
