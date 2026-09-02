"""EXTFP3-G 300 张分层验收集（抽样清单 + 黄金标签框架）的离线测试。

用 polars 在 tmp_path 合成小 staged parquet，覆盖：维度归一化、分层分配（保底/比例/
最大余数/其他区组不保底）、确定性抽样幂等、单元覆盖、资产裁剪、黄金标签模板生成与
校验（正常/缺失/非法类别/非法面积）。全程不触网、不访问真实数据。
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from compsval.ingest.floorplan_acceptance import (
    GOLDEN_LABEL_CSV_COLUMNS,
    AcceptanceSelectionManifest,
    GoldenLabelValidation,
    allocate_quotas,
    bedroom_bucket,
    build_acceptance_sample,
    district_group,
    validate_golden_labels,
    write_golden_label_template,
    write_ocr_draft_golden_labels,
    year_bucket,
)
from compsval.ingest.floorplan_profile import UrlListStatus

GOOD_URL = "http://ke-image.ljcdn.com/hdic-frame/1.jpg?from=ke.com"


def _mk_row(
    i: int,
    district: str = "目标区",
    sale_date: str = "2023-01-01",
    bedrooms: str = "3",
    property_use: str = "普通住宅",
    url_status: str = UrlListStatus.URLS_OK.value,
    candidate_count: int = 1,
) -> dict[str, object]:
    return {
        "row_number": i,
        "source_record_id": f"R{i:05d}",
        "sale_date": sale_date,
        "bedrooms_raw": bedrooms,
        "extra_fields_json": json.dumps({"区县": district}, ensure_ascii=False),
        "floorplan_url_list_raw": f"[{GOOD_URL!r}]",
        "property_use_norm": property_use,
        "floorplan_url_status": url_status,
        "floorplan_candidate_count": candidate_count,
    }


def _write_fixture(tmp_path: Path) -> Path:
    """合成 staged 普通住宅 parquet：多区县/年份/居室 + 噪音 + 非普通住宅 + 多 URL。

    单元结构（EXTFP3-G 抽样测试）：
    - 3 正式区 × 3 年份 × 3 居室 = 27 个正式单元（人口 1）+ 邻乙短名归邻乙区 = 28 个；
    - 多 URL 记录放独立正式单元（目标区 4居室，人口 1）→ 保底必选 → 超目标资产触发裁剪；
    - 4 条噪音归「其他」单元（人口 4）→ 仅按比例不保底 → 高 target 时成为种子敏感来源。
    """
    rows: list[dict[str, object]] = []
    i = 0
    # 3 个正式区 × 3 年份 × 3 居室 = 27 条（27 个正式单元，人口 1）
    for d in ("目标区", "中心区", "邻乙区"):
        for y in ("2017-05-01", "2020-05-01", "2024-05-01"):
            for b in ("1", "2", "3"):
                i += 1
                rows.append(_mk_row(i, district=d, sale_date=y, bedrooms=b))
    # 噪音：南海区（佛山/范围外）、子区域名 → 「其他」（4 条，人口 4，不保底）
    for d in ("南海区", "石滩镇", "无法处理", "珠海市"):
        i += 1
        rows.append(_mk_row(i, district=d, sale_date="2021-01-01", bedrooms="4"))
    # 邻乙短名 → 补「区」→ 邻乙区（新正式单元 邻乙区/2019-2021/4，人口 1）
    i += 1
    rows.append(_mk_row(i, district="邻乙", sale_date="2021-01-01", bedrooms="4"))
    # 非普通住宅 + 无 URL → 排除
    i += 1
    rows.append(_mk_row(i, sale_date="2023-01-01", property_use="商住两用", url_status="NO_URL"))
    # 一条记录多 URL（2 个候选资产）：目标区 4居室 → 独立正式单元 目标区/2019-2021/4
    i += 1
    multi = _mk_row(i, district="目标区", sale_date="2020-05-01", bedrooms="4")
    multi["floorplan_url_list_raw"] = f"[{GOOD_URL!r}, {GOOD_URL.replace('1.jpg', '2.jpg')!r}]"
    multi["floorplan_candidate_count"] = 2
    rows.append(multi)

    df = pl.DataFrame(rows)
    path = tmp_path / "lianjia_ext_ordinary_residential.parquet"
    df.write_parquet(path)
    return path


def _write_fixture_small(tmp_path: Path) -> Path:
    """紧凑 fixture：9 个正式单元（保底 9 < 目标 10），供黄金标签/清单兼容小样本测试。"""
    rows: list[dict[str, object]] = []
    i = 0
    # 2 区 × 2 年份 × 2 居室 = 8 个正式单元（人口 1）
    for d in ("目标区", "中心区"):
        for y in ("2017-05-01", "2024-05-01"):
            for b in ("1", "3"):
                i += 1
                rows.append(_mk_row(i, district=d, sale_date=y, bedrooms=b))
    # 邻乙短名 → 邻乙区（第 9 个正式单元，人口 1）
    i += 1
    rows.append(_mk_row(i, district="邻乙", sale_date="2021-01-01", bedrooms="4"))
    # 噪音 → 「其他」（人口 1，不保底；target=10 时按最大余数获得配额）
    i += 1
    rows.append(_mk_row(i, district="南海区", sale_date="2021-01-01", bedrooms="2"))

    df = pl.DataFrame(rows)
    path = tmp_path / "lianjia_ext_ordinary_residential_small.parquet"
    df.write_parquet(path)
    return path


# ---------------------------------------------------------------------------
# 维度归一化
# ---------------------------------------------------------------------------


def test_district_group_normalization() -> None:
    assert district_group("目标区") == "目标区"
    assert district_group("邻乙") == "邻乙区"  # 短名补「区」
    assert district_group("南海区") == "其他"  # 佛山南海
    assert district_group("石滩镇") == "其他"  # 子区域噪音
    assert district_group(None) == "其他"


def test_year_bucket_normalization() -> None:
    assert year_bucket("2016-01-01") == "2016-2018"
    assert year_bucket("2019-06-01") == "2019-2021"
    assert year_bucket("2022-01-01") == "2022-2024"
    assert year_bucket("2025-03-01") == "2025-2026"
    assert year_bucket("2026-07-20") == "2025-2026"
    assert year_bucket(None) == "2016-2018"  # 未知归最早桶


def test_bedroom_bucket_normalization() -> None:
    assert bedroom_bucket("1") == "1"
    assert bedroom_bucket("2") == "2"
    assert bedroom_bucket("3") == "3"
    assert bedroom_bucket("4") == "4"
    assert bedroom_bucket("5") == "5+"
    assert bedroom_bucket("20") == "5+"
    assert bedroom_bucket(None) == "5+"  # 未知归复杂户型桶


# ---------------------------------------------------------------------------
# 分层分配
# ---------------------------------------------------------------------------


def test_allocate_quotas_proportional_with_floor() -> None:
    cells = {("a", "y1", "1"): 100, ("a", "y1", "2"): 300, ("b", "y2", "3"): 600}
    quotas = allocate_quotas(cells, 10)
    assert sum(quotas.values()) == 10
    # 保底 1 + 按比例
    assert quotas[("a", "y1", "1")] >= 1
    assert quotas[("b", "y2", "3")] >= quotas[("a", "y1", "1")]  # 大单元配额更多


def test_allocate_quotas_other_skip_floor() -> None:
    """其他区组不保底：人口太少时配额可为 0。"""
    cells = {("目标区", "y1", "1"): 1000, ("其他", "y1", "1"): 5}
    quotas = allocate_quotas(cells, 10)
    assert sum(quotas.values()) == 10
    assert quotas[("其他", "y1", "1")] == 0  # 比例占比 <1 不补尾数


def test_allocate_quotas_cap_at_population() -> None:
    cells = {("a", "y1", "1"): 2, ("b", "y2", "3"): 100}
    quotas = allocate_quotas(cells, 50)
    assert quotas[("a", "y1", "1")] <= 2  # 不超人口
    assert sum(quotas.values()) == 50


def test_allocate_quotas_target_less_than_floor_raises() -> None:
    cells = {("a", "y1", "1"): 100, ("b", "y2", "3"): 100}
    try:
        allocate_quotas(cells, 1)  # 保底 2 > 目标 1
    except ValueError:
        return
    raise AssertionError("expected ValueError")


# ---------------------------------------------------------------------------
# 抽样幂等与覆盖
# ---------------------------------------------------------------------------


def test_build_acceptance_sample_deterministic(tmp_path: Path) -> None:
    path = _write_fixture(tmp_path)
    m1 = build_acceptance_sample(path, target=29, seed=20260825)
    m2 = build_acceptance_sample(path, target=29, seed=20260825)
    assert m1.record_ids_hash == m2.record_ids_hash
    assert [e.source_record_id for e in m1.records] == [e.source_record_id for e in m2.records]


def test_build_acceptance_sample_different_seed_different(tmp_path: Path) -> None:
    """不同种子 → 不同样本：target=30 时「其他」单元配额 1<人口 4，种子敏感。"""
    path = _write_fixture(tmp_path)
    m1 = build_acceptance_sample(path, target=30, seed=1)
    m2 = build_acceptance_sample(path, target=30, seed=2)
    assert m1.record_ids_hash != m2.record_ids_hash


def test_build_acceptance_sample_asset_count_exact(tmp_path: Path) -> None:
    path = _write_fixture(tmp_path)
    m = build_acceptance_sample(path, target=29, seed=20260825)
    assert m.asset_count == 29
    assert m.record_count <= 29
    assert len(m.records) == 29


def test_build_acceptance_sample_strata_coverage(tmp_path: Path) -> None:
    """每个非空非「其他」单元至少 1 张（全覆盖）；保底=29 即 target=29）。"""
    path = _write_fixture(tmp_path)
    m = build_acceptance_sample(path, target=29, seed=20260825)
    assert m.record_count == 29
    # 每个正式区 × 年份 × 居室单元都应被覆盖（保底 1）
    covered = {
        (a["district_group"], a["year_bucket"], a["bedroom_bucket"])
        for a in m.allocation_table
        if a["quota"] > 0
    }
    assert ("目标区", "2016-2018", "1") in covered
    assert ("中心区", "2019-2021", "2") in covered
    assert ("邻乙区", "2022-2024", "3") in covered
    # 其他区组不保底 → target=29（保底=29，remain=0）时配额 0，不在覆盖集合
    assert all(c[0] != "其他" for c in covered)
    # 记录全部来自合法池：非普通住宅（R00033）被排除；其他区组噪音（R00028 南海区）
    # 不被选；邻乙短名（R00032）归邻乙区正式单元被选中；多 URL 记录（R00034）在其
    # 独立正式单元被选中
    sids = {e.source_record_id for e in m.records}
    assert "R00033" not in sids
    assert "R00028" not in sids
    assert "R00032" in sids
    assert "R00034" in sids


def test_build_acceptance_sample_multi_url_trim(tmp_path: Path) -> None:
    """多 URL 记录：独立正式单元保底必选，超目标资产被确定性裁剪到 target。"""
    path = _write_fixture(tmp_path)
    # 29 个正式单元（含 1 条双 URL 记录 R00034）= 30 资产；target=30 时剩余 1 配额给
    # 「其他」→ 30 记录 / 31 资产 → 裁剪 1 个多余资产（url_seq=2）→ 30 资产
    m = build_acceptance_sample(path, target=30, seed=20260825)
    assert m.asset_count == 30
    assert m.trimmed_extra_assets == 1
    assert all(e.url_seq == 1 for e in m.records)


def test_build_acceptance_sample_duplicate_sid_dedup(tmp_path: Path) -> None:
    """同一 source_record_id 在 staged 多行（Excel 重复成交记录）只保留首个样本位。

    回归：重复 sid 若不去重，同记录多占样本位 → 资产数超目标且裁剪失效（url_seq 均为 1，
    无可裁剪的多余资产）。去重后记录/资产一一对应，asset_count == target。
    """
    path = _write_fixture_small(tmp_path)
    df = pl.read_parquet(path)
    # 复制第一行并把 source_record_id 改为与首行相同（构造重复成交记录）
    dup = df.head(1).with_columns(
        pl.lit(df["source_record_id"][0]).alias("source_record_id"),
        pl.lit(999, dtype=pl.Int64).alias("row_number"),
    )
    df2 = pl.concat([df, dup])
    dup_path = tmp_path / "lianjia_ext_ordinary_residential_dup.parquet"
    df2.write_parquet(dup_path)

    m = build_acceptance_sample(dup_path, target=10, seed=20260825)
    assert m.duplicate_sid_rows == 1
    assert m.asset_count == 10
    assert m.record_count == 10
    # 重复 sid 只出现一次；无 url_seq>1 的多余资产残留
    sids = [e.source_record_id for e in m.records]
    assert len(sids) == len(set(sids))
    assert all(e.url_seq == 1 for e in m.records)
    assert 999 not in {e.row_number for e in m.records}


def test_build_acceptance_sample_manifest_compat(tmp_path: Path) -> None:
    """AcceptanceSelectionManifest 是 SelectionManifest 子类，可被下载器消费。"""
    from compsval.ingest.floorplan_selection import SelectionManifest

    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    assert isinstance(m, AcceptanceSelectionManifest)
    assert isinstance(m, SelectionManifest)
    payload = m.model_dump()
    # 下载器按父类校验应成功（多余字段被忽略）
    loaded = SelectionManifest.model_validate(payload)
    assert loaded.record_ids_hash == m.record_ids_hash


# ---------------------------------------------------------------------------
# 黄金标签模板与校验
# ---------------------------------------------------------------------------


def test_write_golden_label_template(tmp_path: Path) -> None:
    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    csv_path = write_golden_label_template(m, tmp_path / "golden_template.csv")
    lines = csv_path.read_text(encoding="utf-8-sig").splitlines()
    assert lines[0].split(",") == GOLDEN_LABEL_CSV_COLUMNS
    assert len(lines) == 11  # header + 10 行


def _write_golden_csv(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    import csv

    csv_path = tmp_path / "golden_labels.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=GOLDEN_LABEL_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in GOLDEN_LABEL_CSV_COLUMNS})
    return csv_path


def test_validate_golden_labels_valid(tmp_path: Path) -> None:
    rows = [
        {
            "sample_index": str(i),
            "source_record_id": f"R{i:05d}",
            "图片文字类别": "有房间面积",
            "文字质量": "清晰",
            "房间清单": "主卧=12.5;客厅=20.3;厨房;卫生间",
        }
        for i in range(1, 11)
    ]
    csv_path = _write_golden_csv(tmp_path, rows)
    res = validate_golden_labels(csv_path, None)  # 默认期望 300 会 missing → 用 manifest 版
    assert isinstance(res, GoldenLabelValidation)
    # 用 10 样本 manifest 做期望基数
    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    res = validate_golden_labels(csv_path, m)
    assert res.valid
    assert res.rows_ok == 10
    assert res.room_count_total == 40
    assert res.area_present_count == 20
    assert res.category_counts["有房间面积"] == 10


def test_validate_golden_labels_missing_and_invalid(tmp_path: Path) -> None:
    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    rows = [
        {
            "sample_index": "1",
            "图片文字类别": "非法类别",
            "文字质量": "清晰",
            "房间清单": "主卧=12.5",
        },
        {
            "sample_index": "3",
            "图片文字类别": "有房间面积",
            "文字质量": "清晰",
            "房间清单": "主卧=abc",
        },
    ]
    csv_path = _write_golden_csv(tmp_path, rows)
    res = validate_golden_labels(csv_path, m)
    assert not res.valid
    assert 2 in res.missing_samples  # sample 2 缺失
    assert any("非法" in e for e in res.invalid_entries)  # 非法类别
    assert any("非数值" in e for e in res.invalid_entries)  # 非法面积


def test_validate_golden_labels_room_type_std(tmp_path: Path) -> None:
    """房间清单解析出标准房间类型与面积 Decimal。"""
    from compsval.ingest.floorplan_acceptance import _parse_room_list

    rooms, errors = _parse_room_list("主卧=12.5;厨房;阳台=5")
    assert errors == []
    assert [r.room_type_std for r in rooms] == ["master_bedroom", "kitchen", "balcony"]
    assert rooms[0].area_sqm is not None and str(rooms[0].area_sqm) == "12.5"
    assert rooms[1].area_present is False


def test_validate_golden_labels_excluded_range(tmp_path: Path) -> None:
    """「范围外」为合法枚举（人工判定排除，如多层户型），不参与验收指标。"""
    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    rows = [
        {
            "sample_index": str(i),
            "图片文字类别": "范围外" if i == 1 else "有房间面积",
            "文字质量": "范围外" if i == 1 else "清晰",
            "房间清单": "" if i == 1 else "主卧=12.5",
        }
        for i in range(1, 11)
    ]
    csv_path = _write_golden_csv(tmp_path, rows)
    res = validate_golden_labels(csv_path, m)
    assert res.valid
    assert res.excluded_count == 1
    assert res.category_counts["范围外"] == 1
    assert res.quality_counts["范围外"] == 1
    # 范围外样本不计入房间/面积统计（有效集 = 期望 - 排除数）
    assert res.room_count_total == 9
    assert res.area_present_count == 9


def test_validate_golden_labels_evidence_out_json(tmp_path: Path) -> None:
    path = _write_fixture_small(tmp_path)
    m = build_acceptance_sample(path, target=10, seed=20260825)
    rows = [
        {
            "sample_index": str(i),
            "图片文字类别": "只有房间名",
            "文字质量": "小字",
            "房间清单": "卧室",
        }
        for i in range(1, 11)
    ]
    csv_path = _write_golden_csv(tmp_path, rows)
    out = tmp_path / "golden_validation.json"
    res = validate_golden_labels(csv_path, m, out_json=out)
    assert res.valid
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["valid"] is True


# ---------------------------------------------------------------------------
# OCR 预标注草稿（EXTFP3-G「OCR 预标注 + 人工复核」）
# ---------------------------------------------------------------------------


def _write_ocr_draft_fixtures(
    tmp_path: Path,
    *,
    template_rows: list[dict[str, str]],
    tasks: list[dict[str, str]],
    annotations: list[dict[str, str]],
) -> tuple[Path, Path, Path]:
    """构造 write_ocr_draft_golden_labels 三输入：模板 CSV / 标注 parquet / ocr_state.json。"""
    template_csv = _write_golden_csv(tmp_path, template_rows)

    ocr_state = tmp_path / "ocr_state.json"
    ocr_state.write_text(
        json.dumps({"ocr_run_id": "run-test", "tasks": tasks}, ensure_ascii=False),
        encoding="utf-8",
    )

    ann = pl.DataFrame(annotations)
    ann_path = tmp_path / "floorplan_room_annotation.parquet"
    ann.write_parquet(ann_path)
    return template_csv, ann_path, ocr_state


def test_write_ocr_draft_golden_labels(tmp_path: Path) -> None:
    """SUCCEEDED 预填房间清单/类别；FAILED/无任务不预填并备注；CONFLICT 面积不预填。"""
    template_rows = [
        {
            "sample_index": "1",
            "asset_id": "asset-ok-area",
            "source_record_id": "R00001",
        },
        {
            "sample_index": "2",
            "asset_id": "asset-ok-names",
            "source_record_id": "R00002",
        },
        {
            "sample_index": "3",
            "asset_id": "asset-ok-empty",
            "source_record_id": "R00003",
        },
        {
            "sample_index": "4",
            "asset_id": "asset-failed",
            "source_record_id": "R00004",
        },
        {
            "sample_index": "5",
            "asset_id": "asset-notask",
            "source_record_id": "R00005",
        },
        {
            "sample_index": "6",
            "asset_id": "asset-conflict",
            "source_record_id": "R00006",
        },
    ]
    tasks = [
        {"asset_id": "asset-ok-area", "ocr_task_id": "t-area", "state": "OCR_SUCCEEDED"},
        {"asset_id": "asset-ok-names", "ocr_task_id": "t-names", "state": "OCR_SUCCEEDED"},
        {"asset_id": "asset-ok-empty", "ocr_task_id": "t-empty", "state": "OCR_SUCCEEDED"},
        {"asset_id": "asset-failed", "ocr_task_id": "t-failed", "state": "OCR_FAILED"},
        {"asset_id": "asset-conflict", "ocr_task_id": "t-conflict", "state": "OCR_SUCCEEDED"},
    ]
    annotations = [
        {
            "ocr_task_id": "t-area",
            "room_name_raw": "主卧",
            "room_name_normalized": "主卧",
            "standard_room_type": "master_bedroom",
            "area_value": "12.5",
            "parse_state": "ACCEPTED",
        },
        {
            "ocr_task_id": "t-area",
            "room_name_raw": "客厅",
            "room_name_normalized": "客厅",
            "standard_room_type": "living_room",
            "area_value": "20.3",
            "parse_state": "ACCEPTED",
        },
        {
            "ocr_task_id": "t-names",
            "room_name_raw": "厨房",
            "room_name_normalized": "厨房",
            "standard_room_type": "kitchen",
            "area_value": "",
            "parse_state": "ROOM_ONLY",
        },
        {
            "ocr_task_id": "t-names",
            "room_name_raw": "卫生间",
            "room_name_normalized": "卫生间",
            "standard_room_type": "bathroom",
            "area_value": "",
            "parse_state": "ROOM_ONLY",
        },
        {
            "ocr_task_id": "t-empty",
            "room_name_raw": "",
            "room_name_normalized": "",
            "standard_room_type": "",
            "area_value": "",
            "parse_state": "EMPTY",
        },
        {
            "ocr_task_id": "t-conflict",
            "room_name_raw": "主卧",
            "room_name_normalized": "主卧",
            "standard_room_type": "master_bedroom",
            "area_value": "12.5",
            "parse_state": "CONFLICT",
        },
        {
            "ocr_task_id": "t-conflict",
            "room_name_raw": "主卧",
            "room_name_normalized": "主卧",
            "standard_room_type": "master_bedroom",
            "area_value": "15.0",
            "parse_state": "NEEDS_REVIEW",
        },
    ]
    template_csv, ann_path, ocr_state = _write_ocr_draft_fixtures(
        tmp_path, template_rows=template_rows, tasks=tasks, annotations=annotations
    )
    out = tmp_path / "golden_label_ocr_draft.csv"
    got = write_ocr_draft_golden_labels(template_csv, ann_path, ocr_state, out_csv=out)
    assert got == out

    rows = {r["sample_index"]: r for r in _read_csv_rows(out)}
    # 1：ACCEPTED 面积预填，类别「有房间面积」
    assert rows["1"]["图片文字类别"] == "有房间面积"
    assert rows["1"]["房间清单"] == "主卧=12.5;客厅=20.3"
    assert "复核" in rows["1"]["备注"]
    # 2：只有房间名，面积留空
    assert rows["2"]["图片文字类别"] == "只有房间名"
    assert rows["2"]["房间清单"] == "厨房;卫生间"
    # 3：无任何标注 → 几乎无文字（非空 OCR 不算无文字）
    assert rows["3"]["图片文字类别"] == "几乎无文字"
    # 4：OCR 失败不预填
    assert rows["4"]["图片文字类别"] == ""
    assert rows["4"]["房间清单"] == ""
    assert "OCR_FAILED" in rows["4"]["备注"]
    # 5：无任务
    assert rows["5"]["图片文字类别"] == ""
    assert "无任务" in rows["5"]["备注"]
    # 6：CONFLICT/NEEDS_REVIEW 面积不预填，备注提示人工确认
    assert rows["6"]["图片文字类别"] == "只有房间名"
    assert rows["6"]["房间清单"] == "主卧"  # 不带 =面积
    assert "人工确认" in rows["6"]["备注"]


def test_write_ocr_draft_golden_labels_partial(tmp_path: Path) -> None:
    """OCR_PARTIAL 预填但备注要求重点复核。"""
    template_rows = [
        {"sample_index": "1", "asset_id": "asset-partial", "source_record_id": "R00001"}
    ]
    tasks = [{"asset_id": "asset-partial", "ocr_task_id": "t-partial", "state": "OCR_PARTIAL"}]
    annotations = [
        {
            "ocr_task_id": "t-partial",
            "room_name_raw": "客厅",
            "room_name_normalized": "客厅",
            "standard_room_type": "living_room",
            "area_value": "20",
            "parse_state": "ACCEPTED",
        },
    ]
    template_csv, ann_path, ocr_state = _write_ocr_draft_fixtures(
        tmp_path, template_rows=template_rows, tasks=tasks, annotations=annotations
    )
    out = tmp_path / "golden_label_ocr_draft.csv"
    write_ocr_draft_golden_labels(template_csv, ann_path, ocr_state, out_csv=out)
    rows = {r["sample_index"]: r for r in _read_csv_rows(out)}
    assert rows["1"]["图片文字类别"] == "有房间面积"
    assert rows["1"]["房间清单"] == "客厅=20"
    assert "部分完成" in rows["1"]["备注"]


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    import csv

    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(r) for r in csv.DictReader(fh)]
