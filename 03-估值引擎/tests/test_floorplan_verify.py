"""EXTFP3-E 自动一致性检查 / 质量报告 / 状态分类（floorplan_verify）离线测试。

全部用例离线构造 OCR 运行记录 / 词表 / 标注表 / 资产 manifest / Excel staged 表
（不触网、不付费、不调用模型），覆盖 §9.5 一致性检查、§14 质量报告、§12 状态分类、
consistency_status 回填与 verify_run 编排。
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.ingest.floorplan_asset import (
    ASSET_MANIFEST_FILENAME,
    AssetStatus,
    FloorplanAsset,
    FloorplanAssetRun,
)
from compsval.ingest.floorplan_ocr import (
    OcrRunRecord,
    OcrState,
    OcrTaskRecord,
    raw_response_filename,
)
from compsval.ingest.floorplan_ocr_contract import OCR_MODEL_ID
from compsval.ingest.floorplan_ocr_parse import (
    OcrParseRecord,
    OcrWordRecord,
    WordParseState,
    normalize_text,
    write_word_table,
)
from compsval.ingest.floorplan_transcribe import (
    AnnotationState,
    RoomAnnotationRecord,
    write_annotation_table,
)
from compsval.ingest.floorplan_verify import (
    CHECK_BATCH_UNIQUENESS,
    CHECK_BUILDING_AREA_EXCEL,
    CHECK_MODEL_MATCH,
    CHECK_MULTIPLE_AREAS,
    CHECK_REPEAT_CONSISTENCY,
    CHECK_ROOM_COUNT_EXCEL,
    CHECK_TOTAL_AREA_EXCEL,
    CHECK_WORD_EVIDENCE,
    VERIFY_VERSION,
    ExcelRoomAreaInfo,
    auto_accept_room_claim_audit,
    backfill_consistency_status,
    build_gating_metrics,
    build_status_report,
    check_accepted_word_evidence,
    check_batch_uniqueness,
    check_building_area_vs_excel,
    check_model_match,
    check_multiple_areas_conflict,
    check_repeat_consistency,
    check_room_count_vs_excel,
    check_total_area_vs_transaction,
    derive_consistency_status,
    full_asset_auto_gates,
    load_staged_excel_lookup,
    parse_count,
    parse_decimal,
    read_annotation_table,
    replay_run_annotations,
    run_offline_replay_evaluation,
    verify_run,
)

RUN_ID = "run-v-1"
TASK_A = "task-v-a"
TASK_B = "task-v-b"
LOC = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0]]
REQ_HASH = "req-hash-v-1"


# ---------------------------------------------------------------------------
# fixture 构造
# ---------------------------------------------------------------------------


def _task(
    ocr_task_id: str,
    *,
    state: OcrState = OcrState.OCR_SUCCEEDED,
    asset_id: str = "asset-v-a",
    model_returned: str | None = OCR_MODEL_ID,
    finish_reason: str | None = None,
    response_status: str | None = None,
    error_code: str | None = None,
    attempts: int = 1,
    parser_version: str = "EXTFP3-B-1.0",
) -> OcrTaskRecord:
    return OcrTaskRecord(
        ocr_task_id=ocr_task_id,
        ocr_run_id=RUN_ID,
        asset_id=asset_id,
        request_hash=REQ_HASH,
        image_sha256="img-sha256-v-1",
        state=state,
        model_returned=model_returned,
        finish_reason=finish_reason,
        response_status=response_status,
        error_code=error_code,
        attempts=attempts,
        parser_version=parser_version,
    )


def _run(
    tasks: list[OcrTaskRecord],
    *,
    state_counts: dict[str, int] | None = None,
) -> OcrRunRecord:
    return OcrRunRecord(
        ocr_run_id=RUN_ID,
        asset_manifest_ref="manifest-ref-v-1",
        sourced=True,
        created_at="2026-08-25T00:00:00Z",
        updated_at="2026-08-25T00:00:00Z",
        run_dir=".",
        state_counts=state_counts or {},
        tasks=tasks,
    )


def _word(
    text: str,
    *,
    ocr_task_id: str = TASK_A,
    order: int = 0,
    location: list[list[float]] | None = None,
    parse_state: str = WordParseState.PARSED.value,
) -> OcrWordRecord:
    return OcrWordRecord(
        word_id=f"w-{ocr_task_id}-{order}",
        ocr_run_id=RUN_ID,
        ocr_task_id=ocr_task_id,
        order=order,
        text_raw=text,
        text_normalized=normalize_text(text),
        location=LOC if location is None else location,
        parse_state=parse_state,
    )


def _parse(
    words: list[OcrWordRecord],
    *,
    ocr_task_id: str = TASK_A,
    parse_state: str = "parsed",
) -> OcrParseRecord:
    return OcrParseRecord(
        ocr_task_id=ocr_task_id,
        ocr_run_id=RUN_ID,
        source_state=OcrState.OCR_SUCCEEDED.value,
        model_requested=OCR_MODEL_ID,
        model_returned=OCR_MODEL_ID,
        model_match=True,
        words_count=len(words),
        parsed_count=len(words),
        parse_state=parse_state,
        words=words,
    )


def _ann(
    *,
    ocr_task_id: str = TASK_A,
    parse_state: str = AnnotationState.ACCEPTED.value,
    standard_room_type: str = "master_bedroom",
    room_word_id: str | None = "w-rm-1",
    area_word_id: str | None = "w-ar-1",
    area_value: str | None = "12.5",
    location: list[list[float]] | None = None,
    consistency_status: str | None = None,
    annotation_id: str | None = None,
) -> RoomAnnotationRecord:
    return RoomAnnotationRecord(
        annotation_id=annotation_id or f"a-{ocr_task_id}-{standard_room_type}",
        ocr_run_id=RUN_ID,
        ocr_task_id=ocr_task_id,
        room_word_id=room_word_id,
        area_word_id=area_word_id,
        room_name_raw="主卧",
        room_name_normalized="主卧",
        standard_room_type=standard_room_type,
        area_text_raw=f"{area_value}㎡" if area_value is not None else None,
        area_text_normalized=(f"{area_value}m2" if area_value is not None else None),
        area_value=area_value,
        area_unit="m2" if area_value is not None else None,
        location=LOC if location is None else location,
        parse_state=parse_state,
        consistency_status=consistency_status,
    )


def _write_run(tmp_path: Path, run: OcrRunRecord) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "ocr_run.json").write_text(run.model_dump_json(indent=2), encoding="utf-8")
    return run_dir


def _write_asset_manifest(tmp_path: Path, tasks: list[OcrTaskRecord]) -> Path:
    batch = tmp_path / "manifest_batch"
    batch.mkdir(parents=True, exist_ok=True)
    assets = [
        FloorplanAsset(
            asset_id=t.asset_id,
            download_task_id=f"dt-{t.asset_id}",
            source_record_id=f"R-{t.asset_id}",
            source_row_number=1,
            url_ordinal=1,
            source_url_raw=f"https://ke-image.ljcdn.com/{t.asset_id}.png",
            download_url=f"https://ke-image.ljcdn.com/{t.asset_id}.png",
            downloader_version="EXTFP2-C-DL-1.0",
            asset_status=AssetStatus.DOWNLOADED,
            mime_type="image/png",
            file_extension=".png",
            width=8,
            height=6,
            byte_size=10,
            sha256="a" * 64,
            storage_path=f"{t.asset_id}.png",
        )
        for t in tasks
    ]
    run = FloorplanAssetRun(
        batch_id="batch-v-1",
        download_run_id="dl-run-v-1",
        download_run_dir=".",
        manifest_ref="manifest-ref-v-1",
        sourced=True,
        created_at="2026-08-25T00:00:00Z",
        assets=assets,
        counts={"DOWNLOADED": len(assets)},
    )
    manifest_path = batch / ASSET_MANIFEST_FILENAME
    manifest_path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
    return manifest_path


def _write_excel_table(
    tmp_path: Path,
    rows: list[dict],
) -> Path:
    """把 ExcelRoomAreaInfo 对应行写成 staged 普通住宅 parquet（校验 lookup 用）。"""
    table = pa.Table.from_pylist(rows)
    path = tmp_path / "staged_excel.parquet"
    pq.write_table(table, path, compression="zstd")
    return path


def _excel_row(
    source_record_id: str,
    *,
    bedrooms_raw: str | None = None,
    living_rooms_raw: str | None = None,
    transaction_area_sqm: Decimal | None = None,
    building_area_detail_sqm: Decimal | None = None,
) -> dict:
    return {
        "source_record_id": source_record_id,
        "bedrooms_raw": bedrooms_raw,
        "living_rooms_raw": living_rooms_raw,
        "transaction_area_sqm": transaction_area_sqm,
        "building_area_detail_sqm": building_area_detail_sqm,
    }


# ---------------------------------------------------------------------------
# 解析辅助
# ---------------------------------------------------------------------------


def test_parse_count_and_decimal() -> None:
    assert parse_count("3室2厅") == 3
    assert parse_count("2") == 2
    assert parse_count("无") is None
    assert parse_count(None) is None
    assert parse_decimal(Decimal("12.50")) == Decimal("12.50")
    assert parse_decimal("12.5") == Decimal("12.5")
    assert parse_decimal("abc") is None
    assert parse_decimal(None) is None


def test_load_staged_excel_lookup(tmp_path: Path) -> None:
    p = _write_excel_table(
        tmp_path,
        [
            _excel_row(
                "R-asset-v-a",
                bedrooms_raw="3室2厅",
                living_rooms_raw="2厅",
                transaction_area_sqm=Decimal("80.5"),
                building_area_detail_sqm=Decimal("88.0"),
            )
        ],
    )
    lookup = load_staged_excel_lookup(p)
    info = lookup["R-asset-v-a"]
    assert info.bedrooms == 3
    assert info.living_rooms == 2
    assert info.transaction_area_sqm == Decimal("80.5")
    assert info.building_area_detail_sqm == Decimal("88.0")


# ---------------------------------------------------------------------------
# 一致性检查（§9.5）
# ---------------------------------------------------------------------------


def test_check_model_match_ok() -> None:
    res = check_model_match(_run([_task(TASK_A), _task(TASK_B)]))
    assert res.status == "ok"
    assert res.counts["matched"] == 2
    assert not res.findings


def test_check_model_match_mismatch_warns() -> None:
    res = check_model_match(_run([_task(TASK_A, model_returned="other-model")]))
    assert res.status == "warn"
    assert len(res.findings) == 1
    assert res.findings[0].check_id == CHECK_MODEL_MATCH


def test_check_model_match_missing_is_info_fail_open() -> None:
    # 缺 model_returned 不判 warn（fail-open 边界）；无可核对任务 → not_applicable
    res = check_model_match(_run([_task(TASK_A, model_returned=None)]))
    assert res.status == "not_applicable"
    assert res.counts["missing"] == 1


def test_check_batch_uniqueness_ok_and_mixed() -> None:
    ok = check_batch_uniqueness(_run([_task(TASK_A), _task(TASK_B)]), [], [])
    assert ok.status == "ok"
    mixed = check_batch_uniqueness(
        _run([_task(TASK_A, parser_version="v1"), _task(TASK_B, parser_version="v2")]),
        [],
        [],
    )
    assert mixed.status == "warn"
    assert any(f.check_id == CHECK_BATCH_UNIQUENESS for f in mixed.findings)


def test_check_batch_uniqueness_request_hash_distinct_still_ok() -> None:
    """EXTFP3-F#F1：request_hash 是请求内容哈希（同批每图天然不同），不参与唯一性 warn。

    真实联调 10 张图 10 个 request_hash 被误报 warn；修复后仅版本维度参与判定。
    """
    t_a = _task(TASK_A)
    t_b = _task(TASK_B)
    t_a.request_hash = "hash-a"
    t_b.request_hash = "hash-b"
    res = check_batch_uniqueness(_run([t_a, t_b]), [], [])
    assert res.status == "ok"
    assert res.counts["request_hash_count"] == 2
    assert res.findings == []


def test_check_accepted_word_evidence_ok() -> None:
    res = check_accepted_word_evidence([_ann()])
    assert res.status == "ok"
    assert res.counts["accepted_total"] == 1


def test_check_accepted_word_evidence_missing_warns() -> None:
    missing_room = _ann(room_word_id=None)
    missing_area = _ann(area_word_id=None)
    missing_loc = _ann(location=[])
    res = check_accepted_word_evidence([missing_room, missing_area, missing_loc])
    assert res.status == "warn"
    assert res.counts["missing_room_word"] == 1
    assert res.counts["missing_area_word"] == 1
    assert res.counts["missing_location"] == 1


def test_check_multiple_areas_conflict() -> None:
    ok = check_multiple_areas_conflict([_ann()])
    assert ok.status == "ok"
    conflict = _ann(parse_state=AnnotationState.CONFLICT.value)
    res = check_multiple_areas_conflict([conflict])
    assert res.status == "warn"
    assert res.counts["conflict"] == 1
    assert res.findings[0].check_id == CHECK_MULTIPLE_AREAS


def test_check_room_count_vs_excel_ok() -> None:
    excel = {TASK_A: ExcelRoomAreaInfo(source_record_id="R-1", bedrooms=1, living_rooms=0)}
    res = check_room_count_vs_excel([_ann(standard_room_type="master_bedroom")], excel)
    assert res.status == "ok"
    assert res.counts["compared"] == 1


def test_check_room_count_vs_excel_mismatch_and_na() -> None:
    excel = {TASK_A: ExcelRoomAreaInfo(source_record_id="R-1", bedrooms=3, living_rooms=0)}
    res = check_room_count_vs_excel([_ann(standard_room_type="master_bedroom")], excel)
    assert res.status == "warn"
    assert res.findings[0].check_id == CHECK_ROOM_COUNT_EXCEL
    na = check_room_count_vs_excel([_ann()], {})
    assert na.status == "not_applicable"


def test_check_total_area_vs_transaction() -> None:
    # 成交 12.5㎡，OCR 总面积 12.5 → ok
    excel_ok = {
        TASK_A: ExcelRoomAreaInfo(source_record_id="R-1", transaction_area_sqm=Decimal("12.5"))
    }
    ok = check_total_area_vs_transaction([_ann(area_value="12.5")], excel_ok)
    assert ok.status == "ok"
    # OCR 总面积 20 > 12.5*1.1 → warn 过度转录
    excel_over = {
        TASK_A: ExcelRoomAreaInfo(source_record_id="R-1", transaction_area_sqm=Decimal("12.5"))
    }
    over = check_total_area_vs_transaction([_ann(area_value="20")], excel_over)
    assert over.status == "warn"
    assert over.findings[0].check_id == CHECK_TOTAL_AREA_EXCEL
    # 无 Excel 信息 → not_applicable
    na = check_total_area_vs_transaction([_ann(area_value="12.5")], {})
    assert na.status == "not_applicable"


def test_check_building_area_vs_excel() -> None:
    excel = {
        TASK_A: ExcelRoomAreaInfo(source_record_id="R-1", building_area_detail_sqm=Decimal("88.0"))
    }
    # OCR 建筑面积 88.0 → ok
    words_ok = [_word("建筑面积 88㎡", order=0)]
    ok = check_building_area_vs_excel(words_ok, excel)
    assert ok.status == "ok"
    # OCR 建筑面积 120㎡ 与 88 冲突 → warn
    words_bad = [_word("建筑面积 120㎡", order=0)]
    bad = check_building_area_vs_excel(words_bad, excel)
    assert bad.status == "warn"
    assert bad.findings[0].check_id == CHECK_BUILDING_AREA_EXCEL
    # 词表无建筑面积词条 → not_applicable
    na = check_building_area_vs_excel([_word("主卧", order=0)], excel)
    assert na.status == "not_applicable"


def test_check_repeat_consistency_ok() -> None:
    anns_a = [
        _ann(standard_room_type="master_bedroom", area_value="12.5"),
        _ann(
            standard_room_type="secondary_bedroom",
            area_value="8.0",
            ocr_task_id=TASK_B,
            annotation_id="a-B",
        ),
    ]
    anns_b = [
        _ann(standard_room_type="master_bedroom", area_value="12.5"),
        _ann(
            standard_room_type="secondary_bedroom",
            area_value="8.0",
            ocr_task_id=TASK_B,
            annotation_id="a-B",
        ),
    ]
    res = check_repeat_consistency(anns_a, anns_b)
    assert res.status == "ok"
    assert res.counts["matched_fields"] == res.counts["total_fields"]


def test_check_repeat_consistency_mismatch() -> None:
    anns_a = [_ann(standard_room_type="master_bedroom", area_value="12.5")]
    anns_b = [_ann(standard_room_type="master_bedroom", area_value="15.0")]
    res = check_repeat_consistency(anns_a, anns_b)
    assert res.status == "warn"
    assert res.findings[0].check_id == CHECK_REPEAT_CONSISTENCY
    assert res.counts["mismatched_tasks"] == 1


def test_check_repeat_consistency_no_common_na() -> None:
    res = check_repeat_consistency([], [])
    assert res.status == "not_applicable"


def test_check_repeat_consistency_duplicate_fields_multiset() -> None:
    # RV-EXTFP3-E-01#F2：同一任务内同类型同面积的字段重复出现且次数不同时，
    # 必须按多重集比较（A=2 间同款、B=1 间 → 一致率 50% warn），不得误报 100%。
    anns_a = [
        _ann(standard_room_type="master_bedroom", area_value="12.5", annotation_id="a-1"),
        _ann(standard_room_type="master_bedroom", area_value="12.5", annotation_id="a-2"),
    ]
    anns_b = [
        _ann(standard_room_type="master_bedroom", area_value="12.5", annotation_id="a-1"),
    ]
    res = check_repeat_consistency(anns_a, anns_b)
    assert res.status == "warn"
    assert res.counts["matched_fields"] == 1
    assert res.counts["total_fields"] == 2
    assert res.counts["mismatched_tasks"] == 1
    assert len(res.findings) == 1


# ---------------------------------------------------------------------------
# consistency_status 派生与回填
# ---------------------------------------------------------------------------


def test_derive_consistency_status_mapping() -> None:
    assert derive_consistency_status(_ann()) == "OK"
    assert derive_consistency_status(_ann(room_word_id=None)) == "REVIEW_MISSING_EVIDENCE"
    assert derive_consistency_status(_ann(location=[])) == "REVIEW_MISSING_EVIDENCE"
    assert (
        derive_consistency_status(_ann(parse_state=AnnotationState.CONFLICT.value))
        == "CONFLICT_MULTIPLE_AREAS"
    )
    assert (
        derive_consistency_status(_ann(parse_state=AnnotationState.ROOM_ONLY.value))
        == "ROOM_ONLY_NO_AREA"
    )
    assert (
        derive_consistency_status(_ann(parse_state=AnnotationState.NEEDS_REVIEW.value)) == "REVIEW"
    )


def test_backfill_consistency_status_preserves_fields() -> None:
    original = _ann()
    backfilled = backfill_consistency_status([original])[0]
    assert backfilled.consistency_status == "OK"
    # 原始字段不被覆盖
    assert backfilled.room_word_id == original.room_word_id
    assert backfilled.area_value == original.area_value
    assert original.consistency_status is None


# ---------------------------------------------------------------------------
# 状态分类（§12）
# ---------------------------------------------------------------------------


def test_build_status_report_classifies_failures() -> None:
    tasks = [
        _task(TASK_A, state=OcrState.OCR_SUCCEEDED),
        _task(
            TASK_B,
            state=OcrState.OCR_FAILED,
            error_code="http-429",
            response_status="http-429",
        ),
        _task("task-v-c", state=OcrState.NEEDS_REVIEW, model_returned=None),
        _task("task-v-d", state=OcrState.OCR_PENDING, attempts=0),
    ]
    report = build_status_report(_run(tasks))
    assert report.tasks_total == 4
    assert report.state_counts[OcrState.OCR_SUCCEEDED.value] == 1
    assert report.state_counts[OcrState.OCR_FAILED.value] == 1
    assert report.state_counts[OcrState.NEEDS_REVIEW.value] == 1
    assert report.tasks_terminal == 3
    failed_ids = {f.ocr_task_id for f in report.failures}
    assert TASK_B in failed_ids
    assert "task-v-c" in failed_ids
    retry_ids = {t.ocr_task_id for t in report.retryable_tasks}
    assert TASK_B in retry_ids  # http-429 可重试
    assert "task-v-d" in retry_ids  # pending 可重试
    assert TASK_A not in retry_ids  # succeeded 终态不可重试


def test_build_status_report_finish_reason_length_retryable() -> None:
    # 真实路径（floorplan_ocr.py）：finish_reason=length → error_code=finish_reason_length
    # + 状态迁移为 OCR_PARTIAL（RV-EXTFP3-E-01#F1，用真实状态组合）
    task = _task(
        TASK_A,
        state=OcrState.OCR_PARTIAL,
        error_code="finish_reason_length",
        finish_reason="length",
    )
    report = build_status_report(_run([task]))
    assert len(report.retryable_tasks) == 1
    assert report.state_counts[OcrState.OCR_PARTIAL.value] == 1
    assert {t.ocr_task_id for t in report.failures} == {TASK_A}  # partial 计入失败分类


def test_build_status_report_ocr_partial_length_retryable_without_error_code() -> None:
    # EXTFP3-H#F10：历史运行记录的 OCR_PARTIAL 只回填 finish_reason=length、
    # 未回填 error_code（ocr_run.json 实测 2 条）。finish_reason 本身即可重试证据，
    # 不应因 error_code 缺失判为不可重试（否则 H10 失败不可重试/分类/追溯）。
    task = _task(TASK_A, state=OcrState.OCR_PARTIAL, finish_reason="length")
    report = build_status_report(_run([task]))
    assert len(report.retryable_tasks) == 1
    assert {t.ocr_task_id for t in report.retryable_tasks} == {TASK_A}


def test_build_status_report_raw_present_truncated_filename(tmp_path: Path) -> None:
    # EXTFP3-H#F10 回归：MAX_PATH 修复后原始响应文件名使用截断 task_id（24 hex），
    # build_status_report 须按截断文件名判定 raw_response_present；旧的全 64-hex 文件名
    # 在该路径不存在，否则 H10 会把已落盘响应误判为缺失（实测 0/3 可追溯）。
    long_task_id = "a" * 64  # 真实 ocr_task_id 为 64-hex；截断后与全量文件名不同
    task = _task(long_task_id, state=OcrState.OCR_PARTIAL, finish_reason="length")
    run = _run([task])
    run_dir = _write_run(tmp_path, run)
    # 截断文件名落盘
    raw = run_dir / raw_response_filename(task.ocr_task_id)
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("{}", encoding="utf-8")
    report = build_status_report(run, run_dir=run_dir)
    assert len(report.failures) == 1
    assert report.failures[0].raw_response_present is True
    # 旧的全 64-hex 文件名不存在（截断≠全量）
    assert not (run_dir / f"raw_response_{task.ocr_task_id}.json").exists()


# ---------------------------------------------------------------------------
# verify_run 编排（§14 质量报告 + §10.3 门禁指标）
# ---------------------------------------------------------------------------


def test_verify_run_end_to_end(tmp_path: Path) -> None:
    # 一次 OCR 运行：1 张图成功，词表 + 标注表齐全，Excel 一致
    task = _task(TASK_A, asset_id="asset-v-a")
    run = _run([task], state_counts={OcrState.OCR_SUCCEEDED.value: 1})
    run_dir = _write_run(tmp_path, run)

    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    words = [
        _word("主卧", order=0),
        _word("12.5㎡", order=1),
        _word("建筑面积 88㎡", order=2),
    ]
    write_word_table([_parse(words)], data_dir)
    anns = [
        _ann(
            standard_room_type="master_bedroom",
            area_value="12.5",
            room_word_id="w-task-v-a-0",
            area_word_id="w-task-v-a-1",
        )
    ]
    write_annotation_table(anns, data_dir)

    manifest_path = _write_asset_manifest(tmp_path, [task])
    excel_path = _write_excel_table(
        tmp_path,
        [
            _excel_row(
                "R-asset-v-a",
                bedrooms_raw="1室",
                living_rooms_raw="0厅",
                transaction_area_sqm=Decimal("12.5"),
                building_area_detail_sqm=Decimal("88.0"),
            )
        ],
    )

    report = verify_run(
        run_dir,
        data_dir=data_dir,
        asset_manifest_path=manifest_path,
        staged_table_path=excel_path,
    )
    assert report.ocr_run_id == RUN_ID
    assert report.verify_version == VERIFY_VERSION
    assert report.overall in {"ok", "pass", "warn", "blocked"}
    # 各检查项存在
    by_id = {c.check_id: c.status for c in report.checks}
    assert CHECK_MODEL_MATCH in by_id
    assert CHECK_BATCH_UNIQUENESS in by_id
    assert CHECK_WORD_EVIDENCE in by_id
    assert CHECK_MULTIPLE_AREAS in by_id
    assert CHECK_ROOM_COUNT_EXCEL in by_id
    assert CHECK_TOTAL_AREA_EXCEL in by_id
    assert CHECK_BUILDING_AREA_EXCEL in by_id
    assert CHECK_REPEAT_CONSISTENCY in by_id
    assert by_id[CHECK_REPEAT_CONSISTENCY] == "not_applicable"
    # §14 质量字段
    assert report.quality["ocr_run_id"] == RUN_ID
    assert report.quality["words"] == 3
    assert report.quality["annotations_total"] == 1
    assert report.quality["state_counts"][OcrState.OCR_SUCCEEDED.value] == 1
    assert report.quality["versions"]["model"] == OCR_MODEL_ID
    # §10.3 门禁指标
    assert report.gating["accepted_field_evidence_rate"] == 1.0
    assert report.gating["repeat_run_consistency_rate"] is None
    assert report.gating["gating_decision"] == "pending_H"


def test_verify_run_with_repeat_and_consistency_backfill(tmp_path: Path) -> None:
    task = _task(TASK_A, asset_id="asset-v-a")
    run = _run([task])
    run_dir = _write_run(tmp_path, run)
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    words = [_word("主卧", order=0), _word("12.5㎡", order=1)]
    write_word_table([_parse(words)], data_dir)
    anns = [_ann(room_word_id="w-task-v-a-0", area_word_id="w-task-v-a-1")]
    write_annotation_table(anns, data_dir)

    report = verify_run(
        run_dir,
        data_dir=data_dir,
        repeat_annotations=anns,
        write_consistency=True,
    )
    repeat = next(c for c in report.checks if c.check_id == CHECK_REPEAT_CONSISTENCY)
    assert repeat.status == "ok"
    assert report.quality.get("consistency_backfilled") is True
    # 回填落盘可读回
    from compsval.ingest.floorplan_transcribe import (
        ANNOTATION_STAGED_FILENAME,
    )

    reread = read_annotation_table(data_dir / "staged" / ANNOTATION_STAGED_FILENAME)
    assert reread[0].consistency_status == "OK"


def test_verify_run_all_not_applicable_overall(tmp_path: Path) -> None:
    # RV-EXTFP3-E-01#F4：空运行/无词表/无标注/无 Excel 比对输入 → 全 not_applicable，
    # overall 应为 not_applicable，不得冒充 ok。
    run_dir = _write_run(tmp_path, _run([]))
    report = verify_run(run_dir, data_dir=tmp_path / "data")
    assert report.overall == "not_applicable"
    by_id = {c.check_id: c.status for c in report.checks}
    assert all(s == "not_applicable" for s in by_id.values())


def test_build_gating_metrics_mapping() -> None:
    checks = [
        check_accepted_word_evidence([_ann()]),
        check_repeat_consistency([_ann()], [_ann()]),
        check_model_match(_run([_task(TASK_A)])),
    ]
    metrics = build_gating_metrics(checks, [_ann()], [_word("主卧")])
    assert metrics["accepted_field_evidence_rate"] == 1.0
    assert metrics["repeat_run_consistency_rate"] == 1.0
    assert metrics["model_match_ok"] is True
    assert metrics["word_participation_rate"] == 0.0
    assert metrics["gating_decision"] == "pending_H"


# ---------------------------------------------------------------------------
# OCRNEXT-C 离线回放与全量自动门禁（合同 §5-C / 方案 §6.3/§7.0/§7.1）
# ---------------------------------------------------------------------------


def _words_body(words: list[dict]) -> dict:
    return {"ocr_result": {"words_info": words}}


def _wi(text: str, x: float, y: float) -> dict:
    return {
        "text": text,
        "location": [[x, y], [x + 10, y], [x + 10, y + 10], [x, y + 10]],
    }


def _write_replay_run(
    base: Path,
    run_name: str,
    specs: list[tuple[str, list[dict] | None]],
    *,
    use_full_hex_names: bool = False,
) -> Path:
    """写一个可回放的冻结运行：ocr_run.json + 逐任务原始响应（含真实 SHA256）。

    specs=[(ocr_task_id, words_info|None)...]；None 表示网络失败无响应文件。
    """
    import hashlib
    import json as _json

    run_dir = base / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    tasks: list[OcrTaskRecord] = []
    counts: dict[str, int] = {}
    for task_id, words in specs:
        base_task = OcrTaskRecord(
            ocr_task_id=task_id,
            ocr_run_id=run_name,
            asset_id=f"asset-{task_id[:8]}",
            image_sha256=f"sha-{task_id}",
            width=400,
            height=300,
            model_requested=OCR_MODEL_ID,
            request_hash="req-hash-replay",
        )
        if words is None:
            base_task.state = OcrState.OCR_FAILED
            base_task.error_code = "network_error"
        else:
            payload = _json.dumps(_words_body(words), ensure_ascii=False).encode("utf-8")
            digest = hashlib.sha256(payload).hexdigest()
            fname = (
                f"raw_response_{task_id}.json"
                if use_full_hex_names
                else raw_response_filename(task_id)
            )
            (run_dir / fname).write_bytes(payload)
            base_task.state = OcrState.OCR_SUCCEEDED
            base_task.model_returned = OCR_MODEL_ID
            base_task.raw_response_path = fname
            base_task.raw_response_sha256 = digest
        tasks.append(base_task)
        counts[base_task.state.value] = counts.get(base_task.state.value, 0) + 1
    record = OcrRunRecord(
        ocr_run_id=run_name,
        asset_manifest_ref="manifest-ref-replay",
        sourced=True,
        created_at="2026-08-28T00:00:00Z",
        updated_at="2026-08-28T00:00:00Z",
        run_dir=run_dir.as_posix(),
        state_counts=counts,
        tasks=tasks,
    )
    (run_dir / "ocr_run.json").write_text(record.model_dump_json(indent=2), encoding="utf-8")
    return run_dir


def test_replay_run_annotations_basic(tmp_path: Path) -> None:
    """回放读取运行目录：有响应任务转录出标注，无响应任务计数且不伪造。"""
    run_dir = _write_replay_run(
        tmp_path,
        "run-replay-1",
        [
            ("a" * 64, [_wi("主卧", 10, 10), _wi("12.5㎡", 12, 22)]),
            ("b" * 64, None),
        ],
    )
    ann_by_task, words_by_task, stats = replay_run_annotations(run_dir)
    assert stats["tasks_with_response"] == 1
    assert stats["tasks_without_response"] == 1
    assert stats["total_words"] == 2
    accepted = [a for a in ann_by_task["a" * 64] if a.parse_state == AnnotationState.ACCEPTED.value]
    assert len(accepted) == 1
    assert accepted[0].area_value == "12.5"
    assert ann_by_task["b" * 64] == []
    assert words_by_task["b" * 64] == []


def test_replay_supports_full_hex_raw_names_g_run(tmp_path: Path) -> None:
    """G 运行形态（64-hex 全名响应文件）定位兼容。"""
    task_id = "c" * 64
    run_dir = _write_replay_run(
        tmp_path,
        "run-replay-g",
        [(task_id, [_wi("客厅", 5, 5), _wi("20㎡", 6, 16)])],
        use_full_hex_names=True,
    )
    ann_by_task, _words, stats = replay_run_annotations(run_dir)
    assert stats["tasks_with_response"] == 1
    accepted = [a for a in ann_by_task[task_id] if a.parse_state == AnnotationState.ACCEPTED.value]
    assert len(accepted) == 1


def test_run_offline_replay_evaluation_isolates_divergent_field(tmp_path: Path) -> None:
    """双轮回放评估：跨运行不一致字段被隔离且不进最终 accepted；一致字段保留；
    六项门禁全过；产物入独立 evaluation 目录；旧运行资产哈希不变。"""
    import hashlib

    task_div = "d" * 64  # 分歧字段：primary 卧室E=18.3 / reference 卧室E=8.9
    task_agree = "e" * 64  # 一致字段：两运行合并词「主卧 12.5㎡」
    primary_dir = _write_replay_run(
        tmp_path,
        "run-primary",
        [
            (task_div, [_wi("卧室E", 10, 10), _wi("18.3㎡", 12, 22)]),
            (task_agree, [_wi("主卧 12.5㎡", 30, 30)]),
        ],
    )
    reference_dir = _write_replay_run(
        tmp_path,
        "run-reference",
        [
            (task_div, [_wi("卧室E", 10, 10), _wi("8.9㎡", 12, 22)]),
            (task_agree, [_wi("主卧 12.5㎡", 30, 30)]),
        ],
    )
    before_hashes = {
        p: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(list(primary_dir.rglob("*")) + list(reference_dir.rglob("*")))
        if p.is_file()
    }

    report = run_offline_replay_evaluation(
        primary_dir, reference_dir, tmp_path / "eval", evaluation_id="oc-test-1"
    )
    assert report["gates_all_ok"] is True, report["gates"]
    assert report["isolation"]["primary_isolated_total"] == 1
    assert report["isolation"]["reference_direction_isolated_total"] == 1
    iso = json_loads(tmp_path / "eval" / "evaluation_oc-test-1" / "isolation_list.json")
    assert iso[0]["area_value"] == "18.3"
    assert iso[0]["ocr_task_id"] == task_div

    # 最终标注表：18.3 已隔离为 NEEDS_REVIEW；一致的 12.5 保持 ACCEPTED
    table = pq.read_table(report["artifacts"]["annotation_table"])
    rows = table.to_pylist()
    divergent = [r for r in rows if r["area_value"] == "18.3"]
    assert len(divergent) == 1
    assert divergent[0]["parse_state"] == "NEEDS_REVIEW"
    assert divergent[0]["isolation_reason"] == "cross_run_inconsistent"
    agreeing = [r for r in rows if r["area_value"] == "12.5"]
    assert len(agreeing) == 1 and agreeing[0]["parse_state"] == "ACCEPTED"

    # 旧运行资产零改写（门禁 6 实证）
    after_hashes = {
        p: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(list(primary_dir.rglob("*")) + list(reference_dir.rglob("*")))
        if p.is_file()
    }
    assert after_hashes == before_hashes
    assert (tmp_path / "eval" / "evaluation_oc-test-1" / "replay_report.json").is_file()


def json_loads(path: Path) -> list:
    import json as _json

    return _json.loads(path.read_text(encoding="utf-8"))


def test_gate5_detects_unisolated_excess_accepted(tmp_path: Path) -> None:
    """门禁 5 抓漏：未经隔离的超出对照 accepted 必须 fail。"""
    task_div = "f" * 64
    primary_dir = _write_replay_run(
        tmp_path,
        "run-p5-p",
        [(task_div, [_wi("卧室F", 10, 10), _wi("18.3㎡", 12, 22)])],
    )
    reference_dir = _write_replay_run(
        tmp_path,
        "run-p5-r",
        [(task_div, [_wi("卧室F", 10, 10), _wi("8.9㎡", 12, 22)])],
    )
    prim_by_task, prim_words, _ = replay_run_annotations(primary_dir)
    ref_by_task, _, _ = replay_run_annotations(reference_dir)
    # 故意不做隔离（绕过修复③）→ 主运行 accepted「18.3」超出对照 accepted
    gates = full_asset_auto_gates(
        primary_dir,
        reference_dir,
        final_annotations=list(prim_by_task[task_div]),
        reference_annotations=list(ref_by_task[task_div]),
        words_by_task=prim_words,
    )
    by_id = {g["gate_id"]: g for g in gates}
    assert by_id["cross_run_isolation_complete"]["status"] == "fail"


def test_replay_all_failed_run_gates_pass_vacuously(tmp_path: Path) -> None:
    """全失败无响应的运行：门禁空真成立（无响应被允许、零 accepted）。"""
    run_dir = _write_replay_run(tmp_path, "run-allfail", [("a" * 64, None), ("b" * 64, None)])
    ann_by_task, words_by_task, stats = replay_run_annotations(run_dir)
    assert stats["tasks_without_response"] == 2
    report = run_offline_replay_evaluation(
        run_dir, run_dir, tmp_path / "eval2", evaluation_id="oc-empty"
    )
    assert report["gates_all_ok"] is True, report["gates"]
    assert report["adversarial_oos"] is None
    assert report["isolation"]["primary_isolated_total"] == 0
    assert all(not anns for anns in ann_by_task.values())
    assert not words_by_task["a" * 64]


def test_replay_adversarial_oos_block_discloses_without_gating(tmp_path: Path) -> None:
    """oos_csv 给出时报告含 adversarial_oos 块：逐样本披露 accepted 形态、
    未映射样本计数不伪造、默认（不传）为 None。"""
    task_oos = "e" * 64
    task_plain = "a" * 64
    specs = [
        (task_oos, [_wi("主卧", 10, 10), _wi("12.5㎡", 12, 22)]),
        (task_plain, [_wi("客厅", 40, 40)]),
    ]
    primary_dir = _write_replay_run(tmp_path, "run-oos-p", specs)
    oos_csv = tmp_path / "oos.csv"
    oos_csv.write_text(
        "sample_index,asset_id,oos_reason\n"
        f"21,asset-{task_oos[:8]},多层户型\n"
        "46,asset-missing-xx,多层户型\n",
        encoding="utf-8",
    )
    report = run_offline_replay_evaluation(
        primary_dir,
        primary_dir,
        tmp_path / "eval-oos",
        evaluation_id="oc-oos",
        oos_csv=oos_csv,
    )
    block = report["adversarial_oos"]
    assert block is not None
    assert block["role"] == "adversarial_oos"
    assert block["totals"]["samples"] == 2
    assert block["totals"]["unmapped"] == 1
    assert block["totals"]["accepted_room_claims"] == 1
    assert block["totals"]["accepted_areas"] == 1
    mapped = [s for s in block["samples"] if s["mapped_in_run"]]
    assert len(mapped) == 1
    assert mapped[0]["sample_index"] == "21"
    assert mapped[0]["ocr_task_id"] == task_oos
    assert mapped[0]["state_counts"].get("ACCEPTED") == 1
    assert report["gates_all_ok"] is True, report["gates"]
    assert report["isolation"]["primary_isolated_total"] == 0


def test_replay_corrupt_response_counts_unparseable_without_fabrication(
    tmp_path: Path,
) -> None:
    """RV-OCRNEXT-C-01#F7①：响应文件存在但内容损坏 → 计 unparseable、零转录零伪造。"""
    task_ok = "a" * 64
    task_bad = "b" * 64
    run_dir = _write_replay_run(
        tmp_path,
        "run-corrupt",
        [
            (task_ok, [_wi("客厅", 5, 5), _wi("20㎡", 6, 16)]),
            (task_bad, [_wi("主卧", 10, 10), _wi("12.5㎡", 12, 22)]),
        ],
    )
    (run_dir / raw_response_filename(task_bad)).write_bytes(b"{not-json")
    ann_by_task, words_by_task, stats = replay_run_annotations(run_dir)
    assert stats["tasks_with_response"] == 1
    assert stats["tasks_unparseable_response"] == 1
    assert ann_by_task[task_bad] == []
    assert words_by_task[task_bad] == []
    assert ann_by_task[task_ok] != []


def test_auto_accept_room_claim_audit_candidate_layer_only(tmp_path: Path) -> None:
    """RV-OCRNEXT-C-01#F7②：候选层审计只计 ACCEPTED/ROOM_ONLY、按 room_word_id
    去重、与黄金 std 计数取 min 匹配。"""
    task_id = "a" * 64
    run_dir = _write_replay_run(
        tmp_path,
        "run-audit",
        [(task_id, [_wi("主卧", 10, 10), _wi("12.5㎡", 12, 22)])],
    )
    golden_csv = tmp_path / "golden.csv"
    golden_csv.write_text(
        "sample_index,asset_id,source_record_id,区县,成交年份,居室数,"
        "图片文字类别,文字质量,房间清单,备注\n"
        f"1,asset-{'a' * 8},,,,,有房间面积,,主卧=12.5;客厅=20.0,已确认\n",
        encoding="utf-8",
    )

    def _ann(ann_id: str, std: str, room_word: str, state: str) -> RoomAnnotationRecord:
        return RoomAnnotationRecord(
            annotation_id=ann_id,
            ocr_run_id=RUN_ID,
            ocr_task_id=task_id,
            room_word_id=room_word,
            room_name_normalized=std,
            standard_room_type=std,
            parse_state=state,
        )

    audit = auto_accept_room_claim_audit(
        [
            _ann("ann1", "master_bedroom", "w1", AnnotationState.ACCEPTED.value),
            _ann("ann2", "master_bedroom", "w2", AnnotationState.ROOM_ONLY.value),
            _ann("ann3", "master_bedroom", "w1", AnnotationState.ACCEPTED.value),
            _ann("ann4", "bathroom", "w9", AnnotationState.CONFLICT.value),
        ],
        golden_csv,
        run_dir,
    )
    assert audit["scope_golden_tasks"] == 1
    assert audit["auto_accept_claims"] == 2
    assert audit["auto_accept_matched"] == 1
    assert audit["auto_accept_room_precision"] == 0.5
