"""EXTFP3-C OCR 响应解析与逐词表（floorplan_ocr_parse）的离线测试。

全部用例离线构造响应体（脱敏，不触网、不付费、不调用真实 Qwen）；覆盖四类行为：
正常解析 / 边界（top-level、content[] 图像项、递归兜底、空 words_info）/
缺失与失败分类（无 words、location 非法、模型不一致、源状态沿传）/ 反例与可解析率
统计与 parquet 落盘 roundtrip。
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq

from compsval.ingest.floorplan_ocr import (
    OCR_MODEL_ID,
    OcrRunRecord,
    OcrState,
    OcrTaskRecord,
    raw_response_filename,
)
from compsval.ingest.floorplan_ocr_parse import (
    COORD_VERSION,
    OCR_RESPONSE_PARSE_VERSION,
    WORD_STAGED_FILENAME,
    WordParseState,
    build_word_records,
    compute_parseability_stats,
    extract_words_info,
    normalize_text,
    parse_ocr_run_directory,
    parse_response_body,
    write_word_table,
)

WORDS_OK = [
    {
        "text": "主卧",
        "location": [[1, 1], [2, 1], [2, 2], [1, 2]],
        "rotate_rect": {"center": [1.5, 1.5], "width": 10.0, "height": 10.0, "angle": 0.0},
    },
    {"text": "12.5㎡", "location": [[5, 5], [8, 5], [8, 6], [5, 6]]},
]


def _words_body(
    words: list[dict],
    *,
    model: str | None = None,
    finish_reason: str = "stop",
) -> dict:
    return {
        "output": {
            "choices": [
                {
                    "finish_reason": finish_reason,
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"text": "ignored-prose"},
                            {"ocr_result": {"words_info": words}},
                        ],
                    },
                }
            ]
        },
        "usage": {"input_tokens": 1520, "output_tokens": 30},
        "request_id": "req-c-001",
        "model": model or OCR_MODEL_ID,
    }


def _task(
    ocr_task_id: str = "task-c-1", *, state: OcrState = OcrState.OCR_SUCCEEDED
) -> OcrTaskRecord:
    return OcrTaskRecord(
        ocr_task_id=ocr_task_id,
        ocr_run_id="run-c-1",
        asset_id="A1",
        image_sha256="img-sha-c",
        width=100,
        height=80,
        model_returned=OCR_MODEL_ID if state is OcrState.OCR_SUCCEEDED else None,
        request_hash="req-hash-c",
        state=state,
    )


# ---------------------------------------------------------------------------
# 文本归一化（§9.4-1）
# ---------------------------------------------------------------------------


def test_normalize_text_nfkc_and_whitespace() -> None:
    # 全角字母/数字 NFKC → 半角；连续空白折叠；首尾去空白
    assert normalize_text("ＡＢＣ　１２") == "ABC 12"
    assert normalize_text("  主卧   室 ") == "主卧 室"  # 空白折叠为单个空格
    # NFKC 把 ㎡/m² 规范为 m2（D 阶段面积单位归一化在此基础上识别 m2/平方米）
    assert normalize_text("㎡") == "m2"
    assert normalize_text("") == ""


# ---------------------------------------------------------------------------
# words_info 容错提取（§9.3）
# ---------------------------------------------------------------------------


def test_extract_from_output_choices_content() -> None:
    body = _words_body(WORDS_OK)
    assert extract_words_info(body) == WORDS_OK


def test_extract_from_top_level_ocr_result() -> None:
    body = {"ocr_result": {"words_info": WORDS_OK}}
    assert extract_words_info(body) == WORDS_OK


def test_extract_recursive_fallback() -> None:
    body = {"output": {"choices": [{"message": {"content": {"nested": {"words_info": WORDS_OK}}}}]}}
    assert extract_words_info(body) == WORDS_OK


def test_extract_missing_words_info_returns_empty() -> None:
    assert extract_words_info({"output": {"choices": [{"message": {"content": []}}]}}) == []
    assert extract_words_info({"output": {}}) == []
    assert extract_words_info({}) == []
    assert extract_words_info([]) == []  # 非 dict 体
    # words_info 存在但形状错误（非 list / 无 text）→ 空
    assert extract_words_info({"ocr_result": {"words_info": "not-a-list"}}) == []
    assert extract_words_info({"ocr_result": {"words_info": [{"x": 1}]}}) == []


# ---------------------------------------------------------------------------
# 位置规范化（§9.3）
# ---------------------------------------------------------------------------


def test_normalize_location_forms() -> None:
    from compsval.ingest.floorplan_ocr_parse import _normalize_location

    assert _normalize_location([[1, 2], [3, 4]]) == [[1.0, 2.0], [3.0, 4.0]]
    assert _normalize_location({"x": 1, "y": 2}) == [[1.0, 2.0]]
    assert _normalize_location([]) is None
    assert _normalize_location([[1, 2, 3]]) is None
    assert _normalize_location([[1, "a"]]) is None
    assert _normalize_location("nope") is None
    assert _normalize_location(None) is None


def test_normalize_location_flat_polygon() -> None:
    """EXTFP3-C#F6：真实 Qwen advanced_recognition 的 location 为平铺 8 数四角多边形。"""
    from compsval.ingest.floorplan_ocr_parse import _normalize_location

    assert _normalize_location([582, 29, 851, 29, 851, 61, 582, 61]) == [
        [582.0, 29.0],
        [851.0, 29.0],
        [851.0, 61.0],
        [582.0, 61.0],
    ]
    assert _normalize_location([1, 2, 3, 4, 5, 6]) == [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
    # 平铺但奇数个数 / 含非数值 / 含 bool → 非法
    assert _normalize_location([1, 2, 3]) is None
    assert _normalize_location([1, "a", 3, 4]) is None
    assert _normalize_location([True, 2, 3, 4]) is None


def test_normalize_rotate_rect() -> None:
    from compsval.ingest.floorplan_ocr_parse import _normalize_rotate_rect

    assert _normalize_rotate_rect({"center": [1, 2], "width": 3, "height": 4, "angle": 5}) == {
        "center": [[1.0, 2.0]],
        "width": 3.0,
        "height": 4.0,
        "angle": 5.0,
    }
    assert _normalize_rotate_rect({"width": 3}) is None  # 缺 center
    assert _normalize_rotate_rect("nope") is None
    assert _normalize_rotate_rect(None) is None


def test_normalize_rotate_rect_flat_5() -> None:
    """EXTFP3-C#F6：真实 Qwen advanced_recognition 的 rotate_rect 为平铺 5 数
    [cx, cy, w, h, angle]。"""
    from compsval.ingest.floorplan_ocr_parse import _normalize_rotate_rect

    assert _normalize_rotate_rect([716, 45, 32, 269, 90]) == {
        "center": [[716.0, 45.0]],
        "width": 32.0,
        "height": 269.0,
        "angle": 90.0,
    }
    assert _normalize_rotate_rect([716, 45, 32]) is None  # 长度不是 5
    assert _normalize_rotate_rect([716, 45, 32, 269, "x"]) is None


# ---------------------------------------------------------------------------
# 逐词构建（§7.3）
# ---------------------------------------------------------------------------


def test_build_word_records_ok() -> None:
    words = build_word_records(
        WORDS_OK,
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        image_width=100,
        image_height=80,
    )
    assert len(words) == 2
    first = words[0]
    assert first.order == 0
    assert first.text_raw == "主卧"
    assert first.text_normalized == "主卧"
    assert first.location == [[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 2.0]]
    assert first.rotate_rect == {
        "center": [[1.5, 1.5]],
        "width": 10.0,
        "height": 10.0,
        "angle": 0.0,
    }
    assert first.image_width == 100
    assert first.image_height == 80
    assert first.coord_version == COORD_VERSION
    assert first.parse_state == WordParseState.PARSED.value
    assert first.participates_in_field is None
    assert (
        first.word_id
        == build_word_records(
            WORDS_OK, ocr_run_id="run-c-1", ocr_task_id="task-c-1", image_width=100, image_height=80
        )[0].word_id
    )  # 确定性可复现


def test_build_word_records_bad_location_needs_review() -> None:
    words = build_word_records(
        [{"text": "面积", "location": "invalid"}],
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        image_width=100,
        image_height=80,
    )
    assert len(words) == 1
    assert words[0].parse_state == WordParseState.NEEDS_REVIEW.value


def test_build_word_records_real_qwen_flat_shape() -> None:
    """EXTFP3-C#F6 回归：真实 Qwen advanced_recognition 的 location=平铺 8 数、
    rotate_rect=平铺 5 数，应规范为 PARSED（此前会被误判 NEEDS_REVIEW/partial）。"""
    words = build_word_records(
        [
            {
                "text": "主卧",
                "location": [582, 29, 851, 29, 851, 61, 582, 61],
                "rotate_rect": [716, 45, 32, 269, 90],
            },
            {"text": "17.5m²", "location": [671, 658, 744, 658, 744, 681, 671, 681]},
        ],
        ocr_run_id="run-f-1",
        ocr_task_id="task-f-1",
        image_width=1440,
        image_height=1080,
    )
    assert len(words) == 2
    assert all(w.parse_state == WordParseState.PARSED.value for w in words)
    assert words[0].location == [[582.0, 29.0], [851.0, 29.0], [851.0, 61.0], [582.0, 61.0]]
    assert words[0].rotate_rect == {
        "center": [[716.0, 45.0]],
        "width": 32.0,
        "height": 269.0,
        "angle": 90.0,
    }
    assert words[1].location == [[671.0, 658.0], [744.0, 658.0], [744.0, 681.0], [671.0, 681.0]]
    assert words[0].text_raw == "主卧"  # 保留原文


def test_build_word_records_drops_bad_items() -> None:
    words = build_word_records(
        [{"text": 123}, {"no_text": "x"}, "plain", WORDS_OK[0]],
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        image_width=100,
        image_height=80,
    )
    assert len(words) == 1
    assert words[0].text_raw == "主卧"


# ---------------------------------------------------------------------------
# 响应级解析与分类（§9.3 / §9.5）
# ---------------------------------------------------------------------------


def test_parse_response_body_parsed() -> None:
    rec = parse_response_body(
        _words_body(WORDS_OK),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_SUCCEEDED.value,
        model_requested=OCR_MODEL_ID,
        model_returned=OCR_MODEL_ID,
        image_width=100,
        image_height=80,
    )
    assert rec.parse_state == "parsed"
    assert rec.words_count == 2
    assert rec.parsed_count == 2
    assert rec.model_match is True
    assert rec.parse_version == OCR_RESPONSE_PARSE_VERSION
    assert rec.model_returned == OCR_MODEL_ID


def test_parse_response_body_model_mismatch_needs_review() -> None:
    rec = parse_response_body(
        _words_body(WORDS_OK, model="qwen-vl-ocr-other"),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_SUCCEEDED.value,
        model_requested=OCR_MODEL_ID,
        model_returned="qwen-vl-ocr-other",
        image_width=100,
        image_height=80,
    )
    assert rec.parse_state == "needs_review"
    assert rec.model_match is False
    assert rec.words_count == 2  # 单词仍保留，模型不一致独立标记


def test_parse_response_body_no_words_failed() -> None:
    rec = parse_response_body(
        {"output": {"choices": [{"message": {"content": []}}]}},
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_SUCCEEDED.value,
        model_requested=OCR_MODEL_ID,
        model_returned=OCR_MODEL_ID,
    )
    assert rec.parse_state == "failed"
    assert rec.words_count == 0


def test_parse_response_body_source_partial_propagates() -> None:
    rec = parse_response_body(
        _words_body(WORDS_OK, finish_reason="length"),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_PARTIAL.value,
        model_requested=OCR_MODEL_ID,
        model_returned=OCR_MODEL_ID,
    )
    assert rec.parse_state == "partial"  # length 不得当作成功


def test_parse_response_body_source_failed_and_needs_review() -> None:
    rec = parse_response_body(
        _words_body(WORDS_OK),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_FAILED.value,
    )
    assert rec.parse_state == "failed"
    rec2 = parse_response_body(
        _words_body(WORDS_OK),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-2",
        source_state=OcrState.NEEDS_REVIEW.value,
    )
    assert rec2.parse_state == "needs_review"


def test_parse_response_body_word_needs_review_partial() -> None:
    body = _words_body([WORDS_OK[0], {"text": "坏位置", "location": "bad"}])
    rec = parse_response_body(
        body,
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_SUCCEEDED.value,
        model_requested=OCR_MODEL_ID,
        model_returned=OCR_MODEL_ID,
    )
    assert rec.parse_state == "partial"
    assert rec.needs_review_count == 1


# ---------------------------------------------------------------------------
# 可解析率统计（§12）
# ---------------------------------------------------------------------------


def test_compute_parseability_stats() -> None:
    ok = parse_response_body(
        _words_body(WORDS_OK),
        ocr_run_id="r",
        ocr_task_id="t1",
        source_state=OcrState.OCR_SUCCEEDED.value,
    )
    partial = parse_response_body(
        _words_body([{"text": "x", "location": "bad"}]),
        ocr_run_id="r",
        ocr_task_id="t2",
        source_state=OcrState.OCR_SUCCEEDED.value,
    )
    failed = parse_response_body(
        {"output": {}},
        ocr_run_id="r",
        ocr_task_id="t3",
        source_state=OcrState.OCR_SUCCEEDED.value,
    )
    stats = compute_parseability_stats([ok, partial, failed])
    assert stats["total_responses"] == 3
    assert stats["parsed_ok"] == 1
    assert stats["parsed_partial"] == 1
    assert stats["parsed_failed"] == 1
    assert stats["responses_with_words"] == 2
    assert stats["parseable_rate"] == 2 / 3
    assert stats["total_words"] == 3


def test_compute_parseability_stats_empty() -> None:
    assert compute_parseability_stats([])["parseable_rate"] is None


# ---------------------------------------------------------------------------
# 运行目录解析 + 逐词表落盘（§5 / §7.3）
# ---------------------------------------------------------------------------


def _make_run_dir(tmp_path: Path) -> Path:
    """构造含 ocr_run.json 与原始响应文件的 OCR 运行目录。"""
    run_dir = tmp_path / "run_floorplan-ocr-test"
    run_dir.mkdir(parents=True, exist_ok=True)
    task_ok = _task("task-ok")
    task_partial = _task("task-partial", state=OcrState.OCR_PARTIAL)
    task_failed = _task("task-failed", state=OcrState.OCR_FAILED)
    record = OcrRunRecord(
        ocr_run_id="floorplan-ocr-test",
        asset_manifest_ref="manifest-ref-test",
        sourced=True,
        tasks=[task_ok, task_partial, task_failed],
        created_at="2026-08-25T00:00:00Z",
        updated_at="2026-08-25T00:00:00Z",
        run_dir=run_dir.as_posix(),
    )
    (run_dir / "ocr_run.json").write_text(record.model_dump_json(indent=2), encoding="utf-8")
    (run_dir / raw_response_filename(task_ok.ocr_task_id)).write_text(
        json.dumps(_words_body(WORDS_OK), ensure_ascii=False), encoding="utf-8"
    )
    (run_dir / raw_response_filename(task_partial.ocr_task_id)).write_text(
        json.dumps(_words_body(WORDS_OK, finish_reason="length"), ensure_ascii=False),
        encoding="utf-8",
    )
    return run_dir


def test_parse_ocr_run_directory_and_word_table(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path)
    data_dir = tmp_path / "data"
    stats = parse_ocr_run_directory(run_dir, data_dir=data_dir)

    assert stats.ocr_run_id == "floorplan-ocr-test"
    assert stats.total_tasks == 3
    assert stats.tasks_with_response == 2  # failed 任务无响应文件
    assert stats.tasks_without_response == 1
    assert stats.parsed_ok == 1
    assert stats.parsed_partial == 1
    assert stats.parsed_failed == 0
    assert stats.total_words == 4
    assert stats.parseable_rate == 1.0

    table_path = data_dir / "staged" / WORD_STAGED_FILENAME
    assert Path(stats.table_path) == table_path
    table = pq.read_table(table_path)
    assert table.num_rows == 4
    columns = set(table.column_names)
    assert {
        "word_id",
        "ocr_run_id",
        "ocr_task_id",
        "order",
        "text_raw",
        "text_normalized",
        "location",
        "rotate_rect",
        "image_width",
        "image_height",
        "coord_version",
        "parse_state",
        "participates_in_field",
        "parse_version",
        "response_parse_state",
    } <= columns
    parsed_states = table.column("response_parse_state").to_pylist()
    assert sorted(parsed_states) == ["parsed", "parsed", "partial", "partial"]


def test_parse_ocr_run_directory_without_data_dir(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path)
    stats = parse_ocr_run_directory(run_dir)
    assert stats.table_path is None
    assert stats.total_words == 4


def test_parse_ocr_run_directory_missing_run_record(tmp_path: Path) -> None:
    import pytest

    from compsval.ingest.floorplan_ocr_parse import load_ocr_run_record

    with pytest.raises(FileNotFoundError):
        load_ocr_run_record(tmp_path / "not-exists")


def test_write_word_table_empty(tmp_path: Path) -> None:
    path = write_word_table([], tmp_path / "data")
    table = pq.read_table(path)
    assert table.num_rows == 0


def test_write_word_table_atomic_no_incomplete_leftover(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    rec = parse_response_body(
        _words_body(WORDS_OK),
        ocr_run_id="run-c-1",
        ocr_task_id="task-c-1",
        source_state=OcrState.OCR_SUCCEEDED.value,
    )
    write_word_table([rec], data_dir)
    staged = data_dir / "staged"
    assert not list(staged.glob("*.incomplete"))  # 原子写盘不留半写表
