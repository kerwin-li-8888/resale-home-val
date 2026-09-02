"""EXTFP3-D 确定性转录解析器（floorplan_transcribe）离线测试。

全部用例离线构造词条（不触网、不付费、不调用模型），按 §10.4 四类覆盖：
正常（房间+面积关联/合并词条/多房间）/ 边界（多面积冲突/两房间等距/旋转字/
单位形态）/ 缺失（无面积/无房间/缺位置/空）/ 反例（车库别墅/房间数非面积/
Decimal 精度/确定性 ID/参与字段回填/落盘 roundtrip）。
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from compsval.ingest.floorplan_ocr_parse import (
    OcrParseRecord,
    OcrWordRecord,
    WordParseState,
    normalize_text,
    write_word_table,
)
from compsval.ingest.floorplan_transcribe import (
    ANNOTATION_STAGED_FILENAME,
    TRANSCRIBE_PARSER_VERSION,
    AnnotationState,
    RoomAnnotationRecord,
    apply_numeric_occupation_gate,
    backfill_word_participation,
    classify_room,
    compute_annotation_stats,
    dedupe_homologous_words,
    isolate_cross_run_inconsistencies,
    parse_area,
    transcribe_word_table,
    transcribe_words,
    word_centroid,
)

RUN_ID = "run-d-1"
TASK_ID = "task-d-1"
DEFAULT_LOCATION = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0]]


def _word(
    text: str,
    location: list[list[float]] | None = None,
    *,
    ocr_task_id: str = TASK_ID,
    order: int = 0,
    raw: str | None = None,
) -> OcrWordRecord:
    return OcrWordRecord(
        word_id=f"w-{ocr_task_id}-{order}",
        ocr_run_id=RUN_ID,
        ocr_task_id=ocr_task_id,
        order=order,
        text_raw=raw or text,
        text_normalized=normalize_text(text),
        location=DEFAULT_LOCATION if location is None else location,
        parse_state=WordParseState.PARSED.value,
    )


# ---------------------------------------------------------------------------
# 房间标签 / 面积识别（§9.4-2/3/4）
# ---------------------------------------------------------------------------


def test_classify_room_labels() -> None:
    assert classify_room("主卧") == ("master_bedroom", "主卧")
    assert classify_room("次卧") == ("secondary_bedroom", "次卧")
    assert classify_room("客厅") == ("living_room", "客厅")
    assert classify_room("餐厅") == ("dining_room", "餐厅")
    assert classify_room("厨房") == ("kitchen", "厨房")
    assert classify_room("卫生间") == ("bathroom", "卫生间")
    assert classify_room("洗手间") == ("bathroom", "洗手间")
    assert classify_room("淋浴间") == ("bathroom", "淋浴间")
    assert classify_room("卫") == ("bathroom", "卫")  # EXTFP3-H 词表扩展（原图短标「卫」）
    assert classify_room("主卫") == ("master_bathroom", "主卫")  # 特指优先于「卫」
    assert classify_room("阳台") == ("balcony", "阳台")
    assert classify_room("储物间") == ("storage", "储物间")
    assert classify_room("车库") is None  # 反例：非普通住宅房间
    assert classify_room("别墅") is None
    assert classify_room("主卧室") == ("master_bedroom", "主卧")  # 特指优先


def test_parse_area_units() -> None:
    assert parse_area("12.5㎡") == (__import__("decimal").Decimal("12.5"), "m2")
    assert parse_area("12.5 m2") == (__import__("decimal").Decimal("12.5"), "m2")
    assert parse_area("12.5平方米") == (__import__("decimal").Decimal("12.5"), "m2")
    assert parse_area("8 平米") == (__import__("decimal").Decimal("8"), "m2")
    assert parse_area("3平") == (__import__("decimal").Decimal("3"), "m2")
    # 反例：无单位不进面积候选（房间数/门牌号）
    assert parse_area("3室2厅") is None
    assert parse_area("12.5") is None
    assert parse_area("主卧") is None


def test_parse_area_decimal_precision() -> None:
    value, unit = parse_area("12.50㎡")  # type: ignore[misc]
    assert str(value) == "12.50"
    assert unit == "m2"


# ---------------------------------------------------------------------------
# 正常（§10.4 正常类）
# ---------------------------------------------------------------------------


def test_transcribe_accepted_association_by_distance() -> None:
    room = _word("主卧", [[10, 10], [30, 10], [30, 30], [10, 30]], order=0)
    area = _word("12.5㎡", [[12, 12], [28, 12], [28, 20], [12, 20]], order=1)
    records = transcribe_words([room, area])
    assert len(records) == 1
    ann = records[0]
    assert ann.parse_state == AnnotationState.ACCEPTED.value
    assert ann.room_word_id == room.word_id
    assert ann.area_word_id == area.word_id
    assert ann.standard_room_type == "master_bedroom"
    assert ann.area_value == "12.5"
    assert ann.area_unit == "m2"
    assert ann.room_name_raw == "主卧"
    assert ann.area_text_normalized == "12.5m2"
    assert ann.location  # 位置证据


def test_transcribe_combined_word() -> None:
    w = _word("主卧 12.5㎡", order=0)
    records = transcribe_words([w])
    assert len(records) == 1
    ann = records[0]
    assert ann.parse_state == AnnotationState.ACCEPTED.value
    assert ann.room_word_id == ann.area_word_id == w.word_id
    assert ann.standard_room_type == "master_bedroom"
    assert ann.area_value == "12.5"


def test_transcribe_multiple_rooms() -> None:
    master = _word("主卧", [[10, 10], [30, 10], [30, 30], [10, 30]], order=0)
    master_area = _word("12.5㎡", [[12, 12], [28, 12], [28, 20], [12, 20]], order=1)
    living = _word("客厅", [[60, 60], [90, 60], [90, 90], [60, 90]], order=2)
    living_area = _word("20㎡", [[62, 62], [88, 62], [88, 70], [62, 70]], order=3)
    records = transcribe_words([master, master_area, living, living_area])
    accepted = [r for r in records if r.parse_state == AnnotationState.ACCEPTED.value]
    assert len(accepted) == 2
    types = {a.standard_room_type for a in accepted}
    assert types == {"master_bedroom", "living_room"}
    values = {a.area_value for a in accepted}
    assert values == {"12.5", "20"}


# ---------------------------------------------------------------------------
# 边界（§10.4 边界类）
# ---------------------------------------------------------------------------


def test_transcribe_multiple_areas_same_room_conflict() -> None:
    room = _word("主卧", [[10, 10], [30, 10], [30, 30], [10, 30]], order=0)
    area_a = _word("12.5㎡", [[12, 12], [28, 12], [28, 20], [12, 20]], order=1)
    area_b = _word("15㎡", [[12, 22], [28, 22], [28, 28], [12, 28]], order=2)
    records = transcribe_words([room, area_a, area_b])
    conflicts = [r for r in records if r.parse_state == AnnotationState.CONFLICT.value]
    assert len(conflicts) == 3  # 房间 + 两个面积全部 CONFLICT
    assert all(r.room_word_id == room.word_id for r in conflicts if r.room_word_id)
    assert {r.area_value for r in conflicts if r.area_value} == {"12.5", "15"}


def test_transcribe_area_tie_two_rooms_needs_review() -> None:
    # 面积位于两房间标签正中（质心 x=15）→ 等距 → NEEDS_REVIEW
    left = _word("主卧", [[0, 0], [10, 0], [10, 10], [0, 10]], order=0)
    right = _word("客厅", [[20, 0], [30, 0], [30, 10], [20, 10]], order=1)
    area = _word("12.5㎡", [[14, 4], [16, 4], [16, 6], [14, 6]], order=2)
    records = transcribe_words([left, right, area])
    area_records = [r for r in records if r.area_value == "12.5"]
    assert len(area_records) == 1
    assert area_records[0].parse_state == AnnotationState.NEEDS_REVIEW.value


def test_transcribe_rotated_text_location_kept() -> None:
    room = _word("主卧", [[10, 10], [30, 10], [30, 30], [10, 30]], order=0)
    room.rotate_rect = {"center": [[20, 20]], "width": 20.0, "height": 20.0, "angle": 90.0}
    area = _word("12.5㎡", [[12, 12], [28, 12], [28, 20], [12, 20]], order=1)
    records = transcribe_words([room, area])
    assert records[0].parse_state == AnnotationState.ACCEPTED.value
    assert word_centroid(room) == (20.0, 20.0)  # 旋转字位置仍用于关联


# ---------------------------------------------------------------------------
# 缺失（§10.4 缺失类）
# ---------------------------------------------------------------------------


def test_transcribe_room_only() -> None:
    room = _word("主卧", order=0)
    records = transcribe_words([room])
    assert len(records) == 1
    ann = records[0]
    assert ann.parse_state == AnnotationState.ROOM_ONLY.value
    assert ann.area_value is None  # 面积保持未知，不推断


def test_transcribe_area_without_room_needs_review() -> None:
    area = _word("12.5㎡", order=0)
    records = transcribe_words([area])
    assert len(records) == 1
    assert records[0].parse_state == AnnotationState.NEEDS_REVIEW.value
    assert records[0].area_value == "12.5"


def test_transcribe_word_without_location_needs_review() -> None:
    room = _word("主卧", location=[], order=0)
    area = _word("12.5㎡", location=[], order=1)
    records = transcribe_words([room, area])
    assert all(r.parse_state == AnnotationState.NEEDS_REVIEW.value for r in records)


def test_transcribe_combined_without_location_needs_review() -> None:
    w = _word("主卧 12.5㎡", location=[], order=0)
    records = transcribe_words([w])
    assert records[0].parse_state == AnnotationState.NEEDS_REVIEW.value


def test_transcribe_empty_words() -> None:
    assert transcribe_words([]) == []


# ---------------------------------------------------------------------------
# 反例（§10.4 反例类）
# ---------------------------------------------------------------------------


def test_transcribe_garage_villa_ignored() -> None:
    records = transcribe_words([_word("车库", order=0), _word("别墅", order=1)])
    assert records == []


def test_transcribe_room_count_not_area() -> None:
    records = transcribe_words([_word("3室2厅", order=0), _word("主卧", order=1)])
    room_only = [r for r in records if r.room_word_id]
    assert len(room_only) == 1
    assert room_only[0].parse_state == AnnotationState.ROOM_ONLY.value  # 3室2厅未被当面积


def test_transcribe_deterministic_annotation_id() -> None:
    a = transcribe_words([_word("主卧", order=0), _word("12.5㎡", order=1)])
    b = transcribe_words([_word("主卧", order=0), _word("12.5㎡", order=1)])
    assert [r.annotation_id for r in a] == [r.annotation_id for r in b]
    assert a[0].parse_version == TRANSCRIBE_PARSER_VERSION


def test_transcribe_does_not_fabricate_area_from_excel() -> None:
    # 只有房间标签，Excel 有面积也不进入（解析器无 Excel 输入）
    room = _word("主卧", order=0)
    records = transcribe_words([room])
    assert records[0].area_value is None


# ---------------------------------------------------------------------------
# 参与字段回填（§7.3）
# ---------------------------------------------------------------------------


def test_backfill_word_participation() -> None:
    room = _word("主卧", order=0)
    area = _word("12.5㎡", order=1)
    combined = _word("客厅 20㎡", order=2)
    annotations = transcribe_words([room, area, combined])
    participation = backfill_word_participation(annotations, [room, area, combined])
    assert participation[room.word_id] == "room_name"
    assert participation[area.word_id] == "area"
    assert participation[combined.word_id] == "room_name+area"


# ---------------------------------------------------------------------------
# 统计与落盘（§5 / §7.4）
# ---------------------------------------------------------------------------


def test_compute_annotation_stats() -> None:
    records = transcribe_words([_word("主卧", order=0), _word("12.5㎡", order=1)])
    stats = compute_annotation_stats(records, tasks_transcribed=1, ocr_run_id=RUN_ID)
    assert stats.tasks_transcribed == 1
    assert stats.annotations_total == 1
    assert stats.accepted == 1
    assert stats.accepted_rate == 1.0


def _write_word_table(tmp_path: Path) -> Path:
    """用 C 阶段 write_word_table 构造逐词表 parquet。"""
    words = [
        _word("主卧", [[10, 10], [30, 10], [30, 30], [10, 30]], order=0),
        _word("12.5㎡", [[12, 12], [28, 12], [28, 20], [12, 20]], order=1),
        _word("客厅 20㎡", order=2),
    ]
    rec = OcrParseRecord(
        ocr_task_id=TASK_ID,
        ocr_run_id=RUN_ID,
        source_state="OCR_SUCCEEDED",
        parse_state="parsed",
        words=words,
    )
    return write_word_table([rec], tmp_path / "data")


def test_transcribe_word_table_roundtrip(tmp_path: Path) -> None:
    word_table = _write_word_table(tmp_path)
    data_dir = tmp_path / "data"
    stats = transcribe_word_table(word_table, data_dir)

    assert stats.ocr_run_id == RUN_ID
    assert stats.tasks_transcribed == 1
    assert stats.annotations_total == 2  # 主卧+面积 ACCEPTED、客厅合并 ACCEPTED
    assert stats.accepted == 2
    assert stats.accepted_rate == 1.0

    ann_path = data_dir / "staged" / ANNOTATION_STAGED_FILENAME
    assert Path(stats.table_path) == ann_path
    table = pq.read_table(ann_path)
    assert table.num_rows == 2
    columns = set(table.column_names)
    assert {
        "annotation_id",
        "ocr_run_id",
        "ocr_task_id",
        "parse_version",
        "room_word_id",
        "area_word_id",
        "room_name_raw",
        "room_name_normalized",
        "standard_room_type",
        "area_text_raw",
        "area_text_normalized",
        "area_value",
        "area_unit",
        "location",
        "parse_state",
        "consistency_status",
        "review_state",
        "review_event_ref",
    } <= columns
    assert sorted(table.column("parse_state").to_pylist()) == ["ACCEPTED", "ACCEPTED"]

    # 参与字段回填表
    word_table_backfilled = data_dir / "staged" / "floorplan_ocr_word.parquet"
    assert Path(stats.word_participation_path) == word_table_backfilled
    wt = pq.read_table(word_table_backfilled)
    word_ids = wt.column("word_id").to_pylist()
    participation = dict(
        zip(
            word_ids,
            wt.column("participates_in_field").to_pylist(),
            strict=True,
        )
    )
    assert participation["w-task-d-1-0"] == "room_name"
    assert participation["w-task-d-1-1"] == "area"
    assert participation["w-task-d-1-2"] == "room_name+area"


def test_word_participation_preserves_parse_version_and_response_state(
    tmp_path: Path,
) -> None:
    """RV-EXTFP3-E-01#F5：D#F1 回归——参与字段回填后 parse_version/response_parse_state 原值保留。

    原表为 partial（非 parsed）→ 回填后仍为 partial，不静默覆盖为 parsed。
    """
    words = [_word("主卧", order=0)]
    rec = OcrParseRecord(
        ocr_task_id=TASK_ID,
        ocr_run_id=RUN_ID,
        source_state="OCR_PARTIAL",
        parse_version="EXTFP3-C-0.9",
        parse_state="partial",
        words=words,
    )
    data_dir = tmp_path / "data"
    word_table = write_word_table([rec], data_dir)
    transcribe_word_table(word_table, data_dir)

    wt = pq.read_table(data_dir / "staged" / "floorplan_ocr_word.parquet")
    assert wt.column("parse_version").to_pylist() == ["EXTFP3-C-0.9"]
    assert wt.column("response_parse_state").to_pylist() == ["partial"]


def test_write_annotation_table_atomic_no_incomplete_leftover(tmp_path: Path) -> None:
    from compsval.ingest.floorplan_transcribe import write_annotation_table

    data_dir = tmp_path / "data"
    records = transcribe_words([_word("主卧", order=0), _word("12.5㎡", order=1)])
    write_annotation_table(records, data_dir)
    staged = data_dir / "staged"
    assert not list(staged.glob("*.incomplete"))  # 原子写盘不留半写表


# ---------------------------------------------------------------------------
# OCRNEXT-C 字段安全最小修复（合同 §5-C / 方案 §6）：
# ①同源重复词框去重 / ②数字占用唯一性门禁 / ③跨运行不一致隔离。
# ---------------------------------------------------------------------------


def _box(x: float, y: float, w: float = 10.0, h: float = 10.0) -> list[list[float]]:
    return [[x, y], [x + w, y], [x + w, y + h], [x, y + h]]


def _word_at(text: str, box: list[list[float]], *, order: int) -> OcrWordRecord:
    return _word(text, box, order=order)


# —— 修复①：同源重复词框去重 ——


def test_dedupe_tolerance_uses_image_diagonal_not_word_box() -> None:
    """RV-OCRNEXT-C-01#F5：质心容差基准为图片对角线 1%（同词框、图片更小→不合并）。"""

    def _sized(text: str, box: list[list[float]], order: int, iw: int, ih: int) -> OcrWordRecord:
        return OcrWordRecord(
            word_id=f"w-img-{order}",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            order=order,
            text_raw=text,
            text_normalized=normalize_text(text),
            location=box,
            parse_state=WordParseState.PARSED.value,
            image_width=iw,
            image_height=ih,
        )

    pair = [
        _sized("卧室", _box(100, 100, 40, 40), 0, 400, 300),
        _sized("卧室", _box(103.9, 100, 40, 40), 1, 400, 300),
    ]
    assert [w.order for w in dedupe_homologous_words(pair)] == [0]
    smaller_image = [
        _sized("卧室", _box(100, 100, 40, 40), 2, 200, 150),
        _sized("卧室", _box(103.9, 100, 40, 40), 3, 200, 150),
    ]
    assert [w.order for w in dedupe_homologous_words(smaller_image)] == [2, 3]


def test_dedupe_merges_provably_identical_duplicate_boxes() -> None:
    """H4 形态：「卫生间B」被检测 3 次且词框重叠 → 只保留 order 最小的代表。"""
    dup = [
        _word_at("卫生间B", _box(100, 100), order=0),
        _word_at("卫生间B", _box(100, 100), order=1),
        _word_at("卫生间B", _box(100.5, 100.5), order=2),
    ]
    kept = dedupe_homologous_words(dup)
    assert [w.order for w in kept] == [0]


def test_dedupe_keeps_distant_same_text_rooms() -> None:
    """真实两间同类型房（词框距离远）→ 不可证明同源，全部保留（宁留冲突不错合并）。"""
    rooms = [
        _word_at("卧室", _box(0, 0), order=0),
        _word_at("卧室", _box(500, 500), order=1),
    ]
    kept = dedupe_homologous_words(rooms)
    assert [w.order for w in kept] == [0, 1]


def test_dedupe_keeps_partial_overlap_and_no_location() -> None:
    """部分重叠（IoU<0.9）与缺位置证据都不可证明 → 保留。"""
    partial = [
        _word_at("阳台", _box(0, 0, 20, 10), order=0),
        _word_at("阳台", _box(10, 0, 20, 10), order=1),
    ]
    assert len(dedupe_homologous_words(partial)) == 2
    no_loc = [
        OcrWordRecord(
            word_id="w-nol-0",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            order=0,
            text_raw="厨房",
            text_normalized="厨房",
            location=[],
        ),
        OcrWordRecord(
            word_id="w-nol-1",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            order=1,
            text_raw="厨房",
            text_normalized="厨房",
            location=[],
        ),
    ]
    assert len(dedupe_homologous_words(no_loc)) == 2


def test_transcribe_words_applies_dedupe_end_to_end() -> None:
    """端到端：重复词框 + 单面积词 → 房间 claim 数收敛到 1，不产生多余 claim。"""
    words = [
        _word_at("卫生间B", _box(100, 100), order=0),
        _word_at("卫生间B", _box(100, 100), order=1),
        _word_at("卫生间B", _box(100, 100), order=2),
        _word_at("12.5㎡", _box(102, 112), order=3),
    ]
    records = transcribe_words(words)
    rooms = [r for r in records if r.standard_room_type == "bathroom"]
    assert len(rooms) == 1
    assert rooms[0].parse_state == AnnotationState.ACCEPTED.value


# —— 修复②：数字占用唯一性门禁 ——


def test_numeric_occupation_gate_demotes_ambiguous_multi_claim() -> None:
    """构造违规集：同一「18.3」词框被两间卧室同时接受 → 全部降级 CONFLICT+隔离原因。"""
    room_a = _word_at("卧室A", _box(0, 0), order=0)
    room_b = _word_at("卧室B", _box(50, 0), order=1)
    shared_area = _word_at("18.3㎡", _box(25, 0), order=2)
    records = [
        RoomAnnotationRecord(
            annotation_id="x1",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            room_word_id=room_a.word_id,
            area_word_id=shared_area.word_id,
            standard_room_type="bedroom",
            area_value="18.3",
            area_unit="m2",
            parse_state=AnnotationState.ACCEPTED.value,
        ),
        RoomAnnotationRecord(
            annotation_id="x2",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            room_word_id=room_b.word_id,
            area_word_id=shared_area.word_id,
            standard_room_type="bedroom",
            area_value="18.3",
            area_unit="m2",
            parse_state=AnnotationState.ACCEPTED.value,
        ),
    ]
    out = apply_numeric_occupation_gate(records)
    assert all(r.parse_state == AnnotationState.CONFLICT.value for r in out)
    assert all(r.isolation_reason == "numeric_occupation_ambiguous" for r in out)


def test_numeric_occupation_gate_keeps_legit_equal_counts() -> None:
    """两间真同面积各有独立词框（房间数=词框数）→ 不误伤。"""
    records = [
        RoomAnnotationRecord(
            annotation_id="y1",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            room_word_id="room-1",
            area_word_id="area-1",
            standard_room_type="bedroom",
            area_value="9.2",
            area_unit="m2",
            parse_state=AnnotationState.ACCEPTED.value,
        ),
        RoomAnnotationRecord(
            annotation_id="y2",
            ocr_run_id=RUN_ID,
            ocr_task_id=TASK_ID,
            room_word_id="room-2",
            area_word_id="area-2",
            standard_room_type="bedroom",
            area_value="9.2",
            area_unit="m2",
            parse_state=AnnotationState.ACCEPTED.value,
        ),
    ]
    out = apply_numeric_occupation_gate(records)
    assert all(r.parse_state == AnnotationState.ACCEPTED.value for r in out)
    assert all(r.isolation_reason is None for r in out)


def test_same_name_distant_boxes_are_kept_not_demoted() -> None:
    """数据驱动结论（双轮 300 张回放：同名多框组距离全 >50px，真实多房间主导）：
    同名不可证明重叠 → 保留为独立 claim（ROOM_ONLY），不整体降级冲突。"""
    records = transcribe_words(
        [
            _word_at("卫生间B", _box(0, 0), order=0),
            _word_at("卫生间B", _box(500, 500), order=1),
        ]
    )
    assert len(records) == 2
    assert all(r.parse_state == AnnotationState.ROOM_ONLY.value for r in records)
    assert all(r.isolation_reason is None for r in records)


# —— 修复③：跨运行不一致隔离 ——


def _accepted_ann(
    *,
    task_id: str,
    room_norm: str,
    area_value: str | None,
    ann_id: str,
    room_type: str = "bedroom",
) -> RoomAnnotationRecord:
    return RoomAnnotationRecord(
        annotation_id=ann_id,
        ocr_run_id=RUN_ID,
        ocr_task_id=task_id,
        room_word_id=f"rw-{ann_id}",
        area_word_id=f"aw-{ann_id}" if area_value is not None else None,
        room_name_normalized=room_norm,
        standard_room_type=room_type,
        area_value=area_value,
        area_unit="m2" if area_value is not None else None,
        location=_box(0, 0),
        parse_state=AnnotationState.ACCEPTED.value,
    )


def test_isolate_cross_run_keeps_agreeing_field() -> None:
    task = "t-iso-1"
    primary = [_accepted_ann(task_id=task, room_norm="主卧", area_value="12.5", ann_id="p1")]
    reference = [_accepted_ann(task_id=task, room_norm="主卧", area_value="12.5", ann_id="r1")]
    adjusted, isolated = isolate_cross_run_inconsistencies(primary, reference)
    assert isolated == []
    assert adjusted[0].parse_state == AnnotationState.ACCEPTED.value
    assert adjusted[0].isolation_reason is None


def test_isolate_cross_run_demotes_non_agreeing_field() -> None:
    """primary 接受「卧室E=18.3」而 reference 无此字段（词框差异形态）→ 隔离。"""
    task = "t-iso-2"
    primary = [_accepted_ann(task_id=task, room_norm="卧室E", area_value="18.3", ann_id="p2")]
    reference = [_accepted_ann(task_id=task, room_norm="卧室E", area_value="8.9", ann_id="r2")]
    adjusted, isolated = isolate_cross_run_inconsistencies(primary, reference)
    assert len(isolated) == 1
    assert isolated[0].isolation_reason == "cross_run_inconsistent"
    assert adjusted[0].parse_state == AnnotationState.NEEDS_REVIEW.value
    # 入参不被改写（model_copy 语义）
    assert primary[0].parse_state == AnnotationState.ACCEPTED.value


def test_isolate_cross_run_task_missing_in_reference_not_isolated() -> None:
    """reference 完全没有该任务（无对照依据）→ 不隔离（与不进分母口径一致）。"""
    primary = [_accepted_ann(task_id="t-only", room_norm="主卧", area_value="12.5", ann_id="p3")]
    reference = [_accepted_ann(task_id="t-other", room_norm="主卧", area_value="12.5", ann_id="r3")]
    adjusted, isolated = isolate_cross_run_inconsistencies(primary, reference)
    assert isolated == []
    assert adjusted[0].parse_state == AnnotationState.ACCEPTED.value


def test_isolate_cross_run_multiset_counts() -> None:
    """多重集：primary 同键 2 条、reference 1 条 → 第 2 条隔离、第 1 条保留。"""
    task = "t-iso-4"
    primary = [
        _accepted_ann(task_id=task, room_norm="卧室", area_value="9.2", ann_id="a"),
        _accepted_ann(task_id=task, room_norm="卧室", area_value="9.2", ann_id="b"),
    ]
    reference = [_accepted_ann(task_id=task, room_norm="卧室", area_value="9.2", ann_id="c")]
    adjusted, isolated = isolate_cross_run_inconsistencies(primary, reference)
    assert [a.annotation_id for a in isolated] == ["b"]
    assert adjusted[0].parse_state == AnnotationState.ACCEPTED.value
    assert adjusted[1].parse_state == AnnotationState.NEEDS_REVIEW.value


def test_transcribe_parser_version_ocrnext_c() -> None:
    """OCRNEXT-C 版本升级；标注 ID 派生随版本确定（旧表不改写，新回放自然新 ID）。"""
    assert TRANSCRIBE_PARSER_VERSION == "OCRNEXT-C-1.2"
    words = [_word("主卧", order=0), _word("12.5㎡", order=1)]
    ids_1 = {r.annotation_id for r in transcribe_words(words)}
    ids_2 = {r.annotation_id for r in transcribe_words(words)}
    assert ids_1 == ids_2
