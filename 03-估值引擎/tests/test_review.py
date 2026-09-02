"""WP6-F: review_event 只追加人工复核留痕（VAL1-007）。

对照 WP6-F 验收标准：
① 复核事件只追加、不覆盖自动结果（同一自动 result 两次复核 → 两条事件共存，
   valuation_result 不被改写；反例：不存在的 result 拒绝写入）；
② 每次复核记录前后值/理由/时间/规则版本（before/after/reason/evidence/
   reviewed_at/rule_version 均非空且可回读）；
③ 区分“纠正错误数据”与“主观判断调整”（judgment 显式、两分项证据语义不同）；
④ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.contract.models import (
    ReviewAction,
    ReviewEvent,
    ReviewJudgment,
)
from compsval.ingest.manifests import InputRef
from compsval.valuation.aggregation import VALUATION_RESULT_FILENAME
from compsval.valuation.candidate import VALUATION_LAYER
from compsval.valuation.review import (
    REVIEW_EVENT_FILENAME,
    ReviewError,
    ReviewEventInput,
    append_review_events,
    review_event_table,
)

_RESULT_ID = "RUN-X-RES"


def _write_result(data_dir: Path) -> None:
    """写一个最小 valuation_result（供 FK 校验）。"""
    valuation = data_dir / VALUATION_LAYER
    valuation.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "result_id": [_RESULT_ID],
            "run_id": ["RUN-X"],
            "subject_id": ["SUBJ-TEST-001"],
            "center": [Decimal("42000.00")],
            "range_lower": [Decimal("40000.00")],
            "range_upper": [Decimal("44000.00")],
            "confidence": ["中"],
            "status": ["参考"],
            "valuation_date": [datetime.now(UTC).date()],
            "rule_version": ["1.0"],
            "evidence": ['{"案例数量":"充足"}'],
            "reason": ["单次数据可参考"],
        }
    )
    pq.write_table(table, valuation / VALUATION_RESULT_FILENAME)


def _event(**overrides: Any) -> ReviewEvent:
    base: dict[str, Any] = dict(
        review_id="",
        result_id=_RESULT_ID,
        action=ReviewAction.CONFIRM,
        judgment=ReviewJudgment.SUBJECTIVE_ADJUSTMENT,
        subject="估值结论",
        before={"center": 42000},
        after={"center": 41500},
        reason="复核人认为中心偏高，现场观察更保守",
        evidence="现场观察：楼层景观一般",
        reviewed_at=datetime(2026, 7, 21, 10, 0, tzinfo=UTC),
        reviewer="user",
        rule_version="1.0",
    )
    base.update(overrides)
    return ReviewEvent(**base)


# ---------------------------------------------------------------------------
# 验收① 只追加、不覆盖自动结果
# ---------------------------------------------------------------------------


def test_append_is_append_only_two_events_for_same_result(tmp_path: Path) -> None:
    """同一自动 result 两次追加 → 两条事件共存，review_id 递增（验收①）。"""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    e1 = _event(action=ReviewAction.CONFIRM)
    e2 = _event(action=ReviewAction.MODIFY_ATTRIBUTE, before={"floor": 12}, after={"floor": 15})
    append_review_events(data_dir=data_dir, events=[e1])
    append_review_events(data_dir=data_dir, events=[e2], notes="第二次追加")

    table = pq.read_table(data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME)
    rows = table.to_pylist()
    assert table.num_rows == 2
    ids = {row["review_id"] for row in rows}
    assert ids == {
        f"REV-{_RESULT_ID}-1",
        f"REV-{_RESULT_ID}-2",
    }
    assert {row["action"] for row in rows} == {"确认", "修改属性"}


def test_append_never_overwrites_automatic_result(tmp_path: Path) -> None:
    """只追加写入 review_event，valuation_result 行数与内容不被改动（验收①反例）。"""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    before = pq.read_table(data_dir / VALUATION_LAYER / VALUATION_RESULT_FILENAME)
    result_before = before.to_pylist()

    for _ in range(3):
        append_review_events(data_dir=data_dir, events=[_event()])

    after = pq.read_table(data_dir / VALUATION_LAYER / VALUATION_RESULT_FILENAME)
    assert after.num_rows == before.num_rows == 1
    assert after.to_pylist() == result_before  # 数值未被复核改写


def test_append_rejects_missing_result(tmp_path: Path) -> None:
    """指向不存在 valuation_result 的复核事件拒绝写入（FK，验收①反例）。"""
    data_dir = tmp_path / "data"
    (data_dir / VALUATION_LAYER).mkdir(parents=True, exist_ok=True)
    with pytest.raises(ReviewError, match="不存在或未构建"):
        append_review_events(
            data_dir=data_dir,
            events=[_event(result_id="RUN-NOT-EXIST-RES")],
        )
    assert not (data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME).exists()


def test_append_rejects_empty_and_missing_fields(tmp_path: Path) -> None:
    """无事件 / 缺理由 / 缺证据 → ReviewError（验收②必填）."""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    with pytest.raises(ReviewError, match="无复核事件"):
        append_review_events(data_dir=data_dir, events=[])
    with pytest.raises(ReviewError, match="理由"):
        append_review_events(data_dir=data_dir, events=[_event(reason="  ")])
    with pytest.raises(ReviewError, match="证据"):
        append_review_events(data_dir=data_dir, events=[_event(evidence="  ")])


# ---------------------------------------------------------------------------
# 验收② 记录完整：前后值 / 理由 / 时间 / 规则版本
# ---------------------------------------------------------------------------


def test_event_records_before_after_reason_time_version(tmp_path: Path) -> None:
    """事件完整记录前后值/理由/时间/规则版本，可回读（验收②）。"""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    at = datetime(2026, 7, 20, 9, 30, tzinfo=UTC)
    append_review_events(
        data_dir=data_dir,
        events=[
            _event(
                action=ReviewAction.CORRECT_DATA,
                judgment=ReviewJudgment.CORRECT_ERROR,
                before={"unit_price": 38000},
                after={"unit_price": 38500},
                reason="合同价录入错误，按合同价纠正",
                evidence="合同价复印件：38500 元/㎡",
                reviewed_at=at,
                rule_version="1.1",
            )
        ],
        input_refs=[InputRef(dataset="chengjiao", fetched_at="20260821")],
    )

    table = pq.read_table(data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME)
    row = table.to_pylist()[0]
    assert row["review_id"] == f"REV-{_RESULT_ID}-1"
    assert row["result_id"] == _RESULT_ID
    assert row["action"] == "纠正数据"
    assert row["judgment"] == "纠正错误数据"
    assert row["before"] == '{"unit_price": 38000}'
    assert row["after"] == '{"unit_price": 38500}'
    assert "合同价录入错误" in row["reason"]
    assert row["evidence"]
    assert row["reviewer"] == "user"
    assert row["rule_version"] == "1.1"
    assert row["reviewed_at"] is not None  # 时间已记录
    # manifest 溯源
    manifest = (data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME).with_suffix(".manifest.json")
    assert manifest.is_file()
    assert "chengjiao" in manifest.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 验收③ 区分“纠正错误数据”与“主观判断调整”
# ---------------------------------------------------------------------------


def test_judgment_distinguishes_correct_error_and_subjective(tmp_path: Path) -> None:
    """同一 result 可同时含纠正错误数据与主观判断调整两类事件（验收③）。"""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    append_review_events(
        data_dir=data_dir,
        events=[
            _event(
                action=ReviewAction.CORRECT_DATA,
                judgment=ReviewJudgment.CORRECT_ERROR,
                before={"area": 55},
                after={"area": 51},
                reason="建筑面积口径更正",
                evidence="房产证面积 51㎡",
            ),
            _event(
                action=ReviewAction.SWAP_CASE,
                judgment=ReviewJudgment.SUBJECTIVE_ADJUSTMENT,
                before={"candidate": "c1"},
                after={"candidate": "c7"},
                reason="更倾向新成交案例的可比性",
                evidence="复核人判断",
            ),
        ],
    )

    table = pq.read_table(data_dir / VALUATION_LAYER / REVIEW_EVENT_FILENAME)
    judgments = {row["judgment"] for row in table.to_pylist()}
    assert judgments == {"纠正错误数据", "主观判断调整"}
    # 两分项证据语义不同：纠正错误数据带可复现证据；主观调整标记复核人
    rows = {row["judgment"]: row for row in table.to_pylist()}
    assert "房产证面积" in rows["纠正错误数据"]["evidence"]
    assert "复核人判断" in rows["主观判断调整"]["evidence"]


# ---------------------------------------------------------------------------
# 辅助/边缘：表构造、CLI 输入合同、无既有表空追加
# ---------------------------------------------------------------------------


def test_review_event_table_schema_roundtrip() -> None:
    """review_event_table 模式与字段完整（验收② schema）。"""
    table = review_event_table([_event(review_id="REV-X-1")])
    assert table.num_rows == 1
    assert {
        "review_id",
        "result_id",
        "action",
        "judgment",
        "reason",
        "evidence",
        "reviewed_at",
        "reviewer",
        "rule_version",
    } <= set(table.schema.names)
    assert "before" in table.schema.names
    assert "after" in table.schema.names


def test_append_to_missing_table_creates_it(tmp_path: Path) -> None:
    """首次追加自动建表（无既有 review_event），单条落盘（验收①）。"""
    data_dir = tmp_path / "data"
    _write_result(data_dir)
    path, assigned = append_review_events(data_dir=data_dir, events=[_event()])
    assert path.is_file()
    assert pq.read_table(path).num_rows == 1
    assert assigned[0].review_id == f"REV-{_RESULT_ID}-1"  # append 已分配 review_id


def test_review_event_input_defaults(tmp_path: Path) -> None:
    """CLI 输入合同：未给 reviewed_at/reviewer/rule_version → 合理默认值。"""
    payload: dict[str, Any] = {
        "result_id": _RESULT_ID,
        "action": "确认",
        "judgment": "主观判断调整",
        "subject": "估值结论",
        "reason": "复核通过",
        "evidence": "现场观察佐证",
    }
    event = ReviewEventInput(**payload).to_review_event()
    assert event.reviewer == "user"
    assert event.rule_version == "1.0"
    assert event.reviewed_at is not None  # 默认当前时间
    assert event.review_id == ""  # 由 append 分配
