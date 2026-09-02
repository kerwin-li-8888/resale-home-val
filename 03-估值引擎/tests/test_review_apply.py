"""WP7-D: ``compsval review apply`` 复核留痕（技术方案 §10.2/§11.1）。

对照 WP7-D 验收标准：
① review apply 校验 valuation 存在性（不存在 → 退出码 3），只追加 review_event；
② 复核后自动结果不变，输出指向新版本/事件的引用（不覆盖）；
③ 复核前后 JSON/Markdown 描述同一冻结结果且复核留痕可查；
④ 端到端证据齐全：正常/边界/缺失/反例用例、同版本重复运行一致、备份回滚演练可复现；
⑤ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval import cli
from compsval.contract.models import SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)
from compsval.reporting.markdown import build_report_markdown
from compsval.valuation.candidate import DEFAULT_RULE_VERSION
from compsval.valuation.estimate import run_estimate
from compsval.valuation.review import REVIEW_EVENT_FILENAME
from compsval.valuation.review_apply import apply_review_for_run

_COMMUNITY = "C-XXXX0013"
_VAL_DATE = date(2026, 7, 21)
_RUN_ID = f"RUN-SUBJ-REVIEW-001-20260721-20260721-v{DEFAULT_RULE_VERSION}"


def _subject(**overrides: Any) -> SubjectProperty:
    base: dict[str, Any] = {
        "subject_id": "SUBJ-REVIEW-001",
        "community_id": _COMMUNITY,
        "area_sqm": Decimal("50.3"),
        "layout": "2室1厅",
        "valuation_date": _VAL_DATE,
    }
    base.update(overrides)
    return SubjectProperty(**base)


def _seed_data(tmp_path: Path) -> Path:
    """构造复核链路数据目录（estimate 所需 + 3 条同社区成交）。"""
    marts = tmp_path / MARTS_LAYER
    entities = tmp_path / "entities"
    marts.mkdir(parents=True)
    entities.mkdir(parents=True)
    valid_sale = pa.table(
        {
            "sale_event_id": ["v1", "v2", "v3"],
            "community_id": [_COMMUNITY] * 3,
            "sale_date": [date(2026, 7, 20), date(2026, 6, 20), date(2026, 5, 1)],
            "layout": ["2室1厅"] * 3,
            "area_sqm": [50.0, 52.0, 49.0],
            "total_price_yuan": [1300000.0] * 3,
            "unit_price": [26000, 25000, 26530],
            "anomaly_flag": ["正常"] * 3,
            "raw_locator": ["1", "2", "3"],
            "orientation": ["南"] * 3,
        }
    )
    valid_sale_path = marts / VALID_SALE_FILENAME
    pq.write_table(valid_sale, valid_sale_path)
    write_derived_manifest(
        DerivedManifest(
            layer=MARTS_LAYER,
            table="valid_sale",
            built_at=datetime.now(UTC),
            row_count=3,
            inputs=[InputRef(dataset="lianjia/chengjiao_list", fetched_at="20260821T000000Z")],
            package_version="0.1.0",
            notes="test fixture",
        ),
        valid_sale_path,
    )
    pq.write_table(
        pa.table({"community_id": [_COMMUNITY], "block": ["东泊南"]}),
        entities / "community.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "region": ["东泊南", "东泊南"],
                "month": [date(2026, 5, 1), date(2026, 6, 1)],
                "price": [25500, 26000],
            }
        ),
        entities / "market_series.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "community_id": [_COMMUNITY],
                "total_floors": [30],
                "has_elevator": [True],
                "year_built": [2015],
            }
        ),
        entities / "building.parquet",
    )
    return tmp_path


def _review_payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "action": "确认",
        "judgment": "主观判断调整",
        "subject": "估价结果确认",
        "before": {"center": 25800},
        "after": {"center": 25800},
        "reason": "现场核对装修良好，结果合理",
        "evidence": "现场观察：装修良好",
    }
    base.update(overrides)
    return base


def _frozen_result_bytes(data_dir: Path) -> bytes:
    """读取估值结果表原始字节（用于断言自动结果未被改写）。"""
    path = data_dir / "valuation" / "valuation_result.parquet"
    return path.read_bytes()


# ---- ① 校验估值存在 + 只追加 ----
def test_review_apply_validates_run_existence_exit_3(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    with pytest.raises(MissingDependencyError):
        apply_review_for_run(
            run_id="RUN-NOPE", data_dir=data_dir, input_payload=_review_payload()
        )


def test_review_apply_result_id_mismatch_exit_2(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    with pytest.raises(InvalidInputError):
        apply_review_for_run(
            run_id=_RUN_ID,
            data_dir=data_dir,
            input_payload=_review_payload(result_id="OTHER-RES"),
        )


# ---- ② 复核后自动结果不变 + 输出引用 ----
def test_review_apply_preserves_auto_result_and_returns_refs(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    before = _frozen_result_bytes(data_dir)

    review_path, events = apply_review_for_run(
        run_id=_RUN_ID, data_dir=data_dir, input_payload=_review_payload()
    )

    assert review_path.is_file()
    assert len(events) == 1
    event = events[0]
    assert event.review_id.startswith("REV-")
    assert event.result_id == f"{_RUN_ID}-RES"
    assert _frozen_result_bytes(data_dir) == before  # 自动结果未被覆盖

    # 事件已落 review_event 表且可查
    rows = pq.read_table(review_path).to_pylist()
    assert any(r["review_id"] == event.review_id for r in rows)


def test_review_apply_append_only_multiple_events(tmp_path: Path) -> None:
    """只追加：两次复核产生两条不同事件，自动结果不变（验收②）。"""
    data_dir = _seed_data(tmp_path)
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    before = _frozen_result_bytes(data_dir)

    _, e1 = apply_review_for_run(
        run_id=_RUN_ID, data_dir=data_dir, input_payload=_review_payload()
    )
    _, e2 = apply_review_for_run(
        run_id=_RUN_ID,
        data_dir=data_dir,
        input_payload=_review_payload(reason="第二次复核：补充证据"),
    )
    assert e1[0].review_id != e2[0].review_id
    assert _frozen_result_bytes(data_dir) == before

    path = data_dir / "valuation" / REVIEW_EVENT_FILENAME
    rows = pq.read_table(path).to_pylist()
    assert len(rows) == 2
    assert {r["review_id"] for r in rows} == {e1[0].review_id, e2[0].review_id}


# ---- ③ 复核留痕在报告中可查（JSON/Markdown 同一冻结结果）----
def test_review_apply_appears_in_report_section9(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=reports)
    apply_review_for_run(
        run_id=_RUN_ID, data_dir=data_dir, input_payload=_review_payload()
    )

    result = build_report_markdown(run_id=_RUN_ID, data_dir=data_dir, reports_root=reports)
    section9 = result.markdown.split("## 9. 自动结果和人工复核")[1].split("## 10.")[0]
    assert "REV-" in section9  # 复核事件 review_id 可见
    assert "确认" in section9
    assert "无人工复核事件" not in section9


# ---- ④ 端到端证据：正常/边界/缺失/反例 + 可重复 ----
def test_review_apply_repeat_estimate_same_frozen(tmp_path: Path) -> None:
    """同版本重复运行一致：复核前后重跑 estimate 产物一致（验收④）。"""
    data_dir = _seed_data(tmp_path)
    reports = tmp_path / "reports"
    outcome_a = run_estimate(subject=_subject(), data_dir=data_dir, out_root=reports)
    apply_review_for_run(
        run_id=_RUN_ID, data_dir=data_dir, input_payload=_review_payload()
    )
    outcome_b = run_estimate(subject=_subject(), data_dir=data_dir, out_root=reports)
    # 复核不改变后续重跑（确定性 run_id + 结果一致）；复核事件仍保留
    assert outcome_a.envelope.result == outcome_b.envelope.result
    path = data_dir / "valuation" / REVIEW_EVENT_FILENAME
    assert len(pq.read_table(path).to_pylist()) == 1


def test_review_apply_cli_stdout_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """CLI：review apply 输出 §10.3 JSON（含 review_ids + 不覆盖声明）。"""
    data_dir = _seed_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_subject(), data_dir=data_dir, out_root=reports)
    review_path = tmp_path / "review.json"
    review_path.write_text(json.dumps(_review_payload()), encoding="utf-8")
    rc = cli.main(
        [
            "review", "apply",
            "--valuation", _RUN_ID,
            "--input", str(review_path),
            "--data-dir", str(data_dir),
        ]
    )
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["command"] == "review apply"
    assert parsed["result"]["review_ids"]
    assert "不覆盖" in parsed["result"]["statement"]
    assert parsed["artifacts"]
    # WP8-B 版本治理回填：包络 data_version/rule_version 从 run 表填充（RV-WP7-D-01 F4）
    assert parsed["data_version"] is not None
    assert parsed["rule_version"] == DEFAULT_RULE_VERSION
