"""WP7-A: 输出契约基础设施（统一 JSON 包络 + 退出码 + 命令错误分级）。

对照 WP7-A 验收标准：
① 包络字段与技术方案 §10.3 一致（schema_version=1.0/command/command_status/
   business_status/run_id/data_version/rule_version/result/warnings/errors/
   artifacts）；
② command_status 与 business_status 分离（success 不代表估值可信）；
③ 退出码 0/2/3/4/5 与 §10.4 语义一致，错误分级类型可映射到退出码；
④ 包络可 JSON 序列化/反序列化 round-trip；
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
from pydantic import ValidationError

from compsval import cli
from compsval.contract.models import SubjectProperty
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    EXIT_CODE_LABELS,
    EXIT_INTERNAL_ERROR,
    EXIT_INVALID_INPUT,
    EXIT_MISSING_DEPENDENCY,
    EXIT_OK,
    EXIT_VERSION_MISMATCH,
    VALID_EXIT_CODES,
    CommandError,
    CommandStatus,
    InternalCommandError,
    InvalidInputError,
    MissingDependencyError,
    OutputEnvelope,
    VersionMismatchError,
    envelope_from_error,
)
from compsval.reporting.markdown import (
    REPORT_FILENAME,
    build_report_markdown,
    report_path_for,
)
from compsval.reporting.run_show import show_run
from compsval.valuation.candidate import DEFAULT_RULE_VERSION
from compsval.valuation.estimate import run_estimate


def test_envelope_has_exact_section_10_3_fields() -> None:
    """验收①：包络包含 §10.3 全部字段，schema_version 默认 1.0。"""
    env = OutputEnvelope(command="estimate")
    expected: dict[str, Any] = {
        "schema_version": "1.0",
        "command": "estimate",
        "command_status": "success",
        "business_status": None,
        "run_id": None,
        "data_version": None,
        "rule_version": None,
        "result": {},
        "warnings": [],
        "errors": [],
        "artifacts": [],
    }
    assert env.model_dump() == expected


def test_envelope_success_can_carry_business_status() -> None:
    """验收②：command_status=success 与业务降级/参考状态可共存（完成≠可信）。"""
    env = OutputEnvelope(
        command="estimate",
        command_status=CommandStatus.SUCCESS,
        business_status="参考",
        run_id="RUN-1",
        result={"center": 10000},
        warnings=["正式发布门槛未通过，仅输出参考"],
    )
    assert env.command_status is CommandStatus.SUCCESS
    assert env.business_status == "参考"
    assert env.warnings == ["正式发布门槛未通过，仅输出参考"]


def test_add_error_flips_command_status_to_failure() -> None:
    """错误追加后 command_status 必须变为 failure。"""
    env = OutputEnvelope(command="estimate")
    env.add_error("boom")
    assert env.command_status is CommandStatus.FAILURE
    assert env.errors == ["boom"]


def test_exit_codes_match_section_10_4() -> None:
    """验收③：退出码 0/2/3/4/5 与 §10.4 语义一致。"""
    assert EXIT_OK == 0
    assert EXIT_INVALID_INPUT == 2
    assert EXIT_MISSING_DEPENDENCY == 3
    assert EXIT_VERSION_MISMATCH == 4
    assert EXIT_INTERNAL_ERROR == 5
    assert EXIT_OK in VALID_EXIT_CODES
    assert EXIT_INTERNAL_ERROR in VALID_EXIT_CODES


def test_error_subclasses_map_to_exit_codes() -> None:
    """验收③：错误分级类型映射到对应退出码。"""
    assert InvalidInputError("x").exit_code == EXIT_INVALID_INPUT
    assert MissingDependencyError("x").exit_code == EXIT_MISSING_DEPENDENCY
    assert VersionMismatchError("x").exit_code == EXIT_VERSION_MISMATCH
    assert InternalCommandError("x").exit_code == EXIT_INTERNAL_ERROR
    assert CommandError("x").exit_code == EXIT_INTERNAL_ERROR  # 基类默认未预期


def test_command_error_custom_exit_code_override() -> None:
    """CommandError 允许显式覆盖退出码。"""
    err = CommandError("custom", exit_code=EXIT_MISSING_DEPENDENCY)
    assert err.exit_code == EXIT_MISSING_DEPENDENCY


def test_envelope_from_error_carries_semantics() -> None:
    """failure 包络携带退出码语义与错误文本（Agent 可解析）。"""
    err = MissingDependencyError("valid_sale.parquet 缺失")
    env = envelope_from_error("estimate", err)
    assert env.command == "estimate"
    assert env.command_status is CommandStatus.FAILURE
    assert env.errors == [f"exit {EXIT_MISSING_DEPENDENCY}: valid_sale.parquet 缺失"]


def test_envelope_json_round_trip() -> None:
    """验收④：JSON 序列化/反序列化 round-trip 无损。"""
    env = OutputEnvelope(
        command="estimate",
        business_status="信息不足",
        run_id="RUN-9",
        data_version="v1",
        rule_version="1.0",
        result={"center": 12345},
        warnings=["案例不足"],
        artifacts=["05-估值报告/valuation_id=RUN-9/estimate.json"],
    )
    payload = env.model_dump_json()
    restored = OutputEnvelope.model_validate_json(payload)
    assert restored.model_dump() == env.model_dump()


def test_envelope_invalid_command_status_rejected() -> None:
    """包络字段类型校验：非法 command_status 被拒绝。"""
    with pytest.raises(ValidationError):
        # 故意传非法 str 验证 pydantic 运行时校验
        OutputEnvelope(command="estimate", command_status="maybe")  # type: ignore[arg-type]


def test_exit_code_labels_cover_all_valid_codes() -> None:
    """每个有效退出码都有 §10.4 语义说明。"""
    for code in VALID_EXIT_CODES:
        assert code in EXIT_CODE_LABELS
        assert EXIT_CODE_LABELS[code]


# ---------------------------------------------------------------------------
# WP7-C: compsval run show + compsval report build（Markdown 十二节）
# ---------------------------------------------------------------------------

_WP7C_COMMUNITY = "C-XXXX0013"
_WP7C_VAL_DATE = date(2026, 7, 21)
_WP7C_RUN_ID = f"RUN-SUBJ-REPORT-001-20260721-20260721-v{DEFAULT_RULE_VERSION}"


def _report_subject(**overrides: Any) -> SubjectProperty:
    base: dict[str, Any] = {
        "subject_id": "SUBJ-REPORT-001",
        "community_id": _WP7C_COMMUNITY,
        "area_sqm": Decimal("50.3"),
        "layout": "2室1厅",
        "valuation_date": _WP7C_VAL_DATE,
    }
    base.update(overrides)
    return SubjectProperty(**base)


def _seed_report_data(tmp_path: Path) -> Path:
    """构造 estimate/report 端到端数据目录（valid_sale + entities + manifest）。"""
    marts = tmp_path / MARTS_LAYER
    entities = tmp_path / "entities"
    marts.mkdir(parents=True)
    entities.mkdir(parents=True)
    valid_sale = pa.table(
        {
            "sale_event_id": ["r1", "r2", "r3"],
            "community_id": [_WP7C_COMMUNITY] * 3,
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
        pa.table({"community_id": [_WP7C_COMMUNITY], "block": ["东泊南"]}),
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
                "community_id": [_WP7C_COMMUNITY],
                "total_floors": [30],
                "has_elevator": [True],
                "year_built": [2015],
            }
        ),
        entities / "building.parquet",
    )
    return tmp_path


_REPORT_SECTIONS = [
    "1. 估值对象和估值时点",
    "2. 输出状态",
    "3. 中心值、估值范围和可信度",
    "4. 关键限制与补数建议",
    "5. 入选可比案例",
    "6. 被排除案例及原因",
    "7. 时间和差异处理",
    "8. 可信度分项",
    "9. 自动结果和人工复核",
    "10. 数据、代码、规则和运行版本",
    "11. 来源证据定位",
    "12. 后续结果区",
]


def test_report_build_contains_all_12_sections(tmp_path: Path) -> None:
    """验收①：Markdown 报告含 §11.2 全部十二节固定节。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    result = build_report_markdown(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    for section in _REPORT_SECTIONS:
        assert section in result.markdown, f"缺少报告节：{section}"


def test_report_prices_trace_to_frozen_estimate(tmp_path: Path) -> None:
    """验收②：报告关键价格与冻结 JSON 一致（同一冻结估值）。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    outcome = run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    frozen = json.loads(outcome.estimate_path.read_text(encoding="utf-8"))
    result = build_report_markdown(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    center = frozen["result"]["center"]
    assert f"{center:,.2f}" in result.markdown
    # 报告业务状态与冻结 JSON 一致
    assert result.business_status == frozen["business_status"]


def test_report_rebuild_idempotent_and_read_only(tmp_path: Path) -> None:
    """验收④：报告可重复生成且不修改冻结 JSON（同输入同输出）。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    outcome = run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    frozen_before = outcome.estimate_path.read_bytes()
    build_report_markdown(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    frozen_after = outcome.estimate_path.read_bytes()
    assert frozen_before == frozen_after  # 冻结 JSON 未被改写
    a = build_report_markdown(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    b = build_report_markdown(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    assert a.markdown == b.markdown  # 可重复生成


def test_report_build_missing_frozen_exit_3(tmp_path: Path) -> None:
    """无冻结估值 JSON → MissingDependencyError（退出码 3）。"""
    data_dir = _seed_report_data(tmp_path)
    with pytest.raises(MissingDependencyError):
        build_report_markdown(
            run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=tmp_path / "reports"
        )


def test_report_section8_shows_confidence_factors(tmp_path: Path) -> None:
    """第 8 节展示可信度分项（读 valuation_result.evidence 字段，F1 回归）。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    result = build_report_markdown(
        run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports
    )
    section8 = result.markdown.split("## 8. 可信度分项")[1].split("## 9.")[0]
    assert "分项证据未落表" not in section8
    assert any(
        line.startswith("- ") and "：" in line for line in section8.splitlines()
    )


def test_run_show_outputs_run_manifest(tmp_path: Path) -> None:
    """验收③：run show 输出运行清单（版本/参数/运行时间/产物路径）。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    envelope = show_run(run_id=_WP7C_RUN_ID, data_dir=data_dir, reports_root=reports)
    result = envelope.result
    assert result["run_id"] == _WP7C_RUN_ID
    assert result["rule_version"] == DEFAULT_RULE_VERSION
    assert "data_version" in result
    assert "code_version" in result
    assert "parameters" in result
    assert "run_at" in result
    assert "estimate" in result["products"]  # 冻结 JSON 产物路径


def test_run_show_unknown_run_exit_2(tmp_path: Path) -> None:
    """run 不存在 → InvalidInputError（退出码 2）。"""
    data_dir = _seed_report_data(tmp_path)
    run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    with pytest.raises(InvalidInputError):
        show_run(run_id="RUN-NOPE", data_dir=data_dir, reports_root=tmp_path / "reports")


def test_report_build_cli_writes_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """CLI：report build 生成 report.md 并登记 artifacts（stdout 为 JSON）。"""
    data_dir = _seed_report_data(tmp_path)
    reports = tmp_path / "reports"
    run_estimate(subject=_report_subject(), data_dir=data_dir, out_root=reports)
    rc = cli.main(
        [
            "report", "build",
            "--valuation", _WP7C_RUN_ID,
            "--data-dir", str(data_dir),
            "--out-dir", str(reports),
        ]
    )
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["command"] == "report build"
    assert parsed["artifacts"]
    report_path = report_path_for(_WP7C_RUN_ID, reports)
    assert report_path.is_file()
    assert REPORT_FILENAME in str(parsed["artifacts"][0])
