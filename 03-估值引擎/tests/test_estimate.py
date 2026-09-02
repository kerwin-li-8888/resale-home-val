"""WP7-B: ``compsval estimate`` 端到端估值命令（REP-001 执行）。

对照 WP7-B 验收标准：
① 一次命令从 subject JSON 生成冻结估值 JSON（§10.3 包络全字段），非交互可调用；
② --as-of 与 subject.valuation_date 不一致 → 退出码 2 且不写产物；
③ 必要数据/表缺失 → 退出码 3；版本不一致 → 退出码 4；
④ 同一输入/数据/规则重复运行结果一致（可重复）；
⑤ 冻结 JSON 的估值字段与 valuation_result 表一致（同一冻结）；
⑥ ruff/mypy/pytest 通过。
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
from compsval.contract.models import (
    ConfidenceLevel,
    OutputStatus,
    SubjectProperty,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.reporting.envelope import (
    CommandStatus,
    InvalidInputError,
    MissingDependencyError,
    VersionMismatchError,
)
from compsval.valuation.aggregation import (
    ValuationResultOutcome,
    ValuationResultTabled,
)
from compsval.valuation.candidate import DEFAULT_RULE_VERSION
from compsval.valuation.estimate import run_estimate

_COMMUNITY = "C-XXXX0013"
_VAL_DATE = date(2026, 7, 21)
_RUN_ID = f"RUN-SUBJ-TEST-001-20260721-20260721-v{DEFAULT_RULE_VERSION}"


def _subject(**overrides: Any) -> SubjectProperty:
    base: dict[str, Any] = {
        "subject_id": "SUBJ-TEST-001",
        "community_id": _COMMUNITY,
        "area_sqm": Decimal("50.3"),
        "layout": "2室1厅",
        "valuation_date": _VAL_DATE,
    }
    base.update(overrides)
    return SubjectProperty(**base)


def _seed_data(tmp_path: Path) -> Path:
    """构造 estimate 端到端最小数据目录（marts/valid_sale + entities + manifest）。"""
    marts = tmp_path / MARTS_LAYER
    entities = tmp_path / "entities"
    marts.mkdir(parents=True)
    entities.mkdir(parents=True)

    valid_sale = pa.table(
        {
            "sale_event_id": ["e1", "e2", "e3", "e4", "e5"],
            "community_id": [_COMMUNITY] * 5,
            "sale_date": [
                date(2026, 7, 20),
                date(2026, 7, 10),
                date(2026, 6, 20),
                date(2026, 5, 1),
                date(2026, 4, 1),
            ],
            "layout": ["2室1厅"] * 5,
            "area_sqm": [50.0, 51.0, 52.0, 49.0, 55.0],
            "total_price_yuan": [1300000.0] * 5,
            "unit_price": [26000, 25490, 25000, 26530, 23636],
            "anomaly_flag": ["正常"] * 5,
            "raw_locator": ["1", "2", "3", "4", "5"],
            "orientation": ["南"] * 5,
        }
    )
    valid_sale_path = marts / VALID_SALE_FILENAME
    pq.write_table(valid_sale, valid_sale_path)
    write_derived_manifest(
        DerivedManifest(
            layer=MARTS_LAYER,
            table="valid_sale",
            built_at=datetime.now(UTC),
            row_count=5,
            inputs=[InputRef(dataset="lianjia/chengjiao_list", fetched_at="20260821T000000Z")],
            package_version="0.1.0",
            notes="test fixture",
        ),
        valid_sale_path,
    )

    community = pa.table({"community_id": [_COMMUNITY], "block": ["东泊南"]})
    pq.write_table(community, entities / "community.parquet")

    market_series = pa.table(
        {
            "region": ["东泊南", "东泊南", "东泊南"],
            "month": [date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)],
            "price": [25000, 25500, 26000],
        }
    )
    pq.write_table(market_series, entities / "market_series.parquet")

    building = pa.table(
        {
            "community_id": [_COMMUNITY],
            "total_floors": [30],
            "has_elevator": [True],
            "year_built": [2015],
        }
    )
    pq.write_table(building, entities / "building.parquet")
    return tmp_path


# ---- ① 端到端成功：冻结估值 JSON + §10.3 包络 ----
def test_estimate_end_to_end_success(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    outcome = run_estimate(
        subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports"
    )
    env = outcome.envelope
    assert env.command == "estimate"
    assert env.command_status is CommandStatus.SUCCESS
    # formal 未开启 → 业务状态为参考（不是失败，也不是正式）
    assert env.business_status == "参考"
    assert env.run_id == _RUN_ID
    assert outcome.result is not None
    assert outcome.estimate_path.is_file()
    assert str(outcome.estimate_path) in env.artifacts
    assert env.result["status"] == "参考"
    assert env.result["center"] > 0
    assert env.result["range"][0] is not None
    assert env.result["confidence"] in {"高", "中", "低", "不足"}


def test_estimate_frozen_json_matches_valuation_result(tmp_path: Path) -> None:
    """验收⑤：冻结 JSON 估值字段与 valuation_result 表一致（同一冻结）。"""
    data_dir = _seed_data(tmp_path)
    outcome = run_estimate(
        subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports"
    )
    result_path = data_dir / "valuation" / "valuation_result.parquet"
    assert result_path.is_file()
    row = pq.read_table(result_path).to_pylist()[0]
    payload = outcome.envelope.result
    assert float(payload["center"]) == float(row["center"])
    assert float(payload["range"][0]) == float(row["range_lower"])
    assert float(payload["range"][1]) == float(row["range_upper"])
    assert payload["confidence"] == row["confidence"]
    assert payload["status"] == row["status"]
    assert payload["rule_version"] == row["rule_version"]


# ---- ② --as-of 不一致 → 退出码 2 且不写产物 ----
def test_estimate_as_of_mismatch_exit_2(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    reports = tmp_path / "reports"
    with pytest.raises(InvalidInputError):
        run_estimate(
            subject=_subject(),
            data_dir=data_dir,
            as_of=date(2026, 8, 1),
            out_root=reports,
        )
    assert not reports.exists() or not list(reports.glob("**/*"))


def test_estimate_cli_as_of_mismatch_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data_dir = _seed_data(tmp_path)
    subject_path = tmp_path / "subject.json"
    subject_path.write_text(_subject().model_dump_json(), encoding="utf-8")
    rc = cli.main(
        [
            "estimate",
            "--subject", str(subject_path),
            "--as-of", "2026-08-01",
            "--data-dir", str(data_dir),
            "--out-dir", str(tmp_path / "reports"),
        ]
    )
    assert rc == 2
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["command_status"] == "failure"
    assert parsed["command"] == "estimate"


# ---- ③ 依赖缺失 → 退出码 3；版本不一致 → 退出码 4 ----
def test_estimate_missing_dependency_exit_3(tmp_path: Path) -> None:
    with pytest.raises(MissingDependencyError):
        run_estimate(subject=_subject(), data_dir=tmp_path, out_root=tmp_path / "reports")


def test_estimate_version_mismatch_exit_4(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = _seed_data(tmp_path)

    def _fake_aggregation(**_: Any) -> ValuationResultOutcome:
        return ValuationResultOutcome(
            result_path=data_dir / "valuation" / "valuation_result.parquet",
            result=ValuationResultTabled(
                result_id=f"{_RUN_ID}-RES",
                run_id=_RUN_ID,
                subject_id="SUBJ-TEST-001",
                center=Decimal("10000"),
                range_lower=Decimal("9000"),
                range_upper=Decimal("11000"),
                confidence=ConfidenceLevel.MEDIUM,
                status=OutputStatus.REFERENCE,
                valuation_date=_VAL_DATE,
                rule_version="9.9",  # 与当前不一致 → 拒绝继续
                evidence_json="{}",
                reason="fake",
            ),
            n_comps=3,
            effective_samples=3.0,
        )

    monkeypatch.setattr(
        "compsval.valuation.estimate.apply_aggregation", _fake_aggregation
    )
    with pytest.raises(VersionMismatchError):
        run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")


# ---- ④ 可重复：同输入同结果 ----
def test_estimate_reproducible_same_run(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    a = run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    b = run_estimate(subject=_subject(), data_dir=data_dir, out_root=tmp_path / "reports")
    assert a.run_id == b.run_id
    assert a.envelope.result == b.envelope.result
    assert a.envelope.business_status == b.envelope.business_status


# ---- 业务降级：无入选可比 → 信息不足（success 包络，不冒充失败）----
def test_estimate_no_comps_insufficient(tmp_path: Path) -> None:
    data_dir = _seed_data(tmp_path)
    # subject 小区不在数据中 → 跨社区无可比 → 信息不足
    outcome = run_estimate(
        subject=_subject(community_id="C-NONE"),
        data_dir=data_dir,
        out_root=tmp_path / "reports",
    )
    env = outcome.envelope
    assert env.command_status is CommandStatus.SUCCESS
    assert env.business_status == "信息不足"
    assert outcome.result is None
    assert outcome.estimate_path.is_file()
    assert any("无入选可比" in w for w in env.warnings)


# ---- CLI：stdout 只写机器可解析 JSON（§10.1）----
def test_estimate_cli_stdout_is_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = _seed_data(tmp_path)
    subject_path = tmp_path / "subject.json"
    subject_path.write_text(_subject().model_dump_json(), encoding="utf-8")
    rc = cli.main(
        [
            "estimate",
            "--subject", str(subject_path),
            "--data-dir", str(data_dir),
            "--out-dir", str(tmp_path / "reports"),
        ]
    )
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["command"] == "estimate"
    assert parsed["command_status"] == "success"
    assert parsed["business_status"] == "参考"
    assert parsed["artifacts"], "冻结估值 JSON 应登记到 artifacts"
