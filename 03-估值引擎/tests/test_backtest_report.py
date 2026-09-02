"""WP8-B 分组指标、回放报告与版本治理回填测试。

对照 WP8-B 验收标准：
① 分组指标（§13.3 分组口径）且子组失败不隐藏；
② 回放报告 Markdown/JSON 可复现、关键指标与机器结果一致、含数据深度限制诚实声明；
③ §12.2 运行清单含产物哈希/警告/版本，可复查；
④ report build / review apply 包络 data_version/rule_version 从 run 表填充（回归）；
⑤ 回放入口数据/运行版本一致性预检（退出码 3/4 语义）；
⑥ ruff/mypy/pytest 通过。
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval.reporting.backtest_report import (
    _safe_dir_name,
    build_backtest_report,
)
from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
    VersionMismatchError,
)
from compsval.reporting.run_show import run_versions
from compsval.valuation.backtest import (
    BacktestConfig,
    area_band,
    compute_grouped_metrics,
    compute_metrics,
    over_performance_groups,
    run_backtest,
)
from compsval.valuation.candidate import (
    VALUATION_LAYER,
    VALUATION_RUN_FILENAME,
)
from tests.test_backtest import synthetic_valid_sale, write_synthetic_lake


def _detail_row(
    *,
    replay_date: date,
    target: str,
    community_id: str,
    layout: str,
    area: float,
    source: str,
    actual: float,
    center: float | None,
    status: str | None,
    confidence: str | None = "低",
) -> dict[str, object]:
    return {
        "run_id": "BT-TEST",
        "replay_date": replay_date,
        "target_sale_event_id": target,
        "community_id": community_id,
        "source_id": source,
        "area_sqm": area,
        "layout": layout,
        "actual_unit_price": actual,
        "estimate_center": center,
        "range_lower": None,
        "range_upper": None,
        "confidence": confidence,
        "business_status": status,
        "baseline_median": None,
        "baseline_count": 0,
        "pool_size": 2,
        "pool_matched": 2,
        "skip_reason": None,
        "rule_version": "1.0",
    }


# ---------------------------------------------------------------------------
# ① 分组指标
# ---------------------------------------------------------------------------


def test_area_band_boundaries() -> None:
    assert area_band(None) is None
    assert area_band(49.9) == "<50"
    assert area_band(50.0) == "50-70"
    assert area_band(69.9) == "50-70"
    assert area_band(70.0) == "70-90"
    assert area_band(85.0) == "70-90"
    assert area_band(90.0) == "90-110"
    assert area_band(110.0) == "110-130"
    assert area_band(142.42) == ">=130"


def test_compute_grouped_metrics_dimensions() -> None:
    detail = pa.Table.from_pylist(
        [
            _detail_row(
                replay_date=date(2026, 1, 1), target="A1", community_id="C1",
                layout="2室1厅", area=85.0, source="SRC-007", actual=100.0,
                center=105.0, status="候选", confidence="低",
            ),
            _detail_row(
                replay_date=date(2026, 1, 2), target="A2", community_id="C1",
                layout="2室1厅", area=60.0, source="SRC-007", actual=100.0,
                center=95.0, status="参考", confidence="中",
            ),
            _detail_row(
                replay_date=date(2026, 1, 3), target="B1", community_id="C2",
                layout="3室1厅", area=140.0, source="SRC-005", actual=200.0,
                center=160.0, status="候选", confidence="低",
            ),
        ]
    )
    grouped = compute_grouped_metrics(detail)
    rows = grouped.to_pylist()
    dims = {str(r["group_dimension"]) for r in rows}
    assert dims == {"community_id", "layout", "area_band", "source_id", "confidence"}

    # 分组值与整体同口径：C1 组 ape_median == 对子集整体指标 ape_median
    c1 = detail.take([0, 1])
    expected = {
        m: v
        for m, v in zip(
            compute_metrics(c1).column("metric").to_pylist(),
            compute_metrics(c1).column("value").to_pylist(),
            strict=True,
        )
    }
    c1_rows = [
        r
        for r in rows
        if r["group_dimension"] == "community_id" and r["group_value"] == "C1"
    ]
    by_metric = {str(r["metric"]): r["value"] for r in c1_rows}
    assert by_metric["n_targets"] == 2.0
    assert by_metric["ape_median"] == expected["ape_median"]
    # 面积段分组：85 → 70-90，120 → >=130
    bands = {
        str(r["group_value"]) for r in rows if r["group_dimension"] == "area_band"
    }
    assert "70-90" in bands and ">=130" in bands


def test_compute_grouped_metrics_empty() -> None:
    names = [
        "run_id", "replay_date", "target_sale_event_id", "community_id", "source_id",
        "area_sqm", "layout", "actual_unit_price", "estimate_center", "range_lower",
        "range_upper", "confidence", "business_status", "baseline_median",
        "baseline_count", "pool_size", "pool_matched", "skip_reason", "rule_version",
    ]
    detail = pa.table({name: [] for name in names})
    grouped = compute_grouped_metrics(detail)
    assert grouped.num_rows == 0


def test_over_performance_groups_flags_higher_subgroups() -> None:
    detail = pa.Table.from_pylist(
        [
            _detail_row(
                replay_date=date(2026, 1, 1), target="A1", community_id="C1",
                layout="2室1厅", area=85.0, source="SRC-007", actual=100.0,
                center=105.0, status="候选",
            ),
            _detail_row(
                replay_date=date(2026, 1, 2), target="B1", community_id="C2",
                layout="3室1厅", area=120.0, source="SRC-007", actual=200.0,
                center=100.0, status="候选",
            ),
        ]
    )
    overall = compute_metrics(detail)  # ape_median 由两个点决定
    grouped = compute_grouped_metrics(detail)
    flags = over_performance_groups(grouped, overall)
    # C1 组 APE(0.05) 与 C2 组 APE(0.5)：C2 组高于整体 → 应被标出
    flagged = {(d, v) for d, v, _g, _o in flags}
    assert ("community_id", "C2") in flagged
    assert ("community_id", "C1") not in flagged


# ---------------------------------------------------------------------------
# ② 回放报告（可复现 + 指标一致 + 诚实声明）
# ---------------------------------------------------------------------------


def test_build_backtest_report_sections_and_consistency(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    outcome = run_backtest(BacktestConfig(), data_dir=lake)
    reports = tmp_path / "reports"

    result = build_backtest_report(
        run_id=outcome.run_id, data_dir=lake, out_root=reports
    )
    assert result.run_id == outcome.run_id
    assert result.markdown_path.is_file()
    assert result.json_path.is_file()
    for section in (
        "## 1. 运行清单（§12.2）",
        "## 2. 整体指标（§13.3）",
        "## 3. 与简单基准对比",
        "## 4. 分组指标（§13.3）",
        "## 5. 需关注子组",
        "## 6. 数据深度限制与警告",
        "## 7. 警告",
        "## 8. 产物清单与哈希",
    ):
        assert section in result.markdown, f"缺少报告节：{section}"

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == outcome.run_id
    assert payload["data_version"] == outcome.data_version
    assert payload["metrics"]["n_targets"] == float(outcome.detail.num_rows)
    assert "数据深度限制" in payload["data_depth_limits"]["note"]
    assert isinstance(payload["warnings"], list)
    # 报告关键指标与机器产物一致（n_estimated 与明细表一致）
    centers = outcome.detail.column("estimate_center").to_pylist()
    n_estimated = sum(1 for c in centers if c is not None)
    assert payload["metrics"]["n_estimated"] == float(n_estimated)


def test_build_backtest_report_reproducible(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    outcome = run_backtest(BacktestConfig(), data_dir=lake)
    reports = tmp_path / "reports"
    a = build_backtest_report(run_id=outcome.run_id, data_dir=lake, out_root=reports)
    b = build_backtest_report(run_id=outcome.run_id, data_dir=lake, out_root=reports)
    assert a.markdown == b.markdown
    assert a.markdown_path.read_bytes() == b.markdown_path.read_bytes()


def test_build_backtest_report_run_id_mismatch(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    run_backtest(BacktestConfig(), data_dir=lake)
    with pytest.raises(InvalidInputError):
        build_backtest_report(
            run_id="BT-WRONG", data_dir=lake, out_root=tmp_path / "reports"
        )


def test_build_backtest_report_missing_products(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    with pytest.raises(MissingDependencyError):
        build_backtest_report(
            run_id="BT-X", data_dir=lake, out_root=tmp_path / "reports"
        )


def test_run_manifest_warns_on_unmatched_targets(tmp_path: Path) -> None:
    """覆盖受限（存在未匹配目标）时运行清单与报告如实警告（不假装覆盖）。"""
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME

    base = synthetic_valid_sale()
    # 复制一条成交并置空 community_id → 回放目标将被跳过（未匹配小区）
    unmatched = base.slice(0, 1)
    unmatched = unmatched.set_column(
        unmatched.column_names.index("sale_event_id"),
        "sale_event_id",
        pa.array(["SX"], type=pa.string()),
    )
    unmatched = unmatched.set_column(
        unmatched.column_names.index("community_id"),
        "community_id",
        pa.array([""], type=pa.string()),
    )
    unmatched = unmatched.set_column(
        unmatched.column_names.index("sale_date"),
        "sale_date",
        pa.array([date(2026, 6, 1)], type=pa.date32()),
    )
    merged = pa.concat_tables([base, unmatched])
    pq.write_table(merged, lake / MARTS_LAYER / VALID_SALE_FILENAME)

    outcome = run_backtest(BacktestConfig(), data_dir=lake)
    manifest = json.loads(outcome.run_manifest_path.read_text(encoding="utf-8"))
    assert any("跳过" in w for w in manifest["warnings"])
    # 未匹配目标在明细中如实留痕 skip_reason
    skipped = [
        r for r in outcome.detail.to_pylist() if r["skip_reason"] is not None
    ]
    assert any(r["target_sale_event_id"] == "SX" for r in skipped)


# ---------------------------------------------------------------------------
# ③ 运行清单（哈希/警告/版本）与 ⑤ 版本一致性预检
# ---------------------------------------------------------------------------


def test_run_manifest_has_warnings_and_artifacts_hashes(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    outcome = run_backtest(BacktestConfig(out_dir=tmp_path / "out"), data_dir=lake)
    manifest = json.loads(outcome.run_manifest_path.read_text(encoding="utf-8"))
    assert "warnings" in manifest
    assert "code_version" in manifest
    assert "data_version" in manifest
    assert "rule_version" in manifest
    assert len(manifest["artifacts"]) == 3
    for artifact in manifest["artifacts"]:
        assert artifact["path"]
        assert len(artifact["sha256"]) == 64


def test_version_precheck_mismatch_exit_4(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    with pytest.raises(VersionMismatchError):
        run_backtest(
            BacktestConfig(expected_data_version="WRONG"), data_dir=lake
        )


def test_version_precheck_match_ok(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    # 无 manifest → data_version="UNKNOWN"；期望 "UNKNOWN" 时通过预检
    outcome = run_backtest(
        BacktestConfig(expected_data_version="UNKNOWN"), data_dir=lake
    )
    assert outcome.data_version == "UNKNOWN"


# ---------------------------------------------------------------------------
# ④ 版本治理回填（report build / review apply 包络）
# ---------------------------------------------------------------------------


def test_run_versions_reads_run_table(tmp_path: Path) -> None:
    valuation = tmp_path / VALUATION_LAYER
    valuation.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "run_id": ["RUN-X"],
                "data_version": ["lianjia/chengjiao_list@20260821T000000Z"],
                "rule_version": ["1.0"],
            }
        ),
        valuation / VALUATION_RUN_FILENAME,
    )
    data_version, rule_version = run_versions(run_id="RUN-X", data_dir=tmp_path)
    assert data_version == "lianjia/chengjiao_list@20260821T000000Z"
    assert rule_version == "1.0"
    # 缺失 run → (None, None)，不虚构
    assert run_versions(run_id="RUN-NA", data_dir=tmp_path) == (None, None)
    assert run_versions(run_id="RUN-X", data_dir=tmp_path / "empty") == (None, None)


def test_cli_report_build_envelope_versions_backfilled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """report build 包络 data_version/rule_version 从 run 表填充（回归）。"""
    from compsval import cli
    from compsval.contract.models import SubjectProperty
    from compsval.valuation.estimate import run_estimate
    from tests.test_review_apply import _COMMUNITY, _VAL_DATE
    from tests.test_review_apply import _seed_data as seed_review

    data_dir = seed_review(tmp_path)
    subject = SubjectProperty(
        subject_id="SUBJ-BT-001",
        community_id=_COMMUNITY,
        area_sqm=Decimal("50.3"),
        layout="2室1厅",
        valuation_date=_VAL_DATE,
    )
    reports = tmp_path / "reports"
    outcome = run_estimate(subject=subject, data_dir=data_dir, out_root=reports)
    rc = cli.main(
        [
            "report", "build",
            "--valuation", outcome.run_id,
            "--data-dir", str(data_dir),
            "--out-dir", str(reports),
        ]
    )
    assert rc == 0
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["data_version"] is not None
    assert envelope["rule_version"] == "1.0"


def test_safe_dir_name_truncates_long_run_id_deterministically() -> None:
    """超长 run_id（多源数据版本）消毒名有界且确定性（Windows 255 上限防护，G3R-E）。"""
    base = (
        "BT-1.0-chengjiao@2026-08-22T15:24:43.769650+00:00"
        ";chengjiao@2026-08-22T15:31:31.188978+00:00"
    )
    long_id = base * 3 + "-20160120-20260721-608p"
    assert len(long_id) > 255
    name = _safe_dir_name(long_id)
    assert len(name) <= 120  # 目录名单项安全上限
    assert name == _safe_dir_name(long_id)  # 确定性
    assert name != _safe_dir_name(long_id + "-other")
    # 短 run_id 保持原样（不截断既有行为）
    assert _safe_dir_name("BT-1.0-short") == "BT-1.0-short"


# ---------------------------------------------------------------------------
# CLI：compsval backtest report 退出码
# ---------------------------------------------------------------------------


def test_cli_backtest_report_valid(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from compsval import cli

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    outcome = run_backtest(BacktestConfig(), data_dir=lake)
    reports = tmp_path / "reports"
    rc = cli.main(
        [
            "backtest", "report",
            "--run", outcome.run_id,
            "--data-dir", str(lake),
            "--out-dir", str(reports),
        ]
    )
    assert rc == 0
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["command"] == "backtest report"
    assert envelope["result"]["report_markdown_path"]
    assert len(envelope["artifacts"]) == 2


def test_cli_backtest_report_run_id_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from compsval import cli

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    run_backtest(BacktestConfig(), data_dir=lake)
    rc = cli.main(
        [
            "backtest", "report",
            "--run", "BT-WRONG",
            "--data-dir", str(lake),
            "--out-dir", str(tmp_path / "reports"),
        ]
    )
    assert rc == 2
    envelope = json.loads(capsys.readouterr().out)
    assert envelope["command_status"] == "failure"


def test_cli_backtest_version_mismatch_exit_4(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from compsval import cli

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    cfg = tmp_path / "bt.yaml"
    cfg.write_text(
        "rule_version: '1.0'\nexpected_data_version: WRONG\n", encoding="utf-8"
    )
    rc = cli.main(["backtest", "run", "--config", str(cfg), "--data-dir", str(lake)])
    envelope = json.loads(capsys.readouterr().out)
    assert rc == 4
    assert envelope["command_status"] == "failure"
