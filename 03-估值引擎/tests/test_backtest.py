"""WP8-A 回放引擎与指标（BT-001）测试：时间外切分、简单基准、§13.3 指标、
CLI 退出码与可重复性。

合成多期数据验证引擎正确性（无未来泄漏反例、基准已知答案、指标已知答案、
可重复）；真实数据覆盖限制在 WP8-C 报告中如实声明，不在本测试虚构。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from compsval import catalog
from compsval.reporting.envelope import InvalidInputError
from compsval.valuation.backtest import (
    BacktestConfig,
    compute_metrics,
    filter_pool,
    load_backtest_config,
    run_backtest,
    simple_baseline,
)

# ---------------------------------------------------------------------------
# 合成多期数据夹具（schema 与真实 parquet 表一致，保证 WP6 链路可跑）
# ---------------------------------------------------------------------------


def _utc_ts(value: datetime) -> pa.Array:
    return pa.array([value], type=pa.timestamp("us", tz="UTC"))


def synthetic_valid_sale() -> pa.Table:
    """两个小区、跨 4 个月的多期成交（含一条"未来"高价成交用于泄漏反例）。"""
    return pa.table(
        {
            "sale_event_id": ["S1", "S2", "S3", "S4", "T1", "T2"],
            "source_id": ["SRC-007"] * 6,
            "source_record_id": [f"rec-{i}" for i in range(6)],
            "snapshot_id": ["lianjia-chengjiao_list-20260821T000000Z"] * 6,
            "raw_locator": [f"line{i}" for i in range(6)],
            "fetched_at": pa.array(
                [datetime(2026, 8, 21, tzinfo=UTC)] * 6,
                type=pa.timestamp("us", tz="UTC"),
            ),
            "parser_version": ["1.0"] * 6,
            "sale_date": pa.array(
                [
                    date(2026, 1, 15),
                    date(2026, 2, 15),
                    date(2026, 3, 15),
                    date(2026, 6, 1),
                    date(2026, 2, 10),
                    date(2026, 3, 10),
                ]
            ),
            "event_date_precision": ["DAY"] * 6,
            "community": [
                "合成社区甲",
                "合成社区甲",
                "合成社区甲",
                "合成社区甲",
                "合成社区乙",
                "合成社区乙",
            ],
            "community_id": [
                "C-SYN-001",
                "C-SYN-001",
                "C-SYN-001",
                "C-SYN-001",
                "C-SYN-002",
                "C-SYN-002",
            ],
            "layout": ["2室1厅", "2室1厅", "2室1厅", "3室1厅", "1室1厅", "2室1厅"],
            "area_sqm": [80.0, 85.0, 85.0, 88.0, 70.0, 75.0],
            "total_price_yuan": [960000, 935000, 1003000, 792000, 1050000, 1162500],
            "original_price_text": ["96万", "94万", "100万", "79万", "105万", "116万"],
            "unit_price": [12000, 11000, 11800, 9000, 15000, 15500],
            "unit_price_observed": [12000, 11000, 11800, 9000, 15000, 15500],
            "unit_price_formula": ["total_price_yuan / area_sqm, rounded to integer"] * 6,
            "orientation": ["南", "南", "南", "北", "南", "南"],
            "listing_price_yuan": [1010000, 985000, 1053000, 842000, 1100000, 1212500],
            "listing_period_days": [30, 45, 20, 60, 25, 35],
            "anomaly_flag": ["正常"] * 6,
            "verification_status": ["已核验"] * 6,
        }
    )


def synthetic_communities() -> pa.Table:
    return pa.table(
        {
            "community_id": ["C-SYN-001", "C-SYN-002"],
            "standard_name": ["合成社区甲", "合成社区乙"],
            "block": ["板块A", "板块B"],
            "address": ["合成路1号", "合成路2号"],
            "latitude": [23.1, 23.2],
            "longitude": [113.3, 113.4],
            "coordinate_system": ["WGS84"] * 2,
            "boundary_status": ["机器确认"] * 2,
            "source_id": ["SRC-005", "SRC-005"],
            "source_key": ["1", "2"],
            "source_ref": ["synth:1", "synth:2"],
            "notes": ["合成测试数据"] * 2,
        }
    )


def synthetic_buildings() -> pa.Table:
    return pa.table(
        {
            "building_id": ["B-SYN-001", "B-SYN-002"],
            "community_id": ["C-SYN-001", "C-SYN-002"],
            "building_name": ["1栋", "2栋"],
            "year_built": [2010, 2005],
            "total_floors": [20, 10],
            "has_elevator": [True, False],
            "match_confidence": ["高", "高"],
            "source_id": ["SRC-007", "SRC-007"],
            "source_key": ["1", "2"],
            "source_ref": ["synth:b1", "synth:b2"],
        }
    )


def synthetic_market_series() -> pa.Table:
    return pa.table(
        {
            "series_id": ["MS-SYN-001", "MS-SYN-002"],
            "region": ["板块A", "板块A"],
            "month": [date(2026, 1, 1), date(2026, 2, 1)],
            "price": [12000.0, 11800.0],
            "price_change": [None, None],
            "source_strength": ["中", "中"],
            "revision_flag": [False, False],
            "source_id": ["SRC-008", "SRC-008"],
            "source_key": ["1", "2"],
            "source_ref": ["synth:ms1", "synth:ms2"],
        }
    )


def write_synthetic_lake(root: Path) -> None:
    from compsval.entities import building as eb
    from compsval.entities import community as ec
    from compsval.entities import market_series as ems
    from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME

    data_dir = root
    marts = data_dir / MARTS_LAYER
    marts.mkdir(parents=True, exist_ok=True)
    entities = data_dir / ec.ENTITIES_LAYER
    entities.mkdir(parents=True, exist_ok=True)
    if not (marts / VALID_SALE_FILENAME).exists():
        pq.write_table(synthetic_valid_sale(), marts / VALID_SALE_FILENAME)
    if not (entities / ec.COMMUNITY_FILENAME).exists():
        pq.write_table(synthetic_communities(), entities / ec.COMMUNITY_FILENAME)
    if not (entities / eb.BUILDING_FILENAME).exists():
        pq.write_table(synthetic_buildings(), entities / eb.BUILDING_FILENAME)
    if not (entities / f"{ems.MARKET_TABLE}.parquet").exists():
        pq.write_table(
            synthetic_market_series(), entities / f"{ems.MARKET_TABLE}.parquet"
        )


def _detail_row(
    *,
    replay_date: date,
    target: str,
    community_id: str,
    actual: float,
    center: float | None,
    lower: float | None,
    upper: float | None,
    status: str | None,
    baseline: float | None,
    skip_reason: str | None = None,
) -> dict[str, object]:
    return {
        "run_id": "BT-TEST",
        "replay_date": replay_date,
        "target_sale_event_id": target,
        "community_id": community_id,
        "area_sqm": 80.0,
        "layout": "2室1厅",
        "actual_unit_price": actual,
        "estimate_center": center,
        "range_lower": lower,
        "range_upper": upper,
        "confidence": "低" if center is not None else None,
        "business_status": status,
        "baseline_median": baseline,
        "baseline_count": 1 if baseline is not None else 0,
        "pool_size": 3,
        "pool_matched": 3,
        "skip_reason": skip_reason,
        "rule_version": "1.0",
    }


# ---------------------------------------------------------------------------
# 时间外切分（验收①）
# ---------------------------------------------------------------------------


def test_filter_pool_excludes_future_and_target() -> None:
    table = synthetic_valid_sale()
    pool = filter_pool(table, cutoff=date(2026, 2, 15), exclude_event_id="S2")
    ids = pool.column("sale_event_id").to_pylist()
    # S1(01-15)、T1(02-10) 在截点前且非目标 → 入池；S3/S4(未来) 与 S2(目标) 不入池
    assert ids == ["S1", "T1"]
    dates_in = pool.column("sale_date").to_pylist()
    assert all(isinstance(d, date) and d <= date(2026, 2, 15) for d in dates_in)


# ---------------------------------------------------------------------------
# 简单基准（验收②）
# ---------------------------------------------------------------------------


def test_simple_baseline_known_median() -> None:
    full = synthetic_valid_sale()
    pool = filter_pool(full, cutoff=date(2026, 2, 15), exclude_event_id="S2")
    median, count = simple_baseline(
        pool, "C-SYN-001", cutoff=date(2026, 2, 15), window_days=60
    )
    # 同小区在 [2025-12-17, 2026-02-15] 内且截点前：S1(12000)；S2 目标自身已排除
    assert median == 12000.0
    assert count == 1


def test_simple_baseline_window_excludes_old_sales() -> None:
    full = synthetic_valid_sale()
    pool = filter_pool(full, cutoff=date(2026, 2, 15), exclude_event_id="S2")
    # 窗口仅 1 天 → 同小区无成交（S1 在窗口外）
    median, count = simple_baseline(
        pool, "C-SYN-001", cutoff=date(2026, 2, 15), window_days=1
    )
    assert median is None
    assert count == 0


def test_simple_baseline_future_sales_excluded() -> None:
    # 纵深防御：即使传入未切分的全表，截点之后的成交也不得进入基准
    full = synthetic_valid_sale()
    median, count = simple_baseline(
        full, "C-SYN-001", cutoff=date(2026, 2, 15), window_days=60
    )
    # 同小区窗口内：[S1(12000), S2(11000)]；S3(03-15)/S4(06-01) 在未来，排除
    assert median == 11500.0
    assert count == 2


def test_simple_baseline_other_community_excluded() -> None:
    full = synthetic_valid_sale()
    pool = filter_pool(full, cutoff=date(2026, 3, 10), exclude_event_id="T2")
    # 目标小区为 C2：C1 的成交不算同小区
    median, count = simple_baseline(
        pool, "C-SYN-002", cutoff=date(2026, 3, 10), window_days=60
    )
    assert median == 15000.0
    assert count == 1


# ---------------------------------------------------------------------------
# §13.3 指标（验收③，已知答案对拍）
# ---------------------------------------------------------------------------


def test_compute_metrics_known_answers() -> None:
    detail = pa.Table.from_pylist(
        [
            _detail_row(
                replay_date=date(2026, 1, 1), target="P1", community_id="C", actual=110,
                center=100.0, lower=90.0, upper=120.0, status="参考", baseline=100.0,
            ),
            _detail_row(
                replay_date=date(2026, 1, 2), target="P2", community_id="C", actual=100,
                center=110.0, lower=100.0, upper=115.0, status="候选", baseline=120.0,
            ),
            _detail_row(
                replay_date=date(2026, 1, 3), target="P3", community_id="C", actual=100,
                center=200.0, lower=150.0, upper=180.0, status="候选", baseline=None,
            ),
            _detail_row(
                replay_date=date(2026, 1, 4), target="P4", community_id="C", actual=100,
                center=90.0, lower=None, upper=None, status="候选", baseline=90.0,
            ),
            _detail_row(
                replay_date=date(2026, 1, 5), target="P5", community_id="C", actual=100,
                center=105.0, lower=95.0, upper=110.0, status="候选", baseline=None,
            ),
            _detail_row(
                replay_date=date(2026, 1, 6), target="P6", community_id="C", actual=100,
                center=None, lower=None, upper=None, status=None, baseline=None,
                skip_reason="跳过原因",
            ),
        ]
    )
    metrics = compute_metrics(detail, high_quantile=0.90)
    values = {
        metric: value
        for metric, value in zip(
            metrics.column("metric").to_pylist(),
            metrics.column("value").to_pylist(),
            strict=True,
        )
    }
    assert values["n_targets"] == 6.0
    assert values["n_skipped"] == 1.0
    assert values["n_estimated"] == 5.0
    assert values["ape_median"] == pytest.approx(0.1)
    assert values["ape_high_quantile"] == pytest.approx(1.0)
    assert values["signed_error_median"] == pytest.approx(0.05)
    assert values["overvaluation_rate"] == pytest.approx(0.6)
    assert values["range_coverage_rate"] == pytest.approx(0.75)
    assert values["range_relative_width_median"] == pytest.approx(0.1464285)
    assert values["n_candidate"] == 4.0
    assert values["n_reference"] == 1.0
    assert values["n_formal"] == 0.0
    assert values["n_insufficient"] == 0.0
    assert values["n_with_baseline"] == 3.0
    assert values["baseline_ape_median"] == pytest.approx(0.1)


def test_compute_metrics_empty() -> None:
    detail = pa.table(
        {
            "run_id": [],
            "replay_date": pa.array([], type=pa.date32()),
            "target_sale_event_id": [],
            "community_id": [],
            "area_sqm": pa.array([], type=pa.float64()),
            "layout": [],
            "actual_unit_price": pa.array([], type=pa.float64()),
            "estimate_center": pa.array([], type=pa.float64()),
            "range_lower": pa.array([], type=pa.float64()),
            "range_upper": pa.array([], type=pa.float64()),
            "confidence": [],
            "business_status": [],
            "baseline_median": pa.array([], type=pa.float64()),
            "baseline_count": pa.array([], type=pa.int32()),
            "pool_size": pa.array([], type=pa.int32()),
            "pool_matched": pa.array([], type=pa.int32()),
            "skip_reason": [],
            "rule_version": [],
        }
    )
    metrics = compute_metrics(detail)
    values = {
        metric: value
        for metric, value in zip(
            metrics.column("metric").to_pylist(),
            metrics.column("value").to_pylist(),
            strict=True,
        )
    }
    assert values["n_targets"] == 0.0
    assert values["ape_median"] is None
    assert values["range_coverage_rate"] is None


# ---------------------------------------------------------------------------
# 回放主入口（验收④/⑤ + 无未来泄漏）
# ---------------------------------------------------------------------------


def test_run_backtest_synthetic(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"
    config = BacktestConfig(out_dir=out)
    outcome = run_backtest(config, data_dir=lake)

    assert outcome.detail_path.is_file()
    assert outcome.metrics_path.is_file()
    assert outcome.run_manifest_path.is_file()
    # DerivedManifest 侧车命名：<table>.manifest.json（manifests.derived_manifest_path）
    assert (outcome.detail_path.with_suffix(".manifest.json")).is_file()
    assert (outcome.metrics_path.with_suffix(".manifest.json")).is_file()

    detail = outcome.detail
    rows = detail.to_pylist()
    # 6 条成交 → 6 个回放目标（全部已匹配、字段完整 → 0 跳过）
    assert detail.num_rows == 6
    assert all(row["skip_reason"] is None for row in rows)

    # 未来泄漏反例：S2(02-15) 目标的池应只含截点前非自身成交
    s2 = next(r for r in rows if r["target_sale_event_id"] == "S2")
    assert s2["pool_size"] == 2  # S1(01-15) + T1(02-10)；不含 S3/S4/S2
    assert s2["pool_matched"] == 2
    assert s2["baseline_median"] == 12000.0
    assert s2["estimate_center"] is not None

    metrics = {
        metric: value
        for metric, value in zip(
            outcome.metrics.column("metric").to_pylist(),
            outcome.metrics.column("value").to_pylist(),
            strict=True,
        )
    }
    assert metrics["n_targets"] == 6.0
    assert metrics["n_skipped"] == 0.0
    assert metrics["n_estimated"] > 0.0


def test_run_backtest_deterministic(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"
    first = run_backtest(BacktestConfig(out_dir=out), data_dir=lake)
    second = run_backtest(BacktestConfig(out_dir=out), data_dir=lake)
    assert first.run_id == second.run_id
    assert first.detail.to_pylist() == second.detail.to_pylist()
    assert first.metrics.to_pylist() == second.metrics.to_pylist()
    # §12.2 运行清单产物哈希一致（内容可复现）
    manifest1 = json.loads(first.run_manifest_path.read_text(encoding="utf-8"))
    manifest2 = json.loads(second.run_manifest_path.read_text(encoding="utf-8"))
    assert manifest1["artifacts"] == manifest2["artifacts"]


def test_run_backtest_catalog_views(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    run_backtest(BacktestConfig(), data_dir=lake)
    con = catalog.connect(data_dir=lake)
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    assert "bt_backtest_detail" in tables
    assert "bt_backtest_metrics" in tables


def test_run_backtest_missing_dependency(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    marts = lake / "marts"
    marts.mkdir(parents=True)
    pq.write_table(synthetic_valid_sale(), marts / "valid_sale.parquet")
    with pytest.raises(Exception) as exc_info:
        run_backtest(BacktestConfig(), data_dir=lake)
    from compsval.reporting.envelope import MissingDependencyError

    assert isinstance(exc_info.value, MissingDependencyError)


def test_run_backtest_empty_lake(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME

    empty_sales = synthetic_valid_sale().slice(0, 0)
    pq.write_table(empty_sales, lake / MARTS_LAYER / VALID_SALE_FILENAME)
    out = tmp_path / "out"
    outcome = run_backtest(BacktestConfig(out_dir=out), data_dir=lake)
    assert outcome.detail.num_rows == 0
    metrics = {
        metric: value
        for metric, value in zip(
            outcome.metrics.column("metric").to_pylist(),
            outcome.metrics.column("value").to_pylist(),
            strict=True,
        )
    }
    assert metrics["n_targets"] == 0.0
    assert metrics["ape_median"] is None


# ---------------------------------------------------------------------------
# 配置校验（验收④：退出码 2 语义）
# ---------------------------------------------------------------------------


def test_load_backtest_config_missing_file(tmp_path: Path) -> None:
    with pytest.raises(InvalidInputError):
        load_backtest_config(tmp_path / "nope.yaml")


def test_load_backtest_config_invalid_yaml(tmp_path: Path) -> None:
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("not: [valid: yaml\n  - broken", encoding="utf-8")
    with pytest.raises(InvalidInputError):
        load_backtest_config(cfg)


def test_load_backtest_config_invalid_quantile(tmp_path: Path) -> None:
    cfg = tmp_path / "q.yaml"
    cfg.write_text("high_quantile: 1.5\n", encoding="utf-8")
    with pytest.raises(InvalidInputError):
        load_backtest_config(cfg)


def test_load_backtest_config_valid(tmp_path: Path) -> None:
    cfg = tmp_path / "ok.yaml"
    cfg.write_text(
        "rule_version: '1.0'\n"
        "replay_dates: ['2026-02-15']\n"
        "baseline_window_months: 12\n"
        "high_quantile: 0.9\n",
        encoding="utf-8",
    )
    config = load_backtest_config(cfg)
    assert config.replay_dates == (date(2026, 2, 15),)
    assert config.baseline_window_months == 12


# ---------------------------------------------------------------------------
# 汇总策略接入（add-aggregator-policy-experiment 任务 2.1–2.5）
# ---------------------------------------------------------------------------


def test_load_backtest_config_invalid_policy(tmp_path: Path) -> None:
    cfg = tmp_path / "bad_policy.yaml"
    cfg.write_text("rule_version: '1.0'\naggregation_policy: bogus\n", encoding="utf-8")
    with pytest.raises(InvalidInputError):
        load_backtest_config(cfg)


def test_load_backtest_config_default_policy_is_legacy(tmp_path: Path) -> None:
    cfg = tmp_path / "ok.yaml"
    cfg.write_text("rule_version: '1.0'\n", encoding="utf-8")
    config = load_backtest_config(cfg)
    assert config.aggregation_policy == "c0_weighted_median"


def test_run_backtest_policy_switch_changes_center_and_run_id(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    base = run_backtest(BacktestConfig(out_dir=tmp_path / "out_c0"), data_dir=lake)
    cand = run_backtest(
        BacktestConfig(
            out_dir=tmp_path / "out_c2",
            aggregation_policy="c2_weighted_quantile_p40",
        ),
        data_dir=lake,
    )
    # 默认策略 run_id 与既有构成一致；非默认策略 = 基础 id + 策略后缀
    assert cand.run_id == base.run_id + "-c2_weighted_quantile_p40"
    # 明细策略列：默认=默认id，候选=候选id
    base_rows = base.detail.to_pylist()
    cand_rows = cand.detail.to_pylist()
    assert all(row["aggregation_policy"] == "c0_weighted_median" for row in base_rows)
    assert all(row["aggregation_policy"] == "c2_weighted_quantile_p40" for row in cand_rows)
    # 诊断列在已估计行填充
    estimated = [row for row in cand_rows if row["estimate_center"] is not None]
    assert estimated
    assert all(row["n_comps"] is not None for row in estimated)
    # 区间构造跨候选不变；中心可不同
    by_id_c0 = {row["target_sale_event_id"]: row for row in base_rows}
    by_id_c2 = {row["target_sale_event_id"]: row for row in cand_rows}
    for target, row0 in by_id_c0.items():
        row2 = by_id_c2[target]
        assert row0["range_lower"] == row2["range_lower"]
        assert row0["range_upper"] == row2["range_upper"]
    # run_manifest 记录策略
    manifest = json.loads(cand.run_manifest_path.read_text(encoding="utf-8"))
    assert manifest["parameters"]["aggregation_policy"] == "c2_weighted_quantile_p40"


def test_run_backtest_policy_products_isolated(tmp_path: Path) -> None:
    """既有回放产物目录逐字节不变；实验产物只落指定 out_dir（2.5）。"""
    import hashlib

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    # 预置"既有回放产物目录"（含哨兵内容）
    legacy = lake / "backtest"
    legacy.mkdir()
    sentinel = legacy / "backtest_detail.parquet"
    pq.write_table(synthetic_valid_sale(), sentinel)
    before = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in legacy.iterdir()
    }
    out = tmp_path / "exp" / "c1_trimmed_weighted_mean"
    run_backtest(
        BacktestConfig(out_dir=out, aggregation_policy="c1_trimmed_weighted_mean"),
        data_dir=lake,
    )
    after = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in legacy.iterdir()
    }
    assert before == after
    assert (out / "backtest_detail.parquet").is_file()
    assert (out / "run_manifest.json").is_file()


def test_backtest_report_accepts_backtest_dir_override(tmp_path: Path) -> None:
    """compsval backtest report 可经 backtest_dir 指向实验产物目录（2.4）。"""
    from compsval.reporting.backtest_report import build_backtest_report

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "exp" / "c2_weighted_quantile_p40"
    outcome = run_backtest(
        BacktestConfig(out_dir=out, aggregation_policy="c2_weighted_quantile_p40"),
        data_dir=lake,
    )
    result = build_backtest_report(
        run_id=outcome.run_id,
        data_dir=lake,
        out_root=tmp_path / "reports",
        backtest_dir=out,
    )
    assert result.markdown_path.is_file()
    assert result.json_path.is_file()


# ---------------------------------------------------------------------------
# CLI 退出码（验收④）
# ---------------------------------------------------------------------------


def test_cli_backtest_valid(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from compsval.cli import main

    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    cfg = tmp_path / "bt.yaml"
    cfg.write_text("rule_version: '1.0'\n", encoding="utf-8")
    rc = main(["backtest", "run", "--config", str(cfg), "--data-dir", str(lake)])
    captured = capsys.readouterr()
    envelope = json.loads(captured.out)
    assert rc == 0
    assert envelope["command_status"] == "success"
    assert envelope["business_status"] in {"参考", "信息不足"}
    assert envelope["result"]["metrics"]["n_targets"] >= 0


def test_cli_backtest_missing_config(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from compsval.cli import main

    rc = main(["backtest", "run", "--config", str(tmp_path / "nope.yaml")])
    captured = capsys.readouterr()
    envelope = json.loads(captured.out)
    assert rc == 2
    assert envelope["command_status"] == "failure"
    assert envelope["errors"]


def test_cli_backtest_missing_dependency(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from compsval.cli import main

    lake = tmp_path / "lake"
    marts = lake / "marts"
    marts.mkdir(parents=True)
    pq.write_table(synthetic_valid_sale(), marts / "valid_sale.parquet")
    cfg = tmp_path / "bt.yaml"
    cfg.write_text("rule_version: '1.0'\n", encoding="utf-8")
    rc = main(["backtest", "run", "--config", str(cfg), "--data-dir", str(lake)])
    captured = capsys.readouterr()
    envelope = json.loads(captured.out)
    assert rc == 3
    assert envelope["command_status"] == "failure"
