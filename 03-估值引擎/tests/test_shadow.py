"""WP9-A 影子运行基础设施（SHADOW-001）测试。

覆盖（对应 WP9-A 验收标准）：
- ① 影子标的可登记并逐一冻结估值（复用 estimate，包络完整）；
- ② 追踪表记录冻结结果且只读不改写（同 run 重复登记幂等不覆盖）；
- ③ 后续成交回填与误差计算（APE/区间命中）可复现、时间外（估值时点前
  成交与异小区成交不入）；
- ④ 近期误差滚动窗口与数据新鲜度检测输出可读，README §7.2 触发条件
  （误差扩大/区间失准/数据中断/样本不足）正确；
- ⑤ CLI shadow register/backfill/monitor 包络与退出码 0/2/3。
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
from compsval.entities import building as entities_building
from compsval.entities import community as entities_community
from compsval.entities import market_series as entities_market_series
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME
from compsval.valuation.shadow import (
    MIN_TRIGGER_SAMPLES,
    TRIGGER_DATA_STALE,
    TRIGGER_ERROR_EXPANSION,
    TRIGGER_INSUFFICIENT_SAMPLE,
    TRIGGER_RANGE_MISS,
    ShadowMonitorConfig,
    backfill_followups,
    compute_signed_metrics,
    compute_window_metrics,
    followup_error,
    monitor,
    register_subject,
    shadow_followup_schema,
    shadow_track_schema,
)

# ---------------------------------------------------------------------------
# 合成多期数据（schema 与真实 parquet 表一致；C1 含可比 + 后续成交）
# ---------------------------------------------------------------------------


def synthetic_valid_sale() -> pa.Table:
    """两个小区：C1 四条成交（S4 为估值时点后的后续成交），C2 两条异小区。"""
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
    marts = root / MARTS_LAYER
    marts.mkdir(parents=True, exist_ok=True)
    entities = root / entities_community.ENTITIES_LAYER
    entities.mkdir(parents=True, exist_ok=True)
    pq.write_table(synthetic_valid_sale(), marts / VALID_SALE_FILENAME)
    pq.write_table(synthetic_communities(), entities / entities_community.COMMUNITY_FILENAME)
    pq.write_table(synthetic_buildings(), entities / entities_building.BUILDING_FILENAME)
    pq.write_table(
        synthetic_market_series(), entities / f"{entities_market_series.MARKET_TABLE}.parquet"
    )


def _write_valid_sale_manifest(root: Path, fetched_at: str = "2026-08-21T00:00:00+00:00") -> None:
    """写 valid_sale 的 DerivedManifest 侧车（数据新鲜度检测的溯源依据）。"""
    manifest = {
        "manifest_version": 1,
        "layer": "marts",
        "table": "valid_sale",
        "built_at": "2026-08-21T00:00:00+00:00",
        "row_count": 6,
        "inputs": [{"dataset": "chengjiao", "fetched_at": fetched_at}],
        "package_version": "0.1.0",
        "notes": "synthetic",
    }
    path = (root / MARTS_LAYER / VALID_SALE_FILENAME).with_suffix(".manifest.json")
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def shadow_subject() -> SubjectProperty:
    """影子标的：C1 80㎡ 2室1厅，估值时点 2026-03-20（可比 S1/S2/S3 已成交）。"""
    return SubjectProperty(
        subject_id="SUBJ-SHADOW-001",
        community_id="C-SYN-001",
        area_sqm=Decimal("80"),
        layout="2室1厅",
        valuation_date=date(2026, 3, 20),
    )


# ---------------------------------------------------------------------------
# ① 影子标的登记（复用 estimate，包络完整）
# ---------------------------------------------------------------------------


def test_register_writes_track_with_frozen_estimate(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"

    outcome = register_subject(
        subject=shadow_subject(), data_dir=lake, out_root=out, notes="影子测试标的"
    )

    assert not outcome.duplicated
    assert outcome.run_id.startswith("RUN-")
    assert outcome.estimate_path.is_file()
    track = pq.read_table(outcome.track_path)
    assert track.num_rows == 1
    row = track.to_pylist()[0]
    assert row["subject_id"] == "SUBJ-SHADOW-001"
    assert row["run_id"] == outcome.run_id
    assert row["community_id"] == "C-SYN-001"
    assert row["valuation_date"] == date(2026, 3, 20)
    assert row["frozen_business_status"] == outcome.frozen_business_status
    assert row["rule_version"] == "1.0"
    # 冻结估值 JSON 与追踪行一致（同一冻结结果）
    frozen = json.loads(outcome.estimate_path.read_text(encoding="utf-8"))
    if frozen.get("result"):
        assert float(frozen["result"]["center"]) == row["frozen_center"]


def test_register_append_only_never_overwrites(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"

    first = register_subject(subject=shadow_subject(), data_dir=lake, out_root=out)
    track_before = pq.read_table(first.track_path).to_pylist()[0]
    second = register_subject(subject=shadow_subject(), data_dir=lake, out_root=out)

    assert second.duplicated is True
    track = pq.read_table(first.track_path)
    assert track.num_rows == 1  # 重复登记不追加行
    row = track.to_pylist()[0]
    assert row["run_id"] == first.run_id
    # frozen 结果只读：与首次登记一致（不因重跑改写）
    assert row["frozen_center"] == track_before["frozen_center"]


# ---------------------------------------------------------------------------
# ③ 后续成交回填与误差计算（时间外、不挑选样本、可复现）
# ---------------------------------------------------------------------------


def test_followup_error_known_answers() -> None:
    assert followup_error(
        frozen_center=12000.0, frozen_lower=10000.0, frozen_upper=14000.0, actual_unit_price=12000.0
    ) == (0.0, True)
    assert followup_error(
        frozen_center=12000.0, frozen_lower=10000.0, frozen_upper=14000.0, actual_unit_price=15000.0
    ) == (0.2, False)
    # 信息不足（无冻结值）→ 如实 None，不虚构
    assert followup_error(
        frozen_center=None, frozen_lower=None, frozen_upper=None, actual_unit_price=10000.0
    ) == (None, None)
    # 有中心但无区间 → APE 可算、区间命中 None
    assert followup_error(
        frozen_center=12000.0, frozen_lower=None, frozen_upper=None, actual_unit_price=10000.0
    ) == (0.2, None)


def test_backfill_out_of_time_and_same_community_only(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"

    register_subject(subject=shadow_subject(), data_dir=lake, out_root=out)
    outcome = backfill_followups(data_dir=lake)

    # 时间外：估值时点 2026-03-20 之后的同小区成交只有 S4(06-01)
    assert outcome.n_subjects == 1
    rows = outcome.followup.to_pylist()
    assert [r["sale_event_id"] for r in rows] == ["S4"]
    assert rows[0]["actual_sale_date"] == date(2026, 6, 1)
    assert rows[0]["actual_unit_price"] == 9000.0
    # APE/区间命中与冻结结果对拍（可复现、不虚构）
    track = pq.read_table(lake / "shadow" / "shadow_track.parquet").to_pylist()[0]
    center = track["frozen_center"]
    lower = track["frozen_range_lower"]
    upper = track["frozen_range_upper"]
    if center is not None:
        assert rows[0]["ape"] == pytest.approx(abs(center - 9000.0) / 9000.0)
    if lower is not None and upper is not None:
        assert rows[0]["range_hit"] == (lower <= 9000.0 <= upper)


def test_backfill_reproducible(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    out = tmp_path / "out"

    register_subject(subject=shadow_subject(), data_dir=lake, out_root=out)
    first = backfill_followups(data_dir=lake)
    second = backfill_followups(data_dir=lake)
    # 内容可复现；matched_at 为执行元数据（与 WP8 回放 run_at 同语义），不参与内容判定
    def _content(rows: list[dict[str, object]]) -> list[dict[str, object]]:
        return [{k: v for k, v in r.items() if k != "matched_at"} for r in rows]

    assert _content(first.followup.to_pylist()) == _content(second.followup.to_pylist())
    assert first.followup.num_rows == second.followup.num_rows


def test_backfill_missing_track_raises(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    from compsval.reporting.envelope import MissingDependencyError

    with pytest.raises(MissingDependencyError):
        backfill_followups(data_dir=lake)


# ---------------------------------------------------------------------------
# ④ 近期误差滚动窗口 + 数据新鲜度 + 触发条件
# ---------------------------------------------------------------------------


def _manual_followup(rows: list[dict[str, Any]]) -> pa.Table:
    names = shadow_followup_schema().names
    columns: dict[str, list[object]] = {name: [] for name in names}
    for row in rows:
        for name in names:
            columns[name].append(row.get(name))
    return pa.table(columns, schema=shadow_followup_schema())


def test_compute_window_metrics_known_answers() -> None:
    followup = _manual_followup(
        [
            {
                "subject_id": "S1", "run_id": "R1", "sale_event_id": f"F{i}",
                "source_id": "SRC-007", "community_id": "C1",
                "actual_sale_date": date(2026, 6, 1 + i), "actual_unit_price": 10000.0 + i,
                "area_sqm": 80.0, "layout": "2室1厅", "ape": ape, "range_hit": hit,
                "data_version": "v", "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
            for i, (ape, hit) in enumerate(
                [(0.1, True), (0.2, True), (0.3, True), (0.4, False), (0.5, True)]
            )
        ]
        + [
            {
                "subject_id": "S1", "run_id": "R1", "sale_event_id": "OLD",
                "source_id": "SRC-007", "community_id": "C1",
                "actual_sale_date": date(2026, 4, 1), "actual_unit_price": 11000.0,
                "area_sqm": 80.0, "layout": "2室1厅", "ape": 0.05, "range_hit": True,
                "data_version": "v", "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
        ]
    )
    metrics = compute_window_metrics(followup, window_days=30, as_of=date(2026, 6, 15))
    # 窗口 [2026-05-16, 2026-06-15]：5 行窗口内（OLD 在窗口外不入）
    assert metrics["n_window_sales"] == 5
    assert metrics["n_window_estimated"] == 5
    assert metrics["window_ape_median"] == pytest.approx(0.3)
    assert metrics["window_ape_high_quantile"] == pytest.approx(0.5)
    assert metrics["n_window_range"] == 5
    assert metrics["window_range_coverage_rate"] == pytest.approx(0.8)


def test_compute_signed_metrics_known_answers(tmp_path: Path) -> None:
    track = pa.table(
        {
            "subject_id": ["A", "B"], "run_id": ["R1", "R2"],
            "community_id": ["C1", "C2"], "valuation_date": [date(2026, 3, 1)] * 2,
            "area_sqm": [80.0, 90.0], "layout": ["2室1厅"] * 2,
            "frozen_center": [11000.0, 12000.0],
            "frozen_range_lower": [None, None], "frozen_range_upper": [None, None],
            "frozen_confidence": [None, None], "frozen_business_status": ["参考", "参考"],
            "data_version": ["v"] * 2, "rule_version": ["1.0"] * 2,
            "estimate_path": ["/x"] * 2, "registered_at": [datetime(2026, 3, 1, tzinfo=UTC)] * 2,
            "notes": [None, None],
        },
        schema=shadow_track_schema(),
    )
    followup = _manual_followup(
        [
            {
                "subject_id": "A", "run_id": "R1", "sale_event_id": f"F{i}",
                "source_id": "SRC-007", "community_id": "C1",
                "actual_sale_date": date(2026, 6, 1 + i), "actual_unit_price": 10000.0,
                "area_sqm": 80.0, "layout": "2室1厅", "ape": 0.1, "range_hit": None,
                "data_version": "v", "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
            for i in range(2)
        ]
        + [
            {
                "subject_id": "B", "run_id": "R2", "sale_event_id": "G0",
                "source_id": "SRC-007", "community_id": "C2",
                "actual_sale_date": date(2026, 6, 3), "actual_unit_price": 15000.0,
                "area_sqm": 90.0, "layout": "2室1厅", "ape": 0.2, "range_hit": None,
                "data_version": "v", "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
        ]
    )
    signed = compute_signed_metrics(followup, track, window_days=30, as_of=date(2026, 6, 15))
    # R1: 2×(+0.1)；R2: -0.2 → 中位 0.1、高估率 2/3
    assert signed["n_window_signed"] == 3
    assert signed["window_signed_error_median"] == pytest.approx(0.1)
    assert signed["window_overvaluation_rate"] == pytest.approx(2 / 3)


def test_monitor_triggers_and_freshness(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    _write_valid_sale_manifest(lake, fetched_at="2026-05-01T00:00:00+00:00")
    # 手工登记 1 个影子标的（冻结中心 12000、区间 [10000, 14000]）
    track_path = lake / "shadow" / "shadow_track.parquet"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track = pa.table(
        {
            "subject_id": ["SUBJ-SHADOW-001"], "run_id": ["RUN-SHADOW-1"],
            "community_id": ["C-SYN-001"], "valuation_date": [date(2026, 3, 20)],
            "area_sqm": [80.0], "layout": ["2室1厅"],
            "frozen_center": [12000.0], "frozen_range_lower": [10000.0],
            "frozen_range_upper": [14000.0], "frozen_confidence": ["低"],
            "frozen_business_status": ["参考"], "data_version": ["v"], "rule_version": ["1.0"],
            "estimate_path": ["/x/estimate.json"],
            "registered_at": [datetime(2026, 3, 21, tzinfo=UTC)], "notes": [None],
        },
        schema=shadow_track_schema(),
    )
    pq.write_table(track, track_path)
    # 6 笔窗口内后续成交：APE 中位 0.35 > 0.078（误差扩大）；区间命中 4/6 < 0.80（区间失准）
    followup = _manual_followup(
        [
            {
                "subject_id": "SUBJ-SHADOW-001", "run_id": "RUN-SHADOW-1",
                "sale_event_id": f"F{i}", "source_id": "SRC-007",
                "community_id": "C-SYN-001", "actual_sale_date": date(2026, 6, 1 + i),
                "actual_unit_price": float(price), "area_sqm": 80.0, "layout": "2室1厅",
                "ape": ape, "range_hit": hit, "data_version": "v",
                "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
            for i, (ape, hit, price) in enumerate(
                [
                    (0.1, True, 13000), (0.2, True, 14000), (0.3, True, 12500),
                    (0.4, False, 15000), (0.5, False, 8000), (0.5, True, 11000),
                ]
            )
        ]
    )
    followup_path = lake / "shadow" / "shadow_followup.parquet"
    pq.write_table(followup, followup_path)

    report = monitor(
        data_dir=lake,
        config=ShadowMonitorConfig(
            window_days=30, as_of=date(2026, 6, 15), stale_days=30
        ),
    )

    assert report.window_metrics["n_window_sales"] == 6
    assert report.window_metrics["window_ape_median"] == pytest.approx(0.35)
    assert report.window_metrics["window_range_coverage_rate"] == pytest.approx(4 / 6)
    triggers = {t["trigger"] for t in report.triggers}
    assert TRIGGER_ERROR_EXPANSION in triggers
    assert TRIGGER_RANGE_MISS in triggers
    # 数据新鲜度：最新快照 2026-05-01 距监测时点 2026-06-15 = 45 天 > 30 → 数据中断
    assert TRIGGER_DATA_STALE in triggers
    assert report.freshness["days_since_latest_fetch"] == 45
    assert report.freshness["fresh"] is False


def test_monitor_insufficient_window_sample_not_flagged(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    _write_valid_sale_manifest(lake)
    track_path = lake / "shadow" / "shadow_track.parquet"
    track_path.parent.mkdir(parents=True, exist_ok=True)
    track = pa.table(
        {
            "subject_id": ["S"], "run_id": ["R"], "community_id": ["C-SYN-001"],
            "valuation_date": [date(2026, 3, 20)], "area_sqm": [80.0],
            "layout": ["2室1厅"], "frozen_center": [12000.0],
            "frozen_range_lower": [None], "frozen_range_upper": [None],
            "frozen_confidence": [None], "frozen_business_status": ["参考"],
            "data_version": ["v"], "rule_version": ["1.0"], "estimate_path": ["/x"],
            "registered_at": [datetime(2026, 3, 21, tzinfo=UTC)], "notes": [None],
        },
        schema=shadow_track_schema(),
    )
    pq.write_table(track, track_path)
    # 窗口内样本 < MIN_TRIGGER_SAMPLES → 触发"样本不足"，不判定误差扩大/区间失准
    followup = _manual_followup(
        [
            {
                "subject_id": "S", "run_id": "R", "sale_event_id": "F0",
                "source_id": "SRC-007", "community_id": "C-SYN-001",
                "actual_sale_date": date(2026, 6, 1), "actual_unit_price": 5000.0,
                "area_sqm": 80.0, "layout": "2室1厅", "ape": 1.0, "range_hit": False,
                "data_version": "v", "matched_at": datetime(2026, 6, 15, tzinfo=UTC),
            }
        ]
    )
    pq.write_table(followup, lake / "shadow" / "shadow_followup.parquet")

    report = monitor(
        data_dir=lake,
        config=ShadowMonitorConfig(window_days=30, as_of=date(2026, 6, 15)),
    )
    triggers = {t["trigger"] for t in report.triggers}
    assert TRIGGER_INSUFFICIENT_SAMPLE in triggers
    assert TRIGGER_ERROR_EXPANSION not in triggers
    assert TRIGGER_RANGE_MISS not in triggers
    # 即使 1 笔样本 APE=1.0（远高于基线），样本不足也如实不判定
    assert report.window_metrics["n_window_estimated"] < MIN_TRIGGER_SAMPLES


# ---------------------------------------------------------------------------
# ⑤ CLI：shadow register/backfill/monitor（§10.3 包络 + §10.4 退出码）
# ---------------------------------------------------------------------------


def _subject_json(tmp_path: Path) -> Path:
    path = tmp_path / "subject.json"
    path.write_text(shadow_subject().model_dump_json(), encoding="utf-8")
    return path


def test_cli_shadow_register_backfill_monitor(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    _write_valid_sale_manifest(lake)
    out = tmp_path / "out"
    subject = _subject_json(tmp_path)

    assert (
        cli.main(
            [
                "shadow", "register", "--subject", str(subject),
                "--data-dir", str(lake), "--out-dir", str(out),
            ]
        )
        == 0
    )
    register_out = json.loads(capsys.readouterr().out)
    assert register_out["command"] == "shadow register"
    assert register_out["command_status"] == "success"
    assert register_out["result"]["duplicated"] is False
    run_id = register_out["run_id"]

    assert cli.main(["shadow", "backfill", "--data-dir", str(lake)]) == 0
    backfill_out = json.loads(capsys.readouterr().out)
    assert backfill_out["command"] == "shadow backfill"
    assert backfill_out["result"]["n_followup_sales"] == 1  # S4

    assert (
        cli.main(
            [
                "shadow", "monitor", "--data-dir", str(lake), "--as-of", "2026-06-15",
                "--window-days", "30",
            ]
        )
        == 0
    )
    monitor_out = json.loads(capsys.readouterr().out)
    assert monitor_out["command"] == "shadow monitor"
    assert monitor_out["result"]["subjects"][0]["run_id"] == run_id
    assert monitor_out["result"]["subjects"][0]["n_followup_sales"] == 1


def test_cli_shadow_missing_subject_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    missing = tmp_path / "nope.json"
    assert (
        cli.main(["shadow", "register", "--subject", str(missing), "--data-dir", str(lake)])
        == 2
    )
    out = json.loads(capsys.readouterr().out)
    assert out["command_status"] == "failure"
    assert out["errors"]


def test_cli_shadow_backfill_missing_track_exit_3(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    assert cli.main(["shadow", "backfill", "--data-dir", str(lake)]) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["command_status"] == "failure"


def test_cli_shadow_monitor_missing_track_exit_3(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    write_synthetic_lake(lake)
    assert cli.main(["shadow", "monitor", "--data-dir", str(lake)]) == 3
    out = json.loads(capsys.readouterr().out)
    assert out["command_status"] == "failure"


# ---------------------------------------------------------------------------
# CX-WP9-04 回归：全部子命令 --help 可渲染（argparse help 文案含 % 须转义）
# ---------------------------------------------------------------------------

_ALL_SUBCOMMAND_HELP: list[list[str]] = [
    ["--help"],
    ["version", "--help"],
    ["catalog", "--help"],
    ["estimate", "--help"],
    ["run", "--help"],
    ["run", "show", "--help"],
    ["report", "--help"],
    ["report", "build", "--help"],
    ["review", "--help"],
    ["review", "apply", "--help"],
    ["backtest", "--help"],
    ["backtest", "run", "--help"],
    ["backtest", "report", "--help"],
    ["shadow", "--help"],
    ["shadow", "register", "--help"],
    ["shadow", "backfill", "--help"],
    ["shadow", "monitor", "--help"],
    ["ingest", "--help"],
    ["ingest", "file", "--help"],
    ["data", "--help"],
    ["data", "stage", "--help"],
    ["data", "marts-build", "--help"],
    ["entities", "--help"],
    ["entities", "build", "--help"],
    ["valuation", "--help"],
    ["valuation", "build", "--help"],
    ["valuation", "tier", "--help"],
    ["valuation", "time", "--help"],
    ["valuation", "diff", "--help"],
    ["valuation", "aggregate", "--help"],
    ["valuation", "review", "--help"],
    ["system", "--help"],
    ["system", "check", "--help"],
]


@pytest.mark.parametrize("argv", _ALL_SUBCOMMAND_HELP)
def test_cli_subcommand_help_renders(
    argv: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    """CX-WP9-04：任一子命令 --help 不得崩溃（曾因 7.8%/80-90% 未转义 ValueError）。"""
    with pytest.raises(SystemExit) as excinfo:
        cli.main(argv)
    assert excinfo.value.code == 0
    assert "usage" in capsys.readouterr().out


def test_cli_shadow_monitor_help_percent_rendered(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CX-WP9-04：转义后的 %% 在渲染结果中还原为单个 %（含 % 的 help 展开正常）。"""
    with pytest.raises(SystemExit) as excinfo:
        cli.main(["shadow", "monitor", "--help"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    assert "7.8%" in out
    assert "80-90%" in out
