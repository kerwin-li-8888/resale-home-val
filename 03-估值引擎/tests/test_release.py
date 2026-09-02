"""WP9-E 发布准备测试（G5 formal 输出门禁）。

对应 WP9-E 验收③：formal 输出门禁实现且反例测试（未过 G5 不输出 formal）。
双闸：``formal_release_enabled``（发布开关，G5 通过前 False）+ 目标小区在
ScopePolicy 纳入范围（``_in_formal_scope``）。合成数据验证，不依赖真实湖。

CX-WP9-02 修复追加：受控 formal 启用路径——官方 CLI ``compsval estimate`` 读取
``<data_dir>/release/release_decision.json``（RELEASE1-001 用户发布决定的
运行载体）；缺失配置/未授权记录一律保持候选/参考。覆盖正常、范围外、
缺失配置、未授权（released=false/缺字段/坏 JSON/非对象）反例。
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
from compsval.valuation.estimate import (
    EstimateOutcome,
    load_release_decision,
    release_decision_path,
    run_estimate,
)
from compsval.valuation.scope import (
    ACTIVE_SCOPE_POLICY_VERSION,
    scope_policy_filename,
    scope_policy_schema,
)


def synthetic_valid_sale() -> pa.Table:
    """两个小区：C1 七条（S1-S3/S5-S7 为 2026-03-20 时点前可比，S4 为后续成交），C2 两条。"""
    return pa.table(
        {
            "sale_event_id": ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "T1", "T2"],
            "source_id": ["SRC-007"] * 9,
            "source_record_id": [f"rec-{i}" for i in range(9)],
            "snapshot_id": ["lianjia-chengjiao_list-20260821T000000Z"] * 9,
            "raw_locator": [f"line{i}" for i in range(9)],
            "fetched_at": pa.array(
                [datetime(2026, 8, 21, tzinfo=UTC)] * 9,
                type=pa.timestamp("us", tz="UTC"),
            ),
            "parser_version": ["1.0"] * 9,
            "sale_date": pa.array(
                [
                    date(2026, 1, 15), date(2026, 2, 15), date(2026, 3, 15), date(2026, 6, 1),
                    date(2026, 2, 20), date(2026, 3, 1), date(2026, 3, 5),
                    date(2026, 2, 10), date(2026, 3, 10),
                ]
            ),
            "event_date_precision": ["DAY"] * 9,
            "community": ["合成社区甲"] * 7 + ["合成社区乙"] * 2,
            "community_id": ["C-SYN-001"] * 7 + ["C-SYN-002"] * 2,
            "layout": ["2室1厅"] * 6 + ["3室1厅"] + ["1室1厅", "2室1厅"],
            "area_sqm": [80.0, 85.0, 85.0, 88.0, 82.0, 84.0, 83.0, 70.0, 75.0],
            "total_price_yuan": [
                960000, 935000, 1003000, 792000, 951200, 982800, 983550, 1050000, 1162500,
            ],
            "original_price_text": [
                "96万", "94万", "100万", "79万", "95万", "98万", "98万", "105万", "116万",
            ],
            "unit_price": [12000, 11000, 11800, 9000, 11600, 11700, 11850, 15000, 15500],
            "unit_price_observed": [12000, 11000, 11800, 9000, 11600, 11700, 11850, 15000, 15500],
            "unit_price_formula": ["total_price_yuan / area_sqm, rounded to integer"] * 9,
            "orientation": ["南", "南", "南", "北", "南", "南", "南", "南", "南"],
            "listing_price_yuan": [
                1010000, 985000, 1053000, 842000, 1001000, 1033000, 1034000, 1100000, 1212500,
            ],
            "listing_period_days": [30, 45, 20, 60, 35, 25, 40, 25, 35],
            "anomaly_flag": ["正常"] * 9,
            "verification_status": ["已核验"] * 9,
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


def write_lake(root: Path) -> None:
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


def write_scope_policy(root: Path, decisions: dict[str, str]) -> None:
    """写 ScopePolicy 表（community_id → scope_decision；缺省列为默认值）。"""
    rows = [
        {
            "community_id": cid,
            "standard_name": f"小区{cid}",
            "block": "板块",
            "boundary_status": "机器确认",
            "support_level": "可支撑" if decision == "纳入" else "有条件支撑",
            "property_type": "普通住宅",
            "scope_decision": decision,
            "reason": "合成测试",
            "rule_version": ACTIVE_SCOPE_POLICY_VERSION,
            "source_ref": "synth:policy",
        }
        for cid, decision in decisions.items()
    ]
    path = root / entities_community.ENTITIES_LAYER / scope_policy_filename(
        ACTIVE_SCOPE_POLICY_VERSION
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=scope_policy_schema()), path)


def subject_in_scope() -> SubjectProperty:
    return SubjectProperty(
        subject_id="SUBJ-REL-001",
        community_id="C-SYN-001",
        area_sqm=Decimal("80"),
        layout="2室1厅",
        valuation_date=date(2026, 3, 20),
    )


def subject_out_of_scope() -> SubjectProperty:
    return SubjectProperty(
        subject_id="SUBJ-REL-002",
        community_id="C-SYN-002",
        area_sqm=Decimal("80"),
        layout="2室1厅",
        valuation_date=date(2026, 3, 20),
    )


def _status(outcome: EstimateOutcome) -> str | None:
    return outcome.result.status.value if outcome.result is not None else None


def test_default_release_never_formal(tmp_path: Path) -> None:
    """反例（验收③）：G5 未通过（formal_release_enabled=False 默认）→ 绝不输出 formal。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})
    outcome = run_estimate(subject=subject_in_scope(), data_dir=lake, out_root=tmp_path / "out")
    assert _status(outcome) in ("参考", "候选")
    assert _status(outcome) != "正式"


def test_formal_enabled_and_in_scope(tmp_path: Path) -> None:
    """G5 通过且小区在纳入范围 → 输出正式。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})
    outcome = run_estimate(
        subject=subject_in_scope(), data_dir=lake, out_root=tmp_path / "out",
        formal_release_enabled=True,
    )
    result = outcome.result
    assert result is not None
    assert result.status.value == "正式"
    assert "正式发布门槛" in result.reason


def test_formal_enabled_but_out_of_scope(tmp_path: Path) -> None:
    """反例：发布开关开启但目标在适用范围外 → 仍不输出 formal（范围双闸）。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})  # C-SYN-002 不在纳入名单
    outcome = run_estimate(
        subject=subject_out_of_scope(), data_dir=lake, out_root=tmp_path / "out",
        formal_release_enabled=True,
    )
    # 核心反例：范围外绝不输出 formal（候选=数据不足、参考=范围外，均非正式）
    assert _status(outcome) != "正式"


def test_formal_enabled_but_scope_policy_missing(tmp_path: Path) -> None:
    """反例：范围表缺失 → 保守不输出 formal（范围未固定不发布）。"""
    lake = tmp_path / "lake"
    write_lake(lake)  # 不写 scope_policy
    outcome = run_estimate(
        subject=subject_in_scope(), data_dir=lake, out_root=tmp_path / "out",
        formal_release_enabled=True,
    )
    assert _status(outcome) != "正式"
    assert _status(outcome) == "参考"


def test_default_estimate_still_reference_without_scope(tmp_path: Path) -> None:
    """回归：无范围表 + 默认关闭 → 正常输出参考（不影响现有行为）。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    outcome = run_estimate(subject=subject_in_scope(), data_dir=lake, out_root=tmp_path / "out")
    assert _status(outcome) in ("参考", "候选")


# ---------------------------------------------------------------------------
# CX-WP9-02：受控 formal 启用路径（发布决定记录 + 官方 CLI estimate）
# ---------------------------------------------------------------------------

_VALID_RELEASE_DECISION: dict[str, Any] = {
    "decision_id": "RELEASE1-001",
    "released": True,
    "decided_at": "2026-08-23",
    "decided_by": "用户",
    "gate_evidence": "04-校验/G5-发布门禁证据-V0.1.md",
}


def write_release_decision(
    root: Path, payload: dict[str, Any] | None = None, raw_text: str | None = None
) -> Path:
    """写发布决定记录；缺省为完整已发布记录，自定义 payload/raw_text 构造反例。"""
    path = release_decision_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if raw_text is not None:
        path.write_text(raw_text, encoding="utf-8")
        return path
    path.write_text(
        json.dumps(payload if payload is not None else _VALID_RELEASE_DECISION,
                   ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def _cli_estimate_envelope(
    lake: Path, subject: SubjectProperty, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> dict[str, Any]:
    """经官方 CLI 入口跑一次 estimate，返回包络 dict（退出码必须为 0）。"""
    subject_file = tmp_path / f"{subject.subject_id}.json"
    subject_file.write_text(subject.model_dump_json(), encoding="utf-8")
    code = cli.main(
        [
            "estimate",
            "--subject",
            str(subject_file),
            "--data-dir",
            str(lake),
            "--out-dir",
            str(tmp_path / "out"),
        ]
    )
    assert code == 0
    envelope: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert envelope["command_status"] == "success"
    return envelope


def test_load_release_decision_missing_file(tmp_path: Path) -> None:
    """缺失配置：无记录文件 → recorded=False、enabled=False（保持候选/参考）。"""
    decision = load_release_decision(tmp_path)
    assert decision.recorded is False
    assert decision.enabled is False


def test_load_release_decision_valid(tmp_path: Path) -> None:
    """正常：完整已发布记录 → enabled=True 且 detail 含决定标识。"""
    write_release_decision(tmp_path)
    decision = load_release_decision(tmp_path)
    assert decision.recorded is True
    assert decision.enabled is True
    assert "RELEASE1-001" in decision.detail


@pytest.mark.parametrize(
    ("payload", "raw_text", "fragment"),
    [
        (dict(_VALID_RELEASE_DECISION, released=False), None, "released=False"),
        ({k: v for k, v in _VALID_RELEASE_DECISION.items() if k != "decided_by"},
         None, "decided_by 缺失或类型不符"),
        (dict(_VALID_RELEASE_DECISION, decided_at="not-a-date"), None, "非 ISO 日期"),
        (dict(_VALID_RELEASE_DECISION, gate_evidence="   "), None, "为空白"),
        (None, "{not valid json", "不可解析"),
        (None, "[1, 2, 3]", "必须为 JSON 对象"),
    ],
)
def test_load_release_decision_unauthorized(
    tmp_path: Path,
    payload: dict[str, Any] | None,
    raw_text: str | None,
    fragment: str,
) -> None:
    """未授权反例：记录存在但未发布/缺字段/坏 JSON/非对象 → enabled=False。"""
    write_release_decision(tmp_path, payload=payload, raw_text=raw_text)
    decision = load_release_decision(tmp_path)
    assert decision.recorded is True
    assert decision.enabled is False
    assert fragment in decision.detail


def test_cli_estimate_release_enabled_and_in_scope(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """正常：有效已发布记录 + 纳入范围小区 → 官方 CLI 输出正式（受控路径）。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})
    write_release_decision(lake)
    envelope = _cli_estimate_envelope(lake, subject_in_scope(), tmp_path, capsys)
    assert envelope["business_status"] == "正式"
    assert envelope["result"]["status"] == "正式"
    assert any("formal 输出已按发布决定记录启用" in w for w in envelope["warnings"])


def test_cli_estimate_release_enabled_but_out_of_scope(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """范围外反例：记录生效但目标小区不在纳入名单（参考级）→ 不输出正式。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "参考"})  # 数据充分但范围外（参考级）
    write_release_decision(lake)
    envelope = _cli_estimate_envelope(lake, subject_in_scope(), tmp_path, capsys)
    assert envelope["business_status"] != "正式"
    assert envelope["business_status"] == "参考"


def test_cli_estimate_release_missing_config(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """缺失配置反例：无发布决定记录 → 官方 CLI 保持候选/参考，不输出正式。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})
    # 不写 release/release_decision.json
    envelope = _cli_estimate_envelope(lake, subject_in_scope(), tmp_path, capsys)
    assert envelope["business_status"] in ("参考", "候选")
    assert envelope["business_status"] != "正式"


@pytest.mark.parametrize(
    ("payload", "raw_text"),
    [
        ({"released": False}, None),
        ({k: v for k, v in _VALID_RELEASE_DECISION.items() if k != "decided_by"}, None),
        (None, "{not valid json"),
    ],
)
def test_cli_estimate_release_unauthorized(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    payload: dict[str, Any] | None,
    raw_text: str | None,
) -> None:
    """未授权反例：记录存在但未发布/缺字段/坏 JSON → 不输出正式且包络告警。"""
    lake = tmp_path / "lake"
    write_lake(lake)
    write_scope_policy(lake, {"C-SYN-001": "纳入"})
    write_release_decision(lake, payload=payload, raw_text=raw_text)
    envelope = _cli_estimate_envelope(lake, subject_in_scope(), tmp_path, capsys)
    assert envelope["business_status"] in ("参考", "候选")
    assert envelope["business_status"] != "正式"
    assert any("保持候选/参考" in w for w in envelope["warnings"])
