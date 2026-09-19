"""交换实现: 外部待估值清单解析构造、失败留痕与批次估值编排（design D3/D4）。

解析构造层：清单行（快照列）→ 面积检查（空串/非数值/非正数 →「信息不足」）
→ ``resolve_community_id`` → 结局映射（HIT 构造；域外括注 →「不适用」；
UNMATCHED/BLOCKED →「信息不足」，reason 按 design D3 模板）→
``SubjectProperty``（subject_id=``SUBJ-FC-<house_id>``、valuation_date=批次
日期、layout/floor/orientation/year_built 有值随附；清单挂牌价字段
total_price_yuan/unit_price 不进 subject）。

失败留痕：失败行逐批落 ``<批次目录>/failures.jsonl``（只追加，同 house_id
幂等去重），留痕含 house_id/subject_id/status/valuation_date/结局类别/中文
reason，供回写导出取数，绝不静默丢弃。

批次编排（design D4/D5）：读批次快照 → 逐行解析构造 → 命中行调
``run_estimate``（规则版本显式传参，默认与 ``gzv estimate`` 同源常量
``cli.DEFAULT_ESTIMATE_RULE_VERSION``，不落函数级默认 1.0）→ 失败行落留痕；
幂等：``RUN-SUBJ-FC-<house_id>-<date>-*`` 冻结目录已存在即跳过且不改写。
"""

from __future__ import annotations

import csv
import glob as _glob
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gz_property_valuation.contract.models import SubjectProperty
from gz_property_valuation.entities.backfill import (
    BackfillOutcome,
    CommunityIdLookup,
    load_community_lookup,
    outside_region_bracket,
    resolve_community_id,
)
from gz_property_valuation.ingest.valuation_queue import (
    VALUATION_QUEUE_DATASET,
    snapshot_columns,
)
from gz_property_valuation.reporting.envelope import (
    InvalidInputError,
    OutputEnvelope,
)
from gz_property_valuation.valuation import estimate as valuation_estimate
from gz_property_valuation.valuation.estimate import (
    ESTIMATE_FILENAME,
    run_estimate,
)

#: 清单房源 subject 命名空间（接口约定 V1 §2.4，与 SUBJ-USER-* 隔离）。
QUEUE_SUBJECT_PREFIX = "SUBJ-FC-"

#: 批次失败行留痕文件名（批次目录内，只追加）。
FAILURES_FILENAME = "failures.jsonl"

#: 结局类别（留痕 outcome 字段；HIT 之外的类别都对应失败行）。
OUTCOME_HIT = "HIT"
OUTCOME_AREA_MISSING = "AREA_MISSING"
OUTCOME_EXCLUDED_OUT_OF_REGION = "EXCLUDED_OUT_OF_REGION"
OUTCOME_UNMATCHED = "UNMATCHED"
OUTCOME_BLOCKED = "BLOCKED"

_TOTAL_FLOORS_PATTERN = re.compile(r"(\d+)\s*层\s*$")


@dataclass(frozen=True)
class ConstructionOutcome:
    """一次清单行解析构造的结果（命中行带 subject，失败行带 status/reason）。"""

    house_id: str
    subject_id: str
    subject: SubjectProperty | None  # 失败行为 None（不构造估值请求）
    status: str | None  # 失败行状态：「信息不足」/「不适用」；命中行为 None
    outcome: str  # 结局类别（OUTCOME_* 常量）
    reason: str  # 中文 reason；命中行为空串


def batch_dir(reports_root: Path, valuation_date: date) -> Path:
    """批次产物目录：``<reports_root>/valuation-queue=<YYYYMMDD>/``。"""
    return reports_root / f"valuation-queue={valuation_date:%Y%m%d}"


def _parse_area(text: str) -> Decimal | None:
    """面积口径（design D3）：空串/非数值/非正数 → None（禁 0 代替）。"""
    stripped = (text or "").strip()
    if not stripped:
        return None
    try:
        value = Decimal(stripped)
    except InvalidOperation:
        return None
    return value if value > 0 else None


def _parse_year(text: str) -> int | None:
    """建成年份：数值字符串 → int；空串/非数值 → None（未知不用 0）。"""
    stripped = (text or "").strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        return None


def _parse_total_floors(floor_text: str) -> int | None:
    """楼层信息「中楼层/18层」→ 总层数 18；解析不出 → None（不臆测）。"""
    stripped = (floor_text or "").strip()
    match = _TOTAL_FLOORS_PATTERN.search(stripped)
    return int(match.group(1)) if match else None


def construct_subject(
    row: dict[str, str],
    *,
    valuation_date: date,
    lookup: CommunityIdLookup,
) -> ConstructionOutcome:
    """一行清单快照 → 解析结局映射 + subject 构造（design D3 状态映射）。"""
    house_id = (row.get("source_record_id") or "").strip()
    subject_id = f"{QUEUE_SUBJECT_PREFIX}{house_id}"
    community = (row.get("community_name") or "").strip()

    area = _parse_area(row.get("area_sqm") or "")
    if area is None:
        return ConstructionOutcome(
            house_id=house_id,
            subject_id=subject_id,
            subject=None,
            status="信息不足",
            outcome=OUTCOME_AREA_MISSING,
            reason="面积缺失（空串或非正数），无法构造估值请求，信息不足",
        )

    community_id, sub_area, outcome, _reason = resolve_community_id(community, lookup)
    if outcome is BackfillOutcome.EXCLUDED_OUT_OF_REGION:
        bracket = outside_region_bracket(community) or ""
        return ConstructionOutcome(
            house_id=house_id,
            subject_id=subject_id,
            subject=None,
            status="不适用",
            outcome=OUTCOME_EXCLUDED_OUT_OF_REGION,
            reason=f"小区名括注「{bracket}」为非本估值行政区，域外不适用",
        )
    if outcome is BackfillOutcome.BLOCKED:
        return ConstructionOutcome(
            house_id=house_id,
            subject_id=subject_id,
            subject=None,
            status="信息不足",
            outcome=OUTCOME_BLOCKED,
            reason=f"小区「{community}」别名处于屏蔽/待定状态，需人工确认",
        )
    if community_id is None:
        return ConstructionOutcome(
            house_id=house_id,
            subject_id=subject_id,
            subject=None,
            status="信息不足",
            outcome=OUTCOME_UNMATCHED,
            reason=f"小区「{community}」在权威表与别名库均未命中，信息不足",
        )

    subject = SubjectProperty(
        subject_id=subject_id,
        community_id=community_id,
        sub_area=sub_area or None,
        area_sqm=area,
        layout=(row.get("layout") or "").strip() or "UNKNOWN",
        valuation_date=valuation_date,
        orientation=(row.get("orientation") or "").strip() or "UNKNOWN",
        year_built=_parse_year(row.get("year_built") or ""),
        total_floors=_parse_total_floors(row.get("floor") or ""),
    )
    return ConstructionOutcome(
        house_id=house_id,
        subject_id=subject_id,
        subject=subject,
        status=None,
        outcome=OUTCOME_HIT,
        reason="",
    )


def load_recorded_house_ids(path: Path) -> set[str]:
    """读取既有留痕文件的 house_id 集合（文件缺失/空 → 空集合）。"""
    if not path.is_file():
        return set()
    ids: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        record = json.loads(stripped)
        house_id = record.get("house_id")
        if house_id is not None:
            ids.add(str(house_id))
    return ids


def append_failure_record(path: Path, record: dict[str, str]) -> bool:
    """失败行留痕（只追加；同 house_id 已留痕则跳过，幂等）。返回是否新写入。

    record 必含 ``house_id``；导出取数还需要 ``subject_id``/``status``/
    ``valuation_date``/``outcome``/``reason``（任务 2.2 验收字段）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if record["house_id"] in load_recorded_house_ids(path):
        return False
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return True


# ---------------------------------------------------------------------------
# 批次估值编排（design D4/D5）
# ---------------------------------------------------------------------------


def batch_snapshot_path(data_dir: Path, valuation_date: date) -> Path:
    """批次快照路径（design D1 落盘布局，天级 fetched_at）。"""
    return (
        data_dir
        / "raw"
        / "source=fangcollect"
        / f"dataset={VALUATION_QUEUE_DATASET}"
        / f"fetched_at={valuation_date:%Y%m%d}"
        / "data.parquet"
    )


def load_batch_rows(data_dir: Path, valuation_date: date) -> pa.Table:
    """读批次快照；缺失 → ``InvalidInputError``（先 ingest file 摄入）。"""
    path = batch_snapshot_path(data_dir, valuation_date)
    if not path.is_file():
        raise InvalidInputError(
            f"批次快照缺失：{path}（先 gzv ingest file 摄入待估值清单）"
        )
    return pq.read_table(path)


def frozen_estimate_dir(reports_root: Path, run_prefix: str) -> Path | None:
    """按 ``valuation_id=<run_prefix>*`` 查找已冻结估值目录（幂等依据）。"""
    if not reports_root.is_dir():
        return None
    pattern = f"valuation_id={_glob.escape(run_prefix)}*"
    matches = sorted(path for path in reports_root.glob(pattern) if path.is_dir())
    return matches[-1] if matches else None


def run_queue_estimate(
    *,
    data_dir: Path,
    valuation_date: date,
    rule_version: str,
    formal_release_enabled: bool,
    out_root: Path | None = None,
) -> OutputEnvelope:
    """批次估值编排：解析构造 → 逐户估值 → 失败留痕（design D4）。

    幂等：``RUN-SUBJ-FC-<house_id>-<date>-*`` 冻结目录已存在的房源跳过且
    不改写（重跑零新增估值）。命中行调 ``run_estimate``（``--as-of`` =
    subject.valuation_date = 批次日期，规则版本显式传参）；失败行按 D3 映射
    落 ``failures.jsonl``（只追加、同 house_id 幂等）。返回批次统计包络。
    """
    rows = load_batch_rows(data_dir, valuation_date)
    lookup = load_community_lookup(data_dir=data_dir)
    reports_root = (
        out_root if out_root is not None else valuation_estimate.DEFAULT_REPORTS_ROOT
    )
    failures_path = batch_dir(reports_root, valuation_date) / FAILURES_FILENAME
    columns = snapshot_columns()
    date_compact = f"{valuation_date:%Y%m%d}"

    total = rows.num_rows
    estimated = 0
    skipped = 0
    failed = 0
    for index in range(total):
        row = {
            name: rows.column(name)[index].as_py() or "" for name in columns
        }
        house_id = (row.get("source_record_id") or "").strip()
        prefix = f"RUN-{QUEUE_SUBJECT_PREFIX}{house_id}-{date_compact}-"
        if frozen_estimate_dir(reports_root, prefix) is not None:
            skipped += 1
            continue
        construction = construct_subject(
            row, valuation_date=valuation_date, lookup=lookup
        )
        if construction.subject is None:
            failed += 1
            append_failure_record(
                failures_path,
                {
                    "house_id": construction.house_id,
                    "subject_id": construction.subject_id,
                    "status": construction.status or "",
                    "valuation_date": valuation_date.isoformat(),
                    "outcome": construction.outcome,
                    "reason": construction.reason,
                },
            )
            continue
        run_estimate(
            subject=construction.subject,
            as_of=valuation_date,
            data_dir=data_dir,
            out_root=out_root,
            rule_version=rule_version,
            formal_release_enabled=formal_release_enabled,
        )
        estimated += 1

    return OutputEnvelope(
        command="valuation-queue estimate",
        business_status=None,
        result={
            "fetched_at": date_compact,
            "rows_total": total,
            "estimated": estimated,
            "skipped_frozen": skipped,
            "failed": failed,
            "failures_file": str(failures_path) if failures_path.is_file() else None,
        },
    )


# ---------------------------------------------------------------------------
# 回写导出（design D4 / 契约 §3）
# ---------------------------------------------------------------------------

#: 回写 12 列（契约 §3.2 顺序）。
WRITEBACK_COLUMNS: tuple[str, ...] = (
    "house_id",
    "subject_id",
    "status",
    "unit_price_center",
    "unit_price_low",
    "unit_price_high",
    "confidence",
    "n_comps",
    "rule_version",
    "valuation_date",
    "valuation_id",
    "reason",
)

#: 回写文件名前缀（``估值回写_v1_<YYYYMMDD>.csv``，同批次 _2/_3 递增）。
WRITEBACK_FILENAME_PREFIX = "估值回写_v1_"

def _num_text(value: object) -> str:
    """estimate.json 数值 → CSV 单元格原文（None → 空串，禁 0 充数）。"""
    return "" if value is None else str(value)


def _writeback_row_placeholder() -> dict[str, str]:
    return dict.fromkeys(WRITEBACK_COLUMNS, "")


def _success_row(
    house_id: str, estimate_path: Path, valuation_date_iso: str
) -> dict[str, str]:
    """冻结 estimate.json 原文 → 12 列成功行（回写 = 引擎输出原文）。"""
    payload = json.loads(estimate_path.read_text(encoding="utf-8"))
    result = payload.get("result") or {}
    row = _writeback_row_placeholder()
    subject_id = str(result.get("subject_id") or f"{QUEUE_SUBJECT_PREFIX}{house_id}")
    if result:
        range_values = result.get("range") or [None, None]
        row.update(
            {
                "house_id": house_id,
                "subject_id": subject_id,
                "status": str(result.get("status") or payload.get("business_status") or ""),
                "unit_price_center": _num_text(result.get("center")),
                "unit_price_low": _num_text(range_values[0]),
                "unit_price_high": _num_text(range_values[1]),
                "confidence": _num_text(result.get("confidence")),
                "n_comps": _num_text(result.get("n_comps")),
                "rule_version": _num_text(result.get("rule_version")),
                "valuation_date": _num_text(result.get("valuation_date")) or valuation_date_iso,
                "valuation_id": _num_text(payload.get("run_id") or result.get("run_id")),
                "reason": _num_text(result.get("reason")),
            }
        )
    else:
        # 业务降级（信息不足，§10.3）：衍生列空串禁 0；reason 取包络 warnings 原文
        row.update(
            {
                "house_id": house_id,
                "subject_id": subject_id,
                "status": str(payload.get("business_status") or "信息不足"),
                "valuation_date": valuation_date_iso,
                "valuation_id": _num_text(payload.get("run_id")),
                "reason": "；".join(str(w) for w in payload.get("warnings") or []),
            }
        )
    return row


def _failure_row(record: dict[str, str]) -> dict[str, str]:
    """failures.jsonl 留痕 → 12 列失败行（衍生列空串禁 0）。"""
    row = _writeback_row_placeholder()
    row.update(
        {
            "house_id": str(record.get("house_id") or ""),
            "subject_id": str(record.get("subject_id") or ""),
            "status": str(record.get("status") or ""),
            "valuation_date": str(record.get("valuation_date") or ""),
            "reason": str(record.get("reason") or ""),
        }
    )
    return row


def _failure_records(failures_path: Path) -> dict[str, dict[str, str]]:
    """读 failures.jsonl → house_id → 留痕记录。"""
    records: dict[str, dict[str, str]] = {}
    if not failures_path.is_file():
        return records
    for line in failures_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        record = json.loads(stripped)
        records[str(record.get("house_id"))] = record
    return records


def _next_writeback_path(directory: Path, valuation_date: date) -> Path:
    """同批次第 N 次导出路径：无后缀 → ``_2`` → ``_3`` 递增（只增不覆盖）。"""
    base = WRITEBACK_FILENAME_PREFIX + f"{valuation_date:%Y%m%d}"
    candidate = directory / f"{base}.csv"
    suffix = 1
    while candidate.exists():
        suffix += 1
        candidate = directory / f"{base}_{suffix}.csv"
    return candidate


def export_writeback(
    *,
    data_dir: Path,
    valuation_date: date,
    out_root: Path | None = None,
) -> OutputEnvelope:
    """按批次导出 12 列回写 CSV（契约 §3，design D4）。

    成功行取数于冻结 estimate.json 原文、失败行取数于 failures.jsonl，
    不为导出重新估值；清单行总数 = 回写行总数。同批次重复导出落
    ``_2``/``_3`` 递增新文件，既有导出 MUST NOT 被覆盖。
    """
    rows = load_batch_rows(data_dir, valuation_date)
    reports_root = (
        out_root if out_root is not None else valuation_estimate.DEFAULT_REPORTS_ROOT
    )
    failures = _failure_records(
        batch_dir(reports_root, valuation_date) / FAILURES_FILENAME
    )
    valuation_date_iso = valuation_date.isoformat()
    data_rows: list[dict[str, str]] = []
    columns = snapshot_columns()
    for index in range(rows.num_rows):
        row = {name: rows.column(name)[index].as_py() or "" for name in columns}
        house_id = (row.get("source_record_id") or "").strip()
        prefix = f"RUN-{QUEUE_SUBJECT_PREFIX}{house_id}-{valuation_date:%Y%m%d}-"
        frozen = frozen_estimate_dir(reports_root, prefix)
        if frozen is not None:
            data_rows.append(
                _success_row(house_id, frozen / ESTIMATE_FILENAME, valuation_date_iso)
            )
        elif house_id in failures:
            data_rows.append(_failure_row(failures[house_id]))
        else:
            raise InvalidInputError(
                f"房源 {house_id} 既无冻结估值也无失败留痕（先跑 "
                "gzv valuation-queue estimate）"
            )

    target = _next_writeback_path(
        batch_dir(reports_root, valuation_date), valuation_date
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    declaration = (
        "# format=v1-writeback "
        f"exported_at={datetime.now(UTC).isoformat(timespec='seconds')} "
        f"rows={len(data_rows)}"
    )
    with target.open("w", encoding="utf-8-sig", newline="") as handle:
        handle.write(declaration + "\n")
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(list(WRITEBACK_COLUMNS))
        for data_row in data_rows:
            writer.writerow([data_row[name] for name in WRITEBACK_COLUMNS])

    return OutputEnvelope(
        command="valuation-queue export",
        business_status=None,
        result={
            "fetched_at": f"{valuation_date:%Y%m%d}",
            "rows": len(data_rows),
            "writeback_file": str(target),
        },
    )
