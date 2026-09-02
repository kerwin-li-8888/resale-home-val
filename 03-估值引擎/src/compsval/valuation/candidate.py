"""WP6-A 候选案例池（VAL1-002）：固定时点/截点检索可比候选。

技术方案 §9.2 步骤 1-6：校验目标房源输入 → 固定估值时点/数据截点 → 只检索
数据截点之前（含当日）的成交 → 排除非住宅/车位、明显异常、缺少必要字段、
community_id 未匹配记录。**不做**层级/相似度/修正（归 WP6-B/C/D）。

设计要点（对应 WP6-A 验收标准）：

- **无未来泄漏（验收①）**：``data_cutoff`` 固定，只处理 ``sale_date <=
  data_cutoff`` 的成交；截点之后数据不进入候选（反例测试）；
- **全量留痕（验收②）**：对每一条进入检索范围的 valid_sale 成交都写一行
  ``comp_candidate``，``selected`` 区分入选/排除，排除行 reason 说明原因、
  ``candidate_id``/``raw_locator`` 可逐条溯源到成交事件；
- **不静默纳入（验收③）**：非住宅/车位、明显异常、缺必要字段（面积/总价/
  单价/成交日期）、community_id 未匹配一律排除并记录理由，绝不静默纳入；
- **模型一致（验收④）**：comp_candidate 字段与数据字典 §3.11 一致；WP6-A
  未分层 ``tier=None``（数值未知用 None 不用 0），由 WP6-B 填充；
- **目录注册（验收⑤）**：估值中间结果写 ``data/valuation/``（``val_`` 视图），
  经 ``compsval catalog`` 可列。

本模块只产出一条中间结果表，不改写 raw/staged/marts/entities 既有表。
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.contract.models import (
    CompCandidate,
    SubjectProperty,
    ValuationRun,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    read_derived_manifest,
    write_derived_manifest,
)
from compsval.ingest.stage import (
    MARTS_LAYER,
    VALID_SALE_FILENAME,
    VALID_SALE_TABLE,
)

#: 估值中间结果层（新层，catalog 注册 ``val_`` 前缀视图）。
VALUATION_LAYER = "valuation"

SUBJECT_TABLE = "subject_property"
VALUATION_RUN_TABLE = "valuation_run"
COMP_CANDIDATE_TABLE = "comp_candidate"
SUBJECT_FILENAME = f"{SUBJECT_TABLE}.parquet"
VALUATION_RUN_FILENAME = f"{VALUATION_RUN_TABLE}.parquet"
COMP_CANDIDATE_FILENAME = f"{COMP_CANDIDATE_TABLE}.parquet"

#: 当前规则版本（同 scope 纪律：规则改动 → 新版本输出，不覆盖旧结果）。
DEFAULT_RULE_VERSION = "1.0"

#: 排除理由（逐条可溯源，验收②/③）。
REASON_AFTER_CUTOFF = "成交日期晚于数据截点（未来数据），不得进入候选池"
REASON_UNMATCHED_COMMUNITY = (
    "community_id 未匹配（未回填标准小区ID），无法关联小区，排除"
)
REASON_NON_RESIDENTIAL = "非住宅/车位记录，不进入可比候选"
REASON_ABNORMAL = "明显异常记录（清洗阶段标记非正常），排除"
REASON_MISSING_FIELDS = "缺少必要字段（面积/总价/单价/成交日期），排除"
REASON_SELECTED = "纳入候选池：截点前成交、已匹配小区、字段完整、非异常"

#: 非住宅/车位判定口径（与清洗阶段一致：layout == "车位"）。
_NON_RESIDENTIAL_LAYOUT = "车位"

#: valid_sale 表必读列（缺失任一列 → 该列置未知，走缺字段排除）。
_REQUIRED_COLUMNS = (
    "sale_event_id",
    "community_id",
    "sale_date",
    "layout",
    "area_sqm",
    "total_price_yuan",
    "unit_price",
    "anomaly_flag",
)


@dataclass(frozen=True)
class CandidateRetriever:
    """候选案例池检索规则集（VAL1-002），带 rule_version（验收④）。

    同一版本下同一 valid_sale + subject 输入产出同一候选池；规则改动以
    新版本落地。检索只按时间/完整性/匹配性筛选，不做层级与相似度。
    """

    rule_version: str = DEFAULT_RULE_VERSION

    def retrieve(
        self,
        valid_sale: pa.Table,
        subject: SubjectProperty,
        *,
        data_cutoff: date | None = None,
    ) -> list[CompCandidate]:
        """从 valid_sale 检索截点之前的成交 → 全量留痕候选清单。

        ``data_cutoff`` 默认取 ``subject.valuation_date``（§9.2 步骤 4：固定
        时点/截点）；只处理 ``sale_date <= data_cutoff`` 的成交，之后数据不
        进入候选（验收①）。逐条判定入选/排除并写 reason（验收②/③）。
        """
        cutoff = data_cutoff if data_cutoff is not None else subject.valuation_date
        run_id = run_id_of(subject, cutoff, self.rule_version)
        missing = [c for c in _REQUIRED_COLUMNS if c not in valid_sale.column_names]
        if missing:
            raise ValueError(f"valid_sale 缺少必要列: {', '.join(missing)}")

        candidates: list[CompCandidate] = []
        for row in zip(
            *[valid_sale.column(c).to_pylist() for c in _REQUIRED_COLUMNS],
            strict=True,
        ):
            sale_event_id = _text(row[0])
            community_id = _text(row[1])
            sale_date: date | None = row[2]
            layout = _text(row[3])
            area_sqm = row[4]
            total_price = row[5]
            unit_price = row[6]
            anomaly_flag = _text(row[7])

            selected, reason = self._judge(
                sale_date=sale_date,
                community_id=community_id,
                layout=layout,
                area_sqm=area_sqm,
                total_price=total_price,
                unit_price=unit_price,
                anomaly_flag=anomaly_flag,
                data_cutoff=cutoff,
            )
            candidates.append(
                CompCandidate(
                    candidate_id=f"{run_id}-{sale_event_id}",
                    run_id=run_id,
                    sale_event_id=sale_event_id,
                    community_id=community_id if community_id else "UNKNOWN",
                    selected=selected,
                    tier=None,
                    similarity=None,
                    reason=reason,
                )
            )
        return candidates

    def _judge(
        self,
        *,
        sale_date: date | None,
        community_id: str,
        layout: str,
        area_sqm: object,
        total_price: object,
        unit_price: object,
        anomaly_flag: str,
        data_cutoff: date,
    ) -> tuple[bool, str]:
        """逐条判定（顺序固定，首条命中即定理由，验收②可溯源）。"""
        if sale_date is None:
            return False, REASON_MISSING_FIELDS
        if sale_date > data_cutoff:
            return False, REASON_AFTER_CUTOFF
        if not community_id or community_id == "UNKNOWN":
            return False, REASON_UNMATCHED_COMMUNITY
        if layout == _NON_RESIDENTIAL_LAYOUT:
            return False, REASON_NON_RESIDENTIAL
        if anomaly_flag != "正常":
            return False, REASON_ABNORMAL
        if area_sqm is None or total_price is None or unit_price is None:
            return False, REASON_MISSING_FIELDS
        return True, REASON_SELECTED


def _text(value: object) -> str:
    """pyarrow 标量 → str；None 保持空串（用于判定 community/layout）。"""
    if value is None:
        return ""
    text = str(value).strip()
    return "UNKNOWN" if text == "" else text


def run_id_of(subject: SubjectProperty, data_cutoff: date, rule_version: str) -> str:
    """稳定运行 ID：同一 目标房源+时点+截点+规则版本 产出同一 run（可复现）。"""
    return (
        f"RUN-{subject.subject_id}-{subject.valuation_date:%Y%m%d}"
        f"-{data_cutoff:%Y%m%d}-v{rule_version}"
    )


# ---------------------------------------------------------------------------
# 表模式与构造
# ---------------------------------------------------------------------------


def comp_candidate_schema() -> pa.Schema:
    """``comp_candidate`` 中间结果 PyArrow 模式（模型字段 + 溯源 + 版本）。"""
    return pa.schema(
        [
            pa.field("candidate_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("sale_event_id", pa.string(), nullable=False),
            pa.field("community_id", pa.string(), nullable=False),
            pa.field("sale_date", pa.date32(), nullable=True),
            pa.field("raw_locator", pa.string(), nullable=True),
            pa.field("selected", pa.bool_(), nullable=False),
            pa.field("tier", pa.int32(), nullable=True),
            pa.field("similarity", pa.float64(), nullable=True),
            pa.field("reason", pa.string(), nullable=False),
            pa.field("rule_version", pa.string(), nullable=False),
        ]
    )


def comp_candidate_table(
    candidates: Sequence[CompCandidate],
    valid_sale: pa.Table,
    *,
    rule_version: str,
) -> pa.Table:
    """候选清单 → 中间结果表（携带 sale_date/raw_locator 溯源列）。"""
    rows: dict[str, list[object]] = {name: [] for name in comp_candidate_schema().names}
    by_event = {
        valid_sale.column("sale_event_id")[i].as_py(): i
        for i in range(valid_sale.num_rows)
    }
    for candidate in candidates:
        idx = by_event[candidate.sale_event_id]
        rows["candidate_id"].append(candidate.candidate_id)
        rows["run_id"].append(candidate.run_id)
        rows["sale_event_id"].append(candidate.sale_event_id)
        rows["community_id"].append(candidate.community_id)
        rows["sale_date"].append(valid_sale.column("sale_date")[idx].as_py())
        rows["raw_locator"].append(valid_sale.column("raw_locator")[idx].as_py())
        rows["selected"].append(candidate.selected)
        rows["tier"].append(candidate.tier)
        rows["similarity"].append(
            float(candidate.similarity) if candidate.similarity is not None else None
        )
        rows["reason"].append(candidate.reason)
        rows["rule_version"].append(rule_version)
    return pa.table(rows, schema=comp_candidate_schema())


def subject_property_table(subject: SubjectProperty) -> pa.Table:
    """目标房源快照 → 单行中间结果表（§3.9 字段）。"""
    return pa.table(
        {
            "subject_id": [subject.subject_id],
            "community_id": [subject.community_id],
            "area_sqm": [float(subject.area_sqm)],
            "layout": [subject.layout],
            "valuation_date": [subject.valuation_date],
            "building_name": [subject.building_name],
            "floor": [subject.floor],
            "total_floors": [subject.total_floors],
            "has_elevator": [subject.has_elevator],
            "orientation": [subject.orientation],
            "year_built": [subject.year_built],
            "site_observations": [subject.site_observations],
        }
    )


def valuation_run_table(run: ValuationRun) -> pa.Table:
    """一次运行总清单 → 单行中间结果表（§3.10 字段）。"""
    return pa.table(
        {
            "run_id": [run.run_id],
            "subject_id": [run.subject_id],
            "valuation_date": [run.valuation_date],
            "data_cutoff": [run.data_cutoff],
            "data_version": [run.data_version],
            "rule_version": [run.rule_version],
            "code_version": [run.code_version],
            "parameters": [run.parameters],
            "run_at": [run.run_at],
        }
    )


# ---------------------------------------------------------------------------
# 写盘（原子写入 + DerivedManifest，同 scope/entities 纪律）
# ---------------------------------------------------------------------------


def _write_table(table: pa.Table, path: Path, manifest: DerivedManifest) -> Path:
    final = path
    work = final.with_name(final.name + ".incomplete")
    pq.write_table(table, work, compression="zstd")
    write_derived_manifest(manifest, final)
    work.replace(final)
    return final


def write_valuation_tables(
    *,
    data_dir: Path,
    subject: SubjectProperty,
    run: ValuationRun,
    candidates: Sequence[CompCandidate],
    valid_sale: pa.Table,
    inputs: Sequence[InputRef],
    rule_version: str,
    notes: str | None = None,
) -> tuple[Path, Path, Path]:
    """把 subject_property / valuation_run / comp_candidate 原子写入 data/valuation/。

    三个表及其 DerivedManifest 一次写入；同一 run_id 重跑时按派生表语义
    重新生成（不追加），与 staged/marts 再生纪律一致。返回 (subject, run,
    candidate) 三个 parquet 路径。
    """
    valuation_dir = data_dir / VALUATION_LAYER
    valuation_dir.mkdir(parents=True, exist_ok=True)

    subject_path = valuation_dir / SUBJECT_FILENAME
    run_path = valuation_dir / VALUATION_RUN_FILENAME
    candidate_path = valuation_dir / COMP_CANDIDATE_FILENAME

    _write_table(
        subject_property_table(subject),
        subject_path,
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=SUBJECT_TABLE,
            built_at=datetime.now(UTC),
            row_count=1,
            inputs=list(inputs),
            package_version=__version__,
            notes=notes,
        ),
    )
    _write_table(
        valuation_run_table(run),
        run_path,
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=VALUATION_RUN_TABLE,
            built_at=datetime.now(UTC),
            row_count=1,
            inputs=list(inputs),
            package_version=__version__,
            notes=notes,
        ),
    )
    _write_table(
        comp_candidate_table(candidates, valid_sale, rule_version=rule_version),
        candidate_path,
        DerivedManifest(
            layer=VALUATION_LAYER,
            table=COMP_CANDIDATE_TABLE,
            built_at=datetime.now(UTC),
            row_count=len(candidates),
            inputs=list(inputs),
            package_version=__version__,
            notes=notes,
        ),
    )
    return subject_path, run_path, candidate_path


@dataclass(frozen=True)
class ValuationBuildResult:
    """一次 ``compsval valuation build`` 的结果（供 CLI 打印与测试断言）。"""

    subject_path: Path
    run_path: Path
    candidate_path: Path
    run: ValuationRun
    candidates: list[CompCandidate]


def build_valuation(
    subject: SubjectProperty,
    *,
    data_dir: Path,
    rule_version: str = DEFAULT_RULE_VERSION,
    data_cutoff: date | None = None,
    notes: str | None = None,
) -> ValuationBuildResult:
    """WP6-A 主入口：读 valid_sale → 检索候选 → 写三个中间结果表。

    从 ``data/marts/valid_sale.parquet`` 读取正式成交池（缺失抛
    ``FileNotFoundError``）；``data_cutoff`` 默认取 ``subject.valuation_date``。
    返回写盘路径与运行/候选清单（CLI 可据此打印入选/排除统计）。
    """
    valid_sale_path = data_dir / MARTS_LAYER / VALID_SALE_FILENAME
    valid_sale = pq.read_table(valid_sale_path)

    cutoff = data_cutoff if data_cutoff is not None else subject.valuation_date
    run_id = run_id_of(subject, cutoff, rule_version)

    data_version = "UNKNOWN"
    inputs: list[InputRef] = []
    manifest_path = valid_sale_path.with_suffix(".manifest.json")
    if manifest_path.is_file():
        manifest = read_derived_manifest(valid_sale_path)
        inputs = list(manifest.inputs)
        data_version = ";".join(f"{i.dataset}@{i.fetched_at}" for i in manifest.inputs)
    if not data_version:
        data_version = f"{VALID_SALE_TABLE}@UNKNOWN"

    run = ValuationRun(
        run_id=run_id,
        subject_id=subject.subject_id,
        valuation_date=subject.valuation_date,
        data_cutoff=cutoff,
        data_version=data_version,
        rule_version=rule_version,
        code_version=__version__,
        parameters={"data_cutoff": cutoff.isoformat()},
        run_at=datetime.now(UTC),
    )

    retriever = CandidateRetriever(rule_version=rule_version)
    candidates = retriever.retrieve(valid_sale, subject, data_cutoff=cutoff)

    subject_path, run_path, candidate_path = write_valuation_tables(
        data_dir=data_dir,
        subject=subject,
        run=run,
        candidates=candidates,
        valid_sale=valid_sale,
        inputs=inputs,
        rule_version=rule_version,
        notes=notes,
    )
    return ValuationBuildResult(
        subject_path=subject_path,
        run_path=run_path,
        candidate_path=candidate_path,
        run=run,
        candidates=candidates,
    )
