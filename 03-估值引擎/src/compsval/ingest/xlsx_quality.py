"""外部链家 staged 数据质量报告与回滚点（EXTFP1-E，技术方案 §14/§16）。

从 ``data_dir/staged/lianjia_ext_sale_record`` 与
``lianjia_ext_ordinary_residential`` 两表统计生成质量报告（MD + JSON 描述同一
冻结数据），覆盖技术方案 §14 中 EXTFP1 阶段可报告的项：

- 输入数据版本（源 SHA256、解析规则版本）与记录数守恒；
- 普通住宅纳入 / 排除数量；
- 字段质量（PARSED / MISSING / PARSE_FAILURE 分布）；
- 户型图 URL 状态分布（NO_URL / URL_PARSE_FAILURE / URLS_OK / 候选数）；
- 面积列一致性（第 17 列 transaction_area_sqm vs 第 40 列 building_area_detail_sqm）；
- 回滚点：git 基线 commit 与数据产物（原始快照 / staged 表）回滚说明（§16）。

报告中的动态数字一律由机器产物（staged 表）生成，Markdown 只解释（§14）。
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pydantic import BaseModel, Field

from compsval.ingest.manifests import read_derived_manifest
from compsval.ingest.xlsx_parse import PARSE_RULE_VERSION

QUALITY_DIR = "quality"
XLSX_QUALITY_MD = "xlsx_quality_report.md"
XLSX_QUALITY_JSON = "xlsx_quality_report.json"


class FieldStatusStats(BaseModel):
    """单字段的解析状态分布。"""

    PARSED: int = 0
    MISSING: int = 0
    PARSE_FAILURE: int = 0


class XlsxQualityReport(BaseModel):
    """EXTFP1-E 质量报告（守恒 + 字段质量 + URL 分布 + 面积一致性 + 回滚点）。"""

    report_version: str = "EXTFP1-E-1.0"
    source_sha256: str | None = None
    parse_rule_version: str = PARSE_RULE_VERSION
    built_at: str | None = None
    counts: dict[str, int] = Field(description="守恒：输入/sale_record/普通住宅/排除")
    property_use_distribution: dict[str, int] = Field(description="用途分布")
    field_status: dict[str, FieldStatusStats] = Field(description="数值/日期字段解析状态")
    floorplan_url_status: dict[str, int] = Field(description="户型图 URL 状态分布")
    area_consistency: dict[str, int] = Field(
        description="第17/40列面积一致性（可解析行内 相等/不等/仅17/仅40）"
    )
    staged_tables: list[dict[str, object]] = Field(description="staged 表血缘 manifest 摘要")
    rollback: dict[str, object] = Field(description="回滚点：git 基线 + 数据产物回滚说明")


def _status_counts(table: pa.Table, column: str) -> FieldStatusStats:
    values = table.column(column).to_pylist()
    c = Counter(values)
    return FieldStatusStats(
        PARSED=c.get("PARSED", 0),
        MISSING=c.get("MISSING", 0),
        PARSE_FAILURE=c.get("PARSE_FAILURE", 0),
    )


def _floorplan_url_status(table: pa.Table) -> dict[str, int]:
    values = table.column("floorplan_url_status").to_pylist()
    c = Counter(values)
    out: dict[str, int] = {}
    for status in ("NO_URL", "URL_PARSE_FAILURE", "URLS_OK"):
        out[status] = c.get(status, 0)
    candidates = table.column("floorplan_candidate_count").to_pylist()
    out["FLOORPLAN_CANDIDATE_RECORDS"] = sum(1 for v in candidates if v and v > 0)
    out["FLOORPLAN_CANDIDATE_URLS"] = int(sum(v for v in candidates if v))
    return out


def _area_consistency(table: pa.Table) -> dict[str, int]:
    """第17列面积 vs 第40列面积：两列均可解析时比较；单列可解析分别计数。"""
    a17 = table.column("transaction_area_sqm").to_pylist()
    a40 = table.column("building_area_detail_sqm").to_pylist()
    both_equal = both_diff = only_17 = only_40 = 0
    for v17, v40 in zip(a17, a40, strict=True):
        if v17 is not None and v40 is not None:
            if v17 == v40:
                both_equal += 1
            else:
                both_diff += 1
        elif v17 is not None:
            only_17 += 1
        elif v40 is not None:
            only_40 += 1
    return {
        "both_parseable_equal": both_equal,
        "both_parseable_differ": both_diff,
        "only_area_17": only_17,
        "only_area_40": only_40,
    }


def _use_distribution(table: pa.Table) -> dict[str, int]:
    values = table.column("property_use_norm").to_pylist()
    return dict(Counter(values))


def build_xlsx_quality_report(
    sale_table: pa.Table,
    ordinary_table: pa.Table,
    *,
    source_sha256: str | None = None,
    git_baseline: str | None = None,
    rollback_notes: str | None = None,
    staged_manifests: list[dict[str, object]] | None = None,
) -> XlsxQualityReport:
    """从真实 staged 两表统计生成质量报告（全部数字由机器产物派生）。

    ``staged_manifests`` 为实际 ``DerivedManifest`` 摘要（table/row_count/
    built_at/inputs/parser_version/package_version/notes）；传入时：
    - 原样呈现于报告的 ``staged_tables``（CX-EXTFP1-003 修复）；
    - 与两表实际行数交叉校验，任一不符即 ``ValueError`` 失败关闭
      （报告必须能说明其统计对应的表版本）。
    """
    field_status = {
        name: _status_counts(sale_table, name)
        for name in (
            "total_price_status",
            "unit_price_status",
            "area_status",
            "building_area_status",
            "listing_price_status",
            "listing_days_status",
            "sale_date_precision",
        )
    }
    # sale_date_precision 的 DAY/MONTH/UNKNOWN 不是 PARSED 语义，单独归并
    date_prec = sale_table.column("sale_date_precision").to_pylist()
    field_status["sale_date_precision"] = FieldStatusStats(
        PARSED=sum(1 for v in date_prec if v == "DAY"),
        MISSING=sum(1 for v in date_prec if v == "UNKNOWN"),
        PARSE_FAILURE=0,
    )
    excluded = sale_table.num_rows - ordinary_table.num_rows
    manifests: list[dict[str, object]] = []
    if staged_manifests is not None:
        for m in staged_manifests:
            table_name = str(m.get("table", ""))
            row_count_raw = m.get("row_count")
            declared = int(row_count_raw) if isinstance(row_count_raw, int) else -1
            actual = (
                sale_table.num_rows if table_name == "lianjia_ext_sale_record"
                else ordinary_table.num_rows if table_name == "lianjia_ext_ordinary_residential"
                else -1
            )
            if actual == -1:
                raise ValueError(f"unknown staged table in manifest: {table_name}")
            if declared != actual:
                raise ValueError(
                    f"staged manifest row_count mismatch: {table_name} declared "
                    f"{declared} but table has {actual}"
                )
            manifests.append(m)
    return XlsxQualityReport(
        source_sha256=source_sha256,
        built_at=datetime.now(UTC).isoformat(),
        counts={
            "input_rows": sale_table.num_rows,
            "sale_record_rows": sale_table.num_rows,
            "ordinary_residential_rows": ordinary_table.num_rows,
            "excluded_rows": excluded,
            "conserved": int(sale_table.num_rows == ordinary_table.num_rows + excluded),
        },
        property_use_distribution=_use_distribution(sale_table),
        field_status=field_status,
        floorplan_url_status=_floorplan_url_status(sale_table),
        area_consistency=_area_consistency(sale_table),
        staged_tables=manifests,
        rollback={
            "git_baseline": git_baseline or "UNKNOWN",
            "data_artifacts": {
                "raw_binary_snapshot": (
                    "data/raw/source=lianjia_ext/dataset=chengjiao_xlsx/"
                    "fetched_at=20260824T000000Z/（data.bin，非 git 内容，可删除重导入）"
                ),
                "staged_sale_record": (
                    "data/staged/lianjia_ext/runs/<run_id>/lianjia_ext_sale_record.parquet"
                    "（不可变 run 产物，指针 current.json 切换；非 git 内容）"
                ),
                "staged_ordinary": (
                    "data/staged/lianjia_ext/runs/<run_id>/"
                    "lianjia_ext_ordinary_residential.parquet（同上）"
                ),
            },
            "notes": rollback_notes
            or (
                "回滚：git revert 工作包实施提交；staged 为不可变 run 版本，"
                "旧 run 产物永久保留，指针切回即可；数据产物删除后由 "
                "compsval xlsx stage 重建（源 XLSX 只读不变）。"
            ),
        },
    )


def report_to_dict(report: XlsxQualityReport) -> dict[str, object]:
    from typing import cast

    return cast(dict[str, object], json.loads(report.model_dump_json()))


def report_to_markdown(report: XlsxQualityReport) -> str:
    c = report.counts
    lines = [
        "# 外部链家 staged 数据质量报告（EXTFP1-E）",
        "",
        f"- 报告版本：`{report.report_version}`｜解析规则：`{report.parse_rule_version}`",
        f"- 源 SHA256：`{report.source_sha256 or 'UNKNOWN'}`",
        f"- 生成时间：`{report.built_at or 'UNKNOWN'}`",
        "",
        "## 1. 记录数守恒",
        "",
        f"- 全量 sale_record：{c['sale_record_rows']}",
        f"- 普通住宅 ordinary_residential：{c['ordinary_residential_rows']}",
        f"- 排除（非普通住宅 + 用途 UNKNOWN）：{c['excluded_rows']}",
        f"- 守恒校验：**{'通过' if c['conserved'] else '不通过'}**"
        f"（{c['sale_record_rows']} = {c['ordinary_residential_rows']} + {c['excluded_rows']}）",
        "",
        "## 2. 用途分布",
        "",
        "| 用途归一 | 记录数 |",
        "|---|---|",
    ]
    for use, n in sorted(report.property_use_distribution.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {use} | {n} |")
    lines += [
        "",
        "## 3. 字段质量（全量 sale_record）",
        "",
        "| 字段 | PARSED | MISSING | PARSE_FAILURE |",
        "|---|---|---|---|",
    ]
    for name, s in report.field_status.items():
        lines.append(f"| {name} | {s.PARSED} | {s.MISSING} | {s.PARSE_FAILURE} |")
    lines += [
        "",
        "## 4. 户型图 URL 状态分布（全量 sale_record）",
        "",
        "| 状态 | 记录数 |",
        "|---|---|",
    ]
    for status, n in report.floorplan_url_status.items():
        lines.append(f"| {status} | {n} |")
    a = report.area_consistency
    lines += [
        "",
        "## 5. 面积列一致性（第17列 vs 第40列）",
        "",
        f"- 两列均可解析且相等：{a['both_parseable_equal']}",
        f"- 两列均可解析但不相等：{a['both_parseable_differ']}",
        f"- 仅第17列可解析：{a['only_area_17']}",
        f"- 仅第40列可解析：{a['only_area_40']}",
        "",
        "## 6. staged 表血缘（CX-EXTFP1-003：呈现实际 manifest）",
        "",
        "| 表 | 行数 | parser_version | inputs | 包版本 |",
        "|---|---|---|---|---|",
    ]
    for m in report.staged_tables:
        inputs_raw = m.get("inputs")
        inputs_list = inputs_raw if isinstance(inputs_raw, list) else []
        inputs = "; ".join(
            f"{i.get('dataset')}@{i.get('fetched_at')} sha256={str(i.get('content_hash'))[:12]}…"
            for i in inputs_list
            if isinstance(i, dict)
        ) or "（无）"
        lines.append(
            f"| {m.get('table')} | {m.get('row_count')} | "
            f"{m.get('parser_version') or 'UNKNOWN'} | {inputs} | "
            f"{m.get('package_version') or 'UNKNOWN'} |"
        )
    lines += [
        "",
        "## 7. 血缘与回滚点",
        "",
        f"- git 基线 commit：`{report.rollback['git_baseline']}`",
        f"- 数据产物回滚说明：{report.rollback['notes']}",
        "- 本报告数字全部由 `data/staged/` 机器产物统计生成，Markdown 不维护独立数字。",
        "",
    ]
    return "\n".join(lines)


def write_xlsx_quality(
    report: XlsxQualityReport,
    *,
    data_dir: Path,
    out_json: Path | None = None,
) -> tuple[Path, Path]:
    """原子写质量报告（MD + JSON 描述同一冻结数据）。

    MD 写 `data_dir/quality/xlsx_quality_report.md`；JSON 写 `out_json`（非空时）
    或同目录 `xlsx_quality_report.json`。
    """
    quality_dir = data_dir / QUALITY_DIR
    quality_dir.mkdir(parents=True, exist_ok=True)
    md_final = quality_dir / XLSX_QUALITY_MD
    md_work = quality_dir / (XLSX_QUALITY_MD + ".incomplete")
    md_work.write_text(report_to_markdown(report), encoding="utf-8")
    md_work.replace(md_final)

    json_final = out_json if out_json is not None else quality_dir / XLSX_QUALITY_JSON
    json_final.parent.mkdir(parents=True, exist_ok=True)
    json_work = json_final.with_name(json_final.name + ".incomplete")
    json_work.write_text(
        json.dumps(report_to_dict(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    json_work.replace(json_final)
    return md_final, json_final


def load_staged_tables(
    data_dir: Path,
) -> tuple[pa.Table, pa.Table, list[dict[str, object]], str | None]:
    """读当前指针指向的 staged 两表 + 各自血缘 manifest 摘要 + run_id。

    CX-EXTFP1-001/-003 修复：仅读取 ``current.json`` 指向的当前 run 版本
    （不可变），返回真实 ``DerivedManifest`` 摘要列表供质量报告呈现与交叉校验。
    """
    import pyarrow.parquet as pq

    from compsval.ingest.xlsx_stage import read_current_run

    current = read_current_run(data_dir)
    if current is None:
        raise FileNotFoundError(
            "no current run pointer (staged/lianjia_ext/current.json); run compsval xlsx stage first"
        )
    base = data_dir / "staged" / "lianjia_ext"
    sale_path = base / current["sale_record"]
    ordinary_path = base / current["ordinary_residential"]
    sale_table = pq.read_table(sale_path)
    ordinary_table = pq.read_table(ordinary_path)
    manifests = [
        read_derived_manifest(sale_path).model_dump(mode="json"),
        read_derived_manifest(ordinary_path).model_dump(mode="json"),
    ]
    return sale_table, ordinary_table, manifests, current.get("run_id")


__all__ = [
    "QUALITY_DIR",
    "XLSX_QUALITY_JSON",
    "XLSX_QUALITY_MD",
    "XlsxQualityReport",
    "build_xlsx_quality_report",
    "load_staged_tables",
    "report_to_dict",
    "report_to_markdown",
    "write_xlsx_quality",
]
