"""valid_sale 属性扩列回填（excel-attribute-enrichment 任务③）。

以「小区名 → 标准 community_id（既有 community/community_alias 权威表 +
链家成交社区注册表 lookup）+ 交易身份键（community_id+面积+成交日+总价，
与 marts_build 跨源去重身份键同构）」把 staged v2 标准化属性 join 回
``valid_sale`` mart：

- 命中行回填 ``total_floors/year_built/has_elevator/decoration_norm`` 四列，
  并写 ``attribute_enrich_ref`` 行级注记（来源 staged run + 身份键，可溯源）；
- 同一身份键多行按属性字段丰富度取一（沿用 marts_build ``_richness`` 思路），
  冲突键计数入统计，不静默合并；
- 无匹配行属性如实留 None（未知不用 0，数据字典 §1）；
- 回填为显式步骤：不执行本步骤（或 valid_sale 无属性列）时，估值链自动
  回旧行为（``valuation.comparable`` 按列存在性读取）。

本模块只读源表，产物写到显式 ``out_path``；不改写 raw/staged/既有 marts。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import __version__
from compsval.entities.backfill import (
    CommunityIdLookup,
    load_community_lookup,
    resolve_community_id,
)
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    read_derived_manifest,
    write_derived_manifest,
)
from compsval.ingest.marts_build import lianjia_extended_lookup
from compsval.ingest.xlsx_stage import (
    ORDINARY_FILENAME,
    read_current_run,
)

ENRICH_RULE_VERSION = "ATTR-ENRICH-1.0"
ENRICH_REF_COLUMN = "attribute_enrich_ref"
ENRICH_COLUMNS: tuple[str, ...] = ("total_floors", "year_built", "has_elevator", "decoration_norm")

_COLUMN_TYPES: dict[str, pa.DataType] = {
    "total_floors": pa.int64(),
    "year_built": pa.int64(),
    "has_elevator": pa.bool_(),
    "decoration_norm": pa.string(),
    ENRICH_REF_COLUMN: pa.string(),
}


@dataclass(frozen=True)
class EnrichStats:
    """一次属性回填的统计（命中率/冲突/前后覆盖率，入 manifest notes 与报告）。"""

    source_run_id: str
    rows_total: int
    matched: int
    unmatched: int
    conflict_keys: int
    excel_rows_indexed: int
    excel_rows_unmapped_community: int
    coverage_before: dict[str, float]
    coverage_after: dict[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "rule_version": ENRICH_RULE_VERSION,
            "source_run_id": self.source_run_id,
            "rows_total": self.rows_total,
            "matched": self.matched,
            "unmatched": self.unmatched,
            "conflict_keys": self.conflict_keys,
            "excel_rows_indexed": self.excel_rows_indexed,
            "excel_rows_unmapped_community": self.excel_rows_unmapped_community,
            "coverage_before": self.coverage_before,
            "coverage_after": self.coverage_after,
        }


def _norm_area(value: object) -> float | None:
    """面积归一（与 marts_build._identity_key 同口径：float 后保留 2 位）。"""
    if value is None:
        return None
    try:
        number = float(str(value))
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return round(number, 2)


def _norm_total_price(value: object) -> int | None:
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return None
    if number <= 0:
        return None
    return int(number.to_integral_value())


def identity_key(
    community_id: str | None,
    area_sqm: object,
    sale_date: date | None,
    total_price_yuan: object,
) -> str | None:
    """交易身份键；任一身份字段缺失 → None（不臆测合并）。"""
    cid = str(community_id).strip() if community_id else ""
    area = _norm_area(area_sqm)
    total = _norm_total_price(total_price_yuan)
    if not cid or area is None or not isinstance(sale_date, date) or total is None:
        return None
    return "|".join([cid, f"{area}", sale_date.isoformat(), str(total)])


def _richness(attrs: dict[str, object]) -> int:
    """属性字段丰富度：四列中已知值计数（回填保留依据，同 marts_build 思路）。"""
    return sum(1 for name in ENRICH_COLUMNS if attrs.get(name) is not None)


def build_attribute_index(
    ordinary: pa.Table,
    lookup: CommunityIdLookup,
) -> tuple[dict[str, dict[str, object]], int, int]:
    """staged v2 普通住宅表 → 身份键 → 属性索引（丰富度取一，冲突计数）。

    返回 ``(index, conflict_keys, unmapped_rows)``；小区名无法解析为标准
    community_id 的行计入 ``unmapped_rows`` 并跳过（不臆测归并）。
    缺标准化属性列时抛 ``KeyError``（应先执行 ``xlsx attributes-stage``）。
    """
    needed = {
        "community_name",
        "transaction_area_sqm",
        "sale_date",
        "total_price_yuan",
        *ENRICH_COLUMNS,
    }
    missing = needed - set(ordinary.column_names)
    if missing:
        raise KeyError(f"staged 表缺少标准化属性列：{sorted(missing)}（先执行 attributes-stage）")

    names = ordinary.column("community_name").to_pylist()
    areas = ordinary.column("transaction_area_sqm").to_pylist()
    dates = ordinary.column("sale_date").to_pylist()
    totals = ordinary.column("total_price_yuan").to_pylist()
    attrs_by_row: list[dict[str, object]] = [
        {name: ordinary.column(name)[i].as_py() for name in ENRICH_COLUMNS}
        for i in range(ordinary.num_rows)
    ]

    index: dict[str, dict[str, object]] = {}
    conflict_keys = 0
    unmapped = 0
    for i, name in enumerate(names):
        community_id, _outcome, _reason = resolve_community_id(str(name), lookup)
        sale_date_text = str(dates[i]).strip() if dates[i] is not None else ""
        sale_date = _parse_iso_date(sale_date_text)
        key = identity_key(community_id, areas[i], sale_date, totals[i])
        if key is None:
            if community_id is None:
                unmapped += 1
            continue
        candidate = attrs_by_row[i]
        if key not in index:
            index[key] = candidate
            continue
        conflict_keys += 1
        if _richness(candidate) > _richness(index[key]):
            index[key] = candidate
    return index, conflict_keys, unmapped


def _parse_iso_date(text: str) -> date | None:
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _coverage(table: pa.Table) -> dict[str, float]:
    total = table.num_rows
    if total == 0:
        return {name: 0.0 for name in ENRICH_COLUMNS}
    return {
        name: (
            sum(1 for v in table.column(name).to_pylist() if v is not None) / total
            if name in table.column_names
            else 0.0
        )
        for name in ENRICH_COLUMNS
    }


def enrich_valid_sale(
    valid_sale: pa.Table,
    index: dict[str, dict[str, object]],
    *,
    source_run_id: str,
) -> tuple[pa.Table, EnrichStats]:
    """按身份键回填属性列（无匹配留 None；行级注记可溯源）。输入表不被修改。

    幂等：输入若已含扩列列（如对扩列版 mart 重跑），先去掉旧扩列列再回填，
    保证重复执行结果一致、不产生重复列。
    """
    work = valid_sale
    drop = [
        name
        for name in (*ENRICH_COLUMNS, ENRICH_REF_COLUMN)
        if name in work.column_names
    ]
    if drop:
        work = work.drop_columns(drop)
    ids = work.column("community_id").to_pylist()
    areas = work.column("area_sqm").to_pylist()
    dates = work.column("sale_date").to_pylist()
    totals = work.column("total_price_yuan").to_pylist()

    filled: dict[str, list[object]] = {name: [] for name in ENRICH_COLUMNS}
    refs: list[object] = []
    matched = 0
    for i in range(work.num_rows):
        key = identity_key(ids[i], areas[i], dates[i], totals[i])
        attrs = index.get(key) if key is not None else None
        if attrs is None:
            for name in ENRICH_COLUMNS:
                filled[name].append(None)
            refs.append(None)
            continue
        matched += 1
        for name in ENRICH_COLUMNS:
            filled[name].append(attrs.get(name))
        refs.append(f"lianjia_ext@{source_run_id}|{key}")

    out = work
    for name in ENRICH_COLUMNS:
        out = out.append_column(
            pa.field(name, _COLUMN_TYPES[name], nullable=True),
            pa.array(filled[name], type=_COLUMN_TYPES[name]),
        )
    out = out.append_column(
        pa.field(ENRICH_REF_COLUMN, _COLUMN_TYPES[ENRICH_REF_COLUMN], nullable=True),
        pa.array(refs, type=_COLUMN_TYPES[ENRICH_REF_COLUMN]),
    )
    stats = EnrichStats(
        source_run_id=source_run_id,
        rows_total=out.num_rows,
        matched=matched,
        unmatched=out.num_rows - matched,
        conflict_keys=0,
        excel_rows_indexed=len(index),
        excel_rows_unmapped_community=0,
        coverage_before=_coverage(valid_sale),
        coverage_after=_coverage(out),
    )
    return out, stats


def _load_ordinary_run(data_dir: Path, run_id: str) -> pa.Table:
    path = data_dir / "staged" / "lianjia_ext" / "runs" / f"run_{run_id}" / ORDINARY_FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"staged v2 run 普通住宅表不存在：{path}")
    return pq.read_table(path)


def resolve_attribute_run_id(data_dir: Path, run_id: str | None) -> str:
    """显式 run_id 优先；否则取 staged current 指针；都没有 → 显式报错。"""
    if run_id:
        return run_id
    current = read_current_run(data_dir)
    if current is None or not current.get("run_id"):
        raise FileNotFoundError(
            "未指定 --run-id 且 staged current 指针缺失（先执行 xlsx attributes-stage）"
        )
    return str(current["run_id"])


def enrich_attributes_mart(
    *,
    data_dir: Path,
    out_path: Path,
    run_id: str | None = None,
    notes: str | None = None,
) -> tuple[Path, EnrichStats]:
    """属性回填主入口：staged v2 + 既有 valid_sale → 扩列 mart（显式 out_path）。

    - ``run_id`` 缺省取 staged current 指针；指定 run 缺属性列或文件缺失 →
      显式报错（不静默退化为无属性）；
    - 输出表 DerivedManifest 继承既有 valid_sale 的 inputs 并追加 v2 run
      引用；notes 记录命中率/冲突/前后覆盖率（可复核）；
    - 不改写既有 ``marts/valid_sale.parquet``（正式切换属基线确认后动作）。
    """
    run_id = resolve_attribute_run_id(data_dir, run_id)
    ordinary = _load_ordinary_run(data_dir, run_id)
    lookup = lianjia_extended_lookup(load_community_lookup(data_dir=data_dir))
    index, conflict_keys, unmapped = build_attribute_index(ordinary, lookup)

    valid_sale_path = data_dir / "marts" / "valid_sale.parquet"
    if not valid_sale_path.is_file():
        raise FileNotFoundError(f"valid_sale mart 不存在：{valid_sale_path}")
    valid_sale = pq.read_table(valid_sale_path)

    enriched, base_stats = enrich_valid_sale(valid_sale, index, source_run_id=run_id)
    stats = EnrichStats(
        source_run_id=base_stats.source_run_id,
        rows_total=base_stats.rows_total,
        matched=base_stats.matched,
        unmatched=base_stats.unmatched,
        conflict_keys=conflict_keys,
        excel_rows_indexed=base_stats.excel_rows_indexed,
        excel_rows_unmapped_community=unmapped,
        coverage_before=base_stats.coverage_before,
        coverage_after=base_stats.coverage_after,
    )

    try:
        inherited = list(read_derived_manifest(valid_sale_path).inputs)
    except Exception:  # noqa: BLE001 - 旧 manifest 缺损不阻塞血缘登记，如实标注
        inherited = []
    inputs = [
        InputRef(dataset=item.dataset, fetched_at=item.fetched_at, content_hash=item.content_hash)
        for item in inherited
    ]
    inputs.append(
        InputRef(dataset="lianjia_ext_ordinary_residential", fetched_at=run_id, content_hash=None)
    )
    manifest_notes = notes or (
        f"attribute enrichment @ {ENRICH_RULE_VERSION} from staged run_{run_id}"
        f" matched={stats.matched}/{stats.rows_total}"
        f" conflict_keys={stats.conflict_keys}"
        f" unmapped_excel_rows={stats.excel_rows_unmapped_community}"
        f" coverage_after={ {k: round(v, 4) for k, v in stats.coverage_after.items()} }"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    work_path = out_path.with_name(out_path.name + ".incomplete")
    pq.write_table(enriched, work_path, compression="zstd")
    write_derived_manifest(
        DerivedManifest(
            layer="marts",
            table="valid_sale",
            built_at=datetime.now(UTC),
            row_count=enriched.num_rows,
            inputs=inputs,
            package_version=__version__,
            parser_version=ENRICH_RULE_VERSION,
            notes=manifest_notes,
        ),
        out_path,
    )
    work_path.replace(out_path)
    return out_path, stats
