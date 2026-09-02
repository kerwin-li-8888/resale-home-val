"""外部链家逐行解析结果落 staged 表（EXTFP1-D，技术方案 §13/§16）。

把 EXTFP1-C 的全量逐行解析（``XlsxParsedRecord``）落为两张 staged 表：

- ``lianjia_ext_sale_record``：全量逐行解析结果（原始层，245,410 条，含全部用途）；
- ``lianjia_ext_ordinary_residential``：普通住宅子集（226,482 条，§4.1 本轮
  结构化范围）。

**不可变 run 版本化（CX-EXTFP1-001 修复，技术方案 §16）**：每次运行生成唯一
``run_id``（UTC 时间戳），整套产物写入 ``staged/lianjia_ext/runs/run_<id>/``
（原子：先写 ``run_<id>.incomplete`` 再改名），完成后原子写显式当前指针
``staged/lianjia_ext/current.json`` 切换。旧运行产物永久保留，绝不覆盖；
回滚/复现通过指针指向任意历史 run 实现。

**结构化血缘（CX-EXTFP1-002 修复）**：两表 ``DerivedManifest.inputs`` 以
``InputRef(dataset=…, fetched_at=…, content_hash=…)`` 结构化指向实际原始快照
（如 ``lianjia_ext/chengjiao_xlsx@20260824T000000Z``），并记录解析规则版本
``parser_version``。金额/面积一律用 ``decimal128`` 精确存储（§6.3 禁浮点金额）。
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from compsval import __version__
from compsval.ingest.manifests import (
    DerivedManifest,
    InputRef,
    write_derived_manifest,
)
from compsval.ingest.xlsx_attributes import (
    ATTRIBUTE_COLUMNS,
    ATTRIBUTES_RULE_VERSION,
    AttributesSummary,
    normalize_attributes_table,
)
from compsval.ingest.xlsx_parse import (
    PARSE_RULE_VERSION,
    PropertyUseNorm,
    XlsxParsedRecord,
    iter_parse_xlsx,
)

STAGED_LAYER = "staged"
SALE_RECORD_TABLE = "lianjia_ext_sale_record"
ORDINARY_RESIDENTIAL_TABLE = "lianjia_ext_ordinary_residential"
SALE_RECORD_FILENAME = f"{SALE_RECORD_TABLE}.parquet"
ORDINARY_FILENAME = f"{ORDINARY_RESIDENTIAL_TABLE}.parquet"
LIANJIA_EXT_DIR = "lianjia_ext"  # staged/lianjia_ext/（EXTFP1 表专目录）
RUNS_DIR = "runs"
CURRENT_POINTER = "current.json"

DECIMAL = pa.decimal128(18, 2)

# 列顺序固定（schema 契约；extra_fields 序列化为 JSON 字符串列）
COLUMNS = [
    "row_number",
    "source_record_id",
    "community_name",
    "community_source_id",
    "sale_date_raw",
    "sale_date",
    "sale_date_precision",
    "total_price_raw",
    "total_price_yuan",
    "total_price_status",
    "unit_price_observed_raw",
    "unit_price_observed",
    "unit_price_status",
    "transaction_area_sqm",
    "area_status",
    "building_area_detail_sqm",
    "building_area_status",
    "layout_raw",
    "bedrooms_raw",
    "living_rooms_raw",
    "floor_raw",
    "orientation",
    "decoration",
    "built_year_raw",
    "house_type",
    "has_elevator_raw",
    "property_use_raw",
    "property_use_norm",
    "floorplan_url_list_raw",
    "floorplan_url_status",
    "floorplan_candidate_count",
    "listing_price_raw",
    "listing_price_yuan",
    "listing_price_status",
    "listing_days_raw",
    "listing_days",
    "listing_days_status",
    "price_adjustments_raw",
    "source_property_description",
    "source_property_tags",
    "extra_fields_json",
]

_INT_COLUMNS = frozenset({"row_number", "floorplan_candidate_count", "listing_days"})
_DECIMAL_COLUMNS = frozenset(
    {
        "total_price_yuan",
        "unit_price_observed",
        "transaction_area_sqm",
        "building_area_detail_sqm",
        "listing_price_yuan",
    }
)


def _sale_record_schema() -> pa.Schema:
    fields = []
    for name in COLUMNS:
        if name in _INT_COLUMNS:
            fields.append(pa.field(name, pa.int64(), nullable=True))
        elif name in _DECIMAL_COLUMNS:
            fields.append(pa.field(name, DECIMAL, nullable=True))
        else:
            fields.append(pa.field(name, pa.string(), nullable=True))
    return pa.schema(fields)


def _attribute_field_type(name: str) -> pa.DataType:
    if name in {"total_floors", "year_built"}:
        return pa.int64()
    if name == "has_elevator":
        return pa.bool_()
    return pa.string()


def attributes_schema() -> pa.Schema:
    """staged 表 + 标准化属性列模式（v2 run 两表共用；原文列保持不变）。"""
    base = _sale_record_schema()
    extra = [
        pa.field(name, _attribute_field_type(name), nullable=True)
        for name in ATTRIBUTE_COLUMNS
    ]
    return pa.schema(list(base) + extra)


def sale_record_table(records: Iterator[XlsxParsedRecord]) -> pa.Table:
    """把全量逐行解析记录构建为 staged 宽表（列对齐 COLUMNS）。"""
    rows: dict[str, list[Any]] = {name: [] for name in COLUMNS}
    for rec in records:
        d = rec.model_dump()
        for name in COLUMNS:
            if name == "extra_fields_json":
                rows[name].append(
                    json.dumps(d["extra_fields"], ensure_ascii=False, sort_keys=True)
                )
            else:
                rows[name].append(d[name])
    return pa.table(rows, schema=_sale_record_schema())


def ordinary_residential_table(sale_table: pa.Table) -> pa.Table:
    """普通住宅子集：property_use_norm == 普通住宅（§4.1 本轮结构化范围）。"""
    mask = pc.equal(
        sale_table.column("property_use_norm"),
        PropertyUseNorm.ORDINARY_RESIDENTIAL.value,
    )
    return sale_table.filter(mask)


def _runs_root(data_dir: Path) -> Path:
    return data_dir / STAGED_LAYER / LIANJIA_EXT_DIR / RUNS_DIR


def current_pointer_path(data_dir: Path) -> Path:
    return data_dir / STAGED_LAYER / LIANJIA_EXT_DIR / CURRENT_POINTER


def read_current_run(data_dir: Path) -> dict[str, str] | None:
    """读当前指针（显式 current 指针；无指针返回 None）。"""
    from typing import cast

    path = current_pointer_path(data_dir)
    if not path.is_file():
        return None
    return cast(dict[str, str], json.loads(path.read_text(encoding="utf-8")))


def _write_staged(
    table: pa.Table,
    *,
    table_name: str,
    filename: str,
    run_dir: Path,
    inputs: Sequence[InputRef],
    parser_version: str | None,
    notes: str | None = None,
) -> Path:
    final_path = run_dir / filename
    work_path = run_dir / (filename + ".incomplete")
    pq.write_table(table, work_path, compression="zstd")
    input_refs = [
        InputRef(dataset=i.dataset, fetched_at=i.fetched_at, content_hash=i.content_hash)
        for i in inputs
    ]
    manifest = DerivedManifest(
        layer=STAGED_LAYER,
        table=table_name,
        built_at=datetime.now(UTC),
        row_count=table.num_rows,
        inputs=input_refs,
        package_version=__version__,
        parser_version=parser_version,
        notes=notes,
    )
    write_derived_manifest(manifest, final_path)
    work_path.replace(final_path)
    return final_path


def _write_current_pointer(
    data_dir: Path, run_id: str, sale_record: str, ordinary_residential: str
) -> Path:
    """原子写当前指针（.incomplete + rename）；完成后新 run 才成为当前。"""
    final_path = current_pointer_path(data_dir)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    work_path = final_path.with_name(CURRENT_POINTER + ".incomplete")
    work_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "sale_record": sale_record,
                "ordinary_residential": ordinary_residential,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    work_path.replace(final_path)
    return final_path


@dataclass(frozen=True)
class XlsxStageResult:
    """staged 表写入结果 + 守恒统计（run 版本化产物）。"""

    run_id: str
    run_dir: Path
    current_pointer: Path
    sale_record_path: Path
    ordinary_residential_path: Path
    sale_record_count: int
    ordinary_residential_count: int
    excluded_count: int


def stage_xlsx(
    path: Path,
    *,
    data_dir: Path,
    inputs: Sequence[InputRef] | None = None,
    notes: str | None = None,
    run_id: str | None = None,
) -> XlsxStageResult:
    """完整链路：读 XLSX（只读）→ 全量解析 → 两表 → 不可变 run 目录 + 当前指针。

    ``run_id`` 默认取当前 UTC 时间戳（``%Y%m%dT%H%M%SZ``）；同 run_id 已存在时
    ``FileExistsError``（run 产物不可变，绝不覆盖）。``inputs`` 应为指向实际
    原始快照的 ``InputRef``（含 content_hash）；默认构造
    ``chengjiao_xlsx@UNKNOWN`` 由调用方（CLI）补全。
    """
    if not path.is_file():
        raise FileNotFoundError(f"source xlsx not found: {path}")
    source_sha = _sha256(path)
    run_id = run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    runs_root = _runs_root(data_dir)
    runs_root.mkdir(parents=True, exist_ok=True)
    final_run_dir = runs_root / f"run_{run_id}"
    if final_run_dir.exists():
        raise FileExistsError(f"run already exists: {final_run_dir} (immutable run)")
    work_run_dir = runs_root / f"run_{run_id}.incomplete"
    work_run_dir.mkdir(parents=True, exist_ok=False)

    inputs = list(inputs) if inputs is not None else []
    if not inputs:
        inputs = [InputRef(dataset="chengjiao_xlsx", fetched_at="UNKNOWN", content_hash=source_sha)]
    input_refs = [
        InputRef(dataset=i.dataset, fetched_at=i.fetched_at, content_hash=i.content_hash)
        for i in inputs
    ]

    try:
        table = sale_record_table(iter_parse_xlsx(path))
        ordinary = ordinary_residential_table(table)
        _write_staged(
            table,
            table_name=SALE_RECORD_TABLE,
            filename=SALE_RECORD_FILENAME,
            run_dir=work_run_dir,
            inputs=input_refs,
            parser_version=PARSE_RULE_VERSION,
            notes=notes or f"EXTFP1-D full parse @ sha256={source_sha}",
        )
        _write_staged(
            ordinary,
            table_name=ORDINARY_RESIDENTIAL_TABLE,
            filename=ORDINARY_FILENAME,
            run_dir=work_run_dir,
            inputs=input_refs,
            parser_version=PARSE_RULE_VERSION,
            notes=notes or f"EXTFP1-D ordinary residential @ sha256={source_sha}",
        )
    except Exception:
        # 失败时清理 work run 目录（不留下不可识别残留）
        import shutil

        if work_run_dir.exists():
            shutil.rmtree(work_run_dir, ignore_errors=True)
        raise

    work_run_dir.rename(final_run_dir)
    current = _write_current_pointer(
        data_dir,
        run_id,
        f"{RUNS_DIR}/run_{run_id}/{SALE_RECORD_FILENAME}",
        f"{RUNS_DIR}/run_{run_id}/{ORDINARY_FILENAME}",
    )
    return XlsxStageResult(
        run_id=run_id,
        run_dir=final_run_dir,
        current_pointer=current,
        sale_record_path=final_run_dir / SALE_RECORD_FILENAME,
        ordinary_residential_path=final_run_dir / ORDINARY_FILENAME,
        sale_record_count=table.num_rows,
        ordinary_residential_count=ordinary.num_rows,
        excluded_count=table.num_rows - ordinary.num_rows,
    )


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class AttributesStageResult:
    """属性标准化 v2 run 结果（不可变 run 产物 + 质量摘要）。"""

    run_id: str
    source_run_id: str
    run_dir: Path
    current_pointer: Path
    sale_record_path: Path
    ordinary_residential_path: Path
    quality_json: Path
    sale_record_count: int
    ordinary_residential_count: int
    summary: AttributesSummary


def _run_dir(data_dir: Path, run_id: str) -> Path:
    return _runs_root(data_dir) / f"run_{run_id}"


def stage_attributes_run(
    source_run_id: str,
    *,
    data_dir: Path,
    target_run_id: str | None = None,
    notes: str | None = None,
) -> AttributesStageResult:
    """从指定 v1 run 只读派生属性标准化 v2 run（excel-attribute-enrichment）。

    - 读 ``runs/run_<source_run_id>/`` 两表（只读，原文列逐字节保留），
      追加标准化属性列后写入新 run 目录（不可变，``.incomplete`` 原子写）；
    - DerivedManifest ``inputs`` 结构化指向源 run 两表（含内容 SHA256），
      ``parser_version`` 记录属性标准化规则版本；
    - 质量摘要（各列覆盖率/缺失/解析失败分布）落 run 目录
      ``attributes_quality.json``；
    - 完成后原子切换 staged current 指针；源 run 与冻结版本目录零改动。
    """
    source_dir = _run_dir(data_dir, source_run_id)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"源 run 不存在：{source_dir}")
    source_paths = {
        SALE_RECORD_FILENAME: source_dir / SALE_RECORD_FILENAME,
        ORDINARY_FILENAME: source_dir / ORDINARY_FILENAME,
    }
    for path in source_paths.values():
        if not path.is_file():
            raise FileNotFoundError(f"源 run 缺表：{path}")

    run_id = target_run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    final_run_dir = _run_dir(data_dir, run_id)
    if final_run_dir.exists():
        raise FileExistsError(f"run already exists: {final_run_dir} (immutable run)")
    work_run_dir = _runs_root(data_dir) / f"run_{run_id}.incomplete"
    work_run_dir.mkdir(parents=True, exist_ok=False)

    try:
        summaries: dict[str, AttributesSummary] = {}
        tables: dict[str, pa.Table] = {}
        for filename in (SALE_RECORD_FILENAME, ORDINARY_FILENAME):
            source_table = pq.read_table(source_paths[filename])
            normalized, summary = normalize_attributes_table(source_table)
            tables[filename] = normalized
            summaries[filename] = summary

        quality_json = work_run_dir / "attributes_quality.json"
        quality_payload = {
            "source_run_id": source_run_id,
            "run_id": run_id,
            "sale_record": summaries[SALE_RECORD_FILENAME].to_dict(),
            "ordinary_residential": summaries[ORDINARY_FILENAME].to_dict(),
        }
        quality_work = quality_json.with_name(quality_json.name + ".incomplete")
        quality_work.write_text(
            json.dumps(quality_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        quality_work.replace(quality_json)

        for filename, table in tables.items():
            _write_staged(
                table,
                table_name=filename.removesuffix(".parquet"),
                filename=filename,
                run_dir=work_run_dir,
                inputs=[
                    InputRef(
                        dataset=f"lianjia_ext_staged_run:{filename.removesuffix('.parquet')}",
                        fetched_at=source_run_id,
                        content_hash=_sha256(source_paths[filename]),
                    )
                ],
                parser_version=ATTRIBUTES_RULE_VERSION,
                notes=notes
                or (
                    f"attributes v2 derived from run_{source_run_id} "
                    f"@ {ATTRIBUTES_RULE_VERSION}"
                ),
            )
    except Exception:
        import shutil

        if work_run_dir.exists():
            shutil.rmtree(work_run_dir, ignore_errors=True)
        raise

    work_run_dir.rename(final_run_dir)
    # 质量文件随 run 目录 rename 一并就位；重新定位最终路径。
    final_quality = final_run_dir / "attributes_quality.json"
    current = _write_current_pointer(
        data_dir,
        run_id,
        f"{RUNS_DIR}/run_{run_id}/{SALE_RECORD_FILENAME}",
        f"{RUNS_DIR}/run_{run_id}/{ORDINARY_FILENAME}",
    )
    sale_summary = summaries[SALE_RECORD_FILENAME]
    ordinary_summary = summaries[ORDINARY_FILENAME]
    return AttributesStageResult(
        run_id=run_id,
        source_run_id=source_run_id,
        run_dir=final_run_dir,
        current_pointer=current,
        sale_record_path=final_run_dir / SALE_RECORD_FILENAME,
        ordinary_residential_path=final_run_dir / ORDINARY_FILENAME,
        quality_json=final_quality,
        sale_record_count=sale_summary.row_count,
        ordinary_residential_count=ordinary_summary.row_count,
        summary=ordinary_summary,
    )


__all__ = [
    "ATTRIBUTES_RULE_VERSION",
    "ATTRIBUTE_COLUMNS",
    "AttributesStageResult",
    "CURRENT_POINTER",
    "LIANJIA_EXT_DIR",
    "ORDINARY_FILENAME",
    "ORDINARY_RESIDENTIAL_TABLE",
    "RUNS_DIR",
    "SALE_RECORD_FILENAME",
    "SALE_RECORD_TABLE",
    "STAGED_LAYER",
    "XlsxStageResult",
    "attributes_schema",
    "current_pointer_path",
    "ordinary_residential_table",
    "read_current_run",
    "sale_record_table",
    "stage_attributes_run",
    "stage_xlsx",
]
