"""G3R-C 多源 marts 合并构建（跨源去重 + 合并 valid_sale/valid_listing + 质量报告）。

WP8 时代 marts 层为**单快照**语义（``data stage --snapshot`` 逐快照覆盖
``valid_sale.parquet``）。G3R 引入房天下（SRC-005）7 小区成交后，回放需要
**多源合并**的正式成交池。本模块实现一条新的可复现构建路径：

1. 从 catalog 取参与合并的原始快照（链家 ``chengjiao_list`` + 房天下
   ``chengjiao``，按 (source, fetched_at) 排序保证确定性）；
2. 按来源分派解析器重建规范化记录（链家按行重建、房天下按结构化列 +
   快照→小区映射注册表解析）；
3. 每来源执行 WP4-C 清洗（车位/来源内去重/异常单价）；
4. 合并 sale_event 表并**重新生成全局唯一 sale_event_id**（跨快照行号会
   撞号：房天下每个 CSV 都有 ``line2``）；
5. **跨源去重**：同一交易身份（community_id + 面积 + 成交日 + 总价）多录
   仅保留字段最丰富/来源优先级最高的一条（链家含户型/朝向/挂牌更丰富），
   其余标 ``SUSPECT_DUPLICATE``（跨源注记）——保证同一交易在回放中不重复
   计数（合同非阻断假设 #2）；
6. 构建 marts ``valid_sale``（正式池 NORMAL）+ ``valid_listing``（仅链家有
   挂牌证据）+ 数据质量报告（§8.4 全项，多输入溯源）。

本模块**不改写** raw 快照；单快照 ``data stage`` 行为与 WP4-E 测试不回归
（本路径独立于 ``stage.data_stage``）。合并产物 manifest 记录全部输入快照
溯源，可复现（同输入同输出）。
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.catalog import SnapshotRef, list_snapshots
from compsval.entities.backfill import (
    CommunityIdLookup,
    collect_unmatched_conflicts,
    load_community_lookup,
)
from compsval.ingest.clean import (
    CleanedSale,
    CleaningSummary,
    SaleRecord,
    clean_sales,
    sale_event_table,
)
from compsval.ingest.ext_source import (
    EXT_DATASET,
    EXT_SOURCE_ID,
    ext_rows_to_records,
    read_current_ext_run,
    run_fetched_at,
    split_resolvable,
)
from compsval.ingest.listing import (
    _listing_event_schema,
    listing_event_table,
)
from compsval.ingest.manifests import InputRef
from compsval.ingest.parsers.fang_esf import (
    parse_fang_esf_csv,
    resolve_fang_community,
)
from compsval.ingest.parsers.lianjia import LianjiaRecord
from compsval.ingest.parsers.lianjia_html import parse_lianjia_csv_table
from compsval.ingest.quality import build_quality_report, write_quality_report
from compsval.ingest.stage import (
    raw_snapshot_to_records,
    snapshot_id_of,
    source_id_of,
    valid_listing_table,
    valid_sale_table,
    write_valid_listing_mart,
    write_valid_sale_mart,
)

#: 参与成交合并的 (source 目录名, dataset) 集合。
#: ``lianjia/chengjiao_list``（WP4-B TXT 快照）+ ``lianjia/chengjiao``
#: （LJ-D 36 链家成交全量翻页 CSV 快照）+ ``fang_esf/chengjiao``（房天下 7 补数）。
MERGED_SOURCES: tuple[tuple[str, str], ...] = (
    ("lianjia", "chengjiao_list"),
    ("lianjia", "chengjiao"),
    ("fang_esf", "chengjiao"),
)

#: 跨源去重保留优先级（来源字段丰富度，链家 > ext > 房天下）。
#: ext-sale-ingest-scope-v1-2：SRC-011（staged ext run）插入中间档；既有
#: SRC-007 > SRC-005 的相对序不变（既有保留行不因重映射翻转）。
_SOURCE_PRIORITY: dict[str, int] = {"SRC-007": 3, "SRC-011": 2, "SRC-005": 1}

#: 字段丰富度打分列（跨源去重保留更丰富来源的依据）。
_RICHNESS_COLUMNS = ("layout", "orientation", "listing_price_yuan", "unit_price_observed")

#: 链家成交社区名 → 标准 community_id（LJ-E community_id 回填注册表）。
#: 来源 = `01-数据/sources/链家补数探测-V0.1.md` §2（7 补数映射）+
#: §3（链家 27 条成交小区候选名录映射）+ `候选小区名录-V0.1.md`（标准 ID）。
#: P0 五小区（ext-sale-ingest-scope-v1-2）：来源 = `01-数据/sources/同名核验
#: 证据包-P0五小区-V0.1.md` §0 判定表 + §8 复核记录（唯一复核人 user
#: 2026-08-31 逐条「确认」，ext run 20260831T041648Z 指纹 + census 双管线）。
#: 探测报告标注「待核」（楹隆花园/示例小区220二期/澜庭泊府等）、「名录外」或
#: 证据包未确认的相似命名（示例小区145/示例小区217/红棉苑南区/示例小区009等）
#: 不进入本注册表，由 backfill 如实登记未匹配（LJ-E 合同「名录外如实标记」）。
LIANJIA_COMMUNITY_REGISTRY: dict[str, str] = {
    # --- 7 补数（探测报告 §2）---
    "示例小区166": "C-XXXX0122",
    "示例小区132": "C-XXXX0069",
    "示例小区136": "C-XXXX0033",
    "示例小区136拾光里": "C-XXXX0051",  # 链家归入示例小区136名下分区，房天下独立"拾光里"
    "示例小区121": "C-XXXX0048",
    "示例小区203": "C-XXXX0042",
    "示例小区130": "C-XXXX0063",
    "示例小区130A区": "C-XXXX0063",
    "示例小区130B区": "C-XXXX0063",
    # --- 链家 27 条（探测报告 §3，候选名录可映射）---
    "示例小区126": "C-XXXX0056",
    "示例小区177": "C-XXXX0013",
    "示例小区229": "C-XXXX0034",
    "示例小区229丽华苑": "C-XXXX0034",
    "示例小区229惠侨苑": "C-XXXX0034",
    "示例小区229明华苑": "C-XXXX0034",
    "示例小区229示例小区187": "C-XXXX0034",
    "示例小区229漾日云天": "C-XXXX0034",
    "示例小区157": "C-XXXX0047",
    "前进路(目标区)": "C-XXXX0113",  # 链家搜索词=示例小区141（探测报告 §3）
    # --- P0 五小区（同名核验证据包-P0五小区-V0.1.md §0/§8，用户确认）---
    "示例小区144": "C-XXXX0116",  # 证据包 §0#5：69 条，指纹+双管线+隔离检查
    "示例小区202": "C-XXXX0052",  # 证据包 §0#1：153 条，指纹+双管线
    "示例小区041": "C-XXXX0053",  # 证据包 §0#2：167 条，指纹+名录地址+双管线
    "示例小区219": "C-XXXX0105",  # 证据包 §0#3：164 条，指纹+双管线
    "示例小区031": "C-XXXX0009",  # 证据包 §0#4：381 条，指纹极独特+双管线
}

_MISSING_WORDS = {"UNKNOWN", "MISSING", "NOT_APPLICABLE", "PARSE_FAILURE", "CONFLICT"}


@dataclass(frozen=True)
class CombinedMartResult:
    """一次多源 marts 合并构建的结果（供 CLI 打印与测试断言）。"""

    snapshot_ids: tuple[str, ...]
    valid_sale_path: Path
    valid_listing_path: Path
    quality_md: Path
    quality_json: Path
    summary: CleaningSummary
    cross_source_duplicates: int
    layout_backfilled: int
    sale_table: pa.Table
    listing_table: pa.Table
    # ext 第四来源留痕（ext-sale-ingest-scope-v1-2；未接入时为 None/0）
    ext_run_id: str | None = None
    ext_input_rows: int = 0
    ext_kept_rows: int = 0
    ext_unmatched_rows: int = 0
    ext_unmatched_names: int = 0
    attribute_matched_rows: int = 0


def merge_snapshots(data_dir: Path) -> list[SnapshotRef]:
    """参与合并的原始快照（链家成交列表 + 房天下成交），确定性排序。"""
    refs = [
        ref
        for ref in list_snapshots(data_dir)
        if (ref.source, ref.dataset) in MERGED_SOURCES
    ]
    return sorted(refs, key=lambda ref: (ref.source, ref.fetched_at))


def reconstruct_records(ref: SnapshotRef) -> list[SaleRecord]:
    """按来源分派解析器重建规范化记录（快照不可变，仅读取）。"""
    if ref.source == "lianjia" and ref.dataset == "chengjiao_list":
        return list(raw_snapshot_to_records(ref))
    if ref.source == "lianjia" and ref.dataset == "chengjiao":
        # LJ-D 导入的链家成交 CSV 快照：community 列即为链家页面小区名，
        # 按 CSV 列回读为规范化记录（LJ-C 解析器；缺失语义与 WP4-B 一致）。
        table = pq.read_table(ref.data_path)
        return list(parse_lianjia_csv_table(table, ""))
    if ref.source == "fang_esf" and ref.dataset == "chengjiao":
        community = resolve_fang_community(ref.manifest().query)
        if community is None:
            raise ValueError(
                f"房天下快照 {ref.source}/{ref.dataset}@{ref.fetched_at} 无法解析小区"
                f"（query 不含已登记 loupan ID），拒绝合并以防臆测"
            )
        table = pq.read_table(ref.data_path)
        return list(parse_fang_esf_csv(table, community))
    raise ValueError(f"快照 {ref.source}/{ref.dataset} 不在合并来源集合内")


def _identity_key(row: dict[str, object]) -> str | None:
    """跨源去重身份键：community_id（无则 community）+ 面积 + 成交日 + 总价。

    任一身份字段缺失 → ``None``（不臆测合并，缺失记录不做跨源去重）。
    """
    community_id = row.get("community_id")
    community = row.get("community")
    cid = str(community_id).strip() if community_id else str(community or "").strip()
    area = row.get("area_sqm")
    event_date = row.get("event_date")
    total = row.get("total_price_yuan")
    if not cid or area is None or not isinstance(event_date, date) or total is None:
        return None
    return "|".join([cid, f"{round(float(str(area)), 2)}", event_date.isoformat(), str(total)])


def _is_known(value: object) -> bool:
    return not (
        value is None or isinstance(value, str) and value.strip() in _MISSING_WORDS
    )


def _richness(row: dict[str, object]) -> int:
    """字段完备度：已知字段（layout/orientation/挂牌价/披露单价）计数。"""
    return sum(1 for col in _RICHNESS_COLUMNS if _is_known(row.get(col)))


def _rank(row: dict[str, object]) -> tuple[int, int]:
    """保留排序：(字段完备度, 来源优先级)。"""
    return _richness(row), _SOURCE_PRIORITY.get(str(row.get("source_id")), 0)


def _unique_sale_event_ids(table: pa.Table) -> pa.Table:
    """重建全局唯一 sale_event_id（跨快照行号撞号防护，回放排除目标依赖唯一 ID）。

    ``sale_event_id = <snapshot_id>-line<raw_locator>``：快照 ID 含来源/数据集/
    时间戳，天然区分不同快照的相同行号。
    """
    snapshot_ids = table.column("snapshot_id").to_pylist()
    locators = table.column("raw_locator").to_pylist()
    ids = [f"{sid}-line{loc}" for sid, loc in zip(snapshot_ids, locators, strict=True)]
    idx = table.schema.get_field_index("sale_event_id")
    return table.set_column(
        idx, "sale_event_id", pa.array(ids, type=pa.string())
    )


def cross_source_dedup(sale_table: pa.Table) -> tuple[pa.Table, int]:
    """跨源/跨快照同一交易身份去重（合同非阻断假设 #2）。

    仅 NORMAL 记录可作为保留对象；同身份键多录时保留
    :func:`_rank` 最高的一条，其余标 ``SUSPECT_DUPLICATE`` 并附跨源注记。
    返回 (去重后表, 新增跨源去重条数)。
    """
    rows = sale_table.to_pylist()
    best: dict[str, int] = {}
    for i, row in enumerate(rows):
        if row.get("anomaly_flag") != "正常":
            continue  # 非正常记录不作为保留对象（本身不进正式池）
        key = _identity_key(row)
        if key is None:
            continue
        if key in best:
            j = best[key]
            if _rank(row) > _rank(rows[j]):
                best[key] = i
        else:
            best[key] = i
    keep: set[int] = set(best.values())
    flagged = 0
    out: list[dict[str, object]] = []
    for i, row in enumerate(rows):
        key = _identity_key(row)
        if row.get("anomaly_flag") == "正常" and key is not None and i not in keep:
            flagged += 1
            out.append(
                {
                    **row,
                    "anomaly_flag": "疑似重复",
                    "flag_note": (
                        "跨源/多录同一交易身份（community_id+面积+成交日+总价），"
                        "保留字段更丰富/来源优先级更高的一条，去除本条"
                    ),
                }
            )
        else:
            out.append(row)
    return Table_from_rows(out, sale_table.schema), flagged


def lianjia_extended_lookup(lookup: CommunityIdLookup) -> CommunityIdLookup:
    """把链家成交社区注册表合并进回填查找表（仅补条目，不覆盖既有）。

    既有 community / community_alias 权威表命中优先；注册表只提供权威表
    未覆盖的链家小区名 → 标准 ID（溯源理由 = 链家补数探测 §2/§3 + 候选
    名录）。返回新的 :class:`CommunityIdLookup`，不修改入参（实体表只读）。
    """
    if not LIANJIA_COMMUNITY_REGISTRY:
        return lookup
    canonical = dict(lookup.canonical)
    reason = "链家成交社区注册表（链家补数探测-V0.1 §2/§3 + 候选小区名录）"
    for name, community_id in LIANJIA_COMMUNITY_REGISTRY.items():
        if name not in canonical:
            canonical[name] = (community_id, reason)
    return CommunityIdLookup(
        canonical=canonical,
        alias_consistent=dict(lookup.alias_consistent),
        blocked=dict(lookup.blocked),
    )


def backfill_lianjia_layouts(sale_table: pa.Table) -> tuple[pa.Table, int]:
    """跨源户型回填：链家成交 layout 回填被跨源去重标记的房天下 UNKNOWN 行。

    被标 ``疑似重复`` 且 ``layout == "UNKNOWN"`` 的行，若身份键
    （community_id + 面积 + 成交日 + 总价）匹配到保留的正常链家行且其
    layout 已知，则以链家 layout 回填，并在 ``flag_note`` 追加溯源注记。
    回填只发生在跨源去重确认同一交易身份之后，不臆测、不静默合并。
    返回 (回填后表, 回填条数)。
    """
    rows = sale_table.to_pylist()
    canonical: dict[str, str] = {}
    for row in rows:
        if row.get("anomaly_flag") != "正常":
            continue
        layout = row.get("layout")
        if not _is_known(layout):
            continue
        key = _identity_key(row)
        if key is None:
            continue
        if key not in canonical:
            canonical[key] = str(layout)
    backfilled = 0
    out: list[dict[str, object]] = []
    for row in rows:
        if row.get("anomaly_flag") == "疑似重复" and str(row.get("layout")) == "UNKNOWN":
            key = _identity_key(row)
            if key in canonical:
                backfilled += 1
                out.append(
                    {
                        **row,
                        "layout": canonical[key],
                        "flag_note": (
                            f"{row.get('flag_note') or ''}；"
                            "户型由链家成交记录跨源回填（身份键匹配）"
                        ),
                    }
                )
            else:
                out.append(row)
        else:
            out.append(row)
    return Table_from_rows(out, sale_table.schema), backfilled


def Table_from_rows(rows: list[dict[str, object]], schema: pa.Schema) -> pa.Table:
    """按固定 schema 从 dict 行重建表（类型校验，防漂移）。"""
    batch = pa.RecordBatch.from_pylist(rows, schema=schema)
    return pa.Table.from_batches([batch])


def _summary_from_table(table: pa.Table) -> CleaningSummary:
    """从合并后 sale_event 表重算清洗摘要（跨源去重计入 duplicate）。"""
    flags = table.column("anomaly_flag").to_pylist()
    return CleaningSummary(
        total=len(flags),
        parking_flagged=sum(1 for f in flags if f == "疑似车位"),
        duplicate_flagged=sum(1 for f in flags if f == "疑似重复"),
        abnormal_unit_price_flagged=sum(1 for f in flags if f == "疑似异常单价"),
        formal_pool=sum(1 for f in flags if f == "正常"),
    )


def build_combined_marts(*, data_dir: Path) -> CombinedMartResult:
    """多源 marts 合并构建主入口（G3R-C）。

    从 catalog 合并快照 → 逐来源重建记录/清洗/建表 → 合并 sale 表 → 唯一
    事件 ID → 跨源去重 → 写 marts + 质量报告。无参与合并快照 → ``ValueError``
    （缺表语义由 CLI 层映射为退出码）。
    """
    refs = merge_snapshots(data_dir)
    if not refs:
        raise ValueError(
            "无可合并快照（需要 链家 chengjiao_list/chengjiao 与/或 房天下 chengjiao）"
        )

    # WP5-E 回填查找表 + LJ-E 链家成交社区注册表扩展（权威表命中优先）
    community_lookup: CommunityIdLookup = lianjia_extended_lookup(
        load_community_lookup(data_dir=data_dir)
    )

    # ext 第四来源（ext-sale-ingest-scope-v1-2）：current 指针 run 的普通住宅
    # 表，只读；可解析行经同一 WP4-C 清洗与跨源去重入池，未解析行计数留痕。
    ext_run = read_current_ext_run(data_dir)

    sale_tables: list[pa.Table] = []
    listing_tables: list[pa.Table] = []
    all_records: list[SaleRecord] = []
    all_cleaned: list[CleanedSale] = []
    inputs: list[InputRef] = []

    for ref in refs:
        records = reconstruct_records(ref)
        cleaned, _summary = clean_sales(records)
        source_id = source_id_of(ref)
        snapshot_id = snapshot_id_of(ref)
        fetched_at = ref.manifest().fetched_at
        inputs.append(InputRef(dataset=ref.dataset, fetched_at=fetched_at.isoformat()))

        sale_tables.append(
            sale_event_table(
                cleaned,
                source_id=source_id,
                snapshot_id=snapshot_id,
                fetched_at=fetched_at,
                community_lookup=community_lookup,
            )
        )
        all_records.extend(records)
        all_cleaned.extend(cleaned)
        if ref.source == "lianjia":
            # 该分支仅链家记录（房天下无挂牌证据，不进入挂牌派生）；
            # 链家行重建结果全部为 LianjiaRecord，窄化类型以满足挂牌适配器。
            assert all(isinstance(r, LianjiaRecord) for r in records)
            listing_tables.append(
                listing_event_table(
                    [r for r in records if isinstance(r, LianjiaRecord)],
                    source_id=source_id,
                    snapshot_id=snapshot_id,
                    fetched_at=fetched_at,
                    community_lookup=community_lookup,
                )
            )

    ext_kept_count = 0
    ext_unmatched_counter: Counter[str] = Counter()
    ext_input_count = 0
    if ext_run is not None:
        inputs.append(
            InputRef(
                dataset=EXT_DATASET,
                fetched_at=ext_run.run_id,
                content_hash=ext_run.ordinary_sha256,
            )
        )
        ext_records = ext_rows_to_records(ext_run.table)
        ext_input_count = len(ext_records)
        kept, ext_unmatched_counter = split_resolvable(ext_records, community_lookup)
        cleaned_ext, _summary_ext = clean_sales(kept)
        sale_tables.append(
            sale_event_table(
                cleaned_ext,
                source_id=EXT_SOURCE_ID,
                snapshot_id=f"lianjia_ext-{EXT_DATASET}-{ext_run.run_id}",
                fetched_at=run_fetched_at(ext_run.run_id),
                community_lookup=community_lookup,
            )
        )
        # 挂牌派生不含 ext（本 change 范围仅成交入池；挂牌事件链路不动）。
        ext_kept_count = len(kept)
        all_records.extend(ext_records)
        all_cleaned.extend(cleaned_ext)

    merged_sale = pa.concat_tables(sale_tables)
    merged_sale = _unique_sale_event_ids(merged_sale)
    merged_sale, cross_dups = cross_source_dedup(merged_sale)
    # LJ-E 跨源户型回填：链家 layout 回填被标记的房天下 UNKNOWN 行
    layout_unknown_before = sum(
        1 for v in merged_sale.column("layout").to_pylist() if not _is_known(v)
    )
    merged_sale, layout_backfilled = backfill_lianjia_layouts(merged_sale)
    layout_unknown_after = sum(
        1 for v in merged_sale.column("layout").to_pylist() if not _is_known(v)
    )
    summary = _summary_from_table(merged_sale)

    listing_table = (
        pa.concat_tables(listing_tables) if listing_tables else _empty_listing_table()
    )

    vs = valid_sale_table(merged_sale)
    vl = valid_listing_table(listing_table)
    # 属性扩列回接（ext-sale-ingest-scope-v1-2）：现行正式 valid_sale 已含
    # staged v2 扩列（excel-attribute-enrichment 转正基线）；合并重建若不回接
    # 将静默回退属性列。此处从同一 ext run 重建属性索引并按身份键回填
    # （ext 行自身属性亦由同 run 命中），统计进 manifest notes 可对拍。
    # 函数内导入：attribute_enrich 顶层反向依赖本模块（避免循环导入）。
    attribute_matched = 0
    if ext_run is not None:
        from compsval.ingest.attribute_enrich import (
            build_attribute_index,
            enrich_valid_sale,
        )

        index, _conflict_keys, _unmapped = build_attribute_index(
            ext_run.table, community_lookup
        )
        vs, enrich_stats = enrich_valid_sale(vs, index, source_run_id=ext_run.run_id)
        attribute_matched = enrich_stats.matched
    notes = (
        "G3R-C/LJ-E combined marts @ "
        + ",".join(ref.fetched_at for ref in refs)
        + f" | cross_source_duplicates={cross_dups} layout_backfilled={layout_backfilled}"
        f" layout_unknown={layout_unknown_before}->{layout_unknown_after}"
    )
    if ext_run is not None:
        notes += (
            f" | ext_run={ext_run.run_id}"
            f" ext_input={ext_input_count}"
            f" ext_kept={ext_kept_count}"
            f" ext_unmatched_rows={sum(ext_unmatched_counter.values())}"
            f" ext_unmatched_names={len(ext_unmatched_counter)}"
            f" attribute_matched={attribute_matched}"
        )
    valid_sale_path = write_valid_sale_mart(vs, data_dir=data_dir, inputs=inputs, notes=notes)
    valid_listing_path = write_valid_listing_mart(
        vl, data_dir=data_dir, inputs=inputs, notes=notes
    )

    # 未匹配/低置信小区登记（跨源回填后仍无法命中标准 ID 的，不静默归并）。
    # ext 未解析行未入合并表，其代表性源名在此并入同一登记（LJ-E 如实标记）。
    unmatched = collect_unmatched_conflicts(
        merged_sale.column("community").to_pylist(), community_lookup
    )
    if ext_unmatched_counter:
        unmatched = sorted(set(unmatched) | set(ext_unmatched_counter))
    fetched_ats = [ref.manifest().fetched_at for ref in refs]
    if ext_run is not None:
        fetched_ats.append(run_fetched_at(ext_run.run_id))
    report = build_quality_report(
        all_records,
        all_cleaned,
        summary,
        merged_sale,
        listing_table,
        snapshot_id="combined:" + ",".join(ref.fetched_at for ref in refs),
        source_id="SRC-005+SRC-007",
        fetched_at=max(fetched_ats),
        unmatched_conflicts=unmatched,
    )
    quality_md, quality_json = write_quality_report(
        report, data_dir=data_dir, notes=notes
    )

    return CombinedMartResult(
        snapshot_ids=tuple(ref.fetched_at for ref in refs),
        valid_sale_path=valid_sale_path,
        valid_listing_path=valid_listing_path,
        quality_md=quality_md,
        quality_json=quality_json,
        summary=summary,
        cross_source_duplicates=cross_dups,
        layout_backfilled=layout_backfilled,
        sale_table=merged_sale,
        listing_table=listing_table,
        ext_run_id=ext_run.run_id if ext_run is not None else None,
        ext_input_rows=ext_input_count,
        ext_kept_rows=ext_kept_count,
        ext_unmatched_rows=sum(ext_unmatched_counter.values()),
        ext_unmatched_names=len(ext_unmatched_counter),
        attribute_matched_rows=attribute_matched,
    )


def _empty_listing_table() -> pa.Table:
    """无挂牌来源时的空 valid_listing 输入（保持 schema）。"""
    schema = _listing_event_schema()
    columns = {
        name: pa.array([], type=field.type)
        for name, field in zip(schema.names, schema.types, strict=True)
    }
    return pa.table(columns, schema=schema)


__all__ = [
    "CombinedMartResult",
    "LIANJIA_COMMUNITY_REGISTRY",
    "MERGED_SOURCES",
    "backfill_lianjia_layouts",
    "build_combined_marts",
    "cross_source_dedup",
    "lianjia_extended_lookup",
    "merge_snapshots",
    "reconstruct_records",
]
