"""户型图选择清单生成（EXTFP2-B，技术方案 §4.3）。

每次下载前把「本次要下载哪些普通住宅户型图 URL」固化为不可变 ``selection_manifest``：
固定选择规则与版本、数据快照/run 引用、地理范围、成交时间范围、普通住宅过滤条件、
记录数与资产数、记录 ID 清单及 SHA256 哈希、域名白名单、预计下载量/存储量/成本上限、
工作包合同引用。本模块只读 staged 表（parquet），不下载、不触网、不改动原始数据。

选择规则（EXTFP2-B-SELECT-1.0）：
1. 记录必须是 房屋用途=普通住宅 且 staged 标记 floorplan_url_status=URLS_OK；；
2. 不信任存储的 floorplan_candidate_count，重新用 floorplan_profile.parse_url_list
   安全解析 floorplan_url_list_raw，仅取 url_class==FLOORPLAN_CANDIDATE 的 URL；
3. 仅保留域名在域名白名单内的候选 URL 作为资产；白名单外候选如实记录为违规
   （manifest 统计 forbidden_domain_count），不计入资产，绝不静默丢弃；
4. URL 规范化：保留原始 url，另给 normalized_url（原始 scheme 为 http 时用 https）；
   解析并记录规范化后域名做白名单校验；
5. 幂等：相同输入重跑产出相同记录集与 record_ids_hash。

「记录 ID 清单及哈希」的落盘方式：record_ids_hash 恒为排序后 source_record_id
清单的 SHA256（§4.3 完整性锚点）；records 默认存全量 SelectionEntry 清单，
另附 record_sample 便于人工抽查。
"""

from __future__ import annotations

import hashlib
from collections.abc import Collection
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import polars as pl
from pydantic import BaseModel, Field

from compsval.ingest.floorplan_profile import (
    UrlClass,
    UrlListStatus,
    parse_url_list,
)

# 域名白名单：仪表盘与房源图 CDN（技术方案 §4.3；EXTFP2-B 仅此一个白名单域名）
DOMAIN_WHITELIST: frozenset[str] = frozenset({"ke-image.ljcdn.com"})

# 地理范围描述（示例文案，落盘供复核）
GEOSCOPE = "示例城市西部目标区/外部链家成交数据"

# 冻结的选择规则版本与正文（EXTFP2-B）
SELECTION_RULE_VERSION = "EXTFP2-B-SELECT-1.0"
SELECTION_RULE_TEXT = "\n".join(
    [
        "[EXTFP2-B-SELECT-1.0] 普通住宅户型图候选 下载选择清单规则",
        "1. 记录必须是 房屋用途=普通住宅 且 staged 表 floorplan_url_status=URLS_OK；",
        "2. 不信任存储的 floorplan_candidate_count，重新安全解析 floorplan_url_list_raw，"
        "仅取 url_class=FLOORPLAN_CANDIDATE 的候选 URL；",
        "3. 仅保留域名在域名白名单（ke-image.ljcdn.com）内的候选 URL 作为资产；"
        "白名单外候选如实记录为违规，不计入资产，绝不静默丢弃；",
        "4. URL 规范化：保留原始 url，另给 normalized_url（原 scheme 为 http 时用 https）；"
        "解析并记录域名做白名单校验；",
        "5. 幂等：相同输入重跑产出相同记录集与 record_ids_hash。",
    ]
)

# 过滤条件正文（机器可读，落盘供复核）
FILTER_CONDITION_TEXT = (
    "property_use_norm == '普通住宅' AND "
    "floorplan_url_status == 'URLS_OK' AND "
    "floorplan_candidate_count >= 1 AND "
    f"url_class == '{UrlClass.FLOORPLAN_CANDIDATE.value}' AND "
    "normalized_domain in domain_whitelist"
)

# 默认估参（技术方案 §4.3）
AVG_IMAGE_BYTES = 70 * 1024  # 70 KB/张（示例）
STORAGE_MULTIPLIER = 1.5  # 存储上限 = 预计下载量 * 1.5
BUDGET_PER_IMAGE_YUAN = 0.05  # 每张图片成本上限（元），成本上限 = 资产数 * 单价

PROFILE_ORDINARY_RESIDENTIAL_LATEST = "ordinary-residential-latest"


class SelectionEntry(BaseModel):
    """清单中的一条资产（一张候选户型图 URL，技术方案 §4.3 记录级别）。"""

    source_record_id: str
    row_number: int
    url_seq: int = Field(description="记录内 URL 序号（1 起，资产间连续递增）")
    url: str = Field(description="原始候选 URL，不改写")
    normalized_url: str = Field(description="规范化 URL（原 http 时用 https）")
    domain: str = Field(description="规范化后域名")


# 行级解析中间结构（change extfp4-verify-followups，SUGGESTION ②）：
# (row_number, source_record_id, sale_date, 原始 URL 列表文本, 白名单资产清单)
SelectionRowData = tuple[int, str | None, str | None, str, list[SelectionEntry]]


class SelectionManifest(BaseModel):
    """不可变户型图选择清单（EXTFP2-B 退出证据/下载契约入口）。"""

    selection_rule_version: str
    selection_rule_text: str
    snapshot_ref: str | None = Field(
        default=None,
        description=("来源快照 content_hash（升级为结构化 InputRef 的 content_hash）或 run 引用"),
    )
    run_id: str | None = Field(default=None, description="staged run id")
    geoscope: str = Field(description="地理范围")
    date_range_min: str | None = Field(default=None, description="成交时间范围下限（sale_date）")
    date_range_max: str | None = Field(default=None, description="成交时间范围上限（sale_date）")
    filter_condition: str = Field(description="普通住宅过滤条件文本")
    record_count: int = Field(description="记录数（含 >=1 个合法白名单资产 URL 的记录）")
    asset_count: int = Field(description="资产数（候选 URL 总数，白名单外不计）")
    forbidden_domain_count: int = Field(default=0, description="白名单外候选 URL 违规数")
    forbidden_domains: list[str] = Field(default_factory=list, description="违规域名集合")
    record_ids_hash: str = Field(description="排序后 source_record_id 清单的 SHA256（完整性锚点）")
    dedupe_record_count: int = Field(
        default=0, description="记录级去重被去重行数（dedupe_record_ids 启用时统计）"
    )
    dedupe_record_sample: list[str] = Field(
        default_factory=list, description="被去重记录 source_record_id 样例（最多 20 条）"
    )
    records: list[SelectionEntry] = Field(
        default_factory=list,
        description="全量清单（可选，条目过多时可为空）",
    )
    record_sample: list[SelectionEntry] | None = Field(
        default=None, description="清单抽样（供人工抽查）"
    )
    domain_whitelist: list[str] = Field(default_factory=list, description="域名白名单")
    estimated_download_bytes: int = Field(description="预计下载量 = 资产数 * 每张字节")
    storage_cap_bytes: int = Field(description="存储上限")
    budget_cap_yuan: float = Field(description="成本上限（元）")
    workpackage_ref: str = Field(default="EXTFP2", description="工作包合同引用")
    avg_bytes_estimate: int = Field(description="每张图片字节估参")


def _normalize_https(url: str) -> str:
    """URL 规范化：原始 scheme 为 http 时改 https，其余原样。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if parts.scheme.lower() != "http":
        return url
    return urlunsplit(("https", parts.netloc, parts.path, parts.query, parts.fragment))


def _hostname(url: str) -> str:
    """解析规范化后主机域名；不可解析时返回空串。"""
    try:
        return urlsplit(url).hostname or ""
    except ValueError:
        return ""


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def build_selection(
    parquet_path: Path,
    whitelist: Collection[str] | None = None,
    *,
    out_json: Path | None = None,
    run_id: str | None = None,
    snapshot_ref: str | None = None,
    geoscope: str = GEOSCOPE,
    avg_bytes: int = AVG_IMAGE_BYTES,
    storage_multiplier: float = STORAGE_MULTIPLIER,
    unit_budget_yuan: float = BUDGET_PER_IMAGE_YUAN,
    record_sample_size: int = 20,
    include_records: bool = True,
    selection_rule_version: str = SELECTION_RULE_VERSION,
    selection_rule_text: str = SELECTION_RULE_TEXT,
    filter_condition_text: str = FILTER_CONDITION_TEXT,
    community_names: Collection[str] | None = None,
    date_min: str | None = None,
    date_max: str | None = None,
    workpackage_ref: str = "EXTFP2",
    dedupe_record_ids: bool = False,
) -> SelectionManifest:
    """从 staged 普通住宅 parquet 生成不可变户型图选择清单。

    - 过滤普通住宅 + URLS_OK + candidate_count>=1；
    - 逐行用 floorplan_profile.parse_url_list 重建候选 URL（重新解析，不信计数）；
    - 规范化 URL + 域名白名单校验（白名单外记为违规，不入资产）；
    - 计算 record_count / asset_count / record_ids_hash / 下载与存储及成本上限。
    - ``dedupe_record_ids``（EXTFP4-verify-followups，SUGGESTION ②）：启用时按
      ``source_record_id`` 记录级去重——同 id 多行只保留一行（保留行 = 候选行中
      ``row_number`` 最小者，``row_number`` 重复时按原始 URL 列表字典序兜底），
      被去重行数与样例写入 manifest；默认 ``False`` 与 EXTFP2-B 行为完全一致。
    - 幂等：不引用磁盘顺序以外的不稳定输入，相同输入产出相同结果。

    ``whitelist`` 默认取 DOMAIN_WHITELIST。out_json 非空时以 ``*.incomplete``
    原子替换写 UTF-8 JSON。
    """
    if not parquet_path.is_file():
        raise FileNotFoundError(f"staged parquet not found: {parquet_path}")

    allowed = frozenset(whitelist) if whitelist is not None else DOMAIN_WHITELIST

    if snapshot_ref is None:
        # 尝试从旁路血缘 manifest 读取结构化来源 content_hash
        try:
            from compsval.ingest.manifests import read_derived_manifest

            dm = read_derived_manifest(parquet_path)
            if dm.inputs and dm.inputs[0].content_hash:
                snapshot_ref = dm.inputs[0].content_hash
        except Exception:  # noqa: BLE001 - 血缘缺失时降级为 run 引用
            snapshot_ref = None

    resolved_run_id = run_id
    if resolved_run_id is None and parquet_path.parent.name.startswith("run_"):
        resolved_run_id = parquet_path.parent.name[len("run_") :]

    frame = pl.read_parquet(parquet_path)
    selected = frame.filter(
        (pl.col("property_use_norm") == "普通住宅")
        & (pl.col("floorplan_url_status") == "URLS_OK")
        & (pl.col("floorplan_candidate_count") >= 1)
    )
    # EXTFP4 生产 profile 可选过滤（默认 None 时与全量清单行为完全一致）：
    # 小区过滤按 staged 表 community_name 精确匹配；时间过滤按 sale_date（ISO 文本序）。
    if community_names is not None:
        if "community_name" not in frame.columns:
            raise ValueError("staged parquet 缺少 community_name 列，无法按目标小区过滤")
        selected = selected.filter(pl.col("community_name").is_in(sorted(community_names)))
    if date_min is not None:
        selected = selected.filter(pl.col("sale_date") >= date_min)
    if date_max is not None:
        selected = selected.filter(pl.col("sale_date") <= date_max)
    rows = selected.select(
        [
            pl.col("row_number"),
            pl.col("source_record_id"),
            pl.col("sale_date"),
            pl.col("floorplan_url_list_raw"),
        ]
    ).iter_rows()

    # 逐行解析为「候选资产 + 元信息」；白名单域违规计数按行统计，不因去重改变
    candidate_rows: list[SelectionRowData] = []
    forbidden_domain_count = 0
    forbidden_domain_set: set[str] = set()
    for row_number, source_record_id, sale_date, raw in rows:
        status, items = parse_url_list(raw)
        if status is not UrlListStatus.URLS_OK:
            continue
        candidates = [it for it in items if it.url_class is UrlClass.FLOORPLAN_CANDIDATE]

        record_assets: list[SelectionEntry] = []
        for it in candidates:
            normalized = _normalize_https(it.url)
            domain = _hostname(normalized)
            if domain not in allowed:
                forbidden_domain_count += 1
                forbidden_domain_set.add(domain)
                continue
            record_assets.append(
                SelectionEntry(
                    source_record_id=_coerce_str(source_record_id) or "",
                    row_number=int(row_number or 0),
                    url_seq=0,  # 占位，后续重新编号
                    url=it.url,
                    normalized_url=normalized,
                    domain=domain,
                )
            )

        if not record_assets:
            continue
        candidate_rows.append(
            (
                int(row_number or 0),
                _coerce_str(source_record_id),
                _coerce_str(sale_date),
                str(raw or ""),
                record_assets,
            )
        )

    # 记录级去重（EXTFP4-verify-followups，SUGGESTION ②）：同 source_record_id
    # 多行只保留一行（确定性规则见 design D3）；默认 False 时与 EXTFP2-B 完全一致。
    kept_rows: list[SelectionRowData] = []
    dedupe_record_count = 0
    dedupe_record_sample: list[str] = []
    if dedupe_record_ids:
        groups: dict[str, list[SelectionRowData]] = {}
        for item in candidate_rows:
            sid = item[1]
            if sid is None:  # 无 source_record_id 的行不参与去重
                kept_rows.append(item)
                continue
            groups.setdefault(sid, []).append(item)
        for sid, dup_items in groups.items():
            if len(dup_items) == 1:
                kept_rows.append(dup_items[0])
                continue
            # 保留行 = 候选行中 row_number 最小者；row_number 重复时按原始 URL 列表字典序兜底
            kept_rows.append(min(dup_items, key=lambda it: (it[0], it[3])))
            dedupe_record_count += len(dup_items) - 1
            if len(dedupe_record_sample) < 20 and sid not in dedupe_record_sample:
                dedupe_record_sample.append(sid)
    else:
        kept_rows = candidate_rows

    all_entries: list[SelectionEntry] = []
    record_ids: list[str] = []
    date_values: list[str] = []
    record_count = 0
    asset_count = 0
    for _row_number, sid, sale_date, _raw, record_assets in kept_rows:
        # 记录内 URL 序号 1 起连续递增（保留行内重新编号）
        for seq, entry in enumerate(record_assets, start=1):
            all_entries.append(entry.model_copy(update={"url_seq": seq}))

        if sid is not None:
            record_ids.append(sid)
        record_count += 1
        asset_count += len(record_assets)
        if sale_date is not None:
            date_values.append(sale_date)

    # 确定性排序（幂等）：按 row_number 升序，记录内按 url_seq
    all_entries.sort(key=lambda e: (e.row_number, e.url_seq))

    date_range_min = min(date_values) if date_values else None
    date_range_max = max(date_values) if date_values else None

    digest = hashlib.sha256()
    digest.update("\n".join(sorted(record_ids)).encode("utf-8"))
    record_ids_hash = digest.hexdigest()

    estimated_download_bytes = asset_count * avg_bytes
    storage_cap_bytes = int(estimated_download_bytes * storage_multiplier)
    budget_cap_yuan = round(asset_count * unit_budget_yuan, 2)

    manifest = SelectionManifest(
        selection_rule_version=selection_rule_version,
        selection_rule_text=selection_rule_text,
        snapshot_ref=snapshot_ref,
        run_id=resolved_run_id,
        geoscope=geoscope,
        date_range_min=date_range_min,
        date_range_max=date_range_max,
        filter_condition=filter_condition_text,
        record_count=record_count,
        asset_count=asset_count,
        forbidden_domain_count=forbidden_domain_count,
        forbidden_domains=sorted(forbidden_domain_set),
        record_ids_hash=record_ids_hash,
        dedupe_record_count=dedupe_record_count,
        dedupe_record_sample=dedupe_record_sample,
        records=all_entries if include_records else [],
        record_sample=all_entries[:record_sample_size] or None,
        domain_whitelist=sorted(allowed),
        estimated_download_bytes=estimated_download_bytes,
        storage_cap_bytes=storage_cap_bytes,
        budget_cap_yuan=budget_cap_yuan,
        workpackage_ref=workpackage_ref,
        avg_bytes_estimate=avg_bytes,
    )

    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        work.replace(out_json)

    return manifest


__all__ = [
    "AVG_IMAGE_BYTES",
    "BUDGET_PER_IMAGE_YUAN",
    "DOMAIN_WHITELIST",
    "FILTER_CONDITION_TEXT",
    "GEOSCOPE",
    "PROFILE_ORDINARY_RESIDENTIAL_LATEST",
    "SELECTION_RULE_TEXT",
    "SELECTION_RULE_VERSION",
    "STORAGE_MULTIPLIER",
    "SelectionEntry",
    "SelectionManifest",
    "build_selection",
]
