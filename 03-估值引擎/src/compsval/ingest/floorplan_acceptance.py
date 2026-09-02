"""300 张分层验收集：确定性抽样清单 + 黄金标签框架（EXTFP3-G，技术方案 §10.2/§11.4）。

EXTFP3-G 把「300 张验收」的选择清单与黄金标签框架固化为可复现证据：

- 从 staged 普通住宅表按 §10.2 维度确定性分层抽样：区县、成交年份、居室数三组
  （下载前唯一可确定且具差异的维度）；随机种子、数据快照、抽样规则/SQL 与样本清单
  全部冻结；
- 抽样结果输出为与 ``SelectionManifest`` 兼容的子类清单，可被 ``compsval floorplan download``
  直接消费；
- 黄金标签：生成人工标注 CSV 模板（用户对原图标注文字/房间名/面积，生产 OCR 不参与
  生成标准答案），并提供标注结果校验函数保存标签证据。

抽样设计（EXTFP3-G-SAMPLE-1.0）：
- 分层维度：区县组（11 个正式区 + 其他）、成交年份组（2016-2018/2019-2021/2022-2024/
  2025-2026）、居室组（1/2/3/4/5+）；下载前可确定性确定，保证「一居至四居及更复杂
  户型」与「区县和年份」覆盖；
- 分配：按单元（区县×年份×居室）人口比例分配，非空单元保底 1 张（保证全覆盖），
  其余按最大余数法补足到目标 300；「其他」区组仅按比例不保底（避免抽样到南海区等
  噪音/短名记录）；
- 单元内选择：对候选记录按 row_number 排序后，用「种子 + 单元键」的稳定 SHA256 派生
  随机种子洗牌，取该单元配额——重跑完全一致；
- 冻结锚点：record_ids_hash = 排序后 source_record_id 清单的 SHA256（与选择清单同约定）；
  幂等：相同输入重跑产出相同记录集与哈希。

范围边界（EXTFP3-G 合同）：本模块不下载、不触网、不调 OCR、不改原始数据；只读
staged 普通住宅 parquet 与全量 selection_manifest；真实 300 张下载由 ``compsval floorplan
download`` + ``compsval floorplan asset`` 在显式 CLI 运行中完成。
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl
from pydantic import BaseModel, Field

from compsval.ingest.floorplan_profile import (
    UrlClass,
    UrlListStatus,
    parse_url_list,
)
from compsval.ingest.floorplan_selection import (
    DOMAIN_WHITELIST,
    GEOSCOPE,
    SelectionEntry,
    SelectionManifest,
    _coerce_str,
    _normalize_https,
)
from compsval.ingest.floorplan_transcribe import classify_room

# 抽样规则版本与正文（EXTFP3-G）
SAMPLING_RULE_VERSION = "EXTFP3-G-SAMPLE-1.0"

# 默认随机种子（冻结；重跑必须显式使用相同种子，否则视为新样本）
DEFAULT_SAMPLE_SEED = 20260825
DEFAULT_TARGET_SIZE = 300

# 正式示例城市 11 区（用于区县组规范；短名自动补「区」）
DISTRICT_NAMES: frozenset[str] = frozenset(
    {
        "中心区",
        "目标区",
        "邻丁区",
        "邻甲区",
        "邻戊区",
        "邻己区",
        "邻庚区",
        "邻乙区",
        "邻丙区",
        "邻辛区",
        "邻壬区",
    }
)

# 成交年份组桶（技术方案 §10.2「年份」）
YEAR_BUCKETS: list[tuple[str, int, int]] = [
    ("2016-2018", 2016, 2018),
    ("2019-2021", 2019, 2021),
    ("2022-2024", 2022, 2024),
    ("2025-2026", 2025, 2026),
]

# 居室组桶（技术方案 §10.2「一居至四居及更复杂户型」）
BEDROOM_BUCKETS: list[str] = ["1", "2", "3", "4", "5+"]

# 「其他」区组：噪音/短名/范围外记录（南海区=佛山、子区域名、无法处理等）
OTHER_DISTRICT = "其他"

SAMPLING_RULE_TEXT = "\n".join(
    [
        f"[{SAMPLING_RULE_VERSION}] 300 张分层验收抽样清单规则",
        "1. 记录池 = 普通住宅 AND floorplan_url_status=URLS_OK AND candidate_count>=1"
        " 且重解析含白名单候选 URL（与 EXTFP2-B 选择规则一致）；",
        "2. 分层维度：区县组（11 个正式区 + 其他）、成交年份组（2016-2018/2019-2021/"
        "2022-2024/2025-2026）、居室组（1/2/3/4/5+）；",
        "3. 分配：按单元人口比例，非空单元保底 1 张（其他区组仅按比例不保底），"
        "最大余数法补足到目标 300 记录；",
        "4. 单元内选择：记录按 row_number 排序，用 种子+单元键 稳定 SHA256 派生随机"
        "种子洗牌，取单元配额；",
        "5. 同一 source_record_id 在 staged 多行（Excel 重复成交记录）只保留首个"
        "（row_number 最小），防同记录多占样本位导致资产重复；",
        "6. 幂等：相同输入（parquet 快照 + 种子 + 规则版本）重跑产出相同记录集与 record_ids_hash；",
        "7. 黄金标签由人工对原图建立，生产 OCR 不参与生成自己的标准答案；"
        "黄金标签只覆盖文字/位置/房间名/面积，不评价户型质量。",
    ]
)


def district_group(value: str | None) -> str:
    """区县组：正式 11 区精确匹配；短名补「区」后匹配；其余归「其他」。

    南海区（佛山）与子区域名/「无法处理」等噪音归入「其他」，仅按比例分配不保底。
    """
    v = (value or "").strip()
    if v in DISTRICT_NAMES:
        return v
    if v + "区" in DISTRICT_NAMES:
        return v + "区"
    return OTHER_DISTRICT


def year_bucket(sale_date: str | None) -> str:
    """成交年份组：由 sale_date 前 4 位决定；无法解析归到最早的 2016-2018 桶。"""
    try:
        year = int(str(sale_date or "")[:4])
    except (TypeError, ValueError):
        year = 0
    for name, lo, hi in YEAR_BUCKETS:
        if lo <= year <= hi:
            return name
    return "2016-2018"


def bedroom_bucket(bedrooms_raw: str | None) -> str:
    """居室组：1/2/3/4/5+；无法解析归 5+（复杂户型桶，避免漏掉异常值）。"""
    try:
        n = int(str(bedrooms_raw or "").strip())
    except (TypeError, ValueError):
        return "5+"
    if n <= 1:
        return "1"
    if n == 2:
        return "2"
    if n == 3:
        return "3"
    if n == 4:
        return "4"
    return "5+"


def _stable_seed(seed: int, *parts: str) -> int:
    """稳定随机种子：SHA256(seed|part...) 派生，跨进程一致（不用内置 hash()）。"""
    content = "|".join([str(seed), *parts])
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 分层分配（最大余数法：保底 + 按比例 + 尾数分配）
# ---------------------------------------------------------------------------


def allocate_quotas(
    cell_counts: dict[tuple[str, str, str], int],
    target: int,
    *,
    floor: int = 1,
    floor_skip_district: str = OTHER_DISTRICT,
) -> dict[tuple[str, str, str], int]:
    """按单元人口比例分配配额，非空单元保底 ``floor``（指定区组跳过保底）。

    - 保底：非空且区组 != floor_skip_district 的单元至少 floor；
    - 按比例：剩余配额按人口占比分配（最大余数法补整）；
    - 封顶：任何单元配额不超过其人口。
    """
    if target < 1:
        raise ValueError("target 必须 >= 1")
    if not cell_counts:
        raise ValueError("无候选单元，无法分配")
    total_pop = sum(cell_counts.values())
    if total_pop < 1:
        raise ValueError("候选总人口为 0")

    # 保底
    base: dict[tuple[str, str, str], int] = {}
    for cell, pop in cell_counts.items():
        if cell[0] == floor_skip_district:
            base[cell] = 0
        else:
            base[cell] = floor if pop >= floor else pop
    base_total = sum(base.values())
    if base_total > target:
        raise ValueError(
            f"保底配额 {base_total} 已超过目标 {target}（单元数过多，请降低保底或增大 target）"
        )
    remain = target - base_total

    # 按比例 + 最大余数法
    quotas: dict[tuple[str, str, str], int] = {c: q for c, q in base.items()}
    proportional: dict[tuple[str, str, str], float] = {
        c: remain * pop / total_pop for c, pop in cell_counts.items()
    }
    exact = {c: int(frac) for c, frac in proportional.items()}
    used = sum(exact.values())
    # 尾数从大到小补齐，配额不超人口
    leftovers = sorted(
        (
            (proportional[c] - exact[c], c)
            for c in cell_counts
            if quotas[c] + exact[c] < cell_counts[c]
        ),
        key=lambda t: (-t[0], t[1]),
    )
    idx = 0
    while used + base_total < target and idx < len(leftovers):
        frac, cell = leftovers[idx]
        if quotas[cell] + exact[cell] < cell_counts[cell]:
            exact[cell] += 1
            used += 1
        idx += 1

    for cell in cell_counts:
        quotas[cell] += exact[cell]
        quotas[cell] = min(quotas[cell], cell_counts[cell])
    return quotas


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


class AcceptanceSelectionManifest(SelectionManifest):
    """300 张验收选择清单（EXTFP3-G 派生证据；SelectionManifest 兼容子类）。

    继承 ``SelectionManifest`` 的全部字段，可被 ``compsval floorplan download`` 直接消费
    （pydantic 默认忽略多余字段，下载器只读取父类字段）。附加字段冻结抽样契约。
    """

    sampling_rule_version: str = SAMPLING_RULE_VERSION
    sampling_rule_text: str = Field(default_factory=lambda: SAMPLING_RULE_TEXT)
    random_seed: int = DEFAULT_SAMPLE_SEED
    target_size: int = DEFAULT_TARGET_SIZE
    source_selection_ref: str | None = Field(
        default=None, description="父级全量 selection_manifest 的 record_ids_hash"
    )
    strata_dimensions: list[str] = Field(
        default_factory=lambda: ["district_group", "year_bucket", "bedroom_bucket"]
    )
    strata_buckets: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "district_group": sorted(DISTRICT_NAMES) + [OTHER_DISTRICT],
            "year_bucket": [b[0] for b in YEAR_BUCKETS],
            "bedroom_bucket": BEDROOM_BUCKETS,
        }
    )
    allocation_table: list[dict[str, Any]] = Field(
        default_factory=list,
        description="单元分配表：[{district_group,year_bucket,bedroom_bucket,population,quota}]",
    )
    trimmed_extra_assets: int = Field(default=0, description="多 URL 记录裁剪掉的额外资产数")
    duplicate_sid_rows: int = Field(
        default=0, description="同一 source_record_id 多行（Excel 重复成交记录）被去重的行数"
    )
    created_at: str = ""


# ---------------------------------------------------------------------------
# 抽样主体
# ---------------------------------------------------------------------------


def _hostname(url: str) -> str:
    try:
        return urlsplit(url).hostname or ""
    except ValueError:
        return ""


def build_acceptance_sample(
    parquet_path: Path,
    *,
    target: int = DEFAULT_TARGET_SIZE,
    seed: int = DEFAULT_SAMPLE_SEED,
    out_json: Path | None = None,
    whitelist: frozenset[str] | None = None,
    source_manifest_path: Path | None = None,
    geoscope: str = GEOSCOPE,
    avg_bytes: int = 70 * 1024,
    storage_multiplier: float = 1.5,
    unit_budget_yuan: float = 0.05,
) -> AcceptanceSelectionManifest:
    """从 staged 普通住宅 parquet 生成 300 张确定性分层抽样清单（EXTFP3-G）。

    - 记录池过滤与资产解析复用 EXTFP2-B 规则（普通住宅 + URLS_OK + 白名单候选）；
    - 按 (区县组, 年份组, 居室组) 分层，保底 + 最大余数法分配目标配额；
    - 单元内稳定洗牌（种子+单元键）取配额；多 URL 记录超出目标时确定性裁剪；
    - 全部锚点（种子/快照/抽样规则/分配表/记录哈希）冻结入 manifest。

    幂等：相同输入（parquet 快照 + seed + 规则版本）重跑产出相同记录集与
    ``record_ids_hash``。
    """
    if not parquet_path.is_file():
        raise FileNotFoundError(f"staged parquet not found: {parquet_path}")
    if target < 1:
        raise ValueError("target 必须 >= 1")

    allowed = whitelist if whitelist is not None else DOMAIN_WHITELIST

    # 来源快照 content_hash（与 build_selection 同一旁路血缘约定）
    snapshot_ref: str | None = None
    try:
        from compsval.ingest.manifests import read_derived_manifest

        dm = read_derived_manifest(parquet_path)
        if dm.inputs and dm.inputs[0].content_hash:
            snapshot_ref = dm.inputs[0].content_hash
    except Exception:  # noqa: BLE001 - 血缘缺失时降级为 run 引用
        snapshot_ref = None

    resolved_run_id: str | None = None
    if parquet_path.parent.name.startswith("run_"):
        resolved_run_id = parquet_path.parent.name[len("run_") :]

    # 父级全量选择清单引用
    source_selection_ref: str | None = None
    if source_manifest_path is not None and source_manifest_path.is_file():
        try:
            parent = SelectionManifest.model_validate(
                json.loads(source_manifest_path.read_text(encoding="utf-8"))
            )
            source_selection_ref = parent.record_ids_hash
        except Exception:  # noqa: BLE001 - 父清单缺失/损坏时降级为 None
            source_selection_ref = None

    frame = pl.read_parquet(parquet_path)
    selected = frame.filter(
        (pl.col("property_use_norm") == "普通住宅")
        & (pl.col("floorplan_url_status") == "URLS_OK")
        & (pl.col("floorplan_candidate_count") >= 1)
    )
    rows = selected.select(
        [
            pl.col("row_number"),
            pl.col("source_record_id"),
            pl.col("sale_date"),
            pl.col("bedrooms_raw"),
            pl.col("extra_fields_json"),
            pl.col("floorplan_url_list_raw"),
        ]
    ).iter_rows()

    # 记录 → (细胞键, 资产清单)
    cells: dict[tuple[str, str, str], list[SelectionEntry]] = {}
    record_cell: dict[str, tuple[str, str, str]] = {}  # source_record_id -> cell
    cell_records: dict[tuple[str, str, str], list[tuple[int, str]]] = {}
    date_values: list[str] = []
    forbidden_domain_count = 0
    forbidden_domain_set: set[str] = set()
    duplicate_sid_rows = 0  # 同一 source_record_id 多行（Excel 重复成交记录）→ 只保留首个

    def _get_district(s: str | None) -> str:
        if not s:
            return ""
        try:
            d = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return ""
        if not isinstance(d, dict):
            return ""
        for key in ("区县", "district", "区域"):
            v = d.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    for row_number, source_record_id, sale_date, bedrooms_raw, extra_json, raw in rows:
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
                    url_seq=0,  # 占位，后续编号
                    url=it.url,
                    normalized_url=normalized,
                    domain=domain,
                )
            )
        if not record_assets:
            continue
        for seq, entry in enumerate(record_assets, start=1):
            entry.url_seq = seq

        sid = _coerce_str(source_record_id) or ""
        if sid in record_cell:
            # 同一成交记录在 staged 多行出现（Excel 重复行）：只保留首个（row_number 最小，
            # 遍历按 row_number 升序即首个），防同记录多占样本位导致资产重复、裁剪失效。
            duplicate_sid_rows += 1
            continue
        dg = district_group(_get_district(extra_json))
        yb = year_bucket(sale_date)
        bb = bedroom_bucket(bedrooms_raw)
        cell = (dg, yb, bb)
        cells.setdefault(cell, []).extend(record_assets)
        record_cell[sid] = cell
        cell_records.setdefault(cell, []).append((int(row_number or 0), sid))
        sd = _coerce_str(sale_date)
        if sd is not None:
            date_values.append(sd)

    if not cells:
        raise ValueError("候选记录为空，无法抽样")

    # 分配配额（记录级）
    cell_population = {c: len(cell_records[c]) for c in cells}
    quotas = allocate_quotas(cell_population, target)

    # 单元内确定性选择（按 row_number 排序后稳定洗牌，种子+单元键派生随机）
    chosen_records: list[tuple[int, str, tuple[str, str, str]]] = []
    for cell, quota in sorted(quotas.items()):
        pool = sorted(cell_records[cell], key=lambda t: (t[0], t[1]))
        pick = pool[:quota] if quota >= len(pool) else _stable_sample(pool, quota, seed, cell)
        for row_no, sid in pick:
            chosen_records.append((row_no, sid, cell))

    # 展开为资产并按 row_number 排序（确定性）；多 URL 记录超出 target 时裁剪
    all_entries: list[SelectionEntry] = []
    chosen_sorted = sorted(chosen_records, key=lambda t: (t[0], t[1]))
    for _row_no, sid, _cell in chosen_sorted:
        for entry in cells[record_cell[sid]]:
            if entry.source_record_id == sid:
                all_entries.append(entry)
    # 按 (row_number, url_seq) 确定性排序，与父清单一致
    all_entries.sort(key=lambda e: (e.row_number, e.url_seq))

    trimmed = 0
    if len(all_entries) > target:
        # 从 row_number 最大的记录开始裁剪 url_seq>1 的额外资产（确定性）
        extras = [e for e in all_entries if e.url_seq > 1]
        extras.sort(key=lambda e: (e.row_number, e.url_seq), reverse=True)
        drop = {id(e) for e in extras[: len(all_entries) - target]}
        all_entries = [e for e in all_entries if id(e) not in drop]
        trimmed = len(drop)
        if len(all_entries) != target:
            raise ValueError(f"裁剪后资产数 {len(all_entries)} != 目标 {target}（多 URL 记录过多）")

    record_ids = sorted({e.source_record_id for e in all_entries})
    record_ids_hash = _sha256_hex("\n".join(record_ids))

    date_range_min = min(date_values) if date_values else None
    date_range_max = max(date_values) if date_values else None

    allocation_table: list[dict[str, Any]] = []
    for cell, quota in sorted(quotas.items()):
        allocation_table.append(
            {
                "district_group": cell[0],
                "year_bucket": cell[1],
                "bedroom_bucket": cell[2],
                "population": cell_population[cell],
                "quota": quota,
            }
        )

    estimated_download_bytes = len(all_entries) * avg_bytes
    storage_cap_bytes = int(estimated_download_bytes * storage_multiplier)
    budget_cap_yuan = round(len(all_entries) * unit_budget_yuan, 2)

    manifest = AcceptanceSelectionManifest(
        selection_rule_version=SAMPLING_RULE_VERSION,
        selection_rule_text=SAMPLING_RULE_TEXT,
        snapshot_ref=snapshot_ref,
        run_id=resolved_run_id,
        geoscope=geoscope,
        date_range_min=date_range_min,
        date_range_max=date_range_max,
        filter_condition=(
            "property_use_norm == '普通住宅' AND floorplan_url_status == 'URLS_OK' AND "
            "floorplan_candidate_count >= 1 AND url_class == 'FLOORPLAN_CANDIDATE' AND "
            "normalized_domain in domain_whitelist"
        ),
        record_count=len(record_ids),
        asset_count=len(all_entries),
        forbidden_domain_count=forbidden_domain_count,
        forbidden_domains=sorted(forbidden_domain_set),
        record_ids_hash=record_ids_hash,
        records=all_entries,
        record_sample=all_entries[:20] or None,
        domain_whitelist=sorted(allowed),
        estimated_download_bytes=estimated_download_bytes,
        storage_cap_bytes=storage_cap_bytes,
        budget_cap_yuan=budget_cap_yuan,
        workpackage_ref="EXTFP3",
        avg_bytes_estimate=avg_bytes,
        sampling_rule_version=SAMPLING_RULE_VERSION,
        random_seed=seed,
        target_size=target,
        source_selection_ref=source_selection_ref,
        allocation_table=allocation_table,
        trimmed_extra_assets=trimmed,
        duplicate_sid_rows=duplicate_sid_rows,
        created_at=datetime.now(UTC).isoformat(),
    )

    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        work.replace(out_json)

    return manifest


def _stable_sample(
    pool: list[tuple[int, str]],
    quota: int,
    seed: int,
    cell: tuple[str, str, str],
) -> list[tuple[int, str]]:
    """单元内稳定洗牌取配额：种子 + 单元键派生随机种子，跨进程一致。"""
    local = random.Random(_stable_seed(seed, *cell))
    shuffled = list(pool)
    local.shuffle(shuffled)
    return shuffled[:quota]


# ---------------------------------------------------------------------------
# 黄金标签框架（人工标注模板 + 校验，EXTFP3-G）
# ---------------------------------------------------------------------------

# 黄金标签 CSV 列（用户对原图人工标注；生产 OCR 不参与生成标准答案）
GOLDEN_LABEL_CSV_COLUMNS = [
    "sample_index",
    "asset_id",
    "source_record_id",
    "区县",
    "成交年份",
    "居室数",
    "图片文字类别",
    "文字质量",
    "房间清单",
    "备注",
]

# 图片文字类别（§10.2：有房间面积 / 只有房间名 / 几乎无文字；EXTFP3-G 实际标注新增
# 「范围外」= 人工判定不在验收范围的样本，如多层户型/非标准户型图，不参与验收指标）
GOLDEN_TEXT_CATEGORY: frozenset[str] = frozenset(
    {"有房间面积", "只有房间名", "几乎无文字", "范围外"}
)
# 文字质量（§10.2：清晰、小字、旋转文字、低对比度；「混合」为标注说明补充枚举；
# 「范围外」与 图片文字类别=范围外 对应）
GOLDEN_TEXT_QUALITY: frozenset[str] = frozenset(
    {"清晰", "小字", "旋转文字", "低对比度", "混合", "范围外"}
)

# 房间清单分隔符：分号分隔「房间名[=面积]」，如 "主卧=12.5;客厅=20.3;厨房;卫生间"
ROOM_LIST_SEP = ";"
ROOM_AREA_SEP = "="


class GoldenLabelRoom(BaseModel):
    """黄金标签中的一间房（由「房间清单」文本解析）。"""

    room_text: str = Field(description="图中所见房间文字（原样）")
    room_type_std: str | None = Field(
        default=None, description="标准房间类型（转录词表）；无法分类为 None"
    )
    area_present: bool = Field(description="图中是否明确标注面积")
    area_sqm: Decimal | None = Field(default=None, description="面积值（㎡，规范 Decimal）")


class GoldenLabelRow(BaseModel):
    """一张图片的黄金标签（人工标注）。"""

    sample_index: int
    asset_id: str
    source_record_id: str
    district_group: str | None = None
    year_bucket: str | None = None
    bedroom_bucket: str | None = None
    image_text_category: str = Field(description="有房间面积/只有房间名/几乎无文字")
    text_quality: str = Field(description="清晰/小字/旋转文字/低对比度/混合")
    rooms: list[GoldenLabelRoom] = Field(default_factory=list)
    note: str = ""


class GoldenLabelValidation(BaseModel):
    """黄金标签校验结果（保存为标签证据）。"""

    rules_version: str = SAMPLING_RULE_VERSION
    validated_at: str
    csv_path: str
    expected_samples: int
    rows_ok: int
    missing_samples: list[int] = Field(default_factory=list)
    invalid_entries: list[str] = Field(default_factory=list)
    extra_samples: list[int] = Field(default_factory=list)
    category_counts: dict[str, int] = Field(default_factory=dict)
    quality_counts: dict[str, int] = Field(default_factory=dict)
    room_count_total: int = 0
    area_present_count: int = 0
    excluded_count: int = 0
    valid: bool


def _parse_room_list(text: str) -> tuple[list[GoldenLabelRoom], list[str]]:
    """解析「房间清单」文本（分号分隔，每项 ``房间名[=面积]``）。"""
    rooms: list[GoldenLabelRoom] = []
    errors: list[str] = []
    for i, part in enumerate(str(text or "").split(ROOM_LIST_SEP)):
        part = part.strip()
        if not part:
            continue
        if ROOM_AREA_SEP in part:
            name, _, area_raw = part.partition(ROOM_AREA_SEP)
            name = name.strip()
            area_raw = area_raw.strip()
            if not name:
                errors.append(f"第{i + 1}项缺房间名: {part!r}")
                continue
            try:
                area_dec = Decimal(area_raw)
            except InvalidOperation:
                errors.append(f"面积非数值: {area_raw!r}（第{i + 1}项 {part!r}）")
                continue
            if area_dec < 0:
                errors.append(f"面积为负: {area_raw!r}（第{i + 1}项 {part!r}）")
                continue
            room_type = classify_room(name)
            rooms.append(
                GoldenLabelRoom(
                    room_text=name,
                    room_type_std=room_type[0] if room_type else None,
                    area_present=True,
                    area_sqm=area_dec,
                )
            )
        else:
            name = part
            room_type = classify_room(name)
            rooms.append(
                GoldenLabelRoom(
                    room_text=name,
                    room_type_std=room_type[0] if room_type else None,
                    area_present=False,
                    area_sqm=None,
                )
            )
    return rooms, errors


def write_golden_label_template(
    manifest: AcceptanceSelectionManifest,
    out_csv: Path,
) -> Path:
    """生成人工标注 CSV 模板（300 行，预填样本标识与分层参考列）。

    用户对 300 张原图填写：图片文字类别、文字质量、房间清单；生产 OCR 不参与生成
    标准答案。CSV 以 UTF-8-SIG（BOM）写出，便于 Excel 直接打开中文。
    """
    rows: list[dict[str, Any]] = []
    for idx, entry in enumerate(manifest.records, start=1):
        rows.append(
            {
                "sample_index": idx,
                "asset_id": "",  # 资产化后回填（模板阶段未知）；用户可留空
                "source_record_id": entry.source_record_id,
                "区县": "",
                "成交年份": "",
                "居室数": "",
                "图片文字类别": "",
                "文字质量": "",
                "房间清单": "",
                "备注": "",
            }
        )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=GOLDEN_LABEL_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return out_csv


def validate_golden_labels(
    csv_path: Path,
    manifest: AcceptanceSelectionManifest | None = None,
    *,
    out_json: Path | None = None,
) -> GoldenLabelValidation:
    """校验人工标注的黄金标签 CSV，保存标签证据。

    - 校验 300 行样本齐全（缺/多/重复 sample_index）；
    - 校验 图片文字类别 / 文字质量 枚举；
    - 解析并校验 房间清单（房间名 + 面积）；
    - 聚合统计（房间数、有面积数、类别/质量计数）。
    """
    expected = manifest.target_size if manifest is not None else DEFAULT_TARGET_SIZE
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            rows.append({k: (raw.get(k) or "").strip() for k in GOLDEN_LABEL_CSV_COLUMNS})

    seen: dict[int, list[dict[str, str]]] = {}
    missing: list[int] = []
    invalid: list[str] = []
    extra: list[int] = []
    category_counts: dict[str, int] = {}
    quality_counts: dict[str, int] = {}
    room_count_total = 0
    area_present_count = 0
    excluded_count = 0

    for row in rows:
        try:
            idx = int(row["sample_index"])
        except (TypeError, ValueError):
            invalid.append("sample_index 非整数")
            continue
        seen.setdefault(idx, []).append(row)

    for idx in range(1, expected + 1):
        if idx not in seen:
            missing.append(idx)

    for idx, dup_rows in seen.items():
        if len(dup_rows) > 1:
            extra.append(idx)
        row = dup_rows[0]
        cat = row["图片文字类别"]
        q = row["文字质量"]
        if cat not in GOLDEN_TEXT_CATEGORY:
            invalid.append(f"sample {idx}: 图片文字类别 非法 {cat!r}")
        else:
            category_counts[cat] = category_counts.get(cat, 0) + 1
            if cat == "范围外":
                excluded_count += 1
        if q not in GOLDEN_TEXT_QUALITY:
            invalid.append(f"sample {idx}: 文字质量 非法 {q!r}")
        else:
            quality_counts[q] = quality_counts.get(q, 0) + 1
        rooms, room_errors = _parse_room_list(row["房间清单"])
        for err in room_errors:
            invalid.append(f"sample {idx}: {err}")
        room_count_total += len(rooms)
        area_present_count += sum(1 for r in rooms if r.area_present)

    valid = not missing and not invalid and not extra
    result = GoldenLabelValidation(
        rules_version=SAMPLING_RULE_VERSION,
        validated_at=datetime.now(UTC).isoformat(),
        csv_path=csv_path.as_posix(),
        expected_samples=expected,
        rows_ok=len(rows),
        missing_samples=sorted(missing),
        invalid_entries=invalid,
        extra_samples=sorted(set(extra)),
        category_counts=category_counts,
        quality_counts=quality_counts,
        room_count_total=room_count_total,
        area_present_count=area_present_count,
        excluded_count=excluded_count,
        valid=valid,
    )
    if out_json is not None:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(result.model_dump_json(indent=2), encoding="utf-8")
        work.replace(out_json)
    return result


# OCR 任务状态（与 floorplan_ocr.OcrState 取值保持一致，避免跨模块强耦合）
_OCR_STATE_SUCCEEDED = "OCR_SUCCEEDED"
_OCR_STATE_PARTIAL = "OCR_PARTIAL"


def write_ocr_draft_golden_labels(
    template_csv: Path,
    annotation_table: Path,
    ocr_state: Path,
    *,
    out_csv: Path | None = None,
) -> Path:
    """用 OCR 转录草稿预填黄金标签模板（OCR 预标注 + 人工复核工作流）。

    用户已确认「OCR 预标注 + 人工复核」：先用确定性转录标注表预填 图片文字类别 与
    房间清单，文字质量留空，人工逐张复核修正后作为黄金标签证据。生产 OCR 不参与
    生成自己的最终标准答案——最终判定权在人工。

    - ``template_csv``：黄金模板（sample_index/asset_id/source_record_id + 标注列）；
    - ``annotation_table``：staged/floorplan_room_annotation.parquet（按 ocr_task_id 分组）；
    - ``ocr_state``：OCR 运行 ``ocr_state.json``（ocr_task_id→asset_id、任务状态）；
    - 房间清单用 ``room_name_raw``（图中原文）+ ACCEPTED 的 ``area_value``；
      NEEDS_REVIEW/CONFLICT 面积不预填（防错误数字），仅在备注提示人工确认；
    - OCR 失败/部分完成的图不预填内容，备注提示人工标注，绝不把空 OCR 当「几乎无文字」。
    """
    rows: list[dict[str, str]] = []
    with template_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            rows.append({k: (raw.get(k) or "").strip() for k in GOLDEN_LABEL_CSV_COLUMNS})

    state_data = json.loads(ocr_state.read_text(encoding="utf-8"))
    asset_task: dict[str, tuple[str, str]] = {}
    for task in state_data.get("tasks", []):
        aid = task.get("asset_id")
        if aid:
            asset_task[aid] = (task.get("ocr_task_id", ""), task.get("state", ""))

    ann_by_task: dict[str, list[dict[str, str]]] = {}
    frame = pl.read_parquet(annotation_table)
    for a in frame.select(
        [
            "ocr_task_id",
            "room_name_raw",
            "room_name_normalized",
            "standard_room_type",
            "area_value",
            "parse_state",
        ]
    ).iter_rows():
        tid = a[0]
        ann_by_task.setdefault(tid, []).append(
            {
                "room_name_raw": a[1] or "",
                "room_name_normalized": a[2] or "",
                "standard_room_type": a[3] or "",
                "area_value": a[4] or "",
                "parse_state": a[5] or "",
            }
        )

    for row in rows:
        aid = row["asset_id"]
        task = asset_task.get(aid)
        notes: list[str] = ["OCR草稿，请复核"]
        if task is None:
            row["图片文字类别"] = ""
            row["房间清单"] = ""
            notes = ["OCR 无任务，请人工标注"]
        elif task[1] not in (_OCR_STATE_SUCCEEDED, _OCR_STATE_PARTIAL):
            row["图片文字类别"] = ""
            row["房间清单"] = ""
            notes = [f"OCR {task[1]}，请人工标注"]
        else:
            if task[1] == _OCR_STATE_PARTIAL:
                notes.append("OCR 部分完成，请重点复核")
            parts: list[str] = []
            has_area = False
            has_room = False
            ambiguous = 0
            for ann in ann_by_task.get(task[0], []):
                name = (ann["room_name_raw"] or ann["room_name_normalized"]).strip()
                if not name:
                    continue
                area = ann["area_value"].strip()
                if ann["parse_state"] == "ACCEPTED" and area:
                    parts.append(f"{name}={area}")
                    has_area = True
                    has_room = True
                else:
                    parts.append(name)
                    has_room = True
                    if area and ann["parse_state"] in ("CONFLICT", "NEEDS_REVIEW"):
                        ambiguous += 1
            # 去重连续重复（同文同值不应出现两遍）
            deduped: list[str] = []
            for part in parts:
                if not deduped or deduped[-1] != part:
                    deduped.append(part)
            row["房间清单"] = ROOM_LIST_SEP.join(deduped)
            if has_area:
                row["图片文字类别"] = "有房间面积"
            elif has_room:
                row["图片文字类别"] = "只有房间名"
            else:
                row["图片文字类别"] = "几乎无文字"
            if ambiguous:
                notes.append(f"{ambiguous} 条面积无法唯一关联，请人工确认")
        row["备注"] = "；".join(notes)

    out = out_csv or template_csv.with_name("golden_label_ocr_draft.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=GOLDEN_LABEL_CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return out


__all__ = [
    "AcceptanceSelectionManifest",
    "BEDROOM_BUCKETS",
    "DEFAULT_SAMPLE_SEED",
    "DEFAULT_TARGET_SIZE",
    "DISTRICT_NAMES",
    "GOLDEN_LABEL_CSV_COLUMNS",
    "GOLDEN_TEXT_CATEGORY",
    "GOLDEN_TEXT_QUALITY",
    "GoldenLabelRow",
    "GoldenLabelValidation",
    "OTHER_DISTRICT",
    "SAMPLING_RULE_TEXT",
    "SAMPLING_RULE_VERSION",
    "YEAR_BUCKETS",
    "allocate_quotas",
    "bedroom_bucket",
    "build_acceptance_sample",
    "district_group",
    "validate_golden_labels",
    "write_golden_label_template",
    "write_ocr_draft_golden_labels",
    "year_bucket",
]
