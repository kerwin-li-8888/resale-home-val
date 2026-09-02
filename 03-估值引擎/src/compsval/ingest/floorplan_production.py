"""EXTFP4 授权生产批次核心层（change extfp4-production-batch）。

把已验证的下载与 OCR 能力推进到受控生产路径：生产 profile 选择清单派生与
不可变冻结、磁盘门禁、批次合同（三层预算 + 门禁 + §19.2 停止条件 + 已知限制）、
批次确认与投放门禁、成本台账、自动停止条件评估。

授权链路（design D8）：提案 → 用户实施授权 → 逐批批次确认。任何一层缺失，
``assert_dispatch_allowed`` 抛出 :class:`DispatchBlockedError`，不得投放。

冻结运行配置来源（只读引用，不改写）：
``01-数据/外部数据/画像报告/20260830-外部链家OCR-OCRNEXT-CONCURRENCY.json#frozen_extfp4_config``
（并发 8、单图 max_attempts=3、max_retries 门禁 50、单图成本基线 0.004 元/
异常暂停阈值 1.2 比例、timeout 60s、请求合同 EXTFP3-A-OCR-1.0）。
本模块只离线运行；真实下载与付费 OCR 仅在批次确认通过后由编排入口发起。
"""

from __future__ import annotations

import hashlib
import json
import math
import shutil
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from compsval.ingest.floorplan_ocr_contract import (
    REQUEST_CONTRACT_V1,
    OcrCostConfig,
    OcrRunConfig,
)
from compsval.ingest.floorplan_selection import (
    FILTER_CONDITION_TEXT,
    GEOSCOPE,
    SelectionManifest,
    build_selection,
)
from compsval.valuation.scope import (
    FROZEN_CONDITIONAL_IDS,
    FROZEN_SUPPORTED_IDS,
)

# ---------------------------------------------------------------------------
# change 与批次合同版本
# ---------------------------------------------------------------------------

CHANGE_REF = "extfp4-production-batch"
BATCH_CONTRACT_VERSION = "EXTFP4-CONTRACT-1.0"
PRODUCTION_PROFILE = "supported-community-window-v1"

# 生产时间窗（数据截点 2026-07-20 前推 12 个月，含端点；用户 2026-08-30 冻结）
PRODUCTION_DATE_MIN = "2025-07-20"
PRODUCTION_DATE_MAX = "2026-07-20"

# 生产选择规则（EXTFP4-SELECT-1.0）：全量清单（208,075 基线）的确定性子集
PRODUCTION_RULE_VERSION = "EXTFP4-SELECT-1.0"
PRODUCTION_RULE_TEXT = "\n".join(
    [
        "[EXTFP4-SELECT-1.0] EXTFP4 首个生产选择清单规则（授权生产批次）",
        "1. 唯一派生源：既有 208,075 记录全量清单对应 staged 表，"
        "基线过滤与 EXTFP2-B-SELECT-1.0 完全一致"
        "（房屋用途=普通住宅 AND floorplan_url_status=URLS_OK AND 重解析候选>=1）；",
        "2. 小区口径：scope_policy v1.1 冻结支持小区（8 个 FROZEN_SUPPORTED_IDS；"
        "4 个 conditional 默认排除），经 community 标准名与 community_alias 别名"
        "精确匹配 staged.community_name；未匹配记录进排除清单，绝不静默丢弃，"
        "也不放宽匹配规则以凑量；",
        "3. 时间口径：成交日期 sale_date ∈ [2025-07-20, 2026-07-20]"
        "（数据截点 2026-07-20 前推 12 个月，含端点）；",
        "4. URL 层规则继承 EXTFP2-B-SELECT-1.0：仅 FLOORPLAN_CANDIDATE 且域名白名单"
        "（ke-image.ljcdn.com）内候选入资产；白名单外记为违规；",
        "5. ordinary_residential_all 全量批次（208,075/208,081）不混入本次授权；",
        "6. 幂等：相同输入重跑产出相同记录集与 record_ids_hash。",
    ]
)
PRODUCTION_FILTER_TEXT = (
    FILTER_CONDITION_TEXT
    + " AND community_name IN (scope_policy_v1.1 supported 标准名∪别名)"
    + " AND sale_date >= '2025-07-20' AND sale_date <= '2026-07-20'"
)

# ---------------------------------------------------------------------------
# EXTFP6 全历史 profile（change add-extfp6-full-history-ocr-batch）
# ---------------------------------------------------------------------------

EXTFP6_CHANGE_REF = "add-extfp6-full-history-ocr-batch"
#: change 级累计人民币硬上限（用户 2026-08-31 冻结：≤¥10，fail-closed）
EXTFP6_CHANGE_BUDGET_CAP_YUAN = 10.0
FULLHISTORY_PROFILE = "supported-community-fullhistory-v1"
FULLHISTORY_RULE_VERSION = "EXTFP6-SELECT-1.0"
#: 全历史模式下 manifest 日期窗口字段的显式标记（不限成交时间窗）
FULL_HISTORY_MARK = "FULL_HISTORY"

FULLHISTORY_RULE_TEXT = "\n".join(
    [
        "[EXTFP6-SELECT-1.0] EXTFP6 全历史生产选择清单规则（可比侧户型图定向扩批）",
        "1. 唯一派生源：既有 208,075 记录全量清单对应 staged 表，"
        "基线过滤与 EXTFP2-B-SELECT-1.0 完全一致"
        "（房屋用途=普通住宅 AND floorplan_url_status=URLS_OK AND 重解析候选>=1）；"
        "既有全量清单锚点与已冻结 EXTFP4 批次 1 manifest 不重算不改写；",
        "2. 小区口径：scope_policy v1.1 冻结支持小区（8 个 FROZEN_SUPPORTED_IDS；"
        "4 个 conditional 默认排除），经 community 标准名与 community_alias 中 "
        "conflict_status=一致 的别名精确匹配 staged.community_name（含示例小区130A区/B区）；"
        "待定/冲突别名 blocked 不参与；未匹配记录（含全部全示例城市记录）进排除清单，"
        "绝不静默丢弃，也不放宽匹配规则以凑量；",
        "3. 时间口径：不限成交日期窗口（全历史），区别于 EXTFP4 的 12 个月窗；",
        "4. URL 层规则继承 EXTFP2-B-SELECT-1.0：仅 FLOORPLAN_CANDIDATE 且域名白名单"
        "（ke-image.ljcdn.com）内候选入资产；白名单外记为违规；",
        "5. 记录级去重沿用 EXTFP4-SELECT-1.0（同 source_record_id 多行只保留一行，"
        "被去重行数与样例披露入排除清单）；",
        "6. ordinary_residential_all 全量批次不混入本次授权；本次授权不构成估值接入；",
        "7. 幂等：相同输入重跑产出相同记录集与 record_ids_hash。",
    ]
)
FULLHISTORY_FILTER_TEXT = (
    FILTER_CONDITION_TEXT
    + " AND community_name IN (scope_policy_v1.1 supported 标准名∪一致别名)"
    + " AND sale_date IS NOT NULL（全历史：不限成交时间窗）"
)

# ---------------------------------------------------------------------------
# 冻结成本/预算参数（EXTFP3-H/G 实测 + 用户 2026-08-30 冻结决策）
# ---------------------------------------------------------------------------

#: 300 张实测单图成本（EXTFP3-H 逐任务求和口径）
UNIT_COST_PER_IMAGE_YUAN = 0.001094
#: manifest 级预算 = 资产数 × 单图成本 × 1.2（外推 + 20% 余量）
MANIFEST_BUDGET_MULTIPLIER = 1.2
#: change 级累计人民币硬上限（用户 2026-08-30 冻结：全量口径兜底）
CHANGE_BUDGET_CAP_YUAN = 300.0
#: attempt 门禁 = 资产数 × 1.10（实测 retries 率约 3%，约束失控重试）
ATTEMPT_CAP_MULTIPLIER = 1.10
#: 磁盘门禁：剩余空间 ≥ 外推需求 × 1.5，不足即停
DISK_GATE_MULTIPLIER = 1.5
#: 单张原图均值（EXTFP3-G 300 张下载实测）
PRODUCTION_AVG_IMAGE_BYTES = 164_578

# ---------------------------------------------------------------------------
# 冻结 OCR 运行配置（frozen_extfp4_config；仅获 EXTFP4 授权后使用）
# ---------------------------------------------------------------------------

FROZEN_CONCURRENCY = 8
FROZEN_MAX_ATTEMPTS_PER_IMAGE = 3
FROZEN_MAX_RETRIES_GATE = 50
FROZEN_BASELINE_COST_PER_IMAGE_YUAN = 0.004
FROZEN_PAUSE_RATIO_THRESHOLD = 1.2
FROZEN_TIMEOUT_S = 60.0
#: 下载有界并发（§8.2：默认小并发；冻结 8 仅约束 OCR）
FROZEN_DOWNLOAD_CONCURRENCY = 4

FROZEN_CONFIG_SOURCE = (
    "01-数据/外部数据/画像报告/20260830-外部链家OCR-OCRNEXT-CONCURRENCY.json#frozen_extfp4_config"
)


def assert_frozen_request_contract(config: OcrRunConfig) -> None:
    """校验运行配置的请求合同与 ``frozen_extfp4_config`` 完全一致，偏差即拒绝启动。

    校验模型、任务、像素、旋转与流式参数（fail-closed）；成本门禁数值由调用方按
    批次确认裁剪，不属于请求合同。
    """
    contract = config.request
    frozen = {
        "model": (contract.model, REQUEST_CONTRACT_V1.model),
        "task": (contract.task, REQUEST_CONTRACT_V1.task),
        "min_pixels": (contract.min_pixels, REQUEST_CONTRACT_V1.min_pixels),
        "max_pixels": (contract.max_pixels, REQUEST_CONTRACT_V1.max_pixels),
        "enable_rotate": (contract.enable_rotate, REQUEST_CONTRACT_V1.enable_rotate),
        "stream": (contract.stream, REQUEST_CONTRACT_V1.stream),
    }
    mismatched = {k: (a, b) for k, (a, b) in frozen.items() if a != b}
    if mismatched:
        detail = ", ".join(f"{k}: {a!r} != 冻结 {b!r}" for k, (a, b) in mismatched.items())
        raise ValueError(f"OCR 运行配置偏离 frozen_extfp4_config 请求合同：{detail}")


def production_ocr_run_config(
    *,
    max_images: int,
    hard_cap_yuan: float,
    max_retries_gate: int | None = None,
) -> OcrRunConfig:
    """构造生产 OCR 运行配置：冻结请求合同 + 批次确认裁剪后的成本门禁。

    ``max_retries_gate`` 默认取冻结门禁 50 与 ``floor(max_images * 0.10)`` 的
    较小值：图片数 + 重试数 ≤ 1.10 × 资产数的 change 级 attempt 门禁比冻结门禁
    更紧时，以更紧者生效（两个门禁均写入批次合同，先到即停）。
    """
    derived = int(math.floor(max_images * (ATTEMPT_CAP_MULTIPLIER - 1.0)))
    effective_retries = min(FROZEN_MAX_RETRIES_GATE, max(1, derived))
    if max_retries_gate is not None:
        effective_retries = min(effective_retries, max_retries_gate)
    config = OcrRunConfig(
        request=REQUEST_CONTRACT_V1.model_copy(deep=True),
        cost=OcrCostConfig(
            hard_cap_yuan=hard_cap_yuan,
            max_images=max_images,
            max_retries=effective_retries,
            baseline_cost_per_image_yuan=FROZEN_BASELINE_COST_PER_IMAGE_YUAN,
            pause_ratio_threshold=FROZEN_PAUSE_RATIO_THRESHOLD,
        ),
    )
    assert_frozen_request_contract(config)
    return config


# ---------------------------------------------------------------------------
# 目标小区名称解析（community 权威表 + 别名表，机器可执行）
# ---------------------------------------------------------------------------


def supported_community_names(
    entities_dir: Path,
    *,
    include_conditional: bool = False,
) -> dict[str, str]:
    """返回目标小区「名称 -> community_id」精确匹配集合。

    名称 = 冻结支持小区的 ``standard_name`` ∪ 指向这些小区且
    ``conflict_status=一致`` 的 ``source_alias``（EXTFP6 起：待定与冲突别名维持
    blocked，不参与自动匹配，与 community-alias-registry 匹配语义一致）；
    ``include_conditional=True`` 时并入 4 个 conditional 小区（首批默认排除）。
    标准名与别名冲突时标准名优先（键覆盖语义：别名先写、标准名后写覆盖）。
    """
    ids = set(FROZEN_SUPPORTED_IDS)
    if include_conditional:
        ids |= set(FROZEN_CONDITIONAL_IDS)
    community_path = entities_dir / "community.parquet"
    if not community_path.is_file():
        raise FileNotFoundError(f"community 权威表不存在: {community_path}")
    names: dict[str, str] = {}
    alias_path = entities_dir / "community_alias.parquet"
    if alias_path.is_file():
        alias = pl.read_parquet(alias_path)
        for row in alias.iter_rows(named=True):
            if row["community_id"] in ids and row["source_alias"]:
                if row.get("conflict_status") != "一致":
                    continue  # 待定/冲突别名 blocked，不自动合并
                names.setdefault(str(row["source_alias"]), str(row["community_id"]))
    community = pl.read_parquet(community_path)
    for row in community.iter_rows(named=True):
        if row["community_id"] in ids and row["standard_name"]:
            names[str(row["standard_name"])] = str(row["community_id"])
    return names


# ---------------------------------------------------------------------------
# 生产选择清单派生（任务 1.1/1.2）
# ---------------------------------------------------------------------------


class ProductionSelectionManifest(SelectionManifest):
    """生产选择清单：在不可变 ``SelectionManifest`` 上固定 EXTFP4 授权字段。"""

    change_ref: str = CHANGE_REF
    production_profile: str = PRODUCTION_PROFILE
    date_window_min: str = PRODUCTION_DATE_MIN
    date_window_max: str = PRODUCTION_DATE_MAX
    budget_expected_yuan: float = Field(description="资产数 × 单图实测成本（外推参考）")
    change_budget_cap_yuan: float = CHANGE_BUDGET_CAP_YUAN
    attempt_cap: int = Field(description="attempt 门禁 = ceil(资产数 × 1.10)")
    matched_community_counts: dict[str, int] = Field(default_factory=dict)
    baseline_record_count: int = Field(description="全量基线（EXTFP2-B 三条件）记录数")
    baseline_record_ids_hash: str = Field(description="全量基线 record_ids_hash（派生锚点）")
    full_manifest_ref: str | None = Field(
        default=None, description="派生源全量清单引用（路径 + record_ids_hash）"
    )
    exclusion_report_ref: str | None = Field(default=None, description="排除清单报告路径")
    conditional_included: bool = False


class ProductionExclusionReport(BaseModel):
    """生产选择排除清单报告：计数 + 原因 + 已知缺口（审计可追溯）。"""

    change_ref: str = CHANGE_REF
    generated_at: str
    baseline_record_count: int
    selected_record_count: int
    excluded_matched_out_of_window: int = Field(
        description="命中目标小区但成交日期在窗口外",
    )
    excluded_unmatched_in_window: int = Field(
        description="窗口内但小区未命中目标集合（含别名缺口）",
    )
    excluded_unmatched_outside_window: int
    deduped_record_count: int = Field(
        default=0, description="记录级去重被去重行数（同 source_record_id 多行，生产 profile 启用）"
    )
    deduped_record_sample: list[str] = Field(
        default_factory=list, description="被去重记录 source_record_id 样例"
    )
    unmatched_in_window_top_names: list[dict[str, Any]] = Field(
        default_factory=list,
        description="窗口内未命中小区名 Top10（名称 + 行数），供别名缺口审计",
    )
    matched_community_counts: dict[str, int] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


def compute_record_ids_hash(record_ids: list[str]) -> str:
    """与 build_selection 同口径：排序后 source_record_id 清单的 SHA256。"""
    return hashlib.sha256("\n".join(sorted(record_ids)).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    """文件字节级 SHA256（manifest/合同冻结锚点）。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_sha256_sidecar(path: Path) -> Path:
    """写 ``<path>.sha256`` 旁证文件并返回其路径。"""
    sidecar = path.with_name(path.name + ".sha256")
    sidecar.write_text(f"{sha256_file(path)}  {path.name}\n", encoding="utf-8")
    return sidecar


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    work = path.with_name(path.name + ".incomplete")
    work.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    work.replace(path)


def build_production_selection(
    parquet_path: Path,
    entities_dir: Path,
    *,
    out_json: Path,
    exclusion_out_json: Path | None = None,
    full_manifest_path: Path | None = None,
    include_conditional: bool = False,
    date_min: str | None = PRODUCTION_DATE_MIN,
    date_max: str | None = PRODUCTION_DATE_MAX,
    geoscope: str = GEOSCOPE,
    profile: str = PRODUCTION_PROFILE,
    change_ref: str = CHANGE_REF,
    workpackage_ref: str = "EXTFP4",
    change_budget_cap_yuan: float = CHANGE_BUDGET_CAP_YUAN,
    selection_rule_version: str = PRODUCTION_RULE_VERSION,
    selection_rule_text: str = PRODUCTION_RULE_TEXT,
    filter_condition_text: str = PRODUCTION_FILTER_TEXT,
) -> tuple[ProductionSelectionManifest, ProductionExclusionReport]:
    """派生生产选择清单并原子落盘。

    默认参数派生 EXTFP4「目标小区 ∩ 近 12 个月」清单；``date_min=date_max=None``
    切换为 EXTFP6 全历史 profile（不限成交时间窗，其余口径一致，配套传
    ``profile``/``change_ref``/``workpackage_ref``/``change_budget_cap_yuan``/
    ``selection_rule_*``/``filter_condition_text`` 的 EXTFP6 冻结值）。

    - 基线（EXTFP2-B 三条件）记录数与 ``record_ids_hash`` 与全量清单核对
      （派生锚点；``full_manifest_path`` 提供时强校验，不一致即抛错）；
    - 小区匹配经 community/alias 精确名称集合（别名仅 ``conflict_status=一致``
      参与匹配）；未匹配记录进排除清单报告；
    - manifest 固定 §4.3 全部字段 + 生产授权字段（预算/attempt/授权引用）；
    - 幂等：相同输入产出相同记录集、哈希与排除报告。
    """
    if not parquet_path.is_file():
        raise FileNotFoundError(f"staged parquet not found: {parquet_path}")
    names = supported_community_names(entities_dir, include_conditional=include_conditional)
    if not names:
        raise ValueError("目标小区名称集合为空：community/alias 表解析失败")
    full_history = date_min is None and date_max is None

    frame = pl.read_parquet(parquet_path)
    baseline = frame.filter(
        (pl.col("property_use_norm") == "普通住宅")
        & (pl.col("floorplan_url_status") == "URLS_OK")
        & (pl.col("floorplan_candidate_count") >= 1)
    )
    baseline_ids = baseline["source_record_id"].cast(pl.Utf8).drop_nulls().to_list()
    baseline_count = len(baseline_ids)
    baseline_hash = compute_record_ids_hash(baseline_ids)

    full_ref: str | None = None
    if full_manifest_path is not None:
        full = json.loads(full_manifest_path.read_text(encoding="utf-8"))
        full_ref = f"{full_manifest_path.as_posix()}#{full.get('record_ids_hash', '')}"
        if (
            full.get("record_ids_hash") != baseline_hash
            or full.get("record_count") != baseline_count
        ):
            raise ValueError(
                "派生锚点不一致：staged 基线与全量清单 record_ids_hash/record_count 不符，"
                "拒绝派生（快照可能已变化，须重新核对）"
            )

    matched_ids = set(
        baseline.filter(pl.col("community_name").is_in(sorted(names)))["source_record_id"]
        .cast(pl.Utf8)
        .drop_nulls()
        .to_list()
    )
    if full_history:
        # 全历史 profile：窗口 = 全部基线（不限成交日期），排除计数公式自然退化
        in_window_ids = set(baseline_ids)
    else:
        in_window_ids = set(
            baseline.filter(
                (pl.col("sale_date") >= date_min) & (pl.col("sale_date") <= date_max)
            )["source_record_id"]
            .cast(pl.Utf8)
            .drop_nulls()
            .to_list()
        )
    matched_set, window_set = matched_ids, in_window_ids
    baseline_set = set(baseline_ids)
    excluded_out_of_window = len(matched_set - window_set)
    excluded_unmatched_in_window = len(window_set - matched_set)
    excluded_both = len(baseline_set - matched_set - window_set)

    unmatched_filter = pl.col("community_name").is_in(sorted(names)).not_()
    if not full_history:
        unmatched_filter = unmatched_filter & (pl.col("sale_date") >= date_min) & (
            pl.col("sale_date") <= date_max
        )
    unmatched_in_window = (
        baseline.filter(unmatched_filter)
        .group_by("community_name")
        .agg(pl.len().alias("rows"))
        .sort("rows", descending=True)
        .head(10)
    )
    top_names = [
        {"community_name": r["community_name"], "rows": r["rows"]}
        for r in unmatched_in_window.iter_rows(named=True)
    ]

    manifest = build_selection(
        parquet_path,
        out_json=None,
        selection_rule_version=selection_rule_version,
        selection_rule_text=selection_rule_text,
        filter_condition_text=filter_condition_text,
        community_names=set(names),
        date_min=date_min,
        date_max=date_max,
        workpackage_ref=workpackage_ref,
        avg_bytes=PRODUCTION_AVG_IMAGE_BYTES,
        # 记录级去重（EXTFP4-verify-followups，SUGGESTION ②）：同 source_record_id
        # 多行只保留一行（确定性规则见 design D3），被去重行数与样例披露入排除报告
        dedupe_record_ids=True,
    )
    # 小区命中计数按去重后清单（manifest.records）统计，与 record_count/被去重行数一致；
    # 同 id 多行只取首见行的小区名（同一记录 id 的小区归属一致）
    id_to_community: dict[str, str] = {}
    for row in frame.select(["source_record_id", "community_name"]).iter_rows(named=True):
        sid = str(row["source_record_id"])
        if sid not in id_to_community and row["community_name"] is not None:
            id_to_community[sid] = str(row["community_name"])
    counts_by_id: dict[str, int] = {}
    for entry in manifest.records:
        community_id = names.get(id_to_community.get(entry.source_record_id, ""))
        if community_id is None:
            continue
        counts_by_id[community_id] = counts_by_id.get(community_id, 0) + 1
    matched_counts = dict(sorted(counts_by_id.items()))

    budget_expected = round(manifest.asset_count * UNIT_COST_PER_IMAGE_YUAN, 4)
    budget_cap = round(
        manifest.asset_count * UNIT_COST_PER_IMAGE_YUAN * MANIFEST_BUDGET_MULTIPLIER, 2
    )
    attempt_cap = max(1, int(math.ceil(manifest.asset_count * ATTEMPT_CAP_MULTIPLIER)))

    payload = manifest.model_dump()
    payload.update(
        {
            "change_ref": change_ref,
            "production_profile": profile,
            "workpackage_ref": workpackage_ref,
            "change_budget_cap_yuan": change_budget_cap_yuan,
            "budget_expected_yuan": budget_expected,
            "budget_cap_yuan": budget_cap,
            "attempt_cap": attempt_cap,
            "matched_community_counts": matched_counts,
            "baseline_record_count": baseline_count,
            "baseline_record_ids_hash": baseline_hash,
            "full_manifest_ref": full_ref,
            "date_window_min": date_min if date_min is not None else FULL_HISTORY_MARK,
            "date_window_max": date_max if date_max is not None else FULL_HISTORY_MARK,
            "conditional_included": include_conditional,
        }
    )
    production = ProductionSelectionManifest(**payload)

    # 落盘：manifest（原子）+ SHA256 旁证 + 排除清单报告
    out_json.parent.mkdir(parents=True, exist_ok=True)
    work = out_json.with_name(out_json.name + ".incomplete")
    work.write_text(production.model_dump_json(indent=2), encoding="utf-8")
    work.replace(out_json)
    write_sha256_sidecar(out_json)

    notes = [
        "生产 profile 派生启用记录级去重（同 source_record_id 多行只保留一行，保留行按"
        "确定性规则：户型图字段可解析且字典序稳定）；被去重行数与样例见 "
        "deduped_record_count / deduped_record_sample。",
        "匹配口径为精确名称（标准名 ∪ community_alias 中 conflict_status=一致 的别名）；"
        "待定/冲突别名 blocked 不参与自动匹配；未命中记录不放宽匹配以凑量。",
        "全量 208,075/208,081（ordinary_residential_all）不混入本次授权（§4.3）；"
        "本清单派生不构成估值接入授权。",
    ]
    if full_history:
        notes.append(
            "EXTFP6 全历史 profile：不限成交时间窗，排除报告的 in_window 口径即全体基线"
            "（excluded_matched_out_of_window 恒为 0）；全示例城市未匹配记录全部进排除清单，"
            "不进入下载与 OCR 投放。"
        )
    exclusion = ProductionExclusionReport(
        change_ref=change_ref,
        generated_at=datetime.now(UTC).isoformat(),
        baseline_record_count=baseline_count,
        selected_record_count=production.record_count,
        excluded_matched_out_of_window=excluded_out_of_window,
        excluded_unmatched_in_window=excluded_unmatched_in_window,
        excluded_unmatched_outside_window=excluded_both,
        deduped_record_count=manifest.dedupe_record_count,
        deduped_record_sample=manifest.dedupe_record_sample,
        unmatched_in_window_top_names=top_names,
        matched_community_counts=matched_counts,
        notes=notes,
    )
    if exclusion_out_json is not None:
        _write_json_atomic(exclusion_out_json, exclusion.model_dump())
        production.exclusion_report_ref = exclusion_out_json.as_posix()
        # 回填引用后重写 manifest（排除报告路径属清单元数据）
        work = out_json.with_name(out_json.name + ".incomplete")
        work.write_text(production.model_dump_json(indent=2), encoding="utf-8")
        work.replace(out_json)
        write_sha256_sidecar(out_json)

    return production, exclusion


# ---------------------------------------------------------------------------
# 磁盘门禁（任务 1.3/3.3）
# ---------------------------------------------------------------------------


class DiskGateResult(BaseModel):
    """磁盘门禁判定结果（实测 + 外推 + 判定，留痕）。"""

    ok: bool
    free_bytes: int
    required_bytes: int
    multiplier: float
    checked_path: str
    checked_at: str


def check_disk_gate(
    path: Path,
    required_bytes: int,
    *,
    multiplier: float = DISK_GATE_MULTIPLIER,
) -> DiskGateResult:
    """剩余空间 ≥ 外推需求 × multiplier 才放行（不足即停，不删产物腾挪）。

    相对路径先 ``resolve()``（Windows 下 ``Path('.').anchor`` 为空串会找不到盘符）；
    目标目录尚不存在时向上回溯到最近存在的祖先测量（同一卷，结果一致）。
    """
    resolved = Path(path).resolve()
    probe = resolved
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    required = int(required_bytes * multiplier)
    return DiskGateResult(
        ok=usage.free >= required,
        free_bytes=int(usage.free),
        required_bytes=required,
        multiplier=multiplier,
        checked_path=str(resolved),
        checked_at=datetime.now(UTC).isoformat(),
    )


# ---------------------------------------------------------------------------
# 批次合同 / 批次确认 / 成本台账 / 投放门禁（任务 1.4/2.1）
# ---------------------------------------------------------------------------


class BatchGates(BaseModel):
    """批次合同门禁（三层预算 + attempt + 单图异常 + 磁盘 + 下载错误率）。"""

    budget_expected_yuan: float
    manifest_budget_cap_yuan: float
    change_budget_cap_yuan: float = CHANGE_BUDGET_CAP_YUAN
    attempt_cap: int
    max_retries_gate: int = FROZEN_MAX_RETRIES_GATE
    single_image_baseline_yuan: float = FROZEN_BASELINE_COST_PER_IMAGE_YUAN
    single_image_pause_ratio: float = FROZEN_PAUSE_RATIO_THRESHOLD
    disk_gate_multiplier: float = DISK_GATE_MULTIPLIER
    download_max_consecutive_errors: int = 10
    download_max_failed_ratio: float = 0.2
    ocr_concurrency: int = FROZEN_CONCURRENCY
    ocr_max_attempts_per_image: int = FROZEN_MAX_ATTEMPTS_PER_IMAGE
    ocr_timeout_s: float = FROZEN_TIMEOUT_S
    download_concurrency: int = FROZEN_DOWNLOAD_CONCURRENCY


class BatchContract(BaseModel):
    """EXTFP4 批次合同（三层预算、门禁清单、停止条件、已知限制、授权引用）。"""

    contract_version: str = BATCH_CONTRACT_VERSION
    change_ref: str = CHANGE_REF
    created_at: str
    selection_manifest_path: str
    selection_manifest_sha256: str
    record_count: int
    asset_count: int
    gates: BatchGates
    stop_conditions: list[str] = Field(default_factory=list)
    known_limitations: list[dict[str, str]] = Field(default_factory=list)
    authorization_ref: str = (
        "openspec/changes/extfp4-production-batch（提案 + 用户实施授权 + 逐批批次确认）"
    )
    frozen_config_source: str = FROZEN_CONFIG_SOURCE


class BatchConfirmation(BaseModel):
    """一次批次确认记录（投放前用户确认内容的落盘留痕）。"""

    change_ref: str = CHANGE_REF
    batch_id: str
    confirmed_at: str
    decision: str = Field(description="仅 'approved' 允许投放")
    commit_sha: str = Field(description="投放前固定 commit")
    manifest_sha256: str = Field(description="selection_manifest SHA256（与合同一致）")
    task_count_cap: int = Field(description="本批图片数上限")
    batch_amount_cap_yuan: float = Field(description="本批金额上限（元）")
    cumulative_cost_before_yuan: float = Field(description="确认时 change 级已发生成本")
    confirmed_by: str = "user"
    stage: str = Field(default="download+ocr", description="适用阶段（download/ocr/download+ocr）")
    notes: str | None = None


class BatchCostEntry(BaseModel):
    """成本台账条目（逐任务求和口径，批次结束后登记）。"""

    batch_id: str
    stage: str
    cost_yuan: float
    images: int
    attempts: int
    recorded_at: str
    run_ref: str | None = None


class CostLedger(BaseModel):
    """change 级成本台账（change 硬上限的累计依据）。"""

    change_ref: str = CHANGE_REF
    entries: list[BatchCostEntry] = Field(default_factory=list)

    @property
    def total_cost_yuan(self) -> float:
        return round(sum(e.cost_yuan for e in self.entries), 6)

    @property
    def total_attempts(self) -> int:
        return sum(e.attempts for e in self.entries)


class DispatchBlockedError(RuntimeError):
    """投放门禁未通过（无确认/内容不符/超限）：fail-closed，不得投放。"""


def build_batch_contract(
    manifest: ProductionSelectionManifest,
    manifest_path: Path,
    *,
    authorization_ref: str | None = None,
) -> BatchContract:
    """由冻结的生产清单构建批次合同（门禁数值全部来自冻结决策，不临场发明）。

    ``change_ref`` 与 change 级硬上限从 manifest 继承（EXTFP4 默认不变；EXTFP6
    manifest 携带自身 change_ref 与 ≤¥10 上限）；``authorization_ref`` 缺省时
    保持 EXTFP4 授权引用文本。
    """
    return BatchContract(
        change_ref=manifest.change_ref,
        created_at=datetime.now(UTC).isoformat(),
        selection_manifest_path=manifest_path.as_posix(),
        selection_manifest_sha256=sha256_file(manifest_path),
        record_count=manifest.record_count,
        asset_count=manifest.asset_count,
        gates=BatchGates(
            budget_expected_yuan=manifest.budget_expected_yuan,
            manifest_budget_cap_yuan=manifest.budget_cap_yuan,
            change_budget_cap_yuan=manifest.change_budget_cap_yuan,
            attempt_cap=manifest.attempt_cap,
        ),
        stop_conditions=[c.value for c in AutoStopCondition],
        known_limitations=[{"id": kid, "text": text} for kid, text in KNOWN_LIMITATIONS],
        authorization_ref=(
            authorization_ref
            if authorization_ref is not None
            else BatchContract.model_fields["authorization_ref"].default
        ),
    )


def load_batch_confirmation(path: Path) -> BatchConfirmation:
    """读取批次确认记录；文件缺失或非法即抛错（fail-closed）。"""
    if not path.is_file():
        raise DispatchBlockedError(f"批次确认记录不存在: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise DispatchBlockedError(f"批次确认记录不可读: {exc}") from exc
    return BatchConfirmation.model_validate(data)


def assert_dispatch_allowed(
    contract: BatchContract,
    confirmation: BatchConfirmation,
    *,
    planned_images: int,
    manifest_sha256: str,
    ledger: CostLedger | None = None,
) -> None:
    """投放门禁：确认存在、decision=approved、manifest 一致、数量/金额不超限。

    任一不满足即抛 :class:`DispatchBlockedError`；累计金额按成本台账（逐任务
    求和口径）核对，本批承诺上限 + 已发生成本不得超过 change 级硬上限。
    """
    if confirmation.decision != "approved":
        raise DispatchBlockedError(
            f"批次 {confirmation.batch_id} 未获批准（decision={confirmation.decision!r}）"
        )
    if confirmation.change_ref != contract.change_ref:
        raise DispatchBlockedError(
            f"确认记录不属于本 change: {confirmation.change_ref!r}（合同 {contract.change_ref!r}）"
        )
    if confirmation.manifest_sha256 != contract.selection_manifest_sha256:
        raise DispatchBlockedError(
            "确认记录的 manifest SHA256 与合同不一致："
            f"{confirmation.manifest_sha256[:12]}… != {contract.selection_manifest_sha256[:12]}…"
        )
    if manifest_sha256 != contract.selection_manifest_sha256:
        raise DispatchBlockedError(
            "当前 selection_manifest 文件与合同冻结的 SHA256 不一致（清单被改动）"
        )
    if planned_images > confirmation.task_count_cap:
        raise DispatchBlockedError(
            f"计划投放 {planned_images} 张超出批次确认上限 {confirmation.task_count_cap} 张"
        )
    if planned_images > contract.asset_count:
        raise DispatchBlockedError(
            f"计划投放 {planned_images} 张超出合同资产数 {contract.asset_count}"
        )
    committed = confirmation.batch_amount_cap_yuan + confirmation.cumulative_cost_before_yuan
    if committed > contract.gates.change_budget_cap_yuan + 1e-9:
        raise DispatchBlockedError(
            f"批次金额上限 {confirmation.batch_amount_cap_yuan} + 确认前累计 "
            f"{confirmation.cumulative_cost_before_yuan} 超出 change 级硬上限 "
            f"{contract.gates.change_budget_cap_yuan}"
        )
    if (
        ledger is not None
        and abs(ledger.total_cost_yuan - confirmation.cumulative_cost_before_yuan) > 0.01
    ):
        raise DispatchBlockedError(
            f"成本台账累计 {ledger.total_cost_yuan} 与确认时点 "
            f"{confirmation.cumulative_cost_before_yuan} 不一致"
        )


def record_batch_cost(
    ledger_path: Path,
    entry: BatchCostEntry,
    *,
    contract: BatchContract | None = None,
    change_ref: str = CHANGE_REF,
) -> CostLedger:
    """登记批次成本并核对 change 级硬上限（超限即抛错，留痕不删除）。"""
    ledger = CostLedger(change_ref=change_ref)
    if ledger_path.is_file():
        ledger = CostLedger.model_validate(json.loads(ledger_path.read_text(encoding="utf-8")))
    ledger.entries.append(entry)
    if (
        contract is not None
        and ledger.total_cost_yuan > contract.gates.change_budget_cap_yuan + 1e-9
    ):
        raise DispatchBlockedError(
            f"登记后累计成本 {ledger.total_cost_yuan} 超出 change 级硬上限 "
            f"{contract.gates.change_budget_cap_yuan}（台账已保留，投放必须停止）"
        )
    _write_json_atomic(ledger_path, ledger.model_dump())
    return ledger


# ---------------------------------------------------------------------------
# §19.2 自动停止条件（任务 7.1）
# ---------------------------------------------------------------------------


class AutoStopCondition(StrEnum):
    """技术方案 §19.2 九项自动停止条件（全量写入下载与 OCR 运行门禁）。

    ``DISK_INSUFFICIENT`` 为本 change 级扩展门禁（磁盘不足即停，design D6），
    与 §19.2 九项一并写入批次合同停止条件清单。
    """

    SOURCE_ACCESS_CONTROL = "source_requires_access_control"
    ERROR_PLACEHOLDER_MIME_ANOMALY = "error_or_placeholder_or_mime_anomaly"
    EVIDENCE_MODIFIED = "evidence_or_manifest_modified"
    MODEL_MISMATCH = "qwen_model_mismatch"
    COST_CAP = "cost_hard_cap_reached"
    SCHEMA_PARSE_FAILURE = "schema_systematic_parse_failure"
    SCOPE_VIOLATION = "unauthorized_type_or_scope_in_manifest"
    CREDENTIAL_LEAK_RISK = "credential_leak_risk"
    REPAIR_OUT_OF_SCOPE = "repair_out_of_change_scope"
    DISK_INSUFFICIENT = "disk_insufficient"


class StopConditionTrigger(BaseModel):
    """一次停止条件触发记录（原因 + 数值 + 时刻，写入批次记录）。"""

    condition: str
    detail: str
    observed_value: float | str | None = None
    triggered_at: str


def evaluate_pre_dispatch_gates(
    contract: BatchContract,
    manifest: SelectionManifest,
    disk: DiskGateResult,
) -> list[StopConditionTrigger]:
    """投放前门禁评估：清单完整性/范围违规/磁盘不足（§19.2 第 3/7/磁盘项）。"""
    triggers: list[StopConditionTrigger] = []
    now = datetime.now(UTC).isoformat()
    if manifest.forbidden_domain_count > 0 or manifest.forbidden_domains:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.SCOPE_VIOLATION.value,
                detail="清单存在白名单外域候选（未授权范围混入）",
                observed_value=manifest.forbidden_domain_count,
                triggered_at=now,
            )
        )
    if not disk.ok:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.DISK_INSUFFICIENT.value,
                detail=(
                    f"磁盘剩余 {disk.free_bytes} 字节低于外推需求 {disk.required_bytes} 字节"
                    f"（×{disk.multiplier}），不足即停"
                ),
                observed_value=disk.free_bytes,
                triggered_at=now,
            )
        )
    return triggers


def make_download_stop_check(
    *,
    max_consecutive_errors: int,
) -> tuple[Callable[[Any], str | None], Callable[[], list[StopConditionTrigger]]]:
    """构造下载运行中的 §19.2 停止回调（连续错误率/访问控制）与事后触发清单。

    回调语义：任一任务完成后调用；返回 None 继续投放，返回字符串即停止投放
    （run_download 契约）。触发条件：
    1. 连续失败次数 ≥ ``max_consecutive_errors``（错误率异常）；
    2. 401/403 访问控制响应（来源要求登录/授权，立即停止）。
    整批失败率门槛由 :func:`evaluate_post_download` 事后评估承担。
    """
    consecutive = {"count": 0}
    triggers: list[StopConditionTrigger] = []

    def _trigger(condition: AutoStopCondition, detail: str, value: Any) -> str:
        triggers.append(
            StopConditionTrigger(
                condition=condition.value,
                detail=detail,
                observed_value=str(value),
                triggered_at=datetime.now(UTC).isoformat(),
            )
        )
        return detail

    def check(task: Any) -> str | None:
        error = getattr(task, "last_error", None)
        if error in {"http-401", "http-403"}:
            consecutive["count"] = 0
            return _trigger(
                AutoStopCondition.SOURCE_ACCESS_CONTROL,
                f"下载返回访问控制响应（{error}），来源可能要求登录/验证码",
                error,
            )
        if error is None:
            consecutive["count"] = 0
            return None
        consecutive["count"] += 1
        if consecutive["count"] >= max_consecutive_errors:
            return _trigger(
                AutoStopCondition.ERROR_PLACEHOLDER_MIME_ANOMALY,
                f"连续失败 {consecutive['count']} 次 ≥ 门槛 {max_consecutive_errors}",
                consecutive["count"],
            )
        return None

    def collect() -> list[StopConditionTrigger]:
        return list(triggers)

    return check, collect


def evaluate_post_download(
    record: Any,
    *,
    max_failed_ratio: float,
) -> list[StopConditionTrigger]:
    """下载运行后评估：失败率超门槛 / 访问控制响应（§19.2 第 1/2 项事后口径）。"""
    triggers: list[StopConditionTrigger] = []
    now = datetime.now(UTC).isoformat()
    tasks = list(getattr(record, "tasks", []) or [])
    if not tasks:
        return triggers
    failed = [t for t in tasks if getattr(t, "last_error", None) is not None]
    ratio = len(failed) / len(tasks)
    if ratio > max_failed_ratio:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.ERROR_PLACEHOLDER_MIME_ANOMALY.value,
                detail=f"下载失败率 {ratio:.2%} 超过门槛 {max_failed_ratio:.2%}",
                observed_value=round(ratio, 4),
                triggered_at=now,
            )
        )
    access = [t for t in failed if t.last_error in {"http-401", "http-403"}]
    if access:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.SOURCE_ACCESS_CONTROL.value,
                detail=f"{len(access)} 个任务返回访问控制响应（401/403）",
                observed_value=len(access),
                triggered_at=now,
            )
        )
    return triggers


def evaluate_post_ocr(record: Any) -> list[StopConditionTrigger]:
    """OCR 运行后评估：模型不一致 / 成本硬上限 / 凭证泄露风险（§19.2 第 4/5/8 项）。"""
    triggers: list[StopConditionTrigger] = []
    now = datetime.now(UTC).isoformat()
    tasks = list(getattr(record, "tasks", []) or [])
    mismatch = [t for t in tasks if t.error_code == "model_mismatch"]
    if mismatch:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.MODEL_MISMATCH.value,
                detail=f"{len(mismatch)} 个任务返回模型与固定模型不一致",
                observed_value=len(mismatch),
                triggered_at=now,
            )
        )
    leak = [t for t in tasks if t.error_code == "sensitive_content_in_response"]
    if leak:
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.CREDENTIAL_LEAK_RISK.value,
                detail=f"{len(leak)} 个原始响应疑似含凭证/敏感信息（fail-closed 未落盘）",
                observed_value=len(leak),
                triggered_at=now,
            )
        )
    cost = dict(getattr(record, "cost", {}) or {})
    if cost.get("limit_hit") == "cost_cap_yuan":
        triggers.append(
            StopConditionTrigger(
                condition=AutoStopCondition.COST_CAP.value,
                detail=f"成本达到硬上限 {cost.get('hard_cap_yuan')} 元，自动停止投放",
                observed_value=cost.get("total_cost_yuan"),
                triggered_at=now,
            )
        )
    return triggers


# ---------------------------------------------------------------------------
# 已知限制（结论强制引用；任务 6.3）
# ---------------------------------------------------------------------------

KNOWN_LIMITATIONS: tuple[tuple[str, str], ...] = (
    (
        "KL-1",
        "40 张样本量判别力局限：独立集复验 PASS 不构成对 H3=100% 或 H9≥99.5% 的正式认证",
    ),
    (
        "KL-2",
        "RV-OCRNEXT-C-01#F1：「双轮一致的确定性误关联」无规则防护，本批次不构成对该形态的防护证明",
    ),
    (
        "KL-3",
        "RV-OCRNEXT-C-01#N5：范围外样本仍留 accepted claim（无真值可判、不入分母）；"
        "本批次按 out_of_scope 标注隔离，消费策略归 EXTFP5 另案",
    ),
    (
        "KL-4",
        "独立集不可计算项：H9（单次运行）与 H3–H7（未确认草案不作真值）"
        "按披露数字处理，不静默套用原门槛",
    ),
)

FORBIDDEN_CERTIFICATION_PHRASES: tuple[str, ...] = (
    "H3 已获正式认证",
    "H9 已获正式认证",
    "H3/H9 已获正式认证",
    "正式认证 H3",
    "H3=100% 已认证",
    "H9≥99.5% 已认证",
)


def check_known_limitations_cited(text: str) -> tuple[list[str], list[str]]:
    """校验结论文本：返回（缺失引用的已知限制 ID，出现的违禁认证表述）。"""
    missing = [
        kid
        for kid, note in KNOWN_LIMITATIONS
        if kid not in text and note.split("：")[0] not in text
    ]
    forbidden = [phrase for phrase in FORBIDDEN_CERTIFICATION_PHRASES if phrase in text]
    return missing, forbidden


__all__ = [
    "ATTEMPT_CAP_MULTIPLIER",
    "BatchConfirmation",
    "BatchContract",
    "BatchCostEntry",
    "BatchGates",
    "CHANGE_BUDGET_CAP_YUAN",
    "CHANGE_REF",
    "CostLedger",
    "DISK_GATE_MULTIPLIER",
    "DispatchBlockedError",
    "DiskGateResult",
    "AutoStopCondition",
    "EXTFP6_CHANGE_BUDGET_CAP_YUAN",
    "EXTFP6_CHANGE_REF",
    "FULLHISTORY_FILTER_TEXT",
    "FULLHISTORY_PROFILE",
    "FULLHISTORY_RULE_TEXT",
    "FULLHISTORY_RULE_VERSION",
    "FROZEN_CONFIG_SOURCE",
    "FROZEN_CONCURRENCY",
    "FROZEN_MAX_ATTEMPTS_PER_IMAGE",
    "FROZEN_MAX_RETRIES_GATE",
    "FROZEN_TIMEOUT_S",
    "KNOWN_LIMITATIONS",
    "PRODUCTION_AVG_IMAGE_BYTES",
    "PRODUCTION_DATE_MAX",
    "PRODUCTION_DATE_MIN",
    "PRODUCTION_RULE_TEXT",
    "PRODUCTION_RULE_VERSION",
    "ProductionExclusionReport",
    "ProductionSelectionManifest",
    "StopConditionTrigger",
    "UNIT_COST_PER_IMAGE_YUAN",
    "assert_dispatch_allowed",
    "assert_frozen_request_contract",
    "build_batch_contract",
    "build_production_selection",
    "check_disk_gate",
    "check_known_limitations_cited",
    "compute_record_ids_hash",
    "evaluate_post_download",
    "evaluate_post_ocr",
    "evaluate_pre_dispatch_gates",
    "load_batch_confirmation",
    "make_download_stop_check",
    "production_ocr_run_config",
    "record_batch_cost",
    "sha256_file",
    "supported_community_names",
    "write_sha256_sidecar",
]
