"""户型图 10 张真实试跑链路（EXTFP2-E，技术方案 §4.2/§4.3/§8.1/§11.4/§13）。

EXTFP2-E 把上四个原子交付串成一条**有界**真实试跑链路：
「样本只读登记 → 从全量 selection_manifest 重建 10 张子集 → 真实下载 → 原图资产与校验
→ 下载质量报告 / 链路证据」，用于双向验证下载字节与本地样本 SHA256 的一致性
（技术方案 §4.2 seed 采用决策 (a)：可用下载字节与本地样本 SHA256 比对，正确性与链路双向验证）。

范围边界（EXTFP2-E 合同）：
- 只读登记样本（不改样本、不改原始 XLSX、不改全量 selection_manifest）；
- 子集清单是派生的不可变证据，写入 ``data/selection/lianjia_ext/floorplan/e2e/``；
- 真实下载仅限本子集 10 张，域名白名单 ``ke-image.ljcdn.com``（模块级 fail-closed，绝不放宽）；
- 不做感知哈希、不做像素占位图识别、不做生产批量（EXTFP4）、不写密钥；
- 网络下载只在显式运行本 CLI 时发生；本模块函数本身可离线测试（不联网）。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_selection import (
    DOMAIN_WHITELIST,
    SelectionEntry,
    SelectionManifest,
    _normalize_https,
)

# 质量报告规则版本：随报告落盘，供复核与下游引用（EXTFP2-E-QREP-1.0）
QREP_RULES_VERSION = "EXTFP2-E-QREP-1.0"

# 本地 10 张样本清单（样本来源清单.md 表格头）
SAMPLE_LIST_FILE = "样本来源清单.md"
_SAMPLE_ROW_RE = re.compile(r"^\|\s*(huxingtu_\d+\.\w+)\s*\|\s*(\d+)\s*\|\s*(\S+)\s*\|")

# 子集清单默认输出目录（派生的不可变证据）
SUBSET_SUBDIR = "e2e"
SUBSET_FILENAME = "selection_manifest_e2e10.json"


@dataclass(frozen=True)
class SampleFile:
    """样本清单中的一条：本地文件名、来源 URL、本地字节 SHA256。"""

    filename: str
    url: str  # 原始 URL（http），仅用于与全量清单匹配
    local_sha256: str


class SampleRegistration(BaseModel):
    """样本只读登记证据：确认 10 张样本来自 XLSX ``户型图`` 字段且在 whitelist 内、
    全量清单可定位（只读，不改样本/源数据）。"""

    registration_rule_version: str = Field(default="EXTFP2-E-REG-1.0", description="登记规则版本")
    created_at: str
    source: str  # XLSX 户型图 字段（经 样本来源清单.md 溯源）
    domain_whitelist: list[str] = Field(default_factory=list)
    total_files: int
    samples: list[dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "每样本：file/url/domain/whitelisted/in_manifest/source_record_id/row_number/url_seq"
        ),
    )
    not_found_in_manifest: list[str] = Field(
        default_factory=list, description="样本 URL 在全量清单中无法定位的清单（应为空）"
    )


class E2eBundle(BaseModel):
    """一次 EXTFP2-E 试跑的聚合证据（下载运行 + 资产运行 + 注册）。"""

    created_at: str
    selection_ref: str  # 全量 selection_manifest 路径
    selection_rule_version: str
    subset_path: str
    subset_asset_count: int
    #: 子集目标基数（CX-EXTFP2-01 §5）：seed 命中 + 补选必须 >= 该值（fail-closed）
    expected_count: int = Field(default=10, description="e2e 子集目标资产基数")
    download_run_id: str
    downloader_version: str
    download_state_counts: dict[str, int] = Field(default_factory=dict)
    asset_batch_id: str
    asset_rules_version: str
    asset_counts: dict[str, int] = Field(default_factory=dict)
    domain_whitelist: list[str] = Field(default_factory=list)
    #: 累计证据溯源（CX-EXTFP2-01 §5）：本 bundle 聚合并集的全部下载 run_id / batch_id
    aggregated_download_run_ids: list[str] = Field(
        default_factory=list,
        description="累计聚合的全部下载 run_id（升序去重）",
    )
    aggregated_batch_ids: list[str] = Field(
        default_factory=list, description="累计聚合的全部资产 batch_id（升序去重）"
    )


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_sample_list(sample_dir: Path, sample_list_md: Path) -> list[SampleFile]:
    """只读解析「样本来源清单.md」的 file→url 表，并计算每个本地样本文件的 SHA256。

    只读不改写样本文件；样本清单缺失或某本地文件缺失时抛错（fail-closed）。
    """
    if not sample_list_md.is_file():
        raise FileNotFoundError(f"样本来源清单缺失: {sample_list_md}")
    files: list[SampleFile] = []
    for line in sample_list_md.read_text(encoding="utf-8").splitlines():
        m = _SAMPLE_ROW_RE.match(line.strip())
        if not m:
            continue
        filename, size_str, url = m.group(1), m.group(2), m.group(3)
        fpath = sample_dir / filename
        if not fpath.is_file():
            raise FileNotFoundError(f"样本文件缺失: {fpath}")
        data = fpath.read_bytes()
        if len(data) != int(size_str):
            raise ValueError(f"样本文件大小与清单不符 {filename}: {len(data)} != {size_str}")
        files.append(SampleFile(filename=filename, url=url, local_sha256=_sha256_hex(data)))
    if not files:
        raise ValueError(f"样本来源清单未解析到任何样本: {sample_list_md}")
    return files


def register_samples(
    full_manifest: SelectionManifest,
    sample_files: list[SampleFile],
    *,
    created_at: str | None = None,
) -> SampleRegistration:
    """把 10 张样本登记到只读证据：每样本确认域名在白名单、并在全量清单中按 URL 定位到
    source_record_id/row_number/url_seq。样本 URL 来自 XLSX ``户型图`` 字段（溯源见样本清单）。

    只读，不修改 full_manifest / 样本 / 源数据。
    """
    whitelist = set(DOMAIN_WHITELIST)
    by_norm: dict[str, SelectionEntry] = {}
    for e in full_manifest.records:
        by_norm.setdefault(e.normalized_url, e)

    samples: list[dict[str, Any]] = []
    not_found: list[str] = []
    for sf in sample_files:
        norm = _normalize_https(sf.url)
        entry = by_norm.get(norm)
        try:
            domain = urlsplit(norm).hostname or ""
        except ValueError:
            domain = ""
        samples.append(
            {
                "file": sf.filename,
                "url": sf.url,
                "normalized_url": norm,
                "domain": domain,
                "whitelisted": domain in whitelist,
                "in_manifest": entry is not None,
                "source_record_id": entry.source_record_id if entry else None,
                "row_number": entry.row_number if entry else None,
                "url_seq": entry.url_seq if entry else None,
            }
        )
        if entry is None:
            not_found.append(sf.filename)

    return SampleRegistration(
        created_at=created_at or datetime.now(UTC).isoformat(),
        source=(
            "外部链家成交 Excel『户型图』字段（经本地 户型图样本-20260824/样本来源清单.md 溯源）"
        ),
        domain_whitelist=sorted(whitelist),
        total_files=len(sample_files),
        samples=samples,
        not_found_in_manifest=not_found,
    )


def build_subset_manifest(
    full_manifest: SelectionManifest,
    sample_files: list[SampleFile],
    out_path: Path,
    *,
    selection_rule_version: str | None = None,
    expected_count: int = 10,
) -> SelectionManifest:
    """从全量清单重建 e2e 子集，样本命中不足 ``expected_count`` 时按稳定顺序补选并重建清单。

    补选决策（CX-EXTFP2-01 §5）：样本 URL 命中全量清单的条目优先（seed 命中）；命中不足
    ``expected_count`` 时，排除已命中 source_record_id 后，按既有稳定顺序 ``(row_number,
    url_seq)`` 从全量清单补足白名单候选。补选后仍不足 ``expected_count`` → 抛 ``ValueError``
    （fail-closed：样本/子集不足时不得生成成功 e2e 子集清单）。

    - record_count/asset_count 按子集重算；estimated/storage/budget 按最终资产数重算；
    - record_ids_hash 重算（排序后 source_record_id 的 SHA256）；
    - ``*.incomplete`` 原子写盘（不可变证据）。
    """
    by_norm: dict[str, SelectionEntry] = {}
    for e in full_manifest.records:
        by_norm.setdefault(e.normalized_url, e)

    picked: list[SelectionEntry] = []
    picked_src: set[str] = set()
    for sf in sample_files:
        hit = by_norm.get(_normalize_https(sf.url))
        if hit is None:
            continue
        if hit.source_record_id in picked_src:  # 多样本同 URL 命中同一记录：只取一次
            continue
        picked.append(hit)
        picked_src.add(hit.source_record_id)

    # 补选：seed 命中不足 expected_count 时，排除已命中记录，按稳定顺序 (row_number, url_seq) 补足
    if len(picked) < expected_count:
        ordered = sorted(full_manifest.records, key=lambda entry: (entry.row_number, entry.url_seq))
        for entry in ordered:
            if len(picked) >= expected_count:
                break
            if entry.source_record_id in picked_src:
                continue
            picked.append(entry)
            picked_src.add(entry.source_record_id)

    if len(picked) < expected_count:
        raise ValueError(
            f"子集资产不足 expected_count={expected_count}：seed 命中 + 补选仅 {len(picked)}，"
            "禁止生成成功 e2e 子集（fail-closed）"
        )

    # 确定性排序 + 哈希（与 EXTFP2-B 一致：SHA256 of sorted(source_record_id)）
    picked.sort(key=lambda entry: (entry.row_number, entry.url_seq))
    record_ids_sorted = sorted(picked_src)
    digest = hashlib.sha256()
    digest.update("\n".join(record_ids_sorted).encode("utf-8"))
    record_ids_hash = digest.hexdigest()
    asset_count = len(picked)
    avg = full_manifest.avg_bytes_estimate
    estimated = asset_count * avg
    storage_mult = full_manifest.storage_cap_bytes / max(full_manifest.estimated_download_bytes, 1)
    storage_cap = int(estimated * storage_mult)
    budget = round(
        asset_count * (full_manifest.budget_cap_yuan / max(full_manifest.asset_count, 1)),
        2,
    )

    subset = SelectionManifest(
        selection_rule_version=selection_rule_version or full_manifest.selection_rule_version,
        selection_rule_text=full_manifest.selection_rule_text,
        snapshot_ref=full_manifest.snapshot_ref,
        run_id=full_manifest.run_id,
        geoscope=full_manifest.geoscope,
        date_range_min=full_manifest.date_range_min,
        date_range_max=full_manifest.date_range_max,
        filter_condition=full_manifest.filter_condition,
        record_count=len(record_ids_sorted),
        asset_count=asset_count,
        forbidden_domain_count=0,
        forbidden_domains=[],
        record_ids_hash=record_ids_hash,
        records=picked,
        record_sample=None,
        domain_whitelist=sorted(DOMAIN_WHITELIST),
        estimated_download_bytes=estimated,
        storage_cap_bytes=storage_cap,
        budget_cap_yuan=budget,
        workpackage_ref="EXTFP2",
        avg_bytes_estimate=avg,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    work = out_path.with_suffix(out_path.suffix + ".incomplete")
    work.write_text(subset.model_dump_json(indent=2), encoding="utf-8")
    work.replace(out_path)
    return subset


def resolve_missing_assets(
    subset_records: list[SelectionEntry],
    snapshot_ref: str | None,
    existing_downloaded_asset_ids: set[str],
) -> list[SelectionEntry]:
    """筛出子集记录中尚未以 DOWNLOADED 落盘的资产（供仅补下缺失，遵守网络预算）。

    CX-EXTFP2-01 §5：网络授权只覆盖剩余次数、总数不超过 10；对既有已成功下载的资产
    （按确定性幂等 asset_id 判定）绝不重复下发。asset_id 计算与下载器完全一致
    （sale_record_key + url_ordinal）。
    """
    from compsval.ingest.floorplan_download import (
        FALLBACK_SNAPSHOT_ID,
        compute_asset_id,
        sale_record_key,
    )

    snapshot = snapshot_ref or FALLBACK_SNAPSHOT_ID
    missing: list[SelectionEntry] = []
    for entry in subset_records:
        sale_key = sale_record_key(snapshot, entry.row_number, entry.source_record_id)
        asset_id = compute_asset_id(sale_key, entry.url_seq)
        if asset_id not in existing_downloaded_asset_ids:
            missing.append(entry)
    return missing


def build_download_manifest(
    full_manifest: SelectionManifest,
    entries: list[SelectionEntry],
) -> SelectionManifest:
    """由给定子集条目序列构建一份下载用 selection_manifest（不写盘）。

    仅承载待下载的缺额资产（resolve_missing_assets 的输出），供 run_download 在预算内
    只补下缺失部分；元数据字段沿用全量清单，record_count/asset_count 按 entries 重算。
    """
    picked = sorted(entries, key=lambda entry: (entry.row_number, entry.url_seq))
    record_ids_sorted = sorted({e.source_record_id for e in picked})
    digest = hashlib.sha256()
    digest.update("\n".join(record_ids_sorted).encode("utf-8"))
    asset_count = len(picked)
    avg = full_manifest.avg_bytes_estimate
    estimated = asset_count * avg
    storage_mult = full_manifest.storage_cap_bytes / max(full_manifest.estimated_download_bytes, 1)
    budget = round(
        asset_count * (full_manifest.budget_cap_yuan / max(full_manifest.asset_count, 1)),
        2,
    )
    return SelectionManifest(
        selection_rule_version=full_manifest.selection_rule_version,
        selection_rule_text=full_manifest.selection_rule_text,
        snapshot_ref=full_manifest.snapshot_ref,
        run_id=full_manifest.run_id,
        geoscope=full_manifest.geoscope,
        date_range_min=full_manifest.date_range_min,
        date_range_max=full_manifest.date_range_max,
        filter_condition=full_manifest.filter_condition,
        record_count=len(record_ids_sorted),
        asset_count=asset_count,
        forbidden_domain_count=0,
        forbidden_domains=[],
        record_ids_hash=digest.hexdigest(),
        records=picked,
        record_sample=None,
        domain_whitelist=sorted(DOMAIN_WHITELIST),
        estimated_download_bytes=estimated,
        storage_cap_bytes=int(estimated * storage_mult),
        budget_cap_yuan=budget,
        workpackage_ref="EXTFP2",
        avg_bytes_estimate=avg,
    )


# 资产 manifest 文件名（与 floorplan_asset.ASSET_MANIFEST_FILENAME 一致；避免循环导入）
_ASSET_MANIFEST_FILENAME = "floorplan_asset_manifest.json"


def load_asset_run_manifests(raw_dir: Path) -> list[dict[str, Any]]:
    """加载数据湖 ``raw_dir/batch_id=*`` 下全部资产 manifest（累计证据源，升序）。

    返回原始 dict 列表（每个对应一次 EXTFP2-D 资产化运行）。绝不改写原始字节。
    """
    runs: list[dict[str, Any]] = []
    if not raw_dir.is_dir():
        return runs
    for batch in sorted(raw_dir.glob("batch_id=*")):
        mf = batch / _ASSET_MANIFEST_FILENAME
        if mf.is_file():
            runs.append(json.loads(mf.read_text(encoding="utf-8")))
    return runs


def collect_existing_downloaded_asset_ids(raw_dir: Path) -> set[str]:
    """已落盘 DOWNLOADED 资产的 asset_id 集合（供补下缺失、遵守网络预算）。

    CX-EXTFP2-01 §5：对既有已成功下载的资产（按确定性幂等 asset_id 判定）绝不重复
    下发；只补下子集清单中缺失的余量。
    """
    ids: set[str] = set()
    for run in load_asset_run_manifests(raw_dir):
        for a in run.get("assets", []) or []:
            if a.get("asset_status") == "DOWNLOADED" and a.get("asset_id"):
                ids.add(a["asset_id"])
    return ids


def aggregate_bundle_counts(
    manifests: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    """由累计资产 manifest 聚合下载计数与资产生态计数（CX-EXTFP2-01 §5 累计证据）。

    - ``download_counts``：DOWNLOADED = 累计 DOWNLOADED 资产数（成功下载即成功资产）；
    - ``asset_counts``：DOWNLOADED / IMAGE_INVALID 按累计平铺统计；NOT_AVAILABLE 在
      manifest 中不单列，置 0（累计证据只呈现已落盘资产）；
    - 只读，绝不改写任何 manifest。
    """
    downloaded = 0
    invalid = 0
    for run in manifests:
        for a in run.get("assets", []) or []:
            status = a.get("asset_status")
            if status == "DOWNLOADED":
                downloaded += 1
            elif status == "IMAGE_INVALID":
                invalid += 1
    return (
        {"DOWNLOADED": downloaded},
        {"DOWNLOADED": downloaded, "IMAGE_INVALID": invalid, "NOT_AVAILABLE": 0},
    )


def build_download_quality_report(
    bundle: E2eBundle,
    *,
    sample_registration: SampleRegistration,
    asset_manifests: list[dict[str, Any]],
    local_files: list[SampleFile],
    out_dir: Path,
) -> tuple[Path, Path]:
    """生成下载质量报告（JSON + Markdown 同一内容），含样本 SHA256 双向比对。

    - JSON 与 MD 描述同一冻结报告（review_rules §7 / §15）；
    - ``asset_manifests`` 为累计聚合的资产 manifest 列表（CX-EXTFP2-01 §5：输出累计
      证据，跨全部 batch_id 平铺逐一资产行）；
    - 逐资产行含 mime/扩展名/尺寸/字节/SHA256/重复标记；
    - sample comparison：按 URL 把本地样本映射到资产，比对局部 SHA256 与下载 SHA256。
    """
    if not asset_manifests:
        raise FileNotFoundError("资产 manifest 列表为空，无法生成质量报告")

    asset_rows: list[dict[str, Any]] = []
    for asset_run in asset_manifests:
        for a in asset_run.get("assets", []) or []:
            asset_rows.append(
                {
                    "source_record_id": a.get("source_record_id"),
                    "url_ordinal": a.get("url_ordinal"),
                    "batch_id": asset_run.get("batch_id"),
                    "asset_status": a.get("asset_status"),
                    "http_status": a.get("http_status"),
                    "mime_type": a.get("mime_type"),
                    "file_extension": a.get("file_extension"),
                    "width": a.get("width"),
                    "height": a.get("height"),
                    "byte_size": a.get("byte_size"),
                    "sha256": (a.get("sha256") or "")[:12],
                    "is_duplicate": a.get("is_duplicate"),
                    "storage_path": a.get("storage_path"),
                }
            )

    # 样本 SHA256 双向比对：本地样本 vs 下载资产（用注册的 source_record_id+url_seq 定位资产）
    sample_rows: list[dict[str, Any]] = []
    match = 0
    mismatch = 0
    unmatched: list[str] = []
    assets_by_src_ord: dict[tuple[str | None, int | None], dict[str, Any]] = {}
    for asset_run in asset_manifests:
        for a in asset_run.get("assets", []) or []:
            assets_by_src_ord[(a.get("source_record_id"), a.get("url_ordinal"))] = a
    for sf in local_files:
        reg = next(
            (s for s in sample_registration.samples if s.get("file") == sf.filename),
            None,
        )
        key = (reg.get("source_record_id"), reg.get("url_seq")) if reg else None
        asset = assets_by_src_ord.get(key) if key else None
        if asset is None or not asset.get("sha256"):
            unmatched.append(sf.filename)
            sample_rows.append({"file": sf.filename, "match": "NOT_AVAILABLE"})
            continue
        ok = asset.get("sha256") == sf.local_sha256
        match += 1 if ok else 0
        mismatch += 0 if ok else 1
        sample_rows.append(
            {
                "file": sf.filename,
                "match": "MATCH" if ok else "MISMATCH",
                "local_sha256": sf.local_sha256[:12],
                "asset_sha256": asset.get("sha256"),
            }
        )

    report: dict[str, Any] = {
        "rules_version": QREP_RULES_VERSION,
        "created_at": bundle.created_at,
        "selection_ref": bundle.selection_ref,
        "subset_path": bundle.subset_path,
        "expected_count": bundle.expected_count,
        "subset_asset_count": bundle.subset_asset_count,
        "download_run_id": bundle.download_run_id,
        "aggregated_download_run_ids": bundle.aggregated_download_run_ids,
        "downloader_version": bundle.downloader_version,
        "download_state_counts": bundle.download_state_counts,
        "asset_batch_id": bundle.asset_batch_id,
        "aggregated_batch_ids": bundle.aggregated_batch_ids,
        "asset_counts": bundle.asset_counts,
        "domain_whitelist": bundle.domain_whitelist,
        "assets": asset_rows,
        "sample_comparison": {
            "total_files": len(local_files),
            "sha256_match": match,
            "sha256_mismatch": mismatch,
            "not_available": len(unmatched),
            "unmatched_files": unmatched,
            "rows": sample_rows,
        },
        "sample_registration": sample_registration.model_dump(),
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_out = out_dir / "download_quality_report.json"
    json_out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown 与 JSON 同源渲染（保证同一冻结报告）
    lines: list[str] = [
        "# 户型图 10 张试跑 下载质量报告",
        "",
        f"> 规则版本：`{QREP_RULES_VERSION}`｜创建：{bundle.created_at}",
        f"> 全量清单：`{bundle.selection_ref}`｜子集：`{bundle.subset_path}`",
        f"> 目标基数：expected_count={bundle.expected_count}｜子集：{bundle.subset_asset_count}",
        "",
        "## 下载",
        "",
        f"- run_id：`{bundle.download_run_id}`｜下载器版本：`{bundle.downloader_version}`",
        f"- state_counts：`{json.dumps(bundle.download_state_counts, ensure_ascii=False)}`",
        (
            f"- 聚合 run_id：`{', '.join(bundle.aggregated_download_run_ids)}`"
            if bundle.aggregated_download_run_ids
            else ""
        ),
        "",
        "## 资产化与校验",
        "",
        f"- batch_id：`{bundle.asset_batch_id}`｜资产规则：`{bundle.asset_rules_version}`",
        f"- asset_counts：`{json.dumps(bundle.asset_counts, ensure_ascii=False)}`",
        (
            f"- 聚合 batch_id：`{', '.join(bundle.aggregated_batch_ids)}`"
            if bundle.aggregated_batch_ids
            else ""
        ),
        f"- 域名白名单：`{', '.join(bundle.domain_whitelist)}`（fail-closed）",
        "",
        "## 资产明细",
        "",
        "| batch | src_record | ord | status | mime | ext | 尺寸 | bytes | sha256 | dup |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in asset_rows:
        wh = r.get("width")
        ht = r.get("height")
        dim = f"{wh}x{ht}" if (wh is not None and ht is not None) else "-"
        lines.append(
            f"| {r.get('batch_id')} | {r.get('source_record_id')} | {r.get('url_ordinal')} | "
            f"{r.get('asset_status')} | {r.get('mime_type')} | {r.get('file_extension')} | "
            f"{dim} | {r.get('byte_size')} | {r.get('sha256')} | {r.get('is_duplicate')} |"
        )
    lines += [
        "",
        "## 样本 SHA256 双向比对（本地样本 vs 下载资产）",
        "",
        "- 总计：{} 张".format(report["sample_comparison"]["total_files"]),
        "- SHA256 一致：{} 张".format(report["sample_comparison"]["sha256_match"]),
        "- SHA256 不一致：{} 张".format(report["sample_comparison"]["sha256_mismatch"]),
        "- 不可比对（未定位资产）：{} 张".format(report["sample_comparison"]["not_available"])
        + (
            f"：{', '.join(report['sample_comparison']['unmatched_files'])}"
            if report["sample_comparison"]["unmatched_files"]
            else ""
        ),
        "",
        "| 文件 | 比对 | local_sha256(12) | asset_sha256 |",
        "|---|---|---|---|",
    ]
    for r in report["sample_comparison"]["rows"]:
        lines.append(
            f"| {r.get('file')} | {r.get('match')} | {r.get('local_sha256')} | "
            f"{r.get('asset_sha256') or '-'} |"
        )
    lines.append("")
    md_out = out_dir / "download_quality_report.md"
    md_out.write_text("\n".join(lines), encoding="utf-8")
    return json_out, md_out


__all__ = [
    "E2eBundle",
    "QREP_RULES_VERSION",
    "SampleFile",
    "SampleRegistration",
    "SUBSET_FILENAME",
    "build_download_manifest",
    "build_download_quality_report",
    "build_subset_manifest",
    "collect_existing_downloaded_asset_ids",
    "aggregate_bundle_counts",
    "load_asset_run_manifests",
    "parse_sample_list",
    "register_samples",
    "resolve_missing_assets",
]
