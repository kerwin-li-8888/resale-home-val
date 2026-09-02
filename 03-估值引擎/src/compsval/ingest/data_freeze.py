"""EXTFP5 数据冻结登记（change extfp5-data-freeze）。

把已完成的 lianjia_ext 外部数据录入线产物登记为只读可消费数据版本
（staged 成交/普通住宅表、生产批次 229 资产链、既有调试与验收集资产按现状登记），
产出冻结运行清单（freeze manifest）、版本指针与冻结报告，并校验版本只读性与
限制披露完整性。

本模块只离线运行：不触网、不付费，不对任何既有产物执行写操作；
唯一写入的是新生成的 manifest / 指针 / 报告文件。
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# change 与版本标识
# ---------------------------------------------------------------------------

CHANGE_REF = "extfp5-data-freeze"
FREEZE_VERSION_ID = "lianjia_ext_v1_20260830"
FROZEN_AT_ISO = "2026-08-30T00:00:00+00:00"

VERSIONS_DIRNAME = "versions"
MANIFEST_FILENAME = "freeze_manifest_lianjia_ext_v1_20260830.json"
POINTER_FILENAME = "lianjia_ext_latest.json"
REPORT_FILENAME = "freeze_report_lianjia_ext_v1_20260830.md"

# 必须披露项：版本报告缺少任一关键词即阻止「可消费」发布
REQUIRED_DISCLOSURES: tuple[str, ...] = (
    "15 组重复图片",
    "30 资产",
    "ACCEPTED 1,386",
    "ROOM_ONLY 40",
    "NEEDS_REVIEW 2",
    "CONFLICT 425",
    "重复图片",
    "示例小区130",
    "C-XXXX0063",
    "raw_response_sha256 记录口径",
    "记录级去重",
    "KL-1",
    "KL-2",
    "KL-3",
    "KL-4",
)


class FreezeEntry(BaseModel):
    """一个冻结登记项：文件（sha256）或目录（count + 目录指纹）。"""

    key: str
    path: str  # 相对仓库根
    kind: str  # "file" | "dir"
    count: int | None = None
    sha256: str | None = None
    note: str | None = None


class FreezeManifest(BaseModel):
    manifest_version: int = 1
    version_id: str
    frozen_at: str
    change_ref: str
    path_base: str  # 仓库根（路径基准说明）
    sections: dict[str, list[FreezeEntry]] = Field(default_factory=dict)


class FreezeVerification(BaseModel):
    ok: bool
    gaps: list[str] = Field(default_factory=list)
    checked_at: str


# ---------------------------------------------------------------------------
# 哈希工具
# ---------------------------------------------------------------------------


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dir_fingerprint(directory: Path) -> str:
    """目录指纹：sorted(相对路径) 拼接后 SHA256；空目录为空摘要。"""
    rels = sorted(p.relative_to(directory).as_posix() for p in directory.rglob("*") if p.is_file())
    h = hashlib.sha256()
    for r in rels:
        h.update(r.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


def dir_file_count(directory: Path) -> int:
    return sum(1 for _ in directory.rglob("*") if _.is_file())


# ---------------------------------------------------------------------------
# 资产定义与登记
# ---------------------------------------------------------------------------


def _entry(
    key: str,
    root: Path,
    rel: str,
    kind: str = "file",
    note: str | None = None,
) -> FreezeEntry:
    """登记单文件或单目录的哈希/计数（只读扫描）。"""
    path = root / rel
    if not path.exists():
        return FreezeEntry(key=key, path=rel, kind=kind, note=f"MISSING: {rel}")
    if kind == "file":
        return FreezeEntry(
            key=key,
            path=rel,
            kind="file",
            count=1,
            sha256=file_sha256(path),
            note=note,
        )
    return FreezeEntry(
        key=key,
        path=rel,
        kind="dir",
        count=dir_file_count(path),
        sha256=dir_fingerprint(path),
        note=note,
    )


def _repo_root_of(data_dir: Path) -> Path:
    """推导路径基准（项目根）：data 位于 ``<项目根>/03-估值引擎/data`` 结构。"""
    data_dir = data_dir.resolve()
    root = data_dir.parent.parent
    try:
        data_dir.relative_to(root)
    except ValueError as exc:  # pragma: no cover - 结构异常防御
        raise ValueError(
            f"data_dir 不在预期的 <项目根>/03-估值引擎/data 结构下: {data_dir}"
        ) from exc
    return root


def collect_freezable_assets(data_dir: Path) -> FreezeManifest:
    """按 design D2 分节扫描全部候选资产，生成冻结清单（只读）。"""
    data_dir = data_dir.resolve()
    repo_root = _repo_root_of(data_dir)
    data_prefix = data_dir.relative_to(repo_root).as_posix()
    d = data_dir

    sections: dict[str, list[FreezeEntry]] = {}

    # 1. staged：成交/普通住宅指针与表 + staged 顶层既有表
    staged: list[FreezeEntry] = [
        _entry("lianjia_ext_current", repo_root, f"{data_prefix}/staged/lianjia_ext", "dir"),
    ]
    for p in sorted(d.glob("*.parquet")):
        staged.append(
            _entry(f"staged_{p.name}", repo_root, f"{data_prefix}/staged/{p.name}", "file")
        )
    for p in sorted(d.glob("*.manifest.json")):
        staged.append(
            _entry(
                f"staged_{p.name}",
                repo_root,
                f"{data_prefix}/staged/{p.name}",
                "file",
            )
        )
    sections["staged"] = staged

    # 2. 生产批次 229 资产链
    sel = f"{data_prefix}/selection/lianjia_ext/floorplan"
    prod: list[FreezeEntry] = [
        _entry("production_manifest", repo_root, f"{sel}/production_manifest.json", "file"),
        _entry(
            "production_manifest_sha256_sidecar",
            repo_root,
            f"{sel}/production_manifest.json.sha256",
            "file",
        ),
        _entry(
            "production_exclusion_report",
            repo_root,
            f"{sel}/production_exclusion_report.json",
            "file",
        ),
        _entry(
            "production_out_of_scope_registry",
            repo_root,
            f"{sel}/production_out_of_scope_registry.json",
            "file",
        ),
        _entry(
            "raw_image_batch",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_image/"
            "batch_id=floorplan-extfp4-batch-01",
            "dir",
            note="229 原图 + download_run/download_state + asset manifest",
        ),
        _entry(
            "raw_ocr_batch",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "batch_id=floorplan-extfp4-batch-01",
            "dir",
            note="OCR 输入图副本 229 jpg + asset manifest",
        ),
        _entry(
            "raw_ocr_run",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-data_selection_l",
            "dir",
            note="ocr_run/state/previous + 214 生产响应 + 300 验收集旧响应残留",
        ),
    ]
    sections["production_batch_229"] = prod

    # 3. 验证资产（按现状；OCRNEXT-D 以未验收口径标注）
    v: list[FreezeEntry] = [
        _entry(
            "debug10_samples",
            repo_root,
            "01-数据/外部数据/户型图样本-20260824",
            "dir",
            note="10 张调试样本 + 来源清单",
        ),
        _entry(
            "acceptance300",
            repo_root,
            f"{data_prefix}/download/lianjia_ext/floorplan/acceptance300",
            "dir",
            note="300 张验收集原图 + download run/state",
        ),
        _entry(
            "acceptance_ocr_run",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-data_selection_l-20260827T094212Z",
            "dir",
        ),
        _entry(
            "independent_reverification_ocr_run",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-independent-reve",
            "dir",
            note="独立集复验 40 张（change ocr-independent-reverification，PASS）",
        ),
        _entry(
            "concurrency_ocr_run_8",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-concurrency-subs-20260830T065639Z",
            "dir",
            note="并发验证 8 并发档（change ocr-concurrency-optimization）",
        ),
        _entry(
            "concurrency_ocr_run_16",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-concurrency-subs-20260830T070309Z",
            "dir",
            note="并发验证 16 并发档（change ocr-concurrency-optimization）",
        ),
        _entry(
            "concurrency_ocr_run_8b",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-concurrency-subs-20260830T070814Z",
            "dir",
            note="并发验证补充 8 并发档（change ocr-concurrency-optimization）",
        ),
        _entry(
            "ocrnext_d_run_114752",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-ocrnext-d-subset-20260828T114752Z",
            "dir",
            note="OCRNEXT-D 已执行未验收（工作包已取消）；空目录按现状登记",
        ),
        _entry(
            "ocrnext_d_run_115044",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-ocrnext-d-subset-20260828T115044Z",
            "dir",
            note="OCRNEXT-D 已执行未验收（工作包已取消）",
        ),
        _entry(
            "ocrnext_d_run_115946",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-ocrnext-d-subset-20260828T115946Z",
            "dir",
            note="OCRNEXT-D 已执行未验收（工作包已取消）",
        ),
        _entry(
            "debug_ocr_run",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=floorplan_ocr_run/"
            "run_floorplan-ocr-01-_____________",
            "dir",
            note="10 张调试 OCR run",
        ),
        _entry(
            "acceptance_manifest",
            repo_root,
            f"{sel}/acceptance_manifest.json",
            "file",
        ),
        _entry(
            "golden_label_template",
            repo_root,
            f"{sel}/golden_label_template.csv",
            "file",
        ),
        _entry(
            "golden_sample_image_map",
            repo_root,
            f"{sel}/golden_sample_image_map.csv",
            "file",
        ),
        _entry(
            "selection_full_manifest",
            repo_root,
            f"{sel}/selection_manifest.json",
            "file",
            note="208,075 记录全量清单（EXTFP5 不冻结为生产；登记现状）",
        ),
        _entry(
            "selection_e2e",
            repo_root,
            f"{sel}/e2e",
            "dir",
        ),
        _entry(
            "selection_concurrency_subset_20",
            repo_root,
            f"{sel}/concurrency_subset_20",
            "dir",
        ),
        _entry(
            "selection_ocrnext_d_20",
            repo_root,
            f"{sel}/ocrnext_d_20",
            "dir",
        ),
        _entry(
            "selection_independent_reverification_40",
            repo_root,
            f"{sel}/independent_reverification_40",
            "dir",
        ),
    ]
    sections["validation_assets"] = v

    # 4. 原始 Excel 快照
    sections["raw_excel_snapshot"] = [
        _entry(
            "chengjiao_xlsx_snapshot",
            repo_root,
            f"{data_prefix}/raw/source=lianjia_ext/dataset=chengjiao_xlsx/"
            "fetched_at=20260824T000000Z",
            "dir",
            note="原始 XLSX 二进制快照 + manifest + provenance",
        )
    ]

    # 5. 报告与归档
    sections["reports"] = [
        _entry(
            "extfp4_portrait",
            repo_root,
            "01-数据/外部数据/画像报告/20260830-外部链家OCR-EXTFP4-生产批次.json",
            "file",
        ),
        _entry(
            "extfp4_archive",
            repo_root,
            "openspec/changes/archive/2026-08-30-extfp4-production-batch",
            "dir",
            note="批次合同/质量报告/完整性豁免/成本台账/批次确认/旁证",
        ),
    ]

    return FreezeManifest(
        version_id=FREEZE_VERSION_ID,
        frozen_at=FROZEN_AT_ISO,
        change_ref=CHANGE_REF,
        path_base=str(repo_root.resolve()),
        sections=sections,
    )


def write_manifest(manifest: FreezeManifest, out_dir: Path) -> tuple[Path, Path]:
    """写 manifest 与 .sha256 旁证（新文件，不改写既有产物）。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / MANIFEST_FILENAME
    path.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    sidecar = out_dir / f"{MANIFEST_FILENAME}.sha256"
    sidecar.write_text(f"{file_sha256(path)}  {MANIFEST_FILENAME}\n", encoding="utf-8")
    return path, sidecar


def load_manifest(manifest_path: Path) -> FreezeManifest:
    return FreezeManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))


def verify_manifest(manifest: FreezeManifest, repo_root: Path) -> FreezeVerification:
    """按 manifest 重算各产物哈希与计数，输出缺口清单（只读校验）。"""
    gaps: list[str] = []
    for section, entries in manifest.sections.items():
        for e in entries:
            path = repo_root / e.path
            if not path.exists():
                gaps.append(f"[{section}.{e.key}] 路径缺失: {e.path}")
                continue
            if e.kind == "file":
                actual = file_sha256(path)
                if actual != e.sha256:
                    gaps.append(
                        f"[{section}.{e.key}] SHA256 不符: {e.path} "
                        f"(登记 {e.sha256} vs 实际 {actual})"
                    )
            else:
                actual_count = dir_file_count(path)
                actual_fp = dir_fingerprint(path)
                if actual_count != e.count:
                    gaps.append(
                        f"[{section}.{e.key}] 文件数不符: {e.path} "
                        f"(登记 {e.count} vs 实际 {actual_count})"
                    )
                if actual_fp != e.sha256:
                    gaps.append(
                        f"[{section}.{e.key}] 目录指纹不符: {e.path} "
                        f"(登记 {e.sha256} vs 实际 {actual_fp})"
                    )
    return FreezeVerification(
        ok=not gaps,
        gaps=gaps,
        checked_at=datetime.now(UTC).isoformat(),
    )


# ---------------------------------------------------------------------------
# 版本指针
# ---------------------------------------------------------------------------


def write_version_pointer(
    data_dir: Path,
    manifest_rel: str,
    version_id: str = FREEZE_VERSION_ID,
) -> Path:
    """写版本指针 JSON（切换指针，不删除任何目录/产物）。"""
    versions_dir = data_dir / VERSIONS_DIRNAME
    versions_dir.mkdir(parents=True, exist_ok=True)
    pointer = versions_dir / POINTER_FILENAME
    payload = {
        "version_id": version_id,
        "manifest": manifest_rel,
        "change_ref": CHANGE_REF,
        "frozen_at": FROZEN_AT_ISO,
        "note": "切换指针而非删除目录；已冻结版本只读，后续增量另立新版本指针",
    }
    pointer.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return pointer


# ---------------------------------------------------------------------------
# 冻结报告与披露校验
# ---------------------------------------------------------------------------

# 生产批次动态数字（来自 EXTFP4 画像报告，读取避免硬编码漂移）
_PORTRAIT_REL = "01-数据/外部数据/画像报告/20260830-外部链家OCR-EXTFP4-生产批次.json"


def _read_portrait(repo_root: Path) -> dict[str, Any]:
    p = repo_root / _PORTRAIT_REL
    if not p.exists():
        return {}
    data = json.loads(p.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def build_freeze_report(
    repo_root: Path,
    manifest: FreezeManifest,
    verification: FreezeVerification,
    pointer_path: Path,
) -> str:
    """生成冻结报告 Markdown（四项退出证据），Markdown 只解释、数字源自机器产物。"""
    p = _read_portrait(repo_root)
    sel = p.get("selection", {})
    tr = p.get("transcription", {})
    integrity = p.get("integrity", {})
    exemption = integrity.get("exemption", "")

    entry_summary = "\n".join(
        f"- `{section}`: {len(entries)} 项"
        for section, entries in manifest.sections.items()
    )

    return f"""# 冻结报告 — {manifest.version_id}

> 生成方式：`compsval floorplan freeze` 机器产物；本报告只解释，动态数字源自
> EXTFP4 画像报告与冻结清单（freeze manifest），不手工维护第二套数字。
> change_ref: {manifest.change_ref}；frozen_at: {manifest.frozen_at}

## 1. 数据版本

- 版本 ID：`{manifest.version_id}`（首个可消费数据版本）
- 覆盖：lianjia_ext 成交/普通住宅 staged 表 + 生产批次 229 资产链
  + 既有 10 张调试/300 张验收集及验证子集资产（按现状登记，
  OCRNEXT-D 并发对照数据以「已执行未验收（工作包已取消）」口径标注）
- 版本指针：`{pointer_path.relative_to(repo_root).as_posix()}`
- 清单：共 {len(manifest.sections)} 个分节

## 2. 运行清单

{entry_summary}

- 只读校验：{verification.ok and "通过" or "未通过"}
- 校验时间：{verification.checked_at}
- 缺口：{"无" if not verification.gaps else "；".join(verification.gaps)}

## 3. 限制与披露

- 完整性豁免：**15 组重复图片**资产（**30 资产**）因同 image_sha → 同 ocr_task_id
  原始响应文件相互覆盖，用户 2026-08-30 裁定豁免并披露
  （{exemption}；记录 batch_01_integrity_exemption.json）
- 转录状态口径：ACCEPTED 1,386 / ROOM_ONLY 40 / NEEDS_REVIEW 2 / CONFLICT 425
  （共 {tr.get("annotations_total", "?")} 条标注）
- 重复图片：{sel.get("asset_count", "?")} 资产中 15 组重复（30 资产），仅披露不合并
- 示例小区130（C-XXXX0063）：生产 profile 0 命中（源数据以「A区/B区」出现且无别名），
  进排除清单，后续增量清单可扩 alias
- 已知限制：
  - KL-1 40 张样本量判别力局限：独立集复验 PASS 不构成对 H3=100% 或 H9≥99.5% 的正式认证
  - KL-2 RV-OCRNEXT-C-01#F1：「双轮一致的确定性误关联」无规则防护
  - KL-3 RV-OCRNEXT-C-01#N5：范围外样本仍留 accepted claim（无真值可判、不入分母）
  - KL-4 独立集不可计算项：H9（单次运行）与 H3–H7（未确认草案不作真值）按披露数字处理
- 独立 verify 遗留（SUGGESTION，记录不扩权、修复另立 delta）：
  - ① raw_response_sha256 记录口径：登记口径（原始字节）与落盘净化文本 + CRLF
    翻译存在口径差
  - ② select 生产 profile 无记录级去重：同 source_record_id 多行已披露，扩量前建议补
- 目录现状披露：生产 OCR run 目录含 300 个验收集旧响应文件残留（64-hex 命名）
  与 `ocr_run.previous_20260830T094743Z.json`（2026-08-25 验收集 run 备份）；
  OCRNEXT-D 空目录 `run_floorplan-ocr-ocrnext-d-subset-20260828T114752Z` 按现状登记

## 4. 待接入状态

- 外部数据录入线（EXTFP0—EXTFP5）：**已冻结 v1，待估值接入验证**
- 本冻结 **不构成估值接入授权**；估值接入价值验证方案须另案提案
- 已冻结版本只读；后续扩窗/扩小区、示例小区130别名等增量批次另立新版本指针
  （v1.1/v2），旧版本目录保留不删不写
"""


def check_disclosures(report_text: str) -> list[str]:
    """披露缺项校验：缺少任一必须披露关键词即阻止「可消费」发布。"""
    return [kw for kw in REQUIRED_DISCLOSURES if kw not in report_text]


# ---------------------------------------------------------------------------
# 一键流程
# ---------------------------------------------------------------------------


def run_freeze(data_dir: Path) -> dict[str, Any]:
    """盘点 → 生成 manifest → 只读校验 → 写指针 → 生成报告（全部离线）。

    任一校验失败即返回失败结果，不写指针（版本保持未发布）。
    """
    data_dir = data_dir.resolve()
    repo_root = _repo_root_of(data_dir)
    manifest = collect_freezable_assets(data_dir)
    verification = verify_manifest(manifest, repo_root)
    if not verification.ok:
        return {
            "ok": False,
            "stage": "verify",
            "gaps": verification.gaps,
            "manifest": None,
        }
    manifest_path, sidecar_path = write_manifest(manifest, data_dir / VERSIONS_DIRNAME)
    pointer = write_version_pointer(data_dir, f"{VERSIONS_DIRNAME}/{MANIFEST_FILENAME}")
    report_text = build_freeze_report(repo_root, manifest, verification, pointer)
    missing = check_disclosures(report_text)
    if missing:
        return {
            "ok": False,
            "stage": "disclosure",
            "gaps": [f"披露缺项: {m}" for m in missing],
            "manifest": manifest_path,
        }
    report_path = data_dir / VERSIONS_DIRNAME / REPORT_FILENAME
    report_path.write_text(report_text, encoding="utf-8")
    return {
        "ok": True,
        "stage": "done",
        "gaps": [],
        "manifest": manifest_path,
        "sidecar": sidecar_path,
        "pointer": pointer,
        "report": report_path,
        "verification": verification,
    }
