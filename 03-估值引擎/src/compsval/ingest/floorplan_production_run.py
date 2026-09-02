"""EXTFP4 生产批次编排入口（下载 / OCR）：批次确认与门禁的强制执行层。

编排契约（specs ocr-production-batch）：

1. 投放门禁：``assert_dispatch_allowed`` —— 无批次确认、manifest SHA256 不符、
   数量/金额超限任一即 :class:`DispatchBlockedError`，不产生任何网络/付费请求；
2. 投放前门禁：清单范围违规（白名单外域）、磁盘门禁（外推 ×1.5，不足即停）；
3. 运行中停止：下载连续错误率/访问控制回调（§19.2），OCR 成本门禁由
   ``OcrCostGate`` 运行时强制（逐任务求和口径，先到即停）；
4. 运行后评估：模型不一致、凭证泄露风险、成本硬上限、失败率超门槛；
5. 成本台账：批次结束登记（逐任务求和口径），change 级 ≤300 元硬上限核对。

本模块自身不发起真实请求——真实请求发生在 ``run_download`` / ``run_ocr_batch``
内部，且必须先通过上述全部门禁。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from compsval.ingest.floorplan_batch_report import (
    build_consistency_report,
)
from compsval.ingest.floorplan_download import run_download
from compsval.ingest.floorplan_ocr import run_ocr_batch
from compsval.ingest.floorplan_production import (
    BatchContract,
    BatchCostEntry,
    CostLedger,
    DispatchBlockedError,
    StopConditionTrigger,
    assert_dispatch_allowed,
    check_disk_gate,
    evaluate_post_download,
    evaluate_post_ocr,
    evaluate_pre_dispatch_gates,
    load_batch_confirmation,
    make_download_stop_check,
    production_ocr_run_config,
    record_batch_cost,
    sha256_file,
)

PRODUCTION_ORCHESTRATION_VERSION = "EXTFP4-ORCH-1.0"


class ProductionSelectionManifestIO:
    """生产清单 JSON 读取（避免循环导入的轻量加载）。"""

    @staticmethod
    def load(path: Path) -> Any:
        from compsval.ingest.floorplan_production import (
            ProductionSelectionManifest,
        )

        return ProductionSelectionManifest.model_validate(
            json.loads(path.read_text(encoding="utf-8"))
        )


def _load_contract(contract_path: Path) -> BatchContract:
    if not contract_path.is_file():
        raise DispatchBlockedError(f"批次合同不存在: {contract_path}")
    return BatchContract.model_validate(json.loads(contract_path.read_text(encoding="utf-8")))


def _load_ledger(ledger_path: Path) -> CostLedger:
    if not ledger_path.is_file():
        return CostLedger()
    return CostLedger.model_validate(json.loads(ledger_path.read_text(encoding="utf-8")))


class ProductionRunResult(BaseModel):
    """一次生产阶段运行的结果（运行记录 + 停止触发 + 门禁证据）。"""

    stage: str
    batch_id: str
    run_ref: str | None = None
    stop_triggers: list[dict[str, Any]] = []
    dispatch_blocked: bool = False
    notes: list[str] = []


def run_production_download(
    manifest_path: Path,
    contract_path: Path,
    confirmation_path: Path,
    ledger_path: Path,
    out_dir: Path,
    *,
    transport: Any = None,
    timeout: float = 30.0,
) -> tuple[Any, ProductionRunResult]:
    """生产下载阶段：门禁 → 磁盘 → run_download（§19.2 回调）→ 事后评估。

    返回 (DownloadRunRecord, ProductionRunResult)；门禁不通过时抛
    :class:`DispatchBlockedError`（不产生任何网络请求）。
    """
    contract = _load_contract(contract_path)
    confirmation = load_batch_confirmation(confirmation_path)
    ledger = _load_ledger(ledger_path)
    manifest = ProductionSelectionManifestIO.load(manifest_path)
    actual_sha = sha256_file(manifest_path)

    assert_dispatch_allowed(
        contract,
        confirmation,
        planned_images=manifest.asset_count,
        manifest_sha256=actual_sha,
        ledger=ledger,
    )

    disk = check_disk_gate(out_dir, manifest.estimated_download_bytes)
    pre_triggers = evaluate_pre_dispatch_gates(contract, manifest, disk)
    if pre_triggers:
        raise DispatchBlockedError(
            "投放前门禁触发: " + "; ".join(f"{t.condition}: {t.detail}" for t in pre_triggers)
        )

    stop_check, collect_triggers = make_download_stop_check(
        max_consecutive_errors=contract.gates.download_max_consecutive_errors,
    )
    record = run_download(
        manifest,
        out_dir,
        max_concurrency=contract.gates.download_concurrency,
        max_attempts=contract.gates.ocr_max_attempts_per_image,
        timeout=timeout,
        transport=transport,
        manifest_path=manifest_path,
        stop_check=stop_check,
    )
    triggers = collect_triggers() + evaluate_post_download(
        record, max_failed_ratio=contract.gates.download_max_failed_ratio
    )
    result = ProductionRunResult(
        stage="download",
        batch_id=confirmation.batch_id,
        run_ref=record.run_dir,
        stop_triggers=[t.model_dump() for t in triggers],
        notes=[f"复用完成={record.state_counts.get('DOWNLOADED', 0)}"],
    )
    return record, result


def run_production_ocr(
    asset_manifest_path: Path,
    selection_manifest_path: Path,
    contract_path: Path,
    confirmation_path: Path,
    ledger_path: Path,
    out_raw_dir: Path,
    *,
    transport: Any = None,
    api_key: str | None = None,
) -> tuple[Any, ProductionRunResult]:
    """生产 OCR 阶段：门禁 → 冻结配置 → run_ocr_batch（并发 8）→ 事后评估。

    成本门禁运行时强制：``OcrCostGate`` 按逐任务求和口径实时累计，金额/图片数/
    重试任一先到即停（保留在飞结果与证据）；批次结束登记成本台账并核对 change
    级硬上限。门禁不通过时抛 :class:`DispatchBlockedError`（零付费请求）。
    """
    contract = _load_contract(contract_path)
    confirmation = load_batch_confirmation(confirmation_path)
    ledger = _load_ledger(ledger_path)
    manifest = ProductionSelectionManifestIO.load(selection_manifest_path)
    actual_sha = sha256_file(selection_manifest_path)

    assert_dispatch_allowed(
        contract,
        confirmation,
        planned_images=confirmation.task_count_cap,
        manifest_sha256=actual_sha,
        ledger=ledger,
    )

    # OCR 原始响应余量以原图外推为代理（×1.5 门禁兜底 + 批间复测，design D6/Risk）
    disk = check_disk_gate(out_raw_dir, manifest.estimated_download_bytes)
    pre_triggers = evaluate_pre_dispatch_gates(contract, manifest, disk)
    if pre_triggers:
        raise DispatchBlockedError(
            "投放前门禁触发: " + "; ".join(f"{t.condition}: {t.detail}" for t in pre_triggers)
        )

    config = production_ocr_run_config(
        max_images=confirmation.task_count_cap,
        hard_cap_yuan=confirmation.batch_amount_cap_yuan,
    )
    record = run_ocr_batch(
        asset_manifest_path,
        out_raw_dir,
        config=config,
        api_key=api_key,
        transport=transport,
        concurrency=contract.gates.ocr_concurrency,
        timeout=contract.gates.ocr_timeout_s,
        max_attempts=contract.gates.ocr_max_attempts_per_image,
    )

    triggers = evaluate_post_ocr(record)
    cost = dict(record.cost or {})
    derived = dict(cost.get("derived_from_tasks", {}) or {})
    entry_cost = float(derived.get("total_cost_yuan", cost.get("total_cost_yuan", 0.0)))
    images = int(derived.get("total_images", 0))
    attempts = images + int(derived.get("total_retries", 0))
    ledger = record_batch_cost(
        ledger_path,
        BatchCostEntry(
            batch_id=confirmation.batch_id,
            stage="ocr",
            cost_yuan=entry_cost,
            images=images,
            attempts=attempts,
            recorded_at=datetime.now(UTC).isoformat(),
            run_ref=record.run_dir,
        ),
        contract=contract,
    )

    # change 级 attempt 门禁事后核对（运行中由 max_retries/max_images 双门禁约束）
    attempt_cap = contract.gates.attempt_cap
    if ledger.total_attempts > attempt_cap:
        triggers.append(
            StopConditionTrigger(
                condition="attempt_cap_exceeded",
                detail=f"change 级累计 attempt {ledger.total_attempts} 超出合同上限 {attempt_cap}",
                observed_value=ledger.total_attempts,
                triggered_at=datetime.now(UTC).isoformat(),
            )
        )

    result = ProductionRunResult(
        stage="ocr",
        batch_id=confirmation.batch_id,
        run_ref=record.run_dir,
        stop_triggers=[t.model_dump() for t in triggers],
        notes=[f"成本={entry_cost} 元（逐任务求和口径）"],
    )
    consistency = build_consistency_report(record, Path(record.run_dir))
    _ = consistency  # 报告阶段复用；此处仅保证运行后立即可生成
    return record, result


__all__ = [
    "PRODUCTION_ORCHESTRATION_VERSION",
    "ProductionRunResult",
    "DispatchBlockedError",
    "run_production_download",
    "run_production_ocr",
]
