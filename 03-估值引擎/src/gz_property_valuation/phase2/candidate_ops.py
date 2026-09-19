# -*- coding: utf-8 -*-
"""phase2 S6-A 候选模式调用编排：资产加载＋指纹强制核对、estimate/batch/replay 入口、
成套结果（中心/区间/状态/支持依据/限制/版本指纹）与 Markdown 报告。

行为规格（openspec/changes/prepare-phase2-s6-candidate-ops/specs/phase2-candidate-ops/spec.md
「结果成套输出与版本指纹」「三路径推理一致性」「候选输出不接正式链路」；design D1）：

- 入口为 ``python -m gz_property_valuation.phase2.candidate_ops <estimate|batch|replay|versions>``，
  **不修改既有 cli.py**；纯新增模块。
- 资产加载即强制指纹核对：V1 编码器/权重对 S3 run 自登记值与 S4-B 登记值双检；区间表对
  S4C run manifest（默认 S4C；``--s4c-run`` 可覆盖）核对，降级变体权重、冻结合同与登记文件
  对 S4-B manifest 登记值核对；任一不符即 ``OpsHalt`` 停线（拒绝输出）。
- 版本成套指纹：模型/特征/市场资产/校准/协调策略五项各带 SHA-256 摘要与成套 bundle id；
  错配（加载值与登记代次不符）即拒绝，**不产生部分混用输出**。区间资产换代为缺陷修复：
  仅 calibration 代次更新，其余四组件摘要不变（区间-only 改动的机械证明）。
- 每份结果成套保存六要素；主要限制逐条显式（区间循环性限度、A1 辅助不覆盖 M1、降级路径等）。
  区间披露升级：命中时 interval 字段登记命中层、该层样本量与残差来源；未命中时主要限制
  输出含分层名的明确提示（design D6）。
- 候选输出不接入任何正式链路：本模块无任何正式消费者，可整体停用。

只读消费 V1 推理资产与冻结合同；不修改任何既有实现。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
    os.environ.setdefault(_v, "1")

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

from . import a1_experiment as a1  # noqa: E402
from . import baselines  # noqa: E402
from . import candidate_inference as ci  # noqa: E402
from . import candidate_request as cr  # noqa: E402
from . import formal_binding as fb  # noqa: E402
from . import formal_gate as fg  # noqa: E402
from . import formal_states as fs  # noqa: E402
from . import lineage  # noqa: E402
from . import release_record as rr  # noqa: E402
from . import stop_switch as sw  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[4]
_RUNS_DIR = REPO_ROOT / "examples" / "phase2_demo" / "runs"
DEFAULT_S1_RUN = _RUNS_DIR / "demo_s1_run"
DEFAULT_S3_RUN = _RUNS_DIR / "demo_s3_run"
DEFAULT_S4B_RUN = _RUNS_DIR / "demo_s4b_run"
DEFAULT_S4C_RUN = _RUNS_DIR / "demo_s4c_run"
# build-phase2-block-taxonomy-map design D3（运行时权威路径定死；run 内副本仅留证）
DEFAULT_BLOCK_MAP_PATH = (REPO_ROOT / "examples" / "phase2_demo" / "block_map"
                          / "current-block-map.json")
# enable-phase2-conditional-release D4：正式模式运行时状态目录（数据区；默认不存在＝
# 未发布/未停止/无停用）。测试与回归一律用 --state-dir 指向临时目录，不污染真实数据区。
OPS_STATE_DIR = REPO_ROOT / "03-估值引擎" / "data" / "phase2" / "ops-state"
FORMAL_RECORDS_FILENAME = "formal-records.jsonl"
ADOPTION_CONTRACT_PATH = (REPO_ROOT / "examples" / "phase2_demo" / "adoption-contract.md")
ADOPTION_CONTRACT_ID = "DEMO-RELEASE-001"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

OPS_VERSION = "s6a-candidate-ops-v1"
DEGRADATION_PATH = "B"
CORRECTION_STRATUM_RULE = "stratum = <degradation_state>|<b0_level>；层缺失→<deg>|global→global"
B0_FALLBACK_CHAIN = ["community", "block", "district"]
COORDINATION_POLICY = {
    "id": "s6a-coordination-v1",
    "degradation_path": DEGRADATION_PATH,
    "b0_fallback_chain": B0_FALLBACK_CHAIN,
    "stratum_rule": CORRECTION_STRATUM_RULE,
    "a1_conflict_rule": "|A1 修正后参考价 − M1 报价| / M1 报价 > 阈值 → a1_m1_conflict（不仲裁不隐藏）",
    "interval_residual_space": "log 空间经验分位（e = log 真值 − log 预测）",
    # fix-phase2-request-mapping D4：A1 不可用时的 M1-B0 偏离护栏；阈值与 A1 冲突阈值同源
    # （candidate_inference.CONFLICT_THRESHOLD_DEFAULT），运行前登记冻结，执行期不改
    "m1_b0_divergence_rule": "A1 不可用（a1_status≠ok）且 B0 参照非空且 |M1 报价 − B0 参照| / B0 参照 "
                             "> 阈值 → m1_b0_divergence（仅标注，不仲裁不隐藏，不改 decision）",
    "m1_b0_divergence_threshold": ci.CONFLICT_THRESHOLD_DEFAULT,
}

# 版本成套指纹登记代次（V1 静态冻结）。基准代次来自 S6-A run 冻结登记
# frozen-registration.json version_bundle；fix-phase2-interval-normal-strata 区间资产换代
# （S4C 换代 run）后仅 calibration 组件代次重绑；fix-phase2-request-
# mapping 新增 m1_b0_divergence 护栏（D4/D8）后仅 coordination_policy 组件代次重绑，
# model/feature/market_asset/calibration 四组件摘要逐项不变。资产代次变更须登记新版本并
# 更新此处，不改既有实现。加载时强制核对为默认行为，不依赖调用方自觉。
REGISTERED_BUNDLE_ID = "64268cfed1a33b56270b4a440bc4e5c61d45cfb8144a313506a68ec6b0b66adc"
REGISTERED_BUNDLE_DIGESTS = {
    "model": "d1e75f98db3ca4fb0236049897e2c33e9216cb672c1052bfec6f5ea6d4af20b3",
    "feature": "79cda3d47cd56332a985252090bc0ae8de45be354647f80818dde9cd3b13566d",
    "market_asset": "6a45bb4b004efdab489d90d453b4d692b780cc80c4e4d7e83ae5fc4bc87de0bb",
    "calibration": "2cca3818a8b942ab321fa21821eca9074167eabc200f1658dd3e164892792b7e",
    "coordination_policy": "d0dd625b2b7fec0af0242891855c7c309a114e8c9f931ee4dbe475c981fffe66",
}


class OpsHalt(RuntimeError):
    """停线上报：资产指纹不符、版本错配等停止条件触发（拒绝输出）。"""


# ---------------------------------------------------------------- 版本成套指纹

def _digest_of(pairs: dict) -> str:
    return sha256_bytes(
        json.dumps(pairs, ensure_ascii=False, sort_keys=True).encode("utf-8"))


def build_version_bundle(s1_run: Path, s3_run: Path, s4b_run: Path, s4c_run: Path,
                         assets: dict, variant_weights: np.ndarray,
                         interval_table: dict, conflict_threshold: float) -> dict:
    """五项版本指纹（模型/特征/市场资产/校准/协调策略）＋成套 bundle id。

    区间表（calibration 组件）取自 ``s4c_run``（区间资产换代后的新 run）；其余冻结资产
    （降级变体权重）仍取自 ``s4b_run``。
    """
    s1_master = s1_run / "master_table.parquet"
    s1_feature_dict = s1_run / "feature_dictionary.json"
    interval_path = s4c_run / "interval" / "interval-table.json"
    variant_path = s4b_run / "degradation" / "path-b-variant" / f"{ci.FOLD_ID}.npy"

    model = {
        "version": ci.VERSION_ID, "fold": ci.FOLD_ID, "train_cutoff": ci.TRAIN_CUTOFF,
        "weights_sha256": assets["weights_sha256"],
        "encoder_sha256": assets["encoder_sha256"],
        "n_features": assets["n_features"],
    }
    feature = {
        "feature_dictionary_sha256": lineage.sha256_file(s1_feature_dict),
        "n_features": assets["n_features"],
        "feature_names_sha256": _digest_of({str(i): n for i, n in
                                            enumerate(assets["feature_names"])}),
    }
    market_asset = {
        "s1_master_sha256": lineage.sha256_file(s1_master),
        "window_days": ci.WINDOW_DAYS,
        "b0_source": "S1 冻结主表按锚点前 365 天现算（community→block→district）",
        "note": "版本代标识不含请求锚点；锚点为请求级参数，随每份结果 top-level anchor 记录",
    }
    calibration = {
        "interval_table_sha256": lineage.sha256_file(interval_path),
        "layers": sorted(interval_table.get("layers", {})),
        "nominal_levels": interval_table.get("nominal_levels"),
        "conflict_threshold": float(conflict_threshold),
    }
    coordination = {
        "policy": COORDINATION_POLICY,
        "variant_weights_sha256": sha256_bytes(variant_weights.tobytes()),
        "variant_weights_file_sha256": lineage.sha256_file(variant_path),
    }
    for name, comp in (("model", model), ("feature", feature), ("market_asset", market_asset),
                       ("calibration", calibration), ("coordination_policy", coordination)):
        comp["digest"] = _digest_of({k: v for k, v in comp.items() if k != "digest"})
    bundle_id = _digest_of({k: v["digest"] for k, v in
                            (("model", model), ("feature", feature), ("market_asset", market_asset),
                             ("calibration", calibration), ("coordination_policy", coordination))})
    return {"bundle_id": bundle_id, "model": model, "feature": feature,
            "market_asset": market_asset, "calibration": calibration,
            "coordination_policy": coordination}


def bundle_digests(bundle: dict) -> dict:
    return {k: bundle[k]["digest"] for k in
            ("model", "feature", "market_asset", "calibration", "coordination_policy")}


def assert_bundle_matches(bundle: dict, expected: dict) -> dict:
    """成套核对：任一组件摘要不符即拒绝（不部分混用）。expected 为 {component: digest}。"""
    got = bundle_digests(bundle)
    mismatches = [k for k in got if k in expected and got[k] != expected[k]]
    if mismatches:
        raise OpsHalt(
            f"停线上报：版本错配，组件不符 {mismatches}（拒绝输出，禁止混用不同代资产）")
    missing = [k for k in ("model", "feature", "market_asset", "calibration",
                           "coordination_policy") if k not in got]
    if missing:
        raise OpsHalt(f"停线上报：版本指纹不完整，缺 {missing}")
    return {"ok": True, "components": got, "checked_against": expected}


# ---------------------------------------------------------------- 资产加载

def load_ops_assets(s1_run: Path = DEFAULT_S1_RUN, s3_run: Path = DEFAULT_S3_RUN,
                    s4b_run: Path = DEFAULT_S4B_RUN, s4c_run: Path = DEFAULT_S4C_RUN,
                    strict: bool = True) -> dict:
    """加载 V1 资产、区间表、降级变体权重与冻结合同，并强制指纹核对（不符即 OpsHalt）。

    区间表取自 ``s4c_run``（默认 S4C run），其摘要对 **S4C manifest** 核对；降级变体权重、
    冻结合同与登记文件仍取自 ``s4b_run`` 并按 S4-B manifest 核对。资产换代为缺陷修复：
    校准组件指纹随新表重绑，其余四组件摘要不变。
    """
    assets = ci.load_v1_assets(s3_run, ci.FOLD_ID)
    fp = ci.verify_assets_fingerprint(assets, s3_run)

    registered = json.loads(
        (s4b_run / "inference-assets" / "v1-assets-registration.json").read_text(encoding="utf-8"))
    reg_ok = (registered["encoder_sha256"] == assets["encoder_sha256"]
              and registered["weights_sha256"] == assets["weights_sha256"]
              and registered["n_features"] == assets["n_features"])
    if strict and not reg_ok:
        raise OpsHalt("停线上报：V1 资产指纹与 S4-B 登记值不一致（拒绝输出）")

    manifest_s4b = json.loads((s4b_run / "manifest.json").read_text(encoding="utf-8"))
    artifacts_s4b = manifest_s4b.get("artifacts", {})
    manifest_s4c = json.loads((s4c_run / "manifest.json").read_text(encoding="utf-8"))
    artifacts_s4c = manifest_s4c.get("artifacts", {})
    interval_path = s4c_run / "interval" / "interval-table.json"
    variant_path = s4b_run / "degradation" / "path-b-variant" / f"{ci.FOLD_ID}.npy"
    reg_checks = {}
    for src_tag, name, path, arts in (
            ("S4C", "interval/interval-table.json", interval_path, artifacts_s4c),
            ("S4B", f"degradation/path-b-variant/{ci.FOLD_ID}.npy", variant_path, artifacts_s4b),
            ("S4B", "contract/phase2-s4b-acceptance-contract.json",
             s4b_run / "contract" / "phase2-s4b-acceptance-contract.json", artifacts_s4b)):
        actual = lineage.sha256_file(path)
        exp = (arts.get(name) or {}).get("sha256")
        reg_checks[name] = {"actual": actual, "registered": exp, "match": actual == exp,
                            "manifest": src_tag}
    assets_reg_actual = lineage.sha256_file(
        s4b_run / "inference-assets" / "v1-assets-registration.json")
    exp_assets = (artifacts_s4b.get("inference-assets/v1-assets-registration.json") or {}).get("sha256")
    reg_checks["inference-assets/v1-assets-registration.json"] = {
        "actual": assets_reg_actual, "registered": exp_assets,
        "match": assets_reg_actual == exp_assets, "manifest": "S4B"}
    if strict and not all(v["match"] for v in reg_checks.values()):
        bad = [k for k, v in reg_checks.items() if not v["match"]]
        raise OpsHalt(f"停线上报：冻结资产与登记 manifest 不一致 {bad}（拒绝输出）")

    interval_table = json.loads(interval_path.read_text(encoding="utf-8"))
    variant_weights = np.load(variant_path)
    contract = json.loads(
        (s4b_run / "contract" / "phase2-s4b-acceptance-contract.json").read_text(encoding="utf-8"))
    a1_reg = json.loads(
        (s4b_run / "inference-assets" / "a1-conflict-registration.json").read_text(encoding="utf-8"))
    conflict_threshold = float(a1_reg["conflict_threshold"])

    bundle = build_version_bundle(Path(s1_run), Path(s3_run), Path(s4b_run), Path(s4c_run),
                                  assets, variant_weights, interval_table, conflict_threshold)
    got = bundle_digests(bundle)
    bundle_ok = (got == REGISTERED_BUNDLE_DIGESTS
                 and bundle["bundle_id"] == REGISTERED_BUNDLE_ID)
    if strict and not bundle_ok:
        bad = {k: {"got": got[k], "registered": v}
               for k, v in REGISTERED_BUNDLE_DIGESTS.items() if got.get(k) != v}
        raise OpsHalt(f"停线上报：版本成套指纹与登记代次不符 {bad}"
                      "（拒绝输出，禁止混用不同代资产）")

    return {
        "ops_version": OPS_VERSION,
        "s1_run": Path(s1_run), "s3_run": Path(s3_run), "s4b_run": Path(s4b_run),
        "s4c_run": Path(s4c_run),
        "assets": assets, "fingerprint_check": fp,
        "registration_check": {"v1_assets_match": bool(reg_ok), "files": reg_checks},
        "interval_table": interval_table, "variant_weights": variant_weights,
        "contract": contract, "conflict_threshold": conflict_threshold,
        "version_bundle": bundle, "version_bundle_verified": bool(bundle_ok),
        "_ctx_cache": {},
    }


class CandidateOps:
    """候选模式编排：按锚点缓存市场上下文，单套/批量/重放三路径调用同一推理核。"""

    def __init__(self, loaded: dict):
        self.loaded = loaded
        self.s1_run = loaded["s1_run"]
        self.s4c_run = loaded.get("s4c_run", DEFAULT_S4C_RUN)
        self.assets = loaded["assets"]
        self.interval_table = loaded["interval_table"]
        self.variant_weights = loaded["variant_weights"]
        self.conflict_threshold = loaded["conflict_threshold"]
        self._ctx_cache: dict[str, dict] = {}
        self._block_meta: dict | None = None
        self.block_map_path = loaded.get("block_map_path") or DEFAULT_BLOCK_MAP_PATH

    def context_for(self, anchor: date) -> dict:
        key = anchor.isoformat()
        if key not in self._ctx_cache:
            self._ctx_cache[key] = ci.build_context(self.assets, self.s1_run, anchor,
                                                    ci.WINDOW_DAYS)
        return self._ctx_cache[key]

    def candidates(self, anchor: date) -> ci.CandidateV1:
        return ci.CandidateV1(
            self.assets, self.s1_run, anchor=anchor, window_days=ci.WINDOW_DAYS,
            degradation_path=DEGRADATION_PATH, interval_table=self.interval_table,
            variant_weights=self.variant_weights,
            conflict_threshold=self.conflict_threshold,
            context=self.context_for(anchor))

    def version_bundle(self, anchor: date | None = None) -> dict:
        assets = dict(self.assets)
        assets["anchor"] = anchor.isoformat() if anchor else None
        return build_version_bundle(self.s1_run, self.loaded["s3_run"], self.loaded["s4b_run"],
                                    self.s4c_run, assets, self.variant_weights,
                                    self.interval_table, self.conflict_threshold)

    @property
    def block_meta(self) -> dict:
        """冻结池「小区→板块」元数据（惰性加载一次；与 baselines.load_blocks 同源，只读）＋
        现行商圈映射表（btm D3 挂载；不可用不 halt、结果内披露）。"""
        if self._block_meta is None:
            meta = load_block_meta(self.s1_run)
            meta["block_map"] = load_block_map(self.block_map_path)
            self._block_meta = meta
        return self._block_meta


# ---------------------------------------------------------------- 板块解析
# fix-phase2-request-mapping design D2/D9/D10：请求值优先（过口径校验才采信）→
# build-phase2-block-taxonomy-map D3：口径校验不通过时先尝试映射表翻译（页面词＝登记现行值
# 且 drifted → 采信冻结推导口径）→ 冻结池 comm2block 静态推导（小区内排序首位，与冻结
# 清单路径同源）→ 无（不推导）；全程披露（block_source / effective_block_name /
# provided_block_name），SHALL NOT 静默插补。

BLOCK_SOURCE_VALUES = ("request", "translated", "derived", "none")
BLOCK_DERIVED_LIMITS = ("板块由小区静态推导（{effective}）：多板块小区（冻结池内 49/818）"
                        "可能与该房源实际板块不符，建议显式提供后重估",)
BLOCK_MISMATCH_LIMITS = ("提供板块「{provided}」不在该小区冻结板块集 {hist}，不采信"
                         "（或系链家商圈口径漂移）；已按冻结口径板块「{effective}」出价",)
BLOCK_AMBIGUOUS_LIMITS = ("该小区横跨多个板块（{blocks}），建议提供板块后重估；"
                          "各候选板块报价见 block_candidates",)
BLOCK_TRANSLATED_LIMITS = ("板块口径映射翻译采信：页面词「{provided}」＝登记现行值且口径漂移，"
                           "采信冻结推导口径「{effective}」（与未提供板块时同源同价）；"
                           "映射表指纹 {map_sha}",)
BLOCK_MAP_UNAVAILABLE_LIMITS = ("板块口径映射表不可用（{error}）：页面新词按未覆盖处理、"
                                "维持 mismatch 不采信（不静默降级）",)
M1_B0_DIVERGENCE_LIMIT = ("A1 不可用（a1_status={status}）且 |M1−B0|/B0 = {pct}% 超过阈值 {th}%："
                          "m1_b0_divergence 已标注（仅标注不仲裁，decision 语义不变）",)


def load_block_meta(s1_run: Path) -> dict:
    """从 S1 特征层构建小区→板块映射与小区→历史板块集合（baselines.load_blocks 同源，只读）。

    ``comm2blocks``（design D9 冻结上下文）：过滤与 ``load_blocks`` 相同的缺失板块值后
    按 ``community_source_id`` 聚合去重、组内排序；实测与 D2 复算口径（不过滤）同为
    49 个多板块小区 / 818 小区，无口径分歧。
    """
    features = pl.read_parquet(s1_run / "features.parquet")
    blocks = baselines.load_blocks(features)
    cb = (features.select(["community_source_id", "block_name"])
          .filter(pl.col("block_name").is_not_null()
                  & (~pl.col("block_name").is_in(list(baselines.MISSING_BLOCK_VALUES))))
          .unique()
          .group_by("community_source_id")
          .agg(pl.col("block_name").sort().alias("blocks")))
    return {"comm2block": {r["community_source_id"]: r["block_name"]
                           for r in blocks.to_dicts()},
            "comm2blocks": {r["community_source_id"]: tuple(r["blocks"])
                            for r in cb.to_dicts()}}


def load_block_map(path: Path) -> dict:
    """现行商圈映射表（btm D3）：读运行时权威路径正本并计件指纹。

    整表缺失/不可读不 halt（SHALL NOT 阻断报价）：返回 available=False 与错误说明，
    resolve 结果按"映射未覆盖"维持 mismatch，并在 limits 披露「映射表不可用」。
    映射表指纹不进入版本成套五组件指纹（审1 F8），仅在结果 limits 披露。
    """
    try:
        raw = Path(path).read_bytes()
        payload = json.loads(raw.decode("utf-8"))
        comm2map = payload.get("comm2map")
        if not isinstance(comm2map, dict):
            raise ValueError("comm2map 缺失或非字典")
        return {"available": True, "file_sha256": sha256_bytes(raw), "comm2map": comm2map}
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        return {"available": False, "file_sha256": None, "comm2map": {},
                "error": f"{type(exc).__name__}: {exc}"}


def resolve_block(req: cr.Request, meta: dict) -> dict:
    """板块解析（D2/D9/D10）：请求值（过口径校验）优先 → 冻结池 comm2block 推导 → 无。

    - 小区不在冻结池（comm2block 无该 key）→ 不推导、无校验基准，提供值按既有冷启动
      行为直用（block_source=none，行为与修复前一致）；
    - 提供值 ∈ comm2blocks[小区] → 采信（block_source=request）；∉ → 先查现行商圈映射表
      （btm D3）：登记现行值＝请求词且 category=drifted → 翻译采信推导板块
      （block_source=translated，原提供值以 provided_block_name 留痕，多板块歧义披露不变）；
      未命中映射 → block_mismatch，以推导值出价（不静默采用外部口径）；
    - 未提供有效板块且小区历史板块 ≥2 → block_ambiguous（主报价仍取推导值，
      候选板块报价由 estimate 以同一推理核切换板块哑变量补齐）。
    """
    comm2block = meta["comm2block"]
    hist = meta.get("comm2blocks", {}).get(req.community_source_id) or ()
    derived = comm2block.get(req.community_source_id)
    if derived is None:
        return {"block_source": "none", "effective_block_name": req.block_name,
                "block_mismatch": False, "block_ambiguous": False,
                "block_candidates": None, "candidate_blocks": None,
                "provided_block_name": req.block_name, "history_blocks": ()}
    if req.block_name is not None:
        if req.block_name in hist:
            return {"block_source": "request", "effective_block_name": req.block_name,
                    "block_mismatch": False, "block_ambiguous": False,
                    "block_candidates": None, "candidate_blocks": None,
                    "provided_block_name": req.block_name, "history_blocks": hist}
        bmap = meta.get("block_map") or {}
        entry = (bmap.get("comm2map") or {}).get(req.community_source_id)
        if (bmap.get("available") and entry
                and entry.get("category") == "drifted"
                and entry.get("current_block") == req.block_name):
            multi = len(hist) >= 2
            return {"block_source": "translated", "effective_block_name": derived,
                    "block_mismatch": False, "block_ambiguous": multi,
                    "block_candidates": None,
                    "candidate_blocks": sorted(hist) if multi else None,
                    "provided_block_name": req.block_name, "history_blocks": hist}
        multi = len(hist) >= 2
        return {"block_source": "derived", "effective_block_name": derived,
                "block_mismatch": True, "block_ambiguous": multi,
                "block_candidates": None,
                "candidate_blocks": sorted(hist) if multi else None,
                "provided_block_name": req.block_name, "history_blocks": hist}
    multi = len(hist) >= 2
    return {"block_source": "derived", "effective_block_name": derived,
            "block_mismatch": False, "block_ambiguous": multi,
            "block_candidates": None,
            "candidate_blocks": sorted(hist) if multi else None,
            "provided_block_name": None, "history_blocks": hist}


# ---------------------------------------------------------------- 结果组装

LIMITS_BASE = (
    "区间为校准/训练窗内拟合覆盖（循环性限度），外检验归 S5；不得据此宣称预测区间有效",
    "A1 为辅助参考价，不覆盖、不仲裁 M1 报价；a1_m1_conflict 仅标注差异",
    "M1（V1/F01）编码器不使用市场窗特征；B0/A1 市场材料按锚点前 365 天现算，无跨请求统计",
    "总楼层缺失走降级路径乙（去总楼层列组变体），miss_total_floors 保持置位，不静默外推",
    "关键属性缺失或案例链为空时 A1 不出修正价（降级/标注，不拒绝报价）",
    "适用人群＝云溪区普通住宅；非适用请求按登记规则拒绝或加范围外标注",
    "请求未提供板块时由小区冻结口径静态推导（status.block_source=derived 披露），"
    "不静默插补；多板块小区（冻结池内 49/818）建议显式提供板块后重估",
    "A1 不可用且 |M1−B0|/B0 超过登记阈值（15%，与 A1 冲突阈值同源）时输出 "
    "status.m1_b0_divergence 标注，仅标注不仲裁、不改 decision",
)


def _decision(row: dict, issues: list[dict]) -> str:
    if bool(row["reject"]):
        return "rejected"
    if bool(row["a1_m1_conflict"]):
        return "conflict"
    if row["degradation_state"] != ci.DEGRADATION_STATE_NORMAL or bool(row["cold_start_community"]):
        return "degraded"
    return "ok"


def _rejected_result(req: cr.Request, issues: list[dict], bundle: dict, anchor: date) -> dict:
    return {
        "schema_version": "phase2-s6a-estimate-v1",
        "ops_version": OPS_VERSION,
        "request_id": req.request_id,
        "anchor": anchor.isoformat(),
        "request": _echo_request(req),
        "status": {
            "decision": "rejected",
            "reject": True,
            "reject_reasons": [i for i in issues if i["severity"] == "error"],
            "out_of_scope": [i["code"] for i in issues
                             if i["severity"] == "error" and i["code"].startswith("out_of_scope")],
            "scope_unverified": any(i["code"] == "scope_unverified" for i in issues),
            "degradation_state": None, "degradation_origin": None,
            "known_community": None, "cold_start_community": None,
            "a1_m1_conflict": None,
            "decision_rule": "命中硬域外/非适用人群 → 拒绝出价（不产出未限定报价）",
        },
        "point": {"m1_pred_unit_price": None, "m1_pred_total_price": None,
                  "area_sqm": req.area_sqm},
        "interval": {"nominal_80": {"low": None, "high": None},
                     "nominal_90": {"low": None, "high": None},
                     "stratum": None, "source_layer": None},
        "support": None,
        "limits": list(LIMITS_BASE),
        "issues": issues,
        "version_fingerprints": bundle,
    }


def _echo_request(req: cr.Request) -> dict:
    return {
        "community_source_id": req.community_source_id,
        "community_name": req.community_name,
        "block_name": req.block_name,
        "district": req.district,
        "property_use": req.property_use,
        "area_sqm": req.area_sqm,
        "floor_bucket": req.floor_bucket,
        "year_built": req.year_built,
        "has_elevator": req.has_elevator,
        "total_floors": req.total_floors,
        "bedrooms": req.bedrooms,
        "orientation": req.orientation,
        "decoration": req.decoration,
        "valuation_date": req.valuation_date.isoformat(),
        "data_cutoff": req.data_cutoff.isoformat() if req.data_cutoff else None,
    }


def _interval_source_meta(interval_table: dict | None, source_layer) -> dict:
    """命中层的样本量 ``n`` 与残差来源（train_oof / reserved_holdout）；未命中返回空值。"""
    if not source_layer:
        return {"source_layer_n": None, "residual_source": None}
    layer = ((interval_table or {}).get("layers") or {}).get(source_layer)
    if not layer:
        return {"source_layer_n": None, "residual_source": None}
    return {"source_layer_n": layer.get("n"), "residual_source": layer.get("residual_source")}


def _m1_b0_divergence(row: dict) -> dict:
    """偏离护栏（design D4）：A1 不可用且 |M1−B0|/B0 > 登记阈值 → m1_b0_divergence 标注。

    仅产出标注位与比值，不参与 _decision；阈值取 COORDINATION_POLICY 登记值（与
    candidate_inference.CONFLICT_THRESHOLD_DEFAULT 同源）。rejected 路径不经此函数。
    """
    threshold = COORDINATION_POLICY["m1_b0_divergence_threshold"]
    if row.get("a1_status") == ci.A1_OK:
        return {"flag": False, "ratio": None, "threshold": threshold}
    m1, b0 = row.get("m1_pred_unit_price"), row.get("b0_pred")
    if m1 is None or b0 is None or float(b0) == 0.0:
        return {"flag": False, "ratio": None, "threshold": threshold}
    ratio = abs(float(m1) - float(b0)) / float(b0)
    return {"flag": bool(ratio > threshold), "ratio": ratio, "threshold": threshold}


def _row_result(req: cr.Request, row: dict, issues: list[dict], bundle: dict,
                anchor: date, interval_table: dict | None = None,
                block: dict | None = None) -> dict:
    decision = _decision(row, issues)
    limits = list(LIMITS_BASE)
    block = block or {"block_source": "none", "effective_block_name": req.block_name,
                      "block_mismatch": False, "block_ambiguous": False,
                      "block_candidates": None, "candidate_blocks": None,
                      "provided_block_name": req.block_name, "history_blocks": ()}
    bmap = block.get("block_map") or {}
    if bmap and not bmap.get("available"):
        limits.append(BLOCK_MAP_UNAVAILABLE_LIMITS[0].format(error=bmap.get("error")))
    if block["block_source"] == "translated":
        limits.append(BLOCK_TRANSLATED_LIMITS[0].format(
            provided=block["provided_block_name"], effective=block["effective_block_name"],
            map_sha=(bmap.get("file_sha256") or "unknown")[:12]))
    if block["block_source"] == "derived":
        limits.append(BLOCK_DERIVED_LIMITS[0].format(effective=block["effective_block_name"]))
    if block["block_mismatch"]:
        limits.append(BLOCK_MISMATCH_LIMITS[0].format(
            provided=block["provided_block_name"],
            hist="{" + "、".join(block["history_blocks"]) + "}",
            effective=block["effective_block_name"]))
    if block["block_ambiguous"]:
        limits.append(BLOCK_AMBIGUOUS_LIMITS[0].format(
            blocks="、".join(block["candidate_blocks"] or ())))
    src_meta = _interval_source_meta(interval_table, row["interval_source_layer"])
    if row["interval_source_layer"] is None:
        limits.append(
            f"部署核 interval_for 对分层 {row['interval_stratum']} 未命中任何区间层"
            "（并层规则：自身层 → <deg>|global → global）：本结果无区间")
    for i in issues:
        if i["severity"] == "limited":
            limits.append(f"限定标注（{i['code']}）：{i['message']}")
    m1_b0_div = _m1_b0_divergence(row)
    if m1_b0_div["flag"]:
        limits.append(M1_B0_DIVERGENCE_LIMIT[0].format(
            status=row["a1_status"], pct=f'{m1_b0_div["ratio"] * 100:.2f}',
            th=f'{ci.CONFLICT_THRESHOLD_DEFAULT * 100:.0f}'))
    return {
        "schema_version": "phase2-s6a-estimate-v1",
        "ops_version": OPS_VERSION,
        "request_id": req.request_id,
        "anchor": anchor.isoformat(),
        "request": _echo_request(req),
        "status": {
            "decision": decision,
            "reject": bool(row["reject"]),
            "reject_reasons": [],
            "out_of_scope": [],
            "scope_unverified": any(i["code"] == "scope_unverified" for i in issues),
            "degradation_state": row["degradation_state"],
            "degradation_origin": row["degradation_origin"],
            "known_community": bool(row["known_community"]),
            "cold_start_community": bool(row["cold_start_community"]),
            "a1_m1_conflict": bool(row["a1_m1_conflict"]),
            "m1_b0_divergence": bool(m1_b0_div["flag"]),
            "block_source": block["block_source"],
            "effective_block_name": block["effective_block_name"],
            "provided_block_name": block["provided_block_name"],
            "block_mismatch": bool(block["block_mismatch"]),
            "block_ambiguous": bool(block["block_ambiguous"]),
            "block_candidates": block["block_candidates"],
            "decision_rule": ("reject→rejected；a1_m1_conflict→conflict；"
                              "降级/冷启动→degraded；其余→ok"),
        },
        "point": {
            "m1_pred_unit_price": row["m1_pred_unit_price"],
            "m1_pred_total_price": row["m1_pred_total_price"],
            "area_sqm": req.area_sqm,
        },
        "interval": {
            "nominal_80": {"low": row["interval80_low"], "high": row["interval80_high"]},
            "nominal_90": {"low": row["interval90_low"], "high": row["interval90_high"]},
            "stratum": row["interval_stratum"],
            "source_layer": row["interval_source_layer"],
            "source_layer_n": src_meta["source_layer_n"],
            "residual_source": src_meta["residual_source"],
        },
        "support": {
            "b0_level": row["b0_level"], "b0_pred": row["b0_pred"],
            "b0_window_n": row["b0_window_n"],
            "a1_status": row["a1_status"], "a1_n_cases": row["a1_n_cases"],
            "a1_level": row["a1_level"], "a1_pre_center": row["a1_pre_center"],
            "a1_post_center": row["a1_post_center"],
            "a1_delta_median": row["a1_delta_median"],
            "a1_n_rejected": row["a1_n_rejected"], "a1_n_corrected": row["a1_n_corrected"],
        },
        "limits": limits,
        "issues": issues,
        "version_fingerprints": bundle,
    }


def estimate(ops: CandidateOps, doc: dict, as_of: date | None = None,
             anchor: date | None = None) -> dict:
    """单套估值：请求解析 → 校验 → 板块解析 → 映射 → 单套推理 → 成套结果（含版本指纹）。"""
    as_of = as_of or date.today()
    req = cr.parse_request(doc)
    issues = cr.validate(req, as_of=as_of)
    use_anchor = anchor or req.valuation_date
    bundle = ops.version_bundle(use_anchor)
    if cr.is_rejected(issues):
        return _rejected_result(req, issues, bundle, use_anchor)
    cand = ops.candidates(use_anchor)
    block = resolve_block(req, ops.block_meta)
    block["block_map"] = ops.block_meta.get("block_map") or {
        "available": False, "error": "meta 无 block_map（未走 CandidateOps.block_meta）"}
    frame = cr.to_frame(req, issues, block["effective_block_name"])
    row = cand.predict_one(frame)
    if block["candidate_blocks"]:
        block["block_candidates"] = [
            {"block": b,
             "m1_unit_price": float(cand.predict_one(
                 frame.with_columns(pl.lit(b).alias("block_name")))["m1_pred_unit_price"])}
            for b in block["candidate_blocks"]]
    return _row_result(req, row, issues, bundle, use_anchor, ops.interval_table, block)


def estimate_many(ops: CandidateOps, docs: list[dict], as_of: date | None = None,
                  anchor: date | None = None) -> list[dict]:
    return [estimate(ops, d, as_of=as_of, anchor=anchor) for d in docs]


# ================================================================ 正式模式（D4）
# enable-phase2-conditional-release：候选/研究路径行为不变；以下为正式入口新增面。
# 正式输出四态（priced/ineligible/rejected/version_disabled），正式字段仅 priced 非空；
# 发布记录默认不存在＝未发布；停止开关 fail-closed；资格门唯一实现＝formal_gate。

FORMAL_OPS_VERSION = "s-c-formal-ops-v1"
FORMAL_LIMITS_BASE = (
    "正式输出以发布记录 release-record 生效为前提（判定 B＋审2＋S7＋允许分支＋期限＋组合全件指纹）；"
    "记录缺失/未确认/错配/分支未列/超期一律不出正式价",
    "整体停止开关为运营层随时可停手段，与发布记录独立；状态文件缺失/损坏按停止处理（fail-closed）",
    "正式资格门＝formal_gate 九门唯一实现（评估与生产同源）；资格不通过不出正式价、不静默改用 B0/A1 充当报价",
)
RC_CUTOFF_NOT_ENFORCED = "FM_CUTOFF_NOT_ENFORCED"
RC_COMMUNITY_STALE = "FM_COMMUNITY_STALE"
RC_COMBINATION_UNAVAILABLE = "FM_COMBINATION_UNAVAILABLE"
RC_INPUT_UNPARSEABLE = "FM_INPUT_UNPARSEABLE"
RC_INPUT_REJECTED = "FM_INPUT_REJECTED"


def sha256_module_file(module) -> str:
    return lineage.sha256_file(Path(module.__file__))


# F6（RV-ECR-VERIFY-01）：production_code 组件的确定性代码清单——执行正式放行/时效/
# 分支停用/请求映射/输出/绑定的完整代码依赖。formal_gate.py 与 candidate_inference.py
# 已由 formal_gate_rule / inference_code 组件单列（不重复）；lineage.py 为哈希工具不
# 影响输出语义不入清单；ecr_30_feedback.py 等校验脚本不属生产组合不入。
PRODUCTION_CODE_MODULES = ("candidate_request", "candidate_ops", "release_record",
                           "stop_switch", "formal_binding", "formal_states")


def production_code_digest() -> dict:
    """正式执行代码聚合摘要（确定性）：按模块名排序逐文件 SHA-256 再规范化聚合。"""
    import gz_property_valuation.phase2 as pkg
    files = {}
    for name in PRODUCTION_CODE_MODULES:
        mod = getattr(pkg, name, None) or __import__(
            f"gz_property_valuation.phase2.{name}", fromlist=[name])
        files[name] = sha256_module_file(mod)
    return {"files": files, "digest": _digest_of(files)}


def nine_fingerprint(ops: CandidateOps) -> dict:
    """组合指纹（五组件＋推理代码＋资格规则＋采用合同＋current-block-map＋production_code）。

    F6：production_code 为正式执行代码六文件聚合摘要（PRODUCTION_CODE_MODULES）——
    正式入口文件单独变化（五组件不变）即产生新组合并拒绝旧凭据。
    缓存语义（冻结策略一致）：per-ops 进程内缓存，首次计算后本进程内冻结——映射/
    合同/代码在本进程内的后续变化不刷新指纹（与代码冻结策略一致；新进程/新实例
    重新计算）。九件外层结构保留（历史别名 NINE_COMPONENTS，实际十件）。
    """
    cached = getattr(ops, "_nine_fingerprint_cache", None)
    if cached is not None:
        return cached
    bundle = ops.version_bundle()
    contract_sha = (lineage.sha256_file(ADOPTION_CONTRACT_PATH)
                    if ADOPTION_CONTRACT_PATH.exists() else "ADOPTION_CONTRACT_UNAVAILABLE")
    fp = fb.compose_nine(
        bundle_digests(bundle),
        inference_code_sha=sha256_module_file(ci),
        formal_gate_rule_sha=sha256_module_file(fg),
        adoption_contract_sha=contract_sha,
        current_block_map_sha=lineage.sha256_file(Path(ops.block_map_path)),
        production_code_sha=production_code_digest()["digest"])
    try:
        ops._nine_fingerprint_cache = fp
    except AttributeError:
        pass
    return fp


def branch_of_request(req: cr.Request, issues: list[dict]) -> str:
    """请求级采用分支：总楼层有效＝normal；缺失/软域外按未知＝degraded_total_floors。"""
    row = cr.map_to_v1_row(req, issues)
    return (ci.DEGRADATION_STATE_NORMAL if row["miss_total_floors"] == 0
            else ci.DEGRADATION_STATE_TOTAL_FLOORS)


def formal_support_count(cand: ci.CandidateV1, community_source_id: str) -> int:
    """截点前 365 天去重合格案例数（与存量评估 support_counts 同源：context.comm_idx）。"""
    idx = cand.context.get("comm_idx", {}).get(str(community_source_id))
    return 0 if idx is None else int(len(idx))


def cutoff_registration(cand: ci.CandidateV1, req: cr.Request, use_anchor: date,
                        release: dict | None) -> dict:
    """时效登记：统一开闭区间、请求截点、实际消费最大日期与小区 T−L 实际值。"""
    ctx = cand.context
    case_dates = [d for d in ctx.get("case_dates", []).tolist()]
    consumed_max = max(case_dates) if case_dates else None
    idx = ctx.get("comm_idx", {}).get(str(req.community_source_id))
    comm_max = max((case_dates[i] for i in idx), default=None) if idx is not None else None
    t_l_days = (use_anchor - comm_max).days if comm_max is not None else None
    validity = (release or {}).get("validity") or {}
    return {
        "window_open_inclusive": (use_anchor - timedelta(days=ci.WINDOW_DAYS)).isoformat(),
        "window_close_exclusive": use_anchor.isoformat(),
        "interval_rule": "[window_open, window_close)",
        "request_cutoff": (req.data_cutoff or use_anchor).isoformat(),
        "asset_market_materials_cutoff": validity.get("market_materials_cutoff"),
        "actual_consumed_max_date": consumed_max.isoformat() if consumed_max else None,
        "community_latest_case_date": comm_max.isoformat() if comm_max is not None else None,
        "t_minus_l_days_actual": t_l_days,
        "t_minus_l_days_limit": validity.get("t_minus_l_days"),
    }


def _gate_fail(gate_id: str, name: str, reason_code: str, detail: dict) -> dict:
    return {"gate": gate_id, "name": name, "passed": False,
            "reason_codes": [reason_code], "notes": [], "detail": detail}


def _early_envelope(doc, state: str, reason_codes: list[str], as_of, *,
                    release_check=None, combination=None, branch=None,
                    limits=None, issues=None, eligibility=None,
                    timing_registration=None) -> dict:
    rid = doc.get("request_id") if isinstance(doc, dict) else None
    return fs.build_envelope(
        request_id=rid, state=state, as_of=as_of.isoformat() if as_of else None,
        anchor=None, branch=branch, reason_codes=reason_codes,
        release_check=release_check, combination=combination,
        eligibility=eligibility, timing_registration=timing_registration,
        limits=list(FORMAL_LIMITS_BASE) + list(limits or []), issues=issues)


def formal_estimate(ops: CandidateOps | None, doc: dict, *, state_dir=None,
                    release: dict | None = None, stop: dict | None = None,
                    branch_suspension: dict | None = None,
                    combination: dict | None = None, support_case_count=None,
                    as_of: date | None = None, anchor: date | None = None) -> dict:
    """正式单套估值（四态）：停止开关 → 发布记录 → 输入 → 时效/截点 → 资格门 → priced。

    与候选 estimate 同源推理；非 priced 态不携带任何价格字段（结构性防泄露）。
    ``release``/``stop``/``branch_suspension``/``combination`` 显式注入用于测试与回归；
    缺省时按 state_dir（默认数据区 ops-state）运行时加载。
    """
    as_of = as_of or date.today()
    sdir = Path(state_dir) if state_dir else sw.default_state_dir()

    # 1) 整体停止开关（运营层，独立于发布记录；任何请求先过此门，fail-closed）
    stop = stop if stop is not None else sw.check_stop_switch(sdir)
    if stop["stopped"]:
        return _early_envelope(doc, fs.STATE_VERSION_DISABLED,
                               list(stop["reason_codes"]), as_of,
                               limits=["整体停止开关生效：不输出正式价格（版本停用）"])

    # 2) 发布记录加载（默认不存在＝未发布）；损坏按 fail-closed 处理
    rel_error = None
    if release is None:
        try:
            release = rr.load_release_record(rr.release_record_path(sdir))
        except rr.ReleaseRecordError as exc:
            release, rel_error = None, f"{type(exc).__name__}: {exc}"

    # 3) 请求解析（版本级全局门先于输入语义）
    try:
        req = cr.parse_request(doc)
        issues = cr.validate(req, as_of=as_of)
        parse_failed = False
    except cr.RequestError as exc:
        req, issues, parse_failed = None, list(exc.issues), True

    if combination is None and ops is not None:
        combination = nine_fingerprint(ops)

    # 4) 发布记录全局层（缺失/损坏/schema/判定 B/S7/组合指纹——不依赖请求字段）
    if rel_error is not None:
        rc_global = {"ok": False, "reason_codes": [rr.RC_RECORD_CORRUPT],
                     "detail": {"error": rel_error}}
    else:
        rc_global = rr.check_release(release, combination)
    if not rc_global["ok"]:
        return _early_envelope(doc, fs.STATE_VERSION_DISABLED,
                               list(rc_global["reason_codes"]), as_of,
                               release_check=rc_global, combination=combination)
    if ops is None or combination is None:
        return _early_envelope(doc, fs.STATE_VERSION_DISABLED,
                               [RC_COMBINATION_UNAVAILABLE], as_of,
                               release_check=rc_global, combination=combination,
                               limits=["组合指纹不可用：拒绝出价（fail-closed）"])
    if parse_failed:
        return _early_envelope(doc, fs.STATE_REJECTED, [RC_INPUT_UNPARSEABLE], as_of,
                               release_check=rc_global, combination=combination,
                               issues=issues)
    branch = branch_of_request(req, issues)
    use_anchor = anchor or req.valuation_date

    # 5) 发布记录请求层（分支未列 / 超期）
    rc_full = rr.check_release(release, combination, branch=branch,
                               valuation_date=req.valuation_date)
    if not rc_full["ok"]:
        return _early_envelope(doc, fs.STATE_VERSION_DISABLED,
                               list(rc_full["reason_codes"]), as_of,
                               release_check=rc_full, combination=combination,
                               branch=branch)

    # 6) 分支停用（反馈闭环硬门失败自动登记；恢复仅经发布记录 resumed_suspensions）
    susp = (branch_suspension if branch_suspension is not None
            else sw.check_branch_suspended(sdir, branch, release))
    if susp["suspended"]:
        return _early_envelope(doc, fs.STATE_VERSION_DISABLED,
                               list(susp["reason_codes"]), as_of,
                               release_check=rc_full, combination=combination,
                               branch=branch,
                               limits=[f"分支 {branch} 停用中（恢复仅经发布记录更新）"])

    # 7) 输入硬域拒绝（与候选路径同源判定，不产出报价）
    if cr.is_rejected(issues):
        return _early_envelope(doc, fs.STATE_REJECTED, [RC_INPUT_REJECTED], as_of,
                               release_check=rc_full, combination=combination,
                               branch=branch, issues=issues)

    # 8) 时效与截点（截点须实际进入查询；小区 T−L≤登记上限）
    cand = ops.candidates(use_anchor)
    timing = cutoff_registration(cand, req, use_anchor, release)
    pre_gates: list[dict] = []
    effective_cutoff = req.data_cutoff or use_anchor
    if use_anchor > effective_cutoff:
        pre_gates.append(_gate_fail(
            "FM-CUTOFF", "cutoff_enforced", RC_CUTOFF_NOT_ENFORCED,
            {"valuation_date": use_anchor.isoformat(),
             "request_cutoff": req.data_cutoff.isoformat() if req.data_cutoff else None,
             "rule": "请求截点早于估值时点且引擎按锚点窗消费材料，声明截点未被实际执行 → 不出价"}))
    t_l_limit = timing.get("t_minus_l_days_limit")
    t_l_actual = timing.get("t_minus_l_days_actual")
    if not (isinstance(t_l_limit, int) and not isinstance(t_l_limit, bool)):
        # F4 纵深防御：登记 T−L 上限缺失/类型非法 → 拒绝（不再跳过时效门）；
        # schema 层（release_record.validate_schema）已先行拒绝此类记录。
        pre_gates.append(_gate_fail(
            "FM-STALE", "community_timeliness", RC_COMMUNITY_STALE,
            {"t_minus_l_days_limit": t_l_limit,
             "rule": "登记 T−L 上限缺失或类型非法 → 拒绝出价（fail-closed，不跳过时效门）"}))
    elif t_l_actual is not None and t_l_actual > t_l_limit:
        pre_gates.append(_gate_fail(
            "FM-STALE", "community_timeliness", RC_COMMUNITY_STALE,
            {"t_minus_l_days_actual": t_l_actual, "t_minus_l_days_limit": t_l_limit,
             "community_latest_case_date": timing.get("community_latest_case_date")}))
    if pre_gates:
        eligibility = {"eligible": False,
                       "failed_gates": [g["gate"] for g in pre_gates],
                       "reason_codes": [c for g in pre_gates for c in g["reason_codes"]],
                       "notes": [], "gates": fs.strip_gate_details(pre_gates),
                       "formal_gate_version": fg.FORMAL_GATE_VERSION}
        return fs.build_envelope(
            request_id=req.request_id, state=fs.STATE_INELIGIBLE,
            as_of=as_of.isoformat(), anchor=use_anchor.isoformat(), branch=branch,
            reason_codes=eligibility["reason_codes"], release_check=rc_full,
            combination=combination, eligibility=eligibility,
            timing_registration=timing,
            limits=list(FORMAL_LIMITS_BASE), issues=issues)

    # 9) 候选推理（同源诊断）＋资格门（唯一实现 import）
    res = estimate(ops, doc, as_of=as_of, anchor=anchor)
    sup_n = (support_case_count if support_case_count is not None
             else formal_support_count(cand, req.community_source_id))
    gate = fg.evaluate(res, support_case_count=sup_n, release_check=rc_full)
    eligibility = fs.eligibility_summary(gate)
    if not gate["eligible"]:
        return fs.build_envelope(
            request_id=req.request_id, state=fs.STATE_INELIGIBLE,
            as_of=as_of.isoformat(), anchor=use_anchor.isoformat(), branch=branch,
            reason_codes=list(gate["reason_codes"]), release_check=rc_full,
            combination=combination, eligibility=eligibility,
            timing_registration=timing, limits=list(FORMAL_LIMITS_BASE), issues=issues)

    # 10) priced：正式字段（十项报告）组装；正式采用依据绑定发布记录
    pt, iv, sup = res["point"], res["interval"], res["support"]
    iv80, iv90 = iv["nominal_80"], iv["nominal_90"]
    interval_block = {
        "nominal_80": {"low": iv80["low"], "high": iv80["high"]},
        "nominal_90": {"low": iv90["low"], "high": iv90["high"]},
        "width_80": (iv80["high"] - iv80["low"]) if None not in (iv80["low"], iv80["high"]) else None,
        "width_90": (iv90["high"] - iv90["low"]) if None not in (iv90["low"], iv90["high"]) else None,
        "stratum": iv["stratum"], "source_layer": iv["source_layer"],
        "source_layer_n": iv["source_layer_n"],
    }
    actual_cutoff = timing["actual_consumed_max_date"]
    formal_price = {
        "unit_price_per_sqm": pt["m1_pred_unit_price"],
        "total_price": pt["m1_pred_total_price"],
        "area_sqm": pt["area_sqm"],
        "valuation_date": use_anchor.isoformat(),
        "actual_data_cutoff": actual_cutoff,
        "interval": interval_block,
    }
    formal_report = {
        "unit_price": pt["m1_pred_unit_price"],
        "total_price": pt["m1_pred_total_price"],
        "valuation_date": use_anchor.isoformat(),
        "actual_data_cutoff": actual_cutoff,
        "interval_and_width": interval_block,
        "main_market_basis": {"b0_level": sup["b0_level"], "b0_pred": sup["b0_pred"],
                              "b0_window_n": sup["b0_window_n"],
                              "window_days": ci.WINDOW_DAYS,
                              "rule": "冻结池锚点前 365 天现算（community→block→district）"},
        "main_limits": list(res["limits"]),
        "applicable_scope": {"population": "云溪区·普通住宅", "branch": branch,
                             "release_validity": release.get("validity"),
                             "allowed_branches": release.get("allowed_branches")},
        "formal_adoption_basis": {
            "contract_id": ADOPTION_CONTRACT_ID,
            "verdict_b_conclusion": (release.get("verdict_b") or {}).get("conclusion"),
            "s7_confirmed": (release.get("s7_user_confirmation") or {}).get("confirmed"),
            "composition_id": combination["composition_id"],
            "support_case_count_365d_dedup": sup_n,
            "m1_b0_divergence_ratio": gate and _gate07_ratio(gate),
        },
        "release_version": {"composition_id": combination["composition_id"],
                            "components": combination["components"]},
    }
    return fs.build_envelope(
        request_id=req.request_id, state=fs.STATE_PRICED,
        as_of=as_of.isoformat(), anchor=use_anchor.isoformat(), branch=branch,
        reason_codes=[], release_check=rc_full, combination=combination,
        eligibility=eligibility, timing_registration=timing,
        limits=list(FORMAL_LIMITS_BASE) + list(res["limits"]), issues=issues,
        formal_price=formal_price, formal_report=formal_report)


def _gate07_ratio(gate: dict):
    for g in gate.get("gates") or []:
        if g.get("gate") == "GATE-07":
            return (g.get("detail") or {}).get("ratio")
    return None


def formal_estimate_many(ops: CandidateOps, docs: list[dict], **kwargs) -> list[dict]:
    """正式批量估值：与单套同一 formal_estimate 核（三路径一致性基础）。"""
    return [formal_estimate(ops, d, **kwargs) for d in docs]


def formal_replay(ops: CandidateOps, shadow_records: list[dict], **kwargs) -> dict:
    """正式重放（F5 统一协议）：正式记录逐条重跑并逐位比对；组合指纹变化（旧版）→
    识别为不同组合，重跑结果必须 version_disabled 且无正式价格（旧版重放拒绝）。

    F5 修复：anchor 以记录内保存值为准——kwargs 中的 anchor 被丢弃（消除 CLI 把
    anchor 放入 formal_kw 与本函数显式 anchor=anchor_prev 的重复参数 TypeError）。
    输入为 formal-estimate/batch 落盘的正式记录（record_type=prediction：
    input_snapshot＋envelope），不再要求人工 request/result 包装（兼容读取保留）。
    """
    kwargs.pop("anchor", None)
    current_combo = nine_fingerprint(ops)
    checked = 0
    bitwise_ok = True
    mismatches: list[str] = []
    combination_changed: list[dict] = []
    for rec in shadow_records:
        pair = replay_pair_from_record(rec)
        prev = pair["prev"]
        rid = (pair["request"] or {}).get("request_id") or prev.get("request_id")
        res = formal_estimate(ops, pair["request"], anchor=pair["anchor"], **kwargs)
        checked += 1
        prev_combo = pair["composition_id"]
        if prev_combo and prev_combo != current_combo["composition_id"]:
            compliant = (res["state"] == fs.STATE_VERSION_DISABLED
                         and res["formal_price"] is None
                         and res["formal_report"] is None)
            bitwise_ok = bitwise_ok and compliant
            combination_changed.append({
                "request_id": rid, "prev_composition_id": prev_combo,
                "current_composition_id": current_combo["composition_id"],
                "rerun_state": res["state"],
                "reason_codes": res["reason_codes"],
                "no_formal_price": res["formal_price"] is None,
                "old_version_refused": compliant})
            continue
        if json.dumps(res, sort_keys=True, default=str) != json.dumps(
                prev, sort_keys=True, default=str):
            bitwise_ok = False
            mismatches.append(rid)
    return {"replayed": checked, "bitwise_equal": bitwise_ok,
            "mismatches": mismatches, "combination_changed": combination_changed,
            "current_composition_id": current_combo["composition_id"]}


def replay_pair_from_record(rec: dict) -> dict:
    """重放协议解析（F5）：返回 {request, prev, anchor, composition_id, format}。

    - 权威格式＝正式记录（record_type=prediction）：请求＝input_snapshot、
      上一结果＝envelope（完整正式信封快照，含重放所需全部字段与证据）、
      anchor/composition_id 以记录登记值为准；
    - 兼容读取历史 request/result 影子包装（回归夹具过渡期），不作为落盘协议。
    """
    if rec.get("record_type") == "prediction" or "input_snapshot" in rec:
        prev = rec.get("envelope")
        if not isinstance(prev, dict):
            raise ValueError(
                "正式记录缺 envelope 快照（不可重放；须由 formal-estimate/batch "
                f"当前协议重新生成）: prediction_id={rec.get('prediction_id')!r}")
        return {"request": rec.get("input_snapshot"), "prev": prev,
                "anchor": (date.fromisoformat(prev["anchor"])
                           if prev.get("anchor") else None),
                "composition_id": (rec.get("composition_id")
                                   or (prev.get("combination") or {})
                                   .get("composition_id")),
                "format": "formal-record"}
    legacy_req = rec.get("request")
    if legacy_req is None:
        raise ValueError("重放记录既非正式记录格式（input_snapshot+envelope）"
                         "也无 request/result 键")
    prev = rec.get("result") or {}
    return {"request": legacy_req, "prev": prev,
            "anchor": (date.fromisoformat(prev["anchor"])
                       if prev.get("anchor") else None),
            "composition_id": (prev.get("combination") or {}).get("composition_id"),
            "format": "legacy-request-result"}


def prediction_record_from_envelope(env: dict, doc: dict, created_at: str) -> dict:
    """正式记录（追加式）：预测 ID、组合指纹、输入快照、截点、资格结果与预测。

    ``prediction`` 仅 priced 非空（正式记录同样不泄露价格到非 priced 态）。
    F5：额外保存 ``envelope``＝完整正式信封快照（重放协议权威格式所需全部字段
    与证据——formal_replay 直接从落盘记录重放，无需人工 request/result 包装）。
    """
    pred = None
    if env["state"] == fs.STATE_PRICED:
        fp = env["formal_price"]
        basis = env["formal_report"]["main_market_basis"]
        pred = {"m1_pred_unit_price": fp["unit_price_per_sqm"],
                "m1_pred_total_price": fp["total_price"],
                "b0_pred": basis["b0_pred"], "b0_level": basis["b0_level"],
                "community_source_id": (doc or {}).get("community_source_id"),
                "interval80_low": fp["interval"]["nominal_80"]["low"],
                "interval80_high": fp["interval"]["nominal_80"]["high"],
                "interval90_low": fp["interval"]["nominal_90"]["low"],
                "interval90_high": fp["interval"]["nominal_90"]["high"],
                "reject": False}
    return {
        "record_type": "prediction",
        "prediction_id": env["request_id"],
        "composition_id": (env.get("combination") or {}).get("composition_id"),
        "state": env["state"],
        "input_snapshot": doc,
        "anchor": env["anchor"],
        "data_cutoff": (doc or {}).get("data_cutoff"),
        "branch": env.get("branch"),
        "eligibility": env.get("eligibility"),
        "reason_codes": env.get("reason_codes"),
        "prediction": pred,
        "envelope": env,
        "created_at": created_at,
    }


def formal_records_path(state_dir=None) -> Path:
    d = Path(state_dir) if state_dir else sw.default_state_dir()
    return d / FORMAL_RECORDS_FILENAME


# F9：prediction_id＝请求 ID（request_id，随请求传入）；幂等键＝(prediction_id,
# composition_id)——同键只允许一条预测记录；同键不同内容或重复 CLI 请求一律拒绝，
# 防止重试/重跑把同一成交标签重复计入效果样本（评估层另有按观察单位去重兜底）。
PREDICTION_IDEMPOTENCY_RULE = (
    "prediction_id＝请求 ID；幂等键＝(prediction_id, composition_id)；"
    "同键多预测被禁止（重复 CLI 请求/同键不同内容 → 拒绝追加，退出码 6）")


def duplicate_predictions(records_path, *new_recs) -> list[dict]:
    """F9 幂等检查：同 (prediction_id, composition_id) 预测已存在（含新批内部重复）。"""
    existing = {(r.get("prediction_id"), r.get("composition_id"))
                for r in fb.load_records(records_path)
                if r.get("record_type") == "prediction"}
    dups, seen = [], set()
    for rec in new_recs:
        key = (rec.get("prediction_id"), rec.get("composition_id"))
        if key in existing or key in seen:
            dups.append({"prediction_id": key[0], "composition_id": key[1]})
        seen.add(key)
    return dups


def halt_on_corrupt_chain(state_dir, records_path: Path) -> dict | None:
    """F8：正式记录链损坏 → 自动登记全分支暂停（含证据），返回 halt 报告；完好返回 None。

    先验完整性后消费/追加/输出：调用方须在正式估值/落盘之前调用本函数。
    空链（文件不存在或 0 条记录，errors 为空）不是损坏——放行首条追加。
    """
    chain = fb.verify_chain(records_path)
    if chain["ok"] or not chain["errors"]:
        return None
    susp = sw.write_branch_suspension(
        state_dir, branch="*", reason_codes=["FM_RECORD_CHAIN_CORRUPT"],
        evidence={"n_records": chain["n_records"],
                  "errors": chain["errors"][:20],
                  "records_path": str(records_path)},
        evidence_pointer=str(records_path),
        note="正式记录哈希链损坏：先验完整性后消费/追加/输出——拒绝估值与价格落盘，"
             "全部分支暂停出价（恢复仅经发布记录更新）")
    return {"halted": "FM_RECORD_CHAIN_CORRUPT", "chain": chain,
            "suspension": susp, "price_written": False}


def formal_result_markdown(res: dict) -> str:
    st = res["state"]
    head = [
        f"# 正式估值结果 {res['request_id']}",
        "",
        f"- 状态：**{st}**（四态之一；正式字段仅 priced 非空）",
        f"- 估值锚点：{res['anchor']}　估值时点口径 as_of：{res['as_of']}",
        f"- 采用分支：{res['branch']}",
        f"- 发布版本 composition_id：`{(res.get('combination') or {}).get('composition_id')}`",
        "",
    ]
    if st != fs.STATE_PRICED:
        return "\n".join(head + [
            "## 结论",
            "",
            f"- 无法正式估值（{st}）；正式价格字段为空。",
            f"- 原因码：{'、'.join(res['reason_codes']) or '（无）'}",
            "",
            "> 本结果不含正式价格，也不含候选诊断价格（防泄露结构保证）。",
        ]) + "\n"
    fr = res["formal_report"]
    fp = res["formal_price"]
    iv = fr["interval_and_width"]
    basis = fr["main_market_basis"]
    lines = head + [
        "## 正式报价",
        "",
        "| 项 | 值 |",
        "|---|---|",
        f"| 正式单价（元/㎡） | {fp['unit_price_per_sqm']:,.0f} |",
        f"| 正式总价（元） | {fp['total_price']:,.0f} |",
        f"| 交易面积（㎡） | {fp['area_sqm']} |",
        f"| 估值日 | {fr['valuation_date']} |",
        f"| 实际数据截止 | {fr['actual_data_cutoff']} |",
        f"| 80% 区间（元/㎡） | {iv['nominal_80']['low']:,.0f} ~ {iv['nominal_80']['high']:,.0f}（宽 {iv['width_80']:,.0f}） |",
        f"| 90% 区间（元/㎡） | {iv['nominal_90']['low']:,.0f} ~ {iv['nominal_90']['high']:,.0f}（宽 {iv['width_90']:,.0f}） |",
        f"| 区间来源层 | {iv['source_layer']}（样本 {iv['source_layer_n']}） |",
        "",
        "## 主要市场依据",
        "",
        f"- B0：{basis['b0_level']} 层，中心 {basis['b0_pred']:,.0f} 元/㎡（窗口样本 {basis['b0_window_n']}，"
        f"{basis['rule']}）",
        "",
        "## 适用范围与正式采用依据",
        "",
        f"- 适用人群：{fr['applicable_scope']['population']}；采用分支：{fr['applicable_scope']['branch']}",
        f"- 采用合同：{fr['formal_adoption_basis']['contract_id']}（判定 B："
        f"{fr['formal_adoption_basis']['verdict_b_conclusion']}；S7 确认："
        f"{fr['formal_adoption_basis']['s7_confirmed']}）",
        f"- 发布时效：{fr['applicable_scope']['release_validity']}",
        "",
        "## 主要限制",
        "",
    ] + [f"- {x}" for x in fr["main_limits"]]
    return "\n".join(lines) + "\n"


def write_formal_result(out_dir: Path, res: dict) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / f"{res['request_id']}.formal.json"
    mp = out_dir / f"{res['request_id']}.formal.md"
    jp.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str),
                  encoding="utf-8")
    mp.write_text(formal_result_markdown(res), encoding="utf-8")
    return {"json": str(jp), "markdown": str(mp)}


def formal_status_report(state_dir=None) -> dict:
    """运营状态（无任何价格字段）：开关/发布记录/分支停用/正式记录计数。"""
    sdir = Path(state_dir) if state_dir else sw.default_state_dir()
    stop = sw.check_stop_switch(sdir)
    try:
        rel = rr.load_release_record(rr.release_record_path(sdir))
        rel_err = None
    except rr.ReleaseRecordError as exc:
        rel, rel_err = None, str(exc)
    susp = sw.load_suspensions(sdir)
    recs = fb.load_records(formal_records_path(sdir))
    return {
        "schema_version": "phase2-formal-status-v1",
        "ops_state_dir": str(sdir),
        "stop": {"stopped": stop["stopped"], "present": stop["present"],
                 "reason_codes": stop["reason_codes"]},
        "release_record": ({"present": True, "schema_version": rel.get("schema_version"),
                            "expires_on": (rel.get("validity") or {}).get("expires_on"),
                            "allowed_branches": rel.get("allowed_branches"),
                            "composition_id": (rel.get("combination_fingerprint") or {})
                            .get("composition_id")}
                           if rel is not None else
                           {"present": False, "meaning": "未发布（默认态）",
                            "error": rel_err}),
        "branch_suspensions": {"n": len(susp),
                               "branches": sorted({s.get("branch") for s in susp})},
        "formal_records": {"path": str(formal_records_path(sdir)),
                           "present": formal_records_path(sdir).exists(),
                           "n_records": len(recs)},
        "note": "本报告不含任何价格字段",
    }


def formal_structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.candidate_ops（正式模式新增面）",
        "spec_basis": ("specs/phase2-formal-release「正式输出语义」「替代效果验收与发布生效」"
                       "「完整组合绑定」「上线后监控、补证与停止」；design D4/D5/D6；tasks 2.2/2.3/2.6"),
        "ops_version": FORMAL_OPS_VERSION,
        "four_states": list(fs.STATES),
        "decide_priority": ["stop_switch", "release_record_global",
                            "release_record_branch_expiry", "branch_suspension",
                            "input_rejected", "cutoff_and_timeliness",
                            "formal_gate", "priced"],
        "nine_fingerprint": list(fb.NINE_COMPONENTS),
        "rework_rv_ecr_verify_01": {
            "F5": "正式记录含 envelope 完整信封快照；formal_replay 直接重放落盘记录；"
                  "anchor 以记录保存值为准（消除重复参数）",
            "F6": "production_code 组件＝正式执行代码六文件聚合摘要（"
                  + ", ".join(PRODUCTION_CODE_MODULES) + "）；进程内 per-ops 缓存＝冻结语义",
            "F8": "CLI formal-estimate/batch 先验记录链后消费/追加/输出；损坏→全分支"
                  "停用登记＋退出码 5（无价格落盘）",
            "F9": "幂等键=(prediction_id, composition_id)；重复预测拒绝追加＋退出码 6",
        },
        "runtime_state_files": {"state_dir_default": str(OPS_STATE_DIR),
                                "release_record": rr.RELEASE_RECORD_FILENAME,
                                "stop_switch": sw.STOP_SWITCH_FILENAME,
                                "branch_suspended": sw.BRANCH_SUSPENDED_FILENAME,
                                "formal_records": FORMAL_RECORDS_FILENAME},
        "default_missing_meaning": "发布记录缺失＝未发布；停止开关缺失＝停止（fail-closed）；两者相互独立",
        "candidate_path_unchanged": "候选/研究路径（estimate/batch/replay/versions）行为不变",
        "formal_gate": "唯一资格实现 import（formal_gate." + fg.FORMAL_GATE_VERSION + "）",
    }


# ---------------------------------------------------------------- Markdown 报告

def _fmt_money(v) -> str:
    return "—" if v is None else f"{float(v):,.0f}"


def result_markdown(res: dict) -> str:
    st = res["status"]
    pt = res["point"]
    iv = res["interval"]
    vf = res["version_fingerprints"]
    lines = [
        f"# 候选估值结果 {res['request_id']}",
        "",
        f"- 估值锚点：{res['anchor']}（市场材料按锚点前 365 天现算）",
        f"- 状态：**{st['decision']}**（degradation_state={st['degradation_state']}，"
        f"cold_start={st['cold_start_community']}，reject={st['reject']}，"
        f"a1_m1_conflict={st['a1_m1_conflict']}）",
        "",
        "## 中心价与区间",
        "",
        "| 项 | 值 |",
        "|---|---|",
        f"| M1 中心单价（元/㎡） | {_fmt_money(pt['m1_pred_unit_price'])} |",
        f"| M1 中心总价（元） | {_fmt_money(pt['m1_pred_total_price'])} |",
        f"| 交易面积（㎡） | {pt['area_sqm']} |",
        f"| 80% 区间（元/㎡） | {_fmt_money(iv['nominal_80']['low'])} ~ {_fmt_money(iv['nominal_80']['high'])} |",
        f"| 90% 区间（元/㎡） | {_fmt_money(iv['nominal_90']['low'])} ~ {_fmt_money(iv['nominal_90']['high'])} |",
        f"| 区间分层 | {iv['stratum']}（来源层 {iv['source_layer']}） |",
        "",
        "## 支持依据",
        "",
    ]
    sup = res["support"]
    if sup is None:
        lines.append("- 无（请求被拒绝，不产出报价与支持依据）")
    else:
        lines += [
            f"- B0 层级：{sup['b0_level']}（窗口样本 {sup['b0_window_n']}，中心 {_fmt_money(sup['b0_pred'])} 元/㎡）",
            f"- A1 辅助：{sup['a1_status']}（案例 {sup['a1_n_cases']}，层级 {sup['a1_level']}，"
            f"修正前 {_fmt_money(sup['a1_pre_center'])} → 修正后 {_fmt_money(sup['a1_post_center'])} 元/㎡）",
        ]
    lines += ["", "## 主要限制", ""]
    lines += [f"- {x}" for x in res["limits"]]
    if res.get("issues"):
        lines += ["", "## 校验提示", ""]
        lines += [f"- [{i['severity']}] {i['field']}: {i['message']}" for i in res["issues"]]
    lines += [
        "", "## 版本指纹（模型/特征/市场资产/校准/协调策略）", "",
        f"- bundle_id：`{vf['bundle_id']}`",
    ]
    for k in ("model", "feature", "market_asset", "calibration", "coordination_policy"):
        lines.append(f"- {k}：`{vf[k]['digest']}`")
    lines += ["", "> 候选模式预做输出，不接入任何正式链路；区间外检验归 S5。"]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- 落盘

def write_result(out_dir: Path, res: dict) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / f"{res['request_id']}.json"
    mp = out_dir / f"{res['request_id']}.md"
    jp.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    mp.write_text(result_markdown(res), encoding="utf-8")
    return {"json": str(jp), "markdown": str(mp)}


def structure_report() -> dict:
    bundle_keys = ("model", "feature", "market_asset", "calibration", "coordination_policy")
    return {
        "module": "gz_property_valuation.phase2.candidate_ops",
        "spec_basis": "specs/phase2-candidate-ops「结果成套输出与版本指纹」「三路径推理一致性」"
                      "「候选输出不接正式链路」；design D1/D7",
        "entry": "python -m gz_property_valuation.phase2.candidate_ops <estimate|batch|replay|versions>",
        "modifies_existing_cli": False,
        "asset_load_fingerprint_enforced": ("V1 编码器/权重 ×2 双检；区间表对 S4C manifest（默认 S4C run）"
                                            "核对；降级变体/冻结合同/登记文件对 S4-B manifest 值核对；"
                                            "不符即 OpsHalt"),
        "interval_table_source": ("默认 S4C 演示 run（examples 合成产物）；--s4c-run 可覆盖；"
                                  "摘要对 S4C manifest 核对（换代后旧代区间表被拒绝）"),
        "version_bundle_components": list(bundle_keys),
        "result_six_elements": ["point（中心价）", "interval（区间）", "status（状态）",
                                "support（支持依据）", "limits（主要限制）",
                                "version_fingerprints（版本指纹）"],
        "degradation_path": DEGRADATION_PATH,
        "no_formal_consumer": "候选输出不接入任何正式链路；本模块可整体停用",
        "consumption_boundary": "只读 V1 推理资产与冻结合同；不修改任何既有实现",
        "verdict": "PASS",
    }


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    for line in Path(path).read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase2-candidate-ops")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("structure", help="结构清单自检")
    p_ver = sub.add_parser("versions", help="输出版本成套指纹")
    p_ver.add_argument("--anchor", default=None)
    for name in ("estimate", "batch"):
        p = sub.add_parser(name)
        p.add_argument("--request" if name == "estimate" else "--requests", required=True,
                       dest="requests")
        p.add_argument("--out-dir", required=True)
        p.add_argument("--anchor", default=None)
        p.add_argument("--as-of", default=None)
        p.add_argument("--s1-run", default=str(DEFAULT_S1_RUN))
        p.add_argument("--s3-run", default=str(DEFAULT_S3_RUN))
        p.add_argument("--s4b-run", default=str(DEFAULT_S4B_RUN))
        p.add_argument("--s4c-run", default=str(DEFAULT_S4C_RUN))
    p_rep = sub.add_parser("replay")
    p_rep.add_argument("--shadow", required=True)
    p_rep.add_argument("--out-dir", default=None)
    p_rep.add_argument("--s1-run", default=str(DEFAULT_S1_RUN))
    p_rep.add_argument("--s3-run", default=str(DEFAULT_S3_RUN))
    p_rep.add_argument("--s4b-run", default=str(DEFAULT_S4B_RUN))
    p_rep.add_argument("--s4c-run", default=str(DEFAULT_S4C_RUN))
    # ---- 正式模式子命令（enable-phase2-conditional-release；不改既有子命令行为） ----
    sub.add_parser("formal-structure", help="正式模式结构清单自检（无价格字段）")
    p_fst = sub.add_parser("formal-status", help="运营状态（开关/发布/停用/记录计数；无价格字段）")
    p_fst.add_argument("--state-dir", default=None)
    p_stop = sub.add_parser("stop-switch", help="整体停止开关（运营层；--on/--off）")
    p_stop.add_argument("--on", dest="stopped", action="store_true")
    p_stop.add_argument("--off", dest="stopped", action="store_false")
    p_stop.set_defaults(stopped=None)
    p_stop.add_argument("--reason", default=None)
    p_stop.add_argument("--operator", default=None)
    p_stop.add_argument("--state-dir", default=None)
    for name, req_arg in (("formal-estimate", "--request"), ("formal-batch", "--requests"),
                          ("formal-replay", "--shadow")):
        p = sub.add_parser(name)
        p.add_argument(req_arg, required=True, dest="requests")
        p.add_argument("--out-dir", default=None)
        p.add_argument("--state-dir", default=None)
        p.add_argument("--anchor", default=None)
        p.add_argument("--as-of", default=None)
        p.add_argument("--s1-run", default=str(DEFAULT_S1_RUN))
        p.add_argument("--s3-run", default=str(DEFAULT_S3_RUN))
        p.add_argument("--s4b-run", default=str(DEFAULT_S4B_RUN))
        p.add_argument("--s4c-run", default=str(DEFAULT_S4C_RUN))
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        _print(structure_report())
        return 0

    if args.cmd == "formal-structure":
        _print(formal_structure_report())
        return 0

    if args.cmd == "formal-status":
        _print(formal_status_report(args.state_dir))
        return 0

    if args.cmd == "stop-switch":
        if args.stopped is None:
            print("stop-switch 需要 --on 或 --off", file=sys.stderr)
            return 2
        doc = sw.write_stop_switch(args.state_dir, stopped=args.stopped,
                                   reason=args.reason, operator=args.operator)
        status = sw.check_stop_switch(args.state_dir)
        _print({"written": doc, "check": {"stopped": status["stopped"],
                                          "reason_codes": status["reason_codes"]}})
        return 0

    anchor = date.fromisoformat(args.anchor) if getattr(args, "anchor", None) else None
    if args.cmd == "versions":
        loaded = load_ops_assets()
        ops = CandidateOps(loaded)
        _print(ops.version_bundle(anchor))
        return 0

    loaded = load_ops_assets(Path(args.s1_run), Path(args.s3_run), Path(args.s4b_run),
                             Path(args.s4c_run))
    ops = CandidateOps(loaded)

    if args.cmd in ("formal-estimate", "formal-batch", "formal-replay"):
        as_of_f = date.fromisoformat(args.as_of) if args.as_of else None
        sdir = args.state_dir
        formal_kw = dict(state_dir=sdir, as_of=as_of_f, anchor=anchor)
        if args.cmd == "formal-estimate":
            rec_path = formal_records_path(sdir)
            # F8：先验完整性，后消费/追加/输出——损坏即停用并拒绝（无价格落盘、非零退出）
            halt = halt_on_corrupt_chain(sdir, rec_path)
            if halt is not None:
                _print(halt)
                return 5
            doc = json.loads(Path(args.requests).read_text(encoding="utf-8-sig"))
            env = formal_estimate(ops, doc, **formal_kw)
            rec = prediction_record_from_envelope(
                env, doc, datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))
            # F9：同键幂等——重复 CLI 请求/同键多预测拒绝（无价格落盘）
            dups = duplicate_predictions(rec_path, rec)
            if dups:
                _print({"appended": False, "duplicates": dups,
                        "rule": PREDICTION_IDEMPOTENCY_RULE, "price_written": False})
                return 6
            rec = fb.append_record(rec_path, rec)
            chain_after = fb.verify_chain(rec_path)
            paths = (write_formal_result(Path(args.out_dir), env)
                     if args.out_dir and chain_after["ok"] else None)
            _print({"request_id": env["request_id"], "state": env["state"],
                    "reason_codes": env["reason_codes"], "written": paths,
                    "record_appended": True, "chain_ok": chain_after["ok"]})
            return 0 if chain_after["ok"] else 5
        if args.cmd == "formal-batch":
            rec_path = formal_records_path(sdir)
            halt = halt_on_corrupt_chain(sdir, rec_path)
            if halt is not None:
                _print(halt)
                return 5
            docs = _read_jsonl(Path(args.requests))
            envs = formal_estimate_many(ops, docs, **formal_kw)
            now_s = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            recs = [prediction_record_from_envelope(env, doc, now_s)
                    for doc, env in zip(docs, envs)]
            dups = duplicate_predictions(rec_path, *recs)
            if dups:
                _print({"appended": False, "duplicates": dups,
                        "rule": PREDICTION_IDEMPOTENCY_RULE, "price_written": False})
                return 6
            for rec in recs:
                fb.append_record(rec_path, rec)
            chain_after = fb.verify_chain(rec_path)
            if not chain_after["ok"]:
                _print({"halted": "FM_RECORD_CHAIN_CORRUPT", "chain": chain_after,
                        "price_written": False})
                return 5
            out_dir = Path(args.out_dir) if args.out_dir else None
            if out_dir:
                out_dir.mkdir(parents=True, exist_ok=True)
                for env in envs:
                    write_formal_result(out_dir, env)
            counts: dict[str, int] = {}
            for env in envs:
                counts[env["state"]] = counts.get(env["state"], 0) + 1
            if out_dir:
                (out_dir / "formal-results.json").write_text(
                    json.dumps(envs, ensure_ascii=False, indent=1, default=str),
                    encoding="utf-8")
            _print({"rows": len(envs), "states": counts,
                    "chain_ok": chain_after["ok"],
                    "out_dir": str(out_dir) if out_dir else None})
            return 0
        # formal-replay：读正式落盘记录（record_type=prediction：input_snapshot＋
        # envelope 快照）直接重放逐位比对；旧版组合→拒绝。F5：anchor 以记录内
        # 保存值为准，CLI 不再向 formal_replay 传 anchor（消除重复参数）。
        shadow = _read_jsonl(Path(args.requests))
        replay_kw = {k: v for k, v in formal_kw.items() if k != "anchor"}
        rep = formal_replay(ops, shadow, **replay_kw)
        if args.out_dir:
            out_dir = Path(args.out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "formal-replay.json").write_text(
                json.dumps(rep, ensure_ascii=False, indent=1, default=str),
                encoding="utf-8")
        _print(rep)
        return 0 if rep["bitwise_equal"] else 1

    if args.cmd == "estimate":
        doc = json.loads(Path(args.requests).read_text(encoding="utf-8-sig"))
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        res = estimate(ops, doc, as_of=as_of, anchor=anchor)
        paths = write_result(Path(args.out_dir), res)
        _print({"request_id": res["request_id"], "decision": res["status"]["decision"],
                "written": paths})
        return 0

    if args.cmd == "batch":
        docs = _read_jsonl(Path(args.requests))
        as_of = date.fromisoformat(args.as_of) if args.as_of else None
        results = estimate_many(ops, docs, as_of=as_of, anchor=anchor)
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for res in results:
            write_result(out_dir, res)
        (out_dir / "results.json").write_text(
            json.dumps(results, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        counts: dict[str, int] = {}
        for res in results:
            counts[res["status"]["decision"]] = counts.get(res["status"]["decision"], 0) + 1
        scope = {
            "requests_total": len(results),
            "priced": sum(1 for r in results if r["status"]["decision"] != "rejected"),
            "out_of_scope_rejected": sum(1 for r in results if r["status"]["out_of_scope"]),
            "rejected_other_reason": sum(
                1 for r in results if r["status"]["decision"] == "rejected"
                and not r["status"]["out_of_scope"]),
            "scope_unverified": sum(1 for r in results if r["status"]["scope_unverified"]),
            "rule": "适用人群＝云溪区普通住宅；过滤前后计数按批登记（spec 请求映射与确定行为）",
        }
        (out_dir / "scope-filter-summary.json").write_text(
            json.dumps(scope, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        _print({"rows": len(results), "decisions": counts, "scope_filter": scope,
                "out_dir": str(out_dir)})
        return 0

    # replay：读影子记录（含请求快照），重跑并逐位比对
    shadow = _read_jsonl(Path(args.shadow))
    ok = True
    checked = 0
    mismatches = []
    for rec in shadow:
        prev = rec.get("result") or {}
        anchor_prev = date.fromisoformat(prev["anchor"]) if prev.get("anchor") else anchor
        res = estimate(ops, rec["request"], anchor=anchor_prev)
        checked += 1
        if json.dumps(res, sort_keys=True, default=str) != json.dumps(prev, sort_keys=True,
                                                                     default=str):
            ok = False
            mismatches.append(res["request_id"])
    _print({"replayed": checked, "bitwise_equal": ok, "mismatches": mismatches})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
