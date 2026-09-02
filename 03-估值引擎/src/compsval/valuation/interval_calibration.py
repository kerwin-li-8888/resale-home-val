"""区间校准（OpenSpec change ``interval-calibration-g5``）：经验分位数校准的分层宽度展开。

v1.0 加权分位区间系统性偏窄（回放覆盖 66.42%/影子追踪 62.07% < GATE1-001
确认的 80-90% 门槛，G5 证据 §3.0）。本模块以时间外校准的分层展开参数把
区间宽度放到位，同时守住 README §6.8 单调性与 §7.2「区间相对宽度」防过宽：

- **版本语义**：``rule_version="1.0"`` 保持 v1.0 旧行为（不读配置，G3 回放
  证据可复现）；``"1.1"`` = 校准区间链，读取
  ``<data_dir>/rules/interval_calibration.v1.1.json``——配置缺失 →
  ``MissingDependencyError``、内容非法 → ``InvalidInputError``（保守显式
  失败，不静默回退未校准宽度，也不提供绕行开关）；
- **中心值零改动**：展开只作用于区间两侧半宽
  （``center ± max(k·half, m·center)``，k≥1 为半宽乘数、m≥0 为相对半宽
  下限），中心、可比选择、时间/差异修正路径一律不变；
- **单调性**：同层内可信度越低基础分位越宽（``QUANTILES_BY_CONFIDENCE``），
  展开参数按 ``(community_id, confidence) → (*, confidence) → (*, *)`` 回退
  且只加不减（k≥1、m≥0），数据弱度增加时区间相对宽度不收窄；
- **防过宽**：k/m 上限（caps）与分层样本量下限（min_layer_samples，不足
  回退全局层）在配置加载时强校验；覆盖率靠校准真实达成，宽度代价由双指标
  报告如实呈现。

校准与验证严格时间外：配置只能由冻结的校准窗口（估值时点 ≤ 切分点）样本
推导（见 ``scripts/interval_calibration/`` 构建脚本与 04-校验/ 冻结清单），
留出窗口与漂移窗口信息不得回流调参。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from compsval.reporting.envelope import (
    InvalidInputError,
    MissingDependencyError,
)

#: 校准区间链规则版本（estimate 链 1.0 → 1.1；仅区间宽度构造变更）。
INTERVAL_CALIBRATION_RULE_VERSION: Final = "1.1"

#: 旧行为版本：不读校准配置（G3 回放证据可复现基线）。
LEGACY_RULE_VERSION: Final = "1.0"

#: 校准配置目录（数据湖内，与派生表同源管理）。
CALIBRATION_DIRNAME: Final = "rules"

#: 配置方法标签（加载时强校验，防错配其他方法产物）。
CALIBRATION_METHOD: Final = "empirical_quantile_expansion"

#: 层通配符（community_id/confidence 的回退层键）。
WILDCARD: Final = "*"


def calibration_config_path(data_dir: Path, rule_version: str) -> Path:
    """校准配置路径：``<data_dir>/rules/interval_calibration.v<版本>.json``。"""
    return (
        data_dir
        / CALIBRATION_DIRNAME
        / f"interval_calibration.v{rule_version}.json"
    )


@dataclass(frozen=True)
class ExpansionParams:
    """一层目标的展开参数：k = 半宽乘数（≥1），m = 相对半宽下限（≥0）。"""

    k: Decimal
    m: Decimal
    n: int  # 推导该参数的校准窗口样本数（可溯源；回退层为全局样本数）


@dataclass(frozen=True)
class IntervalCalibration:
    """已加载并校验的区间校准配置（不可变；层参数含通配回退）。"""

    rule_version: str
    split_point: str
    target_coverage: float
    source_dataset_sha256: str
    built_at: str
    n_calibration_samples: int
    min_layer_samples: int
    k_max: Decimal
    m_max: Decimal
    params_by_layer: Mapping[tuple[str, str], ExpansionParams]

    def params_for(self, community_id: str, confidence: str) -> ExpansionParams:
        """层参数回退查找：(小区, 可信度) → (*, 可信度) → (*, *)。

        回退保证任意（小区, 可信度）组合都有参数；k/m 下限保证展开只加不减
        （数据弱度增加时区间相对宽度不收窄，README §6.8）。
        """
        for key in (
            (community_id, confidence),
            (WILDCARD, confidence),
            (WILDCARD, WILDCARD),
        ):
            params = self.params_by_layer.get(key)
            if params is not None:
                return params
        raise InvalidInputError(
            f"区间校准配置缺少全局回退层 ({WILDCARD}, {WILDCARD})，配置不完整"
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InvalidInputError(f"区间校准配置非法：{message}")


def _parse_decimal(value: Any, what: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise InvalidInputError(f"区间校准配置非法：{what} 不是数值（{value!r}）") from exc
    return parsed


def load_interval_calibration(
    data_dir: Path, rule_version: str
) -> IntervalCalibration | None:
    """按规则版本加载校准配置；``1.0`` → None（旧行为），其余必须配置就绪。

    保守原则：配置缺失（``MissingDependencyError``，退出码 3）或内容非法
    （``InvalidInputError``，退出码 2）一律显式失败，不静默回退未校准宽度。
    """
    if rule_version == LEGACY_RULE_VERSION:
        return None
    path = calibration_config_path(data_dir, rule_version)
    if not path.is_file():
        raise MissingDependencyError(
            f"区间校准配置缺失（规则版本 {rule_version} 需要校准资产）：{path}"
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvalidInputError(f"区间校准配置不可解析：{path}：{exc}") from exc
    _require(isinstance(raw, dict), f"配置必须为 JSON 对象（{path}）")

    _require(
        str(raw.get("method", "")) == CALIBRATION_METHOD,
        f"method 必须为 {CALIBRATION_METHOD}",
    )
    _require(
        str(raw.get("config_version", "")) == rule_version,
        f"config_version {raw.get('config_version')!r} 与规则版本 {rule_version} 不一致",
    )
    split_point = str(raw.get("split_point", ""))
    _require(bool(split_point), "split_point 缺失（时间切分点，防泄漏审计必需）")
    built_at = str(raw.get("built_at", ""))
    _require(bool(built_at), "built_at 缺失（校准时间溯源必需）")
    source_sha = str(raw.get("source_dataset_sha256", ""))
    _require(len(source_sha) == 64, "source_dataset_sha256 缺失或非 SHA256")

    target = raw.get("target_coverage")
    _require(
        isinstance(target, (int, float)) and 0.0 < float(target) < 1.0,
        f"target_coverage 必须在 (0,1)（{target!r}）",
    )
    n_samples = raw.get("n_calibration_samples")
    _require(
        isinstance(n_samples, int) and n_samples > 0,
        f"n_calibration_samples 必须为正整数（{n_samples!r}）",
    )
    min_layer = raw.get("min_layer_samples")
    _require(
        isinstance(min_layer, int) and min_layer > 0,
        f"min_layer_samples 必须为正整数（{min_layer!r}）",
    )
    caps = raw.get("caps")
    _require(isinstance(caps, dict), "caps 缺失（k_max/m_max 防过宽上限必需）")
    k_max = _parse_decimal(caps.get("k_max"), "caps.k_max")
    m_max = _parse_decimal(caps.get("m_max"), "caps.m_max")
    _require(k_max >= Decimal("1"), "caps.k_max 必须 ≥ 1")
    _require(m_max > 0, "caps.m_max 必须 > 0")

    layers = raw.get("layers")
    _require(isinstance(layers, list) and len(layers) > 0, "layers 必须为非空数组")
    params_by_layer: dict[tuple[str, str], ExpansionParams] = {}
    for item in layers:
        _require(isinstance(item, dict), "layers 元素必须为对象")
        community = str(item.get("community_id", ""))
        confidence = str(item.get("confidence", ""))
        _require(bool(community) and bool(confidence), "层键 community_id/confidence 缺失")
        key = (community, confidence)
        _require(key not in params_by_layer, f"层键重复：{key}")
        k = _parse_decimal(item.get("k"), f"{key} k")
        m = _parse_decimal(item.get("m"), f"{key} m")
        n = item.get("n")
        _require(isinstance(n, int) and n > 0, f"{key} n 必须为正整数（{n!r}）")
        _require(
            Decimal("1") <= k <= k_max,
            f"{key} k={k} 越界（须在 [1, {k_max}]，展开只加不减且防过宽）",
        )
        _require(
            Decimal("0") <= m <= m_max,
            f"{key} m={m} 越界（须在 [0, {m_max}]）",
        )
        params_by_layer[key] = ExpansionParams(k=k, m=m, n=n)
    _require(
        (WILDCARD, WILDCARD) in params_by_layer,
        f"缺少全局回退层 ({WILDCARD}, {WILDCARD})",
    )

    return IntervalCalibration(
        rule_version=rule_version,
        split_point=split_point,
        target_coverage=float(target),
        source_dataset_sha256=source_sha,
        built_at=built_at,
        n_calibration_samples=n_samples,
        min_layer_samples=min_layer,
        k_max=k_max,
        m_max=m_max,
        params_by_layer=params_by_layer,
    )


def expand_interval(
    center: Decimal,
    lower: Decimal | None,
    upper: Decimal | None,
    params: ExpansionParams,
) -> tuple[Decimal, Decimal] | None:
    """校准展开：``center ± max(k·半宽, m·center)``；无区间（None）不展开。

    中心值原样返回（不参与运算变形）；k≥1 且 m≥0 保证新区间包含原区间
    （只加不减）；调用方负责后续 quantize 落盘精度。
    """
    if lower is None or upper is None:
        return None
    lo_half = center - lower
    up_half = upper - center
    floor = params.m * center
    new_lo_half = max(params.k * lo_half, floor)
    new_up_half = max(params.k * up_half, floor)
    return center - new_lo_half, center + new_up_half
