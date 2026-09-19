"""子区修正配置（OpenSpec change comparable-family-subarea-adjustment）。

配置文件 ``<data_dir>/rules/subarea_adjustment.v1.json`` 承载家族层 + 子区修正
的全部可调参数（启用门槛、bootstrap 参数、面积段格宽、修正幅度上限、方向性
区间扩张预算、可信度封顶映射、随机种子）。

版本语义（design D7）：

- 规则版本 ``1.2`` = 家族层 + 子区修正启用；配置缺失/不可解析/字段非法 →
  **整体回退**现行行为（家族层不启用，即 rule 1.1 行为；任务 4.1 冻结语义，
  与 interval_calibration 的显式失败策略不同——此处按 change 任务约定保守回退）；
- ``1.0``/``1.1`` 不读本配置；
- 生产数值必须由试跑证据提出并经用户明确确认后写入（任务 7.2）；当前文件内容
  为**试跑初值**，不得视为已确认门槛。

汇总策略纪律：rule 1.2 固定 ``aggregation_policy=c0_weighted_median``（与上线前
1.1 同款策略），不开放策略切换（回放归因唯一变量 = 规则版本）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final

#: 家族层 + 子区修正的规则版本（estimate 链 1.1 → 1.2）。
FAMILY_RULE_VERSION: Final = "1.2"

#: 配置文件名（数据湖 rules 层）。
SUBAREA_CONFIG_DIRNAME: Final = "rules"
SUBAREA_CONFIG_FILENAME: Final = "subarea_adjustment.v1.json"

#: 配置方法标签（加载时校验，防错配其他产物）。
SUBAREA_CONFIG_METHOD: Final = "family_subarea_adjustment"


@dataclass(frozen=True)
class SubareaConfig:
    """子区修正可调参数（全部有界；非法配置整体回退，不部分生效）。"""

    #: 启用门槛：跨子区 12 个月有效案例数下限（试跑初值 5，生产值待用户确认）。
    min_subarea_cases_12m: int
    #: bootstrap 重采样次数（试跑初值 1000，Q4 已确认）。
    bootstrap_iterations: int
    #: bootstrap 置信水平（试跑初值 0.95，Q4 已确认；分离 = CI 不含 1）。
    confidence_level: float
    #: bootstrap 随机种子（可复现；同输入重跑逐位一致）。
    random_seed: int
    #: 面积段边界（㎡，升序；如 [50, 70, 90, 120] → ≤50/50-70/70-90/90-120/>120）。
    area_segments: tuple[float, ...]
    #: 格内每侧最低样本数（低于 → 不启用数值修正，降级方向性；试跑初值=门槛值）。
    min_cell_samples_per_side: int
    #: 修正幅度上限：作用于修正后幅度 |1/r-1|（r=源/基准），超出降级方向性
    #: （试跑初值，生产值待确认；语义版本边界见 change fix-subarea-ratio-direction）。
    cap_ratio: float
    #: 方向性因素的区间扩张预算（占价比；试跑初值，生产值待确认）。
    directional_widening_budget: float
    #: 方向性因素的可信度封顶（Q6 已确认初值「中」；生产值待任务 7.2 确认）。
    confidence_cap_directional: str


def subarea_config_path(data_dir: Path) -> Path:
    """配置路径：``<data_dir>/rules/subarea_adjustment.v1.json``。"""
    return data_dir / SUBAREA_CONFIG_DIRNAME / SUBAREA_CONFIG_FILENAME


def load_subarea_config(data_dir: Path) -> SubareaConfig | None:
    """加载子区修正配置；缺失/不可解析/字段非法 → ``None``（整体回退，不部分生效）。

    回退语义（任务 4.1 冻结）：配置缺失或损坏时家族层与子区修正一律不启用，
    估值行为与 rule 1.1 逐值一致；不提供部分启用或静默改参。
    """
    path = subarea_config_path(data_dir)
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    if str(raw.get("method", "")) != SUBAREA_CONFIG_METHOD:
        return None

    def _int(name: str, minimum: int) -> int | None:
        value = raw.get(name)
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            return None
        return int(value)

    def _float(name: str, low: float, high: float) -> float | None:
        value = raw.get(name)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None
        number = float(value)
        if not low < number < high:
            return None
        return number

    min_cases = _int("min_subarea_cases_12m", 1)
    iterations = _int("bootstrap_iterations", 1)
    seed = _int("random_seed", 0)
    min_cell = _int("min_cell_samples_per_side", 1)
    level = _float("confidence_level", 0.0, 1.0)
    cap = _float("cap_ratio", 0.0, 1.0)
    budget = _float("directional_widening_budget", 0.0, 1.0)
    if None in (min_cases, iterations, seed, min_cell, level, cap, budget):
        return None

    segments_raw = raw.get("area_segments")
    if (
        not isinstance(segments_raw, list)
        or len(segments_raw) < 1
        or not all(
            isinstance(item, (int, float)) and not isinstance(item, bool) and item > 0
            for item in segments_raw
        )
    ):
        return None
    segments = tuple(float(item) for item in segments_raw)
    if list(segments) != sorted(set(segments)):
        return None

    cap_level = str(raw.get("confidence_cap_directional", ""))
    if cap_level not in ("高", "中", "低"):
        return None

    return SubareaConfig(
        min_subarea_cases_12m=int(min_cases),  # type: ignore[arg-type]
        bootstrap_iterations=int(iterations),  # type: ignore[arg-type]
        confidence_level=float(level),  # type: ignore[arg-type]
        random_seed=int(seed),  # type: ignore[arg-type]
        area_segments=segments,
        min_cell_samples_per_side=int(min_cell),  # type: ignore[arg-type]
        cap_ratio=float(cap),  # type: ignore[arg-type]
        directional_widening_budget=float(budget),  # type: ignore[arg-type]
        confidence_cap_directional=cap_level,
    )
