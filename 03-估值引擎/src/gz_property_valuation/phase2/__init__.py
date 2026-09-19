# -*- coding: utf-8 -*-
"""第二阶段数据、特征与验证合同层（build-phase2-data-contracts / S1）。

蓝图 §4（S1）：建立目标区外源数据的独立训练层、特征字典、时间与可用性规则、
开发切分及最终测试接收合同，完成泄漏和事件隔离检查。

模块（design D1，扁平结构）：

- :mod:`gz_property_valuation.phase2.lineage`  来源指针固定、运行清单、评估身份映射
- :mod:`gz_property_valuation.phase2.data`     训练主表与清洗规则 v0（S0 修正口径重写）
- :mod:`gz_property_valuation.phase2.features` 特征字典声明与特征生成
- :mod:`gz_property_valuation.phase2.splits`   开发折、用途切片、独立测试接收规则与合同渲染
- :mod:`gz_property_valuation.phase2.checks`   泄漏 / 事件隔离 / 重建一致性检查

行为规格：openspec/changes/build-phase2-data-contracts/specs/phase2-data-contracts/spec.md。
S2 起的模型代码只消费本层产物，不直接读 staged 原始数据（design Goals）。

线程钉死（审2 F1，RV-BP2B-VERIFY-01）：本包被导入时、且必须先于 numpy/polars 导入，
强制将 BLAS/OMP/MKL/NumExpr/Veclib/polars 线程数钉为 1——BLAS 多线程归约顺序不定，
会使 M1 闭式解预测产生 ~1e-12 量级跨进程浮点抖动、逐折 parquet 字节哈希不一致，
违反 spec「结果可复现」；单线程下闭式解与确定性聚合可字节级复现。任何入口若在导入
本包之前已导入 numpy/polars（线程环境变量迟设无效），本包将抛 RuntimeError 拒绝继续。
"""

import os as _os
import sys as _sys

_THREAD_PIN_VARS = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "POLARS_MAX_THREADS",
)
for _var in _THREAD_PIN_VARS:
    _os.environ[_var] = "1"

_LATE_NUMERIC_IMPORTS = sorted({"numpy", "polars"} & set(_sys.modules))
if _LATE_NUMERIC_IMPORTS:
    raise RuntimeError(
        "gz_property_valuation.phase2 必须先于 numpy/polars 导入："
        "BLAS/polars 线程数须在数值库初始化前钉死为 1 才能保证跨进程字节级复现"
        "（spec「结果可复现」/审2 F1，RV-BP2B-VERIFY-01）。"
        f"检测到先导入的模块：{_LATE_NUMERIC_IMPORTS}；"
        "请把对 phase2 的 import 移到 numpy/polars 之前。")

__all__ = ["checks", "data", "features", "lineage", "splits"]
