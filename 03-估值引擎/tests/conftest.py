# -*- coding: utf-8 -*-
"""套件级导入顺序守卫（与 gz_property_valuation.phase2 包守卫配套）。

gz_property_valuation.phase2 要求先于 numpy/polars 导入：BLAS/polars 线程数须在
数值库初始化前钉死为 1 才能保证跨进程字节级复现（spec「结果可复现」/审2 F1，
RV-BP2B-VERIFY-01）。全量 pytest 进程内，字母序靠前的基线测试可能先导入
numpy/polars，导致 phase2 级测试在收集期触发 RuntimeError。

conftest 由 pytest 在收集任何测试模块之前导入——在此最先导入 phase2，即满足
守卫前提，并使整个测试套件与生产入口同序（单线程数值环境）。
"""

import gz_property_valuation.phase2  # noqa: F401  必须保持为本文件首个导入
