"""估值规则层（VAL1-001 适用范围判断、VAL1-002 候选案例池等）。

本包承载不直接产出一条市场证据、而是把多条证据/实体转化为判定结果的规则模块。
当前已实现：

- :mod:`compsval.valuation.scope`（ScopePolicy，VAL1-001，WP5-F）；
- :mod:`compsval.valuation.candidate`（CandidateRetriever，
  VAL1-002，WP6-A）。
"""

from __future__ import annotations

from compsval.valuation import candidate, scope

__all__ = ["candidate", "scope"]
