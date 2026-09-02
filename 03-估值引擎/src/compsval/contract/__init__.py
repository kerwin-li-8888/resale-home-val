"""Phase-1 data contract (WP3-D).

Implements the minimum contract of the ExampleCity property-valuation system:
the four WP3-D entities (``source_registry``, ``raw_snapshot``, ``sale_event``,
``listing_event``), the common evidence fields, JSON Schema output, and the
ExampleCity source/dataset registration — following 数据字典-V0.1 §1-§3 and
第一阶段技术方案 §7.
"""

from __future__ import annotations

from compsval.contract import models, registry, sample_data

__all__ = ["models", "registry", "sample_data"]
