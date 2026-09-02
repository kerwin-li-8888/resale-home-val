"""WP5 领域实体模块（实体权威表与匹配）。

WP5 以"现有数据先行"构建小区/楼栋/市场序列领域实体：candidates 转录候选小区
名录（房天下骨架），community 构建小区实体权威表（写入 ``data/entities/``）；
别名映射（community_alias）、楼栋弱实体（building）、市场序列登记
（market_series）分别在 WP5-B/C/D 落地，community_id 回填在 WP5-E。
"""

from __future__ import annotations

from compsval.entities import (
    alias,
    backfill,
    building,
    candidates,
    community,
    market_series,
)

__all__ = [
    "alias",
    "backfill",
    "building",
    "candidates",
    "community",
    "market_series",
]
