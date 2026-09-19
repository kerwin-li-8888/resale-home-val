# UPSTREAM.md — 上游来源登记（v2）

> 本仓库（resale-home-val）的工程骨架来自一个固定版本的上游开源项目；量化模型
> 层（v2 新增）为本项目原创。本文登记来源、许可证与采用决定；v1 发布时的完整
> 逐文件来源审计在上游底座仓库存档，本文件保留其结论。

## 1. 主底座：Philly Fair Measure

| 项 | 内容 |
|---|---|
| 仓库地址 | https://github.com/nickhand/philly-fair-measure |
| 固定提交 SHA | `e163eba68d989f80dfeb7293e51cf532ac18ef07`（2026-08-21 经 `git ls-remote` 核验为远端 HEAD） |
| 取得日期 | 2026-08-21 |
| 许可证 | MIT License（Copyright (c) 2026 Nick Hand） |
| LICENSE 原文存档 | [`03-估值引擎/upstream/LICENSE-philly-fair-measure-e163eba6.txt`](03-估值引擎/upstream/LICENSE-philly-fair-measure-e163eba6.txt) |
| 采用方式 | 固定版本，禁止浮动 main；升级必须显式差异审计 |

## 2. 专项参考项目（不进入运行路径）

| 项目 | 仓库地址 | 用途 |
|---|---|---|
| mcp-imo | https://github.com/zedd75/mcp-imo | 透明可比案例汇总思路 |
| open-comps | https://github.com/property-hackers/open-comps | 证据和事件数据结构 |
| Cook County residential AVM | https://github.com/ccao-data/model-res-avm | 成交校验和批量估值治理 |

三个参考项目仅作参考阅读，不固定依赖、不 clone 入库、不进入运行路径。

## 3. 逐文件来源登记（继承自 v1 审计结论）

自上游固定 SHA 导入并改造的骨架文件（均为"保留骨架、删除上游城市专属逻辑"
的适配，来源均为 `e163eba6` 对应上游文件）：

| 导入文件（本包） | 上游来源 | 状态 |
|---|---|---|
| `__init__.py` | `philly_fair_measure/__init__.py` | 改造：包名重命名、版本号、骨架说明 |
| `config.py` | `philly_fair_measure/config.py` | 改造：删除上游城市常量，环境变量改为本项目命名 |
| `catalog.py` | `philly_fair_measure/catalog.py` | 保留原样（行为未改）：仅改包名 import |
| `cli.py` | `philly_fair_measure/cli.py` | 改造：仅保留系统骨架命令 |
| `scalars.py` | `philly_fair_measure/scalars.py` | 保留原样 |
| `py.typed` | `philly_fair_measure/py.typed` | 保留原样 |
| `ingest/manifests.py` | `philly_fair_measure/ingest/manifests.py` | 保留原样 |
| `ingest/diff.py` | `philly_fair_measure/ingest/diff.py` | 改造：删除上游数据集定义，保留通用去重与 diff 逻辑 |
| `ingest/snapshots.py` | `philly_fair_measure/ingest/snapshots.py` | 改造：删除上游抓取客户端，保留不可变快照写入原语 |

停用不导入（上游城市专属）：`sources/*`、`staging/*`、`validation/opa.py`、
`diagnostics/*`、`models/*`、`api.py`、`web/` 等上游费城专属与暂缓模块。

上游依赖中的 assesspy（AGPL-3.0）**拒绝引入运行路径**（强 copyleft；比较法
核心与量化模型层均不需要它）。

## 4. 本项目的原创部分

- **比较法估值核心**（v1，历史包名 `compsval`，v2 起更名
  `gz_property_valuation`）：可比候选选择、时间修正、差异处理、稳健聚合、
  区间校准、复核留痕、历史回放——原创，非上游代码；
- **量化模型层**（v2 新增，`src/gz_property_valuation/phase2/` 及配套模块）：
  B0/M1/GBM 受控比较、候选冻结与推理、候选运维与影子记录、冻结与版本指纹、
  降级路径、停止开关与 fail-closed 发布门禁——原创。

## 5. 本地包名沿革

| 代次 | Python 包名 | CLI 名 |
|---|---|---|
| v1（测试版，历史名） | `compsval` | `compsval` |
| v2（本版，正式版） | `gz_property_valuation` | `gzv` |

上游升级规则：仅通过显式差异审计进入，禁止自动追随 main。
