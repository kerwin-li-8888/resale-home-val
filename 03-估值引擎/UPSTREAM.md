# UPSTREAM.md — 上游来源登记（WP0 定稿）

> 状态：**OSS-001-A—E 全部完成**（SHA/LICENSE、依赖与许可证、离线测试复现、未修改基线、复用/改造/停用矩阵与采用决定均已落地；G0 门禁判定见 [`../00-项目总控/G0-上游基线报告.md`](../00-项目总控/G0-上游基线报告.md)）。**WP1 已导入骨架**：§5 复用文件明细已升级为逐文件来源登记表（`compsval` 包 + `compsval` CLI + `system check` 门禁，见 WP1 任务状态文件）。
> 上位规则：[`../README.md`](../README.md) §9、[`../00-项目总控/第一阶段技术方案-V0.1.md`](../00-项目总控/第一阶段技术方案-V0.1.md) §3。
> 更新日期：2026-08-21。

## 1. 主底座：Philly Fair Measure

| 项 | 内容 |
|---|---|
| 仓库地址 | https://github.com/nickhand/philly-fair-measure |
| 固定提交 SHA | `e163eba68d989f80dfeb7293e51cf532ac18ef07`（2026-08-21 经 `git ls-remote` 核验为远端 HEAD） |
| 取得日期 | 2026-08-21 |
| 许可证 | MIT License（Copyright (c) 2026 Nick Hand） |
| LICENSE 原文存档 | [`upstream/LICENSE-philly-fair-measure-e163eba6.txt`](./upstream/LICENSE-philly-fair-measure-e163eba6.txt) |
| 采用方式 | 固定版本，禁止浮动 main（技术方案 §3.1）；升级必须显式差异审计 |
| 原子任务状态 | A 固定来源 ✅ / B 依赖与许可证 ✅ / C 离线测试复现 ✅ / D 未修改基线 ✅ / E 矩阵与采用决定 ✅（本文件 §5/§6/§7） |

## 2. 专项参考项目（README §9，不进入运行路径）

| 项目 | 仓库地址 | 固定 SHA（2026-08-21 核验为远端 HEAD） | 专项参考用途 |
|---|---|---|---|
| mcp-imo | https://github.com/zedd75/mcp-imo | `8094233e9ddd10d1eac162ce21f9df7b4e330d29` | 透明可比案例汇总思路 |
| open-comps | https://github.com/property-hackers/open-comps | `a2c42772c766fe31eb900fd7368cf900cd8b8190` | 证据和事件数据结构 |
| Cook County residential AVM | https://github.com/ccao-data/model-res-avm | `57fe99b9cc603f9fe266d914686192bd909e45ec` | 成交校验和批量估值治理 |

说明：
- 三个参考项目的 SHA 与主底座同日核验，仅作参考阅读，不固定依赖、不 clone 入库、不进入第一阶段运行路径；
- 采用决定（复用/借鉴/拒绝及理由）在 OSS-001-E 汇总；
- 参考项目 LICENSE 与依赖如后续实际引用，在 OSS-001-B/E 阶段补充登记。

## 3. 未修改基线（OSS-001-D）

| 项 | 内容 |
|---|---|
| 基线归档 | `upstream-baseline/philly-fair-measure-e163eba6.zip`（1,037,991 字节） |
| 归档内容 | 固定 SHA `e163eba68d989f80dfeb7293e51cf532ac18ef07` 的完整被跟踪工作树（238 个文件，git archive 导出，不含 .git/.venv） |
| SHA256 | `897E5A3743C9D7C21D41F16CD29BE532B74F5DBEDA5996B2E6BE61F93F9371D6` |
| 生成方式 | `git -C upstream-audit archive --format=zip -o <路径> e163eba68d989f80dfeb7293e51cf532ac18ef07` |
| 验证记录 | 2026-08-21：解压后文件数 238 与 `git ls-files` 一致；抽查 README.md 哈希与上游工作区一致（`753B5850…`）；zip 哈希已记录 |
| 与审计副本关系 | 审计副本 `upstream-audit/` 为可重建克隆（git clone + checkout 固定 SHA），基线 zip 为不可变归档；两者均未修改上游内容 |

### 回滚说明

- **恢复未修改上游源码**：解压 `upstream-baseline/philly-fair-measure-e163eba6.zip` 到任意目录即得固定 SHA 的未修改源码；解压后核对 README.md 哈希应为 `753B5850E52E998E86FFEB411C60A0807486AAED5908D472BFF58CF0D03D0E5A`（zip 哈希 `897E5A37…` 作为归档完整性校验）。
- **重建审计副本**：`git clone https://github.com/nickhand/philly-fair-measure.git <dir>` + `git checkout e163eba68d989f80dfeb7293e51cf532ac18ef07`（需网络）。
- **第一阶段代码回退**：若后续示例城市适配（WP1+）引入问题，回退到本基线 zip 代表的上游未修改状态，再重新导入。
- 归档与审计副本均不进入上级 git 跟踪（`03-估值引擎/.gitignore`）。

## 4. 核验记录

- 2026-08-21：`git ls-remote https://github.com/nickhand/philly-fair-measure.git HEAD` → `e163eba68d989f80dfeb7293e51cf532ac18ef07`
- 2026-08-21：`git ls-remote https://github.com/zedd75/mcp-imo.git HEAD` → `8094233e9ddd10d1eac162ce21f9df7b4e330d29`
- 2026-08-21：`git ls-remote https://github.com/property-hackers/open-comps.git HEAD` → `a2c42772c766fe31eb900fd7368cf900cd8b8190`
- 2026-08-21：`git ls-remote https://github.com/ccao-data/model-res-avm.git HEAD` → `57fe99b9cc603f9fe266d914686192bd909e45ec`
- LICENSE 原文经 `raw.githubusercontent.com/nickhand/philly-fair-measure/<SHA>/LICENSE` 取得并原样保存

## 5. 复用/改造/停用矩阵（OSS-001-E，依据技术方案 §3.2，基于固定 SHA 实际源码核验）

> 与 §3.2 的映射说明：§3.2 的"重写"在本矩阵按实际源码细化为"停用（费城连接器）或改造（保留骨架重写示例城市逻辑）"；§3.2"美国地块、契税、融资与申诉逻辑"对应本矩阵 `staging/parcels.py` 停用、`diagnostics/` 与 `models/risk.py` 等延后/停用（费城专有逻辑不在第一阶段运行路径，WP1 导入时显式清理残留）。

| 上游部分 | 第一阶段决定 | 依据 / 备注 |
|---|---|---|
| Python 3.13、uv 项目管理 | 保留 | pyproject requires-python >=3.12，3.13 实测可用（试跑报告 §1） |
| Polars 数据处理 | 保留 | polars 1.42.1 实测通过全量离线测试 |
| Parquet 原始快照与 manifest | 保留并改造 | `ingest/snapshots.py`、`ingest/manifests.py`：改保存示例城市来源原始证据、查询条件和指纹 |
| DuckDB 目录与查询 | 保留 | `catalog.py`：本地统一查询原始、清洗和估值结果 |
| `sources/`（arcgis/carto/mapillary） | 停用 | 费城/街景连接器，不进入示例城市运行路径 |
| `ingest/`（derived/diff） | 改造 | 官方、链家及手工文件的独立来源适配器 |
| `staging/` | 改造 | `parcels.py`（美国地块）停用；`geometry/tables/temporal/runner` 改造为示例城市字段、单位、实体与异常标记 |
| 成交有效性校验 | 改造 | `validation/sales.py`、`record_quality.py`：区分正常住宅成交、挂牌、车位与异常交易 |
| `validation/opa.py`、`screen_audit.py` | 删除运行依赖 | 美国评税筛查；连带 assesspy（AGPL）不进入运行路径 |
| `cli.py` 调用模式 | 保留并扩展 | Agent 正式调用入口，命令改名 `compsval`（§7） |
| 严格类型、Ruff、mypy、pytest | 保留 | 实测：pytest 191/192 通过、mypy 2.3.1、ruff 0.15.20（dev 依赖） |
| HTTP mock、默认离线测试 | 保留 | respx 0.23.1；`uv run pytest` 默认跳过 live/slow |
| 运行清单、版本一致性检查 | 保留 | `config.py`、`catalog.py`：防止混用不同数据与规则版本 |
| 单套房报告 | 改造 | `report.py`：输出买方估值、案例、区间、限制与复核 |
| `diagnostics/`（ratio_study 等） | 延后/停用 | 费城公平性诊断；assesspy 依赖集中于此处，示例城市阶段不启用 |
| `models/`（baseline/bayesian/condo/conformal/cqr/ensemble/scoring/risk） | 延至第二阶段 | LightGBM/CatBoost/贝叶斯/区间模型第一阶段不得成为估值依赖 |
| `features/` | 改造或延后 | `price_index.py` 等思想参考；示例城市特征集由 DATA-003/VAL1 系列定义 |
| `api.py` | 暂缓 | 第一阶段不启动 FastAPI 服务 |
| `web/`（Vue/MapLibre） | 暂缓 | 第一阶段不安装 Node 依赖 |

复用文件明细（**WP1 已按文件穷举登记**。下表为本骨架实际导入文件，来源均为固定 SHA `e163eba6` 的对应上游文件；"改造"指重命名包名/命令名/删除费城专属逻辑的骨架适配，示例城市数据契约与核心逻辑属 WP3+ 范围）：

| 导入文件（本包） | 上游来源 | 状态 |
|---|---|---|
| `__init__.py` | 上游 `philly_fair_measure/__init__.py` | 改造：包名重命名、版本号、骨架说明；原 __version__ 逻辑保留 |
| `config.py` | 上游 `philly_fair_measure/config.py` | 改造：删除费城 OPA/CARTO 常量，环境变量 `PHILLY_DATA_DIR`→`COMPSVAL_DATA_DIR`，快照注册表延后 WP3 |
| `catalog.py` | 上游 `philly_fair_measure/catalog.py` | 保留原样（行为未改）：仅改包名 import 与 docstring 来源说明 |
| `cli.py` | 上游 `philly_fair_measure/cli.py` | 改造：仅保留系统骨架命令 `version`/`catalog`/`sql`/`system check`；依赖 sources/models/diagnostics/opa/api/web/report/docs_sync 的命令全部停用 |
| `scalars.py` | 上游 `philly_fair_measure/scalars.py` | 保留原样 |
| `py.typed` | 上游 `philly_fair_measure/py.typed` | 保留原样 |
| `ingest/__init__.py` | 上游 `philly_fair_measure/ingest/__init__.py` | 保留（空包标记） |
| `ingest/manifests.py` | 上游 `philly_fair_measure/ingest/manifests.py` | 保留原样（schema 字段 `carto_type` 沿用上游拼写，保证旧 manifest 可读） |
| `ingest/diff.py` | 上游 `philly_fair_measure/ingest/diff.py` | 改造：删除费城 `SNAPSHOT_DIFF_SPECS` 与费城数据集 notes，保留通用 null-safe、keep-last 去重、markdown 逻辑 |
| `ingest/snapshots.py` | 上游 `philly_fair_measure/ingest/snapshots.py` | 改造：删除费城 Carto/ArcGIS 抓取客户端，保留通用不可变快照写入原语（.incomplete 兄弟目录 + rename） |

停用不导入（费城专属，WP1 清零已核验）：`sources/*`（arcgis/carto/mapillary）、`staging/parcels.py`、`validation/opa.py`、`validation/screen_audit.py`、`diagnostics/*`（延后）、`models/*`（延后）、`features/*`（延后）、`api.py`、`web_stats.py`、`docs_sync.py`、`web/`、`equity_context.py`、`vocab.py`、`report.py`（延后）。
- **docs_sync 处理决定（CXWP0-001）**：WP1 停用 `docs_sync.py` 并排除 `tests/test_docs_sync.py`（其 Windows 平台问题：GBK 编码 U+2717 + 路径分隔符，见试跑报告 §4）；不作为上游代码缺陷修复。

## 6. 采用决定（OSS-001-E）

| 候选 | 决定 | 理由 |
|---|---|---|
| Philly Fair Measure（e163eba6） | **复用（工程底座）** | README §9 已确认；G0 四项全部可判定（SHA/许可证、离线测试 191/192 可运行且失败原因已记录、未修改基线/回滚点存在、矩阵完成）；按 §5 保留骨架、重写示例城市核心 |
| mcp-imo（8094233e） | 借鉴，不进入运行路径 | 透明可比案例汇总思路（README §9） |
| open-comps（a2c42772） | 借鉴，不进入运行路径 | 证据和事件数据结构（README §9） |
| Cook County model-res-avm（57fe99b9） | 借鉴，不进入运行路径 | 成交校验与批量估值治理（README §9） |
| assesspy（上游依赖，AGPL-3.0） | **拒绝引入运行路径** | AGPL-3.0 强 copyleft；仅被 2 个模块 3 处函数内延迟导入（`models/metrics.py:103`、`diagnostics/ratio_study.py:51/227`）；示例城市比较法核心不需要费城比率研究；第一阶段不导入 assesspy 即消除 AGPL 传导风险 |

## 7. 本地包名与 CLI（OSS-001-E 登记，WP1 落地）

| 项 | 登记值 |
|---|---|
| Python 包名 | `compsval`（技术方案 §3.3） |
| 命令行名称 | `compsval`（技术方案 §3.3/§4） |
| 导入目录 | `03-估值引擎/`（WP1 从基线导入后包名重命名） |
| 升级规则 | 上游升级仅通过显式差异审计进入（技术方案 §3.3） |
