# 采用记录（adoption record）— resale-home-val

> 沿革：治理机制自 v1（compsval 测试版，2026-08-30 用户确认采用）延续至本 v2 正式版；
> 采用 change：`adopt-openspec-governance`（schema: spec-driven，v1 仓库内执行）
> 当前状态：**`adopted`**（v1 采用时点用户确认；v2 延续同一机制，不重复采用）

## 1. 采用结果摘要

| 项 | 值 |
|---|---|
| 采用版本 | OpenSpec CLI **1.11.0**（v1 采用时点精确版本锁：`package.json` + `package-lock.json`） |
| 工作流集合 | custom profile 七项：propose、explore、apply、update、sync、archive、verify（delivery: both） |
| 行为基线 | `openspec/specs/`：本仓库全部能力规格（比较法底座 13 规格＋开源发布门禁＋量化模型层 10 规格） |
| 入口路由 | `README.md`、`openspec/config.yaml` 为 OpenSpec 变更权威入口，改造到新城市先读 [ADAPTATION.md](../../ADAPTATION.md) |
| 采用截止点 | v1 采用时以「本记录 + freeze-manifest 哈希清单 + OpenSpec 版本锁 + git 固化提交」组合定义 |

## 2. v2 延续说明

- v1 采用时的验证证据、回退演练与采用前备份保存在上游底座仓库的
  `openspec/adopt/evidence/`，本仓库不随发布重复携带；
- v2 起本仓库规格面＝底座规格＋量化模型层规格（见 `openspec/specs/`），
  全部行为变更走 OpenSpec change 流程；
- 治理规则与操作指引见 [`../config.yaml`](../config.yaml)，工作流模板见
  [`../schemas/gov-spec-driven/`](../schemas/gov-spec-driven/)。

## 3. 正式基线声明（重要）

本采用记录与 `openspec archive` 均不构成正式基线。估值系统的候选/正式基线
确认权在仓库主人，采用 OpenSpec 改变的只是变更管理流程，不改变任何业务验收标准。
