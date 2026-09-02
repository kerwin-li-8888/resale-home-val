# 采用记录（adoption record）— 101-房产估值系统

> 采用 change：`adopt-openspec-governance`（schema: spec-driven）
> 记录日期：2026-08-30
> 当前状态：**`adopted`**（用户于 2026-08-30 确认采用，并授权固化提交以固化截止点）
> 采用截止点：**2026-08-30 用户确认时点**，以「本记录 + freeze-manifest 227 份哈希 + OpenSpec 1.11.0 版本锁 + git 固化提交」组合定义

## 1. 采用结果摘要

| 项 | 值 |
|---|---|
| 采用版本 | OpenSpec CLI **1.11.0**（项目锁：`package.json` + `package-lock.json` 精确锁 1.11.0；`npm ci` 还原后 `npx --no-install openspec --version` = 1.11.0） |
| 运行时 | Node v24.16.0 / npm 11.13.0；全局入口 `%APPDATA%\npm\openspec.ps1`（仅交互便利） |
| 工作流集合 | custom profile 七项：propose、explore、apply、update、sync、archive、verify（delivery: both） |
| 在途旧合同 | `OCRNEXT-WP0` 已于 2026-08-30 用户取消（`cancelled_by_user`，台账 §12），无其他在途 |
| 冻结清单 | `openspec/adopt/freeze-manifest.md`（227 份文件 SHA-256：2 规则 + 14 台账 + 211 审核记录） |
| 行为基线 | `openspec/specs/`：`ocr-field-evaluation`、`valuation-comparable-core`、`data-evidence-lineage`（候选/未知/不支持状态显式标注） |
| 入口路由 | `README.md` §1、`AGENTS.md`、`CLAUDE.md`、`openspec/config.yaml` 已切换为 OpenSpec 唯一变更权威，四文件一致 |
| 采用截止点 | 用户在 task 7.2 明确确认后，以 freeze-manifest 哈希 + adoption-record 版本组合为唯一截止点 |

## 2. 验证证据（task 6.1–6.6）

全部原始输出存于 `openspec/adopt/evidence/`：

- `runtime-precheck.txt` — task 1.1/1.2 版本与 profile 记录；task 1.4 双端只读状态命令（TRAE 本会话实测；Codex 侧由用户于 2026-08-30 报告通过，原始文本未留存，见 §4 已知限制）；task 2.4 git 基线（HEAD `4b12b32`）与预改版备份
- `adoption-verification.txt` — validate（4 passed 0 failed）/status/context/schemas/list/doctor 原始输出
- `rollback-drill.txt` — 非破坏性回退演练全记录（旧版入口恢复 → 验证 → 采用版还原 → 复验 4/4 通过）
- `pre-adoption-backup/` — 四入口文件采用前副本（回退点）
- 规格结构校验：3 个基线 spec + change 全部 strict 通过
- `project_rules.md` / `review_rules.md` 相对 git HEAD 零未提交改动（task 2.5 实测）

## 3. 规格符合性核对（task 6.2）

| spec 要求（governance-transition） | 核对结果 |
|---|---|
| 初始化与采用分离 | 实施完成但**未宣布 adopted**，状态停在 `awaiting_user_acceptance` ✓ |
| 在途旧包不改约 | WP0 按旧合同取消，合同/证据未因采用改写，已入冻结清单 ✓ |
| 权威按职责分离 | README/specs/changes/冻结清单 四处路由一致，无重复维护 ✓ |
| 规划不授权实施 | 本次 apply 由用户 2026-08-30 明确指令「开始 apply」授权 ✓ |
| 节点执行者中立 | propose 技能边界条款 + 四入口文件均为中立表述，无固定工具绑定 ✓ |
| 验证与风险相称 | verify 工作流可调用（`/opsx-verify`）；本次治理变更属文档级，按轻量一致性检查执行 ✓ |
| archive ≠ 正式基线 | 四入口文件一致声明；正式基线仍由用户按 README 门槛确认 ✓ |
| 运行时可复现 | 版本锁 + `npm ci` 还原验证 ✓ |
| 证据匹配项目类型 | config.yaml 写入五类项目证据规则 ✓ |
| 采用可恢复 | 回退演练通过，备份与失败证据保留 ✓ |
| canary 范围 | 未修改父级 Workspace 与其他项目 ✓ |

## 4. 已知限制

1. **profile/workflows 为机器全局配置**（OpenSpec 1.11.0 仅支持 global scope）：本机其他 OpenSpec 项目的可用工作流集合与本项目相同；当前无其他项目使用，实际影响为零，跨机恢复时需按 §1 工作流集合重放 `openspec config` 设置。
2. **verify 工作流为 experimental**：OpenSpec 升级时其行为可能变化（见 §5 升级策略）。
3. **Codex 侧 task 1.4 原始输出未留存**：以用户 2026-08-30 的通过报告为准（双记录中的「编排侧记录」，与本项目 review_rules §16 传统一致）。
4. **OCRNEXT-D 阶段数据为「已执行未验收」口径**：引用其数据必须带状态标注（已写入 `openspec/specs/ocr-field-evaluation/spec.md`）。
5. **版本锁文件的跨机效力依赖 git 提交**：`package.json`/`package-lock.json` 与全部采用变更当前为未提交状态；建议在用户确认 adopted 后一并提交，形成不可变截止点。
6. **`.trae/tmp/` 历史脚本**：属旧工作流遗留，未纳入冻结清单（非治理正文），保留原样。

## 5. 升级策略（task 5.3）

任何 OpenSpec 版本升级 SHALL 另建独立 OpenSpec change，并在该 change 内完成：

- `npm ci` 后对照新旧版本 `openspec update --force` 生成的 skills/commands 差异；
- `config.yaml` 与 schema 兼容性检查（`openspec schemas`）；
- 全部 active changes 的 `validate --strict` 回归；
- verify / sync / archive 行为抽查；
- 回退方法验证（锁文件回退 + `npm ci`）。
- SHALL NOT 自动追随 `latest`（本项目所有工具调用一律经锁文件或全局精确版本）。

## 6. 回退方法（task 6.6，演练已通过）

1. 入口路由回退：用 `openspec/adopt/evidence/pre-adoption-backup/` 的四文件覆盖现文件（演练实录见 `evidence/rollback-drill.txt`）；
2. 工具回退：`package-lock.json` 回退版本后 `npm ci`；profile 可 `openspec config set profile core` 恢复六件套；
3. OpenSpec 目录与冻结清单、失败证据一律保留，不删除历史；
4. 回退后项目状态回到 `initialized_not_adopted`，旧合同文件以冻结清单哈希自证未被改写。

## 7. 正式基线声明（重要）

本采用记录与 `openspec archive` 均不构成正式基线。估值系统的候选/正式基线确认权在用户，门槛以 `README.md`（第一阶段通过条件）为准；采用 OpenSpec 改变的只是变更管理流程，不改变任何业务验收标准。
