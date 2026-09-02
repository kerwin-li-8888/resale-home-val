# open-source-release Specification

## Purpose

定义开源发布副本的构建、脱敏、合规与发布门禁要求，使"哪些内容可以公开"成为可检验的规则：方法、引擎与治理可公开，平台数据、真实标的与个人决策不公开；原项目在发布全程保持不可变。

## Requirements

### Requirement: 发布副本隔离

发布副本构建 SHALL 通过可重复执行的复制清单生成本项目之外的独立目录，且 SHALL NOT 修改、移动或删除原项目内的任何文件。

#### Scenario: 重复构建一致

- **WHEN** 以同一复制清单连续执行两次副本构建
- **THEN** 两次产出的发布目录文件集合一致，且原项目目录的内容与字节均未变化

#### Scenario: 构建失败回滚

- **WHEN** 副本内任一构建步骤失败
- **THEN** 可删除副本目录后重新执行复制清单完整重建，原项目不受影响

### Requirement: 数据与隐私内容排除

发布副本 SHALL NOT 包含以下任何内容：平台原始快照（`01-数据/raw/` 全部）、census 与 ScopePolicy 等派生数据、真实标的估值条目（`RUN-SUBJ-USER-*` 与 `RUN-SUBJ-SHADOW-REAL-*` 等）、决策支持个人文档（`00-项目总控/决策支持-*`）、`openspec/changes/` 执行历史、开发临时文件（`.trae/`、`_probe_*`、`__pycache__`、`node_modules` 等）。

#### Scenario: 排除清单核查

- **WHEN** 对定稿副本执行数据与隐私排除核查
- **THEN** 上述每一类路径在副本内均不存在，核查输出逐项 PASS 记录

### Requirement: 敏感信息扫描门禁

发布前 SHALL 对定稿副本全量执行敏感信息正则扫描（手机号、身份证号、链家房源号 `LJ\d+`、`USER-` 标识、API 密钥模式如 `sk-`）；扫描存在命中时 MUST 禁止推送并要求人工处置后重扫。

#### Scenario: 扫描通过方可推送

- **WHEN** 敏感信息扫描输出零命中报告
- **THEN** 该报告作为发布证据存档，推送步骤被允许执行

#### Scenario: 扫描命中即阻断

- **WHEN** 扫描发现任一敏感信息命中
- **THEN** 推送步骤被阻断，命中项清单被输出，处置并重扫通过前不得继续

### Requirement: 许可证合规声明

发布副本 SHALL 包含：用户版权的 MIT `LICENSE`；`NOTICE` 文件声明衍生来源（Philly Fair Measure，固定 SHA `e163eba6`，Copyright (c) 2026 Nick Hand，MIT License）及副本内代码存在修改；上游 LICENSE 原文存档文件。

#### Scenario: 合规三件套齐备

- **WHEN** 对定稿副本执行许可证合规检查
- **THEN** `LICENSE`、`NOTICE`（含上游归属与修改说明）、上游 LICENSE 存档三项齐备且归属信息正确

### Requirement: 地域与标的泛化

发布副本的可见文本 SHALL NOT 包含"示例城市·目标区西部板块"级地域表述与真实小区标识（如 `C-\d+` 格式小区 ID、真实小区名称），MUST 以泛化占位表述（如"示例城市·目标区西部板块"、`C-XXXXXXXX`）替代；真实地域范围仅存在于未公开的数据层。

#### Scenario: 泛化核查通过

- **WHEN** 对副本内全部可见文本执行地域与真实小区标识检查
- **THEN** 无地域级真实表述与真实小区标识命中，或命中项均已完成占位替换

### Requirement: 副本可复现与测试通过

发布副本 SHALL 在克隆后可通过包管理器安装依赖并全量通过 pytest；SHALL 包含基于虚构小区的合成样例数据集，端到端演示（标准化契约数据层→估值链→Markdown 报告）可重复运行且结果确定；演示自清洗后 mart 层（`valid_sale`）进入估值链，原始快照→清洗阶段依赖真实平台文件格式，由 ADAPTATION 指南引导使用者自建。

#### Scenario: 全量测试通过

- **WHEN** 在干净环境中执行副本的全量 pytest
- **THEN** 全部测试通过，无失败、无错误

#### Scenario: 合成样例端到端运行

- **WHEN** 按副本文档指引运行合成样例数据集的端到端演示
- **THEN** 演示完成且输出估值报告，重复运行结果一致，全程不访问任何外部数据源

### Requirement: 发布状态边界

自动化发布任务 SHALL 止于 GitHub 私有仓推送完成；仓库转公开 MUST 由用户在审查后手动确认，SHALL NOT 被自动化任务执行。

#### Scenario: 自动化止于私有仓

- **WHEN** 推送任务执行完成
- **THEN** GitHub 上存在内容一致的私有仓，且仓库可见性仍为 Private

#### Scenario: 转公开由用户确认

- **WHEN** 用户审查私有仓内容后确认转公开
- **THEN** 仓库可见性变更为 Public；若用户未确认，仓库保持 Private
