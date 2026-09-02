# governance-transition Specification

## Purpose

定义本项目自 2026-08-30 起的治理切换合同：初始化与采用状态分离、旧治理文件冻结与截止点效力、权威来源按职责分工（README=章程、specs=行为、changes=历史）、工作流节点执行者中立、验证与风险相称、archive 不等于正式基线、运行时版本可复现、采用可回退。旧治理文件冻结清单见 `openspec/adopt/freeze-manifest.md`，采用记录见 `openspec/adopt/adoption-record.md`。

## Requirements

### Requirement: Initialization and adoption SHALL be separate states

项目 SHALL 区分“已初始化 OpenSpec”与“已正式采用 OpenSpec”。存在 `openspec/` 或生成工具文件不得单独构成采用完成。

#### Scenario: Initialized but not adopted

- **GIVEN** 项目已经生成 OpenSpec 目录和工具文件
- **WHEN** 旧权威入口仍声明旧治理继续生效，或采用验收尚未完成
- **THEN** 项目状态 SHALL 保持 `initialized_not_adopted`
- **AND** 不得以 OpenSpec 初始化为由改变旧工作包合同

#### Scenario: Adoption completes

- **GIVEN** 采用任务全部完成并通过验证
- **WHEN** 用户明确确认采用
- **THEN** 项目 SHALL 记录唯一采用截止点和采用版本
- **AND** 截止点后的新需求 SHALL 统一使用 OpenSpec

### Requirement: In-flight legacy work SHALL not change contract midstream

采用发生时已经授权或执行中的旧工作包 SHALL 按原合同到达终态，不得在执行中更换状态机、审核要求、角色或证据合同。

#### Scenario: OCRNEXT remains active at transition planning time

- **GIVEN** `OCRNEXT-WP0` 已按旧治理授权且仍未到达终态
- **WHEN** 本采用 change 被规划或修订
- **THEN** 本 change SHALL NOT 修改 `OCRNEXT-WP0` 的合同、预算、任务状态、审核链或下一允许动作
- **AND** 101 的正式采用 SHALL 等待该工作包到达用户确认的终态

#### Scenario: Legacy work reaches a terminal state

- **GIVEN** 在途旧工作包已完成、拒绝、取消，或进入由用户明确处置的长期阻塞状态
- **WHEN** 建立采用截止点
- **THEN** 该工作包及其证据 SHALL 进入冻结清单
- **AND** 不得转换为伪造的 OpenSpec 历史 change

### Requirement: Authority SHALL be divided by responsibility

采用后的权威来源 SHALL 按职责分离，避免同一规则在多处重复维护。

#### Scenario: Reading stable project boundaries

- **WHEN** Agent 需要了解项目目标、业务范围、数据安全、保护基线或正式基线权限
- **THEN** Agent SHALL 读取 `README.md`

#### Scenario: Reading current required behavior

- **WHEN** Agent 需要判断系统当前应具备什么行为
- **THEN** Agent SHALL 读取 `openspec/specs/`

#### Scenario: Reading an in-flight or historical change

- **WHEN** Agent 需要了解某项变更的 why、what、design、tasks 或归档历史
- **THEN** Agent SHALL 读取 `openspec/changes/` 或其 archive

#### Scenario: Encountering frozen governance files

- **WHEN** Agent 读取 `project_rules.md`、`review_rules.md` 或旧任务台账
- **THEN** Agent SHALL 将其解释为截止点前旧合同证据
- **AND** SHALL NOT 将其中固定 TRAE/Codex 的规则套用于采用后的新 change

### Requirement: Planning SHALL NOT authorize implementation

Explore 和 propose SHALL 只形成讨论与规划；apply 必须由用户对目标 change 另行明确授权。

#### Scenario: Proposal is approved for review only

- **GIVEN** proposal、specs、design 和 tasks 已经生成
- **WHEN** 用户尚未发出明确 apply 指令
- **THEN** 任一 Agent SHALL NOT 修改业务代码、入口规则或项目状态

#### Scenario: Material plan change occurs during apply

- **GIVEN** change 已获 apply 授权
- **WHEN** 实施需要改变目标、非目标、业务边界、风险、外部访问、保护数据或验收要求
- **THEN** apply SHALL 暂停
- **AND** 规划产物 SHALL 先更新并重新获得用户确认

### Requirement: Workflow nodes SHALL be actor-neutral

Explore、propose、apply、verify 和 archive SHALL 不绑定固定工具、Agent、模型或品牌。

#### Scenario: User assigns node owners

- **WHEN** 一个 change 进入新的工作流节点
- **THEN** 用户 MAY 自由选择当前节点执行者
- **AND** 同一执行者 MAY 执行多个节点
- **AND** 不得仅因执行者不是 TRAE 或 Codex 判定流程无效

#### Scenario: User requests separation

- **WHEN** 用户因风险、成本或争议要求不同执行者处理 apply 与 verify
- **THEN** change SHALL 记录该选择及验证对象
- **AND** 该选择只适用于当前 change，除非用户明确设为项目规则

### Requirement: Verification SHALL be proportional to risk

项目 SHALL 安装并可调用 `verify` workflow。验证深度 SHALL 与变更风险和证据类型相匹配，不再以固定审核角色为前提。

#### Scenario: Low-risk bounded change

- **GIVEN** change 仅涉及低风险文档、孤立配置或无行为影响的维护
- **WHEN** apply 完成
- **THEN** 可使用轻量一致性检查和任务证据完成验证

#### Scenario: High-risk behavior or data change

- **GIVEN** change 涉及估值行为、数据合同、时间边界、外部调用、隐私、安全、正式数据或发布基线
- **WHEN** apply 完成
- **THEN** verify SHALL 检查完整性、正确性、连贯性和关键机器证据
- **AND** 用户 MAY 指定不同执行者复核固定提交或固定产物

### Requirement: Archive SHALL NOT imply formal acceptance

OpenSpec archive SHALL 只表示变更材料完成同步并进入历史，不得自动表示正式基线、业务有效性或用户接受。

#### Scenario: Technical change is ready to archive

- **GIVEN** tasks 已完成、delta specs 已同步、结构验证通过且必要 verify 已完成
- **WHEN** 执行 archive
- **THEN** change MAY 进入 archive
- **AND** archive 记录 SHALL 保留警告和验证结果

#### Scenario: Formal baseline is claimed

- **WHEN** 项目准备声明候选或正式基线
- **THEN** SHALL 另外满足 README 定义的业务证据和保护门槛
- **AND** 正式基线 SHALL 由用户明确确认

### Requirement: OpenSpec runtime SHALL be reproducible

项目 SHALL 锁定实际运行的 OpenSpec 版本，并确保选定工具从项目根目录可调用同一版本。

#### Scenario: Adoption precheck

- **WHEN** 开始 apply 本采用 change
- **THEN** SHALL 记录 `openspec --version`、可执行文件位置、Node 版本、profile 和 delivery
- **AND** Codex 与 TRAE 的实际调用路径 SHALL 至少各完成一次只读状态命令

#### Scenario: Version upgrade

- **WHEN** 需要升级 OpenSpec
- **THEN** SHALL 新建独立 change
- **AND** SHALL 检查生成文件变化、配置兼容性、现有 active changes 和回退方法
- **AND** SHALL NOT 自动追随 latest

### Requirement: Evidence rules SHALL match project type

所有持续项目资产 SHALL 使用同一 OpenSpec 生命周期，但 specs 和 tasks SHALL 定义与项目类型匹配的完成证据。

#### Scenario: Software or automation project

- **THEN** specs SHALL 覆盖行为、接口、异常、兼容性和测试

#### Scenario: Data or OCR project

- **THEN** specs SHALL 覆盖来源、截至时间、冻结输入、字段语义、缺失与冲突、样本排除、指标和可追溯证据

#### Scenario: Research or prediction project

- **THEN** specs SHALL 覆盖研究问题、证据准入、as-of date、未知项、反证、结论边界和验证方法

#### Scenario: Report or document project

- **THEN** specs SHALL 覆盖受众、主张、来源、截止日期、格式和验收条件

### Requirement: Adoption SHALL be recoverable

治理切换 SHALL 具备非破坏性回退路径。

#### Scenario: Adoption verification fails

- **GIVEN** 入口冲突、CLI 不可用、版本不一致、spec 基线不完整或回退测试失败
- **WHEN** 采用验收不能通过
- **THEN** 项目 SHALL 保持或返回 `initialized_not_adopted`
- **AND** SHALL 恢复采用前入口路由
- **AND** SHALL 保留 OpenSpec 规划产物、失败证据和冻结清单，不删除历史

### Requirement: Wider rollout SHALL follow canary acceptance

101 项目 SHALL 是现有项目迁移 canary。父级 Workspace 和未来新项目模板的切换 SHALL 作为后续独立 change。

#### Scenario: Canary has not been accepted

- **WHEN** 101 采用尚未获得用户确认
- **THEN** SHALL NOT 宣布 Workspace 已全面切换
- **AND** SHALL NOT 批量修改其他项目

#### Scenario: Canary is accepted

- **WHEN** 101 完成一次真实 change 的 propose、apply、verify、sync/archive 闭环并获得用户确认
- **THEN** MAY 在父级 Workspace 创建新项目模板切换 change
- **AND** 每个旧项目仍 SHALL 单独采用，不因模板切换自动迁移
