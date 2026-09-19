# phase2-block-taxonomy-map Specification

## Purpose

现行商圈口径映射与漂移监控：把链家现行页面商圈词翻译为冻结板块口径（小区级映射），使请求方照页面填词也能被正确采信；产出漂移监控证据与 V2 重标待办登记，不重标历史数据、不执行任何网络抓取。

## Requirements

### Requirement: 现行商圈登记与来源约束

818 个冻结池小区的现行商圈 SHALL 以用户提供清单为唯一采集来源（零触网）：登记清单原件 SHA-256、提供时间与逐小区现行商圈值；清单行与冻结池的对齐 SHALL 以 `community_source_id` 优先、小区名唯一匹配为兜底，对不上的行 SHALL 归缺失类登记并上报，SHALL NOT 猜测；清单未覆盖的小区 SHALL 归缺失类；本能力 SHALL NOT 执行网络抓取。

#### Scenario: 来源两态可追溯

- **WHEN** 查阅映射表登记文件
- **THEN** 每小区现行商圈带来源（provided/missing）、清单原件哈希与接收时间，两类计数齐备

#### Scenario: 覆盖不足停线

- **WHEN** 清单覆盖 <90% 小区或清单未到位
- **THEN** 登记缺失清单并上报等待用户补充，不抓取、不猜测、不带部分数据静默继续

### Requirement: 映射对账三类统计

映射表 SHALL 与冻结池历史板块集合逐小区对账并分类统计：一致（现行 ∈ 历史集合）、漂移（现行 ∉ 历史集合）、缺失（清单未覆盖）；漂移清单 SHALL 含示例小区130 实例核验与按 F01 权重的偏移量级预估；漂移占比 >50% SHALL 上报。

#### Scenario: 三类统计与漂移清单

- **WHEN** 对账完成
- **THEN** 一致/漂移/缺失三类计数与漂移清单（小区、现行值、历史集合、预估偏移）落盘，示例小区130 案例（滨江中 vs 江南大道中，-7.0%）在列

### Requirement: 翻译先行于校验

请求层 SHALL 在口径校验之前执行翻译：请求提供的板块 ∉ 该小区历史集合、且映射表登记该小区 category=drifted 且现行值＝请求值时，SHALL 翻译为**该小区既有冻结推导板块 `comm2block`（与未提供板块时的推导口径同源，唯一定义、无第二种读法）**，`block_source` SHALL 标注新增枚举值 `translated`（原提供值以 `provided_block_name` 留痕并在 limits 披露），再走既有校验/采信流程；映射未覆盖或 category∈{consistent, missing} SHALL 维持既有 mismatch 行为；翻译 SHALL NOT 改变主报价语义与六类边界行为（回归验证留证）。

#### Scenario: 页面词自动翻译

- **WHEN** 请求提供"江南大道中"而示例小区130A区映射表有 现行=江南大道中（drifted）→ 冻结=滨江中 的翻译
- **THEN** 按"滨江中"走校验并采信，`block_source=translated`，回显保留原提供值，报价与推导口径同源

#### Scenario: 未覆盖不翻译

- **WHEN** 映射表缺失该小区、该小区 category∈{consistent, missing}、或请求值非登记现行值
- **THEN** 维持 fix change 的 `block_mismatch` 行为，不猜测翻译

### Requirement: 漂移监控与 V2 重标待办登记

漂移监控报告 SHALL 登记：漂移小区数/占比、清单、对 `block_mismatch` 率的预期改善、清单接收时点与复采建议（复采＝用户再提供清单）；**V2 待办 SHALL 双落点登记**：漂移报告一节＋`openspec/backlog.md` 追加一行（仅追加，不改既有行）——历史数据按现行口径统一重标＝V2 重训时处理（新数据自带新口径），本能力与请求层均不重标历史。

#### Scenario: V2 待办可查

- **WHEN** 查阅漂移报告与 backlog 正本
- **THEN** V2 重标待办的依据、边界（何时做、谁做、为何现在不做）与登记时点在报告齐备，backlog 新增一行且既有行逐字不变
