# 小区别名登记表（community\_alias）

## Purpose

承载小区别名登记表（`community_alias`）的行为边界：别名登记不静默合并，冲突状态四分（一致/冲突/待定/排除，`排除` 为终态）决定其是否参与自动小区映射；census 来源别名批次（DATA-005 首批补录）按复核裁决落表，登记过程可复现、可溯源、幂等，且不触碰受保护资产。

## Requirements

### Requirement: 别名行 SHALL 可溯源且冲突状态三分

别名表每一行 SHALL 含来源（`source_id`）、可溯源出处（`source_ref`）与冲突状态（`conflict_status` ∈ 一致/冲突/待定/排除，其中 `排除` 为终态：道路级命名等无法唯一解析的源名经用户裁决后永久退出自动映射候选，仅保留登记与溯源）；census 批次新增行的 `source_ref` SHALL 能回指 community-census-v1-2 归并候选清单与本 change 冻结的复核裁决表（含行级对应）。`alias_id` SHALL 全表唯一且不与既有行冲突。

#### Scenario: 新增别名行回指裁决依据

* **WHEN** 审计任一 `AC-` 前缀新增别名行

* **THEN** 其 `source_ref` 可定位到 census 归并候选清单与复核裁决表的对应行，且 `source_id` 标注别名观察来源

#### Scenario: alias\_id 唯一

* **WHEN** 构建完成后读取别名表全量行

* **THEN** `alias_id` 无重复，且新增行不占用既有 `A-<n>-<m>` 编号

#### Scenario: 排除行为终态且可溯源

* **WHEN** 审计任一 `conflict_status=排除` 的行

* **THEN** 其 `source_ref` 保留原批次溯源并追加本轮裁决溯源（裁决日期与口径），且该状态不由后续重建自动改回待定

### Requirement: 自动小区映射 SHALL 仅取一致别名，非一致 SHALL 不参与合并

小区自动映射 SHALL 仅使用 `conflict_status=一致` 的别名；待定、冲突与排除别名 SHALL 保持登记留痕但 SHALL NOT 参与自动映射（blocked 语义，不静默合并）。本语义对既有行与新增行一致适用，匹配消费代码 SHALL NOT 因本批次落表而修改。

#### Scenario: 一致别名参与自动映射

* **WHEN** 以「示例小区130A区」查询小区自动映射

* **THEN** 命中 `conflict_status=一致` 的别名并映射到示例小区130对应 `community_id`

#### Scenario: 待定别名不自动合并

* **WHEN** 以任一待定别名（如「示例小区132榕岸」）查询小区自动映射

* **THEN** 该名称处于 blocked，不产生自动映射结果，仅可经人工裁决后变更状态

#### Scenario: 排除别名不自动合并

* **WHEN** 以任一排除别名（如「工业大道」「泰沙路」）查询小区自动映射

* **THEN** 该名称处于 blocked，不产生自动映射结果，且与待定不同、不再进入待复核队列

### Requirement: 落表范围 SHALL 与冻结裁决一致且拒绝项不落表

本批次落表 SHALL 严格等于冻结裁决表：57 对按 `一致` 写入、16 对按 `待定` 写入；4 个跨区同名假阳性（示例小区154(邻甲区)、示例小区232(邻丙)、示例小区181(邻丙区)、示例小区139(邻乙区)）SHALL NOT 落表，其拒绝理由 SHALL 在复核记录中留痕。裁决表 SHALL 以文件形式冻结（含生成规则与 SHA256）并作为构建输入登记进 manifest。**追加裁决（2026-08-31 用户四项口径）SHALL 按冻结 overrides 应用：5 个源名改一致（其中 3 个改指第二小区目标：示例小区245 C-XXXX0049、拾光里 C-XXXX0051、示例小区244 C-XXXX0170，另榕岸华庭(E区) C-XXXX0184、示例小区186 C-XXXX0145）、1 行冗余待定行（示例小区132榕岸→C-XXXX0069）移除、泰沙路/工业大道/工业大道南 维持待定。最终裁决（2026-08-31 用户三批口径，data005-alias-final-resolution）SHALL 按冻结名录 overrides 应用至既有** **`A-`** **名录批次行：示例小区202→C-XXXX0052、春晖花苑(目标区)→C-XXXX0027、示例小区242→C-XXXX0188 改一致，示例小区008→C-XXXX0151、示例小区039→C-XXXX0067、示例小区053→C-XXXX0097、示例小区132榕岸华庭(E区)→C-XXXX0184、示例小区132榕景四季(D区)→C-XXXX0128 改指同名标准小区；泰沙路（2 行）、工业大道南（3 行）、工业大道（5 行，staged 实为邻丙区房源）维持待定；应用后全表 86 行（一致 72 / 待定 10 / 冲突 4）。** **v1.3 批次（community-family-subarea-census-v1-3）SHALL 追加 1 行：`华标品峰`→C-XXXX0125（该实体标准名变更为「示例小区089」后的旧名承接），`conflict_status=一致`，`source_ref`** **回指该 change 冻结判定表与构建 manifest；追加后全表 87 行（一致 73 / 待定 10 / 冲突 4）。终态裁决（ext-sale-ingest-scope-v1-2）SHALL 将道路级命名待定行 AC-63\~70、AC-72/73 十行置为** **`排除`** **终态；应用后全表 87 行（一致 73 / 待定 0 / 冲突 4 / 排除 10）。**

#### Scenario: 行数与裁决对拍

* **WHEN** 构建完成后核对别名表

* **THEN** 全表 87 行（一致 73 / 待定 0 / 冲突 4 / 排除 10），与冻结裁决、两轮 overrides、v1.3 批次及终态裁决应用后的预期逐行一致，无多余行、无缺失行

#### Scenario: 跨区同名不入表

* **WHEN** 在别名表中检索「示例小区154(邻甲区)」等 4 个跨区同名写法

* **THEN** 均无对应行，且复核记录含其拒绝理由

#### Scenario: 名录批次裁决后终态

* **WHEN** 以「示例小区202」「春晖花苑(目标区)」「示例小区242」「示例小区008」「示例小区039」「示例小区053」「示例小区132榕岸华庭(E区)」「示例小区132榕景四季(D区)」核对别名表

* **THEN** 8 行 `conflict_status` 均为一致且目标为上列 `community_id`，`source_ref` 回指本轮裁决记录

#### Scenario: v1.3 批次行可溯源

* **WHEN** 以「华标品峰」核对别名表

* **THEN** 恰有 1 行 `conflict_status=一致` 且目标为 C-XXXX0125，`source_ref` 回指 community-family-subarea-census-v1-3 冻结判定表与构建 manifest

### Requirement: 待定别名 SHALL 可经用户裁决变更并保持幂等溯源

待定别名经用户裁决 SHALL 按冻结 overrides（改指目标/提升状态/移除）变更：每条 override SHALL 记录裁决日期、裁决口径与证据摘要（价格分布/坐标簇/时间范围/领域确认），`source_ref` SHALL 保留原批次溯源并追加裁决溯源；被取代的冗余行 SHALL 移除且在执行留痕登记。**overrides 适用范围 SHALL 覆盖 census 批次（`AC-`）与名录批次（`A-`）两类待定行；对既有名录行的裁决 SHALL 在重建重读已裁决行时跳过重复追加溯源（幂等护栏）；retarget 目标 SHALL 通过外键校验。** 应用 overrides 后构建 SHALL 保持幂等（同输入重跑逐字节一致）；自动映射语义（仅一致参与）SHALL NOT 改变。

#### Scenario: 裁决提升的别名参与自动映射

* **WHEN** 以「示例小区132榕岸」「示例小区136拾光里」「示例小区242」「春晖花苑(目标区)」查询小区自动映射

* **THEN** 分别命中 C-XXXX0184、C-XXXX0051、C-XXXX0188、C-XXXX0027（HIT\_ALIAS），原待定 blocked 状态解除

#### Scenario: 维持待定的别名仍被 blocked

* **WHEN** 以「泰沙路」「工业大道」「工业大道南」查询小区自动映射

* **THEN** 三者仍为 BLOCKED，不产生自动映射结果

#### Scenario: 裁决应用后重跑幂等

* **WHEN** 以同一冻结 overrides 连续执行两次构建

* **THEN** 两次产出的 `community_alias.parquet` 逐字节一致，全表 86 行，裁决溯源不重复追加

### Requirement: 重建 SHALL 幂等可复现且既有行保留

别名表构建 SHALL 为追加式确定性重建：既有 14 行内容 SHALL 逐字节保留；同输入重跑 SHALL 产出逐字节一致的表文件；构建 SHALL 产出 DerivedManifest（输入指纹含冻结裁决表 SHA256、既有表 SHA256、census 产物引用；行数；构建时间；代码版本）。`community.parquet`、`scope_policy_v1.1.parquet`、staged 数据、原始快照与 `05-估值报告/` SHALL 逐字节不变。

#### Scenario: 重跑幂等

* **WHEN** 以同一冻结裁决输入连续执行两次构建

* **THEN** 两次产出的 `community_alias.parquet` 逐字节一致，既有 14 行内容不变

#### Scenario: 受保护资产不变

* **WHEN** 构建完成后核对受保护目录哈希

* **THEN** 除 `community_alias.parquet` 与 `community_alias.manifest.json` 外，`data/entities/` 其余文件及 staged、原始快照、`05-估值报告/` 哈希与基线一致

### Requirement: 登记行 SHALL 通过数据字典合同校验

新增别名行 SHALL 通过 `CommunityAlias` 合同模型校验（字段、枚举、非空）；`conflict_status` SHALL 仅取枚举合法值；本 change SHALL 提供行为测试覆盖：一致/blocked 语义、幂等、既有行保留、示例小区130等关键映射与裁决表对拍，且既有测试套件 SHALL 无回归。

#### Scenario: 合同校验与回归

* **WHEN** 执行新增测试与既有测试套件

* **THEN** 新增行全部通过 `CommunityAlias` 校验，关键映射抽查与冻结裁决一致，既有测试无回归

### Requirement: 别名指向已合并实体 SHALL 经合并映射转发解析

当别名行的目标 `community_id` 所指实体状态为 `merged` 时，小区解析 SHALL 经该实体的合并映射转发到承接 `(community_id, sub_area)`；别名行本体（`conflict_status`、目标 `community_id`、`source_ref`）SHALL NOT 被改写，既有终态对拍（86 行：一致 72 / 待定 10 / 冲突 4）SHALL 不受影响。转发解析 SHALL 在普查匹配与实体解析中一致生效。

#### Scenario: 拾光里别名经转发归入示例小区136家族

* **WHEN** 以别名「拾光里」做小区解析（其目标 C-XXXX0051 已标记 merged）

* **THEN** 解析结果为承接对 `(C-XXXX0033, 拾光里)`（示例小区136家族），该行命中计数计入示例小区136家族与拾光里子区

#### Scenario: 别名行本体不变

* **WHEN** 实体合并构建完成后核对「拾光里」「示例小区244」「示例小区132榕岸华庭(E区)」「示例小区132榕景四季(D区)」「示例小区245」别名行

* **THEN** 五行 `conflict_status` 与目标 `community_id` 与构建前逐字段一致，转发仅发生在解析层

### Requirement: 道路级命名待定行 SHALL 经用户裁决置为排除终态并清空待定区

名录 §3 冲突清单 #10 对应的道路级命名待定行（AC-63\~70 工业大道/工业大道南、AC-72/73 泰沙路）SHALL 按用户裁决一次性置为 `排除` 终态：行本体保留（不物理删除）、`source_ref` 追加裁决溯源（日期、口径=冲突清单 #10「排除或单列」、证据摘要）、SHALL NOT 进入 `LIANJIA_COMMUNITY_REGISTRY` 或任何自动映射；应用后别名表 SHALL 无残留待定行。裁决 SHALL 通过冻结 overrides 应用并保持幂等（同输入重跑逐字节一致、溯源不重复追加）；重建 SHALL NOT 改写 `community.parquet` 与 `scope_policy` 各版本文件。

#### Scenario: 待定区清零且行数不变

* **WHEN** 应用裁决 overrides 后核对别名表

* **THEN** 全表行数与裁决前一致（87 行），状态分布变为 一致 73 / 待定 0 / 冲突 4 / 排除 10，AC-63\~70 与 AC-72/73 十行均为排除

#### Scenario: 道路级名称不产生映射

* **WHEN** 以「工业大道」「工业大道南」「泰沙路」查询小区自动映射

* **THEN** 三者均为 blocked，不映射到任何 `community_id`，也不进入链家成交社区注册表

#### Scenario: 裁决重跑幂等且受保护资产不变

* **WHEN** 以同一冻结 overrides 连续执行两次重建

* **THEN** `community_alias.parquet` 逐字节一致、裁决溯源不重复追加，`community.parquet`、`scope_policy_v1.0/v1.1.parquet` 与 staged、原始快照、`05-估值报告/` 哈希与基线一致

