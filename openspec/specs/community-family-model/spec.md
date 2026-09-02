# 小区家族/子区实体模型与构建（community_family）

## Purpose

承载小区家族/子区两级实体模型与构建能力：家族表、子区表与 community 表家族字段的登记语义，判定表的冻结应用（新建实体、标准名变更、实体合并映射、别名补录输入），实体不物理删除与合并转发，以及追加式幂等构建（基线哈希、manifest、映射记录）与估值层可查询性。

## Requirements

### Requirement: 实体模型 SHALL 提供家族/子区两级且估值层可查询

小区实体 SHALL 引入家族（family）与子区（sub_area）两级：`community.parquet` SHALL 含 `family_id` 字段（无家族小区 `family_id` SHALL 为 `UNKNOWN`，不补造）；SHALL 新增家族登记表（`community_family.parquet`，含 `family_id`、`family_name`、`main_community_id`、`source_id`、`source_ref`）与子区登记表（`community_subarea.parquet`，含 `subarea_id`、`family_id`、`community_id`、`sub_area_name`、`source_id`、`source_ref`）。家族与主实体 SHALL 解耦：兄弟结构家族的 `main_community_id` SHALL 为 `UNKNOWN`。估值层 SHALL 能经单表查询任一小区的 `family_id`，并 SHALL 能将任一子区名解析到唯一 `(community_id, sub_area)`；无法唯一确定的子区名 SHALL 保持不参与自动解析并留痕，SHALL NOT 静默合并。

#### Scenario: 家族成员经 family_id 可查询

* **WHEN** 查询示例小区132、榕景四季(D区)承接记录与金汐花园示例小区039任一成员小区

* **THEN** 各行 `family_id` 分别指向示例小区132家族与金汐花园家族，家族表可列出全部成员与子区

#### Scenario: 兄弟结构家族无主实体

* **WHEN** 读取金汐花园家族行

* **THEN** 其 `main_community_id` 为 `UNKNOWN`，第一金汐、第二金汐、示例小区039、示例小区053四成员均仅以 `family_id` 挂靠（示例小区053系 2026-09-01 用户追加裁决并入）

#### Scenario: 无家族小区不补造

* **WHEN** 读取示例小区047与任意未列入判定表的小区行

* **THEN** 其 `family_id` 为 `UNKNOWN`，示例小区047 SHALL NOT 因名称相似进入金汐花园家族

#### Scenario: 子区名唯一解析

* **WHEN** 以「示例小区132水岸榕城」「水岸榕城」「示例小区130B区」查询子区解析

* **THEN** 分别唯一解析到 `(C-XXXX0069, 水岸榕城)` 与 `(C-XXXX0063, B区)`，无多义命中

### Requirement: 实体 SHALL NOT 物理删除且合并 SHALL 经状态与映射保留历史

被降级为子区的既有实体 SHALL 保留其 `community.parquet` 行并标记实体状态为 `merged`，行内 SHALL 含合并映射（承接 `community_id` 与 `sub_area`）；指向已合并实体的引用（含别名命中）SHALL 经合并映射转发到承接 `(community_id, sub_area)`，合并实体行本体与历史引用 SHALL NOT 被改写或删除。本 change 适用对象 SHALL 为：拾光里 C-XXXX0051、示例小区132榕岸华庭(E区) C-XXXX0184、示例小区132榕景四季(D区) C-XXXX0128、示例小区244 C-XXXX0170。

#### Scenario: 合并实体行保留且转发可达

* **WHEN** 读取拾光里 C-XXXX0051 行并以别名「拾光里」做小区解析

* **THEN** 该行仍存在且实体状态为 `merged`，解析经合并映射得到 `(C-XXXX0033, 拾光里)`（示例小区136家族），原行内容未被改写

#### Scenario: 合并实体不产生独立统计对象

* **WHEN** 普查或家族聚合读取实体框架

* **THEN** 状态为 `merged` 的实体 SHALL NOT 作为独立统计对象重复计数，其成交归因计入承接子区与家族

### Requirement: 判定表应用 SHALL 冻结可对拍且实体变更留痕

proposal 判定表 SHALL 以文件形式冻结为构建输入并登记进 manifest（含 SHA256）；实体构建 SHALL 严格按判定表执行：新建实体示例小区012、示例小区011（新号段 ID，来源与佐证留痕）；标准名变更示例小区164龙禧 C-XXXX0181 → 示例小区164、华标品峰 C-XXXX0125 → 示例小区089（旧名 SHALL 经别名或子区登记保持可解析）；13 个家族与全部子区按判定表登记；子区中现为独立实体的 SHALL 按合并规则降级。构建后 SHALL 产出逐项对拍记录（判定表行 → 构建动作 → 产物行），行数与内容 SHALL 可核对。

#### Scenario: 标准名变更后旧名可解析

* **WHEN** 以「示例小区164龙禧」「华标品峰」「示例小区164」「示例小区089」分别解析

* **THEN** 前两者经子区登记/别名分别解析到示例小区164与示例小区089承接实体，后两者为标准名直接命中

#### Scenario: 新建实体留痕

* **WHEN** 读取示例小区012、示例小区011实体行

* **THEN** 二者 ID 取自新号段且不与既有 ID 冲突，`source_ref` 回指判定表与构建 manifest，`family_id` 指向金汐花园家族

#### Scenario: 判定表对拍

* **WHEN** 以冻结判定表逐行核对构建产物

* **THEN** 13 个家族、全部子区、2 处标准名变更、5 处合并映射与 1 条新增别名逐行对应，无多余、无缺失

### Requirement: 构建 SHALL 追加式幂等且受保护资产不变

实体构建 SHALL 为追加式确定性重建：构建前 SHALL 记录 `data/entities/`、`data/staged/`、`05-估值报告/` 与 `01-数据/census/community-census-v1-2/`、`community-census-v1-2-r1/` 的基线 SHA256；构建产物（community.parquet 变更、两张新表、别名追加）SHALL 各带 DerivedManifest（输入指纹、行数、构建时间、代码版本）与映射记录；同输入重跑 SHALL 产出逐字节一致；`data/staged/`、`05-估值报告/` 与两个 v1.2 普查目录 SHALL 逐字节不变；UNKNOWN SHALL 保持为合法值，SHALL NOT 补造。

#### Scenario: 重跑幂等

* **WHEN** 以同一冻结判定表输入连续执行两次实体构建

* **THEN** `community.parquet`、`community_family.parquet`、`community_subarea.parquet`、`community_alias.parquet` 逐字节一致，溯源不重复追加

#### Scenario: 受保护资产不变

* **WHEN** 构建完成后核对受保护目录哈希

* **THEN** staged 冻结表、`05-估值报告/`、`community-census-v1-2/` 与 `community-census-v1-2-r1/` 哈希与基线一致，仅 `data/entities/` 内目标表按 manifest 变更
