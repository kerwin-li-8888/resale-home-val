# 链家 ext 成交入 marts 合并（lianjia_ext）

## Purpose

把已确认的链家 ext 精确同名小区成交接入 marts 正式成交池：注册表补录逐条可溯源、合并输入统一走冻结 run、与既有来源同一清洗与跨源去重纪律，未解析名称如实登记不静默丢弃，重建可复现且不触碰任何受保护资产。

## Requirements

### Requirement: 注册表补录 SHALL 逐条证据溯源且未确认不录

`LIANJIA_COMMUNITY_REGISTRY` 补录 SHALL 仅收录已经唯一复核人确认的精确同名映射（示例小区144→C-XXXX0116、示例小区202→C-XXXX0052、示例小区041→C-XXXX0053、示例小区219→C-XXXX0105、示例小区031→C-XXXX0009），每条 SHALL 可溯源到证据包 `01-数据/sources/同名核验证据包-P0五小区-V0.1.md` 的判定表（§0）与复核记录（§8）；证据包中标注待核、名录外或未确认的相似命名 SHALL NOT 进入注册表。

#### Scenario: 五条映射逐条可溯源

* **WHEN** 审计注册表任一新增键

* **THEN** 该键的目标 `community_id` 与证据包 §0 表一致，且代码内溯源注记指向证据包 §0/§8 对应行

#### Scenario: 相似命名不误入

* **WHEN** 检查 ext 中与五小区相似的命名（示例小区145/示例小区217/红棉苑南区/示例小区009等）

* **THEN** 注册表中不存在这些名称的键，其成交行不因本 change 入池

### Requirement: ext 合并源 SHALL 统一走冻结输入并沿用同一清洗与去重纪律

marts 合并 SHALL 将 staged `lianjia_ext` current 指针指向 run 的普通住宅表作为第四输入源：仅读取指针登记的 run（本次 `20260831T041648Z`）、仅普通住宅口径（README §3.3 排除类不得由此入池）；ext 行 SHALL 与既有来源一样经过 WP4-C 清洗（车位/来源内去重/异常单价）与跨源身份去重，跨源去重保留序 SHALL 保持既有相对序不变。ext 行的缺失与不可解析值 SHALL 按数据字典 §1 缺失语义如实携带，SHALL NOT 以零或推断值填充。

#### Scenario: 同一交易跨源去重不重复计数

* **WHEN** 同一交易身份（community_id+面积+成交日+总价）同时出现在既有链家来源与 ext run 中

* **THEN** 仅保留排序最高的一条，其余标疑似重复并附注记，正式池不重复计数

#### Scenario: 排除类房源不入池

* **WHEN** ext run 中存在非普通住宅行（车位/公寓/商办等）

* **THEN** 该行不进入普通住宅合并输入，不出现在 `valid_sale` 中

#### Scenario: 既有保留行不变

* **WHEN** 对比重建前后跨源去重的保留选择

* **THEN** 既有链家与房天下行之间的保留序与重建前一致，不因新来源加入而翻转

### Requirement: 未解析名称 SHALL 如实登记且不静默丢弃

ext 行经标准名、一致别名与注册表三级解析后仍无法确定标准 `community_id` 的，SHALL NOT 进入正式池，其条数与代表性源名 SHALL 计入数据质量报告的未匹配登记（沿用 LJ-E「名录外如实标记」语义），SHALL NOT 静默丢弃或臆测归并。

#### Scenario: 未匹配行计数可对拍

* **WHEN** 重建完成后核对质量报告

* **THEN** ext 输入行数 = 入池行数 + 未匹配登记行数，两侧数字由同一表直接生成且可复算

### Requirement: 重建 SHALL 可复现且受保护资产不变

重建 SHALL 产出 DerivedManifest（登记全部输入快照与 ext run 标识/指纹、行数、构建时间、代码版本）；同输入重跑 SHALL 产出逐字节一致的 marts 产物；`sale_event_id` SHALL 全表唯一。`data/raw/`、`data/staged/`（含全部 ext runs）、`scope_policy_v1.0/v1.1.parquet`、`data/release/`、`05-估值报告/`、`01-数据/census/community-census-v1-2/` 与 `community-census-v1-2-r1/` SHALL 逐字节不变；重建 SHALL NOT 覆盖或改写任何原始与冻结证据。

#### Scenario: 重跑逐字节一致

* **WHEN** 以同一输入连续执行两次合并重建

* **THEN** `valid_sale.parquet`、`valid_listing.parquet` 与质量报告逐字节一致，manifest 输入指纹不变

#### Scenario: 受保护资产零改写

* **WHEN** 重建完成后核对受保护目录哈希

* **THEN** 上述受保护资产哈希与构建前基线一致，变更仅限 marts 层目标产物及其 manifest
