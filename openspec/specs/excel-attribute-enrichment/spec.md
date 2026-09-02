# 外部 Excel 属性字段接入估值链

## Purpose

把冻结数据版本 `lianjia_ext_v1_20260830` 外部链家成交 Excel 中的楼层/朝向/是否有电梯/装修情况/建成时间标准化进 staged 不可变新 run 与 `valid_sale` 中间表，并为估值链开通「相似度 + 可信度」读取通道（金额型差异修正除外），以时间外回放前后对比作为数据版本切换门。零网络零付费；冻结 v1 只读不动。

## Requirements

### Requirement: 属性标准化 SHALL 遵守缺失语义并保留原文

五列（楼层/朝向/是否有电梯/装修情况/建成时间）SHALL 从既有 staged 原文列标准化，SHALL 保留原文列不动；空或「暂无/暂无数据」SHALL 记为 MISSING（落表 None）；原文存在但无法解析 SHALL 记为 PARSE\_FAILURE 并计入质量报告；SHALL NOT 以 0 或臆造值（如由楼层段位推精确楼层、由总层数反推装修）充当任何属性值。电梯字段 SHALL 以来源披露值为准（有→True、无→False）；数据字典 §3.5 的「总层数>7 推断电梯」仅 SHALL 用于来源未披露时的备选口径，且推断值 SHALL 标注推断依据，SHALL NOT 覆盖披露值。

#### Scenario: 暂无占位解析为缺失而非零

* **WHEN** 某行建成时间为「暂无数据」、是否有电梯为「暂无数据」

* **THEN** 该行 `year_built` 与 `has_elevator` 落表为 None（MISSING）

* **AND** 质量报告把这两列计入 MISSING 分布，不计入任何以 0 为有效值的聚合

#### Scenario: 无法解析的原文计为解析失败

* **WHEN** 某行楼层原文不符合「段位/共N层」形态且无法拆出总层数

* **THEN** 该行楼层相关标准化列按字段分别记 PARSE\_FAILURE（或 None）

* **AND** 楼层联合解析失败分布进入 v2 质量报告

#### Scenario: 原文列保持不变

* **WHEN** 生成 staged v2 run

* **THEN** `floor_raw` 等五列原文逐字节保持 v1 run 中的值

* **AND** 标准化列与原文列并存可互相核对

### Requirement: staged v2 SHALL 为不可变新 run 且冻结 v1 不动

staged v2 SHALL 写入新 run 目录（新 run\_id），SHALL NOT 覆盖、移动或删除任何既有 run；staged 层 current 指针 SHALL 以切换指针方式指向 v2；数据冻结版本 `lianjia_ext_v1_20260830` 的目录、freeze manifest 与版本说明 SHALL 保持逐字节不变。v2 质量报告 SHALL 包含各属性列覆盖率（PARSED/MISSING/PARSE\_FAILURE 分布）与楼层联合解析失败分布。

#### Scenario: 新旧 run 并存可回退

* **WHEN** staged v2 run 完成并切换 staged current 指针

* **THEN** v1 run 目录完整保留，按 v1 run\_id 仍可读取原表

* **AND** 消费方按指针回退即可回到 v1 数据

#### Scenario: 冻结版本复核哈希不变

* **WHEN** v2 落盘后按 v1 freeze manifest 复核产物 SHA256

* **THEN** 全部登记值一致，无任何文件被改写

### Requirement: valid\_sale 扩列 SHALL 以身份键回填且无匹配留未知

`valid_sale` 属性扩列 SHALL 通过「小区名→标准 community\_id（既有权威表与已登记链家社区注册表 lookup）+ 交易身份键（community\_id+面积+成交日+总价）」join staged v2 属性实现；命中行 SHALL 记录可溯源注记（来源 run 与身份键）；同一身份多行 SHALL 按既有字段丰富度规则取一并将冲突计数入质量报告；无匹配行 SHALL 如实留 None。回填步骤 SHALL 为显式可跳过步骤：跳过或以无属性列重建 `valid_sale` 时，下游 SHALL 自动回旧行为。质量报告 SHALL 记录 join 命中率、冲突数与各属性列回填前后覆盖率。

#### Scenario: 身份键命中回填可溯源

* **WHEN** 某 valid\_sale 行的身份键与 staged v2 恰一行匹配

* **THEN** 该行属性列被回填，且行级注记指向来源 run 与身份键

* **AND** 人工可按注记回查原始行

#### Scenario: 无匹配与多匹配如实处理

* **WHEN** 某行身份键无匹配，或匹配到多行且字段取值冲突

* **THEN** 无匹配行属性保持 None；多匹配行按丰富度规则取一并把冲突计数入报告

* **AND** 不臆测合并、不静默覆盖

#### Scenario: 回退即旧行为

* **WHEN** 以不执行属性回填的方式重建 valid\_sale（无属性列）并重跑估值

* **THEN** 相似度与可信度行为与本 change 之前一致

* **AND** 不需要修改估值代码即可完成回退

### Requirement: 估值接入 SHALL 只走相似度与可信度通道且可一键回退

属性列 SHALL 仅通过既有相似度分项（电梯/朝向/年代；楼层分项本轮保持不激活并如实披露）与可信度分项（目标房源完整度等既有分项）进入估值；相似度权重、分项公式与 rule\_version SHALL 不变，未知分项仍 SHALL 移出加权分母；金额型差异修正通道 SHALL NOT 因属性接入而自动启用，任何数值修正仍 SHALL 另有市场证据。回读开关 SHALL 满足：恢复不读属性列即回旧行为。

#### Scenario: 属性进入相似度分母

* **WHEN** 目标与可比案例的电梯/朝向/年代均已知

* **THEN** 对应分项按既有公式与权重进入加权

* **AND** 楼层分项因双方精确楼层未知仍被移出分母（披露于证据）

#### Scenario: 金额修正不被自动启用

* **WHEN** 可比与目标的楼层/电梯/装修等属性差异已知

* **THEN** 差异修正表不新增任何金额或比例修正行

* **AND** 属性差异仅通过区间/可信度/人工复核说明体现

#### Scenario: 回滚开关回旧行为

* **WHEN** 关闭属性回读（或 valid\_sale 不含属性列）

* **THEN** 相似度与可信度输出与本 change 之前同配置一致

### Requirement: 数据版本切换 SHALL 以时间外回放对比为门并整体披露

以 v2 属性 valid\_sale 重跑与既有发布版本相同配置的时间外回放 SHALL 产出前后对比（APE 中位/高分位、区间覆盖率与相对宽度、可信度分布、属性覆盖率明细）；中心值 APE 不得因接入而劣化，若劣化 SHALL NOT 切换正式数据引用并保留回退路径；中心值与可信度分布的一切变化 SHALL 整体如实披露，SHALL NOT 静默采用。回放 SHALL 无未来泄漏：subject 属性只来自目标成交自身记录的静态房产事实。

#### Scenario: 前后对比报告产出

* **WHEN** 分别以接入前后数据重跑同一回放配置

* **THEN** 报告给出 APE 中位/高分位、区间覆盖率、可信度分布与属性覆盖率的逐项前后对比

* **AND** 每项变化可追溯到数据版本与回放 run

#### Scenario: APE 劣化阻止切换

* **WHEN** 接入后中心值 APE 中位劣化超过接入前

* **THEN** 正式数据引用保持接入前状态并输出对比证据

* **AND** 改进另案评估，不静默保留劣化版本

### Requirement: 版本登记 SHALL 与正式基线确认分离

v2 SHALL 生成版本指针文件与登记材料（manifest 含产物路径、计数与 SHA256）；`lianjia_ext_latest.json` 的指向切换与 v2 的正式引用 SHALL 待用户确认新基线后执行；确认前系统 SHALL 以显式参数方式试用 v2，SHALL NOT 默认把 v2 当作正式数据版本。既有冻结 v1 及其登记 SHALL NOT 被改写，后续一切增量 SHALL 走新 run 与新版本。

#### Scenario: 试用与正式引用分离

* **WHEN** 用户尚未确认 v2 基线

* **THEN** 回放与验证以显式指定 v2 run 的方式进行

* **AND** `lianjia_ext_latest.json` 仍指向 v1，正式估值默认数据版本不变

#### Scenario: 用户确认后切换指针

* **WHEN** 用户确认 v2 为新基线

* **THEN** 版本指针切换为 v2 且旧版本目录保留不动

* **AND** 切换记录写入版本说明供审计

