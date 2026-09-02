# ScopePolicy 版本化重定级与生效治理（scope\_policy）

## Purpose

ScopePolicy 版本化重定级能力：基于重建后的正式成交主表对全量名录小区统一重算支撑档位并落版本化正式表，保证重算判据统一不开口子、旧版本及其历史结果零覆盖、formal 纳入名单的版本切换受用户确认闸控制（落盘不等于生效）。

## Requirements

### Requirement: 重定级 SHALL 基于冻结输入对全量小区统一重算

ScopePolicy 新版本 SHALL 以 community 权威表全量小区为判定框架，12 个月案例数 SHALL 从重建后的 `valid_sale`（输入 manifest 指纹与 as-of 冻结进产物）统一重算；档位判据 SHALL 沿用现行规则（12 个月 ≥15 可支撑→纳入、8–14 有条件支撑→参考、<8 暂不支撑→拒绝）并保持边界三分门控（边界待定不纳入、范围外拒绝）；重算 SHALL 对全部小区适用同一判据，SHALL NOT 为个别小区设置例外通道。草案 CSV（census r1 design D3）SHALL 仅作方向参考，SHALL NOT 直接采纳为正式表内容。

#### Scenario: 示例小区144按重建数据重定级

* **WHEN** 重建后主表中示例小区144 12 个月案例数 ≥8 且 <15 且边界为机器确认

* **THEN** v1.2 中其支撑档位为有条件支撑、判定为参考，与统一判据的机械重算结果一致

#### Scenario: 既有纳入小区不因重算过程被静默处理

* **WHEN** 某 v1.1 纳入小区在重建后跌破或越过门槛

* **THEN** v1.2 按统一判据给出其新档位并写入差异报告，不做任何小区级的特判维持或特判剔除

#### Scenario: 重算判据机械可复现

* **WHEN** 以同一冻结输入（重建后主表 + 冻结 as-of）重算任一小区档位

* **THEN** 结果与 v1.2 表中该行一致，判据不依赖人工挑选

### Requirement: 版本化产物 SHALL 不覆盖旧版本及其历史结果

新版本 SHALL 写入独立版本化文件（`data/entities/scope_policy_v1.2.parquet` + manifest，登记输入指纹、as-of、行数、构建时间、代码版本）；`scope_policy_v1.0.parquet`、`scope_policy_v1.1.parquet` 及其 manifest SHALL 逐字节不变；基于 v1.1 产出的历史结果（发布门禁证据、冻结估值等）SHALL NOT 被改写或重算覆盖。

#### Scenario: v1.1 零覆盖

* **WHEN** v1.2 落盘后核对实体层

* **THEN** v1.0/v1.1 文件与 manifest 哈希与构建前基线一致，新增文件仅 v1.2 及其 manifest

### Requirement: formal 纳入名单版本切换 SHALL 经用户确认生效

v1.2 表落盘 SHALL NOT 自动改变 formal 输出判定：生效前 estimate 的 formal 闸门 SHALL 维持读取 v1.1 纳入名单，formal 双闸（发布决定记录 + 纳入名单）语义不变；v1.2 的启用属正式基线确认，SHALL 仅在 user 明确确认后切换，change 产物 SHALL 明确记载「由 user 确认后才生效」。

#### Scenario: 落盘不改变 formal 行为

* **WHEN** v1.2 表落盘后、用户确认前对任一目标小区执行 estimate

* **THEN** formal 判定输入仍为 v1.1 纳入名单，输出状态分布与落盘前一致（数据变化除外）

#### Scenario: 用户确认前不出现在生效链路

* **WHEN** 审查 estimate formal 闸门代码与发布决定记录

* **THEN** 二者均不引用 v1.2 文件，v1.2 启用方式在 change 收尾文档中登记为用户确认后的独立动作

### Requirement: 重定级差异 SHALL 全部由机器产物生成并可对拍

v1.1→v1.2 差异报告 SHALL 由两版机器产物直接生成：逐小区档位变化、纳入名单增减、重点复核小区（示例小区144、拾光里、示例小区166、示例小区136、示例小区203）的重算前后 12 个月案例数与档位，SHALL NOT 手填数字；差异报告 SHALL 与 v1.2 表、v1.1 表及重建后主表逐项可对拍。

#### Scenario: 差异表与两版表对拍一致

* **WHEN** 抽取差异报告中任一小区的档位变化

* **THEN** 该行等于 v1.2 表与 v1.1 表对应行的机械对比，无手填差异

#### Scenario: 重点小区重算依据可回溯

* **WHEN** 查看重点复核小区的差异行

* **THEN** 其重算前后案例数可由冻结输入重算复现，档位变化与统一判据一致
