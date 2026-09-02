# data-freeze Specification

## Purpose

承载 EXTFP5 数据冻结行为合同：把已完成的 lianjia_ext 外部数据录入线产物（staged 成交/普通住宅表、生产批次 229 资产链、既有调试与验收集资产）登记为只读可消费数据版本，产出运行清单、版本指针与限制披露，并将外部数据录入线状态标注为「待估值接入验证」。本能力 SHALL NOT 构成估值接入授权，SHALL NOT 执行任何新的下载/OCR/付费调用，SHALL NOT 改写既有产物。

## Requirements

### Requirement: 冻结版本 SHALL 只读登记且不可变更

系统 SHALL 为 lianjia_ext 数据建立首个可消费数据版本 `lianjia_ext_v1_20260830`，覆盖：lianjia_ext 成交/普通住宅 staged 表、生产批次 229 资产链（选择清单、原图、OCR 运行记录与原始响应、逐词、转录标注、质量报告、完整性豁免、成本台账、批次合同、画像报告）、既有 10 张调试与 300 张验收集及验证子集资产（按现状登记，OCRNEXT-D 并发对照数据以「已执行未验收」口径标注）。冻结 SHALL 为纯登记与指针操作：不改写、不移动、不删除任何既有运行记录、原始响应、标注表、豁免与质量报告；版本内容一经登记即视为只读，任何后续写操作 SHALL 生成新版本或新子版本，SHALL NOT 覆盖已冻结内容。如盘点发现缺口，SHALL 如实报告交用户裁定，SHALL NOT 自动扩权或补造证据。

#### Scenario: 冻结登记零网络零付费

- **WHEN** 系统执行数据冻结登记
- **THEN** 不发起任何网络下载或付费 OCR 调用
- **AND** 既有原始文件、运行记录与报告保持字节不变

#### Scenario: 盘点缺口如实报告

- **WHEN** 冻结盘点发现某产物缺失或落盘位置不符
- **THEN** 系统将该缺口列入冻结报告并交用户裁定
- **AND** 不自动新建、补造或重跑该产物

### Requirement: 运行清单 SHALL 完整登记且可复核

系统 SHALL 生成 freeze manifest，登记每个纳入产物的相对路径、计数与 SHA256；manifest 自身 SHA256 与旁证文件 SHALL 落盘；版本指针 JSON SHALL 引用 manifest 与版本说明，指针切换 SHALL 通过切换指针而非删除目录实现（技术方案 §16）。manifest、指针与冻结报告 SHALL 同入 change 目录留档。

#### Scenario: 清单哈希可复核

- **WHEN** 冻结登记完成
- **THEN** 按 manifest 重新计算各产物 SHA256 与登记值一致
- **AND** 出现任何哈希不一致或计数不符时版本 SHALL 保持未发布并输出缺口清单

#### Scenario: 指针切换不删目录

- **WHEN** 后续版本建立并切换指针
- **THEN** 指针指向新版本清单
- **AND** 旧版本目录完整保留，不删除、不写入

### Requirement: 限制与披露 SHALL 全量入版本说明

版本说明 SHALL 显式披露：完整性豁免（15 组重复图片资产/30 资产原始响应覆盖）、转录状态口径（CONFLICT 425 / ROOM_ONLY 40 / NEEDS_REVIEW 2）、重复图片组、示例小区130（C-XXXX0063）0 命中、独立 verify 遗留 SUGGESTION ①②（raw_response_sha256 记录口径差、select 生产 profile 无记录级去重）及既有已知限制四项（KL-1..KL-4）。披露缺失任一必须项时，版本 SHALL NOT 标记为可消费。

#### Scenario: 披露缺项阻止发布

- **WHEN** 版本说明缺失任一必须披露项
- **THEN** 该版本保持未发布并列出缺失项
- **AND** 系统不产出「可消费」结论

### Requirement: 状态标注 SHALL 为待估值接入验证

冻结完成后，外部数据录入线状态 SHALL 标注为「已冻结 v1，待估值接入验证」；该标注 SHALL 不等于估值接入授权，估值接入价值验证方案 SHALL 另案提案，本能力 SHALL NOT 预先创建影子运行或估值接入合同。

#### Scenario: 状态收口不构成授权

- **WHEN** 冻结状态标注发布
- **THEN** 状态明确为「已冻结 v1，待估值接入验证」
- **AND** 标注同步注明估值接入须另行提案

### Requirement: 后续增量 SHALL 走新批次与新版本

已冻结版本只读；后续扩窗/扩小区、示例小区130别名扩充等增量 SHALL 产出新的选择清单与新 run 记录，完成后另立新版本指针（如 v1.1 / v2），旧版本目录 SHALL 保留不删不写。

#### Scenario: 增量批次产生新版本

- **WHEN** 后续增量批次完成
- **THEN** 新版本指针建立并引用新批次产物
- **AND** 既有冻结版本内容不被改写
