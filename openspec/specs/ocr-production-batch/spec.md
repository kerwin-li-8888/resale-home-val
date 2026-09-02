## Purpose

承载 EXTFP4 授权生产批次的行为合同：在用户实施授权与逐批批次确认的约束下，把已验证的下载与 OCR 能力推进到受控生产路径（选择清单派生与冻结、下载批次、OCR 批次、转录与范围外隔离、质量报告与完整性门禁、断点续跑与自动停止），产出可追溯批次资产；本能力 SHALL NOT 接入估值，SHALL NOT 预先决定范围外样本的消费策略。

## Requirements

### Requirement: 生产选择清单 SHALL 从全量清单派生子集并不可变冻结

系统 SHALL 以既有 208,075 记录全量清单（`selection_manifest.json`，snapshot `0b86156b…`）为唯一派生源，按「冻结支持小区 ∩ 近 12 个月成交」口径（scope_policy v1.1 的 8 个 FROZEN_SUPPORTED_IDS；4 个 conditional 默认排除；成交日期窗口 [2025-07-20, 2026-07-20]）派生首个生产 profile，并生成新的不可变 `selection_manifest`。manifest SHALL 至少固定：选择规则与版本、数据快照 ID、地理范围、成交时间范围、普通住宅过滤条件、记录数与资产数、记录 ID 清单及哈希、预计下载量与存储量、Qwen 成本上限（manifest 级按资产数外推 + change 级 ≤300 元兜底）、用户授权合同引用。`ordinary_residential_all` 全量批次 SHALL NOT 混入本次授权。派生 SHALL 幂等可复现；未能匹配目标小区的记录 SHALL 进入排除清单并如实披露计数与原因，SHALL NOT 静默丢弃。生产 profile 派生 SHALL 按 `source_record_id` 做记录级去重：同一 `source_record_id` 对应多行时 SHALL 按确定性规则保留一行（户型图字段可解析且字典序稳定的行），被去重行数与样例 SHALL 写入排除清单报告并披露；既有全量清单锚点（208,075）与已冻结批次 1 manifest（229 资产）SHALL NOT 重算或改写。记录级去重 SHALL 为生产 profile 的显式启用行为，默认参数（不启用去重）下派生行为与 EXTFP2-B 全量清单完全一致，不得改变既有冻结产物的字节。

#### Scenario: 子集派生与 manifest 冻结

- **WHEN** 系统按生产 profile 执行选择
- **THEN** 生成的 selection_manifest 包含上述全部冻结字段并登记 SHA256
- **AND** 相同输入重跑产出相同记录集与哈希，全量清单与其文件保持字节不变

#### Scenario: 小区未匹配记录处置

- **WHEN** 某普通住宅记录无法匹配任何冻结支持小区
- **THEN** 该记录进入排除清单并披露计数与原因
- **AND** 不进入本次生产 selection_manifest，也不放宽匹配规则以凑量

#### Scenario: 记录级去重与披露

- **WHEN** 生产 profile 派生遇到同一 `source_record_id` 对应多行且多行均可产出资产
- **THEN** 该记录只保留一行（按确定性规则：户型图字段可解析且字典序稳定），其余行计为被去重行
- **AND** 被去重行数与样例写入排除清单报告披露，不静默丢弃
- **AND** 既有全量清单锚点与已冻结批次 1 manifest 保持字节不变、不重算

### Requirement: 真实投放 SHALL 逐批取得批次确认

每个下载或 OCR 批次投放前，系统 SHALL 向用户展示并取得批次确认，至少包含：固定 commit、selection_manifest SHA256、批次任务数（图片数）、批次金额上限与 change 级累计预算余量。未取得批次确认时，系统 SHALL NOT 投放任何真实下载或付费调用。批次确认记录 SHALL 落盘留档，后续投放行为 SHALL 与确认内容一致。

#### Scenario: 无批次确认不投放

- **WHEN** 某批次未完成批次确认流程
- **THEN** 该批次保持待确认状态，不产生任何网络下载或付费 OCR 请求
- **AND** 已冻结的 manifest 与配置保持只读

#### Scenario: 批次确认留档

- **WHEN** 用户确认某批次投放
- **THEN** 系统记录确认时刻、确认内容（commit、SHA256、任务数、金额上限）与批次标识
- **AND** 实际投放超出确认内容任一项时立即停止并报告

### Requirement: 下载批次 SHALL 遵守网络纪律与资产校验

下载 SHALL 只访问 selection_manifest 内既有的白名单域名（`ke-image.ljcdn.com`）公开图片 URL，SHALL NOT 抓取新成交页面或绕过验证码、签名、授权与访问限制；对 429、超时、连接中断和 5xx SHALL 指数退避加随机抖动重试，4xx 除 408/429 外 SHALL NOT 重试；单任务重试上限与全批停止条件 SHALL 写入配置。落盘前 SHALL 完成魔数/MIME/尺寸校验并计算 SHA256 写入资产 manifest。磁盘门禁：每批次投放前 SHALL 实测剩余空间并要求 ≥ 外推需求 ×1.5，不足即停。

#### Scenario: 白名单外域拒绝

- **WHEN** 下载队列中出现白名单外域名的 URL
- **THEN** 该 URL 记录为违规且不计入资产，绝不静默丢弃
- **AND** 不发起对该域名的网络请求

#### Scenario: 磁盘不足即停

- **WHEN** 批次投放前实测剩余空间低于外推需求 ×1.5
- **THEN** 系统停止该批次投放并报告缺口数值
- **AND** 不通过删除既有产物或压缩原始字节腾挪空间

### Requirement: OCR 批次 SHALL 使用冻结配置并运行时强制成本门禁

OCR 批次 SHALL 使用冻结生产配置（并发 8、模型 `qwen-vl-ocr-2025-11-20`、task `advanced_recognition`、min_pixels 3072、max_pixels 8388608、enable_rotate false、stream false、timeout 60s、单图 max_attempts=3、全批 max_retries 门禁 50），SHALL NOT 改变任何图片的请求合同。成本门禁 SHALL 运行时强制：成本按逐任务求和口径实时累计（EXTFP3-G#F1 口径）；单图异常阈值沿用 0.004 元基线 / 0.0048 元暂停阈值；attempt ≤ manifest 资产数 ×1.10；change 级累计人民币硬上限 ≤300 元；达到任一门禁 SHALL 自动停止投放新请求，保留在飞结果与证据，不透支后继续。

#### Scenario: 请求合同一致

- **WHEN** OCR 批次以并发 8 运行
- **THEN** 每次请求的模型、任务、像素、旋转、流式与保存参数与冻结配置完全一致且可核对请求合同哈希
- **AND** 并发峰值不超过 8，无状态错乱、响应覆盖或重复扣费

#### Scenario: 门禁先到即停

- **WHEN** 累计成本、attempt 数或重试数达到任一冻结门禁
- **THEN** 系统停止投放新请求并保留全部在飞结果与证据
- **AND** 停止原因与触发时的门禁数值写入批次记录

### Requirement: 原始响应哈希 SHALL 双口径登记

新运行（本 change 生效后创建的 OCR 运行）的每个任务记录 SHALL 同时登记两个哈希：`raw_response_sha256`（HTTP 响应原始字节的 SHA256，历史可比口径）与 `raw_response_file_sha256`（原始响应落盘文件实际字节的 SHA256）。两口径 SHALL 并列保留于运行记录供审计与披露；落盘净化文本与 Windows CRLF 翻译行为 SHALL 保持不消除（不改证据形态）。历史运行记录与冻结版本 v1 SHALL NOT 回填或改写。

#### Scenario: 新运行登记双口径

- **WHEN** 系统落盘一条新 OCR 原始响应
- **THEN** 任务记录同时写入 `raw_response_sha256` 与 `raw_response_file_sha256`
- **AND** `raw_response_file_sha256` 与落盘文件实际字节重算一致

#### Scenario: 历史记录不回填

- **WHEN** 完整性门禁处理无 `raw_response_file_sha256` 字段的历史运行记录
- **THEN** 该记录保持原字节与字段不回填改写
- **AND** 核对按既有换行归一化双口径兼容执行

### Requirement: 转录 SHALL 隔离范围外样本

转录产物 SHALL 对范围外样本（如多层、商住混入等非第一阶段范围图片）显式标注 `out_of_scope`：不自动接受、不入有效分母、不进入有效字段统计，但 SHALL 保留可审计痕迹（继承 #N5 口径）。估值价值验证的消费策略由 change floorplan-value-validation 裁定：ACCEPTED MAY 提供房间类型、数量与面积构成；ROOM_ONLY MAY 提供合格房间类型与数量，但其面积 SHALL 保持缺失且不得推算；CONFLICT、NEEDS_REVIEW 与范围外字段 SHALL NOT 进入自动验证特征。任何 OCR 值 SHALL 只作派生特征，SHALL NOT 覆盖 Excel 原字段。

#### Scenario: 范围外标注隔离
- **WHEN** 转录产出包含判定为范围外的样本
- **THEN** 该样本字段标注 `out_of_scope` 并排除在有效分母、有效字段统计与自动验证特征之外
- **AND** 原始响应、标注痕迹与排除原因保留可审计

#### Scenario: ROOM_ONLY 无面积仍可提供分类信息
- **WHEN** ROOM_ONLY 标注含合格房间名但不含面积
- **THEN** 下游价值验证可使用标准化房间类型与数量
- **AND** 不生成、填补或反推任何面积值

#### Scenario: 消费策略不预先决定
- **WHEN** 生产批次自身需要判断范围外样本、ROOM_ONLY 或冲突字段是否进入估值
- **THEN** 生产批次不自行新增消费规则，而按 floorplan-value-validation 已裁定的分类策略向离线验证交付
- **AND** 该交付不产生正式估值接入或自动接受范围外字段的结论

#### Scenario: OCR 不覆盖 Excel 原值
- **WHEN** OCR 派生值与 Excel 原字段存在差异
- **THEN** 两者按各自血缘保留，OCR 仅进入离线派生特征
- **AND** 生产批次产物不产生自动覆盖 Excel 或正式估值接入的行为

### Requirement: 质量报告与完整性门禁 SHALL 构成批次退出证据

每个批次 SHALL 按技术方案 §14 口径产出质量报告（输入数据版本与选择规则、纳入与各类排除数量、空 URL/占位图/多 URL/解析失败、下载状态与字节分布、URL/SHA256/感知哈希重复分布、OCR 成功/部分/失败/需复核计数、Token 与费用、延迟与重试、转录覆盖与冲突清单、模型/请求/解析器/代码/数据版本、未通过门禁与最小修复建议），动态数字 SHALL 由机器产物生成，Markdown 只解释。批次完成判定 SHALL 通过完整性门禁（资产、原始响应、转录与运行记录可追溯且相互一致）；原始响应哈希核对 SHALL 以落盘实际字节哈希（`raw_response_file_sha256`）为主口径，并与原始字节哈希（`raw_response_sha256`）并列披露；无落盘口径字段的历史运行记录按既有换行归一化双口径兼容核对，SHALL NOT 回填改写。门禁未通过时批次 SHALL 保持未完成并输出缺口清单，SHALL NOT 静默关闭。仅当用户对完整性缺口作出明确豁免裁定并留档（记录范围、理由与披露要求）时，批次方可关闭，且批次结论与后续汇总 SHALL 披露该缺口；成本硬上限与凭证泄露风险两类停止条件 SHALL NOT 适用豁免。批次结论与汇总报告 SHALL 显式引用四项已知限制：40 张样本量判别力局限（不构成 H3=100% 或 H9≥99.5% 正式认证）、RV-OCRNEXT-C-01#F1 双轮一致确定性误关联无规则防护、#N5 范围外样本口径、独立集不可计算项；SHALL NOT 以「复验 PASS」宣称 H3/H9 类门槛已获正式认证。

#### Scenario: 完整性门禁

- **WHEN** 一个批次的下载、OCR 与转录产物齐备
- **THEN** 完整性门禁核对资产、原始响应、转录与运行记录的哈希与数量一致性
- **AND** 门禁未通过时批次保持未完成并输出缺口清单

#### Scenario: 原始响应落盘口径主核对

- **WHEN** 完整性门禁核对新运行的原始响应哈希
- **THEN** 以 `raw_response_file_sha256`（落盘实际字节）为主口径核对
- **AND** `raw_response_sha256`（原始字节）与落盘口径并列披露

#### Scenario: 历史运行记录兼容核对

- **WHEN** 完整性门禁核对无 `raw_response_file_sha256` 字段的历史运行记录
- **THEN** 按既有换行归一化双口径兼容核对（字节或净化文本口径任一匹配即一致）
- **AND** 历史记录与冻结版本不回填、不改写

#### Scenario: 完整性缺口的用户豁免关闭

- **WHEN** 完整性门禁存在缺口且用户作出明确豁免裁定并留档（范围、理由、披露要求）
- **THEN** 批次方可关闭，且批次结论与后续汇总 SHALL 披露该缺口
- **AND** 成本硬上限与凭证泄露风险 SHALL NOT 适用豁免

#### Scenario: 已知限制强制引用

- **WHEN** 批次质量报告或汇总结论产出
- **THEN** 结论显式携带四项已知限制及其对结论的边界作用
- **AND** 不出现「H3/H9 已获正式认证」或等效表述

### Requirement: 断点续跑与自动停止 SHALL 保留证据

批次 SHALL 支持断点续跑：按幂等键（`sale_record_key`/`asset_id`/`download_task_id`/`ocr_task_id`，技术方案 §8.3）识别已有完整成功产物的任务并跳过；中断恢复 SHALL NOT 重复扣费、SHALL NOT 覆盖既有产物；`--force-new-run` SHALL 产生新运行而不改写旧运行。技术方案 §19.2 自动停止条件 SHALL 全量写入运行门禁：来源开始要求登录、验证码或访问控制；连续错误率、占位图率或 MIME 异常超过配置门槛；原始文件、图片或已有 manifest 被意外修改；Qwen 返回模型与固定模型不一致；成本达到硬上限；schema 有系统性解析失败；发现未授权房产类型或范围进入选择清单；凭证或敏感信息有泄露风险；修复需要超出本 change 范围。任一条件触发时批次 SHALL 停止并保留全部证据。

#### Scenario: 中断恢复不重复扣费

- **WHEN** 批次在部分完成后中断并恢复
- **THEN** 已有完整成功产物的任务按幂等键跳过，不重复发起付费请求
- **AND** 恢复后的成本累计与逐任务证据连续一致

#### Scenario: 自动停止触发

- **WHEN** 任一 §19.2 条件触发
- **THEN** 批次立即停止投放并保留全部在飞与已完成证据
- **AND** 停止原因、触发数值与时间写入批次记录并报告用户

### Requirement: 生产批次产物 SHALL 纳入数据冻结版本且冻结后只读

生产批次（批次 1：229 记录/229 资产）的全部产物（selection manifest、原图、OCR 运行记录与原始响应、逐词与转录标注、质量报告、完整性豁免、成本台账、画像报告）SHALL 纳入数据冻结版本 `lianjia_ext_v1_20260830` 登记；冻结后该批次产物 SHALL 只读引用，后续生产批次 SHALL 走增量选择清单 + 新批次确认并另立新版本指针，SHALL NOT 改写已冻结批次产物。

#### Scenario: 冻结后只读

- **WHEN** 冻结版本 `lianjia_ext_v1_20260830` 已发布
- **THEN** 批次 1 产物仅可只读引用
- **AND** 任何修改或扩量需求走新批次与新版本，不覆盖已冻结内容

#### Scenario: 后续批次另立版本

- **WHEN** 后续生产批次（扩窗/扩小区）获得授权并执行
- **THEN** 该批次以增量选择清单与新 run 记录落地
- **AND** 完成后另立新版本指针，旧版本目录保留

### Requirement: 全历史 profile 选择清单 SHALL 从既有全量清单派生并维持地理红线

系统 SHALL 以既有全量清单（`selection_manifest.json`，snapshot `0b86156b…`，208,075 记录锚点）为唯一派生源，按「冻结支持小区 ∩ 全部历史成交（不限成交日期窗口）」口径派生 EXTFP6 生产 profile：匹配 8 个 FROZEN_SUPPORTED_IDS，且小区匹配 SHALL 认可 `community_alias` 中 `conflict_status=一致` 的别名映射（含「示例小区130A区」「示例小区130B区」→ 示例小区130 C-XXXX0063）；4 个 conditional 小区维持默认排除。派生 SHALL 沿用 EXTFP4-SELECT 记录级去重规则（同一 `source_record_id` 按确定性规则保留一行，被去重行数与样例写入排除清单报告），生成新的不可变 `selection_manifest` 并登记 SHA256；未能匹配目标小区的记录（含全部全示例城市记录）SHALL 进入排除清单并如实披露计数与原因，SHALL NOT 静默丢弃，SHALL NOT 放宽匹配规则以凑量。既有 EXTFP4 窗内 profile、已冻结批次 1 manifest（229 资产）与全量清单锚点 SHALL NOT 重算或改写；默认参数下既有派生行为保持字节不变。（change add-extfp6-full-history-ocr-batch，2026-08-31）

#### Scenario: 全历史派生与 manifest 冻结

- **WHEN** 系统按 EXTFP6 全历史 profile 执行选择
- **THEN** 生成的 selection_manifest 包含选择规则与版本、数据快照 ID、全历史口径说明、记录数与资产数、记录 ID 清单及 SHA256、预计下载量与 Qwen 成本上限（change 级 ≤¥10）、用户授权合同引用
- **AND** 相同输入重跑产出相同记录集与哈希，既有锚点与已冻结产物字节不变

#### Scenario: 别名一致映射参与匹配

- **WHEN** 某普通住宅记录的小区源名为 `community_alias` 中 `conflict_status=一致` 的别名（如「示例小区130A区」「示例小区130B区」）
- **THEN** 该记录按别名映射匹配到对应 FROZEN_SUPPORTED_ID 参与选择
- **AND** 待定与冲突别名维持 blocked，不参与自动匹配

#### Scenario: 地理红线与排除披露

- **WHEN** 全量清单中存在不属于 8 个冻结支持小区的全示例城市记录
- **THEN** 该类记录全部进入排除清单并披露计数与原因，不进入本次 selection_manifest
- **AND** 不发生任何对排除记录的下载或 OCR 投放

### Requirement: OCR 批量上限与 change 级预算 SHALL 支持本批次规模且 fail-closed

系统 SHALL 将 OCR 运行批量上限（`OcrCostConfig.max_images`，默认 310）上调至不低于本批次冻结资产数（设计值 1,500），上调 SHALL NOT 改变 OCR 请求合同（模型、task、像素、旋转、流式、timeout）与单图门禁（单图成本基线 0.004 元 / 暂停阈值 0.0048 元、单图 max_attempts=3）；本 change 级累计人民币硬上限 SHALL 为 ≤¥10（含既有成本台账累计，逐任务求和口径实时累计），达到任一门禁（change 级预算、attempt ≤ 资产数 ×1.10、全批 max_retries 门禁）SHALL 自动停止投放新请求并保留在飞结果与证据，不透支后继续。真实下载与 OCR 投放 SHALL 维持逐批批次确认门禁（固定 commit、selection_manifest SHA256、批次任务数、金额上限），未取得批次确认 SHALL NOT 投放。批次完成并通过完整性门禁后，全部产物 SHALL 按数据冻结既有行为另立新数据版本指针（v1 `lianjia_ext_v1_20260830`、v2 `lianjia_ext_v2_20260831` 保持冻结只读），SHALL NOT 改写已冻结批次产物。（change add-extfp6-full-history-ocr-batch，2026-08-31）

#### Scenario: 批量上限覆盖本批次且请求合同不变

- **WHEN** 本批次资产数超过旧默认 310 并以并发 8 运行 OCR
- **THEN** 运行不被 max_images 门禁误停（上调后上限 ≥ 冻结资产数）
- **AND** 每次请求的模型、任务、像素、旋转、流式与保存参数与 EXTFP4 冻结配置完全一致，可核对请求合同哈希

#### Scenario: change 级预算达限即停

- **WHEN** 累计成本（含既有台账）达到 ≤¥10 硬上限，或 attempt、重试任一门禁先到
- **THEN** 系统停止投放新请求，保留全部在飞与已完成证据
- **AND** 停止原因与触发时的门禁数值写入批次记录并报告用户

#### Scenario: 无批次确认不投放

- **WHEN** 下载批次或 OCR 批次未完成批次确认流程
- **THEN** 该批次保持待确认状态，不产生任何网络下载或付费 OCR 请求
- **AND** 批次确认记录落盘留档，实际投放超出确认内容任一项时立即停止并报告

#### Scenario: 批次产物纳入新版本指针

- **WHEN** EXTFP6 批次完成且完整性门禁通过（或用户按既有规则作出留档豁免裁定）
- **THEN** 批次全部产物（selection manifest、原图、OCR 运行记录与原始响应、转录标注、质量报告、成本台账）登记进新数据版本指针
- **AND** v1 与 v2 冻结产物保持只读，逐字节不变
