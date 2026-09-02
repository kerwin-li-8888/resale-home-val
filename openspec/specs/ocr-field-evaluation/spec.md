# OCR 字段评估与离线回放

## Purpose

承载 EXTFT/OCRNEXT 系列旧工作包验收后仍然有效的能力：以冻结输入做字段级离线回放评估，保证 OCR 字段安全修复可验证、可回滚、不改写历史证据；并承载并发档位稳定性验证与候选并发/生产运行配置冻结（2026-08-30 change ocr-concurrency-optimization 验证后，候选并发已选定为 8）。

## Requirements

### Requirement: 离线回放 SHALL 使用冻结输入并新建评估记录

对既有 OCR 原始响应做离线回放时，系统 SHALL 只读取冻结的历史资产（原图批次、双轮原始响应、黄金标签），且 SHALL 为每次回放分配独立 `evaluation_id`，不得覆盖或改写任何历史运行记录。

#### Scenario: 重复回放不污染历史

- **WHEN** 对同一批冻结响应再次执行离线回放
- **THEN** 生成新的 `evaluation_id` 与新评估产物
- **AND** 既有运行记录、原始响应与历史评估内容保持字节不变

### Requirement: 字段安全规则变更 SHALL 登记且可回滚

OCR 字段安全修复规则（去重、数字占用、跨运行不一致隔离）的任何变更 SHALL 在既有规则登记表中登记并保留回滚条目；撤销规则 SHALL 不残留处理痕迹。

#### Scenario: 规则回滚恢复修复前行为

- **WHEN** 按登记条目回滚某条字段修复规则
- **THEN** 转录行为恢复到该规则引入前的口径
- **AND** 历史评估产物不被改写，仅新增回滚登记

### Requirement: 未验收结论 SHALL 按「已执行未验收」口径引用

并发吞吐对照（OCRNEXT-D，2026-08-28 执行）的真实运行数据 SHALL 可引用，但其结论 SHALL 标注为「已执行未验收、所属工作包已由用户取消」；系统 SHALL NOT 在任何新 change 中将其引用为已验收的并发结论，三选一总结论 SHALL 视为不存在。

#### Scenario: 新变更引用 D 阶段数据

- **WHEN** 新 change 需要引用并发对照的耗时、成本或差异数据
- **THEN** 引用附带「已执行未验收（工作包已取消）」状态标注
- **AND** 不产出或暗示 `CANDIDATE_SHADOW` 级别的验收结论

### Requirement: 并发档位稳定性验证 SHALL 使用冻结图片集与冻结判据

系统 SHALL 对冻结的图片子集执行 8 并发真实 OCR 运行，并按预登记判据评估稳定性与质量等价性；8 并发稳定达标后，SHALL 条件式补投 16 并发对照；任一档位未达标时 SHALL 停止升级并按证据产出结论，不追加调用。

#### Scenario: 8 并发稳定性验证

- **WHEN** 对冻结图片集以并发 8 执行 OCR 运行
- **THEN** 系统记录 wall 耗时、并发峰值、429/重试/PARTIAL/失败计数与逐字段差异清单
- **AND** 按预登记判据判定稳定达标或未达标；未达标时不自动补投 16 并发

#### Scenario: 16 并发条件式对照

- **WHEN** 8 并发稳定性验证达标
- **THEN** 系统以并发 16 对同一冻结图片集执行对照运行
- **AND** 按同一判据评估；证据不足时产出 `EVIDENCE_INSUFFICIENT` 而非强制选定档位

### Requirement: 并发验证 SHALL 保持请求合同不变且质量等价

并发档位变化 SHALL NOT 改变任何图片的请求合同（模型、任务、像素、旋转参数、流式与保存策略）；并发运行结果 SHALL 与串行基线在失败/部分/证据缺失计数与正确接受字段上等价，差异 SHALL 可解释。

#### Scenario: 请求合同不变

- **WHEN** 以不同并发档位运行同一图片集
- **THEN** 每次请求的模型、任务、像素、旋转与保存参数完全一致
- **AND** 运行记录可核对 request_hash 与请求参数

#### Scenario: 质量等价与差异可解释

- **WHEN** 并发运行与串行基线对比
- **THEN** 失败/部分/证据缺失计数不高于串行基线，且字段差异全部列入可解释清单
- **AND** 不允许以删除正确证据或降低输出换取吞吐提升

### Requirement: 候选并发与生产运行配置 SHALL 冻结并留档

系统 SHALL 在验证完成后冻结候选并发档位与生产运行配置（并发档位、重试与退避、成本门禁基线、性能观测字段与报告口径），并产出画像报告；冻结配置 SHALL 仅在获得 EXTFP4 授权后使用。

#### Scenario: 候选并发冻结

- **WHEN** 验证完成并选定候选并发
- **THEN** 系统记录候选并发档位、判定依据、质量与吞吐证据、运行配置与报告路径
- **AND** 未获 EXTFP4 授权前 SHALL NOT 用于生产批次

#### Scenario: 生产配置留档

- **WHEN** 冻结生产运行配置
- **THEN** 配置包含并发档位、重试、成本门禁与性能观测字段
- **AND** 与既有运行记录、原始响应保持只读隔离，不改写历史

### Requirement: 并发性能观测 SHALL 支持延迟与吞吐诊断

系统 SHALL 记录每次请求尝试的排队、发起、完成与持久化时间点、HTTP 状态、异常类别与并发峰值，使 Qwen 服务端、网络与本地持久化的耗时可分解；旧运行记录 SHALL NOT 被回填改写。

#### Scenario: 性能埋点

- **WHEN** 新并发运行执行
- **THEN** 每次尝试记录 queued_at、request_started_at、response_completed_at、persisted_at、HTTP 状态与异常类别
- **AND** 性能数据仅写入新 run_id 的运行记录，旧记录保持字节不变

#### Scenario: 连接池显式配置

- **WHEN** 以并发档位运行
- **THEN** HTTP 连接池最大/保活连接数按档位显式配置
- **AND** 429/超时只占用单任务并发槽，不阻塞全批

### Requirement: 独立集复验清单 SHALL 冻结于待验输出之前

系统 SHALL 为 EXTFP3-H REJECT 遗留的复验建立独立集清单：候选池 SHALL 为排除 manifest 并集（`reviewed_sample_exclusion_manifest.json`，并集 172）之外的剩余池（128 张）再减去备注列含「经人工核对」的样本（41 张，保守口径），候选池不足 40 张时 SHALL 如实报告并交用户裁定，SHALL NOT 放宽独立性以换取样本量。清单 SHALL 在待验版本输出之前冻结，并登记随机种子、分层规则与清单 SHA256。

#### Scenario: 清单冻结先于待验输出

- **WHEN** 系统生成 40 张独立集清单
- **THEN** 清单基于保守口径候选池（87 张）确定性抽样并登记随机种子、分层规则与 SHA256
- **AND** 该清单的生成时刻早于任何待验版本输出的产生时刻

#### Scenario: 候选池不足 40 张

- **WHEN** 保守口径候选池少于 40 张
- **THEN** 系统停止冻结并如实报告剩余样本数量及原因
- **AND** 不通过放宽排除范围、降低独立性或重复使用已触达样本补足 40 张

### Requirement: 独立集复验运行 SHALL 使用冻结并发档位且保持请求合同不变

系统 SHALL 以已验证的候选并发 8（change ocr-concurrency-optimization 验证；`frozen_extfp4_config`：max_attempts=3、max_retries=50、单图成本基线 0.004 元/异常阈值 0.0048 元、timeout 60s）对 40 张独立集真实运行一次；串行（并发 1）仅作备选。运行 SHALL NOT 新增图片下载（仅复用既有 300 张原图资产），且 SHALL NOT 改变 OCR 请求合同（模型、任务、像素、旋转、流式与保存策略）。真实调用仅在本 change 获用户实施授权后执行，投放前履行批次确认（固定 commit、清单 SHA256、任务数与金额上限）。

#### Scenario: 并发 8 单次真实运行

- **WHEN** 40 张独立集以并发 8 执行真实 OCR 运行
- **THEN** 系统记录每图请求合同哈希、运行配置与成本证据（逐任务求和口径）
- **AND** 请求合同与冻结配置保持一致，失败/部分/证据缺失计数与差异全部可解释

#### Scenario: 预算与门禁先到即停

- **WHEN** 真实调用达到图片数（≤45，含 5 张重试余量）、attempt（≤120）或人民币硬上限（≤1.00 元）任一门禁
- **THEN** 系统自动停止投放新请求，保留在飞结果与证据，不透支后继续

### Requirement: 复验主判据 SHALL 为用户二元标注并保持红线自动判据

系统 SHALL 以用户逐样本二元标注为复验主判据：标注页面 SHALL 同显原图与冻结候选输出，SHALL 隐藏规则提示、历史答案与置信度，SHALL 逐样本记录 `human_image_vs_frozen_output`。红线自动判据 SHALL 保持不变：自动「错误接受面积 = 0」且「原图外有效面积 = 0」。用户标注未完成时，相关任务 SHALL 保持 `blocked` 并报告，SHALL NOT 静默通过。

#### Scenario: 标注页面同显与隐藏

- **WHEN** 用户对某样本进行二元标注
- **THEN** 页面同时显示该样本原图与冻结候选输出
- **AND** 页面不显示规则提示、历史答案或置信度等可能引导判断的信息

#### Scenario: 标注未完成

- **WHEN** 用户尚未完成 40 张独立集逐样本标注
- **THEN** 依赖该标注的任务保持 `blocked` 并如实报告进度
- **AND** 系统不基于部分标注或自动推定产出复验结论

### Requirement: 复验 SHALL 披露指标、差异清单并产出三选一结论

系统 SHALL 将 §10.3 指标在独立集上的测量值作为披露数字记录，SHALL NOT 静默套用原门槛作为通过/拒绝结论；差异 SHALL 逐样本列入可解释清单（含服务端非确定性漂移形态）。系统 SHALL 产出三选一结论：PASS（复验达标，可关门）／FAIL（复验不达标，维持 REJECT）／EVIDENCE_INSUFFICIENT（证据不足以判定，不追加调用）。结论 SHALL 显式引用 RV-OCRNEXT-C-01#F1（「双轮一致的确定性误关联」无规则防护）与 #N5（15 张范围外样本仍留 accepted claim，无真值可判、不入分母）两项已知限制。

#### Scenario: 指标披露不套用原门槛

- **WHEN** 独立集上的 §10.3 指标测量值低于原候选门槛
- **THEN** 系统将测量值作为披露数字记录在画像报告与差异清单中
- **AND** 通过/拒绝结论由用户依据证据裁定，不由原门槛数值单独决定

#### Scenario: 三选一结论与限制引用

- **WHEN** 复验汇总完成
- **THEN** 系统产出 PASS／FAIL／EVIDENCE_INSUFFICIENT 之一并记录判定依据
- **AND** 结论文档显式引用 #F1 与 #N5 两项已知限制及其对结论的边界作用

### Requirement: 复验结论 SHALL 更新 EXTFP4 状态标注而不构成授权

复验完成后，`ocr-field-evaluation` spec 的状态标注 SHALL 随 delta 更新以反映复验结果；PASS 仅解除验收阻塞，SHALL NOT 构成 EXTFP4 生产批次（208,075 张）或估值接入的授权，后者 SHALL 另行提案。

#### Scenario: 复验 PASS 后的状态标注

- **WHEN** 复验结论为 PASS
- **THEN** 状态标注更新为反映复验通过的事实
- **AND** 同时注明 EXTFP4 与估值接入仍须另行提案授权，本 change 不构成授权

#### Scenario: 复验 FAIL 或证据不足

- **WHEN** 复验结论为 FAIL 或 EVIDENCE_INSUFFICIENT
- **THEN** 状态标注维持 EXTFP4「不支持（阻塞项）」并记录本次复验结果
- **AND** 不产出任何影子运行、估值接入或生产批次结论

### Requirement: EXTFP4 生产批次 SHALL 以提案与逐批授权推进且不扩大边界

change extfp4-production-batch 提出首个 EXTFP4 生产批次（「冻结支持小区 ∩ 近 12 个月成交」子集，非全量 208,075）后，`ocr-field-evaluation` 的状态标注 SHALL 更新为「首个生产批次已提案」口径：真实下载与付费 OCR 调用 SHALL 仅在该 change 获用户实施授权后执行，且每批次投放前 SHALL 履行批次确认（固定 commit、manifest SHA256、任务数与金额上限）。该 change SHALL NOT 构成估值接入、数据冻结（EXTFP5）、数据价值验证或 `ordinary_residential_all` 全量批次的授权；批次结论 SHALL NOT 以「复验 PASS」宣称 H3/H9 类门槛已获正式认证，SHALL 显式引用既有四项已知限制。

#### Scenario: 未获实施授权

- **WHEN** 该 change 仅有提案工件（proposal/design/specs/tasks），用户尚未授权实施
- **THEN** 系统不执行任何真实下载或付费 OCR 调用
- **AND** `frozen_extfp4_config` 保持「仅记录、未进入生产路径」状态

#### Scenario: 状态标注更新与边界保持

- **WHEN** 该 change 获用户实施授权并归档同步
- **THEN** 状态标注反映首个 EXTFP4 生产批次的提案与逐批批次确认口径
- **AND** 估值接入与 EXTFP5 数据冻结仍标注为须另行提案，全量批次仍须单独授权

### Requirement: 外部数据录入线状态 SHALL 收口为已冻结待接入

EXTFP5 数据冻结完成后，外部数据录入线（EXTFP0—EXTFP5）状态 SHALL 标注为「已冻结 v1，待估值接入验证」；仅完成或修订 floorplan-value-validation 的 planning artifacts 时，状态 SHALL 补充为「价值验证方案已修订，待实施」，SHALL NOT 标注为验证已执行。输入重建或第一轮对照实际启动后，状态方可更新为「价值验证进行中」；第一轮结论、第二轮条件是否触发与第二轮结论 SHALL 分别记录。任何验证或优化状态 SHALL NOT 构成正式估值接入授权，接入仍须另案提案并经用户确认正式基线。

#### Scenario: 规划修订不冒充执行
- **WHEN** floorplan-value-validation 仅完成 proposal/design/specs/tasks 修订且尚未 apply
- **THEN** 状态标注为「已冻结 v1；价值验证方案已修订，待实施」
- **AND** 不出现“验证进行中”“验证已完成”或等效执行结论

#### Scenario: 状态收口
- **WHEN** EXTFP5 冻结版本 `lianjia_ext_v1_20260830` 已发布且价值验证尚未实施
- **THEN** 外部数据录入线保持「已冻结 v1，待估值接入验证」并可补充“方案已修订，待实施”
- **AND** 标注同步注明估值接入须另行提案

#### Scenario: 第一轮实际执行后更新状态
- **WHEN** 冻结输入重建门禁通过且第一轮对照回放实际启动
- **THEN** 状态标注更新为「价值验证进行中（第一轮：户型相似度）」
- **AND** 标注同步注明不构成正式估值接入授权

#### Scenario: 第二轮状态独立记录
- **WHEN** 第一轮结束并判断第二轮门禁
- **THEN** 系统分别记录第一轮三选一结论以及第二轮“未触发/进行中/已完成”状态
- **AND** 第二轮面积价格调整仍保持离线优化项，不自动进入正式估值

## 状态标注（2026-08-30 采用基线；2026-08-30 change ocr-concurrency-optimization 验证更新；2026-08-30 change ocr-independent-reverification 复验更新；2026-08-30 change extfp4-production-batch 生产批次更新；2026-08-30 change extfp5-data-freeze 冻结收口更新；2026-08-30 change floorplan-value-validation 价值验证收口更新）

- 字段安全修复（去重/数字占用/隔离）：**已验证**（OCRNEXT-C accepted，FC=0）
- 离线回放工具与资产：**已验证**（C 双轮回放与门禁已运行）
- 有界并发调度实现：**已验证**（OCRNEXT-B accepted；并发 4 仅诊断）
- 并发吞吐结论与候选并发选定：**已验证为候选并发 8**（change ocr-concurrency-optimization，2026-08-30：20 张子集三档真实对照，8 并发三项稳定性判据全部通过、16 并发无额外吞吐且失败更多；OCRNEXT-D 数据引用保持「已执行未验收」口径）
- EXTFP3-H REJECT 遗留独立集复验：**PASS**（change ocr-independent-reverification，2026-08-30 用户裁定）：40 张独立集（保守口径候选池 87）并发 8 真实运行 40/40 成功；用户二元标注 40/40 一致；红线（错误接受面积/原图外面积）0；可计算指标 H1/H2/H8/H10 全 100%；跨运行差异 2 张全为服务端非确定性漂移、黄金草案差异 3 张为草案遗漏（披露）；H3–H7（无已确认黄金）与 H9（单次运行）在独立集上不可计算，如实披露。**复验未发现 REJECT 所针对的错误接受/原图外面积/明显精度问题，REJECT 遗留收口闭合。**
- EXTFP4 授权生产批次：**首个生产批次已提案并执行**（change extfp4-production-batch，2026-08-30）：生产选择清单「冻结支持小区 ∩ 近 12 个月」229 记录/229 资产（基线 208,075 锚点核对一致，manifest SHA256 `59b5adc6…`），批次确认（commit b160098、金额上限 ¥0.30）后真实下载 229/229、OCR 229/229 SUCCEEDED（并发 8，成本 ¥0.2441，逐任务求和口径）、转录 1,853 条标注；完整性门禁曾因 15 组重复图片资产（30 资产，同 image_sha → 同 ocr_task_id）响应文件覆盖未通过并停批报告，用户裁定**豁免并披露**后关闭（batch_01_integrity_exemption.json）。范围外样本按 out_of_scope 标注隔离（登记表当前为空）。独立集复验 PASS 与本批次 **不构成估值接入授权**；`ordinary_residential_all` 全量（208,075/208,081）、扩窗/扩小区批次、EXTFP5 数据冻结与估值接入均须另行提案
- EXTFP5 数据冻结：**已冻结 v1，待估值接入验证**（change extfp5-data-freeze，2026-08-30）：首个可消费数据版本 `lianjia_ext_v1_20260830` 已登记（staged 成交/普通住宅表 + 生产批次 229 资产链 + 10 张调试/300 张验收集及验证子集资产按现状登记；OCRNEXT-D 数据保持「已执行未验收」口径）；冻结运行清单、版本指针与限制披露（完整性豁免 15 组/30 资产、转录状态口径 CONFLICT 425/ROOM_ONLY 40/NEEDS_REVIEW 2、示例小区130 0 命中、verify SUGGESTION ①②、KL-1..KL-4）已入版本说明；冻结后版本只读，后续增量批次另立新版本指针。**冻结不构成估值接入授权**，估值接入价值验证方案另案提案
- EXTFP 估值价值验证：**第一轮已完成（EVIDENCE_INSUFFICIENT），第二轮未触发**（change floorplan-value-validation，2026-08-30）：冻结输入重建门禁通过（229 资产链 / 214 唯一 source_record_id / 1,853 标注及四状态计数，派生表哈希跨运行一致）；第一轮完整比较法双组对照在 64 个确认目标上运行一次，零差值占比 0.9219 > 冻结上限 0.5，判别力不足 → `EVIDENCE_INSUFFICIENT`（不宣称有/无增量价值）；第二轮面积价格调整按门禁未触发。可比侧 OCR 覆盖为 0、Excel 户型可解析率低；同一确认集不得复用于肯定性结论，扩样本须另案授权。全程零网络零付费，冻结 v1 前后哈希不变。**本验证不构成估值接入授权，亦不构成任何正式精度认证**；接入 `valuation-comparable-core` 须另建 change 并经用户确认正式基线
