## Why

<!-- Explain the motivation for this change. What problem does this solve? Why now? -->
<!-- Why / What Changes 用功能语言撰写，避免代码术语 -->

## What Changes

<!-- Describe what will change. Be specific about new capabilities, modifications, or removals. -->

## Capabilities

### New Capabilities
<!-- Capabilities being introduced. Use kebab-case for path segments you introduce
     (e.g., user-auth or identity/user-auth) that follow the project's existing
     spec organization. Each creates specs/<capability-path>/spec.md. -->
- `<capability-path>`: <brief description of what this capability covers>

### Modified Capabilities
<!-- Existing capabilities whose REQUIREMENTS are changing (not just implementation).
     Only list here if spec-level behavior changes. Each needs a delta spec file.
     Use the exact existing path under openspec/specs/. Leave empty if no requirement
     changes. A change with no capabilities at all (pure refactor, tooling, docs)
     must set `skip_specs: true` in its .openspec.yaml - openspec validate rejects
     a zero-delta change without that marker. Do not invent a requirement just to
     satisfy validation. -->
- `<existing-capability-path>`: <what requirement is changing>

## Impact

<!-- Affected code, APIs, dependencies, systems -->

## Cost Estimate

### 模型处理时间范围
<!-- 区间式，禁单点，例：X—Y 小时。模型处理时间 = 执行/独立审核/返工会话的运行时长之和，
     不含人工确认段与等待段；人工确认与等待不计入本节。 -->

### 本机计算量估算
<!-- 凡含本机执行的计算任务，MUST 以计算式登记：数量 × 单位耗时 ÷ 并行度 = 总时长。
     总时长口径为连续计算墙钟时间（机器视角），与上方模型处理时间口径互不吞并、各自登记；
     仅以「分钟级 / 小时级」等结论词作答视为不合规。
     单位耗时 MUST 注明来源（实测 / 外推 / 假设）；无实测来源且按任何合理假设外推总量
     可能超过 1 小时时，MUST 先执行有代表性的抽样实测（记录样本量与实测区间）再填数，
     不得以纯假设值直接登记。
     预计本机连续计算总时长 > 1 小时 MUST 将「本机执行 / 云跑」作为显式决策点提请用户
     裁决，并核对登记判据 J12（本机长时计算，非高危）；选云跑 MUST 另按 J1（直接现金
     支出）核对，两判据同时命中按评级与审核路径映射升档。
     无本机计算任务的提案填一行「无本机计算任务」，本小节不得缺失。 -->

### 难度评级
<!-- 1—5 分制 + 1—2 句评级理由（技术不确定性、外部依赖数量、影响面）。
难度基准：1=成熟方案直接套用、无外部依赖；2=少量适配；3=需集成或涉及 1—2 项外部依赖；
4=跨系统改动或较强架构影响；5=新架构模式或存在架构级不确定性。
难度与时间相互独立：难度反映不确定性，时间反映投入，不得互相倒推。 -->

## 重要性评级

- **评级**：<!-- 低 / 中 / 高 -->
- **判定理由**：<!-- 对照下方客观判据清单逐条说明命中与否；J1/J3/J7 按 What Changes 与 Impact 直接判断 -->
- **建议审核路径**：<!-- 按下方映射填写 -->

### 客观判据清单（命中任一条即强制升级独立审核）

<!-- 清单正文内嵌于模板，保证持久生效与结构保证；逐条核对并在判定理由中说明命中与否 -->

| # | 判据 | 阈值/条件 |
|---|---|---|
| J1 | 直接现金支出 | 预计出现任何直接现金支出（> 0 元，含一次性付费 API/云服务调用），按 What Changes / Impact 判断 |
| J2 | 外部付费且成本不确定 | change 涉及外部付费服务调用且成本不确定（UNKNOWN） |
| J3 | 外部依赖数量 | 新增外部依赖（软件、服务、账号、权限）达到 2 项及以上，按 What Changes / Impact 判断 |
| J4 | 跨系统影响 | 改动影响 2 个及以上系统/仓库，或触及治理分发面（openspec/schemas/**、openspec/config.yaml、01-模板/OpenSpec提案规则模板.yaml） |
| J5 | 授权边界外文件 | 需要修改提案授权范围之外的文件才能完成（高危） |
| J6 | 治理语义变更 | 改变治理规则的状态机、强制审核门、失败熔断或项目采用机制（高危） |
| J7 | 不可回滚 | 变更无明确、可执行的回滚路径，按整体方案的 git 可回滚性判断 |
| J8 | 敏感数据 | 触及密钥、凭据、个人数据或权限边界（高危） |
| J9 | 规模超限 | 实际模型处理时间超过 Cost Estimate 上限且无法通过拆分保持简化 |
| J10 | 架构级不确定性 | 难度评级 = 5，或存在无法在规划期消除的架构级不确定性 |
| J11 | 审核熔断 | 同一任务第三次独立审核失败即熔断停机（高危） |
| J12 | 本机长时计算 | 预计本机连续计算 > 1 小时 → 强制升级，非高危；选云跑另按 J1 判断 |

### 评级与审核路径映射

<!-- 审1 = 提案校准（用户确认大原则即授权，随后自动拉起，按结果自改，最多 3 次）；审2 = 验收审（执行完成、归档前，同样自动拉起最多 3 次；高档两道门均不自动化，流程仅提醒）。模型档位：fast（轻量档）/ strong（强推理档），审核档位不得低于执行档位；治理只定义档位，不绑定具体型号。 -->

- **低**：未命中任何判据 → 用户确认大原则即授权 → 审1 校准（fast 档可用，按结果自改最多 3 次）→ 执行 → 审2（提醒式：发现 MAJOR 记录并通知用户，由用户在归档确认时决定返工或接受；可用 fast 档）
- **中**：命中任一判据（未命中高危条目 J5/J6/J8/J11）→ 授权同低档 → 审1 校准 + 审2（必须 strong 档；审2 阻断式：存在未解决 MAJOR 即不归档）
- **高**：命中 2 条及以上，或命中任一高危条目（J5/J6/J8/J11）→ 审1、审2 均不自动化：流程仅在提案完成后、执行完成后各输出一次提醒，由用户开独立会话亲自审核并将判定结果带回本会话；带回 FAIL 时修改后再次提醒（strong 档语义供用户参考）+ 用户验收

三档均无逐原子任务独立审核。审1 校准循环与审2 各自独立计数：第 3 次独立审核仍 FAIL 即熔断停机，停止自动修复，登记失败链，转用户决策（再修 / 拆任务 / 换方案 / 回滚 / 停止）。

## 确认契约

- **本次授权范围**：<!-- 本提案获批后允许实施的文件与动作边界 -->
- **停止与升级条件**：<!-- 沿用以下九条（1、2、3、5、7、8、9 原文照录；4、6 为退役语境指代替换），各 change 可在此之上具体化，不得删减：
     1 无法确定范围或基线；
     2 需要修改范围之外的文件才能完成任务；
     3 规则必须纳入具体业务内容才能继续；
     4 独立只读审核能力不可用且无替代独立会话；
     5 审核证据无法绑定固定提交；
     6 需要改变本 change 确认契约的目标、授权范围或完成判据；
     7 第三次独立审核失败；
     8 出现伪造证据、破坏历史、越界修改或其他关键发现；
     9 实际规模超过方案上限且无法通过继续拆分保持简化。 -->
- **例外上报规则**：<!-- 遇契约未覆盖情形时，中断执行并向用户上报，不得自行扩大授权 -->
- **执行方式**：apply 按本契约自主执行，任务间不等待用户输入；仅停止条件触发时中断请求确认
