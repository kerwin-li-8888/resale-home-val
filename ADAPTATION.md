# ADAPTATION.md — 把 resale-home-val 改造到你所在的城市

本引擎与任何具体城市解耦：地域相关的只有"数据层"（原始快照、小区普查、范围
政策）。改造三步：接数据 → 建普查 → 按治理流程改行为；要使用 v2 量化模型层，
再补一步：建量化研究合同。

## 1. 接入你自己的数据源

引擎不抓取任何平台数据，只消费**你依法取得的来源快照**（HTML/CSV/文本）。

1. 在 `src/gz_property_valuation/ingest/parsers/` 参照 `fang_esf.py` /
   `lianjia.py` 新增解析器：输入 = 原始快照文件，输出 = 标准化记录（成交与
   挂牌严格分离）；
2. 在 `contract/registry.py` 登记来源（来源 ID、入口 URL、口径、取得方式、
   可重复性）；
3. 原始快照走不可变写入（`ingest/snapshots.py` 的原子写入原语），manifest 记录
   来源、抓取时间、查询条件与指纹；重跑解析不得改变原始证据；
4. 缺失纪律：数值未知用 `None`，文本未知用显式 `UNKNOWN`，不得用 0 代替未知。

验收：`uv run pytest tests/test_import_file.py tests/test_snapshots.py` 通过，
且重跑解析后原始快照哈希不变。

## 2. 重建你所在城市的小区普查与范围政策

`examples/` 的合成样例展示数据形状；真实普查按以下顺序自建：

1. **小区名录**：确定目标板块的机器可执行小区清单（标准名 + 别名 + 竞争关系）；
2. **别名表**：参照 `entities/alias.py` / `alias_census.py`，同一对象可合并、
   冲突不静默覆盖（一致/待定/冲突三态终态）；
3. **普查**：参照 `openspec/specs/community-census/spec.md` 的口径统计
   3/6/12 个月有效案例数（`cases_12m`），产出 scope policy（纳入/参考/拒绝
   名单）；
4. **可行性门槛**：案例密度不足的小区只允许"参考估值"或拒绝正式估值——
   这是特性，不是缺陷；
5. **范围参数化**：研究范围（目标区县）是配置/请求参数，默认值为虚构的
   云溪区；回退链层级名为中性的 community → block → `district`。

验收：普查产物通过 `tests/test_alias_census.py`、`tests/test_scope.py`
同构检查。

## 3. 用 OpenSpec change 修改系统行为

`openspec/specs/` 是当前行为的唯一权威。任何行为变更（估值规则、口径、状态
机、量化层治理门）必须走 change 流程，不得直接改代码了事：

```bash
openspec new change "add-your-change"      # 提案（proposal/design/tasks/specs delta）
openspec status  --change "add-your-change"
openspec validate add-your-change --strict # 工件校验
openspec archive add-your-change           # 验证通过后归档，delta 并入主 spec
```

- 每个 change 的 spec delta 必须带可测试的 WHEN/THEN 场景；
- 实施完成以测试与证据为准，"没有证据不得把任务状态改为完成"；
- `archive` 不等于正式基线：是否启用新行为，由你（仓库主人）按验收门槛确认。

## 4. 校验你的移植

移植完成后，最小验证闭环：

1. 全量测试：`uv run pytest`；
2. 时间外回放：用你城市的历史数据做滚动回放，与"同小区近期可比案例简单
   中位数"基准对比——复杂规则只有稳定超过基准才有采用价值；
3. 区间校准：检查覆盖率与区间宽度，防"靠无限放宽换覆盖率"；
4. 影子运行：真实工作流试运行一段，误差可追踪后再启用正式输出。

## 5. 启用 v2 量化模型层（phase2 移植指南）

量化模型层＝B0 基准 / M1 可解释模型 / GBM 挑战者的受控比较与治理链。移植时
按以下顺序自建（全部行为要求见 `openspec/specs/phase2-*/spec.md`）：

1. **数据合同**（`phase2-data-contracts`）：固定用户自备数据的来源指针与哈希、
   清洗口径、时间切分与信息截点；特征字典逐字段登记"原字段/计算方式/粒度/
   可用时点/缺失处理/是否用标签/预测时可得性"七项元信息；预注册独立测试接收
   规则，不得按表现挑样本；
2. **基线与模型对照**（`phase2-baseline-comparisons`、
   `phase2-nonlinear-comparisons`）：先建 B0（community→block→district 逐级
   回退的近期中位基准）与 M1（FoldEncoder＋正则回归），再以有界预算、折内
   拟合边界引入 CatBoost/LightGBM 挑战者；两用途（独立报价/辅助价差）分列
   结论，无稳定收益时如实登记无增益；
3. **冻结与候选**（`phase2-candidate-freeze`）：选中候选以显式版本序列化为
   推理资产，单套/批量/重放三路径逐位一致；缺失输入走登记过的降级路径；
   区间校准段保持隔离；
4. **候选运维**（`phase2-candidate-ops`）：请求映射与六类边界情形预定处理、
   成套版本指纹输出、追加式影子记录；候选输出不接正式链路；
5. **有条件正式启用**（`phase2-formal-release`）：正式出价必须逐套通过资格门
   （人群、时效、支持度、分歧上限），发布记录默认不存在＝未发布，
   fail-closed——先跑通
   `examples/synthetic_phase2_demo.py` 的第五段（未发布态发布门禁演示），
   再考虑你自己的启用流程；
6. **独立检验**（`phase2-independent-testing`）：任何"通过/启用"结论必须来自
   事前登记口径下的新成交检验，模型、门槛零改动。

最小起步路径：`uv run python examples/synthetic_phase2_demo.py`——一个脚本
端到端跑完造数 → 训练评估 → 冻结 → 候选调用 → 发布门禁，把其中的合成数据
换成你自己的合同产物即可逐步替换。
