# ADAPTATION.md — 把 compsval 改造到你所在的城市

本引擎与任何具体城市解耦：地域相关的只有"数据层"（原始快照、小区普查、范围政策）。
改造三步：接数据 → 建普查 → 按治理流程改行为。

## 1. 接入你自己的数据源

引擎不抓取任何平台数据，只消费**你依法取得的来源快照**（HTML/CSV/文本）。

1. 在 `src/compsval/ingest/parsers/` 参照 `fang_esf.py` / `lianjia.py` 新增解析器：
   输入 = 原始快照文件，输出 = 标准化记录（成交与挂牌严格分离）；
2. 在 `contract/registry.py` 登记来源（来源 ID、入口 URL、口径、取得方式、可重复性）；
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
   3/6/12 个月有效案例数（`cases_12m`），产出 scope policy（纳入/参考/拒绝名单）；
4. **可行性门槛**：案例密度不足的小区只允许"参考估值"或拒绝正式估值——
   这是特性，不是缺陷。

验收：普查产物通过 `tests/test_alias_census.py`、`tests/test_scope.py` 同构检查。

## 3. 用 OpenSpec change 修改系统行为

`openspec/specs/` 是当前行为的唯一权威。任何行为变更（估值规则、口径、状态机）
必须走 change 流程，不得直接改代码了事：

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
2. 时间外回放：用你城市的历史数据做滚动回放，与"同小区近期可比案例简单中位数"
   基准对比——复杂规则只有稳定超过基准才有采用价值；
3. 区间校准：检查覆盖率与区间宽度，防"靠无限放宽换覆盖率"；
4. 影子运行：真实工作流试运行一段，误差可追踪后再启用正式输出。
