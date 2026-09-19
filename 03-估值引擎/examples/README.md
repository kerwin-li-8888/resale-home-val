# examples 目录说明

本目录存放开源发布版的演示材料与演示产物。

## synthetic_phase2_demo.py（合成数据端到端演示）

在仓库根（`03-估值引擎/`）下执行：

```bash
uv run python examples/synthetic_phase2_demo.py
```

- **纯离线**：不访问网络，全部数据由固定随机种子（`SEED=20260918`）合成；
- **确定性**：连续两次运行的关键产物（stdout 摘要与
  `phase2_demo/synthetic_demo/demo_summary.json`）逐字段一致；
- **秒级完成**：合成规模约 690 行成交记录，B0/M1 均为闭式/中位口径，无迭代训练。

### 五段演示内容

1. **固定种子造数**：云溪区 8 个虚构小区、3 个虚构板块的合成成交主表
   （含缺失总楼层的降级样本，schema 与 S1 特征层一致）；
2. **B0/M1 训练评估**：B0 走 `baselines`（community→block→district 回退链＋回退
   比例披露）；M1 走 `models`（FoldEncoder＋闭式 Ridge，log 域训练）；
3. **冻结与版本指纹**：五组件资产＋正式执行代码 → `formal_binding.compose_nine`
   九件组合指纹；预测/标签记录落追加式哈希链并 `verify_chain` 验证；
4. **候选调用含降级路径**：三个演示请求分别命中 B0 的 community/block/district
   层；M1-B0 分歧超阈值（0.15，与引擎 `candidate_inference` 同源）仅标注不仲裁；
   输出一律为候选参考价，不产出正式价；
5. **未发布态发布门禁演示**：无发布记录 → `release_record` fail-closed
   （`RC_RECORD_MISSING`）→ 状态机 `version_disabled` 无正式价 → `formal_gate`
   GATE-09（`FG09_RELEASE_CHECK_MISSING`）→ 停止开关默认停止。
   **本演示不执行任何正式启用操作**，与「暂不可正式启用」业务结论一致。

### 产物位置

```
examples/phase2_demo/synthetic_demo/
├── demo_summary.json        # 确定性结果摘要（两次运行逐字段一致）
├── formal-records.jsonl     # 追加式哈希链示例（预测＋标签记录）
└── empty-ops-state/         # 空运行时目录（演示停止开关默认停止＝fail-closed）
```

### 与生产注册链的关系

本演示在模块级复用引擎真实代码，但**不**构建生产注册链资产（`candidate_ops`
的 S1/S3/S4B/S4C run 冻结产物与登记指纹，其默认登记值与真实训练 run 绑定）。
演示中的组合指纹仅由合成资产与当前代码计算，用于演示指纹与哈希链机制本身。

## demo_raw/（占位）

部分基线集成测试引用 `examples/demo_raw/` 下的演示证据文件；开源发布版未附带
这些文件，相关测试会自动跳过（skipped）。
