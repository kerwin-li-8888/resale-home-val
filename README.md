# resale-home-val — 可解释的二手房比较法估值引擎

[English](README.en.md) | 简体中文

> Transparent, evidence-chained comparable-sales valuation for resale
> residential properties in a bounded urban submarket. 会拒绝错误精确的估值系统。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](pyproject.toml)

> 命名说明：仓库名 `resale-home-val`；Python 包与 CLI 名为 `compsval`（comparable-sales，历史沿用）。

## 这是什么

`resale-home-val` 是一个市场比较法（ comparable-sales / sales-comparison approach ）二手房估值引擎。它对目标板块内一套普通二手商品住宅，在明确估值时点与数据截点下，输出：

1. **估值中心值**在哪里；
2. **合理区间**有多宽；
3. 这一结果**有多可信**（高/中/低/不足 + 分项理由）；
4. **哪些案例与判断**导致了这个结果。

每次估值只能处于四种状态之一：`正式估值`、`参考估值`、`信息不足`、`不适用`。系统宁可说"不知道"，也不输出伪装成精确答案的数字。它不是黑盒 AVM：方法论是估价行业的比较法，人工复核是必经环节，证据链全程可追溯。

## 核心特性

- **可比案例逐级放宽**：同小区同类产品起步，一次只放宽一个主要条件，保留完整放宽轨迹；
- **时间修正**：只用估值时点之前可获得的数据计算，无证据不强修正；
- **抗异常值汇总**：相似度加权中位数 + 加权分位区间，有效样本量反映权重集中度；
- **区间校准**：区间宽度同时反映案例离散度、样本量、新旧、缺失与历史回放误差；
- **单调性约束**：数据越弱 → 区间越宽、可信度越低；
- **人工复核留痕**：自动结果不可被静默覆盖，修改前后结果与理由全程留档；
- **时间外回放**：滚动历史回放 + 简单基准对比 + 分组误差，随机拆分不能替代；
- **证据链**：不可变原始快照、来源清单、字段口径、缺失纪律（未知 ≠ 0）。

## 快速开始

要求：Python 3.12+、[uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/kerwin-li-8888/resale-home-val.git
cd resale-home-val/03-估值引擎
uv sync
uv run pytest              # 全量离线测试
uv run compsval version    # CLI 冒烟
```

合成样例数据（虚构小区"春晖里"，端到端演示标准化契约层→估值链→报告）见
`examples/`，全程离线、结果可复现。

## 仓库结构

```text
resale-home-val/
├─ 03-估值引擎/            # 引擎工程目录
│   ├─ src/compsval/       # 引擎源码（contract/entities/ingest/valuation/reporting）
│   ├─ tests/              # 全量离线测试
│   ├─ UPSTREAM.md         # 上游来源逐文件登记（开源合规审计）
│   └─ upstream/           # 上游 LICENSE 原文存档
├─ openspec/
│   ├─ specs/              # 当前行为权威（13 个能力规格 + 开源发布门禁）
│   └─ adopt/              # OpenSpec 治理采用记录
├─ LICENSE / NOTICE        # MIT + 上游归属声明
└─ ADAPTATION.md           # 移植到你所在城市的改造指南
```

## 数据与合规声明

- 本仓库**不包含任何平台抓取数据**（无成交记录、无房源快照、无小区清单）；
- `examples/` 中的全部数据为**合成样例**（虚构小区、占位 ID），仅用于演示数据契约与估值流程；
- 请自行确保数据获取与使用符合目标平台服务条款、`robots` 协议及所在司法辖区法律；
- 上游工程骨架来自 [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure)（MIT），归属见 [NOTICE](NOTICE) 与 [UPSTREAM.md](03-估值引擎/UPSTREAM.md)。

## 免责声明

本项目的输出是**决策参考**，不构成法定房地产估价报告，不构成投资建议，不能替代现场看房、产权核查或持牌估价师。作者按 "AS IS" 提供本软件，不对任何估值结果承担责任。

## 治理

本项目采用 [OpenSpec](https://github.com/Fission-AI/OpenSpec) 规范驱动治理：
`openspec/specs/` 是当前行为的唯一权威；行为变更必须通过 change 流程提出、验证并归档。
改造到新城市时请先阅读 [ADAPTATION.md](ADAPTATION.md)。

## 致谢

- [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure) — 工程底座（MIT）
- [mcp-imo](https://github.com/zedd75/mcp-imo)、[open-comps](https://github.com/property-hackers/open-comps)、[Cook County model-res-avm](https://github.com/ccao-data/model-res-avm) — 方法论参考
- 方法依据：《房地产估价规范》GB/T 50291-2015、IVS、IAAO AVM 标准、Fannie Mae 可比成交指引

## License

[MIT](LICENSE)（上游归属见 [NOTICE](NOTICE)）
