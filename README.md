# resale-home-val — 可解释的二手房比较法估值引擎（v2 正式版）

[English](README.en.md) | 简体中文

> Transparent, evidence-chained comparable-sales valuation for resale
> residential properties in a bounded urban submarket — now with a governed
> quantified model layer. 会拒绝错误精确的估值系统。

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](03-估值引擎/pyproject.toml)

> 命名说明：仓库名 `resale-home-val`。v1（测试版）的 Python 包与 CLI 名为
> `compsval`（comparable-sales，历史沿用）；**v2（本版，正式版）起主包退役
> `compsval`，更名为 `gz_property_valuation`，CLI 名为 `gzv`**。`compsval`
> 仅作历史名保留于本注记与上游记录中。

## 这是什么

`resale-home-val` 是一个市场比较法（comparable-sales / sales-comparison
approach）二手房估值引擎。v2 正式版由两部分构成：

1. **比较法底座**（v1 测试版演进）：对目标板块内一套普通二手商品住宅，在明确
   估值时点与数据截点下，输出估值中心值、合理区间、可信度（高/中/低/不足＋分项
   理由）与导致该结果的案例与判断。每次估值只处于四种状态之一：`正式估值`、
   `参考估值`、`信息不足`、`不适用`——宁可说"不知道"，也不输出伪装成精确答案
   的数字；
2. **量化模型层**（v2 新增）：在同一数据合同与同一套治理纪律下，B0 朴素基准、
   M1 可解释模型与 GBM 挑战者（CatBoost/LightGBM）同台受控比较；冻结与版本
   指纹、逐级回退与缺失降级、候选调用与影子记录、停止开关与发布门禁全部
   fail-closed——没有生效的发布记录，任何路径都不产出正式价格。

它不是黑盒 AVM：方法论是估价行业的比较法，量化层只做候选参考与对照证据，
人工复核是必经环节，证据链全程可追溯。

## 核心特性

比较法底座：

- **可比案例逐级放宽**：同小区同类产品起步，一次只放宽一个主要条件，保留完整
  放宽轨迹；
- **时间修正**：只用估值时点之前可获得的数据计算，无证据不强修正；
- **抗异常值汇总**：相似度加权中位数＋加权分位区间，有效样本量反映权重集中度；
- **区间校准**：区间宽度同时反映案例离散度、样本量、新旧、缺失与历史回放误差；
- **单调性约束**：数据越弱 → 区间越宽、可信度越低；
- **人工复核留痕**：自动结果不可被静默覆盖，修改前后结果与理由全程留档；
- **时间外回放**：滚动历史回放＋简单基准对比＋分组误差，随机拆分不能替代；
- **证据链**：不可变原始快照、来源清单、字段口径、缺失纪律（未知 ≠ 0）。

量化模型层（v2）：

- **三模型同台**：B0（community→block→district 逐级回退的近期中位基准）、
  M1（FoldEncoder＋正则回归的可解释模型）、GBM 挑战者（CatBoost/LightGBM）
  在同一信息截点与同一指标口径下受控比较，两用途（独立报价/辅助价差）分列
  结论，复杂模型无稳定收益时如实给出无增益结论；
- **冻结与版本指纹**：模型/特征/市场资产/校准/协调策略五组件＋代码指纹成套
  绑定，错代混用一律拒绝；预测与标签记录落追加式哈希链，只追加、不倒改；
- **缺失降级与回退**：总楼层缺失等输入缺陷走登记过的确定降级路径，不静默外推；
  未知小区冷启动行为确定、覆盖率登记；
- **候选调用**：候选输出永不接入正式链路——M1-B0 分歧护栏、A1 冲突标注、
  板块口径翻译与歧义披露全部随结果显式输出；
- **影子记录**：每次请求输入快照与结果冻结保存，成交标签到来只追加对拍记录；
- **停止开关与发布门禁 fail-closed**：发布记录缺失/过期/指纹错配 → 无正式价
  （`version_disabled`）；整体停止开关关闭后无任何残留正式输出；资格门逐套
  校验人群、时效、支持度与分歧上限，不通过即输出具体原因。

## 快速开始

要求：Python 3.12+、[uv](https://docs.astral.sh/uv/)。

```bash
git clone https://github.com/kerwin-li-8888/resale-home-val.git
cd resale-home-val/03-估值引擎
uv sync
uv run pytest              # 全量离线测试
uv run gzv version         # CLI 冒烟
uv run python examples/synthetic_phase2_demo.py   # 合成样例端到端演示
```

合成样例（虚构小区、固定种子、全程离线、两次运行结果一致）演示量化模型层
全链路：合成造数 → B0/M1 训练评估 → 冻结与版本指纹 → 候选调用含降级路径 →
未发布态发布门禁演示。详见 [03-估值引擎/examples/README.md](03-估值引擎/examples/README.md)。

## 研究范围

引擎对目标城市与区县不做任何硬编码：研究范围默认为**可配置的虚构值**
（示例城市·云溪区，回退层名为中性的 `district` 层）。把它接到你自己的城市，
只需按 [ADAPTATION.md](ADAPTATION.md) 提供你依法取得的数据与普查产物。

## 仓库结构

```text
resale-home-val/
├─ 03-估值引擎/                     # 引擎工程目录
│   ├─ src/gz_property_valuation/   # 引擎源码（contract/entities/ingest/valuation/phase2/reporting）
│   ├─ tests/                       # 全量离线测试
│   ├─ examples/                    # 合成样例与演示产物
│   └─ upstream/                    # 上游 LICENSE 原文存档
├─ openspec/
│   ├─ specs/                       # 当前行为权威（比较法底座 13 规格＋开源发布门禁＋量化模型层 10 规格）
│   ├─ schemas/                     # OpenSpec 工作流模板
│   └─ adopt/                       # OpenSpec 治理采用记录
├─ LICENSE / NOTICE / UPSTREAM.md   # MIT ＋ 上游归属声明与来源登记
└─ ADAPTATION.md                    # 移植到你所在城市的改造指南
```

## 数据与合规声明

- 本仓库**不包含任何平台抓取数据**（无成交记录、无房源快照、无小区清单）；
- `examples/` 中的全部数据为**合成样例**（虚构小区、占位 ID），仅用于演示数据
  契约、量化模型层流程与治理机制；
- 请自行确保数据获取与使用符合目标平台服务条款、`robots` 协议及所在司法辖区
  法律；
- 上游工程骨架来自 [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure)（MIT），
  归属见 [NOTICE](NOTICE) 与 [UPSTREAM.md](UPSTREAM.md)。

## 免责声明

本项目的输出是**决策参考**：不构成法定房地产估价报告，不构成投资建议，不能
替代现场看房、产权核查或持牌估价师。**量化模型层的输出目前定位为研究/候选
参考，正式启用流程尚未开启**——发布门禁默认 fail-closed，任何路径都不会输出
正式价格。作者按 "AS IS" 提供本软件，不对任何估值结果承担责任。

## 治理

本项目采用 [OpenSpec](https://github.com/Fission-AI/OpenSpec) 规范驱动治理：
`openspec/specs/` 是当前行为的唯一权威；行为变更必须通过 change 流程提出、
验证并归档。改造到新城市时请先阅读 [ADAPTATION.md](ADAPTATION.md)。

## 致谢

- [Philly Fair Measure](https://github.com/nickhand/philly-fair-measure) — 工程底座（MIT）
- [mcp-imo](https://github.com/zedd75/mcp-imo)、[open-comps](https://github.com/property-hackers/open-comps)、[Cook County model-res-avm](https://github.com/ccao-data/model-res-avm) — 方法论参考
- 方法依据：《房地产估价规范》GB/T 50291-2015、IVS、IAAO AVM 标准、Fannie Mae 可比成交指引

## License

[MIT](LICENSE)（上游归属见 [NOTICE](NOTICE)）
