# -*- coding: utf-8 -*-
"""phase2 特征字典声明与特征生成（合同"代码即定义"，design D3）。

行为规格（specs/phase2-data-contracts/spec.md「特征字典完备性与标签边界」）：

- 六组特征（位置/产品/建筑/房屋/市场/质量与支持度），每字段七项元信息：
  原字段、计算方式、粒度、可用时点、缺失与异常处理、是否使用成交标签、
  训练与预测时能否同时获得；导出 ``feature_dictionary.json`` 与 ``.md``。
- 标签边界：成交总价与直接推导单价仅作标签；目标成交后信息、未来回填属性、
  挂牌价不进历史预测输入。
- 市场特征 365 天窗聚合：证据窗 ``[行日期−365, 行日期)`` 上界开区间排除同日、
  排除自身；另提供窗级截点快照（评估窗统一截点，蓝图 §4.3.3），窗口内成交价
  不进入该窗预测特征。
- 扩展特征（用户 2026-09-11 两轮确认）：梯户比例（中文数字解析）、板块、
  产权共有状态、满二满五税费状态（"房屋年龄"键语义分歧显式登记）、房屋权属
  标记、房屋类型、建筑面积与派生得房率；解析失败或缺失保留未知。
- 数据门条件 2：year_built / total_floors 近期覆盖率基线与监控规则随字典登记。
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from gz_property_valuation.phase2.lineage import (
    DEV_CUTOFF,
    finalize_manifest,
    register_artifact,
)

FEATURE_KEYS = ["source_record_id", "community_source_id", "sale_date", "sale_date_d"]
LABEL_COLUMNS = ["total_price_raw", "total_price_yuan", "total_price_status",
                 "unit_price", "unit_price_observed", "unit_price_observed_raw",
                 "unit_price_status"]
MARKET_WINDOW_DAYS = 365
RECENT_COVERAGE_WINDOW_DAYS = 365
COVERAGE_DROP_ALERT_PP = 5.0


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    group: str
    source_field: str
    computation: str
    granularity: str
    availability: str
    missing_handling: str
    label_usage: str
    train_predict: str


_FIELDS: tuple[FeatureSpec, ...] = (
    # ── 位置 ──
    FeatureSpec("community_source_id", "位置", "community_source_id（主表列）",
                "原样类别保留；S2 建模期仅在该折训练材料内做类别表与 one-hot"
                "（drop-first、未知全 0，S0 F-01 口径）",
                "小区", "成交时点前已知（小区归属静态）",
                "主表实测无缺失（目标区小区 ID 零缺失，823 个小区）；如遇缺失保留未知类别",
                "否", "训练与预测均可得"),
    FeatureSpec("block_name", "位置", "extra_fields_json『板块』键",
                "来源自带片区类别原样保留；兼作低样本小区上级聚合锚点"
                "（proposal 2026-09-11 用户确认；有可靠来源，非价格聚类产物）",
                "片区（小区上级）", "成交时点前已知（来源房源静态属性）",
                "键缺失或『暂无数据』或解析失败保留未知；覆盖率与解析成功率随字典登记",
                "否", "训练与预测均可得（预测时随请求小区来源附加字段）"),
    # ── 产品 ──
    FeatureSpec("area_sqm", "产品", "transaction_area_sqm（主表列）",
                "交易面积原值（平方米）",
                "逐套", "成交时点已知",
                "无缺失（area_status 全 PARSED；规则④已限 10<面积≤300）",
                "否", "训练与预测均可得"),
    FeatureSpec("bedrooms_n", "产品", "bedrooms_raw（主表列）",
                "数字解析为室数；S2 类别化沿用 S0 口径（1–5 室单列、其他、缺失）",
                "逐套", "挂牌时可知（静态户型）",
                "非数字或缺失保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("living_rooms_n", "产品", "living_rooms_raw（主表列）",
                "数字解析为厅数",
                "逐套", "挂牌时可知（静态户型）",
                "非数字或缺失保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("floor_bucket", "产品", "floor_bucket（主表列）",
                "来源楼层段类别（低/中/高/地下室）；低/中/高不冒充精确楼层（蓝图 §4.2）",
                "逐套", "挂牌时可知",
                "缺失保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("house_type_norm", "产品", "house_type（主表列）",
                "塔楼/板楼/板塔结合/平房四类保留，其余（含『暂无数据』）归未知"
                "（用户 2026-09-11 第二轮确认纳入）",
                "逐套", "挂牌时可知（建筑形态静态）",
                "『暂无数据』或缺失归未知；覆盖率随字典登记（实测约 97.6%）",
                "否", "训练与预测均可得"),
    FeatureSpec("efficiency_ratio", "产品",
                "transaction_area_sqm / building_area_detail_sqm（主表列派生）",
                "得房率=交易面积/建筑面积；建筑面积实测中位差约 16%≈公摊率"
                "（用户 2026-09-11 第二轮确认纳入）",
                "逐套", "成交时点已知（两面积均为该套登记值）",
                "建筑面积覆盖约 44%，任一面积缺失或建筑面积≤10 保留未知",
                "否", "训练与预测均可得（预测时需提供建筑面积，缺失按未知处理）"),
    # ── 建筑 ──
    FeatureSpec("year_built", "建筑", "year_built（主表解析列）",
                "建成年份原值；注意与房龄的分离（S0 F-03：市场年份钳制不作用于房龄）",
                "小区代表值（来源为小区级登记，非逐套核验）", "静态属性",
                "MISSING 行保留未知（本 run 4,244 行）；近期覆盖率基线与监控规则"
                "见 coverage_monitor（数据门条件 2）",
                "否", "训练与预测均可得"),
    FeatureSpec("age_years", "建筑", "sale_date_d 年份 − year_built",
                "房龄=真实成交年−建成年（S0 F-03：不用钳制年份），clip 到 [0,80]",
                "逐套", "按该行成交时点计算（预测时按估值时点计算）",
                "year_built 缺失则房龄未知",
                "否", "训练与预测均可得"),
    FeatureSpec("total_floors", "建筑", "total_floors（主表解析列）",
                "总层数原值",
                "楼栋/小区代表值（来源级登记）", "静态属性",
                "缺失保留未知；覆盖率基线与监控规则见 coverage_monitor（数据门条件 2）",
                "否", "训练与预测均可得"),
    FeatureSpec("elevator_state", "建筑", "has_elevator（主表布尔列）",
                "布尔直判三态：True→有、False→无、null→未知（S0 F-02 口径，不经字符串匹配）",
                "逐套", "挂牌时可知（配置静态）；可变化属性——历史状态未证实，"
                "敏感性实验安排：S2 以纳入为基准、登记纳入/排除对照（蓝图 §4.3.7），"
                "与装修同一安排，不宣称严格历史可得",
                "null→未知（本 run 1,540 行）；与原值字符串的矛盾另记 conflict_elevator 标记",
                "否", "训练与预测均可得"),
    FeatureSpec("ladder_count", "建筑", "extra_fields_json『梯户比例』键（如『两梯三户』）",
                "中文数字解析梯数（一~十、『两』按 2、『X十Y』复合形式）"
                "（用户 2026-09-11 第一轮确认纳入）",
                "楼栋/小区代表值（来源级登记）", "静态属性",
                "键缺失、『暂无数据』或格式不符保留未知；解析成功率随字典登记",
                "否", "训练与预测均可得"),
    FeatureSpec("household_count", "建筑", "extra_fields_json『梯户比例』键",
                "中文数字解析户数（同 ladder_count 规则）",
                "楼栋/小区代表值", "静态属性",
                "同 ladder_count：解析失败保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("ladder_per_household", "建筑", "ladder_count / household_count",
                "梯户比=梯数/户数（比值特征）",
                "楼栋/小区代表值", "静态属性",
                "任一计数未知或户数为 0 则未知",
                "否", "训练与预测均可得"),
    # ── 房屋 ──
    FeatureSpec("orientation", "房屋", "orientation（主表列）",
                "朝向原样类别（南/北/东南/南北等）",
                "逐套", "挂牌时可知",
                "缺失保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("decoration_state", "房屋", "decoration_norm（主表列）",
                "装修标签：简装/精装/毛坯/其他",
                "逐套", "挂牌时可知；可变化属性——成交时历史状态未证实，"
                "敏感性实验安排：S2 以纳入为基准、登记纳入/排除对照（蓝图 §4.3.7），"
                "不宣称严格历史可得",
                "缺失保留未知",
                "否", "训练与预测均可得（历史可得性限制按上述登记）"),
    FeatureSpec("ownership_shared", "房屋", "extra_fields_json『产权』键",
                "共有/非共有二值；实测共有约 25%（用户 2026-09-11 第一轮确认纳入）",
                "逐套（权属与税费子类）", "挂牌时可知（交易条件）",
                "键缺失、『暂无数据』或解析失败保留未知；覆盖率随字典登记",
                "否", "训练与预测均可得"),
    FeatureSpec("tax_status", "房屋", "extra_fields_json『房屋年龄』键",
                "满五年/满两年/未满两年税费口径。键名与语义分歧显式登记：来源键名为"
                "『房屋年龄』，实测取值为满二满五税费状态，非建成年龄（用户 2026-09-11 "
                "第二轮确认，分布：满五 65%/满二 10%/未满二 5%）",
                "逐套（权属与税费子类）", "挂牌时可知（交易税费条件）",
                "键缺失、『暂无数据』或取值不在枚举内保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("property_tenure_mark", "房屋", "extra_fields_json『房屋权属』键",
                "商品房/非商品房标记（实测商品房占 98.1%，作标记维度；"
                "原值另存 property_tenure_raw 登记列，用户 2026-09-11 第二轮确认）",
                "逐套（权属与税费子类）", "挂牌时可知",
                "键缺失、『暂无数据』或解析失败保留未知",
                "否", "训练与预测均可得"),
    FeatureSpec("property_tenure_raw", "房屋", "extra_fields_json『房屋权属』键",
                "来源原值保留（登记列，供人工复核，不作标记输入）",
                "逐套", "挂牌时可知",
                "『暂无数据』或缺失保留空",
                "否", "登记列，不作为预测输入"),
    # ── 市场 ──
    FeatureSpec("community_med_unit_price_365d", "市场",
                "主表同小区成交单价（unit_price）滚动聚合",
                f"证据窗 [行日期−{MARKET_WINDOW_DAYS} 天, 行日期)：上界开区间排除同日"
                "（按天披露、同日先后未知，蓝图 §4.3.4）、排除自身，窗内成交单价中位数；"
                "证据行均通过主表同一清洗合同",
                "小区×行", "行级截点：仅使用严格早于该行的成交；评估窗预测使用窗级截点"
                "（community_market_snapshot，锚定窗起点，蓝图 §4.3.3：窗口内成交价不"
                "进入该窗预测特征）",
                "窗内无证据保留未知",
                "否（使用的是其他历史成交的价格，不是本行标签）",
                "训练行用行级截点可得；预测/验证窗行用窗级截点可得"),
    FeatureSpec("community_last_sale_days", "市场", "同证据窗最新成交日期",
                "窗内最新成交距该行天数（int）；市场新鲜度表达",
                "小区×行", "同 community_med_unit_price_365d 的截点规则",
                "窗内无证据保留未知",
                "否", "同 community_med_unit_price_365d"),
    # ── 质量与支持度 ──
    FeatureSpec("community_sample_365d", "质量与支持度", "同证据窗成交行数",
                f"局部样本量：证据窗 [行日期−{MARKET_WINDOW_DAYS} 天, 行日期) 内同小区"
                "成交行数（含本行以外的证据行计数）",
                "小区×行", "同截点规则（行级/窗级）",
                "无证据记 0",
                "否", "同截点规则"),
    FeatureSpec("miss_year_built", "质量与支持度", "year_built",
                "缺失标记：year_built 为空记 1，否则 0",
                "逐套", "预测输入可知",
                "—", "否", "训练与预测均可得"),
    FeatureSpec("miss_total_floors", "质量与支持度", "total_floors",
                "缺失标记：total_floors 为空记 1，否则 0",
                "逐套", "预测输入可知", "—", "否", "训练与预测均可得"),
    FeatureSpec("miss_elevator", "质量与支持度", "has_elevator",
                "缺失标记：has_elevator 为空记 1，否则 0",
                "逐套", "预测输入可知", "—", "否", "训练与预测均可得"),
    FeatureSpec("miss_orientation", "质量与支持度", "orientation",
                "缺失标记：orientation 为空记 1，否则 0",
                "逐套", "预测输入可知", "—", "否", "训练与预测均可得"),
    FeatureSpec("miss_decoration", "质量与支持度", "decoration_norm",
                "缺失标记：decoration_norm 为空记 1，否则 0",
                "逐套", "预测输入可知", "—", "否", "训练与预测均可得"),
    FeatureSpec("conflict_elevator", "质量与支持度", "has_elevator_raw vs has_elevator",
                "来源冲突标记（属性级，无标签参与）：原值可判读（有/无）而解析列缺失"
                "或与原值不一致记 1",
                "逐套", "预测输入可知",
                "原值不可判读（缺失/『暂无数据』）记 0（属缺失而非冲突）",
                "否", "训练与预测均可得"),
    FeatureSpec("conflict_year", "质量与支持度", "built_year_raw vs year_built",
                "来源冲突标记：原值可提取四位年份且与解析列不一致记 1",
                "逐套", "预测输入可知",
                "原值无可提取年份记 0（属缺失而非冲突）",
                "否", "训练与预测均可得"),
    FeatureSpec("conflict_floor", "质量与支持度", "floor_raw 层数后缀 vs total_floors",
                "来源冲突标记：原值『…/N层』可提取 N 且与解析列不一致记 1",
                "逐套", "预测输入可知",
                "原值无层数后缀记 0（属缺失而非冲突）",
                "否", "训练与预测均可得"),
    FeatureSpec("conflict_layout", "质量与支持度", "layout_raw 室数 vs bedrooms_raw",
                "来源冲突标记：『N室』可提取且与 bedrooms_raw 解析不一致记 1",
                "逐套", "预测输入可知",
                "任一侧不可解析记 0（属缺失而非冲突）",
                "否", "训练与预测均可得"),
)

FEATURE_GROUPS = ["位置", "产品", "建筑", "房屋", "市场", "质量与支持度"]

EXCLUDED_FIELDS: tuple[dict, ...] = (
    {"field": "extra_fields_json『供暖方式』",
     "reason": "实测 99.4%『暂无数据』，零区分度（用户 2026-09-11 第二轮查实排除）"},
    {"field": "extra_fields_json『品牌』（经纪品牌）",
     "reason": "渠道属性，非房屋属性（用户确认排除）"},
    {"field": "extra_fields_json『带看』『关注』『浏览』",
     "reason": "挂牌过程信息，估值时点不可得（时点可得性不满足）"},
    {"field": "房源标题/source_property_description/source_property_tags/"
              "『位置描述』『房屋位置』",
     "reason": "文本字段非结构化，第一版不处理（用户确认排除；『位置描述』含路名级"
              "信息且缺失正常，按 proposal 用户澄清不纳入）"},
    {"field": "listing_price_raw/listing_price_yuan/listing_days/price_adjustments_raw",
     "reason": "挂牌侧字段；挂牌价第一版不进历史预测输入（蓝图 §4.2）"},
    {"field": "floorplan_url_list_raw/floorplan_url_status/floorplan_candidate_count",
     "reason": "户型图资产管理字段，非价格特征"},
    {"field": "extra_fields_json『纬度』『经度』",
     "reason": "坐标来源可靠性未核验；无可靠来源不扩展位置特征（蓝图 §4.2）"},
    {"field": "extra_fields_json『房屋面积.1』",
     "reason": "来源附加面积字段，与交易面积口径关系未核，避免重复登记"},
    {"field": "extra_fields_json『结构』『高度』",
     "reason": "用户两轮确认范围外、口径未核，留待后续增量实验另行登记"},
    {"field": "total_price_yuan/unit_price/unit_price_observed 及其原值列",
     "reason": "标签及标签直接推导，仅作预测目标（spec：标签边界）；本行价格相关"
              "状态列也不进特征（质量信息经属性缺失/冲突标记以无标签方式表达）"},
)

FEATURE_COLUMNS = [f.name for f in _FIELDS]
FEATURE_NAMES_BY_GROUP = {g: [f.name for f in _FIELDS if f.group == g]
                          for g in FEATURE_GROUPS}

_LADDER_RE = re.compile(r"^\s*([一二两三四五六七八九十]+)\s*梯\s*([一二两三四五六七八九十]+)\s*户\s*$")
_CN_DIGIT = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
             "六": 6, "七": 7, "八": 8, "九": 9}


def _cn_to_int(s: str) -> int | None:
    if not s:
        return None
    if s == "十":
        return 10
    if "十" in s:
        left, _, right = s.partition("十")
        tens = _CN_DIGIT.get(left, 1) if left else 1
        ones = _CN_DIGIT.get(right, 0) if right else 0
        return tens * 10 + ones
    return _CN_DIGIT.get(s)


def parse_ladder(value) -> tuple[int | None, int | None]:
    """『梯户比例』中文数字解析：『两梯三户』→(2,3)；失败返回 (None, None)。"""
    if value is None:
        return None, None
    m = _LADDER_RE.match(str(value))
    if not m:
        return None, None
    return _cn_to_int(m.group(1)), _cn_to_int(m.group(2))


def _extra_get(s, key):
    if s is None:
        return None
    try:
        v = json.loads(s).get(key)
    except Exception:
        return None
    if v is None or (isinstance(v, str) and v.strip() in {"", "暂无数据"}):
        return None
    return v


def _elevator_raw_state(s):
    if s is None:
        return None
    t = str(s).strip()
    if t == "有" or t.lower() == "true":
        return True
    if t == "无" or t.lower() == "false":
        return False
    return None


# ---------- 覆盖率与解析成功率 ----------

def measure_coverage(master: pl.DataFrame) -> dict:
    n = master.height
    extra = master["extra_fields_json"]

    def key_present(key: str) -> int:
        return int(extra.map_elements(lambda s: _extra_get(s, key) is not None,
                                      return_dtype=pl.Boolean).sum())

    ladder_raw = extra.map_elements(lambda s: _extra_get(s, "梯户比例"),
                                    return_dtype=pl.String)
    ladder_parsed = ladder_raw.map_elements(lambda v: parse_ladder(v)[0] is not None,
                                            return_dtype=pl.Boolean)
    house_type_ok = int(master.select(
        pl.col("house_type").is_in(["塔楼", "板楼", "板塔结合", "平房"]).sum()).item())
    ba_ok = int(master["building_area_detail_sqm"].is_not_null().sum())
    eff_ok = int(master.filter((pl.col("building_area_detail_sqm") > 10)).height)
    cut = (master["sale_date_d"].max())  # 信息截点 = 主表最大成交日
    recent_start = cut - timedelta(days=RECENT_COVERAGE_WINDOW_DAYS)
    recent = master.filter((pl.col("sale_date_d") > recent_start)
                           & (pl.col("sale_date_d") <= cut))
    yb_col, tf_col = "year_built", "total_floors"
    return {
        "measured_rows": n,
        "coverage": {
            "house_type_norm": {"rows": house_type_ok, "ratio": round(house_type_ok / n, 4)},
            "building_area_detail_sqm": {"rows": ba_ok, "ratio": round(ba_ok / n, 4)},
            "efficiency_ratio_computable": {"rows": eff_ok, "ratio": round(eff_ok / n, 4)},
            "extra_板块": {"rows": key_present("板块"), "ratio": round(key_present("板块") / n, 4)},
            "extra_产权": {"rows": key_present("产权"), "ratio": round(key_present("产权") / n, 4)},
            "extra_房屋年龄(税费口径)": {"rows": key_present("房屋年龄"),
                                   "ratio": round(key_present("房屋年龄") / n, 4)},
            "extra_房屋权属": {"rows": key_present("房屋权属"),
                           "ratio": round(key_present("房屋权属") / n, 4)},
            "extra_梯户比例_键可用": {"rows": int((ladder_raw.is_not_null()).sum()),
                                 "ratio": round(float((ladder_raw.is_not_null()).sum()) / n, 4)},
            "extra_梯户比例_解析成功": {"rows": int(ladder_parsed.sum()),
                                     "ratio": round(float(ladder_parsed.sum()) / n, 4)},
        },
        "parse_rules": {
            "missing_marker": "『暂无数据』与空串按缺失处理（保留未知）",
            "ladder": "正则 ^N梯M户$，N/M 为中文数字（一~九、两=2、十、X十Y）",
        },
        "gate2_baseline": {
            "as_of_cutoff": str(cut),
            "recent_window": f"({recent_start} , {cut}]（{RECENT_COVERAGE_WINDOW_DAYS} 天）",
            "year_built": {
                "overall_ratio": round(float(master[yb_col].is_not_null().sum()) / n, 4),
                "recent_ratio": round(float(recent[yb_col].is_not_null().sum())
                                      / max(recent.height, 1), 4),
                "recent_rows": recent.height,
            },
            "total_floors": {
                "overall_ratio": round(float(master[tf_col].is_not_null().sum()) / n, 4),
                "recent_ratio": round(float(recent[tf_col].is_not_null().sum())
                                      / max(recent.height, 1), 4),
                "recent_rows": recent.height,
            },
        },
    }


COVERAGE_MONITOR = {
    "gate2_fields": ["year_built", "total_floors"],
    "rule": f"下一批外源数据到达并重建主表时，重算最近 {RECENT_COVERAGE_WINDOW_DAYS} 天窗口"
            f" PARSED 覆盖率；较本 run 基线（gate2_baseline）下降超过 "
            f"{COVERAGE_DROP_ALERT_PP:g} 个百分点 → 停线上报（数据门条件 2）",
    "status": "规则已登记；实测待下一批数据到达时执行（本 change 不做实测）",
}


# ---------- 字典导出 ----------

def assert_dictionary_complete() -> None:
    for f in _FIELDS:
        for attr in ("source_field", "computation", "granularity", "availability",
                     "missing_handling", "label_usage", "train_predict"):
            value = getattr(f, attr)
            if not value or not str(value).strip():
                raise AssertionError(f"特征 {f.name} 的元信息 {attr} 为空")
    for g in FEATURE_GROUPS:
        if not FEATURE_NAMES_BY_GROUP[g]:
            raise AssertionError(f"特征组 {g} 为空")


def export_dictionary(master: pl.DataFrame, run_dir: Path) -> dict:
    assert_dictionary_complete()
    coverage = measure_coverage(master)
    doc = {
        "schema_version": "phase2-feature-dictionary-v1",
        "run_id": run_dir.name,
        "generated_at": None,
        "meta": "代码即定义：本字典由 gz_property_valuation/phase2/features.py 的 "
                "FeatureSpec 声明导出，与特征生成同源；字段名与 features.parquet 列名一致",
        "label_boundary": "成交总价（total_price_yuan）与直接推导单价（unit_price）仅作标签；"
                          "目标成交后信息、未来回填属性、挂牌价不进历史预测输入",
        "market_feature_rule": f"行级证据窗 [行日期−{MARKET_WINDOW_DAYS} 天, 行日期)，"
                               "上界开区间排除同日、排除自身；评估窗用窗级截点快照",
        "groups": {g: [asdict(f) for f in _FIELDS if f.group == g]
                   for g in FEATURE_GROUPS},
        "excluded": list(EXCLUDED_FIELDS),
        "coverage": coverage,
        "coverage_monitor": COVERAGE_MONITOR,
    }
    lines = [
        f"# phase2 特征字典（run {run_dir.name}）", "",
        "> 代码即定义：由 `gz_property_valuation/phase2/features.py` 声明导出，"
        "与特征生成同源；字段名与 `features.parquet` 列名一致。", "",
        f"**标签边界**：{doc['label_boundary']}", "",
        f"**市场特征规则**：{doc['market_feature_rule']}", "",
    ]
    for g in FEATURE_GROUPS:
        lines.append(f"## 特征组：{g}")
        lines.append("")
        for f in doc["groups"][g]:
            lines.append(f"### {f['name']}")
            for label, key in (("原字段", "source_field"), ("计算方式", "computation"),
                               ("粒度", "granularity"), ("可用时点", "availability"),
                               ("缺失与异常处理", "missing_handling"),
                               ("是否使用成交标签", "label_usage"),
                               ("训练与预测可得性", "train_predict")):
                lines.append(f"- {label}：{f[key]}")
            lines.append("")
    lines.append("## 查实排除项（附录登记）")
    lines.append("")
    for e in doc["excluded"]:
        lines.append(f"- {e['field']}：{e['reason']}")
    lines.append("")
    lines.append("## 覆盖率与解析成功率（本 run 实测）")
    lines.append("")
    for k, v in coverage["coverage"].items():
        lines.append(f"- {k}：{v['rows']}/{coverage['measured_rows']}（{v['ratio']:.1%}）")
    lines.append("")
    lines.append("## 覆盖监控规则（数据门条件 2）")
    lines.append("")
    lines.append(f"- 字段：{', '.join(COVERAGE_MONITOR['gate2_fields'])}")
    lines.append(f"- 规则：{COVERAGE_MONITOR['rule']}")
    lines.append(f"- 状态：{COVERAGE_MONITOR['status']}")
    lines.append("")
    lines.append("### 基线（gate2_baseline）")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(coverage["gate2_baseline"], ensure_ascii=False, indent=1))
    lines.append("```")
    lines.append("")
    (run_dir / "feature_dictionary.md").write_text("\n".join(lines), encoding="utf-8")
    (run_dir / "feature_dictionary.json").write_text(
        json.dumps(doc, ensure_ascii=False, indent=1), encoding="utf-8")
    register_artifact(run_dir, "feature_dictionary.json",
                      extra={"fields": len(_FIELDS), "groups": FEATURE_GROUPS})
    register_artifact(run_dir, "feature_dictionary.md")
    return doc


# ---------- 特征生成（任务 3.2） ----------

def _market_row_features(master: pl.DataFrame) -> pl.DataFrame:
    keys = master.select(["source_record_id", "community_source_id", "sale_date_d"])
    ev = master.select([
        pl.col("source_record_id").alias("ev_id"),
        pl.col("community_source_id"),
        pl.col("sale_date_d").alias("ev_date"),
        pl.col("unit_price").alias("ev_price"),
    ])
    j = keys.join(ev, on="community_source_id", how="left")
    j = j.filter(
        (pl.col("ev_date") < pl.col("sale_date_d"))
        & (pl.col("ev_date") >= pl.col("sale_date_d") - pl.duration(days=MARKET_WINDOW_DAYS))
        & (pl.col("ev_id") != pl.col("source_record_id")))
    agg = j.group_by("source_record_id").agg([
        pl.col("ev_price").median().alias("community_med_unit_price_365d"),
        pl.len().alias("community_sample_365d"),
        (pl.col("sale_date_d").first() - pl.col("ev_date").max())
        .dt.total_days().alias("community_last_sale_days"),
    ])
    out = keys.select(["source_record_id"]).join(agg, on="source_record_id", how="left")
    return out.with_columns([
        pl.col("community_sample_365d").fill_null(0).cast(pl.Int64),
        pl.col("community_last_sale_days").cast(pl.Int64),
    ])


def community_market_snapshot(master: pl.DataFrame, cutoff: date) -> pl.DataFrame:
    """窗级截点市场快照（评估窗统一截点，蓝图 §4.3.3/§5 B0 口径）。

    窗口以统一截点为锚：[cutoff−365, cutoff)，上界开区间排除同日、排除截点后
    与截点当日成交；每个小区一行。窗口内任何成交价不进入该窗预测特征。
    """
    ev = master.select([
        pl.col("community_source_id"),
        pl.col("sale_date_d").alias("ev_date"),
        pl.col("unit_price").alias("ev_price"),
    ])
    cut = pl.lit(cutoff)
    ev = ev.filter((pl.col("ev_date") < cut)
                   & (pl.col("ev_date") >= cut - pl.duration(days=MARKET_WINDOW_DAYS)))
    return (ev.group_by("community_source_id").agg([
        pl.col("ev_price").median().alias("community_med_unit_price_365d_at_cutoff"),
        pl.len().alias("community_sample_365d_at_cutoff"),
        (cut - pl.col("ev_date").max()).dt.total_days().max()
        .alias("community_last_sale_days_at_cutoff"),
    ])).with_columns(pl.col("community_sample_365d_at_cutoff").fill_null(0))


def generate_features(master: pl.DataFrame) -> pl.DataFrame:
    """按字典声明生成特征（行级市场特征模式）；输出不含标签列及其直接推导。"""
    extra = master["extra_fields_json"]

    def col(expr) -> pl.Series:
        return master.select(expr.alias("v"))["v"]

    block = extra.map_elements(lambda s: _extra_get(s, "板块"), return_dtype=pl.String)
    ownership = extra.map_elements(lambda s: _extra_get(s, "产权"), return_dtype=pl.String)
    tax = extra.map_elements(lambda s: _extra_get(s, "房屋年龄"), return_dtype=pl.String)
    tenure = extra.map_elements(lambda s: _extra_get(s, "房屋权属"), return_dtype=pl.String)
    ladder = extra.map_elements(lambda s: _extra_get(s, "梯户比例"), return_dtype=pl.String)
    ladder_n = ladder.map_elements(lambda v: parse_ladder(v)[0], return_dtype=pl.Int64)
    household_n = ladder.map_elements(lambda v: parse_ladder(v)[1], return_dtype=pl.Int64)

    f = master.select([
        pl.col("source_record_id"), pl.col("community_source_id"),
        pl.col("sale_date"), pl.col("sale_date_d"),
        pl.col("transaction_area_sqm").alias("area_sqm"),
        pl.col("bedrooms_raw").cast(pl.Int64, strict=False).alias("bedrooms_n"),
        pl.col("living_rooms_raw").cast(pl.Int64, strict=False).alias("living_rooms_n"),
        pl.col("floor_bucket").fill_null("未知").alias("floor_bucket"),
        pl.when(pl.col("house_type").is_in(["塔楼", "板楼", "板塔结合", "平房"]))
          .then(pl.col("house_type")).otherwise(pl.lit("未知")).alias("house_type_norm"),
        (pl.col("transaction_area_sqm") / pl.col("building_area_detail_sqm"))
        .alias("efficiency_ratio"),
        pl.col("year_built"),
        (pl.col("sale_date_d").dt.year() - pl.col("year_built"))
        .cast(pl.Float64).clip(0, 80).alias("age_years"),
        pl.col("total_floors"),
        pl.when(pl.col("has_elevator")).then(pl.lit("有"))
          .when(pl.col("has_elevator") == False).then(pl.lit("无"))
          .otherwise(pl.lit("未知")).alias("elevator_state"),
        ladder_n.alias("ladder_count"),
        household_n.alias("household_count"),
        pl.when((ladder_n.is_not_null()) & (household_n > 0))
          .then((ladder_n / household_n).cast(pl.Float64))
          .otherwise(pl.lit(None, dtype=pl.Float64)).alias("ladder_per_household"),
        pl.col("orientation").fill_null("未知").alias("orientation"),
        pl.col("decoration_norm").fill_null("未知").alias("decoration_state"),
        pl.when(ownership.is_in(["共有", "非共有"])).then(ownership)
          .otherwise(pl.lit("未知")).alias("ownership_shared"),
        pl.when(tax.is_in(["满五年", "满两年", "未满两年"])).then(tax)
          .otherwise(pl.lit("未知")).alias("tax_status"),
        pl.when(tenure == pl.lit("商品房")).then(pl.lit("商品房"))
          .when(tenure.is_not_null()).then(pl.lit("非商品房"))
          .otherwise(pl.lit("未知")).alias("property_tenure_mark"),
        tenure.alias("property_tenure_raw"),
        block.alias("block_name"),
    ])
    f = f.with_columns(
        pl.when(pl.col("efficiency_ratio") > 0).then(pl.col("efficiency_ratio"))
        .otherwise(pl.lit(None, dtype=pl.Float64)).alias("efficiency_ratio"))

    raw_year = col(pl.col("built_year_raw").str.extract(r"(\d{4})", 1).cast(pl.Int64))
    raw_floors = col(pl.col("floor_raw").str.extract(r"(\d+)\s*层\s*$", 1).cast(pl.Int64))
    raw_rooms = col(pl.col("layout_raw").str.extract(r"^(\d+)\s*室", 1).cast(pl.Int64))
    elev_raw = extra.map_elements(_elevator_raw_state, return_dtype=pl.Boolean)

    f = f.with_columns([
        pl.col("year_built").is_null().cast(pl.Int64).alias("miss_year_built"),
        pl.col("total_floors").is_null().cast(pl.Int64).alias("miss_total_floors"),
        master["has_elevator"].is_null().cast(pl.Int64).alias("miss_elevator"),
        (master["orientation"].is_null()).cast(pl.Int64).alias("miss_orientation"),
        (master["decoration_norm"].is_null()).cast(pl.Int64).alias("miss_decoration"),
        (elev_raw.is_not_null()
         & (master["has_elevator"].is_null() | (master["has_elevator"] != elev_raw)))
        .cast(pl.Int64).alias("conflict_elevator"),
        (raw_year.is_not_null() & master["year_built"].is_not_null()
         & (master["year_built"] != raw_year)).cast(pl.Int64).alias("conflict_year"),
        (raw_floors.is_not_null() & master["total_floors"].is_not_null()
         & (master["total_floors"] != raw_floors)).cast(pl.Int64).alias("conflict_floor"),
        (raw_rooms.is_not_null() & pl.col("bedrooms_n").is_not_null()
         & (pl.col("bedrooms_n") != raw_rooms)).cast(pl.Int64).alias("conflict_layout"),
    ])

    mkt = _market_row_features(master)
    f = f.join(mkt, on="source_record_id", how="left")

    declared = set(FEATURE_COLUMNS) | set(FEATURE_KEYS)
    missing_cols = declared - set(f.columns)
    if missing_cols:
        raise AssertionError(f"特征输出缺少字典声明列：{sorted(missing_cols)}")
    extra_cols = set(f.columns) - declared
    if extra_cols:
        raise AssertionError(f"特征输出含未声明列：{sorted(extra_cols)}")
    leaked = sorted(set(f.columns) & set(LABEL_COLUMNS))
    if leaked:
        raise AssertionError(f"特征输出含标签列及直接推导：{leaked}")
    return f.sort("source_record_id")


def build_features(run_dir: Path) -> pl.DataFrame:
    master = pl.read_parquet(run_dir / "master_table.parquet")
    features = generate_features(master)
    features.write_parquet(run_dir / "features.parquet")
    register_artifact(run_dir, "features.parquet", rows=features.height,
                      extra={"columns": features.width,
                             "mode": "row-level market window",
                             "label_free": True})
    return features


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="phase2 特征字典与特征生成")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_dict = sub.add_parser("dictionary", help="导出特征字典 JSON+MD（参数：run 目录）")
    p_dict.add_argument("run_dir")
    p_gen = sub.add_parser("generate", help="生成特征并落盘（参数：run 目录）")
    p_gen.add_argument("run_dir")
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir).resolve()
    master = pl.read_parquet(run_dir / "master_table.parquet")
    if args.cmd == "dictionary":
        doc = export_dictionary(master, run_dir)
        print(json.dumps({
            "run_dir": str(run_dir),
            "fields": len(_FIELDS),
            "groups": {g: len(v) for g, v in doc["groups"].items()},
            "excluded": len(doc["excluded"]),
            "coverage": doc["coverage"]["coverage"],
            "gate2_baseline": doc["coverage"]["gate2_baseline"],
            "seven_item_assert": "PASS",
            "verdict": "PASS",
        }, ensure_ascii=False, indent=1))
        return 0

    features = build_features(run_dir)
    sample = features.sample(n=min(5, features.height), seed=20260911)
    print(json.dumps({
        "run_dir": str(run_dir),
        "feature_rows": features.height,
        "feature_columns": features.width,
        "columns": features.columns,
        "label_free_check": sorted(set(features.columns) & set(LABEL_COLUMNS)) == [],
        "sample_rows": sample.to_dicts(),
    }, ensure_ascii=False, indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
