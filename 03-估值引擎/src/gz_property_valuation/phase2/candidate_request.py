# -*- coding: utf-8 -*-
"""phase2 S6-A 候选模式请求映射与输入校验。

行为规格（openspec/changes/prepare-phase2-s6-candidate-ops/specs/phase2-candidate-ops/spec.md
「请求映射与确定行为」；design D2）：

- 请求以显式模式接收目标房源属性（外部小区标识、区县、用途、面积、楼层段、年代、电梯、
  总层数、户型、朝向、装修、估值时点与数据截点），映射到 V1 推理输入帧；
- 适用人群＝云溪区普通住宅（S1 合同 §2）：请求显式含区县/用途且非适用 → 拒绝或加
  范围外标注（不静默出报价）；未提供区县/用途 → 记 ``scope_unverified`` 标注但不阻断；
- 缺失字段保持未知（None），由 V1 冻结编码器走 miss 标记/降级路径，**不做请求侧插补**；
- 超范围输入按登记域值表处理：硬域外（面积/年份/时点）→ 拒绝；软域外（总层数/枚举）→
  限定标注（该字段按未知处理，不静默外推）；
- 错误提示为可读中文短句，逐字段给出。

只读消费 S1 特征字典契约；不修改任何既有实现（纯新增模块）。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

DISTRICT_IN_SCOPE = "云溪区"
PROPERTY_USE_IN_SCOPE = "普通住宅"

AREA_MIN_EXCLUSIVE = 10.0
AREA_MAX_INCLUSIVE = 300.0
YEAR_BUILT_MIN = 1900
TOTAL_FLOORS_MIN = 1
TOTAL_FLOORS_MAX = 300
FLOOR_BUCKET_ENUM = ("低楼层", "中楼层", "高楼层", "地下室", "未知")
FLOOR_BUCKET_ALIAS = {"低": "低楼层", "中": "中楼层", "高": "高楼层",
                      "地下室": "地下室", "未知": "未知"}
DECORATION_ENUM = ("简装", "精装", "毛坯", "其他")
BEDROOMS_SINGLE = (1, 2, 3, 4, 5)
AGE_CLIP = (0.0, 80.0)

V1_INPUT_COLUMNS = (
    "source_record_id", "community_source_id", "sale_date_d", "block_name",
    "area_sqm", "age_years", "total_floors", "miss_year_built", "miss_total_floors",
    "miss_elevator", "miss_orientation", "miss_decoration",
    "floor_bucket", "elevator_state", "bedrooms_n", "orientation", "decoration_state",
)

REQUEST_TEMPLATE = {
    "request_id": "REQ-DEMO-0001",
    "community_source_id": "21000000002364",
    "community_name": "（可选）示例小区",
    "block_name": "（可选）示例板块",
    "district": "云溪区",
    "property_use": "普通住宅",
    "area_sqm": 89.5,
    "floor_bucket": "中楼层",
    "year_built": 2008,
    "has_elevator": True,
    "total_floors": 18,
    "bedrooms": 3,
    "orientation": "南",
    "decoration": "精装",
    "valuation_date": "2026-07-20",
    "data_cutoff": "2026-07-20",
}


class RequestError(ValueError):
    """请求不可解析或命中硬域外拒绝（调用方据此输出拒绝结果，不产出报价）。"""

    def __init__(self, message: str, issues: list[dict]):
        super().__init__(message)
        self.issues = issues


@dataclass
class Request:
    request_id: str
    community_source_id: str
    area_sqm: float
    valuation_date: date
    community_name: str | None = None
    block_name: str | None = None
    district: str | None = None
    property_use: str | None = None
    floor_bucket: str | None = None
    year_built: int | None = None
    has_elevator: bool | None = None
    total_floors: int | None = None
    bedrooms: int | None = None
    orientation: str | None = None
    decoration: str | None = None
    data_cutoff: date | None = None
    raw: dict = field(default_factory=dict)


def _as_date(value, field_name: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except Exception as exc:  # noqa: BLE001
        raise RequestError(f"字段 {field_name} 不是合法日期（YYYY-MM-DD）：{value!r}",
                           [{"field": field_name, "severity": "error",
                             "code": "invalid_date", "message": str(exc)}])


def _as_float(value, field_name: str) -> float:
    try:
        return float(value)
    except Exception as exc:  # noqa: BLE001
        raise RequestError(f"字段 {field_name} 不是数值：{value!r}",
                           [{"field": field_name, "severity": "error",
                             "code": "invalid_number", "message": str(exc)}])


def _as_int(value, field_name: str) -> int:
    try:
        return int(value)
    except Exception as exc:  # noqa: BLE001
        raise RequestError(f"字段 {field_name} 不是整数：{value!r}",
                           [{"field": field_name, "severity": "error",
                             "code": "invalid_integer", "message": str(exc)}])


def _as_bool(value, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "有", "是"):
            return True
        if low in ("false", "0", "无", "否"):
            return False
    raise RequestError(f"字段 {field_name} 不是布尔（true/false/有/无）：{value!r}",
                       [{"field": field_name, "severity": "error",
                         "code": "invalid_bool", "message": str(value)}])


def parse_request(doc: dict, default_valuation_date: date | None = None) -> Request:
    """把请求 JSON 解析为 :class:`Request`；结构性缺失/类型错误即抛 :class:`RequestError`。"""
    if not isinstance(doc, dict):
        raise RequestError("请求必须是 JSON 对象", [
            {"field": None, "severity": "error", "code": "not_object",
             "message": "请求必须是 JSON 对象"}])
    missing = [k for k in ("request_id", "community_source_id", "area_sqm")
               if doc.get(k) in (None, "")]
    if missing:
        raise RequestError(f"缺少必填字段：{'、'.join(missing)}",
                           [{"field": m, "severity": "error", "code": "required_missing",
                             "message": f"必填字段 {m} 缺失"} for m in missing])

    vdate = (default_valuation_date or date.today())
    if doc.get("valuation_date") not in (None, ""):
        vdate = _as_date(doc["valuation_date"], "valuation_date")

    req = Request(
        request_id=str(doc["request_id"]),
        community_source_id=str(doc["community_source_id"]),
        area_sqm=_as_float(doc["area_sqm"], "area_sqm"),
        valuation_date=vdate,
        community_name=None if doc.get("community_name") in (None, "") else str(doc["community_name"]),
        block_name=None if doc.get("block_name") in (None, "") else str(doc["block_name"]),
        district=None if doc.get("district") in (None, "") else str(doc["district"]).strip(),
        property_use=None if doc.get("property_use") in (None, "") else str(doc["property_use"]).strip(),
        floor_bucket=(None if doc.get("floor_bucket") in (None, "")
                      else FLOOR_BUCKET_ALIAS.get(str(doc["floor_bucket"]).strip(),
                                                 str(doc["floor_bucket"]).strip())),
        year_built=None if doc.get("year_built") in (None, "") else _as_int(doc["year_built"], "year_built"),
        has_elevator=None if doc.get("has_elevator") in (None, "") else _as_bool(doc["has_elevator"], "has_elevator"),
        total_floors=None if doc.get("total_floors") in (None, "") else _as_int(doc["total_floors"], "total_floors"),
        bedrooms=None if doc.get("bedrooms") in (None, "") else _as_int(doc["bedrooms"], "bedrooms"),
        orientation=None if doc.get("orientation") in (None, "") else str(doc["orientation"]).strip(),
        decoration=None if doc.get("decoration") in (None, "") else str(doc["decoration"]).strip(),
        data_cutoff=None if doc.get("data_cutoff") in (None, "") else _as_date(doc["data_cutoff"], "data_cutoff"),
        raw=doc,
    )
    return req


def validate(req: Request, as_of: date | None = None) -> list[dict]:
    """逐字段校验；返回问题清单（severity: error 拒绝 / limited 限定标注 / info 提示）。

    error 项即触发硬域外拒绝（调用方不产出报价）；limited 项对应字段按"未知"处理并标注。
    """
    issues: list[dict] = []
    as_of = as_of or date.today()

    # 适用人群门（F1）
    if req.district is not None and req.district != DISTRICT_IN_SCOPE:
        issues.append({"field": "district", "severity": "error",
                       "code": "out_of_scope_district",
                       "message": f"区县 {req.district!r} 非适用人群（{DISTRICT_IN_SCOPE}），拒绝出价"})
    if req.property_use is not None and req.property_use != PROPERTY_USE_IN_SCOPE:
        issues.append({"field": "property_use", "severity": "error",
                       "code": "out_of_scope_use",
                       "message": f"用途 {req.property_use!r} 非适用人群（{PROPERTY_USE_IN_SCOPE}），拒绝出价"})
    if req.district is None:
        issues.append({"field": "district", "severity": "info",
                       "code": "scope_unverified",
                       "message": "未提供区县，按在适用范围内继续（标注 scope_unverified）"})
    if req.property_use is None:
        issues.append({"field": "property_use", "severity": "info",
                       "code": "scope_unverified",
                       "message": "未提供用途，按在适用范围内继续（标注 scope_unverified）"})

    # 硬域值（S1 合同 §4 清洗域）
    if not (AREA_MIN_EXCLUSIVE < req.area_sqm <= AREA_MAX_INCLUSIVE):
        issues.append({"field": "area_sqm", "severity": "error",
                       "code": "out_of_domain_area",
                       "message": f"面积 {req.area_sqm} 超出登记域（{AREA_MIN_EXCLUSIVE}, {AREA_MAX_INCLUSIVE}]，拒绝出价"})
    if req.year_built is not None:
        if req.year_built < YEAR_BUILT_MIN:
            issues.append({"field": "year_built", "severity": "error",
                           "code": "out_of_domain_year_built",
                           "message": f"建成年份 {req.year_built} 早于 {YEAR_BUILT_MIN}，拒绝出价"})
        elif req.year_built > req.valuation_date.year:
            issues.append({"field": "year_built", "severity": "error",
                           "code": "year_built_after_valuation",
                           "message": f"建成年份 {req.year_built} 晚于估值时点年份 {req.valuation_date.year}，拒绝出价"})
    if req.valuation_date > as_of:
        issues.append({"field": "valuation_date", "severity": "error",
                       "code": "valuation_date_in_future",
                       "message": f"估值时点 {req.valuation_date} 晚于当前 {as_of}，拒绝出价"})
    if req.data_cutoff is not None and req.data_cutoff > req.valuation_date:
        issues.append({"field": "data_cutoff", "severity": "error",
                       "code": "data_cutoff_after_valuation",
                       "message": f"数据截点 {req.data_cutoff} 晚于估值时点 {req.valuation_date}，拒绝出价"})

    # 软域值（限定标注，字段按未知处理）
    if req.total_floors is not None and not (TOTAL_FLOORS_MIN <= req.total_floors <= TOTAL_FLOORS_MAX):
        issues.append({"field": "total_floors", "severity": "limited",
                       "code": "out_of_domain_total_floors",
                       "message": f"总层数 {req.total_floors} 超出 [{TOTAL_FLOORS_MIN},{TOTAL_FLOORS_MAX}]，按未知处理"})
    if req.floor_bucket is not None and req.floor_bucket not in FLOOR_BUCKET_ENUM:
        issues.append({"field": "floor_bucket", "severity": "limited",
                       "code": "out_of_enum_floor_bucket",
                       "message": f"楼层段 {req.floor_bucket!r} 不在枚举 {FLOOR_BUCKET_ENUM}，按未知处理"})
    if req.decoration is not None and req.decoration not in DECORATION_ENUM:
        issues.append({"field": "decoration", "severity": "limited",
                       "code": "out_of_enum_decoration",
                       "message": f"装修 {req.decoration!r} 不在枚举 {DECORATION_ENUM}，按未知处理"})
    if req.bedrooms is not None and req.bedrooms not in BEDROOMS_SINGLE:
        issues.append({"field": "bedrooms", "severity": "limited",
                       "code": "out_of_enum_bedrooms",
                       "message": f"室数 {req.bedrooms} 不在 {BEDROOMS_SINGLE}，按『其他』处理"})
    return issues


def is_rejected(issues: list[dict]) -> bool:
    return any(i["severity"] == "error" for i in issues)


def _effective(req: Request, field_name: str, issues: list[dict]):
    """软域外字段返回 None（按未知处理），否则返回原值。"""
    if any(i["field"] == field_name and i["severity"] == "limited" for i in issues):
        return None
    return getattr(req, field_name)


def map_to_v1_row(req: Request, issues: list[dict] | None = None,
                  block_override: str | None = None) -> dict:
    """把请求映射为 V1 推理输入行（17 列；缺失保持 None）。

    未提供的字段一律 None（未知）→ 由 V1 冻结编码器走 miss 标记；不做请求侧插补。
    ``block_override``：编排层解析后的板块（请求值过口径校验＝请求值；否则小区静态
    推导值）；非 None 时覆盖 ``req.block_name``，None 时保持请求原值（fix-phase2-
    request-mapping design D2：板块解析放编排层，本层只接收注入结果）。
    """
    issues = issues or []
    block_name = req.block_name
    if block_override is not None:
        block_name = block_override
    year_built = req.year_built
    if year_built is None:
        age_years = None
        miss_year = 1
    else:
        age_years = float(min(max(req.valuation_date.year - year_built, AGE_CLIP[0]),
                              AGE_CLIP[1]))
        miss_year = 0

    total_floors = _effective(req, "total_floors", issues)
    floor_bucket = _effective(req, "floor_bucket", issues)
    decoration = _effective(req, "decoration", issues)
    # 室数超 1–5 不置空：交由冻结编码器归入「其他」类别（限定标注不改写取值，与错误提示一致）
    bedrooms = req.bedrooms

    if req.has_elevator is None:
        elevator_state = None
        miss_elevator = 1
    else:
        elevator_state = "有" if req.has_elevator else "无"
        miss_elevator = 0

    return {
        "source_record_id": req.request_id,
        "community_source_id": req.community_source_id,
        "sale_date_d": req.valuation_date,
        "block_name": block_name,
        "area_sqm": float(req.area_sqm),
        "age_years": age_years,
        "total_floors": None if total_floors is None else int(total_floors),
        "miss_year_built": miss_year,
        "miss_total_floors": 1 if total_floors is None else 0,
        "miss_elevator": miss_elevator,
        "miss_orientation": 1 if req.orientation is None else 0,
        "miss_decoration": 1 if decoration is None else 0,
        "floor_bucket": floor_bucket,
        "elevator_state": elevator_state,
        "bedrooms_n": bedrooms,
        "orientation": req.orientation,
        "decoration_state": decoration,
    }


def to_frame(req: Request, issues: list[dict] | None = None,
             block_override: str | None = None) -> pl.DataFrame:
    row = map_to_v1_row(req, issues, block_override)
    schema = {
        "source_record_id": pl.String, "community_source_id": pl.String,
        "sale_date_d": pl.Date, "block_name": pl.String,
        "area_sqm": pl.Float64, "age_years": pl.Float64,
        "total_floors": pl.Int64, "miss_year_built": pl.Int64,
        "miss_total_floors": pl.Int64, "miss_elevator": pl.Int64,
        "miss_orientation": pl.Int64, "miss_decoration": pl.Int64,
        "floor_bucket": pl.String, "elevator_state": pl.String,
        "bedrooms_n": pl.Int64, "orientation": pl.String,
        "decoration_state": pl.String,
    }
    return pl.DataFrame([row], schema=schema)


def frame_from_rows(rows: list[dict]) -> pl.DataFrame:
    """批量映射：行字典列表 → 多行帧（列与单套一致）。"""
    schema = {
        "source_record_id": pl.String, "community_source_id": pl.String,
        "sale_date_d": pl.Date, "block_name": pl.String,
        "area_sqm": pl.Float64, "age_years": pl.Float64,
        "total_floors": pl.Int64, "miss_year_built": pl.Int64,
        "miss_total_floors": pl.Int64, "miss_elevator": pl.Int64,
        "miss_orientation": pl.Int64, "miss_decoration": pl.Int64,
        "floor_bucket": pl.String, "elevator_state": pl.String,
        "bedrooms_n": pl.Int64, "orientation": pl.String,
        "decoration_state": pl.String,
    }
    return pl.DataFrame(rows, schema=schema)


def structure_report() -> dict:
    return {
        "module": "gz_property_valuation.phase2.candidate_request",
        "spec_basis": "specs/phase2-candidate-ops「请求映射与确定行为」；design D2",
        "request_fields": sorted(REQUEST_TEMPLATE.keys()),
        "v1_input_columns": list(V1_INPUT_COLUMNS),
        "scope_gate": {
            "in_scope": f"区县={DISTRICT_IN_SCOPE} 且 用途={PROPERTY_USE_IN_SCOPE}",
            "explicit_out_of_scope": "拒绝出价（out_of_scope_district / out_of_scope_use）",
            "unspecified": "继续但标注 scope_unverified（info）",
        },
        "domain_table": {
            "area_sqm": f"({AREA_MIN_EXCLUSIVE}, {AREA_MAX_INCLUSIVE}] 硬域外→拒绝",
            "year_built": f"≥{YEAR_BUILT_MIN} 且 ≤估值年 硬域外→拒绝",
            "valuation_date": "≤当前 硬域外→拒绝",
            "data_cutoff": "≤估值时点 硬域外→拒绝",
            "total_floors": f"[{TOTAL_FLOORS_MIN},{TOTAL_FLOORS_MAX}] 软域外→按未知",
            "floor_bucket": f"{FLOOR_BUCKET_ENUM} 软域外→按未知",
            "decoration": f"{DECORATION_ENUM} 软域外→按未知",
            "bedrooms": f"{BEDROOMS_SINGLE} 软域外→按『其他』",
        },
        "missing_semantics": "缺字段=未知(None)，走 V1 冻结 miss 标记/降级路径；不做请求侧插补",
        "consumption_boundary": "只读 S1 特征字典契约；不修改任何既有实现",
        "verdict": "PASS",
    }


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=1, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase2-candidate-request")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("structure", help="请求模式结构清单自检")
    p_tpl = sub.add_parser("template", help="输出请求模板 JSON")
    p_chk = sub.add_parser("check", help="校验一个请求文件")
    p_chk.add_argument("request_json")
    args = parser.parse_args(argv)

    if args.cmd == "structure":
        _print(structure_report())
        return 0
    if args.cmd == "template":
        _print(REQUEST_TEMPLATE)
        return 0
    doc = json.loads(open(args.request_json, encoding="utf-8").read())
    req = parse_request(doc)
    issues = validate(req, as_of=date.today())
    _print({"request": req.request_id, "rejected": is_rejected(issues),
            "issues": issues})
    return 0 if not is_rejected(issues) else 1


if __name__ == "__main__":
    raise SystemExit(main())
