"""确定性转录解析器（EXTFP3-D，技术方案 §7.4/§9.4/§10.4）。

把 EXTFP3-C 的 ``floorplan_ocr_word`` 逐词表，用本地确定性规则转录为
``floorplan_room_annotation`` 标注表：

1. **房间标签识别**（§9.4-4）：主卧/次卧/卧室/客厅/餐厅/厨房/卫生间/阳台/储物间等
   明确标签 → 标准房间类型；
2. **面积单位归一化**（§9.4-1/2）：㎡/m²/平方米/平米/平 → 规范单位 ``m2``
   （C 阶段 NFKC 已把 ㎡/m² 规范为 m2）；
3. **Decimal 解析**（§9.4-3）：数字用 ``Decimal`` 保留精度，字符串存储；
4. **距离/对齐/邻接关联**（§9.4-5）：按文字框质心距离把面积与最近房间标签关联；
   房间标签+面积在同一词条（如「主卧 12.5㎡」）直接合并；
5. **CONFLICT/NEEDS_REVIEW**（§9.4-6）：无法唯一关联（无房间标签、两房间等距、
   缺位置证据）→ ``NEEDS_REVIEW``；同一房间出现多个面积 → ``CONFLICT``；
6. **证据回指**（§9.4/§7.4）：已接受字段回指 ``word_id`` 与位置证据；回填
   ``participates_in_field``（room_name/area/room_name+area）；
7. **保留原文/规范值/解析规则版本**（§9.4-7）。

解析器**不得**（§9.4）：用 Excel 户型字段补写 OCR 没看见的面积；把房间面积之和
强行等于建筑面积；按房间数量推断未标注房间；把 Qwen 自然语言解释当作来源事实。

范围边界（EXTFP3-D 合同）：只做**确定性转录 + 标注表 + 四类测试（§10.4）**，
不触网、不付费。自动一致性检查/质量报告/verify/status CLI 属 EXTFP3-E；
真实 OCR 运行属 EXTFP3-F/H。``area_value`` 为 None 表示面积未知（ROOM_ONLY），
不算已接受数值字段。
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from math import hypot
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_ocr_parse import (
    COORD_VERSION,
    OCR_RESPONSE_PARSE_VERSION,
    OcrWordRecord,
    WordParseState,
)

# 转录解析器版本（写入每条标注记录）
# 1.1：EXTFP3-H 词表扩展——ROOM_ALIASES 增加 卫/淋浴间 → bathroom（H3/H6/H7 诊断证据驱动）。
# 1.2：OCRNEXT-C 字段安全最小修复（OCRNEXT-WP0 合同 §5-C / 方案 §6）——
#      ①同源重复词框去重（可证明重叠才合并，不可证明保留冲突）；
#      ②数字占用唯一性门禁（同一面积值被多房间接受而词框不足解释 → 全部 CONFLICT）；
#      ③跨运行不一致隔离入口（isolate_cross_run_inconsistencies，仅字段、规则由回放层驱动）。
#      注意：OCR 请求侧幂等键成分 OCR_PARSER_VERSION 保持 EXTFP3-B-NO-PARSER 不变，
#      跨运行任务身份可比；本版本只写入标注记录与标注 ID。
TRANSCRIBE_PARSER_VERSION = "OCRNEXT-C-1.2"

# 同源重复词框判据（OCRNEXT-C#①）：归一文本一致 + 包围盒交占小盒比例 ≥ 该值
# + 质心距离不超过 max(中心距绝对容差, 图片对角线相对容差)。宁可少合并（保留冲突），
# 不可错合并（把两间真实同类型房间并为一条）。
HOMOLOGOUS_IOU_MIN = 0.9
HOMOLOGOUS_CENTROID_ABS_PX = 2.0
HOMOLOGOUS_CENTROID_DIAG_RATIO = 0.01

# 标注表 staged 文件名（沿用 write_staged_asset_table 命名约定）
ANNOTATION_STAGED_FILENAME = "floorplan_room_annotation.parquet"

# 面积单位别名 → 规范单位（C 阶段 NFKC 已把 ㎡/m² 规范为 m2）
AREA_UNIT_ALIASES = {
    "㎡": "m2",
    "m²": "m2",
    "m2": "m2",
    "M2": "m2",
    "平方米": "m2",
    "平米": "m2",
    "平": "m2",
}

# 房间标签（标准类型, 原始标签）：特指优先，避免「主卧」被「卧室」先吞
ROOM_ALIASES: list[tuple[str, str]] = [
    ("主卧", "master_bedroom"),
    ("次卧", "secondary_bedroom"),
    ("主卫", "master_bathroom"),
    ("客厅", "living_room"),
    ("餐厅", "dining_room"),
    ("厨房", "kitchen"),
    ("卫生间", "bathroom"),
    ("洗手间", "bathroom"),
    ("厕所", "bathroom"),
    ("淋浴间", "bathroom"),
    ("卫", "bathroom"),
    ("阳台", "balcony"),
    ("储物间", "storage"),
    ("衣帽间", "walk_in_closet"),
    ("书房", "study"),
    ("玄关", "entrance"),
    ("卧室", "bedroom"),
]

# 面积数字 + 单位正则（NFKC 后 ㎡→m2；同时兼容原始 ㎡/m²/平方米）
_AREA_NUM_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>㎡|m²|m2|M2|平方米|平米|平)")

# 两房间标签与同一面积的质心距离差 ≤ 该值视为平局 → NEEDS_REVIEW
TIE_EPSILON = 1e-6

# 词条参与字段回填值
FIELD_ROOM = "room_name"
FIELD_AREA = "area"
FIELD_ROOM_AREA = "room_name+area"


class AnnotationState(StrEnum):
    """标注解析状态。

    - ``ACCEPTED``：房间+面积唯一关联，word_id 与位置证据齐全；
    - ``ROOM_ONLY``：只有房间标签，面积在图中未明确标注（面积保持未知）；
    - ``NEEDS_REVIEW``：面积无法唯一关联（无房间标签/两房间等距/缺位置证据）；
    - ``CONFLICT``：同一房间出现多个无法解释的面积；
    - ``OUT_OF_SCOPE``（EXTFP4）：范围外样本标注隔离——不自动接受、不入有效
      分母与有效字段统计；隔离原因与审计痕迹记录在 ``isolation_reason``。
    """

    ACCEPTED = "ACCEPTED"
    ROOM_ONLY = "ROOM_ONLY"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    CONFLICT = "CONFLICT"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"


class RoomAnnotationRecord(BaseModel):
    """一条房间标注记录（§7.4 floorplan_room_annotation，EXTFP3-D-1.0）。"""

    annotation_id: str
    ocr_run_id: str
    ocr_task_id: str
    parse_version: str = TRANSCRIBE_PARSER_VERSION
    room_word_id: str | None = None
    area_word_id: str | None = None
    room_name_raw: str | None = None
    room_name_normalized: str | None = None
    standard_room_type: str | None = None
    area_text_raw: str | None = None
    area_text_normalized: str | None = None
    area_value: str | None = Field(
        default=None, description="Decimal 面积值字符串（保留精度）；None=面积未知"
    )
    area_unit: str | None = None
    location: list[list[float]] = Field(default_factory=list, description="支撑词的规范化位置证据")
    parse_state: str = AnnotationState.NEEDS_REVIEW.value
    isolation_reason: str | None = Field(
        default=None,
        description="OCRNEXT-C 隔离原因：numeric_occupation_ambiguous / cross_run_inconsistent；"
        "None=未隔离。被隔离记录不得作为自动候选字段",
    )
    consistency_status: str | None = Field(default=None, description="EXTFP3-E 自动一致性检查回填")
    review_state: str | None = Field(default=None, description="人工复核状态（EXTFP3-G/H 回填）")
    review_event_ref: str | None = Field(
        default=None, description="复核事件引用（EXTFP3-G/H 回填）"
    )


class TranscribeRunStats(BaseModel):
    """一次转录运行的统计（供验收线面积召回/精确率与质量报告使用）。"""

    ocr_run_id: str | None = None
    tasks_transcribed: int = 0
    annotations_total: int = 0
    accepted: int = 0
    room_only: int = 0
    needs_review: int = 0
    conflict: int = 0
    accepted_rate: float | None = Field(description="已接受标注占比 = accepted / annotations_total")
    table_path: str | None = None
    word_participation_path: str | None = None


# ---------------------------------------------------------------------------
# 房间标签 / 面积识别（§9.4-2/3/4）
# ---------------------------------------------------------------------------


def classify_room(text: str) -> tuple[str, str] | None:
    """识别房间标签，返回 ``(标准类型, 命中的原始标签)``；未命中返回 None。

    特指优先：按 ``ROOM_ALIASES`` 顺序，先匹配更具体的标签
    （如「主卧」先于「卧室」，避免「主卧室」被判成 bedroom）。
    """
    for label, std in ROOM_ALIASES:
        if label in text:
            return std, label
    return None


def parse_area(text: str) -> tuple[Decimal, str] | None:
    """从文本提取 ``(面积 Decimal, 规范单位 m2)``；无面积单位返回 None。

    不把「3室2厅」等房间数/门牌号当作面积（无单位不进面积候选），
    宁可返回未知，不接受错误数字（§10.3 验收线）。
    """
    match = _AREA_NUM_RE.search(text)
    if not match:
        return None
    unit = AREA_UNIT_ALIASES.get(match.group("unit"), "m2")
    try:
        return Decimal(match.group("value")), unit
    except InvalidOperation:
        return None


# ---------------------------------------------------------------------------
# 关联规则（§9.4-5/6）
# ---------------------------------------------------------------------------


def word_centroid(word: OcrWordRecord) -> tuple[float, float] | None:
    """文字框质心（location 多边形顶点均值）；无位置证据返回 None。"""
    pts = word.location
    if not pts:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return (sum(xs) / len(xs), sum(ys) / len(ys))


def _bbox(points: list[list[float]]) -> tuple[float, float, float, float]:
    """点集包围盒 (min_x, min_y, max_x, max_y)。"""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return (min(xs), min(ys), max(xs), max(ys))


def bbox_overlap_over_smaller(a: list[list[float]], b: list[list[float]]) -> float:
    """两包围盒交集面积占较小盒面积的比例（0..1）。

    退化盒（线/点，面积为 0）用长度/近似处理：交集退化为 0 时返回 0；
    两盒都退化为点时按点重合（1.0）或相离（0.0）。
    """
    ax1, ay1, ax2, ay2 = _bbox(a)
    bx1, by1, bx2, by2 = _bbox(b)
    ix = max(0.0, min(ax2, bx2) - max(ax1, bx1))
    iy = max(0.0, min(ay2, by2) - max(ay1, by1))
    area_a = (ax2 - ax1) * (ay2 - ay1)
    area_b = (bx2 - bx1) * (by2 - by1)
    if area_a <= 0.0 and area_b <= 0.0:
        return 1.0 if ix <= 1e-9 and iy <= 1e-9 and (ax1 == bx1 and ay1 == by1) else 0.0
    smaller = min(area_a, area_b)
    if smaller <= 0.0:
        # 一方退化：退化盒的中心是否落在另一盒内（含边界）
        cx = (bx1 + bx2) / 2 if area_b <= 0 else (ax1 + ax2) / 2
        cy = (by1 + by2) / 2 if area_b <= 0 else (ay1 + ay2) / 2
        ox1, oy1, ox2, oy2 = (ax1, ay1, ax2, ay2) if area_b <= 0 else (bx1, by1, bx2, by2)
        return 1.0 if ox1 - 1e-9 <= cx <= ox2 + 1e-9 and oy1 - 1e-9 <= cy <= oy2 + 1e-9 else 0.0
    return (ix * iy) / smaller


def _image_diag(words: list[OcrWordRecord]) -> float:
    """图片对角线估计（有宽高用宽高；否则用全体词框跨度），供质心相对容差。"""
    for w in words:
        if w.image_width and w.image_height:
            return hypot(float(w.image_width), float(w.image_height))
    pts = [p for w in words for p in w.location]
    if not pts:
        return 0.0
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return hypot(max(xs) - min(xs), max(ys) - min(ys))


def _are_homologous_duplicates(a: OcrWordRecord, b: OcrWordRecord, diag: float) -> bool:
    """可证明的同源重复词框：归一文本一致 + 包围盒近完全重叠 + 质心足够近。

    任一条件不成立（含缺位置证据）都返回 False——保留冲突，不合并成确定事实。
    """
    if a.text_normalized != b.text_normalized:
        return False
    if not a.location or not b.location:
        return False
    if bbox_overlap_over_smaller(a.location, b.location) < HOMOLOGOUS_IOU_MIN:
        return False
    ca = word_centroid(a)
    cb = word_centroid(b)
    if ca is None or cb is None:
        return False
    dist = hypot(ca[0] - cb[0], ca[1] - cb[1])
    tolerance = max(HOMOLOGOUS_CENTROID_ABS_PX, diag * HOMOLOGOUS_CENTROID_DIAG_RATIO)
    return dist <= tolerance


def dedupe_homologous_words(words: list[OcrWordRecord]) -> list[OcrWordRecord]:
    """OCRNEXT-C#①：消除可证明重叠/重复的同源词框（如「卫生间B」被检测 3 次）。

    按 ``(ocr_task_id, text_normalized)`` 分组，组内贪心聚类：与已保留代表可证明
    同源者丢弃（保留 order 最小的代表）；不可证明者保留为独立词条（真实同类型
    多房间、重叠不足或无位置证据都保持冲突原样）。输出保持输入相对顺序，确定性。

    数据驱动边界（OCRNEXT-C 双轮 300 张回放证据）：同名多词框组共 48 组、除 1 组
    （9.71px 但 IoU 0.39）外组内词框质心距离全部 >50px——真实多房间（如「卧室」×N
    无编号标注）是同名组的主导形态，故「同名而不可证明重叠」不触发降级，仅可证明
    重叠才合并；真实数据中本规则只合并水印/房号等非字段词（22/21 词框）。H4 的 5
    条候选层超额 claim 均为同名真实多房间，不由去重/门禁关闭，如实移交后续判定
    （两个口径的 room precision 见回放报告）。
    """
    diag = _image_diag(words)
    kept: list[OcrWordRecord] = []
    # 组键 → 该键下已保留的代表词条列表
    representatives: dict[tuple[str, str], list[OcrWordRecord]] = {}
    for w in sorted(words, key=lambda x: (x.ocr_task_id, x.order, x.word_id)):
        key = (w.ocr_task_id, w.text_normalized)
        reps = representatives.setdefault(key, [])
        duplicate = any(_are_homologous_duplicates(rep, w, diag) for rep in reps)
        if duplicate:
            continue
        reps.append(w)
        kept.append(w)
    kept.sort(key=lambda x: (x.ocr_task_id, x.order, x.word_id))
    return [w for w in words if any(w is k for k in kept)]


def apply_numeric_occupation_gate(
    records: list[RoomAnnotationRecord],
) -> list[RoomAnnotationRecord]:
    """OCRNEXT-C#②：同一数字不得同时成为多个房间的已接受面积。

    对 ACCEPTED 记录按 ``area_value`` 分组：组内被接受到的**不同房间词条数** >
    支撑该值的**不同面积词条数**时，说明存在数字被多房间重复 claim、无法唯一解释
    ——该组全部降级 CONFLICT + ``isolation_reason=numeric_occupation_ambiguous``。
    词框数与房间数相等的组（如两间真同面积各有独立词框）保持原样。只降级，不删除
    记录（正确证据不丢失）。**原地改写入参对象**并返回同一列表（与③的不可变
    语义不同，RV-OCRNEXT-C-01#N6）。

    实证边界（RV-OCRNEXT-C-01#F1）：双轮回放 600 任务触发 0 次——``transcribe_words``
    把每个面积词框唯一分配给一个房间，组内两类计数恒等，该条件在当前生产入口路径
    上结构性不可达；#247「卧室E=18.3」的关闭实际由修复③（双轮值不一致→隔离）完成。
    本规则是为「多房争一数」形态预留的防线；「确定性误关联且双轮一致」形态目前
    无规则防护，该缺口移交 D/E 与包级判定，不得据此宣称已覆盖。
    """
    by_value: dict[str, list[RoomAnnotationRecord]] = {}
    for ann in records:
        if (
            ann.parse_state == AnnotationState.ACCEPTED.value
            and ann.area_value is not None
            and ann.area_word_id is not None
        ):
            by_value.setdefault(ann.area_value, []).append(ann)
    for group in by_value.values():
        rooms = {a.room_word_id for a in group if a.room_word_id is not None}
        area_words = {a.area_word_id for a in group if a.area_word_id is not None}
        if len(rooms) > len(area_words):
            for ann in group:
                ann.parse_state = AnnotationState.CONFLICT.value
                ann.isolation_reason = "numeric_occupation_ambiguous"
    return records


def cross_run_field_key(ann: RoomAnnotationRecord) -> tuple[str, str, str]:
    """跨运行比较的字段身份：(标准房间类型, 房间归一名称, 规范面积值)。

    规范值和证据身份均一致才可跨运行配对（方案 §6.3）；面积值 None 用空串占位。
    """
    return (
        ann.standard_room_type or ann.room_name_normalized or "?",
        ann.room_name_normalized or "?",
        ann.area_value if ann.area_value is not None else "",
    )


def isolate_cross_run_inconsistencies(
    annotations: list[RoomAnnotationRecord],
    reference: list[RoomAnnotationRecord],
) -> tuple[list[RoomAnnotationRecord], list[RoomAnnotationRecord]]:
    """OCRNEXT-C#③：同一图片两个冻结运行中，不一致字段自动隔离，不进候选接受层。

    对 ``annotations`` 的 ACCEPTED 记录，与 ``reference``（同一 ocr_task_id 的另一
    冻结运行的标注）按 :func:`cross_run_field_key` 做多重集比较：超出交集计数的
    成员降级 NEEDS_REVIEW + ``isolation_reason=cross_run_inconsistent``。
    reference 中完全缺席的任务（无对照依据）不隔离，保持原状态（与
    ``check_repeat_consistency`` 的「仅出现在一次运行的任务不进分母」口径一致）。
    返回 ``(adjusted, isolated)``；adjusted 为新列表（model_copy，不改入参对象），
    isolated 为被隔离记录副本，供报告列清单。
    """
    from collections import Counter

    ref_accepted: dict[str, Counter[tuple[str, str, str]]] = {}
    ref_tasks: set[str] = set()
    for ann in reference:
        ref_tasks.add(ann.ocr_task_id)
        if ann.parse_state == AnnotationState.ACCEPTED.value:
            ref_accepted.setdefault(ann.ocr_task_id, Counter())[cross_run_field_key(ann)] += 1

    budget: dict[str, Counter[tuple[str, str, str]]] = {
        task: counter.copy() for task, counter in ref_accepted.items()
    }
    adjusted: list[RoomAnnotationRecord] = []
    isolated: list[RoomAnnotationRecord] = []
    for ann in annotations:
        if ann.parse_state != AnnotationState.ACCEPTED.value or ann.ocr_task_id not in ref_tasks:
            adjusted.append(ann)
            continue
        key = cross_run_field_key(ann)
        remaining = budget.setdefault(ann.ocr_task_id, Counter())
        if remaining[key] > 0:
            remaining[key] -= 1
            adjusted.append(ann)
        else:
            drop = ann.model_copy(
                update={
                    "parse_state": AnnotationState.NEEDS_REVIEW.value,
                    "isolation_reason": "cross_run_inconsistent",
                }
            )
            isolated.append(drop)
            adjusted.append(drop)
    return adjusted, isolated


def _annotation_id(
    *,
    ocr_run_id: str,
    ocr_task_id: str,
    order: int,
    room_word_id: str | None,
    area_word_id: str | None,
) -> str:
    """确定性标注 ID：同一输入 + 解析器版本可复现。"""
    key = "|".join([ocr_run_id, ocr_task_id, str(order), room_word_id or "", area_word_id or ""])
    return hashlib.sha256((key + "|" + TRANSCRIBE_PARSER_VERSION).encode("utf-8")).hexdigest()


def _word_attr(word: OcrWordRecord | None, attr: str) -> str | None:
    """取词条属性；词条为 None 时返回 None（避免长三元表达式）。"""
    if word is None:
        return None
    value = getattr(word, attr)
    return str(value) if value is not None else None


def _make_record(
    *,
    room_word: OcrWordRecord | None,
    area_word: OcrWordRecord | None,
    standard_room_type: str | None,
    area_value: str | None,
    area_unit: str | None,
    state: str,
) -> RoomAnnotationRecord:
    """统一构造标注记录：回指 word_id、位置证据、原文/规范值。"""
    order = room_word.order if room_word is not None else (area_word.order if area_word else 0)
    room_name_raw = room_word.text_raw if room_word is not None else None
    room_name_norm = room_word.text_normalized if room_word is not None else None
    area_raw = area_word.text_raw if area_word is not None else None
    area_norm = area_word.text_normalized if area_word is not None else None
    location: list[list[float]] = []
    if room_word is not None and room_word.location:
        location = room_word.location
    elif area_word is not None and area_word.location:
        location = area_word.location
    ocr_run_id = _word_attr(room_word, "ocr_run_id") or _word_attr(area_word, "ocr_run_id") or ""
    ocr_task_id = _word_attr(room_word, "ocr_task_id") or _word_attr(area_word, "ocr_task_id") or ""
    return RoomAnnotationRecord(
        annotation_id=_annotation_id(
            ocr_run_id=ocr_run_id,
            ocr_task_id=ocr_task_id,
            order=order,
            room_word_id=room_word.word_id if room_word else None,
            area_word_id=area_word.word_id if area_word else None,
        ),
        ocr_run_id=ocr_run_id,
        ocr_task_id=ocr_task_id,
        room_word_id=room_word.word_id if room_word else None,
        area_word_id=area_word.word_id if area_word else None,
        room_name_raw=room_name_raw,
        room_name_normalized=room_name_norm,
        standard_room_type=standard_room_type,
        area_text_raw=area_raw,
        area_text_normalized=area_norm,
        area_value=area_value,
        area_unit=area_unit,
        location=location,
        parse_state=state,
    )


def transcribe_words(words: list[OcrWordRecord]) -> list[RoomAnnotationRecord]:
    """把一张图片的逐词表转录为房间标注记录（纯函数、确定性、不触网）。

    OCRNEXT-C 前置/后置两道字段安全门（合同 §5-C）：
    - 前置：同源重复词框去重（修复①，不可证明者原样保留）；
    - 后置：数字占用唯一性门禁（修复②，词框不足以唯一解释的已接受面积全部 CONFLICT）。

    关联规则（§9.4-5/6）：
    - 房间标签+面积在同一词条 → 直接合并 ACCEPTED；
    - 面积词条按质心距离关联到最近房间标签 → ACCEPTED；
    - 面积词条无房间标签 / 两房间等距 / 缺位置证据 → NEEDS_REVIEW；
    - 同一房间收到 ≥2 个面积 → 全部 CONFLICT（无法解释的多面积）；
    - 房间标签无面积 → ROOM_ONLY（面积保持未知，不推断）。
    """
    words = dedupe_homologous_words(words)
    records: list[RoomAnnotationRecord] = []
    rooms: list[OcrWordRecord] = []
    areas: list[OcrWordRecord] = []

    for w in words:
        room = classify_room(w.text_normalized)
        area = parse_area(w.text_normalized)
        if room is not None and area is not None:
            # 合并词条：房间标签 + 面积同词 → 直接 ACCEPTED（缺位置证据则复核）
            state = (
                AnnotationState.ACCEPTED.value
                if word_centroid(w) is not None
                else AnnotationState.NEEDS_REVIEW.value
            )
            records.append(
                _make_record(
                    room_word=w,
                    area_word=w,
                    standard_room_type=room[0],
                    area_value=str(area[0]),
                    area_unit=area[1],
                    state=state,
                )
            )
        elif room is not None:
            rooms.append(w)
        elif area is not None:
            areas.append(w)

    # 面积词条 → 最近房间标签质心关联
    room_centroids: list[tuple[OcrWordRecord, tuple[float, float] | None]] = [
        (rw, word_centroid(rw)) for rw in rooms
    ]
    assigned: dict[int, list[OcrWordRecord]] = {}
    unassigned: list[OcrWordRecord] = []
    for aw in areas:
        ac = word_centroid(aw)
        if ac is None:
            unassigned.append(aw)
            continue
        best_idx = -1
        best_dist = float("inf")
        tie = False
        for i, (_rw, rc) in enumerate(room_centroids):
            if rc is None:
                continue
            d = hypot(ac[0] - rc[0], ac[1] - rc[1])
            if d < best_dist - TIE_EPSILON:
                best_dist = d
                best_idx = i
                tie = False
            elif abs(d - best_dist) <= TIE_EPSILON:
                tie = True
        if best_idx == -1 or tie:
            unassigned.append(aw)
        else:
            assigned.setdefault(best_idx, []).append(aw)

    # 房间记录 + 面积记录（按每房间面积数定状态）
    for i, (rw, rc) in enumerate(room_centroids):
        std, _label = classify_room(rw.text_normalized) or ("", "")
        areas_here = assigned.get(i, [])
        if rc is None:
            records.append(
                _make_record(
                    room_word=rw,
                    area_word=None,
                    standard_room_type=std,
                    area_value=None,
                    area_unit=None,
                    state=AnnotationState.NEEDS_REVIEW.value,
                )
            )
        elif not areas_here:
            records.append(
                _make_record(
                    room_word=rw,
                    area_word=None,
                    standard_room_type=std,
                    area_value=None,
                    area_unit=None,
                    state=AnnotationState.ROOM_ONLY.value,
                )
            )
        elif len(areas_here) == 1:
            aw = areas_here[0]
            area_value, area_unit = parse_area(aw.text_normalized) or (None, None)
            records.append(
                _make_record(
                    room_word=rw,
                    area_word=aw,
                    standard_room_type=std,
                    area_value=str(area_value) if area_value is not None else None,
                    area_unit=area_unit,
                    state=AnnotationState.ACCEPTED.value,
                )
            )
        else:
            # 同一房间多个面积 → 全部 CONFLICT
            for aw in areas_here:
                area_value, area_unit = parse_area(aw.text_normalized) or (None, None)
                records.append(
                    _make_record(
                        room_word=rw,
                        area_word=aw,
                        standard_room_type=std,
                        area_value=str(area_value) if area_value is not None else None,
                        area_unit=area_unit,
                        state=AnnotationState.CONFLICT.value,
                    )
                )
            records.append(
                _make_record(
                    room_word=rw,
                    area_word=None,
                    standard_room_type=std,
                    area_value=None,
                    area_unit=None,
                    state=AnnotationState.CONFLICT.value,
                )
            )

    # 无法唯一关联的面积 → NEEDS_REVIEW
    for aw in unassigned:
        area_value, area_unit = parse_area(aw.text_normalized) or (None, None)
        records.append(
            _make_record(
                room_word=None,
                area_word=aw,
                standard_room_type=None,
                area_value=str(area_value) if area_value is not None else None,
                area_unit=area_unit,
                state=AnnotationState.NEEDS_REVIEW.value,
            )
        )

    return apply_numeric_occupation_gate(records)


# ---------------------------------------------------------------------------
# word 参与字段回填（§7.3 participates_in_field，EXTFP3-C 约定 D 回填）
# ---------------------------------------------------------------------------


def backfill_word_participation(
    annotations: list[RoomAnnotationRecord],
    words: list[OcrWordRecord],
) -> dict[str, str | None]:
    """按标注记录回填每个 word 的 ``participates_in_field``。

    返回 {word_id: participates_in_field}；未参与任何已接受字段的词条为 None。
    只有 ACCEPTED/ROOM_ONLY 的房间词与 ACCEPTED 的面积词计入参与；CONFLICT 的
    多面积词不视为有效已接受字段（保持 None，由 E 阶段一致性检查承接）。
    """
    participation: dict[str, str | None] = {w.word_id: None for w in words}
    for ann in annotations:
        if ann.room_word_id is not None and ann.parse_state in (
            AnnotationState.ACCEPTED.value,
            AnnotationState.ROOM_ONLY.value,
        ):
            current = participation.get(ann.room_word_id)
            if current == FIELD_AREA:
                participation[ann.room_word_id] = FIELD_ROOM_AREA
            elif current != FIELD_ROOM_AREA:
                participation[ann.room_word_id] = FIELD_ROOM
        if ann.area_word_id is not None and ann.parse_state == AnnotationState.ACCEPTED.value:
            current = participation.get(ann.area_word_id)
            if current == FIELD_ROOM:
                participation[ann.area_word_id] = FIELD_ROOM_AREA
            elif current != FIELD_ROOM_AREA:
                participation[ann.area_word_id] = FIELD_AREA
    return participation


# ---------------------------------------------------------------------------
# 运行级转录 + 标注表落盘（§5 数据边界 / §7.4）
# ---------------------------------------------------------------------------


def compute_annotation_stats(
    annotations: list[RoomAnnotationRecord],
    *,
    tasks_transcribed: int,
    ocr_run_id: str | None = None,
) -> TranscribeRunStats:
    """聚合转录统计（按状态计数 + 已接受占比）。"""
    total = len(annotations)
    accepted = sum(1 for a in annotations if a.parse_state == AnnotationState.ACCEPTED.value)
    room_only = sum(1 for a in annotations if a.parse_state == AnnotationState.ROOM_ONLY.value)
    needs_review = sum(
        1 for a in annotations if a.parse_state == AnnotationState.NEEDS_REVIEW.value
    )
    conflict = sum(1 for a in annotations if a.parse_state == AnnotationState.CONFLICT.value)
    return TranscribeRunStats(
        ocr_run_id=ocr_run_id,
        tasks_transcribed=tasks_transcribed,
        annotations_total=total,
        accepted=accepted,
        room_only=room_only,
        needs_review=needs_review,
        conflict=conflict,
        accepted_rate=(accepted / total) if total else None,
    )


def read_word_table(word_table_path: Path) -> list[OcrWordRecord]:
    """读取 ``floorplan_ocr_word.parquet``（EXTFP3-C 产物）为词条列表。

    同时读取 ``parse_version``/``response_parse_state``，供参与字段回填时按原值写回
    （RV-EXTFP3-D-01#F1：不静默覆盖原表的 partial/needs_review/failed 响应级状态）。
    """
    import pyarrow.parquet as pq

    table = pq.read_table(word_table_path)
    words: list[OcrWordRecord] = []
    for row in table.to_pylist():
        location = _json_to_list(row.get("location"))
        rotate_rect = _json_to_dict(row.get("rotate_rect"))
        words.append(
            OcrWordRecord(
                word_id=row["word_id"],
                ocr_run_id=row["ocr_run_id"],
                ocr_task_id=row["ocr_task_id"],
                order=row["order"],
                text_raw=row["text_raw"],
                text_normalized=row["text_normalized"],
                location=location,
                rotate_rect=rotate_rect,
                image_width=row.get("image_width"),
                image_height=row.get("image_height"),
                coord_version=row.get("coord_version") or COORD_VERSION,
                parse_state=row.get("parse_state") or WordParseState.PARSED.value,
                participates_in_field=row.get("participates_in_field"),
                parse_version=row.get("parse_version") or OCR_RESPONSE_PARSE_VERSION,
                response_parse_state=row.get("response_parse_state") or "parsed",
            )
        )
    return words


def _json_to_list(value: Any) -> list[list[float]]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return value if isinstance(value, list) else []


def _json_to_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return value if isinstance(value, dict) else None


def transcribe_word_table(
    word_table_path: Path,
    data_dir: Path | None = None,
) -> TranscribeRunStats:
    """转录一次 OCR 运行的逐词表为标注表，原子落盘标注表 + 参与字段回填。

    - 按 ``ocr_task_id`` 分组逐张转录（纯函数 ``transcribe_words``）；
    - 若给 ``data_dir``：原子写 ``staged/floorplan_room_annotation.parquet``，
      并回填 ``staged/floorplan_ocr_word.parquet`` 的 ``participates_in_field``。
    """
    words = read_word_table(word_table_path)
    ocr_run_id = words[0].ocr_run_id if words else None
    by_task: dict[str, list[OcrWordRecord]] = {}
    for w in words:
        by_task.setdefault(w.ocr_task_id, []).append(w)

    all_annotations: list[RoomAnnotationRecord] = []
    for _tid, wl in by_task.items():
        all_annotations.extend(transcribe_words(wl))

    stats = compute_annotation_stats(
        all_annotations,
        tasks_transcribed=len(by_task),
        ocr_run_id=ocr_run_id,
    )

    if data_dir is not None:
        stats.table_path = write_annotation_table(all_annotations, data_dir).as_posix()
        participation = backfill_word_participation(all_annotations, words)
        stats.word_participation_path = write_word_participation(
            words, participation, data_dir
        ).as_posix()
    return stats


def write_annotation_table(
    annotations: list[RoomAnnotationRecord],
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """原子写 ``staged/floorplan_room_annotation.parquet``（沿用资产表约定）。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict[str, object]] = []
    for a in annotations:
        rows.append(
            {
                "annotation_id": a.annotation_id,
                "ocr_run_id": a.ocr_run_id,
                "ocr_task_id": a.ocr_task_id,
                "parse_version": a.parse_version,
                "room_word_id": a.room_word_id,
                "area_word_id": a.area_word_id,
                "room_name_raw": a.room_name_raw,
                "room_name_normalized": a.room_name_normalized,
                "standard_room_type": a.standard_room_type,
                "area_text_raw": a.area_text_raw,
                "area_text_normalized": a.area_text_normalized,
                "area_value": a.area_value,
                "area_unit": a.area_unit,
                "location": json.dumps(a.location, ensure_ascii=False),
                "parse_state": a.parse_state,
                "isolation_reason": a.isolation_reason,
                "consistency_status": a.consistency_status,
                "review_state": a.review_state,
                "review_event_ref": a.review_event_ref,
            }
        )
    table = pa.Table.from_pylist(rows)
    staged_dir = data_dir / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / ANNOTATION_STAGED_FILENAME
    work_path = staged_dir / (ANNOTATION_STAGED_FILENAME + ".incomplete")
    pq.write_table(table, work_path, compression=compression)
    work_path.replace(final_path)
    return final_path


def write_word_participation(
    words: list[OcrWordRecord],
    participation: dict[str, str | None],
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """回填 ``staged/floorplan_ocr_word.parquet`` 的 ``participates_in_field``。

    沿用 C 阶段逐词表列结构，仅新增参与字段值；原子替换，不半写。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict[str, object]] = []
    for w in words:
        rows.append(
            {
                "word_id": w.word_id,
                "ocr_run_id": w.ocr_run_id,
                "ocr_task_id": w.ocr_task_id,
                "order": w.order,
                "text_raw": w.text_raw,
                "text_normalized": w.text_normalized,
                "location": json.dumps(w.location, ensure_ascii=False),
                "rotate_rect": json.dumps(w.rotate_rect, ensure_ascii=False)
                if w.rotate_rect is not None
                else None,
                "image_width": w.image_width,
                "image_height": w.image_height,
                "coord_version": w.coord_version,
                "parse_state": w.parse_state,
                "participates_in_field": participation.get(w.word_id),
                "parse_version": w.parse_version,
                "response_parse_state": w.response_parse_state,
            }
        )
    table = pa.Table.from_pylist(rows)
    staged_dir = data_dir / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / "floorplan_ocr_word.parquet"
    work_path = staged_dir / (final_path.name + ".incomplete")
    pq.write_table(table, work_path, compression=compression)
    work_path.replace(final_path)
    return final_path


__all__ = [
    "ANNOTATION_STAGED_FILENAME",
    "AREA_UNIT_ALIASES",
    "HOMOLOGOUS_CENTROID_ABS_PX",
    "HOMOLOGOUS_CENTROID_DIAG_RATIO",
    "HOMOLOGOUS_IOU_MIN",
    "AnnotationState",
    "ROOM_ALIASES",
    "RoomAnnotationRecord",
    "TRANSCRIBE_PARSER_VERSION",
    "TranscribeRunStats",
    "apply_numeric_occupation_gate",
    "backfill_word_participation",
    "bbox_overlap_over_smaller",
    "classify_room",
    "compute_annotation_stats",
    "cross_run_field_key",
    "dedupe_homologous_words",
    "isolate_cross_run_inconsistencies",
    "parse_area",
    "read_word_table",
    "transcribe_word_table",
    "transcribe_words",
    "word_centroid",
    "write_annotation_table",
    "write_word_participation",
]
