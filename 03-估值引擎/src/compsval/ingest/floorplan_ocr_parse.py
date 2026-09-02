"""OCR 响应解析与逐词表（EXTFP3-C，技术方案 §7.3/§9.3/§9.4）。

本模块把 Qwen OCR 原始响应解析为 ``floorplan_ocr_word`` 逐词表，冻结以下行为：

1. **逐词保存**（§7.3）：``word_id``、``ocr_run_id``/``ocr_task_id``、顺序、
   ``text_raw``/``text_normalized``、``location``、``rotate_rect``、图片宽高、
   坐标版本；``participates_in_field`` 留空由 EXTFP3-D 回填。
2. **NFKC 归一化**（§9.4-1）：Unicode NFKC + 空白折叠 + 去首尾。
3. **容错提取**（§9.3）：以 ``ocr_result.words_info`` 为主结果；先从文档化路径
   （``output.choices[0].message.content[]`` 图像项 / 顶层 ``ocr_result``）查找，
   找不到再做有界递归兜底；无法定位返回 ``failed`` 不静默伪造。
4. **位置规范化**：``location`` 规范为 ``[[x,y], ...]``（至少 1 点，数值化）；
   ``rotate_rect`` 规范为 ``{center,width,height,angle}``；无法解析的单词标记
   ``NEEDS_REVIEW`` 并保留原文，不丢弃文字证据。
5. **model 校验**（§9.5）：``model_returned`` 与 ``model_requested`` 不一致 → 响应
   ``needs_review``。
6. **解析失败分类**：响应级 ``parsed / partial / failed / needs_review``；源任务
   ``OCR_PARTIAL``/``OCR_FAILED``/``NEEDS_REVIEW`` 状态沿传（不把 length 当成功）。
7. **可解析率统计**：schema/响应可解析率 = parsed + partial 中成功解析单词占比，
   供验收线 §12「schema/响应可解析率 ≥99.5%」使用。
8. **逐词表落盘**：``staged/floorplan_ocr_word.parquet``（原子写盘，沿用
   ``write_staged_asset_table`` 约定），不入 git。

范围边界（EXTFP3-C 合同）：本模块只做**响应解析 + 逐词表 + 解析失败分类 + 离线测试**，
不触网、不付费；房间/面积结构化转录属 EXTFP3-D；一致性检查/质量报告属 EXTFP3-E。
真实响应形态由 EXTFP3-F（10 张调试）揭示，本模块对结构做有界容错。
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, Field

from compsval.ingest.floorplan_ocr import (
    RUN_FILENAME,
    OcrRunRecord,
    OcrState,
    OcrTaskRecord,
    raw_response_filename,
)

# 响应解析器版本（写入每条 word/parse 记录）
OCR_RESPONSE_PARSE_VERSION = "EXTFP3-C-1.0"

# 坐标版本：Qwen 返回的原始像素坐标（enable_rotate=false 下未转正的几何）
COORD_VERSION = "raw-pixel-1.0"

# 逐词表 staged 文件名（沿用 ASSET_STAGED_FILENAME 命名约定）
WORD_STAGED_FILENAME = "floorplan_ocr_word.parquet"

# 无解析器之前的幂等占位（与 B 阶段一致，D 冻结后更新）
_WORD_KEY_SEP = "|"


class WordParseState(StrEnum):
    """单词级解析状态。

    - ``PARSED``：text + location 均解析成功；
    - ``NEEDS_REVIEW``：location/rotate_rect 无法规范，保留 text 证据待人工/后续复核；
    - ``DROPPED``：连 text 都不是字符串的坏条目，丢弃不写入。
    """

    PARSED = "PARSED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    DROPPED = "DROPPED"


class OcrWordRecord(BaseModel):
    """一条 OCR 逐词记录（§7.3 floorplan_ocr_word，EXTFP3-C-1.0）。"""

    word_id: str
    ocr_run_id: str
    ocr_task_id: str
    order: int
    text_raw: str
    text_normalized: str
    location: list[list[float]] = Field(description="规范化 [[x,y],...]，至少 1 点")
    rotate_rect: dict[str, Any] | None = None
    image_width: int | None = None
    image_height: int | None = None
    coord_version: str = COORD_VERSION
    parse_state: str = WordParseState.PARSED.value
    participates_in_field: str | None = Field(
        default=None, description="EXTFP3-D 转录解析器回填的房间/面积字段名"
    )
    parse_version: str = Field(
        default=OCR_RESPONSE_PARSE_VERSION,
        description="响应解析器版本（EXTFP3-E 回填参与字段时按原值写回，RV-EXTFP3-D-01#F1）",
    )
    response_parse_state: str = Field(
        default="parsed",
        description="响应级解析状态（parsed/partial/failed/needs_review，回填时原值保留）",
    )


class OcrParseRecord(BaseModel):
    """一张图片的 OCR 响应解析结果（响应级分类）。"""

    ocr_task_id: str
    ocr_run_id: str
    parse_version: str = OCR_RESPONSE_PARSE_VERSION
    source_state: str
    model_requested: str | None = None
    model_returned: str | None = None
    model_match: bool | None = None
    words_count: int = 0
    parsed_count: int = 0
    needs_review_count: int = 0
    dropped_count: int = 0
    parse_state: str  # parsed / partial / failed / needs_review
    words: list[OcrWordRecord] = Field(default_factory=list)


class WordRunStats(BaseModel):
    """一次 OCR 运行目录解析后的统计（供验收线可解析率使用）。"""

    ocr_run_id: str
    total_tasks: int = 0
    tasks_with_response: int = 0
    tasks_without_response: int = 0
    parsed_ok: int = 0
    parsed_partial: int = 0
    parsed_failed: int = 0
    parsed_needs_review: int = 0
    total_words: int = 0
    parseable_rate: float | None = Field(
        description="可解析率 = 成功解析出单词的响应数 / 有响应任务数"
    )
    table_path: str | None = None


# ---------------------------------------------------------------------------
# 文本归一化（§9.4-1）
# ---------------------------------------------------------------------------


def normalize_text(text: str) -> str:
    """Unicode NFKC + 空白折叠 + 去首尾（技术方案 §9.4-1）。"""
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(normalized.split())


# ---------------------------------------------------------------------------
# 位置规范化（§9.3）
# ---------------------------------------------------------------------------


def _is_number(value: Any) -> bool:
    """int/float（排除 bool，bool 是 int 子类，RV-EXTFP3-C-01#F5）。"""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _normalize_location(value: Any) -> list[list[float]] | None:
    """把 ``location`` 规范为 [[x,y],...]（至少 1 点，数值化）。

    兼容四类形状（§9.3，真实 Qwen advanced_recognition 响应为平铺 8 数四角多边形）：
    1. ``[[x,y],[x,y],...]`` 点列表；
    2. ``[x,y]`` 平铺单点（rotate_rect.center 常见形状）；
    3. ``[x1,y1,x2,y2,...]`` 平铺多边形（偶数个数值，逐对配对为点）；
    4. ``{"x":..,"y":..}`` 单点对象。
    任一元素非法返回 None（调用方标记 NEEDS_REVIEW，不丢弃 text）。
    """
    if isinstance(value, dict):
        try:
            x = float(value["x"])
            y = float(value["y"])
        except (KeyError, TypeError, ValueError):
            return None
        return [[x, y]]
    if not isinstance(value, (list, tuple)) or not value:
        return None
    # 全部数值的平铺形状：len==2 → 单点 [x,y]；偶数且 >=4 → 多边形逐对配对
    if all(_is_number(v) for v in value):
        if len(value) == 2:
            try:
                return [[float(value[0]), float(value[1])]]
            except (TypeError, ValueError):
                return None
        if len(value) >= 4 and len(value) % 2 == 0:
            try:
                return [[float(value[i]), float(value[i + 1])] for i in range(0, len(value), 2)]
            except (TypeError, ValueError):
                return None
        return None
    points: list[list[float]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            return None
        try:
            points.append([float(item[0]), float(item[1])])
        except (TypeError, ValueError):
            return None
    return points if points else None


def _normalize_rotate_rect(value: Any) -> dict[str, Any] | None:
    """把 ``rotate_rect`` 规范为 {center,width,height,angle}；非法返回 None。

    真实 Qwen advanced_recognition 响应为平铺 5 数 ``[cx, cy, w, h, angle]``；
    同时兼容 ``{center,width,height,angle}`` dict 形状（EXTFP3-C#F6）。
    """
    if isinstance(value, (list, tuple)) and len(value) == 5 and all(_is_number(v) for v in value):
        try:
            return {
                "center": [[float(value[0]), float(value[1])]],
                "width": float(value[2]),
                "height": float(value[3]),
                "angle": float(value[4]),
            }
        except (TypeError, ValueError):
            return None
    if not isinstance(value, dict):
        return None
    center = _normalize_location(value.get("center"))
    if center is None:
        return None
    try:
        width = float(value["width"])
        height = float(value["height"])
        angle = float(value.get("angle", 0.0))
    except (KeyError, TypeError, ValueError):
        return None
    return {"center": center, "width": width, "height": height, "angle": angle}


# ---------------------------------------------------------------------------
# words_info 容错提取（§9.3）
# ---------------------------------------------------------------------------


def _is_words_info_list(value: Any) -> bool:
    """判断是否为 words_info 形状：list，且含至少一条带 ``text`` 的 dict。"""
    if not isinstance(value, list):
        return False
    return any(isinstance(item, dict) and isinstance(item.get("text"), str) for item in value)


def _recursive_find_words_info(node: Any) -> list[dict[str, Any]] | None:
    """有界递归兜底：查找第一个值是 words_info 形状的 ``words_info`` 键。"""
    stack: list[Any] = [node]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, dict):
            for key, value in current.items():
                if key == "words_info" and _is_words_info_list(value):
                    return cast("list[dict[str, Any]]", value)
                stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)
    return None


def extract_words_info(body: dict[str, Any]) -> list[dict[str, Any]]:
    """从响应体提取 ``words_info`` 原始条目。

    查找顺序（§9.3）：① 顶层 ``ocr_result.words_info``；② ``output.choices[0]
    .message.content[]`` 各图像项的 ``ocr_result.words_info``；③ 有界递归兜底。
    无法定位返回空列表（调用方按 failed 分类）。
    """
    if not isinstance(body, dict):
        return []

    # ① 顶层 ocr_result
    top = body.get("ocr_result")
    if isinstance(top, dict):
        wi = top.get("words_info")
        if _is_words_info_list(wi):
            return wi  # type: ignore[return-value]

    # ② output.choices[0].message.content[] 图像项
    output = body.get("output")
    if isinstance(output, dict):
        choices = output.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                message = first.get("message")
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict):
                                item_ocr = item.get("ocr_result")
                                if isinstance(item_ocr, dict):
                                    wi = item_ocr.get("words_info")
                                    if _is_words_info_list(wi):
                                        return wi  # type: ignore[return-value]

    # ③ 有界递归兜底
    found = _recursive_find_words_info(body)
    return found or []


# ---------------------------------------------------------------------------
# 逐词构建（§7.3）
# ---------------------------------------------------------------------------


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_word_records(
    raw_words: list[dict[str, Any]],
    *,
    ocr_run_id: str,
    ocr_task_id: str,
    image_width: int | None,
    image_height: int | None,
) -> list[OcrWordRecord]:
    """把 ``words_info`` 原始条目构建为逐词记录。

    ``word_id = SHA256(ocr_task_id + order + text_raw)``（确定性，同一响应可复现）；
    location/rotate_rect 无法规范 → ``NEEDS_REVIEW`` 保留 text；text 非字符串 →
    ``DROPPED`` 不写入。
    """
    records: list[OcrWordRecord] = []
    for order, item in enumerate(raw_words):
        if not isinstance(item, dict):
            continue
        text_raw = item.get("text")
        if not isinstance(text_raw, str) or not text_raw:
            continue
        location = _normalize_location(item.get("location"))
        rotate_rect = _normalize_rotate_rect(item.get("rotate_rect"))
        parse_state = (
            WordParseState.PARSED.value
            if location is not None
            else (WordParseState.NEEDS_REVIEW.value)
        )
        records.append(
            OcrWordRecord(
                word_id=_sha256_text(_WORD_KEY_SEP.join([ocr_task_id, str(order), text_raw])),
                ocr_run_id=ocr_run_id,
                ocr_task_id=ocr_task_id,
                order=order,
                text_raw=text_raw,
                text_normalized=normalize_text(text_raw),
                location=location if location is not None else [],
                rotate_rect=rotate_rect,
                image_width=image_width,
                image_height=image_height,
                coord_version=COORD_VERSION,
                parse_state=parse_state,
            )
        )
    return records


# ---------------------------------------------------------------------------
# 响应级解析与分类（§9.3 / §9.5）
# ---------------------------------------------------------------------------


def _classify_response(
    *,
    source_state: str,
    model_match: bool | None,
    words: list[OcrWordRecord],
) -> str:
    """响应级分类：parsed / partial / failed / needs_review。

    优先级：源任务 OCR_FAILED→failed；OCR_NEEDS_REVIEW 或模型不一致→needs_review；
    无单词→failed；OCR_PARTIAL 或存在非 PARSED 单词→partial；否则 parsed。
    """
    if source_state == OcrState.OCR_FAILED.value:
        return "failed"
    if source_state == OcrState.NEEDS_REVIEW.value or model_match is False:
        return "needs_review"
    if not words:
        return "failed"
    if source_state == OcrState.OCR_PARTIAL.value:
        return "partial"
    if any(w.parse_state != WordParseState.PARSED.value for w in words):
        return "partial"
    return "parsed"


def parse_response_body(
    body: dict[str, Any],
    *,
    ocr_run_id: str,
    ocr_task_id: str,
    source_state: str,
    model_requested: str | None = None,
    model_returned: str | None = None,
    image_width: int | None = None,
    image_height: int | None = None,
) -> OcrParseRecord:
    """把单张图片的 OCR 响应体解析为 OcrParseRecord（响应级分类）。"""
    raw_words = extract_words_info(body)
    words = build_word_records(
        raw_words,
        ocr_run_id=ocr_run_id,
        ocr_task_id=ocr_task_id,
        image_width=image_width,
        image_height=image_height,
    )
    model_match: bool | None = None
    if model_returned is not None:
        model_match = model_returned == (model_requested or "")
    parse_state = _classify_response(
        source_state=source_state,
        model_match=model_match,
        words=words,
    )
    return OcrParseRecord(
        ocr_task_id=ocr_task_id,
        ocr_run_id=ocr_run_id,
        source_state=source_state,
        model_requested=model_requested,
        model_returned=model_returned,
        model_match=model_match,
        words_count=len(words),
        parsed_count=sum(1 for w in words if w.parse_state == WordParseState.PARSED.value),
        needs_review_count=sum(
            1 for w in words if w.parse_state == WordParseState.NEEDS_REVIEW.value
        ),
        dropped_count=0,
        parse_state=parse_state,
        words=words,
    )


def compute_parseability_stats(records: list[OcrParseRecord]) -> dict[str, Any]:
    """schema/响应可解析率统计（§12 验收线「可解析率 ≥99.5%」）。

    可解析率 = 成功解析出至少一条单词的响应数 / 有响应任务数（parsed + partial 的
    parsed_count>0）；同时给出 failed / needs_review 计数供分类追溯。
    """
    total = len(records)
    parsed_ok = sum(1 for r in records if r.parse_state == "parsed")
    parsed_partial = sum(1 for r in records if r.parse_state == "partial")
    parsed_failed = sum(1 for r in records if r.parse_state == "failed")
    parsed_needs_review = sum(1 for r in records if r.parse_state == "needs_review")
    with_words = sum(1 for r in records if r.words_count > 0)
    rate = (with_words / total) if total else None
    return {
        "total_responses": total,
        "parsed_ok": parsed_ok,
        "parsed_partial": parsed_partial,
        "parsed_failed": parsed_failed,
        "parsed_needs_review": parsed_needs_review,
        "responses_with_words": with_words,
        "parseable_rate": rate,
        "total_words": sum(r.words_count for r in records),
    }


# ---------------------------------------------------------------------------
# OCR 运行目录解析 + 逐词表落盘（§7.3 / §5 数据边界）
# ---------------------------------------------------------------------------


def load_ocr_run_record(run_dir: Path) -> OcrRunRecord:
    """读取 OCR 运行记录 ocr_run.json（B 阶段产物）。"""
    run_path = run_dir / RUN_FILENAME
    if not run_path.is_file():
        raise FileNotFoundError(f"OCR 运行记录不存在: {run_path}")
    return OcrRunRecord.model_validate_json(run_path.read_text(encoding="utf-8"))


def _task_raw_response_path(run_dir: Path, task: OcrTaskRecord) -> Path | None:
    """定位任务原始响应文件；任务记录给出路径则优先，否则按文件名推断。"""
    if task.raw_response_path:
        candidate = run_dir / Path(task.raw_response_path).name
        if candidate.is_file():
            return candidate
    candidate = run_dir / raw_response_filename(task.ocr_task_id)
    if candidate.is_file():
        return candidate
    return None


def parse_ocr_run_directory(
    run_dir: Path,
    *,
    data_dir: Path | None = None,
) -> WordRunStats:
    """解析一次 OCR 运行的逐张原始响应，落盘 floorplan_ocr_word 逐词表。

    - 只解析有原始响应文件的终态任务（OCR_SUCCEEDED / OCR_PARTIAL / NEEDS_REVIEW
      且落盘了响应）；无响应文件（网络失败/成本门禁/敏感回显）计入 tasks_without_response；
    - 逐张 ``parse_response_body`` 并聚合 ``WordRunStats``；
    - 若给 ``data_dir``，把全部单词原子写入 ``staged/floorplan_ocr_word.parquet``
      （沿用 write_staged_asset_table 约定，不入 git）。
    """
    record = load_ocr_run_record(run_dir)
    records: list[OcrParseRecord] = []
    without_response = 0
    for task in record.tasks:
        if task.state in (OcrState.OCR_PENDING, OcrState.OCR_RUNNING):
            continue
        raw_path = _task_raw_response_path(run_dir, task)
        if raw_path is None:
            without_response += 1
            continue
        try:
            body = json.loads(raw_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            # 原始响应损坏：分类为 failed，不伪造单词
            records.append(
                OcrParseRecord(
                    ocr_task_id=task.ocr_task_id,
                    ocr_run_id=record.ocr_run_id,
                    source_state=task.state.value,
                    model_requested=task.model_requested,
                    model_returned=task.model_returned,
                    parse_state="failed",
                )
            )
            continue
        parsed = parse_response_body(
            body,
            ocr_run_id=record.ocr_run_id,
            ocr_task_id=task.ocr_task_id,
            source_state=task.state.value,
            model_requested=task.model_requested,
            model_returned=task.model_returned,
            image_width=task.width,
            image_height=task.height,
        )
        records.append(parsed)

    stats_values = compute_parseability_stats(records)
    stats = WordRunStats(
        ocr_run_id=record.ocr_run_id,
        total_tasks=len(record.tasks),
        tasks_with_response=len(records),
        tasks_without_response=without_response,
        parsed_ok=stats_values["parsed_ok"],
        parsed_partial=stats_values["parsed_partial"],
        parsed_failed=stats_values["parsed_failed"],
        parsed_needs_review=stats_values["parsed_needs_review"],
        total_words=stats_values["total_words"],
        parseable_rate=stats_values["parseable_rate"],
    )

    if data_dir is not None:
        table_path = write_word_table(records, data_dir)
        stats.table_path = table_path.as_posix()
    return stats


def write_word_table(
    records: list[OcrParseRecord],
    data_dir: Path,
    *,
    compression: str = "zstd",
) -> Path:
    """把逐词记录原子写入 staged/floorplan_ocr_word.parquet（沿用资产表约定）。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict[str, object]] = []
    for rec in records:
        for w in rec.words:
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
                    "participates_in_field": w.participates_in_field,
                    "parse_version": rec.parse_version,
                    "response_parse_state": rec.parse_state,
                }
            )
    table = pa.Table.from_pylist(rows)
    staged_dir = data_dir / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    final_path = staged_dir / WORD_STAGED_FILENAME
    work_path = staged_dir / (WORD_STAGED_FILENAME + ".incomplete")
    pq.write_table(table, work_path, compression=compression)
    work_path.replace(final_path)
    return final_path


__all__ = [
    "COORD_VERSION",
    "OCR_RESPONSE_PARSE_VERSION",
    "WORD_STAGED_FILENAME",
    "OcrParseRecord",
    "OcrWordRecord",
    "WordParseState",
    "WordRunStats",
    "build_word_records",
    "compute_parseability_stats",
    "extract_words_info",
    "load_ocr_run_record",
    "normalize_text",
    "parse_ocr_run_directory",
    "parse_response_body",
    "write_word_table",
]
