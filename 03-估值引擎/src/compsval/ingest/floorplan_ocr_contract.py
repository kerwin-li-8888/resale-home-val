"""Qwen OCR 请求合同、运行配置、成本门禁与安全审计（EXTFP3-A，技术方案 §9/§11/§15）。

本模块是 EXTFP3 的**合同层**，冻结以下内容并全程只读：

1. **请求合同 v1**（``REQUEST_CONTRACT_V1``）：固定模型 ``qwen-vl-ocr-2025-11-20``、
   内置任务 ``advanced_recognition``、``min_pixels=3072``/``max_pixels=8388608``、
   ``enable_rotate=false``、非流式、华北2（北京）DashScope 原生接口。请求体形状与
   官方 API 参考（2026-08-11 更新）一致：``min_pixels/max_pixels/enable_rotate`` 位于
   ``input.messages[].content[]`` 的图像项内，``ocr_options.task`` 位于 ``parameters`` 内。
2. **运行配置 YAML schema**（``OcrRunConfig``）：请求参数、成本门禁（人民币硬上限、
   最大图片数、最大 Token、最大重试数、单图成本超 300 张基线 20% 自动暂停阈值）与
   安全约定（密钥环境变量名）。配置用 ``load_ocr_run_config`` 读取并校验。
3. **成本估算与门禁**（``estimate_cost_yuan`` / ``OcrCostGate``）：按官方价格
   （输入 0.3 元/百万 Token、输出 0.5 元/百万 Token，2026-08-25 已复核）估算单图与
   累计费用；``OcrCostGate`` 逐条记录 usage 并 fail-closed 执行四类上限。
4. **安全审计**（``read_dashscope_api_key`` / ``redact_secrets`` / ``audit_no_secrets``）：
   ``DASHSCOPE_API_KEY`` 仅从环境读取；日志/报告在落盘前用脱敏审计函数检查并抹除
   密钥与 Base64 图片 Data URL。

本模块**不触网、不调用任何真实 Qwen**；真实付费调用属 EXTFP3-B（请求器）与
EXTFP3-F/H（真实运行）。价格是外部动态事实，每次付费工作包启动前必须重新核验官方
页面并把当时价格写入合同（技术方案 §11.1）。
"""

from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

# ---- 请求合同 v1（EXTFP3-A-OCR-1.0，冻结，不改写） ----
REQUEST_CONTRACT_VERSION = "EXTFP3-A-OCR-1.0"

#: 固定模型（技术方案 §9.1；固定日期版本便于复现，不用 latest/稳定别名）
OCR_MODEL_ID = "qwen-vl-ocr-2025-11-20"

#: 内置任务（高精识别：逐行文字 + 边界框，技术方案 §9.1）
OCR_TASK = "advanced_recognition"

#: 华北2（北京）DashScope 原生接口（multimodal-generation）
DASHSCOPE_BEIJING_ENDPOINT = (
    "https://dashscope.aliyuncs.com/api/v1/services/aigc/multimodal-generation/generation"
)
OCR_REGION = "cn-beijing"

#: 图像像素阈值（技术方案 §9.2；官方该模型每 Token 对应 32×32 像素）
OCR_MIN_PIXELS = 3072  # 3×32×32，模型默认值与最小值
OCR_MAX_PIXELS = 8388608  # 8192×32×32，模型默认值

#: 不自动转正：避免改变户型图几何坐标；文字自身角度由 rotate_rect 记录（§9.2）
OCR_ENABLE_ROTATE = False

#: 非流式，确保原始响应完整（§9.2）
OCR_STREAM = False

#: 密钥环境变量名（技术方案 §15；仅从环境读取，绝不写进源码/日志/报告）
DASHSCOPE_API_KEY_ENV = "DASHSCOPE_API_KEY"


class OcrRequestContract(BaseModel):
    """冻结的 OCR 请求合同 v1（EXTFP3-A-OCR-1.0）。

    ``frozen=True`` 使实例不可变；任何字段修改都会抛错，防止实施期静默漂移。
    请求体形状与官方 API 参考一致：图像项内带 min_pixels/max_pixels/enable_rotate，
    ``parameters.ocr_options.task`` 指定内置任务；``stream`` 显式 false（非流式）。
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    contract_version: str = Field(default=REQUEST_CONTRACT_VERSION)
    model: str = Field(default=OCR_MODEL_ID)
    task: str = Field(default=OCR_TASK)
    min_pixels: int = Field(default=OCR_MIN_PIXELS, ge=1)
    max_pixels: int = Field(default=OCR_MAX_PIXELS, ge=1)
    enable_rotate: bool = Field(default=OCR_ENABLE_ROTATE)
    stream: bool = Field(default=OCR_STREAM)
    region: str = Field(default=OCR_REGION)
    endpoint: str = Field(default=DASHSCOPE_BEIJING_ENDPOINT)

    def request_body(self, *, image_data_url: str, text: str | None = None) -> dict[str, Any]:
        """构造 DashScope 原生接口请求体（图像项携带像素阈值/旋转参数）。

        ``text`` 为 None 时使用官方任务默认提示（不传 text 项，模型按任务默认提取
        全部文本）。返回的 dict 供 EXTFP3-B 序列化发送；本方法不触网。
        """
        content: list[dict[str, Any]] = [
            {
                "image": image_data_url,
                "min_pixels": self.min_pixels,
                "max_pixels": self.max_pixels,
                "enable_rotate": self.enable_rotate,
            }
        ]
        if text is not None:
            content.append({"text": text})
        return {
            "model": self.model,
            "input": {
                "messages": [
                    {
                        "role": "user",
                        "content": content,
                    }
                ]
            },
            "parameters": {"ocr_options": {"task": self.task}},
            "stream": self.stream,
        }


#: 请求合同 v1 冻结实例（EXTFP3-A 唯一权威入口；下游一律引用本常量）
REQUEST_CONTRACT_V1 = OcrRequestContract()

# ---- 价格核验记录（技术方案 §11.1；外部动态事实，付费前必须重核） ----
#: 官方价格：输入 0.3 元/百万 Token、输出 0.5 元/百万 Token（华北2北京，qwen-vl-ocr-2025-11-20）
INPUT_PRICE_PER_MT = 0.3
OUTPUT_PRICE_PER_MT = 0.5

#: 该模型每 Token 对应像素（官方 32×32，技术方案 §11.2 按此估算图片 Token）
PIXELS_PER_IMAGE_TOKEN = 32 * 32

PRICE_SOURCE_URL = "https://help.aliyun.com/zh/model-studio/qwenvl-ocr"
PRICE_VERIFIED_AT = "2026-08-25"
PRICE_VERIFICATION_VERSION = "EXTFP3-A-PRICE-1.0"


@dataclass(frozen=True)
class PriceVerificationRecord:
    """一次官方价格核验记录（随每次付费工作包启动前重核更新）。"""

    verified_at: str
    region: str
    model: str
    input_price_per_mt: float
    output_price_per_mt: float
    source_url: str


PRICE_VERIFICATION_V1 = PriceVerificationRecord(
    verified_at=PRICE_VERIFIED_AT,
    region=OCR_REGION,
    model=OCR_MODEL_ID,
    input_price_per_mt=INPUT_PRICE_PER_MT,
    output_price_per_mt=OUTPUT_PRICE_PER_MT,
    source_url=PRICE_SOURCE_URL,
)


def estimate_image_tokens(width: int, height: int) -> int:
    """按像素/1024 估算图片 Token（技术方案 §11.2；向上取整，至少 1）。"""
    return max(1, -(-(width * height) // PIXELS_PER_IMAGE_TOKEN))


def estimate_cost_yuan(
    prompt_tokens: int,
    completion_tokens: int,
    *,
    input_price_per_mt: float = INPUT_PRICE_PER_MT,
    output_price_per_mt: float = OUTPUT_PRICE_PER_MT,
) -> float:
    """按官方单价估算单次请求费用（元）。返回四舍五入到 6 位。"""
    cost = (
        prompt_tokens * input_price_per_mt + completion_tokens * output_price_per_mt
    ) / 1_000_000
    return round(cost, 6)


# ---- 运行配置 YAML schema（EXTFP3-A-OCR-CONFIG-1.0） ----
OCR_RUN_CONFIG_SCHEMA_VERSION = "EXTFP3-A-OCR-CONFIG-1.0"

#: 人民币硬上限（2026-08-25 用户授权，含重试余量；达到即自动停止不透支）
DEFAULT_COST_CAP_YUAN = 30.0

#: 最大图片数（EXTFP6 起上调：310=10 调试 + 300 验收旧口径不足以覆盖生产批次；
#: 上限只是运行级保险丝，真实约束 = manifest 冻结资产数 + attempt 门禁 + change 级
#: 人民币硬上限，三者先到即停。change extfp6-full-history-ocr-batch 授权上调）
DEFAULT_MAX_IMAGES = 1_500

#: 单图最大请求次数（含首次，技术方案 §11.3「不因 API 失败无限重试」）
DEFAULT_MAX_RETRIES = 3

#: 300 张基线单图成本初始估计（元/张）：约 1520 输入 Token + 少量输出 Token。
#: EXTFP3-H 用 300 张实际 usage 刷新此基线（技术方案 §11.2/§11.3）。
DEFAULT_BASELINE_COST_PER_IMAGE_YUAN = 0.003

#: 单图成本超过 300 张基线 20% 自动暂停（技术方案 §11.3）
DEFAULT_PAUSE_RATIO_THRESHOLD = 1.2

#: 单运行最大 Token 总量（防失控的独立上限，远低于 30 元对应量级）
DEFAULT_MAX_TOKENS_PER_RUN = 2_000_000


class OcrCostConfig(BaseModel):
    """成本门禁配置（技术方案 §11.3）。

    任一上限被触发即 fail-closed 停止运行，绝不透支后继续。
    """

    model_config = ConfigDict(extra="forbid")

    hard_cap_yuan: float = Field(default=DEFAULT_COST_CAP_YUAN, gt=0)
    max_images: int = Field(default=DEFAULT_MAX_IMAGES, ge=1)
    max_tokens_per_run: int = Field(default=DEFAULT_MAX_TOKENS_PER_RUN, ge=1)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=1)
    base_backoff: float = Field(default=1.0, ge=0)
    max_backoff: float = Field(default=30.0, ge=0)
    baseline_cost_per_image_yuan: float = Field(default=DEFAULT_BASELINE_COST_PER_IMAGE_YUAN, gt=0)
    pause_ratio_threshold: float = Field(default=DEFAULT_PAUSE_RATIO_THRESHOLD, ge=1.0)
    input_price_per_mt: float = Field(default=INPUT_PRICE_PER_MT, ge=0)
    output_price_per_mt: float = Field(default=OUTPUT_PRICE_PER_MT, ge=0)


class OcrRunConfig(BaseModel):
    """OCR 运行配置（YAML schema，EXTFP3-A-OCR-CONFIG-1.0）。

    YAML 顶层三个键：``request``（冻结请求参数，校验必须等于 REQUEST_CONTRACT_V1 或
    保持默认）、``cost``（成本门禁）、``security``（密钥环境变量名）。
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(default=OCR_RUN_CONFIG_SCHEMA_VERSION)
    request: OcrRequestContract = Field(default_factory=lambda: REQUEST_CONTRACT_V1)
    cost: OcrCostConfig = Field(default_factory=OcrCostConfig)
    security: dict[str, str] = Field(
        default_factory=lambda: {"api_key_env_var": DASHSCOPE_API_KEY_ENV}
    )


def default_ocr_run_config() -> OcrRunConfig:
    """返回默认运行配置（冻结合同 + 授权成本上限）。"""
    return OcrRunConfig()


def default_ocr_run_config_yaml() -> str:
    """生成默认运行配置 YAML 文本（供用户本地保存/修改）。"""
    return yaml.safe_dump(
        default_ocr_run_config().model_dump(mode="json"),
        allow_unicode=True,
        sort_keys=False,
    )


def load_ocr_run_config(path: Path) -> OcrRunConfig:
    """从 YAML 读取并校验运行配置；非法即抛错（fail-closed）。

    请求参数缺省时回退冻结合同；若显式给出且与冻结合同不一致则报错（技术方案 §9.1：
    模型漂移必须重新验收）。本函数不触网。
    """
    if not path.is_file():
        raise FileNotFoundError(f"OCR 运行配置不存在: {path}")
    text = path.read_text(encoding="utf-8")
    return validate_ocr_run_config_yaml(text)


def validate_ocr_run_config_yaml(text: str) -> OcrRunConfig:
    """校验 YAML 文本并返回 ``OcrRunConfig``；缺省 request 时回退冻结合同。"""
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError("OCR 运行配置顶层必须是对象")
    if "request" not in data:
        data["request"] = REQUEST_CONTRACT_V1.model_dump()
    if "cost" not in data:
        data["cost"] = OcrCostConfig().model_dump()
    if "security" not in data:
        data["security"] = {"api_key_env_var": DASHSCOPE_API_KEY_ENV}
    cfg = OcrRunConfig.model_validate(data)
    if cfg.request.model != REQUEST_CONTRACT_V1.model:
        raise ValueError(
            f"配置请求模型 {cfg.request.model!r} 与冻结合同 {REQUEST_CONTRACT_V1.model!r} 不一致；"
            "模型变更必须创建新配置版本并重新验收"
        )
    return cfg


# ---- 成本门禁（fail-closed，技术方案 §11.3） ----


@dataclass
class OcrUsageRecord:
    """单次请求的实际用量（EXTFP3-B 运行记录回填）。"""

    ocr_task_id: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_yuan: float = 0.0
    attempts: int = 0


class OcrCostGate:
    """逐条累加用量并执行四类上限：金额硬上限/图片数/Token/重试。

    ``check_before_request`` 在每次请求前检查是否允许继续；``record_usage`` 在每次
    请求后登记实际用量。任一上限已触及即返回 False（fail-closed），调用方必须停止。
    单图成本与基线比较在 ``record_usage`` 中判定，超过 ``pause_ratio_threshold`` 时
    置 ``pause_cost_anomaly=True``。

    OCRNEXT-B：内部用可重入锁保护全部状态读写，使同一门禁实例可被并发 worker 安全
    共享；单线程下的行为与语义和 EXTFP3-B 完全一致（锁只串行化，不改变判定顺序）。
    """

    def __init__(self, cost_config: OcrCostConfig | None = None) -> None:
        self.config = cost_config or OcrCostConfig()
        self.total_images = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_retries = 0
        self.total_cost_yuan = 0.0
        self.pause_cost_anomaly = False
        self.limit_hit: str | None = None
        self.usages: list[OcrUsageRecord] = []
        self._lock = threading.RLock()

    @property
    def total_tokens(self) -> int:
        return self.total_prompt_tokens + self.total_completion_tokens

    def check_before_request(self) -> bool:
        """请求前检查：未触上限才允许发起下一次请求。"""
        with self._lock:
            if self.limit_hit is not None:
                return False
            if self.total_cost_yuan >= self.config.hard_cap_yuan:
                self.limit_hit = "cost_cap_yuan"
                return False
            if self.total_images >= self.config.max_images:
                self.limit_hit = "max_images"
                return False
            if self.total_tokens >= self.config.max_tokens_per_run:
                self.limit_hit = "max_tokens"
                return False
            if self.total_retries >= self.config.max_retries:
                self.limit_hit = "max_retries"
                return False
            return True

    def record_usage(self, usage: OcrUsageRecord) -> None:
        """登记一次请求的实际用量并累加计数；同时评估单图成本异常。"""
        with self._lock:
            if self.pause_cost_anomaly:
                return
            self.total_images += 1
            self.total_prompt_tokens += usage.prompt_tokens
            self.total_completion_tokens += usage.completion_tokens
            self.total_retries += max(0, usage.attempts - 1)
            self.total_cost_yuan = round(
                self.total_cost_yuan
                + estimate_cost_yuan(
                    usage.prompt_tokens,
                    usage.completion_tokens,
                    input_price_per_mt=self.config.input_price_per_mt,
                    output_price_per_mt=self.config.output_price_per_mt,
                ),
                6,
            )
            self.usages.append(usage)
            # 单图成本异常：本图费用 > 基线 * 阈值
            per_image = estimate_cost_yuan(
                usage.prompt_tokens,
                usage.completion_tokens,
                input_price_per_mt=self.config.input_price_per_mt,
                output_price_per_mt=self.config.output_price_per_mt,
            )
            if (
                per_image
                > self.config.baseline_cost_per_image_yuan * self.config.pause_ratio_threshold
            ):
                self.pause_cost_anomaly = True
                self.limit_hit = "per_image_cost_anomaly"

    def snapshot(self) -> dict[str, Any]:
        """导出当前门禁状态（供运行记录与报告；不含任何密钥/图片内容）。"""
        with self._lock:
            return {
                "total_images": self.total_images,
                "total_prompt_tokens": self.total_prompt_tokens,
                "total_completion_tokens": self.total_completion_tokens,
                "total_tokens": self.total_tokens,
                "total_retries": self.total_retries,
                "total_cost_yuan": self.total_cost_yuan,
                "pause_cost_anomaly": self.pause_cost_anomaly,
                "limit_hit": self.limit_hit,
            }


# ---- 安全审计（技术方案 §15） ----


def read_dashscope_api_key(*, env_name: str = DASHSCOPE_API_KEY_ENV) -> str:
    """从环境变量读取 DashScope API Key。

    仅接受本地环境变量，绝不从文件/参数/配置读取密钥。缺失或空白时抛 ``KeyError``。
    """
    key = os.environ.get(env_name, "").strip()
    if not key:
        raise KeyError(f"未找到 {env_name} 环境变量；密钥只从本地环境读取（技术方案 §15）")
    return key


#: Base64 图片 Data URL 匹配（用于脱敏与审计；最长容忍 256MB 量级的 base64 片段）
_DATA_URL_RE = re.compile(r"data:image/[A-Za-z0-9.+-]+;base64,[A-Za-z0-9+/=]+")


def redact_secrets(text: str, *, api_key: str | None = None) -> str:
    """抹除密钥与 Base64 图片 Data URL，返回可安全落盘/打印的文本。

    - 若提供 ``api_key``，将其整段替换为 ``<REDACTED>``；
    - 将 ``data:image/...;base64,<payload>`` 替换为 ``data:image/...;base64,<REDACTED>``。
    """
    out = text
    if api_key:
        out = out.replace(api_key, "<REDACTED>")
    out = _DATA_URL_RE.sub(lambda m: m.group(0).split("base64,")[0] + "base64,<REDACTED>", out)
    return out


def audit_no_secrets(text: str, *, api_key: str | None = None) -> bool:
    """审计：文本是否不含密钥与 Base64 图片 Data URL。

    返回 True 表示未发现敏感内容，可安全写入日志/报告；False 表示发现泄露风险，
    调用方必须停止写入并排查（技术方案 §19.2「任何凭证或敏感信息有泄露风险」）。
    """
    if api_key and api_key in text:
        return False
    return not _DATA_URL_RE.search(text)


def utc_now_iso() -> str:
    """UTC 时间 ISO 字符串（运行记录统一时间源）。"""
    return datetime.now(UTC).isoformat()


__all__ = [
    "DASHSCOPE_API_KEY_ENV",
    "DASHSCOPE_BEIJING_ENDPOINT",
    "DEFAULT_BASELINE_COST_PER_IMAGE_YUAN",
    "DEFAULT_COST_CAP_YUAN",
    "DEFAULT_MAX_IMAGES",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_MAX_TOKENS_PER_RUN",
    "DEFAULT_PAUSE_RATIO_THRESHOLD",
    "INPUT_PRICE_PER_MT",
    "OCR_ENABLE_ROTATE",
    "OCR_MAX_PIXELS",
    "OCR_MIN_PIXELS",
    "OCR_MODEL_ID",
    "OCR_REGION",
    "OCR_RUN_CONFIG_SCHEMA_VERSION",
    "OCR_STREAM",
    "OCR_TASK",
    "OUTPUT_PRICE_PER_MT",
    "PIXELS_PER_IMAGE_TOKEN",
    "PRICE_SOURCE_URL",
    "PRICE_VERIFIED_AT",
    "PRICE_VERIFICATION_V1",
    "PRICE_VERIFICATION_VERSION",
    "REQUEST_CONTRACT_V1",
    "REQUEST_CONTRACT_VERSION",
    "OcrCostConfig",
    "OcrCostGate",
    "OcrRequestContract",
    "OcrRunConfig",
    "OcrUsageRecord",
    "PriceVerificationRecord",
    "audit_no_secrets",
    "default_ocr_run_config",
    "default_ocr_run_config_yaml",
    "estimate_cost_yuan",
    "estimate_image_tokens",
    "load_ocr_run_config",
    "read_dashscope_api_key",
    "redact_secrets",
    "utc_now_iso",
    "validate_ocr_run_config_yaml",
]
