"""EXTFP3-A 离线测试：OCR 请求合同、配置 schema、成本门禁与安全审计。

只测纯函数与数据模型，绝不触网、不调用真实 Qwen、不读写真实密钥。
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from compsval.ingest.floorplan_ocr_contract import (
    DASHSCOPE_API_KEY_ENV,
    DASHSCOPE_BEIJING_ENDPOINT,
    DEFAULT_BASELINE_COST_PER_IMAGE_YUAN,
    DEFAULT_MAX_IMAGES,
    DEFAULT_MAX_RETRIES,
    DEFAULT_PAUSE_RATIO_THRESHOLD,
    INPUT_PRICE_PER_MT,
    OCR_ENABLE_ROTATE,
    OCR_MAX_PIXELS,
    OCR_MIN_PIXELS,
    OCR_MODEL_ID,
    OCR_REGION,
    OCR_STREAM,
    OCR_TASK,
    OUTPUT_PRICE_PER_MT,
    PRICE_VERIFICATION_V1,
    REQUEST_CONTRACT_V1,
    REQUEST_CONTRACT_VERSION,
    OcrCostConfig,
    OcrCostGate,
    OcrUsageRecord,
    audit_no_secrets,
    default_ocr_run_config,
    default_ocr_run_config_yaml,
    estimate_cost_yuan,
    estimate_image_tokens,
    load_ocr_run_config,
    read_dashscope_api_key,
    redact_secrets,
    validate_ocr_run_config_yaml,
)

FAKE_KEY = "sk-EXTFP3-test-dummy-0123456789abcdef"


def test_contract_version_and_core_values() -> None:
    """冻结合同 v1 的模型/任务/像素/旋转/非流式/地域/端点与合同版本一致。"""
    c = REQUEST_CONTRACT_V1
    assert c.contract_version == REQUEST_CONTRACT_VERSION == "EXTFP3-A-OCR-1.0"
    assert c.model == OCR_MODEL_ID == "qwen-vl-ocr-2025-11-20"
    assert c.task == OCR_TASK == "advanced_recognition"
    assert c.min_pixels == OCR_MIN_PIXELS == 3072
    assert c.max_pixels == OCR_MAX_PIXELS == 8388608
    assert c.enable_rotate == OCR_ENABLE_ROTATE is False
    assert c.stream == OCR_STREAM is False
    assert c.region == OCR_REGION == "cn-beijing"
    assert c.endpoint == DASHSCOPE_BEIJING_ENDPOINT


def test_contract_is_frozen() -> None:
    """冻结模型不可变：修改任何字段都抛错（防实施期漂移）。"""
    with pytest.raises(ValidationError):
        REQUEST_CONTRACT_V1.model = "qwen-vl-ocr-latest"


def test_request_body_shape() -> None:
    """请求体形状与官方 DashScope 原生接口一致（§9.1/§9.2）。"""
    body = REQUEST_CONTRACT_V1.request_body(image_data_url="data:image/jpeg;base64,AAAA")
    assert body["model"] == OCR_MODEL_ID
    assert body["stream"] is False
    params = body["parameters"]
    assert params["ocr_options"]["task"] == OCR_TASK
    content = body["input"]["messages"][0]["content"]
    image_item = content[0]
    assert image_item["image"] == "data:image/jpeg;base64,AAAA"
    assert image_item["min_pixels"] == 3072
    assert image_item["max_pixels"] == 8388608
    assert image_item["enable_rotate"] is False


def test_request_body_with_text() -> None:
    """显式 text 时追加 text 项；缺省时只发图像项（任务默认提示）。"""
    body = REQUEST_CONTRACT_V1.request_body(
        image_data_url="data:image/png;base64,BBBB", text="仅输出户型图文字"
    )
    content = body["input"]["messages"][0]["content"]
    assert content[1]["text"] == "仅输出户型图文字"
    plain = REQUEST_CONTRACT_V1.request_body(image_data_url="data:image/png;base64,BBBB")
    assert len(plain["input"]["messages"][0]["content"]) == 1


def test_price_verification_record() -> None:
    """2026-08-25 官方价格核验记录：输入 0.3、输出 0.5 元/百万 Token。"""
    p = PRICE_VERIFICATION_V1
    assert p.model == OCR_MODEL_ID
    assert p.region == OCR_REGION
    assert p.input_price_per_mt == INPUT_PRICE_PER_MT == 0.3
    assert p.output_price_per_mt == OUTPUT_PRICE_PER_MT == 0.5
    assert p.source_url.startswith("https://help.aliyun.com")


def test_estimate_image_tokens() -> None:
    """图片 Token ≈ 像素/1024（32×32 每 Token，向上取整，至少 1）。"""
    assert estimate_image_tokens(32, 32) == 1
    assert estimate_image_tokens(1024, 1024) == 1024
    assert estimate_image_tokens(0, 0) == 1
    # 1440×1080 → 1519（技术方案 §11.2 估算约 1520）
    assert estimate_image_tokens(1440, 1080) == 1519


def test_estimate_cost_yuan() -> None:
    """按官方单价估算：输入 0.3、输出 0.5 元/百万 Token。"""
    assert estimate_cost_yuan(1_000_000, 0) == 0.3
    assert estimate_cost_yuan(0, 1_000_000) == 0.5
    assert estimate_cost_yuan(1_000_000, 1_000_000) == 0.8


def test_default_config_yaml_roundtrip() -> None:
    """默认配置 YAML 可被 schema 校验回读，且与默认对象一致。"""
    yaml_text = default_ocr_run_config_yaml()
    cfg = validate_ocr_run_config_yaml(yaml_text)
    assert cfg.schema_version == "EXTFP3-A-OCR-CONFIG-1.0"
    assert cfg.request.model == OCR_MODEL_ID
    assert cfg.cost.hard_cap_yuan == 30.0
    assert cfg.security["api_key_env_var"] == DASHSCOPE_API_KEY_ENV


def test_load_config_file(tmp_path: Path) -> None:
    """从 YAML 文件读取并校验配置。"""
    cfg_path = tmp_path / "ocr.yaml"
    cfg_path.write_text(default_ocr_run_config_yaml(), encoding="utf-8")
    cfg = load_ocr_run_config(cfg_path)
    assert cfg.cost.max_images == DEFAULT_MAX_IMAGES
    assert cfg.cost.max_retries == 3


def test_default_max_images_upscaled_for_extfp6() -> None:
    """EXTFP6 上调：默认 max_images ≥ 1,500 覆盖全历史批次；请求合同与单图门禁不变。"""
    assert DEFAULT_MAX_IMAGES >= 1_500
    cfg = OcrCostConfig()
    assert cfg.max_images == DEFAULT_MAX_IMAGES
    assert cfg.max_retries == DEFAULT_MAX_RETRIES
    assert cfg.baseline_cost_per_image_yuan == DEFAULT_BASELINE_COST_PER_IMAGE_YUAN
    assert cfg.pause_ratio_threshold == DEFAULT_PAUSE_RATIO_THRESHOLD
    run_cfg = default_ocr_run_config()
    assert run_cfg.request == REQUEST_CONTRACT_V1


def test_load_config_missing_file(tmp_path: Path) -> None:
    """配置文件缺失时 fail-closed 抛错。"""
    with pytest.raises(FileNotFoundError):
        load_ocr_run_config(tmp_path / "nope.yaml")


def test_config_model_mismatch_rejected() -> None:
    """显式请求模型与冻结合同不一致时拒绝（技术方案 §9.1 模型漂移门禁）。"""
    bad = default_ocr_run_config_yaml().replace(OCR_MODEL_ID, "qwen-vl-ocr-latest")
    with pytest.raises(ValueError, match="不一致"):
        validate_ocr_run_config_yaml(bad)


def test_cost_gate_accumulates_and_limits() -> None:
    """成本门禁逐条累加并 fail-closed：任一上限触及即停止。"""
    gate = OcrCostGate()
    assert gate.check_before_request() is True
    for _ in range(3):
        gate.record_usage(
            OcrUsageRecord(
                ocr_task_id="t",
                prompt_tokens=1000,
                completion_tokens=100,
                attempts=1,
            )
        )
    snap = gate.snapshot()
    assert snap["total_images"] == 3
    assert snap["total_prompt_tokens"] == 3000
    assert snap["total_cost_yuan"] > 0
    assert gate.limit_hit is None

    # 图片数触顶：max_images=2 的配置在第 3 张前停止
    tight = OcrCostGate(OcrCostConfig(max_images=2))
    assert tight.check_before_request() is True
    tight.record_usage(OcrUsageRecord(prompt_tokens=1, completion_tokens=0, attempts=1))
    assert tight.check_before_request() is True
    tight.record_usage(OcrUsageRecord(prompt_tokens=1, completion_tokens=0, attempts=1))
    assert tight.check_before_request() is False
    assert tight.limit_hit == "max_images"


def test_cost_gate_hard_cap() -> None:
    """金额硬上限：累计费用达到上限后不再允许请求。"""
    cfg = OcrCostConfig(hard_cap_yuan=0.0004)
    gate = OcrCostGate(cfg)
    # 每张 0.0003 元；第二张后累计 0.0006 > 0.0004，第三张前应停止
    gate.record_usage(OcrUsageRecord(prompt_tokens=1000, completion_tokens=0, attempts=1))
    gate.record_usage(OcrUsageRecord(prompt_tokens=1000, completion_tokens=0, attempts=1))
    assert gate.check_before_request() is False
    assert gate.limit_hit == "cost_cap_yuan"


def test_cost_gate_retry_limit() -> None:
    """重试上限：累计重试次数达到 max_retries 后停止。"""
    gate = OcrCostGate(OcrCostConfig(max_retries=2))
    gate.record_usage(OcrUsageRecord(prompt_tokens=10, completion_tokens=0, attempts=2))
    gate.record_usage(OcrUsageRecord(prompt_tokens=10, completion_tokens=0, attempts=2))
    assert gate.total_retries == 2
    assert gate.check_before_request() is False
    assert gate.limit_hit == "max_retries"


def test_cost_gate_per_image_anomaly() -> None:
    """单图成本超基线 20% 自动暂停（技术方案 §11.3）。"""
    cfg = OcrCostConfig(
        baseline_cost_per_image_yuan=0.0003,
        pause_ratio_threshold=1.2,
    )
    gate = OcrCostGate(cfg)
    gate.record_usage(OcrUsageRecord(prompt_tokens=2000, completion_tokens=0, attempts=1))
    assert gate.pause_cost_anomaly is True
    assert gate.limit_hit == "per_image_cost_anomaly"
    assert gate.check_before_request() is False


def test_read_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """密钥只从环境读取。"""
    monkeypatch.setenv(DASHSCOPE_API_KEY_ENV, FAKE_KEY)
    assert read_dashscope_api_key() == FAKE_KEY


def test_read_api_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境变量缺失/空白时抛错。"""
    monkeypatch.delenv(DASHSCOPE_API_KEY_ENV, raising=False)
    with pytest.raises(KeyError):
        read_dashscope_api_key()


def test_redact_secrets() -> None:
    """脱敏：抹除密钥与 Base64 图片 Data URL。"""
    sample = f"Authorization: Bearer {FAKE_KEY} img=data:image/jpeg;base64,AAAAAAAAAAAAAAAAAAA"
    redacted = redact_secrets(sample, api_key=FAKE_KEY)
    assert FAKE_KEY not in redacted
    assert "<REDACTED>" in redacted
    assert "base64,AAAAAAAAAAAAAAAAAAA" not in redacted


def test_audit_no_secrets() -> None:
    """审计：文本含密钥或 Base64 图片时返回 False，否则 True。"""
    assert audit_no_secrets("plain report text") is True
    assert audit_no_secrets(f"leak {FAKE_KEY}", api_key=FAKE_KEY) is False
    assert audit_no_secrets("data:image/png;base64,CCCC") is False


def test_no_network_involved() -> None:
    """本模块纯函数/数据模型，不发起任何网络调用。"""
    cfg = default_ocr_run_config()
    body = REQUEST_CONTRACT_V1.request_body(image_data_url="data:image/jpeg;base64,AAAA")
    assert isinstance(body, dict)
    assert cfg.request.endpoint.startswith("https://dashscope.aliyuncs.com")
