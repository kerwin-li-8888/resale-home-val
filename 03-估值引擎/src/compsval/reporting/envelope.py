"""WP7-A 输出契约基础设施：统一 JSON 输出包络 + 退出码 + 命令错误分级。

技术方案 §10.1/§10.3/§10.4：所有 Agent 可调用命令输出统一包络，退出码与
业务状态分离。``command_status=success`` 只表示程序完成，不代表估值可信；
Agent 必须解析 ``business_status``、``warnings`` 与可信度后才判断结果。

设计要点（对应 WP7-A 验收标准）：

- **包络字段与 §10.3 一致（验收①）**：``schema_version=1.0``、``command``、
  ``command_status``、``business_status``、``run_id``、``data_version``、
  ``rule_version``、``result``、``warnings``、``errors``、``artifacts``；
- **command_status 与 business_status 分离（验收②）**：程序完成状态与估值
  业务状态是两回事，success 包络同样可以携带业务降级/不足/不适用信息；
- **退出码 0/2/3/4/5 与 §10.4 语义一致（验收③）**：``CommandError`` 分级异常
  携带退出码，命令层捕获后映射为退出码与 ``errors`` 文本；
- **可 JSON 序列化 round-trip（验收④）**：包络经 ``model_dump_json`` /
  ``model_validate_json`` 无损往返。

本模块只定义契约数据结构与错误类型，不实现具体命令（estimate/run show/
report build/review apply 归 WP7-B/C/D）。
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

#: 统一输出包络版本（§10.3）。
ENVELOPE_SCHEMA_VERSION = "1.0"

# ---------------------------------------------------------------------------
# 退出码（§10.4）
# ---------------------------------------------------------------------------

#: 命令成功完成，包括业务状态为信息不足或不适用。
EXIT_OK = 0
#: 输入或配置不合法。
EXIT_INVALID_INPUT = 2
#: 必要文件、数据版本或依赖缺失。
EXIT_MISSING_DEPENDENCY = 3
#: 数据、规则或运行版本不一致，拒绝继续。
EXIT_VERSION_MISMATCH = 4
#: 未预期内部错误。
EXIT_INTERNAL_ERROR = 5

#: 退出码 -> §10.4 语义说明（用于错误文本与测试校验）。
EXIT_CODE_LABELS: dict[int, str] = {
    EXIT_OK: "命令成功完成，包括业务状态为信息不足或不适用",
    EXIT_INVALID_INPUT: "输入或配置不合法",
    EXIT_MISSING_DEPENDENCY: "必要文件、数据版本或依赖缺失",
    EXIT_VERSION_MISMATCH: "数据、规则或运行版本不一致，拒绝继续",
    EXIT_INTERNAL_ERROR: "未预期内部错误",
}

#: §10.4 允许的退出码集合（1 未定义，禁止使用）。
VALID_EXIT_CODES = frozenset(EXIT_CODE_LABELS)


class CommandStatus(StrEnum):
    """程序完成状态（§10.3 ``command_status``）。"""

    SUCCESS = "success"
    FAILURE = "failure"


#: business_status 允许的业务状态值（README §6.10 四状态 + 候选）。
#: 来自 WP6 输出（OutputStatus=候选/参考/正式）与聚合降级路径（信息不足/
#: 不适用由 reason 表达）。字段类型为 str，由命令层从估值结果填充。
BUSINESS_STATUS_VALUES: frozenset[str] = frozenset(
    {"候选", "参考", "正式", "信息不足", "不适用"}
)


class CommandError(Exception):
    """命令错误：携带退出码与稳定错误类别。

    命令层应捕获具体子类并按 ``exit_code`` 退出、把信息写入包络 ``errors``。
    """

    #: 本错误对应的退出码（§10.4），子类覆盖。
    exit_code: int = EXIT_INTERNAL_ERROR

    def __init__(self, message: str, *, exit_code: int | None = None) -> None:
        super().__init__(message)
        if exit_code is not None:
            self.exit_code = exit_code


class InvalidInputError(CommandError):
    """输入或配置不合法（退出码 2）。"""

    exit_code = EXIT_INVALID_INPUT


class MissingDependencyError(CommandError):
    """必要文件、数据版本或依赖缺失（退出码 3）。"""

    exit_code = EXIT_MISSING_DEPENDENCY


class VersionMismatchError(CommandError):
    """数据、规则或运行版本不一致，拒绝继续（退出码 4）。"""

    exit_code = EXIT_VERSION_MISMATCH


class InternalCommandError(CommandError):
    """未预期内部错误（退出码 5）。"""

    exit_code = EXIT_INTERNAL_ERROR


class OutputEnvelope(BaseModel):
    """统一输出包络（§10.3）。

    所有命令的 stdout 只写这一个 JSON 对象；日志与进度写 stderr。
    """

    #: 包络 schema 版本（当前 1.0）。
    schema_version: str = ENVELOPE_SCHEMA_VERSION
    #: 命令名（如 ``estimate``）。
    command: str
    #: 程序完成状态（success 不代表估值可信）。
    command_status: CommandStatus = CommandStatus.SUCCESS
    #: 估值业务状态（候选/参考/正式/信息不足/不适用；可为空表示未产出判定）。
    business_status: str | None = None
    #: 本次运行 ID（如无运行则为 None）。
    run_id: str | None = None
    #: 数据版本（如无则为 None）。
    data_version: str | None = None
    #: 规则版本（如无则为 None）。
    rule_version: str | None = None
    #: 机器可解析的结果对象（命令相关，可为空对象）。
    result: dict[str, Any] = Field(default_factory=dict)
    #: 非阻断警告列表（Agent 必须读取）。
    warnings: list[str] = Field(default_factory=list)
    #: 错误列表（command_status=failure 时必填）。
    errors: list[str] = Field(default_factory=list)
    #: 产物路径列表（冻结 JSON、Markdown 报告等）。
    artifacts: list[str] = Field(default_factory=list)

    def add_warning(self, message: str) -> None:
        """追加一条非阻断警告。"""
        self.warnings.append(message)

    def add_error(self, message: str) -> None:
        """追加一条错误（同时把 command_status 置为 failure）。"""
        self.errors.append(message)
        self.command_status = CommandStatus.FAILURE

    def add_artifact(self, path: str) -> None:
        """登记一个产物路径。"""
        self.artifacts.append(path)


def envelope_from_error(command: str, error: CommandError) -> OutputEnvelope:
    """从命令错误构造 failure 包络（携带退出码语义与错误文本）。"""
    envelope = OutputEnvelope(command=command, command_status=CommandStatus.FAILURE)
    envelope.add_error(f"exit {error.exit_code}: {error}")
    return envelope
