"""冻结目录写保护与 run_id 冲突校验（change fix-fvv-frozen-evidence-integrity）。

冻结目录登记文件（``frozen_dirs_registry.json``，位于 ``data/versions/``）登记各
冻结目录的**全目录内容指纹**（sorted 相对路径 + 逐文件 SHA256 拼接哈希）与关键
运行记录（``ocr_run.json`` / ``ocr_state.json``）内容哈希。

fail-closed 语义（审核第二轮整改）：

- 消费路径（写保护、run_id 校验、指纹核验）在登记文件**缺失或损坏**时一律抛错
  拒绝服务，SHALL NOT 静默降级为无保护；
- 登记条目路径相对仓库根存储（与 freeze manifest 同基准），SHALL NOT 含绝对
  路径，保证入库后跨机器可移植；
- 登记的生产者（``sync_frozen_registry``，首次冻结时尚无登记文件）是唯一允许
  以缺失初始化的入口；冻结流程产出的登记随文件入库。
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REGISTRY_FILENAME = "frozen_dirs_registry.json"
KEY_RUN_FILENAMES: tuple[str, ...] = ("ocr_run.json", "ocr_state.json")


class FrozenRegistryError(RuntimeError):
    """冻结目录登记缺失或损坏：消费路径 SHALL 拒绝服务（fail-closed）。"""


class FrozenDirWriteError(RuntimeError):
    """目标路径命中冻结登记目录，写/改/删操作被拒绝。"""


class FrozenRunIdConflictError(RuntimeError):
    """run_id 命中冻结登记或与其他运行目录冲突，启动/续跑被拒绝并要求显式新 run_id。"""


def default_registry_path() -> Path:
    """默认登记文件位置：``<03-估值引擎>/data/versions/frozen_dirs_registry.json``。"""
    return Path(__file__).resolve().parents[3] / "data" / "versions" / REGISTRY_FILENAME


def registry_base_dir(registry_path: Path) -> Path:
    """条目相对路径锚点：仓库根（登记文件位于 ``<仓库根>/03-估值引擎/data/versions/``）。

    条目路径 SHALL 相对仓库根存储（与 freeze manifest 同基准），SHALL NOT 含绝对
    路径；clone 后目录结构不变，锚点解析跨机器可移植。
    """
    return registry_path.resolve().parents[3]


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def directory_content_fingerprint(directory: Path) -> tuple[str, int]:
    """全目录内容指纹：sorted(相对路径, 文件 SHA256) 拼接后 SHA256（含空目录哨兵）。"""
    digest = hashlib.sha256()
    count = 0
    for file_path in sorted(directory.rglob("*")):
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(directory).as_posix()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(_sha256_of(file_path).encode("utf-8"))
        digest.update(b"\x00")
        count += 1
    if count == 0:
        digest.update(b"EMPTY_DIR\x00")
    return digest.hexdigest(), count


_CACHE: dict[Path, tuple[tuple[int, int], dict[str, Any]]] = {}


def load_frozen_registry(registry_path: Path | None = None) -> dict[str, Any]:
    """读取冻结目录登记（消费路径）。

    登记文件缺失、非法 JSON、缺少 ``dirs``、sidecar 缺失或与登记实测 SHA256
    不一致时一律抛 :class:`FrozenRegistryError`，SHALL NOT 静默降级为无保护
    （登记内容可被篡改而不被察觉视同损坏）。登记的初始化仅由
    :func:`sync_frozen_registry` 在登记文件确实不存在时进行。
    """
    path = (registry_path or default_registry_path()).resolve()
    if not path.is_file():
        raise FrozenRegistryError(
            f"冻结目录登记文件缺失：{path}；消费冻结数据前 SHALL 先由冻结流程"
            "生成登记并入库（fail-closed，不降级为无保护）"
        )
    sidecar = path.with_name(path.name + ".sha256")
    sidecar_mtime = sidecar.stat().st_mtime_ns if sidecar.is_file() else -1
    cache_key = (path.stat().st_mtime_ns, sidecar_mtime)
    cached = _CACHE.get(path)
    if cached is not None and cached[0] == cache_key:
        return cached[1]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise FrozenRegistryError(
            f"冻结目录登记文件损坏（非法 JSON：{exc}）：{path}"
        ) from exc
    if not isinstance(data, dict) or not isinstance(data.get("dirs"), dict):
        raise FrozenRegistryError(f"冻结目录登记文件损坏（缺少 dirs 对象）：{path}")
    if not sidecar.is_file():
        raise FrozenRegistryError(
            f"冻结目录登记 sidecar 缺失：{sidecar}；无法核验登记内容，"
            "fail-closed 拒绝服务"
        )
    registered = sidecar.read_text(encoding="utf-8").strip()
    actual = _sha256_of(path)
    if registered != actual:
        raise FrozenRegistryError(
            f"冻结目录登记与 sidecar 不一致：{path}"
            f"（实测 {actual} / 登记 {registered}）；登记内容疑被改动，"
            "fail-closed 拒绝服务，处置交用户裁定"
        )
    _CACHE[path] = (cache_key, data)
    return data


def _frozen_dir_paths(registry: dict[str, Any], registry_path: Path) -> list[tuple[Path, str]]:
    base = registry_base_dir(registry_path)
    return [(base / rel, rel) for rel in registry["dirs"]]


def ensure_writable(path: Path, *, registry_path: Path | None = None) -> None:
    """路径（或其任一祖先目录）命中冻结登记时抛 :class:`FrozenDirWriteError`。

    登记缺失/损坏时经 :class:`FrozenRegistryError` fail-closed（调用方同为拒绝语义）。
    """
    path_arg = Path(path).resolve()
    resolved_registry = (registry_path or default_registry_path()).resolve()
    frozen_dirs = _frozen_dir_paths(
        load_frozen_registry(resolved_registry), resolved_registry
    )
    for frozen_dir, rel in frozen_dirs:
        if path_arg == frozen_dir or frozen_dir in path_arg.parents:
            raise FrozenDirWriteError(
                f"写操作被拒绝：{path_arg} 命中冻结登记目录 {rel}；"
                "冻结产物只读，修复须走用户确认的留痕式恢复流程"
            )


def _read_run_id(run_dir: Path) -> str | None:
    record_path = run_dir / "ocr_run.json"
    if not record_path.is_file():
        return None
    try:
        data = json.loads(record_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    run_id = data.get("ocr_run_id") if isinstance(data, dict) else None
    return str(run_id) if run_id else None


def validate_run_id(
    run_id: str,
    run_dir: Path,
    out_raw_dir: Path,
    *,
    registry_path: Path | None = None,
) -> None:
    """run 启动/续跑前校验 run_id 唯一性与冲突，违规抛 :class:`FrozenRunIdConflictError`。

    三条 fail-closed 规则：

    1. run_id 命中冻结目录登记（含冻结 run 目录名派生的 run_id）；
    2. ``out_raw_dir`` 下存在其他目录的 ``ocr_run.json`` 记录同一 run_id
       （run_id 已映射到别处，如被隔离/迁移的目录）；
    3. 解析到的 run 目录已属于其他运行（目录内 ``ocr_run.json`` 的
       ``ocr_run_id`` 与本次 run_id 不一致）。
    """
    resolved_registry = (registry_path or default_registry_path()).resolve()
    registry = load_frozen_registry(resolved_registry)
    for entry in registry["dirs"].values():
        if isinstance(entry, dict) and entry.get("run_id") == run_id:
            raise FrozenRunIdConflictError(
                f"run_id 命中冻结登记目录（{entry.get('path', '')}），"
                f"SHALL 显式提供新 run_id：{run_id}"
            )
    raw_root = Path(out_raw_dir)
    if raw_root.is_dir():
        for child in sorted(raw_root.iterdir()):
            if child == Path(run_dir) or not child.is_dir():
                continue
            if _read_run_id(child) == run_id:
                raise FrozenRunIdConflictError(
                    f"run_id 已映射到其他运行目录（{child.name}），"
                    f"SHALL 显式提供新 run_id：{run_id}"
                )
    existing = _read_run_id(Path(run_dir))
    if existing is not None and existing != run_id:
        raise FrozenRunIdConflictError(
            f"run 目录已属于其他运行（ocr_run_id={existing}），"
            f"SHALL 显式提供新 run_id：{run_id}"
        )


def _entry_fingerprints(dir_path: Path) -> dict[str, Any]:
    content_fingerprint, file_count = directory_content_fingerprint(dir_path)
    key_files = {
        filename: _sha256_of(dir_path / filename)
        for filename in KEY_RUN_FILENAMES
        if (dir_path / filename).is_file()
    }
    return {
        "content_fingerprint": content_fingerprint,
        "file_count": file_count,
        "key_files": key_files,
    }


def sync_frozen_registry(
    manifest: Any,
    repo_root: Path,
    *,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    """把冻结清单中的目录条目合并写入冻结目录登记（全目录内容指纹）。

    只新增、不改写既有登记（既有指纹是冻结时点权威）；返回
    ``{"ok": bool, "gaps": [...]}``，失败时冻结流程 SHALL NOT 标记完成。

    第四轮审核 C1：仅当登记文件**确实不存在**时才允许空初始化；登记文件已存在
    时生产者同样按 ``strict=True`` 校验（含 sidecar），校验失败即返回 ``ok=False``
    且**不得改写登记与 sidecar 任何字节**（损坏/被截断的登记 SHALL NOT 被重新
    签名为有效，既有条目 SHALL NOT 丢失）。
    """
    path = (registry_path or default_registry_path()).resolve()
    if path.is_file():
        try:
            registry = load_frozen_registry(path)
        except FrozenRegistryError as exc:
            return {
                "ok": False,
                "gaps": [f"既有冻结目录登记损坏或被篡改，拒绝改写: {exc}"],
                "registry_path": path,
            }
    else:
        registry = {"registry_version": 2, "updated_at": None, "notes": [], "dirs": {}}
    dirs: dict[str, Any] = dict(registry.get("dirs", {}))
    gaps: list[str] = []
    added = 0
    sections = getattr(manifest, "sections", None) or {}
    for section, entries in sections.items():
        for entry in entries:
            if getattr(entry, "kind", "") != "dir":
                continue
            rel = str(entry.path)
            dir_path = repo_root / rel
            if not dir_path.is_dir():
                gaps.append(f"登记条目目录缺失：{rel}")
                continue
            if rel in dirs:
                continue
            name = Path(rel).name
            run_id = name.removeprefix("run_") if name.startswith("run_") else None
            fingerprints = _entry_fingerprints(dir_path)
            dirs[rel] = {
                "path": rel,
                "section": section,
                "key": str(getattr(entry, "key", "")),
                "run_id": run_id,
                "fingerprint_level": "content",
                **fingerprints,
                "registered_at": datetime.now(UTC).isoformat(),
            }
            added += 1
    payload = {
        "registry_version": 2,
        "updated_at": datetime.now(UTC).isoformat(),
        "notes": list(registry.get("notes", [])),
        "dirs": dirs,
    }
    work = path.with_name(path.name + ".incomplete")
    work.parent.mkdir(parents=True, exist_ok=True)
    work.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    work.replace(path)
    sidecar = path.with_name(path.name + ".sha256")
    sidecar.write_text(_sha256_of(path), encoding="utf-8")
    _CACHE.pop(path, None)
    return {
        "ok": not gaps,
        "gaps": gaps,
        "registry_path": path,
        "added_dirs": added,
        "total_dirs": len(dirs),
    }
