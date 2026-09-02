"""Project configuration: default data-lake root and the COMPSVAL_DATA_DIR override."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from compsval import config
from compsval.ingest.snapshots import write_raw_snapshot


def test_default_data_dir_is_relative_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COMPSVAL_DATA_DIR", raising=False)
    assert config.data_dir() == Path("data")
    assert not config.data_dir().is_absolute()


def test_env_override_points_at_custom_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPSVAL_DATA_DIR", str(Path("D:/custom-lake")))
    assert config.data_dir() == Path("D:/custom-lake")


def test_data_dir_used_by_snapshot_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("COMPSVAL_DATA_DIR", str(tmp_path))
    result = write_raw_snapshot(
        pa.table({"house_id": ["1"]}),
        source="lianjia",
        dataset="chengjiao",
        fetched_at=datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC),
        query="SELECT ...",
    )
    assert result.directory.resolve().parent.parent.parent == tmp_path / "raw"


def test_evidence_dir_points_at_repo_raw_dir() -> None:
    evidence = config.evidence_dir()
    assert evidence.name == "raw"
    assert (evidence.parent.name) == "01-数据"


def test_evidence_dir_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPSVAL_EVIDENCE_DIR", str(Path("D:/custom-evidence")))
    assert config.evidence_dir() == Path("D:/custom-evidence")