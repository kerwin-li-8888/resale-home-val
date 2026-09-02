"""compsval CLI skeleton: version, catalog (empty + populated), and sql over the
latest raw snapshot. `system check` is exercised separately (it shells out)."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

from compsval import __version__, cli
from compsval.ingest.snapshots import write_raw_snapshot


def test_version_prints_package_name_and_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["version"]) == 0
    assert capsys.readouterr().out.strip() == f"compsval {__version__}"


def test_no_args_prints_help_and_fails(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main([]) == 2
    out = capsys.readouterr().out
    assert "usage: compsval" in out


def test_catalog_empty_reports_no_snapshots(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["catalog", "--data-dir", str(tmp_path)]) == 0
    assert f"(no raw snapshots under {tmp_path})" in capsys.readouterr().out


def test_catalog_lists_registered_sources_and_datasets(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["catalog", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "registered example-city sources (data contract):" in out
    assert "SRC-005" in out
    assert "registered datasets:" in out
    assert "chengjiao" in out


def _seed_snapshot(tmp_path: Path) -> None:
    write_raw_snapshot(
        pa.table({"house_id": ["1", "2"], "price": [3_200_000.0, 4_500_000.0]}),
        root=tmp_path,
        source="lianjia",
        dataset="chengjiao",
        fetched_at=datetime(2026, 8, 21, 0, 0, 0, tzinfo=UTC),
        query="SELECT house_id, price FROM ...",
    )


def test_catalog_lists_snapshot(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_snapshot(tmp_path)
    assert cli.main(["catalog", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "lianjia/chengjiao@20260821T000000Z" in out


def test_sql_queries_views(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_snapshot(tmp_path)
    assert (
        cli.main(
            ["sql", "SELECT count(*) AS n FROM raw_chengjiao", "--data-dir", str(tmp_path)]
        )
        == 0
    )
    assert "2" in capsys.readouterr().out


def test_sql_max_rows_limits_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_snapshot(tmp_path)
    assert (
        cli.main(
            [
                "sql",
                "SELECT house_id FROM raw_chengjiao ORDER BY house_id",
                "--data-dir",
                str(tmp_path),
                "--max-rows",
                "1",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "1" in out
    assert "2" not in out