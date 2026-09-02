"""snapshot-diff: null-safe change counts, key add/remove, dedup keep-last,
schema drift notes, and the markdown report (ExampleCity-neutral registry)."""

from pathlib import Path

import polars as pl

from compsval.ingest.diff import (
    ColumnChange,
    DiffSpec,
    SnapshotDiff,
    diff_dataset,
    render_markdown,
)


def _write(path: Path, df: pl.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def _diff(tmp_path: Path, prev: pl.DataFrame, new: pl.DataFrame, spec: DiffSpec) -> SnapshotDiff:
    return diff_dataset(
        _write(tmp_path / "prev.parquet", prev),
        _write(tmp_path / "new.parquet", new),
        spec,
        dataset="gzf_house",
        prev_stamp="20260701T000000Z",
        new_stamp="20260801T000000Z",
    )


SPEC = DiffSpec(keys=("house_id",), watched=("price", "quality"))


def _by_col(d: SnapshotDiff, name: str) -> ColumnChange:
    return next(c for c in d.columns if c.column == name)


def test_added_removed_and_changed(tmp_path: Path) -> None:
    prev = pl.DataFrame(
        {"house_id": ["1", "2", "3"], "price": [100, 200, 300], "quality": ["C", "C", "C"]}
    )
    new = pl.DataFrame(
        {"house_id": ["2", "3", "4"], "price": [250, 300, 400], "quality": ["C", "B", "C"]}
    )
    d = _diff(tmp_path, prev, new, SPEC)
    assert (d.n_prev, d.n_new, d.n_added, d.n_removed) == (3, 3, 1, 1)
    assert d.n_changed_rows == 2  # houses 2 and 3
    assert _by_col(d, "price").n_changed == 1
    assert _by_col(d, "price").median_delta == 50.0
    assert _by_col(d, "quality").n_changed == 1
    assert _by_col(d, "quality").median_delta is None  # non-numeric


def test_null_safe_comparison(tmp_path: Path) -> None:
    prev = pl.DataFrame(
        {"house_id": ["1", "2", "3"], "price": [None, None, 300], "quality": ["C", "C", "C"]}
    )
    new = pl.DataFrame(
        {"house_id": ["1", "2", "3"], "price": [None, 200, None], "quality": ["C", "C", "C"]}
    )
    d = _diff(tmp_path, prev, new, SPEC)
    assert _by_col(d, "price").n_changed == 2  # null->200 and 300->null; null->null is not
    assert d.n_changed_rows == 2


def test_duplicate_keys_keep_last(tmp_path: Path) -> None:
    prev = pl.DataFrame(
        {"house_id": ["1", "1"], "price": [100, 150], "quality": ["C", "C"]}
    )
    new = pl.DataFrame({"house_id": ["1"], "price": [150], "quality": ["C"]})
    d = _diff(tmp_path, prev, new, SPEC)
    assert d.n_prev == 1
    assert d.n_changed_rows == 0


def test_missing_watched_column_is_noted_not_fatal(tmp_path: Path) -> None:
    prev = pl.DataFrame({"house_id": ["1"], "price": [100]})
    new = pl.DataFrame({"house_id": ["1"], "price": [100]})
    d = _diff(tmp_path, prev, new, SPEC)
    assert {c.column for c in d.columns} == {"price"}
    assert not any("quality" in n for n in d.notes)  # absent in BOTH: silently ignored


def test_schema_drift_is_noted(tmp_path: Path) -> None:
    prev = pl.DataFrame({"house_id": ["1"], "price": [100]})
    new = pl.DataFrame({"house_id": ["1"], "price": [100], "quality": ["C"]})
    d = _diff(tmp_path, prev, new, SPEC)
    assert any("quality" in n and "skipped" in n for n in d.notes)


def test_watched_column_named_like_the_join_suffix(tmp_path: Path) -> None:
    """A real column named *_new must not collide with the join suffix."""
    spec = DiffSpec(keys=("house_id",), watched=("price", "price_new"))
    prev = pl.DataFrame(
        {"house_id": ["1", "2"], "price": ["A", "B"], "price_new": ["X", "Y"]}
    )
    new = pl.DataFrame(
        {"house_id": ["1", "2"], "price": ["A", "B"], "price_new": ["X", "Z"]}
    )
    d = _diff(tmp_path, prev, new, spec)
    assert _by_col(d, "price").n_changed == 0
    assert _by_col(d, "price_new").n_changed == 1


def test_dedup_drop_count_is_reported(tmp_path: Path) -> None:
    prev = pl.DataFrame(
        {
            "house_id": ["1", "1", None],  # one dup + one null key
            "price": [100, 150, 900],
            "quality": ["C", "C", "C"],
        }
    )
    new = pl.DataFrame({"house_id": ["1"], "price": [150], "quality": ["C"]})
    d = _diff(tmp_path, prev, new, SPEC)
    assert d.n_prev == 1
    assert any("dropped before comparing" in n and "2 prev, 0 new" in n for n in d.notes)


def test_markdown_renders_counts_and_notes(tmp_path: Path) -> None:
    prev = pl.DataFrame(
        {"house_id": ["1", "2"], "price": [100, 200], "quality": ["C", "C"]}
    )
    new = pl.DataFrame(
        {"house_id": ["1", "2"], "price": [100, 260], "quality": ["C", "C"]}
    )
    d = _diff(tmp_path, prev, new, SPEC)
    md = render_markdown([d], generated_at="2026-08-01")
    assert "# Snapshot diff, 2026-08-01" in md
    assert "## gzf_house" in md
    assert "| price | 1 | +60 |" in md
    assert "`20260701T000000Z` -> `20260801T000000Z`" in md