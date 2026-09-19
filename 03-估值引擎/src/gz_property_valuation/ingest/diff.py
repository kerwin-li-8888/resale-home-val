"""Diff two raw snapshots of a current-only table.

Ported and renamed from Philly Fair Measure (fixed SHA e163eba6). The generic
null-safe, keep-last dedup diff logic is retained; the Philadelphia dataset
registry (``SNAPSHOT_DIFF_SPECS``) and dataset-specific notes are removed here
because they hard-code that city's tables. The ExampleCity dataset registry and
any dataset-specific notes are defined by the data work packages (WP3+), which
provide a ``DiffSpec`` (identity + watched columns) per dataset.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Final

import polars as pl

# Internal column-name sentinels. CARTO column names are lowercase
# alphanumerics/underscores, so "::" cannot collide with a real column
# (a plain "_new" suffix does: `building_code_new` is a real column).
_NEW: Final = "::new"
_CHG: Final = "chg::"


@dataclass(frozen=True)
class DiffSpec:
    """How to diff one dataset: identity columns + columns to track."""

    keys: tuple[str, ...]
    watched: tuple[str, ...]


@dataclass(frozen=True)
class ColumnChange:
    column: str
    n_changed: int
    # median of (new - old) among changed rows; None for non-numeric columns
    # or when nothing changed
    median_delta: float | None = None


@dataclass(frozen=True)
class SnapshotDiff:
    dataset: str
    prev_stamp: str
    new_stamp: str
    n_prev: int
    n_new: int
    n_added: int
    n_removed: int
    n_changed_rows: int
    columns: list[ColumnChange] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _load(path: Path, spec: DiffSpec) -> tuple[pl.DataFrame, int]:
    """(frame, n_dropped): key + watched columns only, keys stringified;
    duplicate-key rows deduplicate keep-last and null-key rows drop, with the
    dropped count returned so the report can say so instead of hiding it."""
    lf = pl.scan_parquet(path)
    present = set(lf.collect_schema().names())
    watched = [c for c in spec.watched if c in present]
    missing = [k for k in spec.keys if k not in present]
    if missing:
        raise ValueError(f"{path}: missing key column(s) {missing}")
    raw = lf.select(
        [pl.col(k).cast(pl.String).alias(k) for k in spec.keys] + [pl.col(c) for c in watched]
    ).collect()
    deduped = raw.drop_nulls(list(spec.keys)).unique(subset=list(spec.keys), keep="last")
    return deduped, raw.height - deduped.height


def diff_dataset(
    prev_path: Path,
    new_path: Path,
    spec: DiffSpec,
    *,
    dataset: str,
    prev_stamp: str,
    new_stamp: str,
) -> SnapshotDiff:
    prev, prev_dropped = _load(prev_path, spec)
    new, new_dropped = _load(new_path, spec)
    keys = list(spec.keys)
    # only columns present in BOTH snapshots are comparable (schema drift is a note)
    watched = [c for c in spec.watched if c in prev.columns and c in new.columns]
    notes = [
        f"column `{c}` not present in both snapshots; skipped"
        for c in spec.watched
        if (c in prev.columns) != (c in new.columns)
    ]
    if prev_dropped or new_dropped:
        notes.append(
            f"duplicate/null-key rows dropped before comparing (keep-last): "
            f"{prev_dropped:,} prev, {new_dropped:,} new"
        )

    n_added = new.join(prev, on=keys, how="anti").height
    n_removed = prev.join(new, on=keys, how="anti").height

    joined = prev.join(new, on=keys, how="inner", suffix=_NEW)
    flags = joined.with_columns(
        [pl.col(c).ne_missing(pl.col(c + _NEW)).alias(_CHG + c) for c in watched]
    )
    changed_any = (
        int(flags.select(pl.any_horizontal([pl.col(_CHG + c) for c in watched]).sum()).item())
        if watched
        else 0
    )

    columns: list[ColumnChange] = []
    for c in watched:
        n_changed = int(flags.select(pl.col(_CHG + c).sum()).item())
        median_delta: float | None = None
        if n_changed and flags.schema[c].is_numeric():
            delta = flags.filter(pl.col(_CHG + c)).select((pl.col(c + _NEW) - pl.col(c)).median())
            value = delta.item()
            median_delta = float(value) if value is not None else None
        columns.append(ColumnChange(column=c, n_changed=n_changed, median_delta=median_delta))

    return SnapshotDiff(
        dataset=dataset,
        prev_stamp=prev_stamp,
        new_stamp=new_stamp,
        n_prev=prev.height,
        n_new=new.height,
        n_added=n_added,
        n_removed=n_removed,
        n_changed_rows=changed_any,
        columns=columns,
        notes=notes,
    )


def render_markdown(diffs: list[SnapshotDiff], *, generated_at: str) -> str:
    """One compact markdown report for a snapshot run (committed to the repo)."""
    lines = [
        f"# Snapshot diff, {generated_at}",
        "",
        "What the source published in the current-only tables since the",
        "previous snapshot. This summary is the greppable provenance record.",
        "",
    ]
    for d in diffs:
        lines += [
            f"## {d.dataset}",
            "",
            f"`{d.prev_stamp}` -> `{d.new_stamp}`: "
            f"{d.n_prev:,} -> {d.n_new:,} rows, "
            f"{d.n_added:,} added, {d.n_removed:,} removed, "
            f"{d.n_changed_rows:,} rows with watched-column changes.",
            "",
        ]
        changed = [c for c in d.columns if c.n_changed]
        if changed:
            lines += ["| column | rows changed | median delta |", "| --- | ---: | ---: |"]
            for c in sorted(changed, key=lambda c: c.n_changed, reverse=True):
                delta = "" if c.median_delta is None else f"{c.median_delta:+,.0f}"
                lines.append(f"| {c.column} | {c.n_changed:,} | {delta} |")
            lines.append("")
        else:
            lines += ["No watched-column changes.", ""]
        for note in d.notes:
            lines.append(f"- {note}")
        if d.notes:
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"