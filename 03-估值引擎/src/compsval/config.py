"""Project configuration.

Phase-1 skeleton configuration. Philadelphia-specific constants (the OPA
condo prefix and the CARTO "core tables" snapshot set) are intentionally
removed; the data-lake root and its environment override remain, and the
environment variable is renamed from ``PHILLY_DATA_DIR`` to ``COMPSVAL_DATA_DIR``.
The snapshot-dataset registry is deferred to the data-contract work package
(WP3), which defines the ExampleCity source/dataset set (see ``contract``).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

DEFAULT_DATA_DIR: Final = Path("data")


def data_dir() -> Path:
    """Root of the local data lake (raw/, staged/, marts/).

    Defaults to ./data; override with the COMPSVAL_DATA_DIR environment variable.
    """
    return Path(os.environ.get("COMPSVAL_DATA_DIR", str(DEFAULT_DATA_DIR)))


def evidence_dir() -> Path:
    """Root of the immutable raw evidence (page screenshots/text).

    Distinct from :func:`data_dir`: evidence holds the source-of-record pages
    captured by WP2/WP3-A (PNG/TXT/HTML under ``01-数据/raw``), registered as
    ``raw_snapshot`` records by the data contract without a parquet main chain.
    Defaults to the repository's ``01-数据/raw`` regardless of the working
    directory; override with the COMPSVAL_EVIDENCE_DIR environment variable.
    """
    default = Path(__file__).resolve().parents[3] / "01-数据" / "raw"
    return Path(os.environ.get("COMPSVAL_EVIDENCE_DIR", str(default)))