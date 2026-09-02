"""Synthetic end-to-end demo for compsval (offline, deterministic).

Builds a fictional community 春晖里 (C-DEMO-0001) data drop, runs the engine's
frozen-estimate chain and the 12-section Markdown report builder, then prints
a result summary. All data is SYNTHETIC; nothing is fetched from any source.

Usage (from 03-估值引擎/):
    uv run python examples/synthetic_demo.py            # run, print summary
    uv run python examples/synthetic_demo.py --check    # compare vs last summary
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval import cli
from compsval.ingest.manifests import DerivedManifest, InputRef, write_derived_manifest
from compsval.ingest.stage import MARTS_LAYER, VALID_SALE_FILENAME

COMMUNITY_ID = "C-DEMO-0001"
COMMUNITY_NAME = "春晖里"
VAL_DATE = date(2026, 7, 21)
RUN_DATE = VAL_DATE.isoformat()
EXAMPLES_DIR = Path(__file__).resolve().parent
OUT_DIR = EXAMPLES_DIR / "out"
SUMMARY = OUT_DIR / "result-summary.json"


def build_data_drop(data_dir: Path) -> None:
    """Deterministic minimal data drop: marts/valid_sale + 3 entity tables."""
    marts = data_dir / MARTS_LAYER
    entities = data_dir / "entities"
    marts.mkdir(parents=True, exist_ok=True)
    entities.mkdir(parents=True, exist_ok=True)

    valid_sale = pa.table(
        {
            "sale_event_id": ["d1", "d2", "d3", "d4", "d5"],
            "community_id": [COMMUNITY_ID] * 5,
            "sale_date": [
                date(2026, 7, 20),
                date(2026, 7, 10),
                date(2026, 6, 20),
                date(2026, 5, 1),
                date(2026, 4, 1),
            ],
            "layout": ["2室1厅"] * 5,
            "area_sqm": [55.0, 56.0, 54.0, 57.0, 53.0],
            "total_price_yuan": [1430000.0, 1428000.0, 1377000.0, 1453500.0, 1307400.0],
            "unit_price": [26000, 25500, 25500, 25500, 24668],
            "anomaly_flag": ["正常"] * 5,
            "raw_locator": ["1", "2", "3", "4", "5"],
            "orientation": ["南"] * 5,
        }
    )
    valid_sale_path = marts / VALID_SALE_FILENAME
    pq.write_table(valid_sale, valid_sale_path)
    write_derived_manifest(
        DerivedManifest(
            layer=MARTS_LAYER,
            table="valid_sale",
            built_at=datetime(2026, 7, 21, 0, 0, 0, tzinfo=UTC),
            row_count=5,
            inputs=[InputRef(dataset="demo/synthetic", fetched_at="20260721T000000Z")],
            package_version="0.1.0",
            notes="synthetic demo fixture (fictional community)",
        ),
        valid_sale_path,
    )

    pq.write_table(
        pa.table({"community_id": [COMMUNITY_ID], "block": [COMMUNITY_NAME]}),
        entities / "community.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "region": [COMMUNITY_NAME] * 3,
                "month": [date(2026, 4, 1), date(2026, 5, 1), date(2026, 6, 1)],
                "price": [24800, 25200, 25500],
            }
        ),
        entities / "market_series.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "community_id": [COMMUNITY_ID],
                "total_floors": [18],
                "has_elevator": [True],
                "year_built": [2008],
            }
        ),
        entities / "building.parquet",
    )


def write_subject(data_dir: Path) -> Path:
    subject = {
        "subject_id": "SUBJ-DEMO-001",
        "community_id": COMMUNITY_ID,
        "area_sqm": 55.6,
        "layout": "2室1厅",
        "valuation_date": RUN_DATE,
        "building_name": "UNKNOWN",
        "floor": None,
        "total_floors": 18,
        "has_elevator": True,
        "orientation": "南",
        "year_built": 2008,
        "site_observations": None,
    }
    path = data_dir / "subject.json"
    path.write_text(json.dumps(subject, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_command(argv: list[str]) -> dict:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = cli.main(argv)
    if rc != 0:
        raise SystemExit(f"command failed rc={rc}: {' '.join(argv)}\n{buf.getvalue()}")
    return json.loads(buf.getvalue())


def main() -> int:
    import shutil

    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    data_dir = OUT_DIR / "data"
    reports = OUT_DIR / "reports"
    build_data_drop(data_dir)
    subject_path = write_subject(data_dir)

    env = run_command(
        [
            "estimate",
            "--subject",
            str(subject_path),
            "--as-of",
            RUN_DATE,
            "--data-dir",
            str(data_dir),
            "--out-dir",
            str(reports),
        ]
    )
    run_id = env.get("run_id") or ""
    if not run_id:
        raise SystemExit(f"no run_id in envelope: {json.dumps(env)[:400]}")

    report_env = run_command(
        [
            "report",
            "build",
            "--valuation",
            run_id,
            "--data-dir",
            str(data_dir),
            "--out-dir",
            str(reports),
        ]
    )

    result = env["result"]
    summary = {
        "community": COMMUNITY_NAME,
        "community_id": COMMUNITY_ID,
        "subject_id": "SUBJ-DEMO-001",
        "as_of": RUN_DATE,
        "status": result["status"],
        "center": str(result["center"]),
        "range": [str(result["range"][0]), str(result["range"][1])],
        "confidence": result["confidence"],
        "run_id": run_id,
        "report_artifacts": report_env.get("artifacts", []),
    }
    SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print("=" * 62)
    print(f"compsval synthetic demo — {COMMUNITY_NAME} ({COMMUNITY_ID})")
    print(f"  as-of      : {RUN_DATE}")
    print(f"  status     : {summary['status']}")
    print(f"  center     : {summary['center']} 元/㎡")
    print(f"  range      : {summary['range'][0]} ~ {summary['range'][1]} 元/㎡")
    print(f"  confidence : {summary['confidence']}")
    print(f"  run_id     : {run_id}")
    print(f"  outputs    : {OUT_DIR}")
    print("=" * 62)
    return 0


if __name__ == "__main__":
    if "--check" in sys.argv:
        previous = SUMMARY.read_text(encoding="utf-8") if SUMMARY.exists() else ""
        main()
        current = SUMMARY.read_text(encoding="utf-8")
        if previous and previous != current:
            print("CHECK FAILED: summary changed between runs", file=sys.stderr)
            raise SystemExit(1)
        print("CHECK OK: summary identical across runs")
        raise SystemExit(0)
    raise SystemExit(main())
