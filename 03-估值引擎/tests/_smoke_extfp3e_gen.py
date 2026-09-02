"""EXTFP3-E CLI 冒烟：生成一次最小 OCR 运行 + 词表 + 标注表（临时目录，不入 repo）。"""

import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from compsval.ingest.floorplan_asset import (
    ASSET_MANIFEST_FILENAME,
    AssetStatus,
    FloorplanAsset,
    FloorplanAssetRun,
)
from compsval.ingest.floorplan_ocr import OcrRunRecord, OcrState, OcrTaskRecord
from compsval.ingest.floorplan_ocr_parse import (
    OcrParseRecord,
    OcrWordRecord,
    WordParseState,
    normalize_text,
    write_word_table,
)
from compsval.ingest.floorplan_transcribe import (
    RoomAnnotationRecord,
    write_annotation_table,
)

base = Path(tempfile.mkdtemp(prefix="extfp3e-smoke-"))
run_dir = base / "run"
run_dir.mkdir(parents=True)
data_dir = base / "data"
data_dir.mkdir(parents=True)
LOC = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0]]
MODEL = "qwen-vl-ocr-2025-11-20"

task = OcrTaskRecord(
    ocr_task_id="task-v-a",
    ocr_run_id="run-v-a",
    asset_id="asset-v-a",
    request_hash="req-hash-v-1",
    image_sha256="img-sha256-v-1",
    state=OcrState.OCR_SUCCEEDED,
    model_returned=MODEL,
)
run = OcrRunRecord(
    ocr_run_id="run-v-a",
    asset_manifest_ref="manifest-ref-v-1",
    sourced=True,
    created_at="2026-08-25T00:00:00Z",
    updated_at="2026-08-25T00:00:00Z",
    run_dir=".",
    state_counts={OcrState.OCR_SUCCEEDED.value: 1},
    tasks=[task],
)
(run_dir / "ocr_run.json").write_text(run.model_dump_json(indent=2), encoding="utf-8")

words = [
    OcrWordRecord(
        word_id="w-0",
        ocr_run_id="run-v-a",
        ocr_task_id="task-v-a",
        order=0,
        text_raw="主卧",
        text_normalized=normalize_text("主卧"),
        location=LOC,
        parse_state=WordParseState.PARSED.value,
    ),
    OcrWordRecord(
        word_id="w-1",
        ocr_run_id="run-v-a",
        ocr_task_id="task-v-a",
        order=1,
        text_raw="12.5㎡",
        text_normalized=normalize_text("12.5㎡"),
        location=LOC,
        parse_state=WordParseState.PARSED.value,
    ),
]
write_word_table(
    [
        OcrParseRecord(
            ocr_task_id="task-v-a",
            ocr_run_id="run-v-a",
            source_state=OcrState.OCR_SUCCEEDED.value,
            model_requested=MODEL,
            model_returned=MODEL,
            model_match=True,
            words_count=2,
            parsed_count=2,
            parse_state="parsed",
            words=words,
        )
    ],
    data_dir,
)
anns = [
    RoomAnnotationRecord(
        annotation_id="a-1",
        ocr_run_id="run-v-a",
        ocr_task_id="task-v-a",
        room_word_id="w-0",
        area_word_id="w-1",
        room_name_raw="主卧",
        room_name_normalized="主卧",
        standard_room_type="master_bedroom",
        area_text_raw="12.5㎡",
        area_text_normalized="12.5m2",
        area_value="12.5",
        area_unit="m2",
        location=LOC,
        parse_state="ACCEPTED",
    )
]
write_annotation_table(anns, data_dir)

# 资产 manifest + Excel staged 表
batch = base / "batch"
batch.mkdir()
asset = FloorplanAsset(
    asset_id="asset-v-a",
    download_task_id="dt-a",
    source_record_id="R-a",
    source_row_number=1,
    url_ordinal=1,
    source_url_raw="https://ke-image.ljcdn.com/a.png",
    download_url="https://ke-image.ljcdn.com/a.png",
    downloader_version="EXTFP2-C-DL-1.0",
    asset_status=AssetStatus.DOWNLOADED,
    mime_type="image/png",
    file_extension=".png",
    width=8,
    height=6,
    byte_size=10,
    sha256="a" * 64,
    storage_path="a.png",
)
arun = FloorplanAssetRun(
    batch_id="batch-v-1",
    download_run_id="dl-run-v-1",
    download_run_dir=".",
    manifest_ref="manifest-ref-v-1",
    sourced=True,
    created_at="2026-08-25T00:00:00Z",
    assets=[asset],
    counts={"DOWNLOADED": 1},
)
manifest = batch / ASSET_MANIFEST_FILENAME
manifest.write_text(arun.model_dump_json(indent=2), encoding="utf-8")

rows = [
    {
        "source_record_id": "R-a",
        "bedrooms_raw": "1室",
        "living_rooms_raw": "0厅",
        "transaction_area_sqm": Decimal("12.5"),
        "building_area_detail_sqm": Decimal("12.5"),
    }
]
excel = base / "staged_excel.parquet"
pq.write_table(pa.Table.from_pylist(rows), excel, compression="zstd")

print(base)
print(f"--run={run_dir} --data-dir={data_dir} --asset-manifest={manifest} --staged-table={excel}")
