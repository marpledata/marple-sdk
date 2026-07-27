# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyarrow==24.0.0",
# ]
# ///
"""Oracle for parquet-transcode prepare-upload.

Derives temporary MATLAB-like Snappy staging files from canonical lake
fixtures under test_data/dataset=8, runs prepare-upload, and checks:

- Arrow schema equality including PARQUET:field_id metadata
- ZSTD compression on every column
- Identity / time statistics and null counts
- Semantic table equality against the original lake fixture
- JSON stdout size / rows / footer
- One ZSTD -> Snappy round trip via the legacy directory mode
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = [
    ROOT / "test_data/dataset=8/signal=86/mdb_Usage_kWh.parquet",
    ROOT / "test_data/dataset=8/signal=82/mdb_Load_Type.parquet",
]
ROW_GROUP_SIZE = 1_048_576

LAKE_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"}),
        pa.field("signal", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "2"}),
        pa.field("time", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "3"}),
        pa.field("value", pa.float64(), nullable=True, metadata={"PARQUET:field_id": "4"}),
        pa.field("value_text", pa.string(), nullable=True, metadata={"PARQUET:field_id": "5"}),
    ]
)


def find_binary() -> Path:
    env = os.environ.get("PARQUET_TRANSCODE_BIN")
    if env:
        return Path(env)
    candidates = [
        ROOT / "target/debug/parquet-transcode",
        ROOT / "target/release/parquet-transcode",
    ]
    cargo_target = os.environ.get("CARGO_TARGET_DIR")
    if cargo_target:
        candidates.extend(
            [
                Path(cargo_target) / "debug/parquet-transcode",
                Path(cargo_target) / "release/parquet-transcode",
            ]
        )
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError(
        "parquet-transcode binary not found; set PARQUET_TRANSCODE_BIN or build with cargo test/build"
    )


def read_lake(path: Path) -> pa.Table:
    # Prefer ParquetFile.read to avoid dataset schema merge quirks.
    return pq.ParquetFile(path).read()


def write_staging(lake: pa.Table, path: Path) -> None:
    staging = lake.select(["time", "value", "value_text"])
    staging = staging.set_column(
        2,
        "value_text",
        staging["value_text"].cast(pa.large_string()),
    )
    pq.write_table(staging, path, compression="snappy")


def footer_length(path: Path) -> int:
    with path.open("rb") as file:
        file.seek(-8, 2)
        return int.from_bytes(file.read(4), "little")


def assert_lake_metadata(path: Path, expected: pa.Table, meta: dict) -> None:
    pf = pq.ParquetFile(path)
    table = pf.read()
    assert table.schema.equals(LAKE_SCHEMA, check_metadata=True), table.schema
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected), "logical table does not match canonical fixture"

    assert meta["rows"] == expected.num_rows
    assert meta["size"] == path.stat().st_size
    assert meta["footer"] == footer_length(path)
    assert meta["footer"] > 0

    assert pf.metadata.num_rows == expected.num_rows
    expected_groups = (expected.num_rows + ROW_GROUP_SIZE - 1) // ROW_GROUP_SIZE
    assert pf.metadata.num_row_groups == expected_groups

    dataset_id = expected["dataset"][0].as_py()
    signal_id = expected["signal"][0].as_py()
    for rg_idx in range(pf.metadata.num_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            assert col.compression == "ZSTD", col.path_in_schema
            stats = col.statistics
            assert stats is not None, col.path_in_schema
            name = col.path_in_schema
            if name == "dataset":
                assert stats.has_min_max and stats.min == dataset_id and stats.max == dataset_id
                assert stats.null_count == 0
            elif name == "signal":
                assert stats.has_min_max and stats.min == signal_id and stats.max == signal_id
                assert stats.null_count == 0
            elif name == "time":
                assert stats.has_min_max
                assert stats.null_count == 0
            elif name == "value":
                assert stats.null_count == expected["value"].null_count
            elif name == "value_text":
                assert stats.null_count == expected["value_text"].null_count


def run_prepare_upload(binary: Path, staging: Path, output: Path, lake: pa.Table) -> dict:
    dataset_id = lake["dataset"][0].as_py()
    signal_id = lake["signal"][0].as_py()
    result = subprocess.run(
        [
            str(binary),
            "prepare-upload",
            "--input",
            str(staging),
            "--output",
            str(output),
            "--dataset-id",
            str(dataset_id),
            "--signal-id",
            str(signal_id),
            "--expected-rows",
            str(lake.num_rows),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stderr == "" or "error" not in result.stderr.lower()
    return json.loads(result.stdout)


def round_trip_snappy(binary: Path, upload: Path, expected: pa.Table) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        copy = work / "roundtrip.parquet"
        copy.write_bytes(upload.read_bytes())
        subprocess.run([str(binary), str(work)], check=True, capture_output=True, text=True)
        got = pq.ParquetFile(copy).read()
        assert got.schema.equals(expected.schema, check_metadata=True)
        assert got.equals(expected)
        pf = pq.ParquetFile(copy)
        assert pf.metadata.row_group(0).column(0).compression == "SNAPPY"


def check_fixture(binary: Path, fixture: Path) -> None:
    lake = read_lake(fixture)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        staging = tmp_path / "staging.parquet"
        output = tmp_path / "upload.parquet"
        write_staging(lake, staging)
        meta = run_prepare_upload(binary, staging, output, lake)
        assert_lake_metadata(output, lake, meta)
        # Keep one semantic round trip (numeric fixture is enough).
        if fixture.name.endswith("Usage_kWh.parquet"):
            round_trip_snappy(binary, output, lake)
    print(f"ok {fixture.relative_to(ROOT)}")


def main() -> int:
    binary = find_binary()
    print(f"using binary {binary}")
    for fixture in FIXTURES:
        if not fixture.is_file():
            raise FileNotFoundError(fixture)
        check_fixture(binary, fixture)
    return 0


if __name__ == "__main__":
    sys.exit(main())
