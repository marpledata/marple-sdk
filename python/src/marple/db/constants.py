import pyarrow as pa

SAAS_URL = "https://db.marpledata.com/api/v1"

COL_TIME = "time"
COL_SIG = "signal"
COL_VAL = "value"
COL_VAL_TEXT = "value_text"
COL_DATASET = "dataset"
COL_VAL_IDX = 3
COL_VAL_TEXT_IDX = 4

# Realtime append schema (string signal names).
SCHEMA = pa.schema(
    [
        pa.field(COL_TIME, pa.int64()),
        pa.field(COL_SIG, pa.string()),
        pa.field(COL_VAL, pa.float64()),
        pa.field(COL_VAL_TEXT, pa.string()),
    ]
)

# Lake / Iceberg parquet schema for signal upload (int64 signal IDs).
ROW_GROUP_SIZE = 1_048_576  # ~1M rows per row group
MAX_ROWS_PER_FILE = 16 * ROW_GROUP_SIZE  # ~16M rows per file
MAX_SIGNALS_PER_ADD = 10_000

# Lake / Iceberg arrow schema during signal upload planning / validation.
LAKE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("time", pa.int64(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
        pa.field("value_text", pa.string(), nullable=True),
    ]
)

# Lake / Iceberg parquet schema for signal upload (int64 signal IDs).
LAKE_PARQUET_SCHEMA = pa.schema(
    [
        pa.field(COL_DATASET, pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1"}),
        pa.field(COL_SIG, pa.int64(), nullable=False, metadata={"PARQUET:field_id": "2"}),
        pa.field(COL_TIME, pa.int64(), nullable=False, metadata={"PARQUET:field_id": "3"}),
        pa.field(COL_VAL, pa.float64(), nullable=True, metadata={"PARQUET:field_id": "4"}),
        pa.field(COL_VAL_TEXT, pa.string(), nullable=True, metadata={"PARQUET:field_id": "5"}),
    ]
)
