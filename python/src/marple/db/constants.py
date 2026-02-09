import pyarrow as pa

SAAS_URL = "https://db.marpledata.com/api/v1"

COL_TIME = "time"
COL_SIG = "signal"
COL_VAL = "value"
COL_VAL_TEXT = "value_text"

WRITE_SCHEMA = pa.schema(
    [
        pa.field(COL_TIME, pa.int64()),
        pa.field(COL_SIG, pa.string()),
        pa.field(COL_VAL, pa.float64()),
        pa.field(COL_VAL_TEXT, pa.string()),
    ]
)
