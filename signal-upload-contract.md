# Signal Upload Contract

Add signals to an existing dataset by uploading parquet directly: **presign → upload → complete**.

## Endpoints

| Step | Method | Path |
|------|--------|------|
| Presign | `POST` | `/api/v1/stream/{stream_id}/dataset/{dataset_id}/signal/uploads` |
| Complete | `POST` | `/api/v1/stream/{stream_id}/dataset/{dataset_id}/signal/uploads/complete` |

## Presign

**Request body:** `{ "signals": [SignalUploadRequest, ...] }`

| Field | Description |
|-------|-------------|
| `name` | Unique within the batch. Conflicts with an existing signal unless `overwrite` is `true`. |
| `metadata` | Optional. `unit` and `description` are extracted into signal fields. `_mdb_api_upload` is reserved. |
| `files` | `[{index, rows}]` — declare each parquet file's intended row count before upload. |
| `overwrite` | Default `false`. Deletes the existing signal (Postgres, hot, Iceberg) before inserting a placeholder. |

**Response:** `[{ signal_id, files: [{index, rows, path, url, expires_in}] }]`

The client PUTs parquet bytes to each `url` (object lands at `path`). Parquet **must** embed `signal_id` in the `signal` column.

**Side effects:** validate batch → delete overwritten signals → insert placeholders (`storage_status = FROZEN_TO_COLD`, `_mdb_api_upload: true`) → return presigned URLs.

**409** when any signal fails validation:

```json
{
  "error": "signals_already_exist",
  "signals": [{"name": "speed", "status": "DUPLICATE|EXISTS|SIZE_INVALID", "message": "..."}]
}
```

| Status | When |
|--------|------|
| `DUPLICATE` | Same `name` twice in the batch |
| `EXISTS` | Name already in dataset and `overwrite` is `false` |
| `SIZE_INVALID` | Row count rules violated (`message` has detail) |

Abandoned presigns leave a `FROZEN_TO_COLD` placeholder that blocks the name until `overwrite: true` or `mh.delete_signals(...)`.

## Row count rules

Limits match internal Iceberg write sizing (`MAX_FILE_SIZE` = 16 × `ROW_GROUP_SIZE` ≈ 16M rows):

- At least one file per signal; each row count positive and ≤ 16M rows.
- At most **one** file per signal may be < 8M rows; all others must be ≥ 8M rows.
- Presign validates declared `rows`; complete re-validates actual `num_rows` from each parquet footer.

## Parquet schema

Use `PARQUET_SCHEMA` (`muhandis/core/schema.py`): `dataset` int64, `signal` int64, `time` int64, `value` float64?, `value_text` string?.

Row-group statistics must include min/max for `dataset`, `signal`, and `time`. On complete, `dataset` and `signal` min/max must equal the dataset ID and allocated signal ID.

## Complete

**Request body:** `{ "signals": [SignalUploadComplete, ...] }`

| Field | Description |
|-------|-------------|
| `id` | Signal ID from presign |
| `priority` | `default` or `high` |
| `stats` | `{sum, mean, frequency}` — merged over footer-derived stats |
| `files` | `[{path, size, footer}]` — `size` is file size in bytes (for footer fetch); `footer` is parquet footer size in bytes |

**Checks:** signal exists; `_mdb_api_upload: true`; `storage_status == FROZEN_TO_COLD`; unique IDs in batch; parquet schema/identity/row-count validation. When the dataset has `timestamp_start` and `timestamp_stop`, signal `[time_min, time_max]` must overlap that range.

**200:** `{"status": "success"}` — enqueues an Iceberg cold commit; signal becomes `COLD` asynchronously.

**400:** `{"error": "<message>"}` via `MdbException`.

## Overwrite

With `overwrite: true` for an existing name: delete the old signal, re-insert a placeholder **reusing the same signal enum ID**, return new presigned paths. Client uploads parquet embedding that same ID.

## Delete

- Python: `mh.delete_signals(stream_id, dataset_id, signal_ids)` (empty list is a no-op)
- CLI: `muhandis.py drop_signals <stream_id> <dataset_id> <signal_id...>`
