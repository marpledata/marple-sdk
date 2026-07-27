use std::fs;
use std::path::Path;
use std::process::Command;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use serde_json::Value;

fn bin() -> String {
    env!("CARGO_BIN_EXE_parquet-transcode").to_string()
}

fn get_compression(path: &Path) -> Compression {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let rg = reader.metadata().row_group(0);
    rg.column(0).compression()
}

fn read_identity(path: &Path) -> (i64, i64, usize) {
    let file = fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0usize;
    let mut dataset_id = None;
    let mut signal_id = None;
    for batch in reader {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        let dataset = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let signal = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        dataset_id.get_or_insert(dataset.value(0));
        signal_id.get_or_insert(signal.value(0));
    }
    (dataset_id.unwrap(), signal_id.unwrap(), rows)
}

/// Derive a temporary 3-column Snappy staging file from a lake fixture by
/// dropping identity columns. Uses the Rust reader only (no Python).
fn write_staging_from_lake(lake: &Path, staging: &Path) {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    let file = fs::File::open(lake).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let staging_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("value_text", DataType::Utf8, true),
    ]));
    let out = fs::File::create(staging).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(out, staging_schema.clone(), Some(props)).unwrap();

    for batch in reader {
        let batch = batch.unwrap();
        // lake columns: dataset, signal, time, value, value_text
        let cols: Vec<ArrayRef> = vec![
            batch.column(2).clone(),
            batch.column(3).clone(),
            batch.column(4).clone(),
        ];
        writer
            .write(&RecordBatch::try_new(staging_schema.clone(), cols).unwrap())
            .unwrap();
    }
    writer.close().unwrap();
}

#[test]
fn prepare_upload_numeric_smoke() {
    let tmp = tempfile::tempdir().unwrap();
    let lake = Path::new("test_data/dataset=8/signal=86/mdb_Usage_kWh.parquet");
    let staging = tmp.path().join("staging.parquet");
    let output = tmp.path().join("upload.parquet");
    write_staging_from_lake(lake, &staging);

    let (dataset_id, signal_id, rows) = read_identity(lake);

    let result = Command::new(bin())
        .args([
            "prepare-upload",
            "--input",
            staging.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--dataset-id",
            &dataset_id.to_string(),
            "--signal-id",
            &signal_id.to_string(),
            "--expected-rows",
            &rows.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let meta: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(meta["rows"], rows);
    assert_eq!(meta["size"], fs::metadata(&output).unwrap().len());
    assert!(meta["footer"].as_u64().unwrap() > 0);
    assert!(matches!(get_compression(&output), Compression::ZSTD(_)));

    let (out_dataset, out_signal, out_rows) = read_identity(&output);
    assert_eq!(out_dataset, dataset_id);
    assert_eq!(out_signal, signal_id);
    assert_eq!(out_rows, rows);
}

#[test]
fn prepare_upload_text_smoke() {
    let tmp = tempfile::tempdir().unwrap();
    let lake = Path::new("test_data/dataset=8/signal=82/mdb_Load_Type.parquet");
    let staging = tmp.path().join("staging.parquet");
    let output = tmp.path().join("upload.parquet");
    write_staging_from_lake(lake, &staging);

    let (dataset_id, signal_id, rows) = read_identity(lake);

    let result = Command::new(bin())
        .args([
            "prepare-upload",
            "--input",
            staging.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--dataset-id",
            &dataset_id.to_string(),
            "--signal-id",
            &signal_id.to_string(),
        ])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let meta: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(meta["rows"], rows);
    assert!(matches!(get_compression(&output), Compression::ZSTD(_)));
}

#[test]
fn legacy_directory_mode_still_works() {
    let tmp = tempfile::tempdir().unwrap();
    let src = Path::new("test_data/dataset=1/signal=2/mdb_m.engineRate.parquet");
    let dst = tmp.path().join("mdb_m.engineRate.parquet");
    fs::copy(src, &dst).unwrap();
    assert!(matches!(get_compression(&dst), Compression::ZSTD(_)));

    let result = Command::new(bin()).arg(tmp.path()).output().unwrap();
    assert!(
        result.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(get_compression(&dst), Compression::SNAPPY);
}
