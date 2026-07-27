use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_cast::cast;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;

pub const ROW_GROUP_SIZE: usize = 1_048_576;

#[derive(Debug, Parser)]
#[command(about = "Convert MATLAB staging Parquet into Iceberg lake upload format")]
pub struct PrepareUploadArgs {
    /// Staging Parquet with time / value / value_text columns
    #[arg(long)]
    pub input: PathBuf,

    /// Destination lake Parquet path
    #[arg(long)]
    pub output: PathBuf,

    /// Dataset identity column value
    #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
    pub dataset_id: i64,

    /// Signal identity column value
    #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
    pub signal_id: i64,

    /// Fail if the streamed row count does not match this value
    #[arg(long)]
    pub expected_rows: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct PrepareUploadResult {
    pub output: String,
    pub rows: u64,
    pub size: u64,
    pub footer: u32,
}

fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

fn lake_schema() -> Schema {
    Schema::new(vec![
        field_with_id("dataset", DataType::Int64, false, 1),
        field_with_id("signal", DataType::Int64, false, 2),
        field_with_id("time", DataType::Int64, false, 3),
        field_with_id("value", DataType::Float64, true, 4),
        field_with_id("value_text", DataType::Utf8, true, 5),
    ])
}

fn column_index(schema: &Schema, name: &str) -> Option<usize> {
    schema.index_of(name).ok()
}

fn normalize_time(array: &ArrayRef) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let casted =
        cast(array, &DataType::Int64).map_err(|e| format!("time must be int64-compatible: {e}"))?;
    if casted.null_count() > 0 {
        return Err("time must not contain nulls".into());
    }
    let times = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("time cast did not produce Int64Array")?;
    if times.values().iter().any(|&t| t < 0) {
        return Err("time must be greater than or equal to 0".into());
    }
    Ok(casted)
}

fn normalize_value(
    array: Option<&ArrayRef>,
    len: usize,
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let Some(array) = array else {
        return Ok(new_null_array(&DataType::Float64, len));
    };
    let casted = cast(array, &DataType::Float64)
        .map_err(|e| format!("value must be float64-compatible: {e}"))?;
    let values = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("value cast did not produce Float64Array")?;

    let mut builder = Float64Array::builder(len);
    for i in 0..len {
        if values.is_null(i) {
            builder.append_null();
            continue;
        }
        let v = values.value(i);
        if v.is_finite() {
            builder.append_value(v);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn normalize_value_text(
    array: Option<&ArrayRef>,
    len: usize,
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let Some(array) = array else {
        return Ok(new_null_array(&DataType::Utf8, len));
    };
    let casted = match array.data_type() {
        DataType::Utf8 => array.clone(),
        DataType::LargeUtf8 | DataType::Utf8View => cast(array, &DataType::Utf8)
            .map_err(|e| format!("value_text must be string-compatible: {e}"))?,
        other => cast(array, &DataType::Utf8)
            .map_err(|e| format!("value_text must be string-compatible (got {other}): {e}"))?,
    };
    if casted.len() != len {
        return Err("value_text length mismatch".into());
    }
    Ok(casted)
}

fn transform_batch(
    batch: &RecordBatch,
    dataset_id: i64,
    signal_id: i64,
    lake: &Arc<Schema>,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = batch.schema();
    let time_idx = column_index(&schema, "time").ok_or("data must include a 'time' column")?;
    let value_idx = column_index(&schema, "value");
    let value_text_idx = column_index(&schema, "value_text");
    if value_idx.is_none() && value_text_idx.is_none() {
        return Err("data must include 'value' and/or 'value_text'".into());
    }

    let n = batch.num_rows();
    if n == 0 {
        return Err("signal must have at least one row".into());
    }

    let time = normalize_time(batch.column(time_idx))?;
    let value = normalize_value(value_idx.map(|i| batch.column(i)), n)?;
    let value_text = normalize_value_text(value_text_idx.map(|i| batch.column(i)), n)?;

    let dataset: ArrayRef = Arc::new(Int64Array::from_value(dataset_id, n));
    let signal: ArrayRef = Arc::new(Int64Array::from_value(signal_id, n));

    Ok(RecordBatch::try_new(
        lake.clone(),
        vec![dataset, signal, time, value, value_text],
    )?)
}

fn parquet_footer_length(path: &Path) -> Result<u32, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::End(-8))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn run(args: PrepareUploadArgs) -> Result<(), Box<dyn std::error::Error>> {
    if !args.input.is_file() {
        return Err(format!("input is not a file: {}", args.input.display()).into());
    }
    if let Some(parent) = args.output.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        fs::create_dir_all(parent)?;
    }

    let input = File::open(&args.input)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?.build()?;

    let lake = Arc::new(lake_schema());
    let tmp_path = args.output.with_extension("parquet.tmp");
    // Clean any leftover temp from a previous crash.
    let _ = fs::remove_file(&tmp_path);

    let tmp_file = File::create(&tmp_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(ROW_GROUP_SIZE)
        .build();
    let mut writer = ArrowWriter::try_new(tmp_file, lake.clone(), Some(props))?;

    let mut rows: u64 = 0;
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        for batch in reader {
            let batch = batch?;
            let out = transform_batch(&batch, args.dataset_id, args.signal_id, &lake)?;
            rows += out.num_rows() as u64;
            writer.write(&out)?;
        }
        if rows == 0 {
            return Err("signal must have at least one row".into());
        }
        if let Some(expected) = args.expected_rows
            && rows != expected
        {
            return Err(format!("row count {rows} != expected {expected}").into());
        }
        writer.close()?;
        Ok(())
    })();

    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    fs::rename(&tmp_path, &args.output)?;

    let size = fs::metadata(&args.output)?.len();
    let footer = parquet_footer_length(&args.output)?;
    let payload = PrepareUploadResult {
        output: args.output.display().to_string(),
        rows,
        size,
        footer,
    };
    println!("{}", serde_json::to_string(&payload)?);
    Ok(())
}
