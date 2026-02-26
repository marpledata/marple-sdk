use std::fs;
use std::path::Path;
use std::process::ExitCode;

use arrow::array::RecordBatchReader;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

fn needs_transcode(path: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    for i in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(i);
        for j in 0..rg.num_columns() {
            if matches!(rg.column(j).compression(), Compression::ZSTD(_)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn transcode(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file = fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let schema = reader.schema();

    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

    let tmp_path = path.with_extension("parquet.tmp");
    let tmp_file = fs::File::create(&tmp_path)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(tmp_file, schema, Some(props))?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.close()?;

    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .ok_or("Usage: parquet-transcode <directory>")?;

    let dir = Path::new(&dir);
    if !dir.is_dir() {
        return Err(format!("{} is not a directory", dir.display()).into());
    }

    let mut transcoded = 0u32;
    let mut skipped = 0u32;

    for entry in walkdir::WalkDir::new(dir) {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }

        if needs_transcode(path)? {
            transcode(path)?;
            transcoded += 1;
        } else {
            skipped += 1;
        }
    }

    eprintln!("transcoded {transcoded}, skipped {skipped}");
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
