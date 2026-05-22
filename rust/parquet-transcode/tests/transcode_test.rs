use std::fs;
use std::path::Path;
use std::process::Command;

use parquet::basic::Compression;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

fn get_compression(path: &std::path::Path) -> Compression {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let rg = reader.metadata().row_group(0);
    rg.column(0).compression()
}

fn file_hash(path: &std::path::Path) -> Vec<u8> {
    fs::read(path).unwrap()
}

fn copy_parquet_dir(src: &std::path::Path, dst: &std::path::Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
        }
    }
}

fn copy_tree(src: &std::path::Path, dst: &std::path::Path) {
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry.unwrap();
        let rel = entry.path().strip_prefix(src).unwrap();
        let target = dst.join(rel);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target).unwrap();
        } else if entry.path().extension().and_then(|e| e.to_str()) == Some("parquet") {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn bin() -> String {
    env!("CARGO_BIN_EXE_parquet-transcode").to_string()
}

#[test]
fn transcodes_zstd_to_snappy() {
    let tmp = tempfile::tempdir().unwrap();
    let src = std::path::Path::new("test_data/dataset=1/signal=2");
    let dst = tmp.path().join("signal=2");
    copy_parquet_dir(src, &dst);

    let parquet_file = dst.join("mdb_m.engineRate.parquet");
    assert!(
        matches!(get_compression(&parquet_file), Compression::ZSTD(_)),
        "fixture should be zstd-compressed"
    );

    let output = Command::new(bin()).arg(&dst).output().unwrap();
    assert!(output.status.success(), "failed: {}", String::from_utf8_lossy(&output.stderr));

    assert_eq!(get_compression(&parquet_file), Compression::SNAPPY);
}

#[test]
fn skips_already_snappy() {
    let tmp = tempfile::tempdir().unwrap();
    let src = std::path::Path::new("test_data/dataset=1/signal=5");
    let dst = tmp.path().join("signal=5");
    copy_parquet_dir(src, &dst);

    let parquet_file = dst.join("mdb_m.speed.parquet");
    assert_eq!(
        get_compression(&parquet_file),
        Compression::SNAPPY,
        "fixture should already be snappy"
    );

    let hash_before = file_hash(&parquet_file);

    let output = Command::new(bin()).arg(&dst).output().unwrap();
    assert!(output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("transcoded 0"), "expected skip, got: {stderr}");

    let hash_after = file_hash(&parquet_file);
    assert_eq!(hash_before, hash_after, "file should not be modified");
}

#[test]
fn preserves_data_after_transcode() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow::array::RecordBatchReader;

    let tmp = tempfile::tempdir().unwrap();
    let src = std::path::Path::new("test_data/dataset=1/signal=2");
    let dst = tmp.path().join("signal=2");
    copy_parquet_dir(src, &dst);

    let parquet_file = dst.join("mdb_m.engineRate.parquet");

    // Read row count and schema before transcode
    let file_before = fs::File::open(&parquet_file).unwrap();
    let reader_before = ParquetRecordBatchReaderBuilder::try_new(file_before).unwrap().build().unwrap();
    let schema_before = reader_before.schema().clone();
    let rows_before: usize = reader_before.map(|b| b.unwrap().num_rows()).sum();

    // Transcode
    let output = Command::new(bin()).arg(&dst).output().unwrap();
    assert!(output.status.success());

    // Read row count and schema after transcode
    let file_after = fs::File::open(&parquet_file).unwrap();
    let reader_after = ParquetRecordBatchReaderBuilder::try_new(file_after).unwrap().build().unwrap();
    let schema_after = reader_after.schema().clone();
    let rows_after: usize = reader_after.map(|b| b.unwrap().num_rows()).sum();

    assert_eq!(schema_before, schema_after, "schema should be preserved");
    assert_eq!(rows_before, rows_after, "row count should be preserved");
    assert!(rows_before > 0, "test fixture should have data");
}

#[test]
fn recursive_traversal() {
    let tmp = tempfile::tempdir().unwrap();
    let src = Path::new("test_data");
    let dst = tmp.path().join("test_data");
    copy_tree(src, &dst);

    // dataset=1/signal=5 is already snappy, capture its bytes to verify it's untouched
    let snappy_file = dst.join("dataset=1/signal=5/mdb_m.speed.parquet");
    let snappy_hash = file_hash(&snappy_file);

    // zstd files that should be transcoded
    let zstd_files = [
        dst.join("dataset=1/signal=2/mdb_m.engineRate.parquet"),
        dst.join("dataset=7/signal=2/mdb_m.engineRate.parquet"),
        dst.join("dataset=7/signal=5/mdb_m.speed.parquet"),
    ];
    for f in &zstd_files {
        assert!(
            matches!(get_compression(f), Compression::ZSTD(_)),
            "{} should be zstd before transcode",
            f.display()
        );
    }

    let output = Command::new(bin()).arg(&dst).output().unwrap();
    assert!(output.status.success(), "failed: {}", String::from_utf8_lossy(&output.stderr));

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("transcoded 3"), "expected 3 transcoded, got: {stderr}");
    assert!(stderr.contains("skipped 1"), "expected 1 skipped, got: {stderr}");

    for f in &zstd_files {
        assert_eq!(
            get_compression(f),
            Compression::SNAPPY,
            "{} should be snappy after transcode",
            f.display()
        );
    }

    // Snappy file should be byte-identical
    assert_eq!(file_hash(&snappy_file), snappy_hash, "already-snappy file should be untouched");
}
