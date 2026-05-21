use marple_db::{Dataset, Stream};
use mdb_cli::{
    dataset_queue_table_header, dataset_table_header, format_dataset_queue_table_row,
    format_dataset_table_row, format_stream_table_row, stream_table_header,
};
use serde_json::json;

#[test]
fn formats_stream_table_header() {
    assert_eq!(
        stream_table_header(),
        "Stream\tdatasets\tdatapoints\tcold\thot\tplugin\tdescription"
    );
}

#[test]
fn formats_stream_table_row() {
    let stream: Stream = serde_json::from_value(json!({
        "id": 3,
        "name": "IMC",
        "type": "files",
        "datapool": "default",
        "description": "Race telemetry",
        "n_datasets": 4,
        "n_datapoints": 9_481_422_904_u64,
        "cold_bytes": 63_331_968_278_u64,
        "hot_bytes": 357_048_320_u64,
        "plugin": "imc",
        "plugin_args": "--unzip"
    }))
    .expect("stream JSON");

    assert_eq!(
        format_stream_table_row(&stream),
        "IMC\t4\t9.5G\t59.0 GiB\t340.5 MiB\timc --unzip\tRace telemetry"
    );
}

#[test]
fn formats_dataset_table_header() {
    assert_eq!(
        dataset_table_header(),
        "ID\tpath\tstatus\tdatapoints\tsignals\tcold\thot\tbackup\tcreated_by\tmessage"
    );
}

#[test]
fn formats_dataset_table_row() {
    let dataset: Dataset = serde_json::from_value(json!({
        "id": 42,
        "datastream_id": 3,
        "datastream_version": null,
        "created_at": 1_714_000_000.0,
        "created_by": "racer@example.com",
        "import_status": "IMPORTING",
        "import_progress": 0.42,
        "import_message": "Parsing signals",
        "import_time": null,
        "path": "race-001.mf4",
        "metadata": {
            "car": "M7"
        },
        "cold_path": "cold/race-001.mf4",
        "cold_bytes": 1536,
        "hot_bytes": null,
        "backup_path": null,
        "backup_size": 4096,
        "plugin": "mdf",
        "plugin_args": "",
        "n_datapoints": 1_234_567_u64,
        "n_signals": 42,
        "timestamp_start": null,
        "timestamp_stop": null,
        "import_speed": null
    }))
    .expect("dataset JSON");

    assert_eq!(
        format_dataset_table_row(&dataset),
        "42\trace-001.mf4\tIMPORTING\t1.2M\t42\t1.5 KiB\t?\t4.0 KiB\tracer@example.com\tParsing signals"
    );
}

#[test]
fn formats_dataset_queue_table_header() {
    assert_eq!(
        dataset_queue_table_header(),
        "ID\tpath\tstatus\tprogress\tdatapoints\tsignals\tbackup\tcreated_by\tmessage"
    );
}

#[test]
fn formats_dataset_queue_table_row() {
    let dataset: Dataset = serde_json::from_value(json!({
        "id": 42,
        "datastream_id": 3,
        "datastream_version": null,
        "created_at": 1_714_000_000.0,
        "created_by": "racer@example.com",
        "import_status": "IMPORTING",
        "import_progress": 0.42,
        "import_message": "Parsing signals",
        "import_time": null,
        "path": "race-001.mf4",
        "metadata": {
            "car": "M7"
        },
        "cold_path": "cold/race-001.mf4",
        "cold_bytes": 1536,
        "hot_bytes": null,
        "backup_path": null,
        "backup_size": 4096,
        "plugin": "mdf",
        "plugin_args": "",
        "n_datapoints": 1_234_567_u64,
        "n_signals": 42,
        "timestamp_start": null,
        "timestamp_stop": null,
        "import_speed": null
    }))
    .expect("dataset JSON");

    assert_eq!(
        format_dataset_queue_table_row(&dataset),
        "42\trace-001.mf4\tIMPORTING\t42%\t1.2M\t42\t4.0 KiB\tracer@example.com\tParsing signals"
    );
}
