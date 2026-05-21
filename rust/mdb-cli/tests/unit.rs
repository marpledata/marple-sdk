use marple_db::Stream;
use mdb_cli::{format_stream_table_row, stream_table_header};
use serde_json::json;

#[test]
fn formats_stream_table_header() {
    assert_eq!(
        stream_table_header(),
        "Stream\tdatasets\tdatapoints\tcold\thot\tplugin"
    );
}

#[test]
fn formats_stream_table_row() {
    let stream: Stream = serde_json::from_value(json!({
        "id": 3,
        "name": "IMC",
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
        "IMC\t4\t9.5G\t59.0 GiB\t340.5 MiB\timc --unzip"
    );
}
