use marple_db::{Stream, StreamType};
use serde_json::json;

#[test]
fn deserializes_typed_file_stream_fields() {
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
        "plugin_args": "--unzip",
        "layer_shifts": [0, 1],
        "insight_workspace": "workspace-a"
    }))
    .expect("stream JSON");

    assert_eq!(stream.id, 3);
    assert_eq!(stream.name, "IMC");
    assert_eq!(stream.stream_type, StreamType::Files);
    assert_eq!(stream.datapool, "default");
    assert_eq!(stream.description, "Race telemetry");
    assert_eq!(stream.n_datasets, Some(4));
    assert_eq!(stream.n_datapoints, Some(9_481_422_904));
    assert_eq!(stream.cold_bytes, Some(63_331_968_278));
    assert_eq!(stream.hot_bytes, Some(357_048_320));
    assert_eq!(stream.plugin.as_deref(), Some("imc"));
    assert_eq!(stream.plugin_args.as_deref(), Some("--unzip"));
    assert_eq!(stream.extra.get("layer_shifts"), Some(&json!([0, 1])));
    assert_eq!(
        stream.extra.get("insight_workspace"),
        Some(&json!("workspace-a"))
    );
    assert!(stream.extra.get("datapool").is_none());
}

#[test]
fn deserializes_realtime_stream_without_file_plugin_fields() {
    let stream: Stream = serde_json::from_value(json!({
        "id": 4,
        "name": "Live",
        "type": "realtime",
        "datapool": "default",
        "description": null,
        "n_datasets": null,
        "n_datapoints": null,
        "cold_bytes": null,
        "hot_bytes": null
    }))
    .expect("stream JSON");

    assert_eq!(stream.stream_type, StreamType::Realtime);
    assert_eq!(stream.datapool, "default");
    assert_eq!(stream.description, "");
    assert_eq!(stream.plugin, None);
    assert_eq!(stream.plugin_args, None);
}

#[test]
fn stream_type_and_datapool_are_required() {
    let missing_type = serde_json::from_value::<Stream>(json!({
        "id": 3,
        "name": "IMC",
        "datapool": "default"
    }));
    assert!(missing_type.is_err());

    let missing_datapool = serde_json::from_value::<Stream>(json!({
        "id": 3,
        "name": "IMC",
        "type": "files"
    }));
    assert!(missing_datapool.is_err());

    let unknown_type = serde_json::from_value::<Stream>(json!({
        "id": 3,
        "name": "IMC",
        "type": "archive",
        "datapool": "default"
    }));
    assert!(unknown_type.is_err());
}
