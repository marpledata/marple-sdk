use marple_db::{
    LicenseType, Signal, StorageStatus, Stream, StreamType, UsageSeries, UserInfo,
    WorkspaceLicense,
};
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

#[test]
fn deserializes_workspace_license() {
    let license: WorkspaceLicense = serde_json::from_value(json!({
        "id": 7,
        "version": "3",
        "issued_at": 1_710_000_000,
        "cached_at": 1_710_000_100,
        "workspace": "acme",
        "payload": {
            "type": "PAID",
            "product": "MarpleDB",
            "deployment": "saas",
            "workspace": "acme",
            "expiry_date": 1_730_000_000,
            "features": {
                "hot_bytes": 1_000_000_000_u64,
                "cold_bytes": 10_000_000_000_u64,
                "archive_bytes": 5_000_000_000_u64,
                "ingestion_workers": 2,
                "realtime": "FAST"
            }
        }
    }))
    .expect("license JSON");

    assert_eq!(license.id, 7);
    assert_eq!(license.workspace, "acme");
    assert_eq!(license.payload.license_type, LicenseType::Paid);
    assert_eq!(license.payload.features.archive_bytes, 5_000_000_000);
}

#[test]
fn deserializes_user_info_workspaces() {
    let info: UserInfo = serde_json::from_value(json!({
        "id": 1,
        "email": "dev@example.com",
        "workspaces": [
            {
                "workspace_id": "acme",
                "name": "Acme Racing",
                "role": "editor",
                "last_active": 1_710_000_000
            }
        ],
        "superuser": false,
        "can_create_workspace": false
    }))
    .expect("user info JSON");

    assert_eq!(info.email, "dev@example.com");
    assert_eq!(info.workspaces[0].name, "Acme Racing");
    assert_eq!(info.extra.get("superuser"), Some(&json!(false)));
}

#[test]
fn usage_series_latest_is_last_sample() {
    let series: UsageSeries = serde_json::from_value(json!({
        "timestamps": [1.0, 2.0],
        "values": [100.0, 250.5],
        "integrated": true,
        "unit": "bytes"
    }))
    .expect("usage series JSON");

    assert_eq!(series.latest(), Some(250));
}

#[test]
fn deserializes_signal_and_ignores_unknown_fields() {
    let signal: Signal = serde_json::from_value(json!({
        "id": 99,
        "name": "speed",
        "unit": "km/h",
        "description": "Vehicle speed",
        "metadata": { "axis": "x" },
        "storage_status": "COLD",
        "count": 1200,
        "time_min": 0,
        "time_max": 1_000_000_000,
        "parquet_version": 1,
        "unexpected": true
    }))
    .expect("signal");
    assert_eq!(signal.id, 99);
    assert_eq!(signal.name, "speed");
    assert_eq!(signal.unit.as_deref(), Some("km/h"));
    assert_eq!(signal.storage_status, StorageStatus::Cold);
    assert_eq!(signal.count, Some(1200));
}
