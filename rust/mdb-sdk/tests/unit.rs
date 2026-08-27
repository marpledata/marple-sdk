use marple_db::{
    Dataset, ImportStatus, LicenseType, RealtimeTier, SAAS_URL, Settings, Signal, StorageQuota,
    StorageStatus, Stream, StreamType, UsageSeries, UserInfo, VERSION, WorkspaceLicense,
};
use serde_json::json;

#[test]
fn publishes_saas_url_and_crate_version() {
    assert_eq!(SAAS_URL, "https://db.marpledata.com/api/v1");
    assert!(!VERSION.is_empty());
    assert!(VERSION.chars().next().unwrap().is_ascii_digit());
}

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
fn stream_type_and_datapool_default_instead_of_failing() {
    let missing_type: Stream = serde_json::from_value(json!({
        "id": 3,
        "name": "IMC",
        "datapool": "default"
    }))
    .expect("missing type");
    assert_eq!(missing_type.stream_type, StreamType::Unknown);

    let missing_datapool: Stream = serde_json::from_value(json!({
        "id": 3,
        "name": "IMC",
        "type": "files"
    }))
    .expect("missing datapool");
    assert_eq!(missing_datapool.datapool, "");

    let unknown_type: Stream = serde_json::from_value(json!({
        "id": 3,
        "name": "IMC",
        "type": "archive",
        "datapool": "default"
    }))
    .expect("unknown type");
    assert_eq!(unknown_type.stream_type, StreamType::Unknown);
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

    assert_eq!(license.id, Some(7));
    assert_eq!(license.workspace, "acme");
    assert_eq!(license.payload.license_type, LicenseType::Paid);
    assert_eq!(
        license.payload.features.archive_bytes,
        Some(StorageQuota::Bytes(5_000_000_000))
    );
    assert_eq!(license.payload.features.realtime, Some(RealtimeTier::Fast));
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
                "last_active": 1_780_905_024.258647
            }
        ],
        "superuser": false,
        "can_create_workspace": false
    }))
    .expect("user info JSON");

    assert_eq!(info.email, "dev@example.com");
    assert_eq!(info.workspaces[0].name, "Acme Racing");
    assert_eq!(info.workspaces[0].last_active, Some(1_780_905_024));
    assert_eq!(info.extra.get("superuser"), Some(&json!(false)));
}

#[test]
fn user_info_accepts_postgres_epoch_floats_and_null() {
    let info: UserInfo = serde_json::from_str(
        r#"{
            "id": 1,
            "email": "dev@example.com",
            "workspaces": [
                {
                    "workspace_id": "castro_comrades",
                    "name": "Castro Comrades",
                    "role": "admin",
                    "last_active": 1780905024.258647
                },
                {
                    "workspace_id": "security_factory",
                    "name": "Security Factory",
                    "role": "admin",
                    "last_active": null
                }
            ],
            "license": {
                "workspace": "staging",
                "payload": { "type": "DEV" }
            },
            "info": { "sub": "google-oauth2|1" }
        }"#,
    )
    .expect("user info JSON with float and null last_active");

    assert_eq!(info.workspaces[0].last_active, Some(1_780_905_024));
    assert_eq!(info.workspaces[1].last_active, None);
    assert_eq!(info.current_workspace_id(), Some("staging"));
    assert_eq!(info.workspace_name("staging"), "staging");
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

    assert_eq!(series.latest(), Some(251));
}

#[test]
fn usage_series_latest_skips_non_finite() {
    let mut series: UsageSeries = serde_json::from_value(json!({ "values": [] })).expect("series");
    series.values = vec![f64::NAN, f64::INFINITY];
    assert_eq!(series.latest(), None);
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
    assert_eq!(signal.parquet_version, Some(1));
}

#[test]
fn license_defaults_missing_fields_and_unknown_enums() {
    let license: WorkspaceLicense = serde_json::from_value(json!({
        "workspace": "acme",
        "payload": {
            "type": "ENTERPRISE",
            "features": {
                "hot_bytes": -1,
                "cold_bytes": 10_000,
                "realtime": "TURBO"
            }
        }
    }))
    .expect("partial license JSON");

    assert_eq!(license.workspace_id(), Some("acme"));
    assert_eq!(license.payload.license_type, LicenseType::Unknown);
    assert_eq!(
        license.payload.features.hot_bytes,
        Some(StorageQuota::Unlimited)
    );
    assert_eq!(
        license.payload.features.cold_bytes,
        Some(StorageQuota::Bytes(10_000))
    );
    assert_eq!(license.payload.features.archive_bytes, None);
    assert_eq!(
        license.payload.features.realtime,
        Some(RealtimeTier::Unknown)
    );
    assert_eq!(license.issued_at, None);
}

#[test]
fn user_info_resolves_workspace_from_license() {
    let info: UserInfo = serde_json::from_value(json!({
        "id": 1,
        "workspaces": [
            {
                "workspace_id": "other",
                "name": "Other"
            },
            {
                "workspace_id": "acme",
                "name": "Acme Racing"
            }
        ],
        "license": {
            "workspace": "acme",
            "payload": {
                "type": "PAID",
                "features": { "hot_bytes": 1024 }
            }
        }
    }))
    .expect("user info JSON");

    assert_eq!(info.current_workspace_id(), Some("acme"));
    assert_eq!(info.workspace_name("acme"), "Acme Racing");
    assert_eq!(
        info.license
            .as_ref()
            .and_then(|license| license.payload.features.hot_bytes),
        Some(StorageQuota::Bytes(1024))
    );
}

#[test]
fn user_info_falls_back_to_sole_membership() {
    let info: UserInfo = serde_json::from_value(json!({
        "id": 1,
        "workspaces": [{ "workspace_id": "solo", "name": "Solo Lab" }]
    }))
    .expect("user info JSON");

    assert_eq!(info.current_workspace_id(), Some("solo"));
    assert_eq!(info.workspace_name("solo"), "Solo Lab");
}

#[test]
fn user_info_cannot_resolve_workspace_without_license_or_sole_membership() {
    let info: UserInfo = serde_json::from_value(json!({
        "id": 1,
        "workspaces": [
            { "workspace_id": "a", "name": "A" },
            { "workspace_id": "b", "name": "B" }
        ]
    }))
    .expect("user info JSON");

    assert_eq!(info.current_workspace_id(), None);
}

#[test]
fn usage_series_latest_skips_empty_and_clamps_negative() {
    let empty: UsageSeries = serde_json::from_value(json!({ "values": [] })).expect("empty");
    assert_eq!(empty.latest(), None);

    let negative: UsageSeries =
        serde_json::from_value(json!({ "values": [-3.2] })).expect("negative");
    assert_eq!(negative.latest(), Some(0));
}

#[test]
fn signal_defaults_unknown_storage_status() {
    let signal: Signal = serde_json::from_value(json!({
        "id": 1,
        "name": "temp",
        "storage_status": "ARCHIVING"
    }))
    .expect("signal");

    assert_eq!(signal.storage_status, StorageStatus::Unknown);
    assert_eq!(signal.parquet_version, None);
}

#[test]
fn deserializes_signal_stats_when_float_max_is_an_integer() {
    // Staging dataset 6599 stores ±f64::MAX as a 310-digit integer in stats.
    let json = r#"{
        "id": 1,
        "name": "BR_XiL_Model_cal_impl.BCsO_Cmd_B_EngFueTankLidOpen_adm_const_Value",
        "stats": {
            "max": -179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
            "min": -179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
        }
    }"#;
    let signal: Signal = serde_json::from_str(json).expect("signal with huge stats");
    let stats = signal.stats.expect("stats");
    assert!(stats.get("max").is_some());
    assert!(stats.get("min").is_some());
}

#[test]
fn import_status_exposes_api_names_and_terminal_helpers() {
    assert_eq!(ImportStatus::Finished.as_str(), "FINISHED");
    assert_eq!(ImportStatus::Finished.to_string(), "FINISHED");
    assert!(ImportStatus::Finished.is_success());
    assert!(!ImportStatus::Finished.is_failure());

    assert!(ImportStatus::Live.is_success());
    assert!(ImportStatus::Failed.is_failure());
    assert!(ImportStatus::PostprocessingFailed.is_failure());
    assert!(ImportStatus::CoolingFailed.is_failure());
    assert!(!ImportStatus::Cooling.is_success());
    assert!(!ImportStatus::Cooling.is_failure());
    assert_eq!(ImportStatus::Unknown.as_str(), "UNKNOWN");
    assert!(!ImportStatus::Unknown.is_success());
    assert!(!ImportStatus::Unknown.is_failure());
}

#[test]
fn dataset_accepts_stream_id_alias_unknown_status_and_extra_fields() {
    let dataset: Dataset = serde_json::from_value(json!({
        "id": 42,
        "stream_id": 3,
        "path": "run.csv",
        "import_status": "PREPARING",
        "unexpected": true,
        "import_id": 9,
        "parquet_version": 2
    }))
    .expect("dataset JSON");

    assert_eq!(dataset.id, 42);
    assert_eq!(dataset.datastream_id, 3);
    assert_eq!(dataset.import_status, ImportStatus::Unknown);
    assert_eq!(dataset.import_id, Some(9));
    assert_eq!(dataset.parquet_version, Some(2));
    assert_eq!(dataset.extra.get("unexpected"), Some(&json!(true)));
}

#[test]
fn dataset_id_accepts_float_json_numbers() {
    let dataset: Dataset = serde_json::from_value(json!({
        "id": 12.0,
        "datastream_id": 4.0,
        "path": "run.csv",
        "import_status": "FINISHED"
    }))
    .expect("dataset JSON with float ids");

    assert_eq!(dataset.id, 12);
    assert_eq!(dataset.datastream_id, 4);
    assert_eq!(dataset.import_status, ImportStatus::Finished);
}

#[test]
fn settings_types_known_keys_and_keeps_the_rest() {
    let settings: Settings = serde_json::from_value(json!({
        "INSIGHT_URL": "https://insight.example",
        "INSIGHT_DISTANCE_MODE_ENABLED": "true",
        "DB_PORT": 5432.0,
        "SANDBOX_JOBS_ENABLED": 1,
        "CUSTOM_FLAG": "on"
    }))
    .expect("settings JSON");

    assert_eq!(
        settings.insight_url.as_deref(),
        Some("https://insight.example")
    );
    assert_eq!(settings.insight_distance_mode_enabled, Some(true));
    assert_eq!(settings.db_port, Some(5432));
    assert_eq!(settings.sandbox_jobs_enabled, Some(true));
    assert_eq!(settings.extra.get("CUSTOM_FLAG"), Some(&json!("on")));
}
