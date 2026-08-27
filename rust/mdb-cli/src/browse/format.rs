use marple_db::{Dataset, LicenseType, Signal, StorageQuota, StorageStatus, Stream, StreamType};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Cell;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

const META_REFERENCE: &str = "_mdb_reference_signal";
const META_UPLOADED_BY: &str = "_mdb_uploaded_by";
const META_UPLOADED_VIA: &str = "_mdb_uploaded_via";
const META_UPLOADED_AT: &str = "_mdb_uploaded_at";
const META_RESERVED_PREFIX: &str = "_mdb_";
const MISSING: &str = "—";

pub(super) fn body_style() -> Style {
    Style::default().fg(Color::White)
}

pub(super) fn kv(key: &str, value: impl std::fmt::Display) -> Line<'static> {
    kv_styled(key, value, body_style())
}

pub(super) fn kv_styled(key: &str, value: impl std::fmt::Display, style: Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<18}"), Style::default().fg(Color::DarkGray)),
        Span::styled(value.to_string(), style),
    ])
}

pub(super) fn stream_info(
    stream: Option<&Stream>,
    expanded: bool,
    import: Option<(usize, usize)>,
) -> (String, Vec<Line<'static>>) {
    let Some(stream) = stream else {
        return ("info".to_string(), vec![Line::from("no stream selected")]);
    };
    let mut lines = vec![
        kv("id", stream.id),
        kv("type", stream_kind(stream)),
        kv("datasets", opt_count(stream.n_datasets)),
    ];
    if let Some((finished, live)) = import {
        lines.push(kv("finished", finished));
        lines.push(kv("live", live));
    }
    lines.extend([
        kv("plugin", opt_text(stream.plugin.as_deref())),
        kv("args", opt_text(stream.plugin_args.as_deref())),
        kv("cold", opt_bytes(stream.cold_bytes)),
        kv("hot", opt_bytes(stream.hot_bytes)),
    ]);
    if expanded {
        lines.push(kv("points", compact_count(stream.n_datapoints)));
        if !stream.description.is_empty() {
            lines.push(kv("desc", stream.description.clone()));
        }
        lines.push(kv("pool", stream.datapool.clone()));
    }
    (format!("stream  {}", stream.name), lines)
}

pub(super) fn dataset_info(
    dataset: Option<&Dataset>,
    expanded: bool,
) -> (String, Vec<Line<'static>>) {
    let Some(dataset) = dataset else {
        return ("info".to_string(), vec![Line::from("no dataset selected")]);
    };
    let mut lines = vec![
        kv("id", dataset.id),
        kv("status", crate::format_import_status(dataset.import_status)),
        kv("signals", opt_count(dataset.n_signals)),
        kv("points", compact_count(dataset.n_datapoints)),
        kv("plugin", opt_text(dataset.plugin.as_deref())),
        kv("args", opt_text(dataset.plugin_args.as_deref())),
        kv("cold", opt_bytes(dataset.cold_bytes)),
        kv("hot", opt_bytes(dataset.hot_bytes)),
        kv("archive", opt_bytes(dataset.backup_size)),
    ];
    if expanded {
        lines.push(kv("progress", opt_percent(dataset.import_progress)));
        lines.push(kv("import time", opt_seconds(dataset.import_time)));
        lines.push(kv("import speed", opt_speed(dataset.import_speed)));
        lines.push(kv(
            "message",
            dataset
                .import_message
                .as_deref()
                .filter(|message| !message.is_empty())
                .unwrap_or(MISSING),
        ));
        if let Some(created_by) = &dataset.created_by {
            lines.push(kv("created by", created_by.clone()));
        }
        if dataset.created_at > 0.0 {
            lines.push(kv(
                "created at",
                crate::format_epoch_utc(dataset.created_at),
            ));
        }
    }
    push_metadata(&mut lines, &dataset.metadata, false);
    (format!("dataset  {}", dataset.path), lines)
}

pub(super) fn signal_info(signal: Option<&Signal>) -> (String, Vec<Line<'static>>) {
    let Some(signal) = signal else {
        return ("info".to_string(), vec![Line::from("no signal selected")]);
    };
    let mut lines = vec![
        kv("id", signal.id),
        kv("type", signal_kind(signal)),
        kv("source", signal_source(signal)),
        kv("unit", opt_text(signal.unit.as_deref())),
        kv("points", compact_count(signal.count)),
        kv("cold", opt_bytes(signal.cold_bytes)),
        kv("hot", opt_bytes(signal.hot_bytes)),
        kv("storage", storage_status(signal.storage_status)),
    ];
    if let Some(description) = &signal.description
        && !description.is_empty()
    {
        lines.push(kv("desc", description.clone()));
    }
    if let Some(reference) = meta_str(&signal.metadata, META_REFERENCE) {
        lines.push(kv("alias of", reference));
    }
    if let Some(uploaded_by) = meta_str(&signal.metadata, META_UPLOADED_BY) {
        lines.push(kv("uploaded", uploaded_by));
    }
    if let Some(uploaded_via) = meta_str(&signal.metadata, META_UPLOADED_VIA) {
        lines.push(kv("via", uploaded_via));
    }
    if let Some(uploaded_at) = meta_str(&signal.metadata, META_UPLOADED_AT) {
        lines.push(kv("at", uploaded_at));
    }
    push_metadata(&mut lines, &signal.metadata, true);
    (format!("signal  {}", signal.name), lines)
}

pub(super) fn stream_kind(stream: &Stream) -> &'static str {
    match stream.stream_type {
        StreamType::Files => "files",
        StreamType::Realtime => "realtime",
        _ => "unknown",
    }
}

pub(super) fn signal_kind(signal: &Signal) -> &'static str {
    let numeric = signal.count_value.unwrap_or(0);
    let text = signal.count_text.unwrap_or(0);
    match (numeric > 0, text > 0) {
        (true, false) => "[#]",
        (false, true) => "[T]",
        (true, true) => "[=]",
        (false, false) => "[ ]",
    }
}

pub(super) fn signal_source(signal: &Signal) -> &'static str {
    if signal.metadata.contains_key(META_REFERENCE) {
        "Alias"
    } else if signal.metadata.contains_key(META_UPLOADED_AT) {
        "API"
    } else {
        "Import"
    }
}

pub(super) fn opt_text(value: Option<&str>) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or(MISSING)
        .to_string()
}

pub(super) fn clip_args(value: Option<&str>, width: usize) -> String {
    match value.filter(|value| !value.is_empty()) {
        Some(value) => ellipsis(value, width),
        None => MISSING.to_string(),
    }
}

pub(super) fn ellipsis(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if text.chars().count() <= width {
        return text.to_string();
    }
    if width == 1 {
        return "…".to_string();
    }
    let mut clipped: String = text.chars().take(width - 1).collect();
    clipped.push('…');
    clipped
}

pub(super) fn count_cell(count: Option<u64>, unit: &str) -> Cell<'static> {
    Cell::from(Line::from(count_label(count, unit)).right_aligned())
}

pub(super) fn compact_count(value: Option<u64>) -> String {
    crate::format_compact_count_with(value, MISSING, "G")
}

pub(super) fn opt_count(value: Option<u64>) -> String {
    crate::format_count_with(value, MISSING)
}

pub(super) fn opt_bytes(value: Option<u64>) -> String {
    crate::format_bytes_with(value, MISSING)
}

pub(super) fn format_usage(used: Option<u64>, quota: Option<StorageQuota>) -> String {
    let used = opt_bytes(used);
    match quota {
        Some(StorageQuota::Unlimited) => format!("{used} / unlimited"),
        Some(StorageQuota::Bytes(limit)) => format!("{used} / {}", opt_bytes(Some(limit))),
        None => used,
    }
}

pub(super) fn usage_ratio(used: Option<u64>, quota: Option<StorageQuota>) -> Option<f64> {
    match (used, quota) {
        (Some(used), Some(StorageQuota::Bytes(limit))) if limit > 0 => {
            Some(used as f64 / limit as f64)
        }
        _ => None,
    }
}

pub(super) fn bar_color(ratio: f64) -> Color {
    if ratio >= 0.9 {
        Color::Red
    } else if ratio >= 0.7 {
        Color::Yellow
    } else {
        Color::Cyan
    }
}

pub(super) fn usage_bar(
    used: Option<u64>,
    quota: Option<StorageQuota>,
    width: u16,
) -> Line<'static> {
    let width = usize::from(width).max(4);
    let (filled, color) = match usage_ratio(used, quota) {
        Some(ratio) => (
            ((ratio * width as f64).round() as usize).min(width),
            bar_color(ratio),
        ),
        None => (0, Color::DarkGray),
    };
    Line::from(Span::styled(
        format!("{}{}", "█".repeat(filled), "░".repeat(width - filled)),
        Style::default().fg(color),
    ))
}

pub(super) fn license_color(license_type: LicenseType) -> Color {
    match license_type {
        LicenseType::Paid => Color::Green,
        LicenseType::Sponsorship => Color::Yellow,
        LicenseType::Poc => Color::Magenta,
        LicenseType::Dev => Color::LightMagenta,
        _ => Color::Gray,
    }
}

pub(super) fn license_type(license_type: LicenseType) -> &'static str {
    match license_type {
        LicenseType::Dev => "DEV",
        LicenseType::Free => "FREE",
        LicenseType::Trial => "TRIAL",
        LicenseType::Paid => "PAID",
        LicenseType::Poc => "POC",
        LicenseType::Sponsorship => "SPONSORSHIP",
        _ => "UNKNOWN",
    }
}

pub(super) fn storage_status(status: StorageStatus) -> &'static str {
    match status {
        StorageStatus::FrozenToCold => "FROZEN_TO_COLD",
        StorageStatus::Cold => "COLD",
        StorageStatus::ColdToHot => "COLD_TO_HOT",
        StorageStatus::Hot => "HOT",
        _ => "UNKNOWN",
    }
}

pub(super) fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

pub(super) fn format_expiry(expiry: Option<i64>, now: i64) -> (String, Color) {
    let Some(secs) = expiry else {
        return ("no expiry".to_string(), Color::Gray);
    };
    let date = crate::format_epoch_utc(secs as f64)
        .chars()
        .take(10)
        .collect::<String>();
    if secs < now {
        (format!("expired {date}"), Color::Red)
    } else if secs - now < 30 * 86_400 {
        (format!("expires {date}"), Color::Yellow)
    } else {
        (format!("expires {date}"), Color::Gray)
    }
}

pub(super) fn host_from_url(url: &str) -> &str {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    rest.split('/')
        .next()
        .filter(|host| !host.is_empty())
        .unwrap_or(rest)
}

pub(super) fn sum_bytes(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut total = 0;
    let mut any = false;
    for value in values.flatten() {
        total += value;
        any = true;
    }
    any.then_some(total)
}

fn opt_percent(value: Option<f64>) -> String {
    crate::format_progress_with(value, MISSING)
}

fn opt_seconds(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}s"))
        .unwrap_or_else(|| MISSING.to_string())
}

fn opt_speed(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| MISSING.to_string())
}

fn count_label(count: Option<u64>, unit: &str) -> String {
    match count {
        Some(count) => format!("{count} {unit}"),
        None => format!("? {unit}"),
    }
}

fn meta_str(metadata: &marple_db::Metadata, key: &str) -> Option<String> {
    metadata
        .get(key)
        .map(format_meta_value)
        .filter(|value| !value.is_empty())
}

fn format_meta_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn push_metadata(
    lines: &mut Vec<Line<'static>>,
    metadata: &marple_db::Metadata,
    skip_reserved: bool,
) {
    let mut entries: Vec<_> = metadata
        .iter()
        .filter(|(key, _)| !skip_reserved || !key.starts_with(META_RESERVED_PREFIX))
        .collect();
    if entries.is_empty() {
        return;
    }
    entries.sort_by_key(|(left, _)| *left);
    for (key, value) in entries {
        lines.push(kv(key, format_meta_value(value)));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bar_color, ellipsis, format_expiry, format_usage, host_from_url, license_color,
        license_type, storage_status, usage_bar, usage_ratio,
    };
    use marple_db::{LicenseType, StorageQuota, StorageStatus};
    use ratatui::style::Color;

    #[test]
    fn ellipsis_keeps_short_text() {
        assert_eq!(ellipsis("MB Racing", 20), "MB Racing");
    }

    #[test]
    fn ellipsis_cuts_long_names() {
        assert_eq!(
            ellipsis("Flight Testing - Carlitos Airlines", 12),
            "Flight Test…"
        );
        assert_eq!(ellipsis("ab", 1), "…");
        assert_eq!(ellipsis("hello", 0), "");
    }

    #[test]
    fn usage_shows_quota_when_present() {
        assert_eq!(format_usage(Some(1024), None), "1.0 KiB");
        assert_eq!(
            format_usage(Some(1024), Some(StorageQuota::Unlimited)),
            "1.0 KiB / unlimited"
        );
        assert_eq!(
            format_usage(Some(1024), Some(StorageQuota::Bytes(2048))),
            "1.0 KiB / 2.0 KiB"
        );
    }

    #[test]
    fn labels_match_api_enum_names() {
        assert_eq!(license_type(LicenseType::Paid), "PAID");
        assert_eq!(
            storage_status(StorageStatus::FrozenToCold),
            "FROZEN_TO_COLD"
        );
        assert_eq!(license_type(LicenseType::Unknown), "UNKNOWN");
        assert_eq!(storage_status(StorageStatus::Unknown), "UNKNOWN");
    }

    #[test]
    fn license_colors_match_muhandis_badges() {
        assert_eq!(license_color(LicenseType::Paid), Color::Green);
        assert_eq!(license_color(LicenseType::Sponsorship), Color::Yellow);
        assert_eq!(license_color(LicenseType::Poc), Color::Magenta);
        assert_eq!(license_color(LicenseType::Dev), Color::LightMagenta);
        assert_eq!(license_color(LicenseType::Trial), Color::Gray);
        assert_eq!(license_color(LicenseType::Free), Color::Gray);
    }

    #[test]
    fn usage_ratio_and_bar_color() {
        assert_eq!(
            usage_ratio(Some(50), Some(StorageQuota::Bytes(100))),
            Some(0.5)
        );
        assert_eq!(usage_ratio(Some(10), Some(StorageQuota::Unlimited)), None);
        assert_eq!(bar_color(0.5), Color::Cyan);
        assert_eq!(bar_color(0.7), Color::Yellow);
        assert_eq!(bar_color(0.9), Color::Red);
        assert_eq!(
            usage_bar(Some(50), Some(StorageQuota::Bytes(100)), 20).width(),
            20
        );
    }

    #[test]
    fn host_and_env_are_compact() {
        assert_eq!(
            host_from_url("https://db.marpledata.com/api/v1"),
            "db.marpledata.com"
        );
        assert_eq!(
            host_from_url("http://localhost:8080/api/v1"),
            "localhost:8080"
        );
    }

    #[test]
    fn expiry_warns_when_close_or_past() {
        let now = 1_800_000_000;
        assert_eq!(
            format_expiry(Some(now + 60 * 86_400), now),
            ("expires 2027-03-16".to_string(), Color::Gray)
        );
        assert_eq!(
            format_expiry(Some(now + 10 * 86_400), now),
            ("expires 2027-01-25".to_string(), Color::Yellow)
        );
        assert_eq!(
            format_expiry(Some(now - 86_400), now),
            ("expired 2027-01-14".to_string(), Color::Red)
        );
        assert_eq!(
            format_expiry(None, now),
            ("no expiry".to_string(), Color::Gray)
        );
    }
}
