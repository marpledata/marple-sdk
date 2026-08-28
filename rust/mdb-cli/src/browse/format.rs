use super::style::body_style;
use crate::table::row_matches;
use marple_db::{
    Dataset, ImportStatus, LicenseType, Signal, StorageQuota, StorageStatus, Stream, StreamType,
};
use ratatui::layout::Constraint;
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

pub(super) fn kv(key: &str, value: impl std::fmt::Display) -> Line<'static> {
    kv_styled(key, value, body_style())
}

pub(super) fn kv_styled(key: &str, value: impl std::fmt::Display, style: Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<18}"), body_style()),
        Span::styled(value.to_string(), style),
    ])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ImportMix {
    pub finished: usize,
    pub live: usize,
    pub failed: usize,
    pub total: usize,
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

pub(super) fn stream_card(
    stream: Option<&Stream>,
    import: Option<ImportMix>,
    width: usize,
) -> (String, Vec<Line<'static>>) {
    let Some(stream) = stream else {
        return (
            "stream".to_string(),
            vec![card_line("no stream selected", width)],
        );
    };
    let title = ellipsis(&format!("stream  {}", stream.name), width);
    let mut lines = vec![card_line(stream_plugin_line(stream, width), width)];
    if let Some(mix) = import_mix_line(import) {
        lines.push(card_line(mix, width));
    }
    lines.push(card_line(
        format!("cold {}", opt_bytes(stream.cold_bytes)),
        width,
    ));
    lines.push(card_line(
        format!("hot {}", opt_bytes(stream.hot_bytes)),
        width,
    ));
    lines.truncate(4);
    (title, lines)
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
        kv("status", dataset.import_status.as_str()),
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

pub(super) fn dataset_card(
    dataset: Option<&Dataset>,
    width: usize,
) -> (String, Vec<Line<'static>>) {
    let Some(dataset) = dataset else {
        return (
            "dataset".to_string(),
            vec![card_line("no dataset selected", width)],
        );
    };
    let title = ellipsis(&format!("dataset  {}", dataset.path), width);
    let status = dataset.import_status.as_str();
    let status_line = match dataset
        .import_progress
        .filter(|_| shows_progress(dataset.import_status))
    {
        Some(_) => format!(
            "{status}  {}",
            crate::format_progress_with(dataset.import_progress, MISSING)
        ),
        None => status.to_string(),
    };
    let mut lines = vec![card_line(status_line, width)];
    if !dataset.import_status.is_success() {
        let message = dataset
            .import_message
            .as_deref()
            .filter(|message| !message.is_empty())
            .unwrap_or(MISSING);
        lines.push(card_line(message, width));
        lines.push(card_line(dataset_points_archive(dataset), width));
        lines.push(card_line(
            format!(
                "cold {}  hot {}",
                opt_bytes(dataset.cold_bytes),
                opt_bytes(dataset.hot_bytes)
            ),
            width,
        ));
    } else {
        lines.push(card_line(dataset_points_archive(dataset), width));
        lines.push(card_line(
            format!("cold {}", opt_bytes(dataset.cold_bytes)),
            width,
        ));
        lines.push(card_line(
            format!("hot {}", opt_bytes(dataset.hot_bytes)),
            width,
        ));
    }
    lines.truncate(4);
    (title, lines)
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

fn stream_kind(stream: &Stream) -> &'static str {
    match stream.stream_type {
        StreamType::Files => "files",
        StreamType::Realtime => "realtime",
        _ => "unknown",
    }
}

fn signal_kind(signal: &Signal) -> &'static str {
    let numeric = signal.count_value.unwrap_or(0);
    let text = signal.count_text.unwrap_or(0);
    match (numeric > 0, text > 0) {
        (true, false) => "[#]",
        (false, true) => "[T]",
        (true, true) => "[=]",
        (false, false) => "[ ]",
    }
}

fn signal_source(signal: &Signal) -> &'static str {
    if signal.metadata.contains_key(META_REFERENCE) {
        "Alias"
    } else if signal.metadata.contains_key(META_UPLOADED_AT) {
        "API"
    } else {
        "Import"
    }
}

fn opt_text(value: Option<&str>) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or(MISSING)
        .to_string()
}

fn clip_args(value: Option<&str>, width: usize) -> String {
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

fn compact_count(value: Option<u64>) -> String {
    crate::format_compact_count_with(value, MISSING, "G")
}

fn opt_count(value: Option<u64>) -> String {
    crate::format_count_with(value, MISSING)
}

fn opt_bytes(value: Option<u64>) -> String {
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

fn usage_ratio(used: Option<u64>, quota: Option<StorageQuota>) -> Option<f64> {
    match (used, quota) {
        (Some(used), Some(StorageQuota::Bytes(limit))) if limit > 0 => {
            Some(used as f64 / limit as f64)
        }
        _ => None,
    }
}

fn bar_color(ratio: f64) -> Color {
    if ratio >= 0.9 {
        Color::Red
    } else if ratio >= 0.7 {
        Color::Yellow
    } else {
        Color::Blue
    }
}

pub(super) fn usage_bar(
    used: Option<u64>,
    quota: Option<StorageQuota>,
    width: u16,
) -> Line<'static> {
    let width = usize::from(width).max(4);
    let (filled, style) = match usage_ratio(used, quota) {
        Some(ratio) => (
            ((ratio * width as f64).round() as usize).min(width),
            Style::default().fg(bar_color(ratio)),
        ),
        None => (0, body_style()),
    };
    Line::from(Span::styled(
        format!("{}{}", "█".repeat(filled), "░".repeat(width - filled)),
        style,
    ))
}

pub(super) fn progress_cell(value: Option<f64>, in_flight: bool) -> Cell<'static> {
    if !in_flight {
        return Cell::from(MISSING);
    }
    Cell::from(ratio_bar(normalize_progress(value), 8))
}

pub(super) fn shows_progress(status: ImportStatus) -> bool {
    matches!(
        status,
        ImportStatus::Uploading | ImportStatus::Importing | ImportStatus::Postprocessing
    )
}

pub(super) struct Col<T: 'static> {
    pub header: &'static str,
    pub width: Constraint,
    pub field: fn(&T) -> String,
}

impl<T> Col<T> {
    const fn new(header: &'static str, width: Constraint, field: fn(&T) -> String) -> Self {
        Self {
            header,
            width,
            field,
        }
    }
}

pub(super) const STREAM_COLS: &[Col<Stream>] = &[
    Col::new("id", Constraint::Length(8), |stream| stream.id.to_string()),
    Col::new("type", Constraint::Length(9), |stream| {
        stream_kind(stream).to_string()
    }),
    Col::new("name", Constraint::Min(16), |stream| stream.name.clone()),
    Col::new("plugin", Constraint::Length(16), |stream| {
        opt_text(stream.plugin.as_deref())
    }),
    Col::new("args", Constraint::Length(40), |stream| {
        clip_args(stream.plugin_args.as_deref(), 40)
    }),
    Col::new("datasets", Constraint::Length(9), |stream| {
        opt_count(stream.n_datasets)
    }),
    Col::new("cold", Constraint::Length(12), |stream| {
        opt_bytes(stream.cold_bytes)
    }),
    Col::new("hot", Constraint::Length(12), |stream| {
        opt_bytes(stream.hot_bytes)
    }),
];

pub(super) const DATASET_COLS: &[Col<Dataset>] = &[
    Col::new("id", Constraint::Length(8), |dataset| {
        dataset.id.to_string()
    }),
    Col::new("path", Constraint::Min(16), |dataset| dataset.path.clone()),
    Col::new("signals", Constraint::Length(8), |dataset| {
        opt_count(dataset.n_signals)
    }),
    Col::new("datapoints", Constraint::Length(10), |dataset| {
        compact_count(dataset.n_datapoints)
    }),
    Col::new("archive", Constraint::Length(12), |dataset| {
        opt_bytes(dataset.backup_size)
    }),
    Col::new("cold", Constraint::Length(12), |dataset| {
        opt_bytes(dataset.cold_bytes)
    }),
    Col::new("hot", Constraint::Length(12), |dataset| {
        opt_bytes(dataset.hot_bytes)
    }),
];

pub(super) const DATASET_EXTRA: [(&str, Constraint); 2] = [
    ("status", Constraint::Length(16)),
    ("progress", Constraint::Length(10)),
];

pub(super) const SIGNAL_COLS: &[Col<Signal>] = &[
    Col::new("type", Constraint::Length(5), |signal| {
        signal_kind(signal).to_string()
    }),
    Col::new("id", Constraint::Length(8), |signal| signal.id.to_string()),
    Col::new("name", Constraint::Min(16), |signal| signal.name.clone()),
    Col::new("unit", Constraint::Length(8), |signal| {
        signal.unit.clone().unwrap_or_default()
    }),
    Col::new("source", Constraint::Length(8), |signal| {
        signal_source(signal).to_string()
    }),
    Col::new("datapoints", Constraint::Length(10), |signal| {
        compact_count(signal.count)
    }),
    Col::new("cold", Constraint::Length(12), |signal| {
        opt_bytes(signal.cold_bytes)
    }),
    Col::new("hot", Constraint::Length(12), |signal| {
        opt_bytes(signal.hot_bytes)
    }),
];

pub(super) fn col_headers<'a, T>(
    cols: &'a [Col<T>],
    extra: &'a [(&'a str, Constraint)],
) -> Vec<&'a str> {
    cols.iter()
        .map(|col| col.header)
        .chain(extra.iter().map(|(header, _)| *header))
        .collect()
}

pub(super) fn col_widths<T>(cols: &[Col<T>], extra: &[(&str, Constraint)]) -> Vec<Constraint> {
    cols.iter()
        .map(|col| col.width)
        .chain(extra.iter().map(|(_, width)| *width))
        .collect()
}

pub(super) fn col_cells<T>(cols: &[Col<T>], row: &T) -> Vec<Cell<'static>> {
    cols.iter()
        .map(|col| Cell::from((col.field)(row)))
        .collect()
}

pub(super) fn stream_matches(stream: &Stream, query: &str) -> bool {
    col_matches(STREAM_COLS, &[stream.description.as_str()], stream, query)
}

pub(super) fn dataset_matches(dataset: &Dataset, query: &str) -> bool {
    col_matches(
        DATASET_COLS,
        &[
            dataset.import_status.as_str(),
            dataset.import_message.as_deref().unwrap_or(""),
        ],
        dataset,
        query,
    )
}

pub(super) fn signal_matches(signal: &Signal, query: &str) -> bool {
    col_matches(
        SIGNAL_COLS,
        &[signal.description.as_deref().unwrap_or("")],
        signal,
        query,
    )
}

fn col_matches<T>(cols: &[Col<T>], extras: &[&str], row: &T, query: &str) -> bool {
    let fields: Vec<String> = cols
        .iter()
        .map(|col| (col.field)(row))
        .chain(extras.iter().map(|extra| extra.to_string()))
        .collect();
    row_matches(query, fields)
}

fn normalize_progress(value: Option<f64>) -> Option<f64> {
    value.map(|value| {
        let ratio = if value > 1.0 { value / 100.0 } else { value };
        ratio.clamp(0.0, 1.0)
    })
}

fn ratio_bar(ratio: Option<f64>, width: usize) -> String {
    let width = width.max(1);
    let filled = match ratio {
        Some(ratio) => ((ratio * width as f64).round() as usize).min(width),
        None => 0,
    };
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

pub(super) fn license_color(license_type: LicenseType) -> Color {
    match license_type {
        LicenseType::Paid => Color::Green,
        LicenseType::Sponsorship => Color::Yellow,
        LicenseType::Poc => Color::Magenta,
        LicenseType::Dev => Color::LightMagenta,
        _ => Color::Reset,
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

fn storage_status(status: StorageStatus) -> &'static str {
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
        return ("no expiry".to_string(), Color::Reset);
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
        (format!("expires {date}"), Color::Reset)
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

fn card_line(text: impl AsRef<str>, width: usize) -> Line<'static> {
    Line::from(Span::styled(ellipsis(text.as_ref(), width), body_style()))
}

fn stream_plugin_line(stream: &Stream, width: usize) -> String {
    match (
        stream.plugin.as_deref().filter(|plugin| !plugin.is_empty()),
        stream
            .plugin_args
            .as_deref()
            .filter(|args| !args.is_empty()),
    ) {
        (Some(plugin), Some(args)) => ellipsis(&format!("{plugin} {args}"), width),
        (Some(plugin), None) => ellipsis(plugin, width),
        _ => stream_kind(stream).to_string(),
    }
}

fn import_mix_line(import: Option<ImportMix>) -> Option<String> {
    let mix = import?;
    if mix.total > 0 && mix.failed == 0 && mix.live == 0 && mix.finished == mix.total {
        return None;
    }
    let mut parts = Vec::new();
    if mix.finished > 0 {
        parts.push(format!("{} finished", mix.finished));
    }
    if mix.live > 0 {
        parts.push(format!("{} live", mix.live));
    }
    if mix.failed > 0 {
        parts.push(format!("{} failed", mix.failed));
    }
    (!parts.is_empty()).then(|| parts.join("  "))
}

fn dataset_points_archive(dataset: &Dataset) -> String {
    format!(
        "{} pts  {} archive",
        compact_count(dataset.n_datapoints),
        opt_bytes(dataset.backup_size)
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ImportMix, bar_color, dataset_card, ellipsis, format_expiry, format_usage, host_from_url,
        license_color, license_type, progress_cell, shows_progress, storage_status, stream_card,
        stream_matches, usage_bar, usage_ratio,
    };
    use marple_db::{Dataset, ImportStatus, LicenseType, StorageQuota, StorageStatus, Stream};
    use ratatui::style::Color;
    use ratatui::widgets::Cell;
    use serde_json::json;

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
    fn stream_card_omits_id_name_and_hides_finished_mix() {
        let stream: Stream = serde_json::from_value(json!({
            "id": 3,
            "name": "IMC",
            "type": "files",
            "datapool": "default",
            "plugin": "imc",
            "plugin_args": "--unzip",
            "cold_bytes": 1024,
            "hot_bytes": 2048
        }))
        .expect("stream JSON");
        let (title, lines) = stream_card(
            Some(&stream),
            Some(ImportMix {
                finished: 4,
                live: 0,
                failed: 0,
                total: 4,
            }),
            40,
        );
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(title, "stream  IMC");
        assert_eq!(texts, vec!["imc --unzip", "cold 1.0 KiB", "hot 2.0 KiB"]);
    }

    #[test]
    fn stream_card_clips_args_and_shows_failed_mix() {
        let stream: Stream = serde_json::from_value(json!({
            "id": 3,
            "name": "IMC",
            "type": "files",
            "datapool": "default",
            "plugin": "imc",
            "plugin_args": "--unzip --time-factor 1 --extra"
        }))
        .expect("stream JSON");
        let (title, lines) = stream_card(
            Some(&stream),
            Some(ImportMix {
                finished: 2,
                live: 0,
                failed: 1,
                total: 4,
            }),
            18,
        );
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(title, "stream  IMC");
        assert_eq!(
            texts[0],
            ellipsis("imc --unzip --time-factor 1 --extra", 18)
        );
        assert_eq!(texts[1], ellipsis("2 finished  1 failed", 18));
    }

    #[test]
    fn stream_search_uses_table_columns_and_description() {
        let stream: Stream = serde_json::from_value(json!({
            "id": 3,
            "name": "IMC",
            "type": "files",
            "datapool": "default",
            "plugin": "imc",
            "description": "mallorca traffic"
        }))
        .expect("stream JSON");
        assert!(stream_matches(&stream, "imc"));
        assert!(stream_matches(&stream, "mallorca"));
        assert!(stream_matches(&stream, "files"));
        assert!(!stream_matches(&stream, "xyz"));
    }

    #[test]
    fn stream_card_uses_kind_when_plugin_missing() {
        let stream: Stream = serde_json::from_value(json!({
            "id": 1,
            "name": "Live",
            "type": "realtime",
            "datapool": "default"
        }))
        .expect("stream JSON");
        let (_, lines) = stream_card(Some(&stream), None, 40);
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(texts[0], "realtime");
    }

    #[test]
    fn dataset_card_shows_status_sizes_not_path() {
        let dataset: Dataset = serde_json::from_value(json!({
            "id": 42,
            "datastream_id": 3,
            "path": "race-001.mf4",
            "import_status": "FINISHED",
            "n_datapoints": 1_234_567_u64,
            "cold_bytes": 1536,
            "hot_bytes": 0,
            "backup_size": 4096
        }))
        .expect("dataset JSON");
        let (title, lines) = dataset_card(Some(&dataset), 40);
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(title, "dataset  race-001.mf4");
        assert_eq!(
            texts,
            vec![
                "FINISHED",
                "1.2M pts  4.0 KiB archive",
                "cold 1.5 KiB",
                "hot 0 B"
            ]
        );
        assert!(texts.iter().all(|line| !line.contains("race-001.mf4")));
        assert!(texts.iter().all(|line| !line.contains("42")));
    }

    #[test]
    fn progress_cell_draws_a_bar_when_in_flight() {
        let cell = progress_cell(Some(0.5), true);
        assert_eq!(cell, Cell::from("████░░░░"));
        assert_eq!(progress_cell(Some(0.5), false), Cell::from("—"));
        assert!(!shows_progress(ImportStatus::Waiting));
        assert!(shows_progress(ImportStatus::Importing));
        assert!(shows_progress(ImportStatus::Postprocessing));
    }

    #[test]
    fn dataset_card_failed_status_shows_message() {
        let dataset: Dataset = serde_json::from_value(json!({
            "id": 42,
            "datastream_id": 3,
            "path": "race-001.mf4",
            "import_status": "FAILED",
            "import_message": "Parsing signals",
            "n_datapoints": 1_234_567_u64,
            "cold_bytes": 1536,
            "hot_bytes": 0,
            "backup_size": 4096
        }))
        .expect("dataset JSON");
        let (_, lines) = dataset_card(Some(&dataset), 40);
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(texts[0], "FAILED (IMPORT)");
        assert_eq!(texts[1], "Parsing signals");
        assert_eq!(texts[2], "1.2M pts  4.0 KiB archive");
        assert_eq!(texts[3], "cold 1.5 KiB  hot 0 B");
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
        assert_eq!(license_color(LicenseType::Trial), Color::Reset);
        assert_eq!(license_color(LicenseType::Free), Color::Reset);
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
    fn host_from_url_strips_scheme_and_path() {
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
            ("expires 2027-03-16".to_string(), Color::Reset)
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
            ("no expiry".to_string(), Color::Reset)
        );
    }
}
