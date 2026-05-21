use anyhow::Result;
use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use marple_db::{ProgressReporter, Stream};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum StreamListFormat {
    #[default]
    Short,
    Long,
}

pub fn stream_table_header() -> &'static str {
    "Stream\tdatasets\tdatapoints\tcold\thot\tplugin"
}

pub fn format_stream_table_row(stream: &Stream) -> String {
    let dataset_count = value_u64(&stream.extra, "n_datasets");
    let datapoints = value_u64(&stream.extra, "n_datapoints");
    let cold_bytes = value_u64(&stream.extra, "cold_bytes");
    let hot_bytes = value_u64(&stream.extra, "hot_bytes");
    let plugin = value_str(&stream.extra, "plugin");
    let plugin_args = value_str(&stream.extra, "plugin_args");
    let plugin = match (plugin, plugin_args) {
        (Some(plugin), Some(args)) => format!("{plugin} {args}"),
        (Some(plugin), None) => plugin.to_string(),
        _ => String::new(),
    };

    [
        stream.name.clone(),
        format_count(dataset_count),
        format_compact_count(datapoints),
        format_bytes(cold_bytes),
        format_bytes(hot_bytes),
        plugin,
    ]
    .join("\t")
}

pub fn progress_bar(message: &str, total_size: u64) -> Result<ProgressBar> {
    let bar = ProgressBar::new(total_size);
    bar.set_style(ProgressStyle::default_bar().template(
        "- {msg} [{wide_bar}] ({binary_bytes_per_sec}, eta {eta}) {binary_bytes}/{binary_total_bytes}",
    )?.progress_chars("=> "));
    bar.set_message(message.to_string());
    Ok(bar)
}

pub fn progress_bar_or_hidden(message: &str, total_size: Option<u64>) -> IndicatifProgress {
    let bar = total_size.map_or_else(ProgressBar::hidden, |size| {
        progress_bar(message, size).unwrap_or_else(|_| ProgressBar::hidden())
    });
    IndicatifProgress(bar)
}

pub struct IndicatifProgress(pub ProgressBar);

impl ProgressReporter for IndicatifProgress {
    fn set_position(&self, position: u64) {
        self.0.set_position(position);
    }

    fn finish(&self) {
        self.0.finish_and_clear();
    }
}

fn value_u64(value: &Value, key: &str) -> Option<u64> {
    value
        .get(key)
        .and_then(|v| v.as_u64().or_else(|| v.as_i64()?.try_into().ok()))
}

fn value_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn format_count(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_string(), |value| value.to_string())
}

fn format_compact_count(value: Option<u64>) -> String {
    let Some(value) = value else {
        return "?".to_string();
    };
    let units = ["", "K", "M", "G", "T", "P"];
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= 1000.0 && unit < units.len() - 1 {
        scaled /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        value.to_string()
    } else {
        let formatted = format!("{scaled:.1}");
        format!("{}{}", formatted.trim_end_matches(".0"), units[unit])
    }
}

fn format_bytes(value: Option<u64>) -> String {
    let Some(value) = value else {
        return "?".to_string();
    };
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= 1024.0 && unit < units.len() - 1 {
        scaled /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{scaled:.1} {}", units[unit])
    }
}
