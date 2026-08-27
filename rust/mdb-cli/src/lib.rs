use anyhow::Result;
use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use marple_db::{Dataset, ImportStatus, MarpleDB, ProgressReporter, Stream};

pub mod browse;
pub(crate) mod table;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum StreamListFormat {
    #[default]
    Short,
    Long,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum DatasetListFormat {
    #[default]
    Short,
    Long,
}

pub fn stream_table_header() -> &'static str {
    "Stream\tdatasets\tdatapoints\tcold\thot\tplugin\tdescription"
}

pub fn format_stream_table_row(stream: &Stream) -> String {
    let plugin = match (stream.plugin.as_deref(), stream.plugin_args.as_deref()) {
        (Some(plugin), Some(args)) => format!("{plugin} {args}"),
        (Some(plugin), None) => plugin.to_string(),
        _ => String::new(),
    };

    [
        stream.name.clone(),
        format_count(stream.n_datasets),
        format_compact_count(stream.n_datapoints),
        format_bytes(stream.cold_bytes),
        format_bytes(stream.hot_bytes),
        plugin,
        stream.description.clone(),
    ]
    .join("\t")
}

pub fn dataset_table_header() -> &'static str {
    "ID\tpath\tstatus\tdatapoints\tsignals\tcold\thot\tbackup\tcreated_by\tmessage"
}

pub fn format_dataset_table_row(dataset: &Dataset) -> String {
    [
        dataset.id.to_string(),
        dataset.path.clone(),
        format_import_status(dataset.import_status).to_string(),
        format_compact_count(dataset.n_datapoints),
        format_count(dataset.n_signals),
        format_bytes(dataset.cold_bytes),
        format_bytes(dataset.hot_bytes),
        format_bytes(dataset.backup_size),
        dataset.created_by.clone().unwrap_or_default(),
        dataset.import_message.clone().unwrap_or_default(),
    ]
    .join("\t")
}

pub fn dataset_queue_table_header() -> &'static str {
    "ID\tpath\tstatus\tprogress\tdatapoints\tsignals\tbackup\tcreated_by\tmessage"
}

pub fn format_dataset_queue_table_row(dataset: &Dataset) -> String {
    [
        dataset.id.to_string(),
        dataset.path.clone(),
        format_import_status(dataset.import_status).to_string(),
        format_progress(dataset.import_progress),
        format_compact_count(dataset.n_datapoints),
        format_count(dataset.n_signals),
        format_bytes(dataset.backup_size),
        dataset.created_by.clone().unwrap_or_default(),
        dataset.import_message.clone().unwrap_or_default(),
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

const CLI_MISSING: &str = "?";

pub fn connect(url: &str, token: &str) -> Result<MarpleDB> {
    Ok(MarpleDB::builder()
        .url(url)
        .token(token)
        .request_source(concat!("cli/rust:", env!("CARGO_PKG_VERSION")))
        .build()?)
}

pub(crate) fn format_import_status(status: ImportStatus) -> &'static str {
    status.as_str()
}

fn format_count(value: Option<u64>) -> String {
    format_count_with(value, CLI_MISSING)
}

fn format_progress(value: Option<f64>) -> String {
    format_progress_with(value, CLI_MISSING)
}

fn format_compact_count(value: Option<u64>) -> String {
    format_compact_count_with(value, CLI_MISSING, "G")
}

fn format_bytes(value: Option<u64>) -> String {
    format_bytes_with(value, CLI_MISSING)
}

pub(crate) fn format_count_with(value: Option<u64>, missing: &str) -> String {
    value.map_or_else(|| missing.to_string(), |value| value.to_string())
}

pub(crate) fn format_progress_with(value: Option<f64>, missing: &str) -> String {
    let Some(value) = value else {
        return missing.to_string();
    };
    let percent = if value <= 1.0 { value * 100.0 } else { value };
    format!("{percent:.0}%")
}

pub(crate) fn format_compact_count_with(
    value: Option<u64>,
    missing: &str,
    billion: &str,
) -> String {
    let Some(value) = value else {
        return missing.to_string();
    };
    format_scaled(value, &["", "K", "M", billion, "T", "P"], 1000.0, "", true)
}

pub(crate) fn format_bytes_with(value: Option<u64>, missing: &str) -> String {
    let Some(value) = value else {
        return missing.to_string();
    };
    format_scaled(
        value,
        &["B", "KiB", "MiB", "GiB", "TiB", "PiB"],
        1024.0,
        " ",
        false,
    )
}

fn format_scaled(value: u64, units: &[&str], base: f64, join: &str, trim_zero: bool) -> String {
    let mut scaled = value as f64;
    let mut unit = 0;
    while scaled >= base && unit < units.len() - 1 {
        scaled /= base;
        unit += 1;
    }
    if unit == 0 {
        if join.is_empty() {
            value.to_string()
        } else {
            format!("{value}{join}{}", units[0])
        }
    } else {
        let formatted = format!("{scaled:.1}");
        let number = if trim_zero {
            formatted.trim_end_matches(".0")
        } else {
            formatted.as_str()
        };
        format!("{number}{join}{}", units[unit])
    }
}

pub(crate) fn format_epoch_utc(seconds: f64) -> String {
    let secs = seconds as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hours = rem / 3600;
    let minutes = (rem % 3600) / 60;
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02} UTC")
}

// Howard Hinnant, "chrono-Compatible Low-Level Date Algorithms"
// https://howardhinnant.github.io/date_algorithms.html#civil_from_days
fn civil_from_days(mut days: i64) -> (i32, u32, u32) {
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    (year as i32, month, day)
}
