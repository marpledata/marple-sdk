use anyhow::Result;
use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressStyle};
use marple_db::{Dataset, ImportStatus, ProgressReporter, Stream};

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
        format_import_status(dataset.import_status),
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
        format_import_status(dataset.import_status),
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

fn format_count(value: Option<u64>) -> String {
    value.map_or_else(|| "?".to_string(), |value| value.to_string())
}

fn format_import_status(status: ImportStatus) -> String {
    match status {
        ImportStatus::Uploading => "UPLOADING",
        ImportStatus::Waiting => "WAITING",
        ImportStatus::Importing => "IMPORTING",
        ImportStatus::Postprocessing => "POSTPROCESSING",
        ImportStatus::PostprocessingFailed => "POSTPROCESSING_FAILED",
        ImportStatus::Finished => "FINISHED",
        ImportStatus::Live => "LIVE",
        ImportStatus::Failed => "FAILED",
        _ => "?",
    }
    .to_string()
}

fn format_progress(value: Option<f64>) -> String {
    let Some(value) = value else {
        return "?".to_string();
    };
    let percent = if value <= 1.0 { value * 100.0 } else { value };
    format!("{percent:.0}%")
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
