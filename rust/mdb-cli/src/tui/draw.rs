use super::{AUTO_LOAD_LIMIT, App, BrowseLevel, Focus};
use marple_db::{
    CurrentWorkspace, Dataset, LicenseType, Signal, StorageQuota, StorageStatus, Stream, StreamType,
};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, TableState, Wrap,
};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

const LEFT_PANE: [Constraint; 2] = [Constraint::Percentage(24), Constraint::Percentage(76)];
const ID_COL: u16 = 6;
const COUNT_COL: u16 = 8;

pub(super) fn draw(frame: &mut Frame, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(frame.area());
    draw_breadcrumb(frame, app, root[0]);
    if app.browse_level == BrowseLevel::Root {
        if app.info_expanded {
            draw_info(frame, app, root[1]);
        } else {
            let stack = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(7), Constraint::Min(8)])
                .split(root[1]);
            draw_workspace(frame, app, stack[0]);
            draw_table(frame, app, stack[1]);
        }
    } else {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(LEFT_PANE)
            .split(root[1]);
        draw_list(frame, app, body[0]);
        if app.info_expanded {
            draw_info(frame, app, body[1]);
        } else {
            let right = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(11), Constraint::Min(8)])
                .split(body[1]);
            draw_info(frame, app, right[0]);
            draw_table(frame, app, right[1]);
        }
    }
    draw_help(frame, app, root[2]);
    frame.render_widget(
        Paragraph::new(app.status.clone()).style(Style::default().fg(Color::Gray)),
        root[3],
    );

    if app.show_env {
        draw_picker(
            frame,
            "env file  (saved in ~/.config/mdb/tui.toml)",
            app.env_files
                .iter()
                .map(|choice| format!("{}  {}", choice.label, choice.path.display()))
                .collect(),
            &mut app.env_state,
        );
    }
}

fn draw_breadcrumb(frame: &mut Frame, app: &App, area: Rect) {
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            app.breadcrumb_path(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )])),
        area,
    );
}

fn draw_list(frame: &mut Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Focus::List;
    match app.browse_level {
        BrowseLevel::Root => {}
        BrowseLevel::Streams => {
            let name_width = text_col(area, ID_COL + COUNT_COL, 2);
            let rows: Vec<Row> = app
                .streams
                .iter()
                .map(|stream| {
                    Row::new([
                        Cell::from(stream.id.to_string()),
                        Cell::from(ellipsis(&stream.name, name_width)),
                        count_cell(stream.n_datasets, "d"),
                    ])
                })
                .collect();
            let table = Table::new(
                rows,
                [
                    Constraint::Length(ID_COL),
                    Constraint::Min(4),
                    Constraint::Length(COUNT_COL),
                ],
            )
            .block(block("streams", focused))
            .style(body_style())
            .row_highlight_style(highlight())
            .highlight_symbol("");
            frame.render_stateful_widget(table, area, &mut app.stream_state);
        }
        BrowseLevel::Datasets => {
            let path_width = text_col(area, COUNT_COL, 1);
            let rows: Vec<Row> = app
                .datasets
                .iter()
                .map(|dataset| {
                    Row::new([
                        Cell::from(ellipsis(&dataset.path, path_width)),
                        count_cell(dataset.n_signals, "s"),
                    ])
                })
                .collect();
            let title = app
                .loaded_stream()
                .map(|stream| format!("datasets  /{}", stream.name))
                .unwrap_or_else(|| "datasets".to_string());
            let table = Table::new(rows, [Constraint::Min(4), Constraint::Length(COUNT_COL)])
                .block(block(&title, focused))
                .style(body_style())
                .row_highlight_style(highlight())
                .highlight_symbol("");
            frame.render_stateful_widget(table, area, &mut app.dataset_state);
        }
    }
}

fn header_row(cells: &[&str]) -> Row<'static> {
    Row::new(
        cells
            .iter()
            .map(|cell| Cell::from((*cell).to_string()))
            .collect::<Vec<_>>(),
    )
    .style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
}

fn load_hint(count: Option<u64>, noun: &str) -> String {
    match count {
        Some(count) if count >= AUTO_LOAD_LIMIT => format!("→ to load ({count} {noun})"),
        _ => format!("→ to load {noun}"),
    }
}

fn draw_hint(frame: &mut Frame, area: Rect, title: &str, focused: bool, message: &str) {
    frame.render_widget(
        Paragraph::new(message)
            .style(body_style())
            .block(block(title, focused)),
        area,
    );
}

fn draw_table(frame: &mut Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Focus::Table;
    match app.browse_level {
        BrowseLevel::Root => {
            if app.streams.is_empty() {
                draw_hint(frame, area, "streams", focused, "no streams");
                return;
            }
            let rows = app.streams.iter().map(|stream| {
                Row::new(vec![
                    Cell::from(stream.id.to_string()),
                    Cell::from(stream_kind(stream)),
                    Cell::from(stream.name.clone()),
                    Cell::from(opt_text(stream.plugin.as_deref())),
                    Cell::from(clip_args(stream.plugin_args.as_deref(), 40)),
                    Cell::from(opt_count(stream.n_datasets)),
                    Cell::from(opt_bytes(stream.cold_bytes)),
                    Cell::from(opt_bytes(stream.hot_bytes)),
                ])
            });
            let table = Table::new(
                rows,
                [
                    Constraint::Length(8),
                    Constraint::Length(9),
                    Constraint::Min(16),
                    Constraint::Length(12),
                    Constraint::Length(40),
                    Constraint::Length(9),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(header_row(&[
                "id", "type", "name", "plugin", "args", "datasets", "cold", "hot",
            ]))
            .block(block("streams", focused))
            .style(body_style())
            .row_highlight_style(highlight())
            .highlight_symbol("");
            frame.render_stateful_widget(table, area, &mut app.stream_state);
        }
        BrowseLevel::Streams => {
            let stream_id = app.selected_stream().map(|stream| stream.id);
            let loaded = stream_id.is_some() && app.loaded_stream_id == stream_id;
            let title = app
                .selected_stream()
                .map(|stream| format!("datasets  /{}", stream.name))
                .unwrap_or_else(|| "datasets".to_string());
            if !loaded {
                let hint = load_hint(
                    app.selected_stream().and_then(|stream| stream.n_datasets),
                    "datasets",
                );
                draw_hint(frame, area, &title, focused, &hint);
                return;
            }
            if app.datasets.is_empty() {
                draw_hint(frame, area, &title, focused, "no datasets");
                return;
            }
            let rows = app.datasets.iter().map(|dataset| {
                Row::new(vec![
                    Cell::from(dataset.id.to_string()),
                    Cell::from(dataset.path.clone()),
                    Cell::from(opt_count(dataset.n_signals)),
                    Cell::from(compact_count(dataset.n_datapoints)),
                    Cell::from(opt_bytes(dataset.backup_size)),
                    Cell::from(opt_bytes(dataset.cold_bytes)),
                    Cell::from(opt_bytes(dataset.hot_bytes)),
                    Cell::from(crate::format_import_status(dataset.import_status)),
                ])
            });
            let table = Table::new(
                rows,
                [
                    Constraint::Length(8),
                    Constraint::Min(16),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(12),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(header_row(&[
                "id",
                "path",
                "signals",
                "datapoints",
                "archive",
                "cold",
                "hot",
                "status",
            ]))
            .block(block(&title, focused))
            .style(body_style())
            .row_highlight_style(highlight())
            .highlight_symbol("");
            frame.render_stateful_widget(table, area, &mut app.dataset_state);
        }
        BrowseLevel::Datasets => {
            let dataset_id = app.selected_dataset().map(|dataset| dataset.id);
            let loaded = dataset_id.is_some() && app.signals_dataset_id == dataset_id;
            let title = app
                .selected_dataset()
                .map(|dataset| format!("signals  /{}", dataset.path))
                .unwrap_or_else(|| "signals".to_string());
            if !loaded {
                let hint = load_hint(
                    app.selected_dataset().and_then(|dataset| dataset.n_signals),
                    "signals",
                );
                draw_hint(frame, area, &title, focused, &hint);
                return;
            }
            if app.signals.is_empty() {
                draw_hint(frame, area, &title, focused, "no signals");
                return;
            }
            let view = table_view_rows(area.height);
            let selected = app.signal_state.selected();
            let (start, end) = visible_range(app.signals.len(), selected, view);
            let rows = app.signals[start..end].iter().map(|signal| {
                Row::new(vec![
                    Cell::from(signal_kind(signal)),
                    Cell::from(signal.id.to_string()),
                    Cell::from(signal.name.clone()),
                    Cell::from(signal.unit.clone().unwrap_or_default()),
                    Cell::from(signal_source(signal)),
                    Cell::from(compact_count(signal.count)),
                    Cell::from(opt_bytes(signal.cold_bytes)),
                    Cell::from(opt_bytes(signal.hot_bytes)),
                ])
            });
            let title = match app.selected_dataset() {
                Some(dataset) => {
                    format!(
                        "signals  /{}  {}–{} of {}",
                        dataset.path,
                        start + 1,
                        end,
                        app.signals.len()
                    )
                }
                None => "signals".to_string(),
            };
            let table = Table::new(
                rows,
                [
                    Constraint::Length(5),
                    Constraint::Length(8),
                    Constraint::Min(16),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(12),
                    Constraint::Length(12),
                ],
            )
            .header(header_row(&[
                "type",
                "id",
                "name",
                "unit",
                "source",
                "datapoints",
                "cold",
                "hot",
            ]))
            .block(block(&title, focused))
            .style(body_style())
            .row_highlight_style(highlight())
            .highlight_symbol("");
            let mut window =
                TableState::default().with_selected(selected.map(|index| index - start));
            frame.render_stateful_widget(table, area, &mut window);
        }
    }
}

fn draw_info(frame: &mut Frame, app: &mut App, area: Rect) {
    app.info_view = area.height;
    let (title, lines) = if app.info_expanded {
        app.info_for_inspect()
    } else {
        app.info_for_highlight()
    };
    let title = if app.info_expanded {
        format!("{title}  i close")
    } else {
        title
    };
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block(&title, app.info_expanded))
            .wrap(Wrap { trim: false })
            .scroll((app.info_scroll, 0)),
        area,
    );
}

fn kv(key: &str, value: impl std::fmt::Display) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<18}"), Style::default().fg(Color::DarkGray)),
        Span::styled(value.to_string(), body_style()),
    ])
}

fn body_style() -> Style {
    Style::default().fg(Color::White)
}

pub(super) fn is_cheap(count: Option<u64>) -> bool {
    count.is_some_and(|count| count < AUTO_LOAD_LIMIT)
}

fn draw_workspace(frame: &mut Frame, app: &App, area: Rect) {
    let bordered = block("workspace", false);
    let inner = bordered.inner(area);
    frame.render_widget(bordered, area);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(28), Constraint::Length(50)])
        .split(inner);

    let datasets: u64 = app
        .streams
        .iter()
        .filter_map(|stream| stream.n_datasets)
        .sum();
    let (name, slug) = match &app.workspace {
        Some(workspace) => (workspace.name.as_str(), workspace.id.as_str()),
        None => ("not connected", "—"),
    };
    frame.render_widget(
        Paragraph::new(vec![
            kv_styled(
                "name",
                name,
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            kv("id", slug),
            kv("host", host_from_url(&app.url)),
            kv("streams", app.streams.len()),
            kv("datasets", datasets),
        ]),
        cols[0],
    );

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(cols[1]);
    let (archive, cold, hot) = workspace_usage(app);
    let features = app
        .workspace
        .as_ref()
        .and_then(|workspace| workspace.license.as_ref())
        .map(|license| &license.payload.features);
    draw_license_row(frame, rows[1], app.workspace.as_ref());
    draw_usage_row(
        frame,
        rows[2],
        "archive",
        archive,
        features.and_then(|f| f.archive_bytes),
    );
    draw_usage_row(
        frame,
        rows[3],
        "cold",
        cold,
        features.and_then(|f| f.cold_bytes),
    );
    draw_usage_row(
        frame,
        rows[4],
        "hot",
        hot,
        features.and_then(|f| f.hot_bytes),
    );
}

fn kv_styled(key: &str, value: impl std::fmt::Display, style: Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!("{key:<18}"), Style::default().fg(Color::DarkGray)),
        Span::styled(value.to_string(), style),
    ])
}

fn metric_cols(area: Rect) -> [Rect; 3] {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(9),
            Constraint::Length(19),
            Constraint::Length(22),
        ])
        .split(area);
    [cols[0], cols[1], cols[2]]
}

fn draw_license_row(frame: &mut Frame, area: Rect, workspace: Option<&CurrentWorkspace>) {
    let [label, middle, value] = metric_cols(area);
    frame.render_widget(
        Paragraph::new("license").style(Style::default().fg(Color::DarkGray)),
        label,
    );
    let Some(license) = workspace.and_then(|workspace| workspace.license.as_ref()) else {
        frame.render_widget(Paragraph::new("—").style(body_style()), middle);
        return;
    };
    let kind = license.payload.license_type;
    let (expiry, expiry_color) = format_expiry(license.payload.expiry_date, now_epoch());
    frame.render_widget(
        Paragraph::new(Span::styled(
            license_type(kind),
            Style::default()
                .fg(license_color(kind))
                .add_modifier(Modifier::BOLD),
        )),
        middle,
    );
    frame.render_widget(
        Paragraph::new(expiry)
            .style(Style::default().fg(expiry_color))
            .alignment(Alignment::Right),
        value,
    );
}

fn draw_usage_row(
    frame: &mut Frame,
    area: Rect,
    name: &str,
    used: Option<u64>,
    quota: Option<StorageQuota>,
) {
    let [label, middle, value] = metric_cols(area);
    frame.render_widget(
        Paragraph::new(name).style(Style::default().fg(Color::DarkGray)),
        label,
    );
    frame.render_widget(Paragraph::new(usage_bar(used, quota, middle.width)), middle);
    frame.render_widget(
        Paragraph::new(format_usage(used, quota))
            .style(body_style())
            .alignment(Alignment::Right),
        value,
    );
}

fn workspace_usage(app: &App) -> (Option<u64>, Option<u64>, Option<u64>) {
    match &app.workspace {
        Some(workspace) => (
            workspace.archive_bytes,
            workspace.cold_bytes,
            workspace.hot_bytes,
        ),
        None => (
            None,
            sum_bytes(app.streams.iter().map(|stream| stream.cold_bytes)),
            sum_bytes(app.streams.iter().map(|stream| stream.hot_bytes)),
        ),
    }
}

pub(super) fn stream_info(stream: Option<&Stream>, expanded: bool) -> (String, Vec<Line<'static>>) {
    let Some(stream) = stream else {
        return ("info".to_string(), vec![Line::from("no stream selected")]);
    };
    let mut lines = vec![
        kv("id", stream.id),
        kv("type", stream_kind(stream)),
        kv("datasets", opt_count(stream.n_datasets)),
        kv("plugin", opt_text(stream.plugin.as_deref())),
        kv("args", opt_text(stream.plugin_args.as_deref())),
        kv("cold", opt_bytes(stream.cold_bytes)),
        kv("hot", opt_bytes(stream.hot_bytes)),
    ];
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
                .unwrap_or("—"),
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
        push_metadata(&mut lines, &dataset.metadata, false);
    } else if !dataset.metadata.is_empty() {
        push_metadata(&mut lines, &dataset.metadata, false);
    }
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
    if let Some(reference) = meta_str(&signal.metadata, "_mdb_reference_signal") {
        lines.push(kv("alias of", reference));
    }
    if let Some(uploaded_by) = meta_str(&signal.metadata, "_mdb_uploaded_by") {
        lines.push(kv("uploaded", uploaded_by));
    }
    if let Some(uploaded_via) = meta_str(&signal.metadata, "_mdb_uploaded_via") {
        lines.push(kv("via", uploaded_via));
    }
    if let Some(uploaded_at) = meta_str(&signal.metadata, "_mdb_uploaded_at") {
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

fn opt_text(value: Option<&str>) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .to_string()
}

fn clip_args(value: Option<&str>, width: usize) -> String {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return String::new();
    };
    ellipsis(value, width)
}

fn ellipsis(text: &str, width: usize) -> String {
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

fn inner_cols(area: Rect) -> usize {
    area.width.saturating_sub(2) as usize
}

fn text_col(area: Rect, reserved: u16, gaps: u16) -> usize {
    inner_cols(area).saturating_sub(usize::from(reserved) + usize::from(gaps))
}

fn count_cell(count: Option<u64>, unit: &str) -> Cell<'static> {
    Cell::from(Line::from(count_label(count, unit)).right_aligned())
}

fn compact_count(value: Option<u64>) -> String {
    crate::format_compact_count_with(value, "—", "B")
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

fn table_view_rows(height: u16) -> usize {
    height.saturating_sub(3).max(1) as usize
}

fn visible_range(len: usize, selected: Option<usize>, view: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let selected = selected.unwrap_or(0).min(len - 1);
    let start = selected
        .saturating_sub(view / 2)
        .min(len.saturating_sub(view));
    (start, (start + view).min(len))
}

fn opt_percent(value: Option<f64>) -> String {
    crate::format_progress_with(value, "—")
}

fn opt_seconds(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}s"))
        .unwrap_or_else(|| "—".to_string())
}

fn opt_speed(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}"))
        .unwrap_or_else(|| "—".to_string())
}

fn format_usage(used: Option<u64>, quota: Option<StorageQuota>) -> String {
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
        Color::Cyan
    }
}

fn usage_bar(used: Option<u64>, quota: Option<StorageQuota>, width: u16) -> Line<'static> {
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

fn license_color(license_type: LicenseType) -> Color {
    match license_type {
        LicenseType::Paid => Color::Green,
        LicenseType::Sponsorship => Color::Yellow,
        LicenseType::Poc => Color::Magenta,
        LicenseType::Dev => Color::LightMagenta,
        _ => Color::Gray,
    }
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn format_expiry(expiry: Option<i64>, now: i64) -> (String, Color) {
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

fn host_from_url(url: &str) -> &str {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    rest.split('/')
        .next()
        .filter(|host| !host.is_empty())
        .unwrap_or(rest)
}

fn license_type(license_type: LicenseType) -> &'static str {
    match license_type {
        LicenseType::Dev => "DEV",
        LicenseType::Free => "FREE",
        LicenseType::Trial => "TRIAL",
        LicenseType::Paid => "PAID",
        LicenseType::Poc => "POC",
        LicenseType::Sponsorship => "SPONSORSHIP",
        LicenseType::Unknown => "UNKNOWN",
        _ => "UNKNOWN",
    }
}

fn storage_status(status: StorageStatus) -> &'static str {
    match status {
        StorageStatus::FrozenToCold => "FROZEN_TO_COLD",
        StorageStatus::Cold => "COLD",
        StorageStatus::ColdToHot => "COLD_TO_HOT",
        StorageStatus::Hot => "HOT",
        StorageStatus::Unknown => "UNKNOWN",
        _ => "UNKNOWN",
    }
}

fn signal_source(signal: &Signal) -> &'static str {
    if signal.metadata.contains_key("_mdb_reference_signal") {
        "Alias"
    } else if signal.metadata.contains_key("_mdb_uploaded_at") {
        "API"
    } else {
        "Import"
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
        .filter(|(key, _)| !skip_reserved || !key.starts_with("_mdb_"))
        .collect();
    if entries.is_empty() {
        return;
    }
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (key, value) in entries {
        lines.push(kv(key, format_meta_value(value)));
    }
}

fn count_label(count: Option<u64>, unit: &str) -> String {
    match count {
        Some(count) => format!("{count} {unit}"),
        None => format!("? {unit}"),
    }
}

fn sum_bytes(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut total = 0;
    let mut any = false;
    for value in values.flatten() {
        total += value;
        any = true;
    }
    any.then_some(total)
}

fn opt_count(value: Option<u64>) -> String {
    crate::format_count_with(value, "—")
}

fn opt_bytes(value: Option<u64>) -> String {
    crate::format_bytes_with(value, "—")
}

fn draw_help(frame: &mut Frame, app: &App, area: Rect) {
    let env = app.env_label();
    let help = if app.show_env {
        "j/k  S-↓/↑ page  gg/G  enter use  esc close".to_string()
    } else if app.info_expanded {
        format!("j/k next  S-↓/↑ page  gg/G  i/esc close  v env ({env})  q quit")
    } else if app.browse_level == BrowseLevel::Root {
        format!("j/k  S-↓/↑ page  gg/G  → open  i info  v env ({env})  q quit")
    } else {
        format!(
            "tab list|table  j/k  S-↓/↑ page  gg/G  → open  i info  ← back  v env ({env})  q quit"
        )
    };
    frame.render_widget(
        Paragraph::new(help).style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_picker(frame: &mut Frame, title: &str, items: Vec<String>, state: &mut ListState) {
    let area = centered(frame.area(), 70, 50);
    let list = List::new(items.into_iter().map(ListItem::new).collect::<Vec<_>>())
        .block(block(title, true))
        .highlight_style(highlight());
    frame.render_widget(ratatui::widgets::Clear, area);
    frame.render_stateful_widget(list, area, state);
}

fn block(title: &str, focused: bool) -> Block<'_> {
    Block::default()
        .title(Span::styled(title, Style::default().fg(Color::White)))
        .borders(Borders::ALL)
        .border_style(if focused {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        })
}

fn highlight() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

fn centered(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let popup = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup[1])[1]
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
            usage_bar(Some(50), Some(StorageQuota::Bytes(100)), 20).width() as usize,
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
