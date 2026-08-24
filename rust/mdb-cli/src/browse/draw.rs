use super::format::{
    body_style, clip_args, compact_count, count_cell, ellipsis, format_expiry, format_usage,
    host_from_url, kv, kv_styled, license_color, license_type, now_epoch, opt_bytes, opt_count,
    opt_text, signal_kind, signal_source, stream_kind, sum_bytes, usage_bar,
};
use super::picker::FilePicker;
use super::session::settings_path;
use super::{AUTO_LOAD_LIMIT, App, BrowseLevel, Focus};
use marple_db::{CurrentWorkspace, StorageQuota};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, TableState, Wrap,
};

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

    if let Some(picker) = &app.env_picker {
        draw_file_picker(frame, picker);
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

fn draw_list(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::List;
    match app.browse_level {
        BrowseLevel::Streams => {
            let name_width = text_col(area, ID_COL + COUNT_COL, 2);
            render_table(
                frame,
                area,
                "streams",
                focused,
                &[],
                [
                    Constraint::Length(ID_COL),
                    Constraint::Min(4),
                    Constraint::Length(COUNT_COL),
                ],
                app.streams.len(),
                app.stream_state.selected(),
                |index| {
                    let stream = &app.streams[index];
                    Row::new([
                        Cell::from(stream.id.to_string()),
                        Cell::from(ellipsis(&stream.name, name_width)),
                        count_cell(stream.n_datasets, "d"),
                    ])
                },
            );
        }
        BrowseLevel::Datasets => {
            let path_width = text_col(area, COUNT_COL, 1);
            let title = app
                .loaded_stream()
                .map(|stream| format!("datasets  /{}", stream.name))
                .unwrap_or_else(|| "datasets".to_string());
            render_table(
                frame,
                area,
                &title,
                focused,
                &[],
                [Constraint::Min(4), Constraint::Length(COUNT_COL)],
                app.datasets.len(),
                app.dataset_state.selected(),
                |index| {
                    let dataset = &app.datasets[index];
                    Row::new([
                        Cell::from(ellipsis(&dataset.path, path_width)),
                        count_cell(dataset.n_signals, "s"),
                    ])
                },
            );
        }
        _ => {}
    }
}

fn header_row<'a>(cells: &'a [&str]) -> Row<'a> {
    Row::new(cells.iter().copied().map(Cell::from).collect::<Vec<_>>()).style(
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

fn loaded_or_hint(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    focused: bool,
    loaded: bool,
    loading: bool,
    dots: &str,
    count: Option<u64>,
    noun: &str,
    empty: bool,
) -> bool {
    if loading {
        draw_hint(
            frame,
            area,
            title,
            focused,
            &format!("loading {noun} {dots}"),
        );
        return false;
    }
    if !loaded {
        draw_hint(frame, area, title, focused, &load_hint(count, noun));
        return false;
    }
    if empty {
        draw_hint(frame, area, title, focused, &format!("no {noun}"));
        return false;
    }
    true
}

fn render_table(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    focused: bool,
    headers: &[&str],
    widths: impl IntoIterator<Item = Constraint>,
    len: usize,
    selected: Option<usize>,
    row_at: impl FnMut(usize) -> Row<'static>,
) {
    let (start, end) = visible_range(len, selected, table_view_rows(area.height));
    let title = window_title(title, start, end, len);
    let rows: Vec<Row> = (start..end).map(row_at).collect();
    let mut table = Table::new(rows, widths)
        .block(block(&title, focused))
        .style(body_style())
        .row_highlight_style(highlight())
        .highlight_symbol("");
    if !headers.is_empty() {
        table = table.header(header_row(headers));
    }
    let mut window = TableState::default().with_selected(selected.map(|index| index - start));
    frame.render_stateful_widget(table, area, &mut window);
}

fn draw_table(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Table;
    match app.browse_level {
        BrowseLevel::Root => {
            if app.streams.is_empty() {
                draw_hint(frame, area, "streams", focused, "no streams");
                return;
            }
            render_table(
                frame,
                area,
                "streams",
                focused,
                &[
                    "id", "type", "name", "plugin", "args", "datasets", "cold", "hot",
                ],
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
                app.streams.len(),
                app.stream_state.selected(),
                |index| {
                    let stream = &app.streams[index];
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
                },
            );
        }
        BrowseLevel::Streams => {
            let stream = app.selected_stream();
            let title = stream
                .map(|stream| format!("datasets  /{}", stream.name))
                .unwrap_or_else(|| "datasets".to_string());
            let stream_id = stream.map(|stream| stream.id);
            let loaded = stream_id.is_some() && app.loaded_stream_id == stream_id;
            if !loaded_or_hint(
                frame,
                area,
                &title,
                focused,
                loaded,
                app.is_loading_datasets(),
                app.loading_dots(),
                stream.and_then(|stream| stream.n_datasets),
                "datasets",
                app.datasets.is_empty(),
            ) {
                return;
            }
            render_table(
                frame,
                area,
                &title,
                focused,
                &[
                    "id",
                    "path",
                    "signals",
                    "datapoints",
                    "archive",
                    "cold",
                    "hot",
                    "status",
                ],
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
                app.datasets.len(),
                app.dataset_state.selected(),
                |index| {
                    let dataset = &app.datasets[index];
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
                },
            );
        }
        BrowseLevel::Datasets => {
            let dataset = app.selected_dataset();
            let dataset_id = dataset.map(|dataset| dataset.id);
            let loaded = dataset_id.is_some() && app.signals_dataset_id == dataset_id;
            let title = dataset
                .map(|dataset| format!("signals  /{}", dataset.path))
                .unwrap_or_else(|| "signals".to_string());
            if !loaded_or_hint(
                frame,
                area,
                &title,
                focused,
                loaded,
                app.is_loading_signals(),
                app.loading_dots(),
                dataset.and_then(|dataset| dataset.n_signals),
                "signals",
                app.signals.is_empty(),
            ) {
                return;
            }
            render_table(
                frame,
                area,
                &title,
                focused,
                &[
                    "type",
                    "id",
                    "name",
                    "unit",
                    "source",
                    "datapoints",
                    "cold",
                    "hot",
                ],
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
                app.signals.len(),
                app.signal_state.selected(),
                |index| {
                    let signal = &app.signals[index];
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
                },
            );
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
        None if app.connected => ("unknown", "—"),
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

fn text_col(area: Rect, reserved: u16, gaps: u16) -> usize {
    (area.width.saturating_sub(2) as usize)
        .saturating_sub(usize::from(reserved) + usize::from(gaps))
}

fn table_view_rows(height: u16) -> usize {
    height.saturating_sub(3).max(1) as usize
}

fn window_title(title: &str, start: usize, end: usize, len: usize) -> String {
    if len == 0 {
        title.to_string()
    } else {
        format!("{title}  {}–{} of {len}", start + 1, end)
    }
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

fn draw_help(frame: &mut Frame, app: &App, area: Rect) {
    let env = app.env_label();
    let help = if !app.status.is_empty() {
        app.status.clone()
    } else if let Some(picker) = &app.env_picker {
        if picker.editing {
            "enter use or open  esc cancel".to_string()
        } else {
            "j/k  tab recent|files  enter open/use  ← parent  / path  esc close".to_string()
        }
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

fn draw_file_picker(frame: &mut Frame, picker: &FilePicker) {
    let area = centered(frame.area(), 80, 60);
    let title = format!("env file  (saved in {})", settings_path().display());
    let bordered = block(&title, true);
    let inner = bordered.inner(area);
    frame.render_widget(ratatui::widgets::Clear, area);
    frame.render_widget(bordered, area);

    let mut constraints = Vec::new();
    if !picker.recents.is_empty() {
        constraints.push(Constraint::Length(1));
        constraints.push(Constraint::Length(picker.recents.len() as u16));
        constraints.push(Constraint::Length(1));
    }
    constraints.push(Constraint::Length(1));
    constraints.push(Constraint::Min(3));
    if picker.editing {
        constraints.push(Constraint::Length(1));
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    let mut index = 0;
    if !picker.recents.is_empty() {
        frame.render_widget(
            Paragraph::new("recent").style(Style::default().fg(Color::DarkGray)),
            chunks[index],
        );
        index += 1;
        let items: Vec<ListItem> = picker
            .recents
            .iter()
            .map(|entry| {
                let workspace = entry.workspace.as_deref().unwrap_or("—");
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{workspace:<22}"), Style::default().fg(Color::Cyan)),
                    Span::styled(entry.name.clone(), Style::default().fg(Color::DarkGray)),
                ]))
            })
            .collect();
        let in_recents = picker.selected < picker.recents.len();
        let list = List::new(items)
            .style(body_style())
            .highlight_style(highlight());
        let mut state = ListState::default().with_selected(in_recents.then_some(picker.selected));
        frame.render_stateful_widget(list, chunks[index], &mut state);
        index += 1;
        frame.render_widget(
            Paragraph::new("─".repeat(chunks[index].width as usize))
                .style(Style::default().fg(Color::DarkGray)),
            chunks[index],
        );
        index += 1;
    }

    frame.render_widget(
        Paragraph::new(picker.dir.display().to_string())
            .style(Style::default().fg(Color::DarkGray)),
        chunks[index],
    );
    index += 1;
    let items: Vec<ListItem> = picker
        .entries
        .iter()
        .map(|entry| {
            let label = if entry.is_dir {
                format!("{}/", entry.name)
            } else {
                entry.name.clone()
            };
            ListItem::new(label)
        })
        .collect();
    let file_selected = picker
        .selected
        .checked_sub(picker.recents.len())
        .filter(|_| picker.selected >= picker.recents.len());
    let list = List::new(items)
        .style(body_style())
        .highlight_style(highlight());
    let mut state = ListState::default().with_selected(file_selected);
    frame.render_stateful_widget(list, chunks[index], &mut state);
    index += 1;
    if picker.editing {
        frame.render_widget(
            Paragraph::new(format!("path  {}", picker.input)).style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            chunks[index],
        );
    }
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
    use super::{visible_range, window_title};

    #[test]
    fn window_title_shows_visible_slice() {
        assert_eq!(window_title("streams", 0, 0, 0), "streams");
        assert_eq!(window_title("streams", 0, 5, 20), "streams  1–5 of 20");
        assert_eq!(
            window_title("signals  /speed", 8, 13, 20),
            "signals  /speed  9–13 of 20"
        );
    }

    #[test]
    fn visible_range_keeps_selection_in_window() {
        assert_eq!(visible_range(0, Some(0), 5), (0, 0));
        assert_eq!(visible_range(3, Some(1), 10), (0, 3));
        assert_eq!(visible_range(20, Some(0), 5), (0, 5));
        assert_eq!(visible_range(20, Some(19), 5), (15, 20));
        assert_eq!(visible_range(20, Some(10), 5), (8, 13));
    }
}
