use super::format::{
    DATASET_COLS, DATASET_EXTRA, SIGNAL_COLS, STREAM_COLS, col_cells, col_headers, col_widths,
    count_cell, dataset_card, ellipsis, format_expiry, format_usage, host_from_url, kv, kv_styled,
    license_color, license_type, now_epoch, progress_cell, shows_progress, stream_card, sum_bytes,
    usage_bar,
};
use super::picker::FilePicker;
use super::session::settings_path;
use super::style::{accent, accent_bold, block, body_style, highlight};
use super::upload::{FormFocus, UploadForm};
use super::{AUTO_LOAD_LIMIT, App, BrowseLevel, Focus};
use crate::table::{render_table, search_title, text_col};
use marple_db::{CurrentWorkspace, StorageQuota};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, List, ListItem, ListState, Paragraph, Row, Wrap};
use std::collections::HashSet;
use std::path::PathBuf;

const LEFT_PANE: [Constraint; 2] = [Constraint::Percentage(24), Constraint::Percentage(76)];
const ID_COL: u16 = 6;
const COUNT_COL: u16 = 8;

pub(super) fn draw(frame: &mut Frame, app: &App) -> u16 {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(8),
            Constraint::Length(1),
        ])
        .split(frame.area());
    draw_breadcrumb(frame, app, root[0]);
    let mut info_view = app.info_view;
    if app.browse_level == BrowseLevel::Root {
        if app.info_expanded {
            info_view = root[1].height;
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
        if app.info_expanded {
            draw_list(frame, app, body[0]);
            info_view = body[1].height;
            draw_info(frame, app, body[1]);
        } else {
            let left = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(8), Constraint::Length(6)])
                .split(body[0]);
            draw_list(frame, app, left[0]);
            draw_card(frame, app, left[1]);
            draw_table(frame, app, body[1]);
        }
    }
    draw_help(frame, app, root[2]);

    if let Some(form) = &app.upload.form {
        draw_upload_picker(frame, form);
    } else if let Some(picker) = &app.env_picker {
        draw_file_picker(
            frame,
            picker,
            &format!("env file  (saved in {})", settings_path().display()),
            None,
            None,
        );
    }
    info_view
}

fn draw_breadcrumb(frame: &mut Frame, app: &App, area: Rect) {
    frame.render_widget(
        Paragraph::new(Line::from(vec![Span::styled(
            app.breadcrumb_path(),
            accent_bold(),
        )])),
        area,
    );
}

fn draw_list(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::List;
    match app.browse_level {
        BrowseLevel::Streams => {
            let name_width = text_col(area, ID_COL + COUNT_COL, 2);
            let indices = app.stream_indices(focused);
            let title = if focused {
                search_title("streams", &app.search)
            } else {
                "streams".to_string()
            };
            render_table(
                frame,
                area,
                &title,
                focused,
                &[],
                [
                    Constraint::Length(ID_COL),
                    Constraint::Min(4),
                    Constraint::Length(COUNT_COL),
                ],
                &indices,
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
            let indices = app.dataset_indices(focused);
            let title = if focused {
                search_title(&title, &app.search)
            } else {
                title
            };
            render_table(
                frame,
                area,
                &title,
                focused,
                &[],
                [Constraint::Min(4), Constraint::Length(COUNT_COL)],
                &indices,
                app.loaded_datasets
                    .as_ref()
                    .and_then(|loaded| loaded.selected_index()),
                |index| {
                    let dataset = &app.datasets()[index];
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

#[allow(clippy::too_many_arguments)]
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

fn draw_table(frame: &mut Frame, app: &App, area: Rect) {
    let focused = app.focus == Focus::Table;
    match app.browse_level {
        BrowseLevel::Root => {
            if app.is_loading_streams() {
                draw_hint(
                    frame,
                    area,
                    "streams",
                    focused,
                    &format!("loading streams {}", app.loading_dots()),
                );
                return;
            }
            if app.streams.is_empty() {
                draw_hint(frame, area, "streams", focused, "no streams");
                return;
            }
            let indices = app.stream_indices(true);
            render_table(
                frame,
                area,
                &search_title("streams", &app.search),
                focused,
                &col_headers(STREAM_COLS, &[]),
                col_widths(STREAM_COLS, &[]),
                &indices,
                app.stream_state.selected(),
                |index| Row::new(col_cells(STREAM_COLS, &app.streams[index])),
            );
        }
        BrowseLevel::Streams => {
            let stream = app.selected_stream();
            let title = stream
                .map(|stream| format!("datasets  /{}", stream.name))
                .unwrap_or_else(|| "datasets".to_string());
            let stream_id = stream.map(|stream| stream.id);
            let loaded = stream_id.is_some() && app.loaded_stream_id() == stream_id;
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
                app.datasets().is_empty(),
            ) {
                return;
            }
            let indices = app.dataset_indices(focused);
            render_table(
                frame,
                area,
                &if focused {
                    search_title(&title, &app.search)
                } else {
                    title
                },
                focused,
                &col_headers(DATASET_COLS, &DATASET_EXTRA),
                col_widths(DATASET_COLS, &DATASET_EXTRA),
                &indices,
                app.loaded_datasets
                    .as_ref()
                    .and_then(|loaded| loaded.selected_index()),
                |index| {
                    let dataset = &app.datasets()[index];
                    let mut cells = col_cells(DATASET_COLS, dataset);
                    cells.push(Cell::from(dataset_status_cell(app, dataset)));
                    cells.push(progress_cell(
                        app.upload
                            .byte_ratio(dataset.id)
                            .or(dataset.import_progress),
                        shows_progress(dataset.import_status)
                            || app.upload.byte_ratio(dataset.id).is_some(),
                    ));
                    Row::new(cells)
                },
            );
        }
        BrowseLevel::Datasets => {
            let dataset = app.selected_dataset();
            let dataset_id = dataset.map(|dataset| dataset.id);
            let loaded = dataset_id.is_some() && app.signals_dataset_id() == dataset_id;
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
                app.signals().is_empty(),
            ) {
                return;
            }
            let indices = app.signal_indices(focused);
            render_table(
                frame,
                area,
                &if focused {
                    search_title(&title, &app.search)
                } else {
                    title
                },
                focused,
                &col_headers(SIGNAL_COLS, &[]),
                col_widths(SIGNAL_COLS, &[]),
                &indices,
                app.loaded_signals
                    .as_ref()
                    .and_then(|loaded| loaded.selected_index()),
                |index| Row::new(col_cells(SIGNAL_COLS, &app.signals()[index])),
            );
        }
    }
}

fn draw_card(frame: &mut Frame, app: &App, area: Rect) {
    let width = usize::from(area.width.saturating_sub(2));
    let (title, lines) = match app.browse_level {
        BrowseLevel::Streams => stream_card(app.selected_stream(), app.loaded_import_mix(), width),
        BrowseLevel::Datasets => dataset_card(app.selected_dataset(), width),
        BrowseLevel::Root => return,
    };
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block(&title, false))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_info(frame: &mut Frame, app: &App, area: Rect) {
    let (title, lines) = app.info_for_inspect();
    let title = format!("{title}  i close");
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(block(&title, true))
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
            kv_styled("name", name, accent_bold()),
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
    frame.render_widget(Paragraph::new("license").style(body_style()), label);
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
    frame.render_widget(Paragraph::new(name).style(body_style()), label);
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

fn draw_help(frame: &mut Frame, app: &App, area: Rect) {
    frame.render_widget(Paragraph::new(app.help_text()).style(body_style()), area);
}

fn dataset_status_cell(app: &App, dataset: &marple_db::Dataset) -> String {
    let status = dataset.import_status.as_str();
    if app.upload.is_active(dataset.id) {
        format!("{status} {}", app.loading_dots())
    } else {
        status.to_string()
    }
}

fn draw_upload_picker(frame: &mut Frame, form: &UploadForm) {
    let title = if form.selected.is_empty() {
        format!("upload  /{}", form.stream_name)
    } else {
        format!(
            "upload  /{}  ({} selected)",
            form.stream_name,
            form.selected.len()
        )
    };
    draw_file_picker(
        frame,
        &form.picker,
        &title,
        Some(upload_footer(form)),
        Some(&form.selected),
    );
}

fn upload_footer(form: &UploadForm) -> Vec<Line<'static>> {
    vec![
        upload_options_line(form),
        upload_metadata_line(form),
        upload_submit_line(form),
    ]
}

fn field_style(form: &UploadForm, field: FormFocus) -> Style {
    if form.focus == field {
        highlight()
    } else {
        body_style()
    }
}

fn upload_options_line(form: &UploadForm) -> Line<'static> {
    let check = |on: bool| if on { "[x]" } else { "[ ]" };
    let ext = if form.ext_editing {
        format!("{}_", form.extension)
    } else if form.extension.is_empty() {
        "any".to_string()
    } else {
        form.extension.clone()
    };
    Line::from(vec![
        Span::styled(
            format!("overwrite {}  ", check(form.overwrite)),
            field_style(form, FormFocus::Overwrite),
        ),
        Span::styled(
            format!("skip existing {}  ", check(form.skip_existing)),
            field_style(form, FormFocus::SkipExisting),
        ),
        Span::styled(
            format!("ext [{ext}]"),
            field_style(form, FormFocus::Extension),
        ),
    ])
}

fn upload_metadata_line(form: &UploadForm) -> Line<'static> {
    let mut spans = vec![Span::styled(
        "metadata  ",
        field_style(form, FormFocus::Metadata),
    )];
    for (index, (key, value)) in form.metadata.iter().enumerate() {
        if index > 0 {
            spans.push(Span::styled("  ", field_style(form, FormFocus::Metadata)));
        }
        spans.push(Span::styled(
            format!("{key}={}", meta_display(value)),
            field_style(form, FormFocus::Metadata),
        ));
    }
    let add = if form.meta_editing {
        format!("{}_", form.meta_input)
    } else {
        "[+]".to_string()
    };
    if !form.metadata.is_empty() || form.meta_editing {
        spans.push(Span::styled("  ", field_style(form, FormFocus::Metadata)));
    }
    spans.push(Span::styled(add, field_style(form, FormFocus::Metadata)));
    Line::from(spans)
}

fn upload_submit_line(form: &UploadForm) -> Line<'static> {
    let n = form.selected.len();
    let label = if n == 0 {
        "upload  (select files)".to_string()
    } else {
        format!("upload  {n} file{}", if n == 1 { "" } else { "s" })
    };
    Line::from(Span::styled(label, field_style(form, FormFocus::Upload)))
}

fn meta_display(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn draw_file_picker(
    frame: &mut Frame,
    picker: &FilePicker,
    title: &str,
    footer: Option<Vec<Line<'static>>>,
    picked: Option<&HashSet<PathBuf>>,
) {
    let area = centered(frame.area(), 80, 70);
    let bordered = block(title, true);
    let inner = bordered.inner(area);
    frame.render_widget(ratatui::widgets::Clear, area);
    frame.render_widget(bordered, area);

    let footer_len = footer.as_ref().map(|lines| lines.len() as u16).unwrap_or(0);
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
    if footer_len > 0 {
        constraints.push(Constraint::Length(1));
        constraints.push(Constraint::Length(footer_len));
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    let mut index = 0;
    if !picker.recents.is_empty() {
        frame.render_widget(Paragraph::new("recent").style(body_style()), chunks[index]);
        index += 1;
        let items: Vec<ListItem> = picker
            .recents
            .iter()
            .map(|entry| {
                let workspace = entry.workspace.as_deref().unwrap_or("—");
                ListItem::new(Line::from(vec![
                    Span::styled(format!("{workspace:<22}"), accent()),
                    Span::styled(entry.name.clone(), body_style()),
                ]))
            })
            .collect();
        let in_recents = picker.selected < picker.recents.len();
        let list = List::new(items)
            .style(body_style())
            .highlight_style(highlight());
        let mut state = ListState::default();
        state.select(in_recents.then_some(picker.selected));
        *state.offset_mut() = picker.recents_offset();
        frame.render_stateful_widget(list, chunks[index], &mut state);
        picker.set_recents_offset(state.offset());
        index += 1;
        frame.render_widget(
            Paragraph::new("─".repeat(chunks[index].width as usize)).style(body_style()),
            chunks[index],
        );
        index += 1;
    }

    frame.render_widget(
        Paragraph::new(picker.dir.display().to_string()).style(body_style()),
        chunks[index],
    );
    index += 1;
    let items: Vec<ListItem> = picker
        .entries
        .iter()
        .map(|entry| {
            let mark =
                if picked.is_some_and(|set| set.contains(&super::picker::path_key(&entry.path))) {
                    "[x] "
                } else if picked.is_some() && entry.name != ".." {
                    "[ ] "
                } else {
                    ""
                };
            let label = if entry.is_dir {
                format!("{mark}{}/", entry.name)
            } else {
                format!("{mark}{}", entry.name)
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
    let mut state = ListState::default();
    state.select(file_selected);
    *state.offset_mut() = picker.files_offset();
    frame.render_stateful_widget(list, chunks[index], &mut state);
    picker.set_files_offset(state.offset());
    index += 1;
    if picker.editing {
        frame.render_widget(
            Paragraph::new(format!("path  {}", picker.input)).style(accent_bold()),
            chunks[index],
        );
        index += 1;
    }
    if let Some(lines) = footer {
        frame.render_widget(
            Paragraph::new("─".repeat(chunks[index].width as usize)).style(body_style()),
            chunks[index],
        );
        index += 1;
        frame.render_widget(Paragraph::new(lines).style(body_style()), chunks[index]);
    }
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
