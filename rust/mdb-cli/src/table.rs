use crate::browse::style::{block, body_style, highlight};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Cell, Row, Table, TableState};

#[derive(Clone, Debug, Default)]
pub(crate) struct TableSearch {
    pub query: String,
    pub editing: bool,
    saved: Option<String>,
}

impl TableSearch {
    pub fn start(&mut self) {
        self.saved = Some(self.query.clone());
        self.query.clear();
        self.editing = true;
    }

    pub fn active(&self) -> bool {
        self.editing || !self.query.trim().is_empty()
    }

    pub fn clear(&mut self) {
        self.query.clear();
        self.saved = None;
        self.editing = false;
    }

    pub fn cancel(&mut self) {
        if let Some(saved) = self.saved.take() {
            self.query = saved;
        }
        self.editing = false;
    }

    pub fn finish(&mut self) {
        self.saved = None;
        self.editing = false;
    }

    pub fn push(&mut self, ch: char) {
        self.query.push(ch);
    }

    pub fn backspace(&mut self) {
        self.query.pop();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchAction {
    Changed,
    Applied,
    Closed,
    Ignored,
}

pub(crate) fn handle_search_key(search: &mut TableSearch, key: KeyEvent) -> SearchAction {
    match key.code {
        KeyCode::Esc => {
            search.cancel();
            SearchAction::Closed
        }
        KeyCode::Enter => {
            search.finish();
            SearchAction::Applied
        }
        KeyCode::Backspace => {
            search.backspace();
            SearchAction::Changed
        }
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            search.push(ch);
            SearchAction::Changed
        }
        _ => SearchAction::Ignored,
    }
}

pub(crate) fn row_matches(query: &str, fields: impl IntoIterator<Item = impl AsRef<str>>) -> bool {
    let query = query.trim();
    if query.is_empty() {
        return true;
    }
    let fields: Vec<String> = fields
        .into_iter()
        .map(|field| field.as_ref().to_lowercase())
        .collect();
    query.split_whitespace().all(|token| {
        let token = token.to_lowercase();
        fields.iter().any(|field| field.contains(&token))
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Visible {
    All(usize),
    Filtered(Vec<usize>),
}

impl Visible {
    pub fn len(&self) -> usize {
        match self {
            Self::All(len) => *len,
            Self::Filtered(indices) => indices.len(),
        }
    }

    pub fn get(&self, pos: usize) -> Option<usize> {
        match self {
            Self::All(len) if pos < *len => Some(pos),
            Self::Filtered(indices) => indices.get(pos).copied(),
            _ => None,
        }
    }

    pub fn position(&self, index: usize) -> Option<usize> {
        match self {
            Self::All(len) if index < *len => Some(index),
            Self::Filtered(indices) => indices.iter().position(|&item| item == index),
            _ => None,
        }
    }
}

pub(crate) fn step_visible(
    visible: &Visible,
    selected: Option<usize>,
    delta: i32,
) -> Option<usize> {
    let len = visible.len();
    if len == 0 {
        return None;
    }
    let pos = selected
        .and_then(|selected| visible.position(selected))
        .unwrap_or(0) as i32;
    visible.get(wrap_index(len, pos, delta))
}

pub(crate) fn wrap_index(len: usize, index: i32, delta: i32) -> usize {
    if len == 0 {
        return 0;
    }
    (index + delta).rem_euclid(len as i32) as usize
}

pub(crate) fn goto_visible(visible: &Visible, index: usize) -> Option<usize> {
    let len = visible.len();
    if len == 0 {
        return None;
    }
    visible.get(index.min(len - 1))
}

pub(crate) fn snap_visible(visible: &Visible, selected: Option<usize>) -> Option<usize> {
    match selected {
        Some(selected) if visible.position(selected).is_some() => Some(selected),
        _ => visible.get(0),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn render_table(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    focused: bool,
    headers: &[&str],
    widths: impl IntoIterator<Item = Constraint>,
    visible: &Visible,
    selected: Option<usize>,
    mut row_at: impl FnMut(usize) -> Row<'static>,
) {
    let pos = selected.and_then(|selected| visible.position(selected));
    let (start, end) = visible_range(visible.len(), pos, table_view_rows(area.height));
    let title = window_title(title, start, end, visible.len());
    let rows: Vec<Row> = (start..end)
        .filter_map(|pos| visible.get(pos).map(&mut row_at))
        .collect();
    let mut table = Table::new(rows, widths)
        .block(block(&title, focused))
        .style(body_style())
        .row_highlight_style(highlight())
        .highlight_symbol("");
    if !headers.is_empty() {
        table = table.header(header_row(headers));
    }
    let mut window = TableState::default().with_selected(pos.map(|index| index - start));
    frame.render_stateful_widget(table, area, &mut window);
}

pub(crate) fn search_title(base: &str, search: &TableSearch) -> String {
    if !search.editing && search.query.trim().is_empty() {
        return base.to_string();
    }
    let caret = if search.editing { "_" } else { "" };
    format!("{base}  /{}{caret}", search.query)
}

fn header_row<'a>(cells: &'a [&str]) -> Row<'a> {
    Row::new(cells.iter().copied().map(Cell::from).collect::<Vec<_>>())
        .style(Style::default().add_modifier(Modifier::BOLD))
}

pub(crate) fn text_col(area: Rect, reserved: u16, gaps: u16) -> usize {
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

#[cfg(test)]
mod tests {
    use super::{
        TableSearch, Visible, goto_visible, handle_search_key, row_matches, search_title,
        snap_visible, step_visible, visible_range, window_title, wrap_index,
    };
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn row_matches_case_insensitive_field_substrings() {
        assert!(row_matches("", ["anything"]));
        assert!(row_matches("traffic", ["mallorca-traffic/add_traffic.py"]));
        assert!(row_matches("FAILED", ["ok.csv", "failed"]));
        assert!(row_matches("fail csv", ["ok.csv", "failed"]));
        assert!(row_matches("mb racing", ["MB Racing"]));
        assert!(!row_matches("mbr", ["MB Racing"]));
        assert!(!row_matches("failed", ["traffic.csv", "succeeded"]));
        assert!(!row_matches("xyz", ["MB Racing"]));
    }

    #[test]
    fn search_title_shows_prompt_while_editing() {
        let mut search = TableSearch::default();
        assert_eq!(search_title("streams", &search), "streams");
        search.start();
        assert_eq!(search_title("streams", &search), "streams  /_");
        search.push('f');
        assert_eq!(search_title("streams", &search), "streams  /f_");
        search.finish();
        assert_eq!(search_title("streams", &search), "streams  /f");
    }

    #[test]
    fn search_esc_cancels_edit_then_clear_drops_filter() {
        let mut search = TableSearch {
            query: "old".into(),
            ..TableSearch::default()
        };
        search.start();
        search.push('n');
        assert_eq!(
            handle_search_key(&mut search, KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)),
            super::SearchAction::Closed
        );
        assert!(!search.editing);
        assert_eq!(search.query, "old");
        search.clear();
        assert!(!search.active());
        assert_eq!(search.query, "");
    }

    #[test]
    fn search_enter_keeps_query() {
        let mut search = TableSearch::default();
        search.start();
        search.push('f');
        assert_eq!(
            handle_search_key(
                &mut search,
                KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
            ),
            super::SearchAction::Applied
        );
        assert!(!search.editing);
        assert_eq!(search.query, "f");
        assert!(search.active());
    }

    #[test]
    fn visible_motion_steps_filtered_rows() {
        let visible = Visible::Filtered(vec![2, 5, 9]);
        assert_eq!(step_visible(&visible, Some(5), 1), Some(9));
        assert_eq!(step_visible(&visible, Some(5), -1), Some(2));
        assert_eq!(step_visible(&visible, Some(2), -1), Some(9));
        assert_eq!(step_visible(&visible, Some(9), 1), Some(2));
        assert_eq!(step_visible(&visible, Some(9), 4), Some(2));
        assert_eq!(goto_visible(&visible, 0), Some(2));
        assert_eq!(goto_visible(&visible, 99), Some(9));
        assert_eq!(snap_visible(&visible, Some(5)), Some(5));
        assert_eq!(snap_visible(&visible, Some(1)), Some(2));
        assert_eq!(step_visible(&Visible::All(0), Some(0), 1), None);
        let all = Visible::All(3);
        assert_eq!(step_visible(&all, Some(1), 1), Some(2));
        assert_eq!(snap_visible(&all, Some(1)), Some(1));
        assert_eq!(goto_visible(&all, 99), Some(2));
    }

    #[test]
    fn wrap_index_wraps_and_handles_empty() {
        assert_eq!(wrap_index(3, 0, -1), 2);
        assert_eq!(wrap_index(3, 2, 1), 0);
        assert_eq!(wrap_index(3, 1, -4), 0);
        assert_eq!(wrap_index(0, 0, 1), 0);
    }

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
