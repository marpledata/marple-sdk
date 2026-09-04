use super::{App, BrowseLevel, DatasetView, Focus, Message};
use crate::table::{TableSearch, row_matches};
use ratatui::text::Line;

#[derive(Default)]
pub(super) struct DebugState {
    load_gen: u64,
    dataset_id: Option<i64>,
    pub(super) pending: bool,
    messages: Option<Result<Vec<String>, String>>,
    pub(super) scroll: u16,
    pub(super) search: TableSearch,
}

impl DebugState {
    pub(super) fn invalidate(&mut self) {
        self.load_gen = self.load_gen.wrapping_add(1);
        self.dataset_id = None;
        self.pending = false;
        self.messages = None;
        self.scroll = 0;
        self.search.clear();
    }
}

impl App {
    pub(super) fn request_debug(&mut self) {
        let Some(dataset) = self.selected_dataset() else {
            return;
        };
        let stream_id = dataset.datastream_id;
        let dataset_id = dataset.id;
        if self.debug.dataset_id == Some(dataset_id)
            && (self.debug.pending || self.debug.messages.is_some())
        {
            return;
        }
        self.debug.load_gen = self.debug.load_gen.wrapping_add(1);
        let load_gen = self.debug.load_gen;
        self.debug.dataset_id = Some(dataset_id);
        self.debug.pending = true;
        self.debug.messages = None;
        self.debug.scroll = 0;
        let db = self.db.clone();
        let tx = self.events.clone();
        tokio::spawn(async move {
            let result = db
                .get_debug_messages(stream_id, dataset_id)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(Message::DebugLoaded(load_gen, dataset_id, result));
        });
    }

    pub(super) fn apply_debug_result(
        &mut self,
        load_gen: u64,
        dataset_id: i64,
        result: Result<Vec<String>, String>,
    ) {
        if load_gen != self.debug.load_gen || self.debug.dataset_id != Some(dataset_id) {
            return;
        }
        self.debug.pending = false;
        self.debug.messages = Some(result);
    }

    pub(super) fn debug_filter_enabled(&self) -> bool {
        self.browse_level == BrowseLevel::Datasets
            && self.dataset_view == DatasetView::Debug
            && self.focus == Focus::Table
    }

    pub(super) fn debug_lines(&self) -> Vec<Line<'static>> {
        if self.debug.pending {
            return vec![Line::from(format!(
                "loading debug messages {}",
                self.loading_dots()
            ))];
        }
        match &self.debug.messages {
            None => vec![Line::from("no debug messages")],
            Some(Err(error)) => vec![Line::from(error.clone())],
            Some(Ok(messages)) if messages.is_empty() => {
                vec![Line::from("no debug messages")]
            }
            Some(Ok(messages)) => debug_message_lines(messages, self.debug.search.query.trim()),
        }
    }

    pub(super) fn debug_line_count(&self) -> u16 {
        self.debug_lines().len() as u16
    }
}

fn debug_message_lines(messages: &[String], query: &str) -> Vec<Line<'static>> {
    let lines: Vec<Line<'static>> = messages
        .iter()
        .filter(|message| row_matches(query, [message.as_str()]))
        .flat_map(|message| message.lines().map(|line| Line::from(line.to_string())))
        .collect();
    if lines.is_empty() {
        vec![Line::from("no matches")]
    } else {
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::debug_message_lines;

    #[test]
    fn debug_messages_split_embedded_newlines() {
        let lines = debug_message_lines(
            &[
                "parser started\nchannel A skipped\r\nok".to_string(),
                "done".to_string(),
            ],
            "",
        );
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(
            texts,
            vec!["parser started", "channel A skipped", "ok", "done"]
        );
    }

    #[test]
    fn debug_filter_keeps_whole_matching_message() {
        let lines = debug_message_lines(
            &["keep\nthis block".to_string(), "ignore me".to_string()],
            "block",
        );
        let texts: Vec<String> = lines.iter().map(ToString::to_string).collect();
        assert_eq!(texts, vec!["keep", "this block"]);
    }
}
