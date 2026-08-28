use super::{App, BrowseLevel, Focus};
use crate::table::visible_span;

impl App {
    pub(super) fn toggle_dataset_selection(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let Some(id) = self.selected_dataset().map(|dataset| dataset.id) else {
            return;
        };
        self.selection_anchor = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index());
        if !self.selected_datasets.remove(&id) {
            self.selected_datasets.insert(id);
        }
    }

    pub(super) fn select_dataset_range(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return;
        };
        let visible = self.dataset_indices(true);
        for index in visible_span(&visible, self.selection_anchor, current) {
            if let Some(id) = self.datasets().get(index).map(|dataset| dataset.id) {
                self.selected_datasets.insert(id);
            }
        }
        if self.selection_anchor.is_none() {
            self.selection_anchor = Some(current);
        }
    }

    pub(super) fn select_all_datasets(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let visible = self.dataset_indices(true);
        let ids: Vec<i64> = (0..visible.len())
            .filter_map(|pos| visible.get(pos))
            .map(|index| self.datasets()[index].id)
            .collect();
        if ids.is_empty() {
            return;
        }
        let all_checked = ids.iter().all(|id| self.selected_datasets.contains(id));
        if all_checked {
            for id in ids {
                self.selected_datasets.remove(&id);
            }
        } else {
            self.selected_datasets.extend(ids);
        }
    }

    pub(super) fn is_dataset_checked(&self, dataset_id: i64) -> bool {
        self.selected_datasets.contains(&dataset_id)
    }

    pub(super) fn dataset_table_focused(&self) -> bool {
        self.browse_level == BrowseLevel::Streams && self.focus == Focus::Table
    }
}
