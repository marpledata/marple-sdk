use super::{App, BrowseLevel, Focus};
use crate::table::{Visible, visible_forward, visible_span};
use std::collections::HashSet;

pub(super) struct VisualSelection {
    anchor: usize,
    snapshot: HashSet<i64>,
}

impl App {
    pub(super) fn in_visual(&self) -> bool {
        self.visual.is_some()
    }

    pub(super) fn toggle_dataset_selection(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        self.commit_visual();
        let Some(id) = self.selected_dataset().map(|dataset| dataset.id) else {
            return;
        };
        if !self.selected_datasets.remove(&id) {
            self.selected_datasets.insert(id);
        }
    }

    pub(super) fn visual_or_select_n(&mut self, count: Option<u32>) {
        if !self.dataset_table_focused() {
            return;
        }
        if let Some(n) = count {
            self.commit_visual();
            self.select_n_datasets(n);
            return;
        }
        if self.visual.is_some() {
            self.commit_visual();
            return;
        }
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return;
        };
        self.visual = Some(VisualSelection {
            anchor: current,
            snapshot: self.selected_datasets.clone(),
        });
        self.sync_visual_selection();
    }

    pub(super) fn select_all_datasets(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        self.commit_visual();
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

    pub(super) fn clear_dataset_selection(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        self.visual = None;
        self.selected_datasets.clear();
    }

    pub(super) fn cancel_visual(&mut self) -> bool {
        let Some(visual) = self.visual.take() else {
            return false;
        };
        self.selected_datasets = visual.snapshot;
        true
    }

    pub(super) fn commit_visual(&mut self) {
        self.visual = None;
    }

    pub(super) fn sync_visual_selection(&mut self) {
        let Some((anchor, snapshot)) = self
            .visual
            .as_ref()
            .map(|visual| (visual.anchor, visual.snapshot.clone()))
        else {
            return;
        };
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return;
        };
        let visible = self.dataset_indices(true);
        let ids: Vec<i64> = self.datasets().iter().map(|dataset| dataset.id).collect();
        self.selected_datasets = visual_set(&snapshot, &ids, &visible, anchor, current);
    }

    pub(super) fn visual_rows(&self) -> HashSet<usize> {
        let Some(visual) = &self.visual else {
            return HashSet::new();
        };
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return HashSet::new();
        };
        visible_span(&self.dataset_indices(true), Some(visual.anchor), current)
            .into_iter()
            .collect()
    }

    pub(super) fn is_dataset_checked(&self, dataset_id: i64) -> bool {
        self.selected_datasets.contains(&dataset_id)
    }

    pub(super) fn dataset_table_focused(&self) -> bool {
        self.browse_level == BrowseLevel::Streams && self.focus == Focus::Table
    }

    fn select_n_datasets(&mut self, n: u32) {
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return;
        };
        let visible = self.dataset_indices(true);
        let span = visible_forward(&visible, current, n.max(1) as usize);
        let last = span.last().copied();
        for index in span {
            if let Some(id) = self.datasets().get(index).map(|dataset| dataset.id) {
                self.selected_datasets.insert(id);
            }
        }
        if let Some(index) = last
            && let Some(loaded) = self.loaded_datasets.as_mut()
        {
            loaded.state.select(Some(index));
        }
    }
}

fn visual_set(
    snapshot: &HashSet<i64>,
    ids: &[i64],
    visible: &Visible,
    anchor: usize,
    current: usize,
) -> HashSet<i64> {
    let mut selected = snapshot.clone();
    for index in visible_span(visible, Some(anchor), current) {
        if let Some(&id) = ids.get(index) {
            selected.insert(id);
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::visual_set;
    use crate::table::Visible;
    use std::collections::HashSet;

    #[test]
    fn visual_set_unions_span_and_shrinks_back() {
        let ids = [10, 20, 30, 40, 50];
        let visible = Visible::All(ids.len());
        let snapshot = HashSet::from([99]);
        let grown = visual_set(&snapshot, &ids, &visible, 0, 3);
        assert_eq!(grown, HashSet::from([99, 10, 20, 30, 40]));
        let shrunk = visual_set(&snapshot, &ids, &visible, 0, 1);
        assert_eq!(shrunk, HashSet::from([99, 10, 20]));
    }

    #[test]
    fn visual_set_follows_filtered_rows() {
        let ids = [10, 20, 30, 40, 50];
        let visible = Visible::filtered(ids.len(), vec![1, 3, 4]);
        let selected = visual_set(&HashSet::new(), &ids, &visible, 1, 4);
        assert_eq!(selected, HashSet::from([20, 40, 50]));
    }
}
