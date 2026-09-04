use marple_db::ProgressReporter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct AtomicProgress(pub Arc<AtomicU64>);

impl ProgressReporter for AtomicProgress {
    fn set_position(&self, position: u64) {
        self.0.store(position, Ordering::Relaxed);
    }

    fn finish(&self) {}
}
