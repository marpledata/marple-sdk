/// Receives byte-position updates from long-running transfers.
///
/// ```
/// use marple_db::ProgressReporter;
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// struct Counter(AtomicU64);
///
/// impl ProgressReporter for Counter {
///     fn set_position(&self, position: u64) {
///         self.0.store(position, Ordering::Relaxed);
///     }
///
///     fn finish(&self) {}
/// }
/// ```
pub trait ProgressReporter: Send + Sync + 'static {
    /// Sets the current byte position.
    fn set_position(&self, position: u64);

    /// Marks the transfer as finished.
    fn finish(&self);
}

/// Progress reporter that ignores all updates.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProgress;

impl ProgressReporter for NoopProgress {
    fn set_position(&self, _position: u64) {}

    fn finish(&self) {}
}
