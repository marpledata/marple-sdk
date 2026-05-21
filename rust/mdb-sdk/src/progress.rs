/// Receives byte-position updates from long-running transfers.
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
