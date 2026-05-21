pub trait ProgressReporter: Send + Sync + 'static {
    fn set_position(&self, position: u64);
    fn finish(&self);
}

pub struct NoopProgress;

impl ProgressReporter for NoopProgress {
    fn set_position(&self, _position: u64) {}

    fn finish(&self) {}
}
