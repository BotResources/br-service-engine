#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Retry,
    Park,
    Terminal,
}

pub trait ReactionError {
    fn disposition(&self) -> Disposition;
}
