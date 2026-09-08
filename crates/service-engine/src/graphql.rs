#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Reset,
    Upsert,
    Remove,
}
