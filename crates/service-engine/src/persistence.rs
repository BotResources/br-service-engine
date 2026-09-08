#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStyle {
    Crud,
    SoftEda,
    FullEda,
}

pub trait Persistence: Send + Sync + 'static {
    type Aggregate;

    const STYLE: PersistenceStyle;
}
