use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersonId(pub Uuid);

impl PersonId {
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

pub trait Erasable: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
}
