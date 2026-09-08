pub trait Presence: Send + Sync + 'static {
    type Key;
    type Value;
}
