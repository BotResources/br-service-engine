pub trait Offer: Send + Sync + 'static {
    type Row;
    type Published;

    fn publish(row: &Self::Row) -> Option<Self::Published>;
}
