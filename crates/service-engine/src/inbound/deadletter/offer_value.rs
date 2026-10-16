use sqlx::PgConnection;
use uuid::Uuid;

use super::store::upsert;
use super::{DeadLetterSource, StagedDeadLetter};

const OFFER_VALUE_NAMESPACE: Uuid = Uuid::from_u128(0x91130000_0ffe7000_b0000000_dead1e78u128);

pub(crate) async fn stage_offer_value(
    conn: &mut PgConnection,
    offer: &str,
    key: &str,
    error: &str,
) -> Result<(), sqlx::Error> {
    let message_id = Uuid::new_v5(
        &OFFER_VALUE_NAMESPACE,
        format!("{offer}\u{0}{key}").as_bytes(),
    );
    let entry = StagedDeadLetter {
        source: DeadLetterSource::Mirror,
        reaction: offer,
        subject: key,
        message_id,
        payload: &[],
        producer: None,
        seq_key: None,
        seq: None,
        error,
        delivered: 1,
    };
    upsert(conn, &entry).await.map(|_| ())
}
