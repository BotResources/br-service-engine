use uuid::Uuid;

use crate::inbound::deadletter::{DeadLetterSource, DeadLetters, StagedDeadLetter};
use crate::inbound::message::Incoming;

impl DeadLetters {
    pub async fn record(
        &self,
        source: DeadLetterSource,
        msg: &Incoming,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let (producer, seq_key, seq) = match &msg.sequence {
            Some(s) => (
                Some(s.key.producer.as_str()),
                Some(s.key.key.as_str()),
                Some(s.seq as i64),
            ),
            None => (None, None, None),
        };
        let entry = StagedDeadLetter {
            source,
            reaction: &msg.reaction,
            subject: &msg.subject,
            message_id: msg.message_id,
            payload: msg.payload.as_ref(),
            producer,
            seq_key,
            seq,
            error,
            delivered: msg.delivered as i32,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_work(
        &self,
        source: DeadLetterSource,
        reaction: &str,
        subject: &str,
        message_id: Uuid,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let entry = StagedDeadLetter {
            source,
            reaction,
            subject,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_mirror(
        &self,
        mirror: &str,
        subject: &str,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let message_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{mirror}\u{0}{subject}").as_bytes(),
        );
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Mirror,
            reaction: mirror,
            subject,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }

    pub async fn record_render(
        &self,
        projector: &str,
        key: &str,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let message_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{projector}\u{0}{key}").as_bytes(),
        );
        let entry = StagedDeadLetter {
            source: DeadLetterSource::Render,
            reaction: projector,
            subject: key,
            message_id,
            payload: &[],
            producer: None,
            seq_key: None,
            seq: None,
            error,
            delivered: 1,
        };
        let mut tx = self.pool.begin().await?;
        let staged = self.stage(&mut tx, &entry).await?;
        tx.commit().await?;
        crate::observe::record_impacts_committed(usize::from(staged));
        Ok(())
    }
}
