use sqlx::PgConnection;
use uuid::Uuid;

use crate::error::EngineError;
use crate::impact::Impact;
use crate::inbound::Source;
use crate::name::NounName;
use crate::relays::outbox::{OutboxRecord, stage as stage_outbox};
use crate::schema::TABLE_SCHEDULED_MESSAGE;
use crate::time::Timestamp;
use crate::transport::ImpactTransport;
use crate::wire::KeyBytes;

pub(crate) struct ScheduledMessage {
    pub at: Timestamp,
    pub source: Source,
    pub subject: String,
    pub message_id: Uuid,
    pub payload: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct Staged {
    pub impacts: Vec<Impact>,
    pub scheduled_impacts: Vec<(NounName, KeyBytes, Timestamp)>,
    pub outbox: Vec<OutboxRecord>,
    pub scheduled_messages: Vec<ScheduledMessage>,
}

impl Staged {
    pub(crate) fn is_within(&self, impacts_per_commit: usize) -> bool {
        self.impacts.len() <= impacts_per_commit
    }

    pub(crate) async fn flush(
        &self,
        conn: &mut PgConnection,
        transport: &dyn ImpactTransport,
    ) -> Result<(), EngineError> {
        if !self.impacts.is_empty() {
            transport.stage_in(conn, &self.impacts).await?;
        }
        for (noun, key, at) in &self.scheduled_impacts {
            transport
                .schedule_in(conn, noun.clone(), key.clone(), *at)
                .await?;
        }
        for record in &self.outbox {
            stage_outbox(&mut *conn, record).await?;
        }
        for message in &self.scheduled_messages {
            insert_scheduled_message(conn, message).await?;
        }
        Ok(())
    }
}

async fn insert_scheduled_message(
    conn: &mut PgConnection,
    message: &ScheduledMessage,
) -> Result<(), EngineError> {
    let source = match message.source {
        Source::Command => "command",
        Source::Event => "event",
    };
    sqlx::query(&format!(
        "INSERT INTO {TABLE_SCHEDULED_MESSAGE} \
           (id, at, source, subject, message_id, payload) \
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(Uuid::now_v7())
    .bind(message.at.as_datetime())
    .bind(source)
    .bind(&message.subject)
    .bind(message.message_id)
    .bind(&message.payload)
    .execute(conn)
    .await?;
    Ok(())
}
