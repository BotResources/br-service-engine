use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use crate::name::NounName;
use crate::transport::ImpactTransport;

pub const DEAD_LETTER_NOUN: NounName = NounName::from_static("service_engine_dead_letter");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterSource {
    Reaction,
    Scheduled,
    Cron,
    Mirror,
    Outbox,
    Render,
}

impl DeadLetterSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reaction => "reaction",
            Self::Scheduled => "scheduled",
            Self::Cron => "cron",
            Self::Mirror => "mirror",
            Self::Outbox => "outbox",
            Self::Render => "render",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: Uuid,
    pub source: String,
    pub reaction: String,
    pub subject: String,
    pub message_id: Uuid,
    pub payload: Vec<u8>,
    pub producer: Option<String>,
    pub seq_key: Option<String>,
    pub seq: Option<i64>,
    pub error: String,
    pub delivered: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    Republished,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardOutcome {
    Discarded,
    Absent,
}

pub(crate) struct StagedDeadLetter<'a> {
    pub(crate) source: DeadLetterSource,
    pub(crate) reaction: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) message_id: Uuid,
    pub(crate) payload: &'a [u8],
    pub(crate) producer: Option<&'a str>,
    pub(crate) seq_key: Option<&'a str>,
    pub(crate) seq: Option<i64>,
    pub(crate) error: &'a str,
    pub(crate) delivered: i32,
}

#[derive(Clone)]
pub struct DeadLetters {
    pool: PgPool,
    transport: Option<Arc<dyn ImpactTransport>>,
}

impl DeadLetters {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            transport: None,
        }
    }

    pub fn with_transport(mut self, transport: Arc<dyn ImpactTransport>) -> Self {
        self.transport = Some(transport);
        self
    }
}

mod record;
mod store;
