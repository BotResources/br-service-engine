use br_core_integration::{
    Aggregate as CoordAggregate, Bc, CommandCoords, EventCoords, PastFact, Verb,
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::inbound::{
    Disposition, ReactionCoordinates, ReactionError, ReactionMessage, sqlx_is_terminal,
};
use service_engine::pipeline::{OutboundEvent, Reaction};
use uuid::Uuid;

use crate::sample::widget::{Widget, WidgetRow};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateWidget {
    pub id: Uuid,
    pub tenant: Uuid,
    pub label: String,
}

pub fn create_widget_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("sample").unwrap(),
        aggregate: CoordAggregate::new("widget").unwrap(),
        verb: Verb::new("create").unwrap(),
        version: 1,
    }
}

impl ReactionMessage for CreateWidget {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(create_widget_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

pub struct WidgetCreated {
    pub id: Uuid,
    pub tenant: Uuid,
}

impl Serialize for WidgetCreated {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut event = serializer.serialize_struct("WidgetCreated", 2)?;
        event.serialize_field("id", &self.id)?;
        event.serialize_field("tenant", &self.tenant)?;
        event.end()
    }
}

impl OutboundEvent for WidgetCreated {
    fn coords(&self) -> EventCoords {
        EventCoords {
            producer: Bc::new("sample").unwrap(),
            aggregate: CoordAggregate::new("widget").unwrap(),
            fact: PastFact::new("created").unwrap(),
            version: 1,
        }
    }

    fn event_id(&self) -> Uuid {
        self.id
    }
}

#[derive(Debug)]
pub enum SampleReactionFault {
    Store(String),
    Terminal(String),
}

impl std::fmt::Display for SampleReactionFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(detail) => write!(f, "store: {detail}"),
            Self::Terminal(detail) => write!(f, "terminal: {detail}"),
        }
    }
}

impl std::error::Error for SampleReactionFault {}

impl ReactionError for SampleReactionFault {
    fn disposition(&self) -> Disposition {
        match self {
            Self::Store(_) => Disposition::Retry,
            Self::Terminal(_) => Disposition::Terminal,
        }
    }
}

impl From<EngineError> for SampleReactionFault {
    fn from(error: EngineError) -> Self {
        match &error {
            EngineError::Db(db) if sqlx_is_terminal(db) => Self::Terminal(error.to_string()),
            _ => Self::Store(error.to_string()),
        }
    }
}

pub fn create_widget<'r>(
    cx: &'r mut Reaction<'r>,
    cmd: CreateWidget,
) -> BoxFuture<'r, Result<(), SampleReactionFault>> {
    Box::pin(async move {
        let widget = WidgetRow {
            id: cmd.id,
            tenant_id: cmd.tenant,
            label: cmd.label,
            closed: false,
        };
        cx.create(&widget).await?;
        cx.emit(WidgetCreated {
            id: widget.id,
            tenant: widget.tenant_id,
        })?;
        cx.impact_caused::<Widget, _>(&widget.id, "created")?;
        Ok(())
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockWidget {
    pub id: Uuid,
    pub label: String,
}

pub fn lock_widget_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("sample").unwrap(),
        aggregate: CoordAggregate::new("widget").unwrap(),
        verb: Verb::new("lock").unwrap(),
        version: 1,
    }
}

impl ReactionMessage for LockWidget {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(lock_widget_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

pub fn lock_widget<'r>(
    cx: &'r mut Reaction<'r>,
    cmd: LockWidget,
) -> BoxFuture<'r, Result<(), SampleReactionFault>> {
    Box::pin(async move {
        sqlx::query("UPDATE sample_widget SET label = $2 WHERE id = $1")
            .bind(cmd.id)
            .bind(&cmd.label)
            .execute(cx.connection())
            .await
            .map_err(|error| SampleReactionFault::from(EngineError::from(error)))?;
        cx.impact_caused::<Widget, _>(&cmd.id, "relabelled")?;
        Ok(())
    })
}
