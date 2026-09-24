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

#[derive(Debug, thiserror::Error)]
#[error("the store failed")]
pub struct SampleReactionFault(#[from] EngineError);

impl ReactionError for SampleReactionFault {
    fn disposition(&self) -> Disposition {
        match &self.0 {
            EngineError::Db(db) if sqlx_is_terminal(db) => Disposition::Terminal,
            _ => Disposition::Retry,
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
            .map_err(EngineError::from)?;
        cx.impact_caused::<Widget, _>(&cmd.id, "relabelled")?;
        Ok(())
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DetonateWidget {
    pub id: Uuid,
    pub tenant: Uuid,
    pub label: String,
    pub explode: bool,
}

pub fn detonate_widget_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("sample").unwrap(),
        aggregate: CoordAggregate::new("widget").unwrap(),
        verb: Verb::new("detonate").unwrap(),
        version: 1,
    }
}

impl ReactionMessage for DetonateWidget {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(detonate_widget_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

pub fn detonate_widget<'r>(
    cx: &'r mut Reaction<'r>,
    cmd: DetonateWidget,
) -> BoxFuture<'r, Result<(), SampleReactionFault>> {
    Box::pin(async move {
        assert!(!cmd.explode, "detonate_widget was told to explode");
        let widget = WidgetRow {
            id: cmd.id,
            tenant_id: cmd.tenant,
            label: cmd.label,
            closed: false,
        };
        cx.create(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "created")?;
        Ok(())
    })
}
