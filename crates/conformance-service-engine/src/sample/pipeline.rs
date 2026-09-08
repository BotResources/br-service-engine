use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::pipeline::{Bulk, Mutation, MutationFault, MutationInput, OneShot};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;
use crate::sample::widget::{Widget, WidgetRow};

pub use crate::sample::pipeline_support::{
    insert_widget, publish_command, publish_raw, wait_for_widget, widget_count, widget_label,
};
pub use crate::sample::reactions::{
    CreateWidget, LockWidget, SampleReactionFault, WidgetCreated, create_widget,
    create_widget_coords, lock_widget, lock_widget_coords,
};

#[derive(Debug)]
pub enum SampleFault {
    Refused(Reason),
    NotFound,
    Store(String),
}

impl std::fmt::Display for SampleFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {}", reason.code()),
            Self::NotFound => f.write_str("the widget does not exist"),
            Self::Store(detail) => write!(f, "store: {detail}"),
        }
    }
}

impl MutationFault for SampleFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            _ => None,
        }
    }
}

impl From<Reason> for SampleFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

impl From<EngineError> for SampleFault {
    fn from(error: EngineError) -> Self {
        Self::Store(error.to_string())
    }
}

#[derive(Debug, Deserialize)]
pub struct CloseWidget {
    pub id: Uuid,
}

impl MutationInput for CloseWidget {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "close_widget";
}

pub fn close_widget<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: CloseWidget,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let mut widget = cx
            .load::<WidgetRow>(&input.id)
            .await?
            .ok_or(SampleFault::NotFound)?;
        widget.close(cx.principal())?;
        cx.save(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "closed")?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct MintSecret {
    pub id: Uuid,
    pub label: String,
    pub tenant: Uuid,
}

impl MutationInput for MintSecret {
    type Output = OneShot<String>;
    type Error = SampleFault;
    const NAME: &'static str = "mint_secret";
}

pub fn mint_secret<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: MintSecret,
) -> BoxFuture<'m, Result<OneShot<String>, SampleFault>> {
    Box::pin(async move {
        let widget = WidgetRow {
            id: input.id,
            tenant_id: input.tenant,
            label: input.label,
            closed: false,
        };
        cx.create(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "minted")?;
        Ok(OneShot(format!("secret-for-{}", widget.id)))
    })
}

#[derive(Debug, Deserialize)]
pub struct ImportWidgets {
    pub tenant: Uuid,
    pub ids: Vec<Uuid>,
}

impl MutationInput for ImportWidgets {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "import_widgets";
}

pub fn import_widgets<'m>(
    cx: &'m mut Bulk<'m, SamplePrincipal>,
    input: ImportWidgets,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        for id in input.ids {
            let widget = WidgetRow {
                id,
                tenant_id: input.tenant,
                label: format!("import-{id}"),
                closed: false,
            };
            cx.create(&widget).await?;
        }
        cx.impact_all::<Widget>()?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct ScheduleCreate {
    pub id: Uuid,
    pub tenant: Uuid,
    pub label: String,
}

impl MutationInput for ScheduleCreate {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "schedule_create";
}

pub fn schedule_create<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: ScheduleCreate,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let at = cx.now();
        cx.schedule_at(
            at,
            CreateWidget {
                id: input.id,
                tenant: input.tenant,
                label: input.label,
            },
        )?;
        Ok(())
    })
}
