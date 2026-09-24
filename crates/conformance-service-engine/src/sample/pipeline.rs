use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::pipeline::{
    Bulk, Mutation, MutationFault, MutationInput, OneShot, PostSave, Refused, Saved,
};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;
use crate::sample::widget::{Widget, WidgetProjector, WidgetRow};

pub const BREACH_LABEL: Reason = Reason::new("BREACH_LABEL");
pub const UNCHANGED_LABEL: Reason = Reason::new("UNCHANGED_LABEL");
pub const PINNED_DELETE: Reason = Reason::new("PINNED_DELETE");

pub fn refuse_breach_label(
    saved: Saved<'_, WidgetRow>,
    ps: &mut PostSave<'_, '_>,
) -> Result<(), Refused> {
    if saved.next.label.starts_with("BREACH") {
        Err(ps.refuse(BREACH_LABEL))
    } else {
        Ok(())
    }
}

pub fn refuse_unchanged_label(
    saved: Saved<'_, WidgetRow>,
    ps: &mut PostSave<'_, '_>,
) -> Result<(), Refused> {
    if saved.transitioned(|widget| widget.label.clone()) {
        Ok(())
    } else {
        Err(ps.refuse(UNCHANGED_LABEL))
    }
}

pub fn refuse_pinned_delete(
    saved: Saved<'_, WidgetRow>,
    ps: &mut PostSave<'_, '_>,
) -> Result<(), Refused> {
    if saved.next.label.starts_with("PINNED") {
        Err(ps.refuse(PINNED_DELETE))
    } else {
        Ok(())
    }
}

pub use crate::sample::pipeline_support::{
    insert_widget, publish_command, publish_raw, wait_for_widget, widget_count, widget_label,
};
pub use crate::sample::reactions::{
    CreateWidget, DetonateWidget, LockWidget, SampleReactionFault, WidgetCreated, create_widget,
    create_widget_coords, detonate_widget, detonate_widget_coords, lock_widget, lock_widget_coords,
};

#[derive(Debug, thiserror::Error)]
pub enum SampleFault {
    #[error("refused: {}", .0.code())]
    Refused(Reason),
    #[error("the widget does not exist")]
    NotFound,
    #[error("the handler rolls back after staging, as the scenario asks")]
    DeliberateRollback,
    #[error("the store failed")]
    Store(#[from] EngineError),
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
pub struct RelabelWidget {
    pub id: Uuid,
    pub label: String,
}

impl MutationInput for RelabelWidget {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "relabel_widget";
}

pub fn relabel_widget<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: RelabelWidget,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let mut widget = cx
            .load::<WidgetRow>(&input.id)
            .await?
            .ok_or(SampleFault::NotFound)?;
        widget.label = input.label;
        cx.save(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "relabelled")?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteWidget {
    pub id: Uuid,
}

impl MutationInput for DeleteWidget {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "delete_widget";
}

pub fn delete_widget<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: DeleteWidget,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let widget = cx
            .load::<WidgetRow>(&input.id)
            .await?
            .ok_or(SampleFault::NotFound)?;
        cx.delete(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "deleted")?;
        Ok(())
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
        cx.impact_all(WidgetProjector);
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

#[derive(Debug, Deserialize)]
pub struct RelabelBoth {
    pub first: Uuid,
    pub second: Uuid,
    pub label: String,
}

impl MutationInput for RelabelBoth {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "relabel_both";
}

pub fn relabel_both<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: RelabelBoth,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let mut widgets = cx
            .load_many::<WidgetRow>(&[input.first, input.second])
            .await?;
        if widgets.len() != 2 {
            return Err(SampleFault::NotFound);
        }
        for widget in widgets.iter_mut() {
            widget.label = input.label.clone();
        }
        for widget in &widgets {
            cx.save(widget).await?;
            cx.impact_caused::<Widget, _>(&widget.id, "relabelled")?;
        }
        Ok(())
    })
}
