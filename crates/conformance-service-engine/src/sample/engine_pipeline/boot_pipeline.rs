use std::time::Duration;

use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::nats::Nats;

use crate::infra::TestDb;
use crate::sample::engine::engine_config;
use crate::sample::pipeline::{
    CloseWidget, CreateWidget, DetonateWidget, ImportWidgets, LockWidget, MintSecret, RelabelBoth,
    ScheduleCreate, close_widget, create_widget, detonate_widget, import_widgets, lock_widget,
    mint_secret, relabel_both, schedule_create,
};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::WidgetProjector;

pub async fn boot_pipeline_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_lock_timeout(Duration::from_millis(300)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the pipeline engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_mutation::<CloseWidget, _>(close_widget)
        .expect("register the close mutation");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<ScheduleCreate, _>(schedule_create)
        .expect("register the schedule mutation");
    engine
        .register_mutation::<RelabelBoth, _>(relabel_both)
        .expect("register the relabel-both mutation (load_many over two widgets)");
    engine
        .register_bulk::<ImportWidgets, _>(import_widgets)
        .expect("register the import bulk");
    engine
        .register_reaction::<CreateWidget, _, _>("sample-create-widget", create_widget)
        .expect("register the create-widget reaction");
    engine
        .register_reaction::<LockWidget, _, _>("sample-lock-widget", lock_widget)
        .expect("register the lock-widget reaction");
    engine
}

pub async fn boot_panic_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_lock_timeout(Duration::from_millis(300)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the panic engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_reaction::<DetonateWidget, _, _>("sample-detonate-widget", detonate_widget)
        .expect("register the detonate reaction whose handler can panic");
    engine
}
