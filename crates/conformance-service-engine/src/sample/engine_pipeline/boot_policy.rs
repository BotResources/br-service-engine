use std::time::Duration;

use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::nats::Nats;

use crate::infra::TestDb;
use crate::sample::engine::engine_config;
use crate::sample::pipeline::{
    DeleteWidget, MintSecret, RelabelBoth, RelabelWidget, delete_widget, mint_secret,
    refuse_breach_label, refuse_pinned_delete, refuse_unchanged_label, relabel_both,
    relabel_widget,
};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::{WidgetProjector, WidgetRow};

pub async fn boot_policy_engine(
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
    .expect("the policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-save policy");
    engine
        .register_post_save_policy::<WidgetRow, _>(refuse_breach_label)
        .expect("honour the subjection with the breach-label policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
        .register_mutation::<RelabelBoth, _>(relabel_both)
        .expect("register the relabel-both mutation");
    engine
}

pub async fn boot_unhonoured_seam_engine(
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
    .expect("the engine boots; the unhonoured seam is caught by run, not boot");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the subjection but deliberately never honour it");
    engine
}

pub async fn boot_transition_policy_engine(
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
    .expect("the transition-policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-save policy");
    engine
        .register_post_save_policy::<WidgetRow, _>(refuse_unchanged_label)
        .expect("honour the subjection with the unchanged-label policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
}

pub async fn boot_delete_policy_engine(
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
    .expect("the delete-policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_delete_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-delete policy");
    engine
        .register_post_delete_policy::<WidgetRow, _>(refuse_pinned_delete)
        .expect("honour the subjection with the pinned-delete policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<DeleteWidget, _>(delete_widget)
        .expect("register the delete mutation");
    engine
}
