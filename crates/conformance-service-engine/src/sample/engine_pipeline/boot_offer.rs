use std::time::Duration;

use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::nats::Nats;

use crate::infra::TestDb;
use crate::sample::engine::engine_config;
use crate::sample::offer::{MintThenReject, WidgetOffer, mint_then_reject};
use crate::sample::pipeline::{
    CloseWidget, DeleteWidget, MintSecret, RelabelWidget, close_widget, delete_widget, mint_secret,
    relabel_widget,
};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::WidgetProjector;
use crate::sample::widget_tag::{
    DeleteWidgetTag, SetWidgetTag, WidgetTag, delete_widget_tag, set_widget_tag,
};

pub async fn boot_offer_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    boot_offer_engine_reconciling(db, nats, channel, pod, Duration::from_secs(300)).await
}

pub async fn boot_offer_engine_reconciling(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    reconcile: Duration,
) -> Engine<SamplePrincipal> {
    boot_offer_engine_leased(
        db,
        nats,
        channel,
        pod,
        reconcile,
        service_engine::config::DEFAULT_LEASE,
    )
    .await
}

pub async fn boot_offer_engine_leased(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    reconcile: Duration,
    lease: Duration,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_lock_timeout(Duration::from_millis(300))
            .with_offer_reconcile(reconcile)
            .with_lease(lease),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the offer engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<CloseWidget, _>(close_widget)
        .expect("register the close mutation");
    engine
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
        .register_mutation::<DeleteWidget, _>(delete_widget)
        .expect("register the delete mutation");
    engine
        .register_mutation::<MintThenReject, _>(mint_then_reject)
        .expect("register the mint-then-reject mutation");
    engine
        .register_offer::<WidgetOffer>()
        .expect("register the widget offer");
    engine
}

pub async fn boot_offer_trigger_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    reconcile: Duration,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_lock_timeout(Duration::from_millis(300))
            .with_offer_reconcile(reconcile),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the offer-trigger engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<SetWidgetTag, _>(set_widget_tag)
        .expect("register the set-tag mutation");
    engine
        .register_mutation::<DeleteWidgetTag, _>(delete_widget_tag)
        .expect("register the delete-tag mutation");
    engine
        .register_offer::<WidgetOffer>()
        .expect("register the widget offer");
    engine
        .register_offer_trigger::<WidgetOffer, WidgetTag>()
        .expect("register the widget-tag trigger for the widget offer");
    engine
}
