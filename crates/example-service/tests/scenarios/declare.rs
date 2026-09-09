use br_util_axum_readiness::Readiness;

use crate::harness::{World, WorldOptions};

#[tokio::test]
async fn declaring_scopes_gates_readiness_on_identity_acceptance() {
    let world = World::start_with(
        "pod-scopes",
        WorldOptions {
            blobs: false,
            declare_scopes: true,
        },
    )
    .await;

    assert_eq!(
        world.service.readiness().snapshot(),
        Readiness::Ready,
        "the pod only reaches readiness UP once identity accepted its scope declaration"
    );
    assert!(
        world.scope_identity().declarations_seen() >= 1,
        "the service published its scope manifest to identity during boot"
    );

    world.cleanup().await;
}
