use uuid::Uuid;

use crate::harness::{World, error_code, ok, passport};

#[tokio::test]
async fn full_eda_ledger_gate_and_totals() {
    let world = World::start("pod-ledger").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let l = Uuid::now_v7();

    for amount in [5, 3] {
        ok(&world
            .gql(
                &pass,
                "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
                serde_json::json!({ "id": l, "a": amount }),
            )
            .await);
    }
    let view = world
        .gql(
            &pass,
            "query($id:UUID!){ledger(id:$id){total}}",
            serde_json::json!({ "id": l }),
        )
        .await;
    assert_eq!(ok(&view)["ledger"]["total"], 8);

    let refused = world
        .gql(
            &pass,
            "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": l, "a": -100 }),
        )
        .await;
    assert_eq!(error_code(&refused), "ledger_would_go_negative");

    world.cleanup().await;
}
