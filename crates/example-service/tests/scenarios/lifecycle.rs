use std::time::Duration;

use example_contract::CreateCard;
use service_engine::erase::PersonId;
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

const BOARD_ARCHIVE: &str = "example:board_archive";
const CARD_ADVANCE: &str = "example:card_advance";

#[tokio::test]
async fn erase_removes_a_person_across_every_slice() {
    let world = World::start("pod-erase").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[], false);

    let board = Uuid::now_v7();
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Owned" }),
        )
        .await);
    let ledger = Uuid::now_v7();
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": ledger, "a": 5 }),
        )
        .await);

    let members_before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM board_member WHERE user_id = $1")
            .bind(user)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!(members_before, 1);

    let outcome = world.service.erase(PersonId(user)).await.unwrap();
    assert!(outcome.fresh, "the first erasure of a person is fresh");
    assert!(
        outcome.rows_erased >= 2,
        "board and ledger rows were touched"
    );

    let members_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM board_member WHERE user_id = $1")
            .bind(user)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
    assert_eq!(members_after, 0, "the person left the board slice");

    let authored: i64 = sqlx::query_scalar("SELECT count(*) FROM ledger_event WHERE author = $1")
        .bind(user)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert_eq!(
        authored, 0,
        "the person's authorship was scrubbed from the log"
    );

    world.cleanup().await;
}

async fn create_card_via_twin(world: &World, board: Uuid, title: &str) -> Uuid {
    let card_id = Uuid::now_v7();
    example_twin::send_create_card(
        &world.nats,
        &CreateCard {
            card_id,
            board_id: board,
            title: title.to_string(),
        },
    )
    .await
    .unwrap();
    poll_until!(Duration::from_secs(5), {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE id = $1")
            .bind(card_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        (n > 0).then_some(())
    });
    card_id
}

#[tokio::test]
async fn a_scheduled_reaction_fires_the_card_deadline_across_pods() {
    let world = World::start("pod-scheduled").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let card = create_card_via_twin(&world, board, "Deadline soon").await;

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$s:Int!){scheduleCardDeadline(id:$id,inSeconds:$s){success}}",
            serde_json::json!({ "id": card, "s": 1 }),
        )
        .await);

    poll_until!(Duration::from_secs(6), {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM card_fact WHERE card = $1 AND payload::text LIKE '%DeadlinePassed%'",
        )
        .bind(card)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
        (n > 0).then_some(())
    });

    world.cleanup().await;
}

#[tokio::test]
async fn a_cron_purges_done_cards_on_the_leader() {
    let world = World::start("pod-cron").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[CARD_ADVANCE], false);
    let board = Uuid::now_v7();
    let card = create_card_via_twin(&world, board, "To be done").await;

    for _ in 0..2 {
        ok(&world
            .gql(
                &pass,
                "mutation($id:UUID!){advanceCard(id:$id){success}}",
                serde_json::json!({ "id": card }),
            )
            .await);
    }

    poll_until!(Duration::from_secs(6), {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE id = $1")
            .bind(card)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        (n == 0).then_some(())
    });

    world.cleanup().await;
}

#[tokio::test]
async fn a_bulk_import_creates_every_card_in_one_transaction() {
    let world = World::start("pod-bulk").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    let titles = vec!["one", "two", "three", "four", "five"];
    ok(&world
        .gql(
            &pass,
            "mutation($b:UUID!,$t:[String!]!){importCards(boardId:$b,titles:$t){success}}",
            serde_json::json!({ "b": board, "t": titles }),
        )
        .await);

    let cards = world
        .gql(
            &pass,
            "query($b:UUID!){cards(boardId:$b){id title}}",
            serde_json::json!({ "b": board }),
        )
        .await;
    let imported = ok(&cards)["cards"].as_array().unwrap().len();
    assert_eq!(imported, titles.len(), "every bulk row was written");

    world.cleanup().await;
}
