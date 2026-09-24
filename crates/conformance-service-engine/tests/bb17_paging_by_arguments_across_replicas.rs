mod blackbox_support;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use blackbox_support::sdl::assert_no_session_argument;
use blackbox_support::{GraphqlWs, World, error_code, ok, passport};
use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::graphql::boot_graphql_service;
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use serde_json::{Value, json};
use service_engine::WINDOW_SIZE_INVALID_CODE;
use uuid::Uuid;

const CARD_ADVANCE: &str = "example:card_advance";
const RECV: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(100);

fn card_deltas(board: Uuid, size: usize, before: Option<&str>) -> String {
    let before = before
        .map(|card| format!(",before:\"{card}\""))
        .unwrap_or_default();
    format!(
        "subscription{{exampleCardDeltas(boardId:\"{board}\",size:{size}{before}){{__typename \
         ... on CardReset{{revision views{{... on CardView{{id title status}}}}}} \
         ... on CardUpsert{{revision view{{... on CardView{{id title status}}}}}} \
         ... on CardRemove{{revision}}}}}}"
    )
}

fn titles(reset: &Value) -> Vec<String> {
    let mut titles: Vec<String> = reset["exampleCardDeltas"]["views"]
        .as_array()
        .expect("a Reset carries its views")
        .iter()
        .map(|view| {
            view["title"]
                .as_str()
                .expect("a card has a title")
                .to_string()
        })
        .collect();
    titles.sort();
    titles
}

async fn open_window(
    base_url: &str,
    pass: &str,
    board: Uuid,
    size: usize,
    before: Option<&str>,
) -> (GraphqlWs, Value) {
    let mut ws = GraphqlWs::connect(base_url, pass).await;
    ws.subscribe("1", &card_deltas(board, size, before)).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the window opens with a Reset");
    assert_eq!(
        reset["exampleCardDeltas"]["__typename"], "CardReset",
        "{reset}"
    );
    assert_eq!(
        reset["exampleCardDeltas"]["revision"],
        json!(1),
        "a window change is a new session that counts from 1: {reset}"
    );
    (ws, reset)
}

async fn advance(world: &World, base_url: &str, pass: &str, card: &str) {
    ok(&world
        .gql_at(
            base_url,
            pass,
            "mutation($id:UUID!){exampleAdvanceCard(id:$id){success}}",
            json!({ "id": card }),
        )
        .await);
}

async fn expect_advanced(ws: &mut GraphqlWs, card: &str) {
    let delta = ws
        .next_data(RECV)
        .await
        .expect("a write committed on the other replica reaches this one");
    let delta = &delta["exampleCardDeltas"];
    assert_eq!(delta["__typename"], "CardUpsert", "{delta}");
    assert_eq!(delta["revision"], json!(2), "{delta}");
    assert_eq!(delta["view"]["id"], json!(card), "{delta}");
    assert_eq!(delta["view"]["status"], json!("doing"), "{delta}");
}

async fn live_sessions(base_url: &str) -> Option<f64> {
    let text = reqwest::Client::new()
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    text.lines()
        .find(|line| {
            line.starts_with("service_engine_sessions{")
                || line.starts_with("service_engine_sessions ")
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<f64>().ok())
}

async fn await_live_sessions(base_url: &str, want: f64, note: &str) {
    let deadline = Instant::now() + RECV;
    loop {
        if live_sessions(base_url).await == Some(want) {
            return;
        }
        assert!(Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

#[tokio::test]
async fn bb17_window_changes_land_on_either_replica_and_writes_cross_between_them() {
    let world = World::start("bb17-pod-a").await;
    let pod_b = world.spawn_pod("bb17-pod-b").await;
    let pod_a = world.base_url().to_string();
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[CARD_ADVANCE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql(
            &pass,
            "mutation($b:UUID!,$t:[String!]!){exampleImportCards(boardId:$b,titles:$t){success}}",
            json!({ "b": board, "t": ["c1", "c2", "c3"] }),
        )
        .await);
    let listed = world
        .gql(
            &pass,
            "query($b:UUID!){exampleCards(boardId:$b){id title}}",
            json!({ "b": board }),
        )
        .await;
    let ids: BTreeMap<String, String> = ok(&listed)["exampleCards"]
        .as_array()
        .expect("the board lists its cards")
        .iter()
        .map(|card| {
            (
                card["title"].as_str().expect("a title").to_string(),
                card["id"].as_str().expect("an id").to_string(),
            )
        })
        .collect();

    let (mut narrow, reset) = open_window(&pod_a, &pass, board, 2, None).await;
    assert_eq!(
        titles(&reset),
        ["c2", "c3"],
        "the window is what populate returned for size 2"
    );

    narrow.complete("1").await;
    let (mut wide, reset) = open_window(pod_b.base_url(), &pass, board, 3, None).await;
    assert_eq!(
        titles(&reset),
        ["c1", "c2", "c3"],
        "the wider window opens on the other replica with no session to find there"
    );
    advance(&world, &pod_a, &pass, &ids["c1"]).await;
    expect_advanced(&mut wide, &ids["c1"]).await;
    await_live_sessions(
        &pod_a,
        0.0,
        "the window the client completed on pod A is still a live session there while its \
         socket stays open",
    )
    .await;
    drop(narrow);

    wide.complete("1").await;
    let (mut head, reset) = open_window(&pod_a, &pass, board, 1, None).await;
    assert_eq!(
        titles(&reset),
        ["c3"],
        "the window moves back to the first replica"
    );
    advance(&world, pod_b.base_url(), &pass, &ids["c3"]).await;
    expect_advanced(&mut head, &ids["c3"]).await;

    head.complete("1").await;
    let (mut behind, reset) =
        open_window(pod_b.base_url(), &pass, board, 2, Some(&ids["c3"])).await;
    assert_eq!(
        titles(&reset),
        ["c1", "c2"],
        "the next page is its own subscription, the size cards behind the cursor"
    );
    advance(&world, &pod_a, &pass, &ids["c2"]).await;
    expect_advanced(&mut behind, &ids["c2"]).await;

    pod_b.shutdown().await;
    world.cleanup().await;
}

#[tokio::test]
async fn bb17_a_window_size_below_one_is_refused_before_any_session() {
    let world = World::start("bb17-size").await;
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let mut ws = GraphqlWs::connect(world.base_url(), &pass).await;
    ws.subscribe("1", &card_deltas(Uuid::now_v7(), 0, None))
        .await;
    let payload = ws
        .next_payload(RECV)
        .await
        .expect("the refused size answers the operation");
    assert_eq!(
        error_code(&payload),
        WINDOW_SIZE_INVALID_CODE,
        "a size the client can fix keeps its own code, never INTERNAL: {payload}"
    );
    assert!(payload["data"].is_null(), "{payload}");
    ws.expect_complete(RECV).await;
    world.cleanup().await;
}

#[tokio::test]
async fn bb17_no_root_field_lets_a_client_name_a_session() {
    let reference = async_graphql::Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish()
    .sdl();
    assert!(
        reference.contains("exampleCardDeltas(boardId: UUID!, size: Int!, before: UUID)"),
        "the reference service pages by subscription arguments, a live list always sized"
    );
    assert_no_session_argument(&reference, "the reference service", "exampleCardDeltas");

    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let service = boot_graphql_service(&db, nats.nats().await, "se_bb17", "pod-bb17").await;
    let sample = reqwest::get(service.http("/sdl"))
        .await
        .expect("the sample answers /sdl")
        .text()
        .await
        .expect("the SDL body");
    assert_no_session_argument(&sample, "the battery's sample", "sampleAssignments");

    service.shutdown().await;
    db.cleanup().await;
}
