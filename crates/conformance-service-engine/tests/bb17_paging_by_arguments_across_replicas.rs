mod blackbox_support;

use std::collections::BTreeMap;
use std::time::Duration;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{TypeKind, TypeSystemDefinition};
use blackbox_support::{GraphqlWs, World, ok, passport};
use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::graphql::boot_graphql_service;
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use serde_json::{Value, json};
use uuid::Uuid;

const CARD_ADVANCE: &str = "example:card_advance";
const RECV: Duration = Duration::from_secs(15);
const SILENCE: Duration = Duration::from_millis(500);

fn card_deltas(board: Uuid, size: usize) -> String {
    format!(
        "subscription{{exampleCardDeltas(boardId:\"{board}\",size:{size}){{__typename \
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

async fn open_window(base_url: &str, pass: &str, board: Uuid, size: usize) -> (GraphqlWs, Value) {
    let mut ws = GraphqlWs::connect(base_url, pass).await;
    ws.subscribe("1", &card_deltas(board, size)).await;
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

    let (mut narrow, reset) = open_window(&pod_a, &pass, board, 2).await;
    assert_eq!(
        titles(&reset),
        ["c2", "c3"],
        "the window is what populate returned for size 2"
    );

    narrow.complete("1").await;
    let (mut wide, reset) = open_window(pod_b.base_url(), &pass, board, 3).await;
    assert_eq!(
        titles(&reset),
        ["c1", "c2", "c3"],
        "the wider window opens on the other replica with no session to find there"
    );
    advance(&world, &pod_a, &pass, &ids["c1"]).await;
    expect_advanced(&mut wide, &ids["c1"]).await;
    assert!(
        narrow.next_data(SILENCE).await.is_none(),
        "the window the client completed receives nothing more"
    );

    wide.complete("1").await;
    let (mut head, reset) = open_window(&pod_a, &pass, board, 1).await;
    assert_eq!(
        titles(&reset),
        ["c3"],
        "the window moves back to the first replica"
    );
    advance(&world, pod_b.base_url(), &pass, &ids["c3"]).await;
    expect_advanced(&mut head, &ids["c3"]).await;

    pod_b.shutdown().await;
    world.cleanup().await;
}

fn root_fields(sdl: &str) -> BTreeMap<String, Vec<String>> {
    let document = parse_schema(sdl).expect("the SDL parses");
    let mut roots = vec![
        "Query".to_string(),
        "Mutation".to_string(),
        "Subscription".to_string(),
    ];
    for definition in &document.definitions {
        if let TypeSystemDefinition::Schema(schema) = definition {
            let schema = &schema.node;
            for root in [&schema.query, &schema.mutation, &schema.subscription]
                .into_iter()
                .flatten()
            {
                roots.push(root.node.to_string());
            }
        }
    }
    let mut fields = BTreeMap::new();
    for definition in &document.definitions {
        let TypeSystemDefinition::Type(ty) = definition else {
            continue;
        };
        let TypeKind::Object(object) = &ty.node.kind else {
            continue;
        };
        if !roots.contains(&ty.node.name.node.to_string()) {
            continue;
        }
        for field in &object.fields {
            fields.insert(
                field.node.name.node.to_string(),
                field
                    .node
                    .arguments
                    .iter()
                    .map(|argument| argument.node.name.node.to_string())
                    .collect(),
            );
        }
    }
    fields
}

fn assert_no_session_argument(sdl: &str, service: &str, subscription: &str) {
    let fields = root_fields(sdl);
    assert!(
        fields.contains_key(subscription),
        "the walk reached the subscription root of {service}: {fields:?}"
    );
    let naming_a_session: Vec<(&String, &String)> = fields
        .iter()
        .flat_map(|(field, arguments)| arguments.iter().map(move |argument| (field, argument)))
        .filter(|(_, argument)| argument.to_ascii_lowercase().contains("session"))
        .collect();
    assert!(
        naming_a_session.is_empty(),
        "no root field of {service} lets a client name a session; the engine mints every one: \
         {naming_a_session:?}"
    );
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
        reference.contains("exampleCardDeltas(boardId: UUID!, size: Int)"),
        "the reference service pages by subscription arguments"
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
