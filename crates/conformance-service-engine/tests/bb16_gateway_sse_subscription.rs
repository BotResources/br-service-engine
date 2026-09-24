mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::sse::{EVENT_STREAM, Frame, SseFrames, stalled_request};
use blackbox_support::{SessionBounds, World, human, ok, passport};
use br_test_harness::SseSubscription;
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(15);
const COMPLETE_WITHIN: Duration = Duration::from_secs(10);
const STALL_WAIT: Duration = Duration::from_secs(5);
const SESSION_TTL: Duration = Duration::from_millis(500);
const SESSION_MAX_AGE: Duration = Duration::from_millis(2000);
const SUB: &str = "subscription{exampleBoardDeltas{__typename \
    ... on BoardReset{revision views{... on BoardView{id archived}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived}}} \
    ... on BoardRemove{revision}}}";

async fn create_board(world: &World, pass: &str, board: Uuid) {
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Gateway board" }),
        )
        .await);
}

async fn answer(base_url: &str, passport: Option<&str>, accept: Option<&str>) -> (u16, String) {
    let mut request = reqwest::Client::new()
        .post(format!("{base_url}/graphql"))
        .json(&serde_json::json!({ "query": SUB }));
    if let Some(passport) = passport {
        request = request.header("x-passport", passport);
    }
    if let Some(accept) = accept {
        request = request.header("accept", accept);
    }
    let response = request.send().await.expect("the POST reaches the server");
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

#[tokio::test]
async fn bb16_a_gateway_sse_subscription_resets_then_upserts_on_a_contiguous_revision() {
    let world = World::start("bb16a-pod").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();
    create_board(&world, &pass, board).await;

    let mut sse = SseSubscription::open(
        world.base_url(),
        &human(user, org, &[BOARD_ARCHIVE], false),
        SUB,
    )
    .await;
    let reset = sse.expect_event_on("exampleBoardDeltas", RECV).await;
    assert_eq!(
        reset["__typename"], "BoardReset",
        "the attach opens with a Reset: {reset}"
    );
    assert_eq!(
        reset["revision"],
        serde_json::json!(1),
        "a session's first revision is 1: {reset}"
    );
    assert_eq!(
        reset["views"][0]["id"],
        serde_json::json!(board),
        "the Reset carries the committed board: {reset}"
    );

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);

    let upsert = loop {
        let delta = sse.expect_event_on("exampleBoardDeltas", RECV).await;
        if delta["__typename"] == "BoardUpsert" {
            break delta;
        }
    };
    assert_eq!(
        upsert["revision"],
        serde_json::json!(2),
        "the next delta advances the revision by exactly one: {upsert}"
    );
    assert_eq!(
        upsert["view"]["archived"],
        serde_json::json!(true),
        "the committed state, not the mutation response, reaches the stream: {upsert}"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn bb16_an_sse_subscription_without_a_trusted_passport_is_refused_401_before_its_body_is_read()
 {
    let world = World::start("bb16b-pod").await;
    let not_a_passport = "not-a-valid-passport";
    let decodes_to_no_passport = "eyJub3QiOiJhIHBhc3Nwb3J0In0";

    for (label, header) in [
        ("absent", None),
        ("undecodable", Some(not_a_passport)),
        ("not a passport", Some(decodes_to_no_passport)),
    ] {
        let on_the_stream = answer(world.base_url(), header, Some(EVENT_STREAM)).await;
        assert_eq!(
            on_the_stream.0, 401,
            "a passport that is {label} is refused on the event stream: {on_the_stream:?}"
        );
        assert_eq!(
            on_the_stream,
            answer(world.base_url(), header, None).await,
            "passport {label}: the event stream is refused with the very 401 a JSON request gets"
        );

        let answer = stalled_request(world.base_url(), header, STALL_WAIT)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "passport {label}: the pod waited for a body it never received before refusing"
                )
            });
        assert!(
            answer.starts_with("HTTP/1.1 401"),
            "passport {label}: the refusal comes before the body is read: {answer}"
        );
    }

    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[BOARD_ARCHIVE], false);
    assert_eq!(
        stalled_request(world.base_url(), Some(&pass), STALL_WAIT).await,
        None,
        "control: with a trusted passport the pod reads the body, so it waits for the rest"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn bb16_an_sse_stream_older_than_session_max_age_completes_and_a_new_request_attaches() {
    let world = World::start_with_bounds(
        "bb16c-pod",
        Some(SessionBounds {
            ttl: SESSION_TTL,
            max_age: SESSION_MAX_AGE,
        }),
    )
    .await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    create_board(&world, &pass, Uuid::now_v7()).await;

    let opened = Instant::now();
    let mut sse = SseFrames::open(world.base_url(), Some(&pass), SUB).await;
    assert_eq!(
        (
            sse.status,
            sse.content_type.as_str(),
            sse.cache_control.as_str()
        ),
        (200, EVENT_STREAM, "no-cache"),
        "the gateway's subscription request is answered with an event stream"
    );
    match sse.next_event(RECV).await {
        Some(Frame::Next(reset)) => assert_eq!(
            reset["data"]["exampleBoardDeltas"]["__typename"], "BoardReset",
            "the stream opens with a Reset: {reset}"
        ),
        other => panic!("the stream opens with a next frame carrying the Reset, got {other:?}"),
    }

    assert_eq!(
        sse.next_event(COMPLETE_WITHIN).await,
        Some(Frame::Complete),
        "past session_max_age the engine completes the stream, so the gateway re-subscribes \
         with a fresh passport"
    );
    assert!(
        opened.elapsed() >= SESSION_MAX_AGE,
        "the stream is completed at session_max_age, not before"
    );
    assert_eq!(
        sse.next_frame(COMPLETE_WITHIN).await,
        None,
        "the complete is the last frame: the response ends with it"
    );

    let mut fresh = SseFrames::open(world.base_url(), Some(&pass), SUB).await;
    match fresh.next_event(RECV).await {
        Some(Frame::Next(reset)) => assert_eq!(
            reset["data"]["exampleBoardDeltas"]["__typename"], "BoardReset",
            "a new request opens a new session from committed state: {reset}"
        ),
        other => panic!("a new request still attaches — the bound is on the stream: {other:?}"),
    }

    world.cleanup().await;
}
