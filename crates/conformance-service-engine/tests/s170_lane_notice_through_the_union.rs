use conformance_service_engine::sample::graphql::EngineDelta;
use futures_util::{StreamExt, stream};
use service_engine::{Delta, Lane, LaneNotice};

#[tokio::test]
async fn s170_a_lane_notice_reaches_the_subscription_union_as_a_typed_member() {
    match EngineDelta::from_lane_notice(&LaneNotice::Paused(vec![Lane::Presence])) {
        EngineDelta::LanesPaused(signal) => assert_eq!(
            signal.lanes,
            vec![Lane::Presence],
            "a paused lane maps to the union's LanesPaused member carrying the lanes"
        ),
        _ => panic!("a paused lane must map to the union's LanesPaused member"),
    }

    match EngineDelta::from_lane_notice(&LaneNotice::Resumed(vec![Lane::Accumulated])) {
        EngineDelta::LanesResumed(signal) => assert_eq!(signal.lanes, vec![Lane::Accumulated]),
        _ => panic!("a resumed lane must map to the union's LanesResumed member"),
    }

    let deltas = stream::empty::<Delta>();
    let notices = stream::iter(vec![
        LaneNotice::Paused(vec![Lane::Presence]),
        LaneNotice::Resumed(vec![Lane::Presence]),
    ]);
    let members: Vec<EngineDelta> = EngineDelta::subscribe(deltas, notices)
        .map(|item| item.expect("a lane notice never fails the union mapping"))
        .collect()
        .await;

    let paused = members
        .iter()
        .filter(|member| matches!(member, EngineDelta::LanesPaused(_)))
        .count();
    let resumed = members
        .iter()
        .filter(|member| matches!(member, EngineDelta::LanesResumed(_)))
        .count();
    assert_eq!(
        (paused, resumed),
        (1, 1),
        "the merged subscription stream a service wires with subscribe(deltas, notices) delivers \
         the out-of-band lane signals as union members alongside Reset/Upsert/Remove, so a client \
         watching the GraphQL subscription is told when a lane pauses and resumes"
    );
}
