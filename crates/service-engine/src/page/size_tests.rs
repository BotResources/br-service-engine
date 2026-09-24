use async_graphql::{EmptyMutation, EmptySubscription, Object, Request, Schema, Variables};
use serde_json::json;

use super::*;

struct Root;

#[Object]
impl Root {
    async fn window(&self, size: WindowSize) -> u32 {
        size.get()
    }

    async fn sized(&self, size: Option<WindowSize>) -> Option<u32> {
        size.map(WindowSize::get)
    }

    async fn offset(&self, n: i32) -> i32 {
        n
    }
}

fn schema() -> Schema<Root, EmptyMutation, EmptySubscription> {
    Schema::build(Root, EmptyMutation, EmptySubscription).finish()
}

async fn answer(request: impl Into<Request>) -> serde_json::Value {
    serde_json::to_value(schema().execute(request).await).expect("a response serializes")
}

fn assert_refused(response: &serde_json::Value) {
    assert_eq!(
        response["errors"][0]["extensions"][CODE_EXTENSION],
        json!(WINDOW_SIZE_INVALID_CODE),
        "a size the client can fix is refused with its own code, before any resolver runs: \
         {response}"
    );
    assert!(response["data"].is_null(), "{response}");
}

#[test]
fn a_window_size_is_a_whole_number_from_one_to_the_graphql_int_ceiling() {
    assert!(WindowSize::new(0).is_err());
    assert_eq!(WindowSize::new(1).map(WindowSize::get), Ok(1));
    assert_eq!(
        WindowSize::new(WindowSize::MAX).map(WindowSize::limit),
        Ok(i64::from(i32::MAX))
    );
    assert!(WindowSize::new(WindowSize::MAX + 1).is_err());
}

#[test]
fn stored_window_arguments_cannot_carry_a_size_the_constructor_refuses() {
    let size = WindowSize::new(50).expect("50 is a window size");
    let encoded = serde_json::to_value(size).expect("a size encodes");
    assert_eq!(encoded, json!(50));
    assert_eq!(
        serde_json::from_value::<WindowSize>(encoded).expect("a size decodes"),
        size
    );
    assert!(serde_json::from_value::<WindowSize>(json!(0)).is_err());
    assert!(serde_json::from_value::<WindowSize>(json!(-3)).is_err());
}

#[tokio::test]
async fn a_zero_or_negative_size_is_refused_with_window_size_invalid() {
    assert_refused(&answer("{ window(size: 0) }").await);
    assert_refused(&answer("{ sized(size: -1) }").await);
    assert_refused(
        &answer(
            Request::new("query($s: Int!) { window(size: $s) }")
                .variables(Variables::from_json(json!({ "s": -20 }))),
        )
        .await,
    );
}

#[tokio::test]
async fn a_valid_size_reaches_the_resolver_and_an_absent_optional_one_is_none() {
    assert_eq!(
        answer("{ window(size: 25) sized }").await["data"],
        json!({ "window": 25, "sized": null })
    );
}

#[test]
fn the_client_sees_a_plain_int() {
    let sdl = schema().sdl();
    assert!(sdl.contains("window(size: Int!): Int!"), "{sdl}");
    assert!(sdl.contains("sized(size: Int): Int"), "{sdl}");
    assert!(!sdl.contains("scalar WindowSize"), "{sdl}");
}

#[tokio::test]
async fn a_window_size_does_not_narrow_the_other_int_arguments_of_the_schema() {
    assert_eq!(
        answer("{ offset(n: 0) other: offset(n: -4) }").await["data"],
        json!({ "offset": 0, "other": -4 })
    );
}
