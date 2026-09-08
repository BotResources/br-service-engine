use async_graphql::Schema;
use conformance_service_engine::sample::graphql::{MutationRoot, QueryRoot, SubscriptionRoot};

#[test]
fn s46_two_slices_sdl_fragments_compose_into_one_valid_schema() {
    let schema = Schema::build(QueryRoot::default(), MutationRoot, SubscriptionRoot).finish();
    let sdl = schema.sdl();

    assert!(
        sdl.contains("widget("),
        "the widget slice's query fragment is in the composed schema:\n{sdl}"
    );
    assert!(
        sdl.contains("assignment("),
        "a second slice's query fragment composes into the same schema:\n{sdl}"
    );
    assert!(
        sdl.contains("closeWidget("),
        "the widget slice contributes its mutation:\n{sdl}"
    );
    assert!(
        sdl.contains("mintSecret("),
        "the one-shot mutation is in the schema:\n{sdl}"
    );
    assert!(
        sdl.contains("widgets"),
        "the subscription fragment composes too:\n{sdl}"
    );
    assert!(
        sdl.contains("union EngineDelta"),
        "the engine's Reset/Upsert/Remove wire is one union in the schema:\n{sdl}"
    );
}
