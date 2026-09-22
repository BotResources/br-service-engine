mod graphql_support;

use std::collections::BTreeSet;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{TypeKind, TypeSystemDefinition};
use br_core_auth::PassportHeader;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::graphql::{boot_graphql_service, passport_for};
use conformance_service_engine::sample::pipeline::insert_widget;
use graphql_support::{get_text, post_json};
use service_engine::RootPrefix;
use uuid::Uuid;

fn root_fields(sdl: &str) -> Vec<String> {
    let document = parse_schema(sdl).expect("the composed schema parses");
    let mut declared_roots: Option<BTreeSet<String>> = None;
    for definition in &document.definitions {
        if let TypeSystemDefinition::Schema(schema) = definition {
            let mut roots = BTreeSet::new();
            for root in [
                &schema.node.query,
                &schema.node.mutation,
                &schema.node.subscription,
            ]
            .into_iter()
            .flatten()
            {
                roots.insert(root.node.to_string());
            }
            declared_roots = Some(roots);
        }
    }
    let roots = declared_roots.unwrap_or_else(|| {
        ["Query", "Mutation", "Subscription"]
            .into_iter()
            .map(String::from)
            .collect()
    });
    let mut fields = Vec::new();
    for definition in document.definitions {
        if let TypeSystemDefinition::Type(type_def) = definition {
            let type_def = type_def.node;
            let name = type_def.name.node.to_string();
            if !roots.contains(&name) {
                continue;
            }
            if let TypeKind::Object(object) = type_def.kind {
                for field in object.fields {
                    let field = field.node.name.node.to_string();
                    if !field.starts_with("__") {
                        fields.push(field);
                    }
                }
            }
        }
    }
    fields
}

#[tokio::test]
async fn s212_a_prefixed_service_boots_serves_and_exposes_only_prefixed_roots() {
    let prefix = RootPrefix::from_snake("sample").expect("`sample` is a valid root prefix");

    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let user = Uuid::now_v7();
    let mine = Uuid::now_v7();
    insert_widget(&pool, mine, tenant, "alpha").await;

    let service = boot_graphql_service(&db, nats.nats().await, "se_s212", "pod-s212").await;

    let (status, sdl) = get_text(&service.http("/sdl")).await;
    assert_eq!(
        status, 200,
        "the booted service serves its SDL over the edge"
    );
    let fields = root_fields(&sdl);
    assert!(
        !fields.is_empty(),
        "the served schema exposes root fields:\n{sdl}"
    );
    for field in &fields {
        assert!(
            prefix.owns(field),
            "every served root field is under the `sample` prefix, `{field}` is not:\n{sdl}"
        );
    }
    for expected in ["sampleWidget", "sampleCloseWidget", "sampleWidgets"] {
        assert!(
            fields.iter().any(|f| f == expected),
            "the served schema exposes `{expected}`:\n{sdl}"
        );
    }

    let passport = passport_for(user, tenant).to_header();
    let query = format!("query {{ sampleWidget(id: \"{mine}\") }}");
    let (status, body) = post_json(
        &service.base_url,
        Some(&passport),
        &query,
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "the prefixed query answers over HTTP: {body}");
    assert_eq!(
        body["data"]["sampleWidget"]["id"],
        serde_json::json!(mine.to_string()),
        "the prefixed root field serves the rendered view: {body}"
    );

    service.shutdown().await;
    drop(nats);
    db.cleanup().await;
}
