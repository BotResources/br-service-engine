use std::collections::BTreeMap;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{TypeKind, TypeSystemDefinition};

pub fn root_fields(sdl: &str) -> BTreeMap<String, Vec<String>> {
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

pub fn assert_no_session_argument(sdl: &str, service: &str, subscription: &str) {
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
