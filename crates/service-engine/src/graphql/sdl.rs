use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{TypeKind, TypeSystemDefinition};
use async_graphql::registry::{MetaType, MetaTypeName, Registry};
use async_graphql::{EmptyMutation, EmptySubscription, ObjectType, SubscriptionType};

use crate::error::EngineError;

const LANES_PAUSED_TYPE: &str = "LanesPaused";
const LANES_RESUMED_TYPE: &str = "LanesResumed";

pub(crate) struct DerivedMembers {
    pub root_fields: Vec<String>,
    pub object_types: Vec<String>,
    pub reactive_envelopes: BTreeSet<String>,
}

pub(crate) fn derive_members<Q, M, S>() -> DerivedMembers
where
    Q: ObjectType,
    M: ObjectType,
    S: SubscriptionType,
{
    let mut registry = Registry::default();
    let query = MetaTypeName::concrete_typename(&Q::create_type_info(&mut registry)).to_string();
    let mutation = MetaTypeName::concrete_typename(&M::create_type_info(&mut registry)).to_string();
    let subscription =
        MetaTypeName::concrete_typename(&S::create_type_info(&mut registry)).to_string();
    let roots: BTreeSet<&str> = [query.as_str(), mutation.as_str(), subscription.as_str()]
        .into_iter()
        .collect();
    members_from_registry(&registry, &roots)
}

fn members_from_registry(registry: &Registry, roots: &BTreeSet<&str>) -> DerivedMembers {
    let mut root_fields = Vec::new();
    let mut object_types = Vec::new();
    let mut unions = Vec::new();
    for (name, ty) in &registry.types {
        if name.starts_with("__") {
            continue;
        }
        match ty {
            MetaType::Object { fields, .. } => {
                if roots.contains(name.as_str()) {
                    root_fields.extend(fields.keys().filter(|f| !f.starts_with("__")).cloned());
                } else {
                    object_types.push(name.clone());
                }
            }
            MetaType::Union { possible_types, .. } => {
                unions.push(possible_types.iter().cloned().collect());
            }
            _ => {}
        }
    }
    DerivedMembers {
        root_fields,
        object_types,
        reactive_envelopes: reactive_envelopes(&unions),
    }
}

fn reactive_envelopes(unions: &[BTreeSet<String>]) -> BTreeSet<String> {
    let mut envelopes = BTreeSet::new();
    for members in unions {
        if members.contains(LANES_PAUSED_TYPE) && members.contains(LANES_RESUMED_TYPE) {
            envelopes.extend(members.iter().cloned());
        }
    }
    envelopes
}

pub(crate) fn engine_injected_object_types() -> &'static BTreeSet<String> {
    static SET: OnceLock<BTreeSet<String>> = OnceLock::new();
    SET.get_or_init(|| {
        derive_members::<EngineWrappers, EmptyMutation, EmptySubscription>()
            .object_types
            .into_iter()
            .collect()
    })
}

#[derive(Default)]
struct EngineWrappers;

#[async_graphql::Object]
impl EngineWrappers {
    async fn ack(&self) -> Option<crate::graphql::MutationAck> {
        None
    }

    async fn lanes_paused(&self) -> Option<crate::lanes::LanesPaused> {
        None
    }

    async fn lanes_resumed(&self) -> Option<crate::lanes::LanesResumed> {
        None
    }
}

pub(crate) struct ParsedSchema {
    pub root_fields: Vec<String>,
    pub object_types: Vec<String>,
    pub reactive_envelopes: BTreeSet<String>,
}

pub(crate) fn parse_schema_members(sdl: &str) -> Result<ParsedSchema, EngineError> {
    let document = parse_schema(sdl).map_err(|error| EngineError::SchemaParse {
        detail: crate::chain::describe(&error),
    })?;

    let mut declared_roots: Option<BTreeSet<String>> = None;
    let mut objects: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut unions: Vec<BTreeSet<String>> = Vec::new();
    for definition in document.definitions {
        match definition {
            TypeSystemDefinition::Schema(schema) => {
                let schema = schema.node;
                let mut roots = BTreeSet::new();
                for root in [schema.query, schema.mutation, schema.subscription]
                    .into_iter()
                    .flatten()
                {
                    roots.insert(root.node.to_string());
                }
                declared_roots = Some(roots);
            }
            TypeSystemDefinition::Type(type_def) => {
                let type_def = type_def.node;
                let name = type_def.name.node.to_string();
                if name.starts_with("__") {
                    continue;
                }
                match type_def.kind {
                    TypeKind::Object(object) => {
                        let fields = object
                            .fields
                            .into_iter()
                            .map(|field| field.node.name.node.to_string())
                            .collect();
                        objects.insert(name, fields);
                    }
                    TypeKind::Union(union) => {
                        unions.push(
                            union
                                .members
                                .into_iter()
                                .map(|member| member.node.to_string())
                                .collect(),
                        );
                    }
                    _ => {}
                }
            }
            TypeSystemDefinition::Directive(_) => {}
        }
    }

    let roots = declared_roots.unwrap_or_else(|| {
        ["Query", "Mutation", "Subscription"]
            .into_iter()
            .map(String::from)
            .filter(|name| objects.contains_key(name))
            .collect()
    });

    let mut root_fields = Vec::new();
    let mut object_types = Vec::new();
    for (name, fields) in objects {
        if roots.contains(&name) {
            root_fields.extend(fields.into_iter().filter(|field| !field.starts_with("__")));
        } else {
            object_types.push(name);
        }
    }
    Ok(ParsedSchema {
        root_fields,
        object_types,
        reactive_envelopes: reactive_envelopes(&unions),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_string_description_that_wraps_onto_a_field_shaped_line_is_not_a_root_field() {
        let sdl = "\"\"\"\nDescribes the widget query (see the manual).\nfoo: not a field\n\"\"\"\n\
                   type Query {\n\twidget(id: UUID!): WidgetView\n}\n\
                   type WidgetView {\n\tid: UUID!\n}\n";
        let parsed = parse_schema_members(sdl).expect("SDL with a block string parses");
        assert_eq!(parsed.root_fields, vec!["widget".to_string()]);
        assert_eq!(parsed.object_types, vec!["WidgetView".to_string()]);
    }

    #[test]
    fn a_field_description_wrapping_to_a_colon_line_inside_the_root_is_not_a_second_field() {
        let sdl = "type Query {\n\t\"\"\"\n\tThe board this query returns\n\tname: the board name\n\t\"\"\"\n\
                   \tboard(id: UUID!): BoardView\n}\ntype BoardView {\n\tid: UUID!\n}\n";
        let parsed = parse_schema_members(sdl).expect("wrapped field description parses");
        assert_eq!(parsed.root_fields, vec!["board".to_string()]);
    }

    #[test]
    fn root_fields_are_read_from_the_named_root_the_schema_block_points_at() {
        let sdl = "schema {\n\tquery: QueryRoot\n}\ntype QueryRoot {\n\twidget(id: UUID!): WidgetView\n}\n\
                   type WidgetView {\n\tid: UUID!\n}\n";
        let parsed = parse_schema_members(sdl).expect("schema-block SDL parses");
        assert_eq!(parsed.root_fields, vec!["widget".to_string()]);
        assert_eq!(parsed.object_types, vec!["WidgetView".to_string()]);
    }

    #[test]
    fn the_engine_injected_set_is_derived_from_the_engine_wrapper_types() {
        let injected = engine_injected_object_types();
        assert!(injected.contains("MutationAck"));
        assert!(injected.contains("LanesPaused"));
        assert!(injected.contains("LanesResumed"));
    }

    #[test]
    fn a_delta_union_with_both_lane_notices_yields_its_object_members_as_envelopes() {
        let sdl = "type Subscription {\n\twidgets: WidgetDelta\n}\n\
                   union WidgetDelta = WidgetReset | WidgetUpsert | WidgetRemove | LanesPaused | LanesResumed\n\
                   type WidgetReset {\n\trevision: Int!\n}\ntype WidgetUpsert {\n\trevision: Int!\n}\n\
                   type WidgetRemove {\n\trevision: Int!\n}\n\
                   type LanesPaused {\n\tid: Int!\n}\ntype LanesResumed {\n\tid: Int!\n}\n";
        let parsed = parse_schema_members(sdl).expect("a delta union parses");
        assert!(parsed.reactive_envelopes.contains("WidgetReset"));
        assert!(parsed.reactive_envelopes.contains("WidgetUpsert"));
        assert!(parsed.reactive_envelopes.contains("WidgetRemove"));
    }

    #[test]
    fn a_view_union_without_the_lane_notices_yields_no_envelopes() {
        let sdl = "type Query {\n\twidget: WidgetView\n}\n\
                   union ProjectedView = WidgetView | AssignmentView\n\
                   type WidgetView {\n\tid: Int!\n}\ntype AssignmentView {\n\tid: Int!\n}\n";
        let parsed = parse_schema_members(sdl).expect("a view union parses");
        assert!(parsed.reactive_envelopes.is_empty());
    }
}
