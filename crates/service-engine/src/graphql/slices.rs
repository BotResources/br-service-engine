use std::collections::BTreeMap;

use async_graphql::{ObjectType, SubscriptionType};

use crate::error::EngineError;
use crate::graphql::sdl;

#[derive(Clone, Debug)]
pub struct SliceFragment {
    slice: &'static str,
    root_fields: Vec<String>,
    owned_types: Vec<String>,
}

impl SliceFragment {
    pub fn derive<Q, M, S>(slice: &'static str) -> Self
    where
        Q: ObjectType,
        M: ObjectType,
        S: SubscriptionType,
    {
        let members = sdl::derive_members::<Q, M, S>();
        let injected = sdl::engine_injected_object_types();
        let owned_types = members
            .object_types
            .into_iter()
            .filter(|ty| !injected.contains(ty) && !members.reactive_envelopes.contains(ty))
            .collect();
        Self {
            slice,
            root_fields: members.root_fields,
            owned_types,
        }
    }

    pub fn from_claims(
        slice: &'static str,
        root_fields: Vec<String>,
        owned_types: Vec<String>,
    ) -> Self {
        Self {
            slice,
            root_fields,
            owned_types,
        }
    }

    pub fn slice(&self) -> &'static str {
        self.slice
    }
}

#[derive(Debug, Default)]
pub struct SchemaSlices {
    fields: BTreeMap<String, &'static str>,
    types: BTreeMap<String, &'static str>,
}

impl SchemaSlices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assemble(fragments: &[SliceFragment]) -> Result<Self, EngineError> {
        let mut slices = Self::new();
        for fragment in fragments {
            slices.add(fragment)?;
        }
        Ok(slices)
    }

    pub fn add(&mut self, fragment: &SliceFragment) -> Result<(), EngineError> {
        for field in &fragment.root_fields {
            claim_field(&mut self.fields, field, fragment.slice)?;
        }
        for ty in &fragment.owned_types {
            claim_type(&mut self.types, ty, fragment.slice)?;
        }
        Ok(())
    }

    pub fn verify(&self, sdl: &str) -> Result<(), EngineError> {
        let parsed = sdl::parse_schema_members(sdl)?;
        for field in parsed.root_fields {
            if field.starts_with("__") {
                continue;
            }
            if !self.fields.contains_key(&field) {
                return Err(EngineError::UndeclaredSchemaMember { member: field });
            }
        }
        let injected = sdl::engine_injected_object_types();
        for ty in parsed.object_types {
            if injected.contains(&ty)
                || parsed.reactive_envelopes.contains(&ty)
                || self.types.contains_key(&ty)
            {
                continue;
            }
            return Err(EngineError::UndeclaredSchemaType { ty });
        }
        Ok(())
    }
}

fn claim_field(
    registry: &mut BTreeMap<String, &'static str>,
    member: &str,
    slice: &'static str,
) -> Result<(), EngineError> {
    if let Some(first) = registry.get(member) {
        return Err(EngineError::DuplicateSchemaMember {
            kind: "root field",
            member: member.to_string(),
            first,
            second: slice,
        });
    }
    registry.insert(member.to_string(), slice);
    Ok(())
}

fn claim_type(
    registry: &mut BTreeMap<String, &'static str>,
    member: &str,
    slice: &'static str,
) -> Result<(), EngineError> {
    match registry.get(member) {
        Some(&first) if first != slice => Err(EngineError::DuplicateSchemaMember {
            kind: "type",
            member: member.to_string(),
            first,
            second: slice,
        }),
        Some(_) => Ok(()),
        None => {
            registry.insert(member.to_string(), slice);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fragment(slice: &'static str, root_fields: &[&str], owned_types: &[&str]) -> SliceFragment {
        SliceFragment::from_claims(
            slice,
            root_fields.iter().map(|f| f.to_string()).collect(),
            owned_types.iter().map(|t| t.to_string()).collect(),
        )
    }

    #[test]
    fn two_slices_with_disjoint_fields_and_types_assemble() {
        SchemaSlices::assemble(&[
            fragment("widget", &["widget", "closeWidget"], &["WidgetView"]),
            fragment("assignment", &["assignment"], &["AssignmentView"]),
        ])
        .expect("disjoint slices assemble");
    }

    #[test]
    fn a_root_field_claimed_by_two_slices_fails_loud_with_both_names() {
        let clash = SchemaSlices::assemble(&[
            fragment("widget", &["widget"], &["WidgetView"]),
            fragment("shadow", &["widget"], &["ShadowView"]),
        ]);
        assert!(matches!(
            clash,
            Err(EngineError::DuplicateSchemaMember {
                kind: "root field",
                first: "widget",
                second: "shadow",
                ref member,
            }) if member == "widget"
        ));
    }

    #[test]
    fn two_distinct_aggregates_claiming_one_type_fail_loud() {
        let clash = SchemaSlices::assemble(&[
            fragment("widget", &["widget"], &["View"]),
            fragment("assignment", &["assignment"], &["View"]),
        ]);
        assert!(matches!(
            clash,
            Err(EngineError::DuplicateSchemaMember {
                kind: "type",
                first: "widget",
                second: "assignment",
                ref member,
            }) if member == "View"
        ));
    }

    #[test]
    fn capabilities_of_one_aggregate_may_share_an_owned_type() {
        SchemaSlices::assemble(&[
            fragment("card", &["card", "advanceCard"], &["CardView"]),
            fragment("card", &["cards", "pageCards"], &["CardView", "CardPage"]),
        ])
        .expect("two capabilities of one aggregate share CardView");
    }

    #[test]
    fn a_root_field_the_schema_exposes_but_no_slice_declared_is_rejected() {
        let slices =
            SchemaSlices::assemble(&[fragment("widget", &["widget"], &["WidgetView"])]).unwrap();
        let sdl = "type Query {\n\twidget(id: UUID!): WidgetView\n\t\
                   assignment(id: UUID!): AssignmentView\n}\n\
                   type WidgetView {\n\tid: UUID!\n}\ntype AssignmentView {\n\tid: UUID!\n}\n";
        let err = slices.verify(sdl).unwrap_err();
        assert!(
            matches!(err, EngineError::UndeclaredSchemaMember { ref member } if member == "assignment")
        );
    }

    #[test]
    fn an_object_type_the_schema_exposes_but_no_fragment_owns_fails_the_gate_loud() {
        let slices =
            SchemaSlices::assemble(&[fragment("widget", &["widget", "peek"], &["WidgetView"])])
                .unwrap();
        let sdl = "type Query {\n\twidget(id: UUID!): WidgetView\n\tpeek: AssignmentView\n}\n\
                   type WidgetView {\n\tid: UUID!\n}\ntype AssignmentView {\n\tid: UUID!\n}\n";
        let err = slices.verify(sdl).unwrap_err();
        assert!(
            matches!(err, EngineError::UndeclaredSchemaType { ref ty } if ty == "AssignmentView"),
            "an unclaimed object type fails the boot gate loud: {err:?}"
        );
    }

    #[test]
    fn engine_injected_object_types_need_no_fragment_claim() {
        let slices = SchemaSlices::assemble(&[fragment("widget", &["closeWidget"], &[])]).unwrap();
        let sdl = "type Mutation {\n\tcloseWidget(id: UUID!): MutationAck!\n}\n\
                   type MutationAck {\n\tsuccess: Boolean!\n}\n";
        slices
            .verify(sdl)
            .expect("engine-injected types are allowed unclaimed");
    }

    #[test]
    fn a_schema_whose_members_are_all_declared_or_engine_injected_verifies() {
        let slices = SchemaSlices::assemble(&[fragment(
            "widget",
            &["widget", "closeWidget"],
            &["WidgetView"],
        )])
        .unwrap();
        let sdl = "type Query {\n\twidget(id: UUID!): WidgetView\n}\n\
                   type Mutation {\n\tcloseWidget(id: UUID!): MutationAck!\n}\n\
                   type WidgetView {\n\tid: UUID!\n}\ntype MutationAck {\n\tsuccess: Boolean!\n}\n";
        slices
            .verify(sdl)
            .expect("declared and injected members verify");
    }

    #[test]
    fn subscription_delta_envelope_types_are_exempt_without_a_fragment_claim() {
        let slices =
            SchemaSlices::assemble(&[fragment("widget", &["widgets"], &["WidgetView"])]).unwrap();
        let sdl = "type Subscription {\n\twidgets: WidgetDelta\n}\n\
                   union WidgetDelta = WidgetReset | WidgetUpsert | WidgetRemove | LanesPaused | LanesResumed\n\
                   union ProjectedView = WidgetView\n\
                   type WidgetView {\n\tid: UUID!\n}\n\
                   type WidgetReset {\n\trevision: Int!\n}\ntype WidgetUpsert {\n\trevision: Int!\n}\n\
                   type WidgetRemove {\n\trevision: Int!\n}\n\
                   type LanesPaused {\n\tid: Int!\n}\ntype LanesResumed {\n\tid: Int!\n}\n";
        slices
            .verify(sdl)
            .expect("delta envelope object types need no fragment claim");
    }

    #[test]
    fn two_subscription_slices_sharing_one_delta_union_verify_without_a_synthetic_slice() {
        let slices = SchemaSlices::assemble(&[
            fragment("widget", &["widgets"], &["WidgetView"]),
            fragment("assignment", &["assignments"], &["AssignmentView"]),
        ])
        .unwrap();
        let sdl = "type Subscription {\n\twidgets: EngineDelta\n\tassignments: EngineDelta\n}\n\
                   union EngineDelta = ResetPayload | UpsertPayload | RemovePayload | LanesPaused | LanesResumed\n\
                   union ProjectedView = WidgetView | AssignmentView\n\
                   type WidgetView {\n\tid: UUID!\n}\ntype AssignmentView {\n\tid: UUID!\n}\n\
                   type ResetPayload {\n\trevision: Int!\n}\ntype UpsertPayload {\n\trevision: Int!\n}\n\
                   type RemovePayload {\n\trevision: Int!\n}\n\
                   type LanesPaused {\n\tid: Int!\n}\ntype LanesResumed {\n\tid: Int!\n}\n";
        slices
            .verify(sdl)
            .expect("one shared delta union across two slices needs no reactive slice");
    }
}
