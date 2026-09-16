use std::collections::BTreeMap;

use async_graphql::{ObjectType, SubscriptionType};

use crate::error::EngineError;
use crate::graphql::sdl;

/// One capability's contribution to a service's GraphQL surface: the root
/// fields it adds and the object types it owns.
///
/// A fragment is normally *derived* from its async-graphql root objects with
/// [`SliceFragment::derive`] — the root fields and owned object types are read
/// from the `#[Object]`/`#[SimpleObject]` impls, never restated by hand. A
/// slice may register several fragments, one per capability file, all sharing
/// one aggregate name; capabilities of the same aggregate may reference the
/// same owned type, while two *different* aggregates claiming one type name is
/// the collision the boot gate refuses.
///
/// [`SliceFragment::from_claims`] is the lower-level primitive `derive` is built
/// on, for the rare fragment whose claims cannot be read from a real schema
/// (synthetic test fixtures, chiefly). Prefer `derive` in service code.
#[derive(Clone, Debug)]
pub struct SliceFragment {
    slice: &'static str,
    root_fields: Vec<String>,
    owned_types: Vec<String>,
}

impl SliceFragment {
    /// Derive a fragment from its root objects. `Q`, `M`, `S` are the query,
    /// mutation and subscription roots of one capability; use
    /// `async_graphql::EmptyMutation` / `async_graphql::EmptySubscription` for a
    /// slot the capability does not populate.
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
            .filter(|ty| !injected.contains(ty))
            .collect();
        Self {
            slice,
            root_fields: members.root_fields,
            owned_types,
        }
    }

    /// Construct a fragment from explicit claims. The primitive behind
    /// [`SliceFragment::derive`]; prefer `derive` unless the claims genuinely
    /// cannot be read from a schema.
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

    /// Gate a composed schema against the registered fragments: every root
    /// field and every object type the schema exposes must be claimed by a
    /// fragment, or (for object types) injected by the engine. A member with no
    /// claim is a seam a slice wired into the composed roots but never
    /// registered — the boot fails loud rather than serving it unowned.
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
            if injected.contains(&ty) || self.types.contains_key(&ty) {
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
        // Two distinct aggregates naming the same type is the real collision.
        Some(&first) if first != slice => Err(EngineError::DuplicateSchemaMember {
            kind: "type",
            member: member.to_string(),
            first,
            second: slice,
        }),
        // Capabilities of the same aggregate share its types — claim once.
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
        // The root field `peek` is claimed, but the object type it returns,
        // `AssignmentView`, is owned by no fragment — the seam of a slice
        // merged into the roots yet never registered.
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
        // `MutationAck` is engine-injected: the schema exposes it, no fragment
        // claims it, and the gate passes.
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
}
