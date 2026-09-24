use super::*;
use crate::error::CompositionError;

fn prefix() -> RootPrefix {
    RootPrefix::from_snake("sample").expect("sample is a valid prefix")
}

fn fragment(slice: &'static str, root_fields: &[&str], owned_types: &[&str]) -> SliceFragment {
    SliceFragment::from_claims(
        slice,
        root_fields.iter().map(|f| f.to_string()).collect(),
        owned_types.iter().map(|t| t.to_string()).collect(),
    )
}

#[test]
fn two_slices_with_disjoint_fields_and_types_assemble() {
    SchemaSlices::assemble(
        &[
            fragment(
                "widget",
                &["sampleWidget", "sampleCloseWidget"],
                &["WidgetView"],
            ),
            fragment("assignment", &["sampleAssignment"], &["AssignmentView"]),
        ],
        Some(&prefix()),
    )
    .expect("disjoint slices assemble");
}

#[test]
fn a_root_field_claimed_by_two_slices_fails_loud_with_both_names() {
    let clash = SchemaSlices::assemble(
        &[
            fragment("widget", &["sampleWidget"], &["WidgetView"]),
            fragment("shadow", &["sampleWidget"], &["ShadowView"]),
        ],
        Some(&prefix()),
    );
    assert!(matches!(
        clash,
        Err(EngineError::Composition(CompositionError::DuplicateSchemaMember {
            kind: "root field",
            first: "widget",
            second: "shadow",
            ref member,
        })) if member == "sampleWidget"
    ));
}

#[test]
fn two_distinct_aggregates_claiming_one_type_fail_loud() {
    let clash = SchemaSlices::assemble(
        &[
            fragment("widget", &["sampleWidget"], &["View"]),
            fragment("assignment", &["sampleAssignment"], &["View"]),
        ],
        Some(&prefix()),
    );
    assert!(matches!(
        clash,
        Err(EngineError::Composition(CompositionError::DuplicateSchemaMember {
            kind: "type",
            first: "widget",
            second: "assignment",
            ref member,
        })) if member == "View"
    ));
}

#[test]
fn capabilities_of_one_aggregate_may_share_an_owned_type() {
    SchemaSlices::assemble(
        &[
            fragment("card", &["sampleCard", "sampleAdvanceCard"], &["CardView"]),
            fragment(
                "card",
                &["sampleCards", "sampleCardHistory"],
                &["CardView", "CardHistory"],
            ),
        ],
        Some(&prefix()),
    )
    .expect("two capabilities of one aggregate share CardView");
}

#[test]
fn fragments_registered_with_no_prefix_declared_is_rejected() {
    let err = SchemaSlices::assemble(
        &[fragment("widget", &["sampleWidget"], &["WidgetView"])],
        None,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        EngineError::Composition(CompositionError::RootPrefixUndeclared)
    ));
}

#[test]
fn a_fragment_root_field_outside_the_prefix_fails_loud_with_slice_field_and_prefix() {
    let err = SchemaSlices::assemble(
        &[fragment("widget", &["widget"], &["WidgetView"])],
        Some(&prefix()),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        EngineError::Composition(CompositionError::RootFieldOutsidePrefix { slice: "widget", ref field, ref prefix })
            if field == "widget" && prefix == "sample"
    ));
}

#[test]
fn a_fragment_root_field_equal_to_the_bare_prefix_is_not_owned() {
    let err = SchemaSlices::assemble(
        &[fragment("widget", &["sample"], &["WidgetView"])],
        Some(&prefix()),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        EngineError::Composition(CompositionError::RootFieldOutsidePrefix { ref field, .. }) if field == "sample"
    ));
}

#[test]
fn a_root_field_the_schema_exposes_but_no_slice_declared_is_rejected() {
    let slices = SchemaSlices::assemble(
        &[fragment("widget", &["sampleWidget"], &["WidgetView"])],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Query {\n\tsampleWidget(id: UUID!): WidgetView\n\t\
               sampleAssignment(id: UUID!): AssignmentView\n}\n\
               type WidgetView {\n\tid: UUID!\n}\ntype AssignmentView {\n\tid: UUID!\n}\n";
    let err = slices.verify(sdl).unwrap_err();
    assert!(
        matches!(err, EngineError::Composition(CompositionError::UndeclaredSchemaMember { ref member }) if member == "sampleAssignment")
    );
}

#[test]
fn verify_refuses_an_sdl_root_field_outside_the_prefix() {
    let slices = SchemaSlices::assemble(
        &[fragment("widget", &["sampleWidget"], &["WidgetView"])],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Query {\n\tsampleWidget(id: UUID!): WidgetView\n\t\
               widget(id: UUID!): WidgetView\n}\n\
               type WidgetView {\n\tid: UUID!\n}\n";
    let err = slices.verify(sdl).unwrap_err();
    assert!(matches!(
        err,
        EngineError::Composition(CompositionError::RootFieldOutsidePrefix { ref field, .. }) if field == "widget"
    ));
}

#[test]
fn an_object_type_the_schema_exposes_but_no_fragment_owns_fails_the_gate_loud() {
    let slices = SchemaSlices::assemble(
        &[fragment(
            "widget",
            &["sampleWidget", "samplePeek"],
            &["WidgetView"],
        )],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Query {\n\tsampleWidget(id: UUID!): WidgetView\n\tsamplePeek: AssignmentView\n}\n\
               type WidgetView {\n\tid: UUID!\n}\ntype AssignmentView {\n\tid: UUID!\n}\n";
    let err = slices.verify(sdl).unwrap_err();
    assert!(
        matches!(err, EngineError::Composition(CompositionError::UndeclaredSchemaType { ref ty }) if ty == "AssignmentView"),
        "an unclaimed object type fails the boot gate loud: {err:?}"
    );
}

#[test]
fn engine_injected_object_types_need_no_fragment_claim() {
    let slices = SchemaSlices::assemble(
        &[fragment("widget", &["sampleCloseWidget"], &[])],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Mutation {\n\tsampleCloseWidget(id: UUID!): MutationAck!\n}\n\
               type MutationAck {\n\tsuccess: Boolean!\n}\n";
    slices
        .verify(sdl)
        .expect("engine-injected types are allowed unclaimed");
}

#[test]
fn a_schema_whose_members_are_all_declared_or_engine_injected_verifies() {
    let slices = SchemaSlices::assemble(
        &[fragment(
            "widget",
            &["sampleWidget", "sampleCloseWidget"],
            &["WidgetView"],
        )],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Query {\n\tsampleWidget(id: UUID!): WidgetView\n}\n\
               type Mutation {\n\tsampleCloseWidget(id: UUID!): MutationAck!\n}\n\
               type WidgetView {\n\tid: UUID!\n}\ntype MutationAck {\n\tsuccess: Boolean!\n}\n";
    slices
        .verify(sdl)
        .expect("declared and injected members verify");
}

#[test]
fn subscription_delta_envelope_types_are_exempt_without_a_fragment_claim() {
    let slices = SchemaSlices::assemble(
        &[fragment("widget", &["sampleWidgets"], &["WidgetView"])],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Subscription {\n\tsampleWidgets: WidgetDelta\n}\n\
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
    let slices = SchemaSlices::assemble(
        &[
            fragment("widget", &["sampleWidgets"], &["WidgetView"]),
            fragment("assignment", &["sampleAssignments"], &["AssignmentView"]),
        ],
        Some(&prefix()),
    )
    .unwrap();
    let sdl = "type Subscription {\n\tsampleWidgets: EngineDelta\n\tsampleAssignments: EngineDelta\n}\n\
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
