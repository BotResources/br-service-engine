use service_engine::{EngineError, SchemaSlices, SliceFragment};

#[test]
fn s66_two_slices_contributing_the_same_root_field_fail_boot_assembly_loud() {
    let mut slices = SchemaSlices::new();
    slices
        .add(SliceFragment::new(
            "widget",
            &["widget", "closeWidget"],
            &["WidgetView"],
        ))
        .expect("the first slice assembles");

    let collision = slices.add(SliceFragment::new("shadow", &["widget"], &["ShadowView"]));

    assert!(
        matches!(
            collision,
            Err(EngineError::DuplicateSchemaMember {
                kind: "root field",
                first: "widget",
                second: "shadow",
                ref member,
            }) if member == "widget"
        ),
        "a duplicate root field across two slices is named and rejected before the schema builds: {collision:?}"
    );
}

#[test]
fn s66_two_slices_contributing_the_same_type_fail_boot_assembly_loud() {
    let mut slices = SchemaSlices::new();
    slices
        .add(SliceFragment::new("widget", &["widget"], &["View"]))
        .expect("the first slice assembles");

    let collision = slices.add(SliceFragment::new("assignment", &["assignment"], &["View"]));

    assert!(
        matches!(
            collision,
            Err(EngineError::DuplicateSchemaMember { kind: "type", .. })
        ),
        "a duplicate graphql type across two slices fails loud too: {collision:?}"
    );
}
