use std::collections::BTreeMap;

use crate::error::EngineError;

pub struct SliceFragment {
    pub slice: &'static str,
    pub root_fields: &'static [&'static str],
    pub types: &'static [&'static str],
}

impl SliceFragment {
    pub fn new(
        slice: &'static str,
        root_fields: &'static [&'static str],
        types: &'static [&'static str],
    ) -> Self {
        Self {
            slice,
            root_fields,
            types,
        }
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

    pub fn add(&mut self, fragment: SliceFragment) -> Result<(), EngineError> {
        for field in fragment.root_fields {
            claim(&mut self.fields, "root field", field, fragment.slice)?;
        }
        for ty in fragment.types {
            claim(&mut self.types, "type", ty, fragment.slice)?;
        }
        Ok(())
    }
}

fn claim(
    registry: &mut BTreeMap<String, &'static str>,
    kind: &'static str,
    member: &str,
    slice: &'static str,
) -> Result<(), EngineError> {
    if let Some(first) = registry.get(member) {
        return Err(EngineError::DuplicateSchemaMember {
            kind,
            member: member.to_string(),
            first,
            second: slice,
        });
    }
    registry.insert(member.to_string(), slice);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_slices_with_disjoint_fields_and_types_assemble() {
        let mut slices = SchemaSlices::new();
        slices
            .add(SliceFragment::new(
                "widget",
                &["widget", "closeWidget"],
                &["WidgetView"],
            ))
            .unwrap();
        slices
            .add(SliceFragment::new(
                "assignment",
                &["assignment"],
                &["AssignmentView"],
            ))
            .unwrap();
    }

    #[test]
    fn a_root_field_claimed_by_two_slices_fails_loud_with_both_names() {
        let mut slices = SchemaSlices::new();
        slices
            .add(SliceFragment::new("widget", &["widget"], &["WidgetView"]))
            .unwrap();
        let clash = slices.add(SliceFragment::new("shadow", &["widget"], &["ShadowView"]));
        assert!(matches!(
            clash,
            Err(EngineError::DuplicateSchemaMember {
                kind: "root field",
                first: "widget",
                second: "shadow",
                ..
            })
        ));
    }

    #[test]
    fn a_type_claimed_by_two_slices_fails_loud() {
        let mut slices = SchemaSlices::new();
        slices
            .add(SliceFragment::new("widget", &["widget"], &["View"]))
            .unwrap();
        let clash = slices.add(SliceFragment::new("assignment", &["assignment"], &["View"]));
        assert!(matches!(
            clash,
            Err(EngineError::DuplicateSchemaMember { kind: "type", .. })
        ));
    }
}
