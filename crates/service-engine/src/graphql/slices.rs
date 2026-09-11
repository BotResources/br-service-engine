use std::collections::{BTreeMap, BTreeSet};

use crate::error::EngineError;

#[derive(Clone, Copy)]
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

    pub fn assemble(fragments: &[SliceFragment]) -> Result<Self, EngineError> {
        let mut slices = Self::new();
        for fragment in fragments {
            slices.add(*fragment)?;
        }
        Ok(slices)
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

    pub fn verify_root_fields(&self, sdl: &str) -> Result<(), EngineError> {
        for member in root_fields_in(sdl) {
            if member.starts_with("__") {
                continue;
            }
            if !self.fields.contains_key(&member) {
                return Err(EngineError::UndeclaredSchemaMember { member });
            }
        }
        Ok(())
    }
}

fn root_fields_in(sdl: &str) -> Vec<String> {
    let roots = root_type_names(sdl);
    let mut fields = Vec::new();
    let mut lines = sdl.lines();
    while let Some(line) = lines.next() {
        let Some(name) = type_header_name(line) else {
            continue;
        };
        if !roots.contains(&name) {
            continue;
        }
        let mut depth = brace_delta(line);
        if depth == 0 {
            for header in lines.by_ref() {
                depth += header.matches('{').count() as i32;
                if depth > 0 {
                    break;
                }
            }
        }
        for body in lines.by_ref() {
            depth += brace_delta(body);
            if depth <= 0 {
                break;
            }
            if depth == 1
                && let Some(field) = field_name(body)
            {
                fields.push(field);
            }
        }
    }
    fields
}

fn brace_delta(line: &str) -> i32 {
    line.matches('{').count() as i32 - line.matches('}').count() as i32
}

fn type_header_name(line: &str) -> Option<String> {
    let mut tokens = line.split_whitespace();
    if tokens.next() != Some("type") {
        return None;
    }
    tokens
        .next()
        .map(|name| name.trim_end_matches('{').to_string())
        .filter(|name| !name.is_empty())
}

fn root_type_names(sdl: &str) -> BTreeSet<String> {
    let mut roots: BTreeSet<String> = ["Query", "Mutation", "Subscription"]
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut lines = sdl.lines();
    for line in lines.by_ref() {
        if line.split_whitespace().next() == Some("schema") {
            roots.clear();
            for entry in lines.by_ref() {
                let entry = entry.trim();
                if entry.starts_with('}') {
                    break;
                }
                for role in ["query:", "mutation:", "subscription:"] {
                    if let Some(rest) = entry.strip_prefix(role) {
                        roots.insert(rest.trim().trim_end_matches([',', '{']).trim().to_string());
                    }
                }
            }
            break;
        }
    }
    roots
}

fn field_name(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let name: String = trimmed
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        return None;
    }
    let rest = trimmed[name.len()..].trim_start();
    (rest.starts_with('(') || rest.starts_with(':')).then_some(name)
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

    #[test]
    fn a_root_field_the_schema_exposes_but_no_slice_declared_is_rejected() {
        let slices =
            SchemaSlices::assemble(&[SliceFragment::new("widget", &["widget"], &["WidgetView"])])
                .unwrap();
        let sdl = "type Query {\n\twidget(id: UUID!): WidgetView\n\t\
                   assignment(id: UUID!): AssignmentView\n}\n";
        let err = slices.verify_root_fields(sdl).unwrap_err();
        assert!(
            matches!(err, EngineError::UndeclaredSchemaMember { ref member } if member == "assignment")
        );
    }

    #[test]
    fn root_fields_are_read_from_the_named_root_types_the_schema_block_points_at() {
        let slices =
            SchemaSlices::assemble(&[SliceFragment::new("widget", &["widget"], &["WidgetView"])])
                .unwrap();
        let sdl = "schema {\n\tquery: QueryRoot\n}\ntype QueryRoot {\n\t\
                   widget(id: UUID!): WidgetView\n\tassignment(id: UUID!): AssignmentView\n}\n";
        let err = slices.verify_root_fields(sdl).unwrap_err();
        assert!(
            matches!(err, EngineError::UndeclaredSchemaMember { ref member } if member == "assignment")
        );
    }

    #[test]
    fn a_schema_whose_root_fields_are_all_declared_or_introspection_verifies() {
        let slices = SchemaSlices::assemble(&[SliceFragment::new(
            "widget",
            &["widget", "closeWidget"],
            &["WidgetView"],
        )])
        .unwrap();
        let sdl = "type Query {\n\twidget(id: UUID!): WidgetView\n\t__schema: __Schema!\n}\n\
                   type Mutation {\n\tcloseWidget(id: UUID!): Boolean\n}\n";
        slices
            .verify_root_fields(sdl)
            .expect("declared fields verify");
    }
}
