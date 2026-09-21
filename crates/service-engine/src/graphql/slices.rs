use std::collections::BTreeMap;

use async_graphql::{ObjectType, SubscriptionType};

use crate::error::EngineError;
use crate::graphql::prefix::RootPrefix;
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
    prefix: Option<RootPrefix>,
}

impl SchemaSlices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn assemble(
        fragments: &[SliceFragment],
        prefix: Option<&RootPrefix>,
    ) -> Result<Self, EngineError> {
        if !fragments.is_empty() && prefix.is_none() {
            return Err(EngineError::RootPrefixUndeclared);
        }
        let mut slices = Self::new();
        slices.prefix = prefix.cloned();
        for fragment in fragments {
            slices.add(fragment)?;
        }
        Ok(slices)
    }

    pub fn add(&mut self, fragment: &SliceFragment) -> Result<(), EngineError> {
        for field in &fragment.root_fields {
            self.check_owned(fragment.slice, field)?;
            claim_field(&mut self.fields, field, fragment.slice)?;
        }
        for ty in &fragment.owned_types {
            claim_type(&mut self.types, ty, fragment.slice)?;
        }
        Ok(())
    }

    fn check_owned(&self, slice: &'static str, field: &str) -> Result<(), EngineError> {
        if let Some(prefix) = &self.prefix
            && !prefix.owns(field)
        {
            return Err(EngineError::RootFieldOutsidePrefix {
                slice,
                field: field.to_string(),
                prefix: prefix.as_str().to_string(),
            });
        }
        Ok(())
    }

    pub fn verify(&self, sdl: &str) -> Result<(), EngineError> {
        let parsed = sdl::parse_schema_members(sdl)?;
        for field in parsed.root_fields {
            if field.starts_with("__") {
                continue;
            }
            self.check_owned("<schema sdl>", &field)?;
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
mod tests;
