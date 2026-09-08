mod handshake;
mod wire;

use std::collections::HashSet;

use br_core_scope::{
    KeyValidationError, ScopeDeclaration, ScopeDeclarationError, ScopeKey, ScopeSpec, ServiceKey,
    ServiceManifest,
};

use crate::error::BoxedError;

pub(crate) use handshake::run_handshake;

pub const REASON_SCOPES_PENDING: &str = "declaring scopes to identity";
pub const REASON_SCOPES_FAILED: &str = "the scope-declaration handshake could not reach identity";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeManifest {
    scopes: Vec<&'static str>,
}

impl ScopeManifest {
    pub fn of(groups: &[&[&'static str]]) -> Self {
        Self {
            scopes: groups
                .iter()
                .flat_map(|group| group.iter().copied())
                .collect(),
        }
    }

    pub fn scopes(&self) -> &[&'static str] {
        &self.scopes
    }

    pub fn is_empty(&self) -> bool {
        self.scopes.is_empty()
    }

    pub(crate) fn declaration(&self) -> Result<ScopeDeclaration, ScopeError> {
        if self.scopes.is_empty() {
            return Err(ScopeError::Empty);
        }
        let mut seen = HashSet::new();
        let mut keys: Vec<ScopeKey> = Vec::new();
        for raw in &self.scopes {
            if !seen.insert(*raw) {
                continue;
            }
            let key = ScopeKey::new(*raw).map_err(|validation| ScopeError::MalformedKey {
                key: (*raw).to_string(),
                validation,
            })?;
            keys.push(key);
        }
        let service_segment = keys[0].service_segment().to_string();
        if let Some(other) = keys
            .iter()
            .find(|key| key.service_segment() != service_segment)
        {
            return Err(ScopeError::MixedServices {
                first: service_segment,
                other: other.service_segment().to_string(),
            });
        }
        let service = ServiceKey::new(service_segment.clone()).map_err(|validation| {
            ScopeError::MalformedService {
                service: service_segment,
                validation,
            }
        })?;
        let manifest = ServiceManifest::new(service.clone(), service.as_str(), service.as_str());
        let specs = keys
            .into_iter()
            .map(|key| {
                let label = key.as_str().to_string();
                ScopeSpec::new(key, label.clone(), label, false)
            })
            .collect();
        ScopeDeclaration::new(manifest, specs).map_err(ScopeError::Declaration)
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScopeError {
    #[error(
        "an empty manifest cannot be declared; a scopeless service does not call declare_scopes"
    )]
    Empty,

    #[error("scope key {key} is malformed")]
    MalformedKey {
        key: String,
        #[source]
        validation: KeyValidationError,
    },

    #[error(
        "the manifest mixes scopes of {first} and {other}; a service declares only its own scopes"
    )]
    MixedServices { first: String, other: String },

    #[error("the service key {service} derived from the manifest is invalid")]
    MalformedService {
        service: String,
        #[source]
        validation: KeyValidationError,
    },

    #[error(transparent)]
    Declaration(#[from] ScopeDeclarationError),

    #[error("{reason}")]
    Rejected { reason: String },

    #[error("the scope-declaration handshake failed")]
    Handshake(#[source] BoxedError),
}

impl ScopeError {
    pub(crate) fn readiness_reason(&self) -> String {
        match self {
            ScopeError::Rejected { reason } => reason.clone(),
            _ => REASON_SCOPES_FAILED.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manifest_builds_a_declaration_that_carries_every_scope_under_one_service() {
        let manifest =
            ScopeManifest::of(&[&["project:read", "project:archive"], &["project:write"]]);
        let declaration = manifest
            .declaration()
            .expect("a single-service manifest declares");
        assert_eq!(declaration.manifest().key.as_str(), "project");
        let keys: Vec<&str> = declaration
            .scopes()
            .iter()
            .map(|spec| spec.key.as_str())
            .collect();
        assert_eq!(keys, ["project:read", "project:archive", "project:write"]);
    }

    #[test]
    fn duplicate_scope_strings_collapse_before_the_declaration_rejects_them() {
        let manifest = ScopeManifest::of(&[&["project:read"], &["project:read"]]);
        let declaration = manifest
            .declaration()
            .expect("an identical repeat is deduped, not a duplicate-key rejection");
        assert_eq!(declaration.scopes().len(), 1);
    }

    #[test]
    fn an_empty_manifest_is_a_misuse_not_a_scopeless_declaration() {
        assert!(matches!(
            ScopeManifest::of(&[]).declaration(),
            Err(ScopeError::Empty)
        ));
    }

    #[test]
    fn a_manifest_that_spans_two_services_is_refused_before_the_wire() {
        let manifest = ScopeManifest::of(&[&["project:read", "billing:read"]]);
        assert!(matches!(
            manifest.declaration(),
            Err(ScopeError::MixedServices { .. })
        ));
    }

    #[test]
    fn a_malformed_scope_key_is_named_with_its_validation() {
        let manifest = ScopeManifest::of(&[&["project:Read"]]);
        match manifest.declaration() {
            Err(ScopeError::MalformedKey { key, .. }) => assert_eq!(key, "project:Read"),
            other => panic!("expected a malformed-key error, got {other:?}"),
        }
    }

    #[test]
    fn a_rejection_reason_is_what_readiness_reports() {
        let error = ScopeError::Rejected {
            reason: "scope declaration rejected: scope_prefix_mismatch".to_string(),
        };
        assert_eq!(
            error.readiness_reason(),
            "scope declaration rejected: scope_prefix_mismatch"
        );
    }

    #[test]
    fn a_handshake_failure_never_leaks_below_the_static_reason() {
        let error = ScopeError::Handshake("nats://someone:hunter2@10.0.0.4 is unreachable".into());
        assert_eq!(error.readiness_reason(), REASON_SCOPES_FAILED);
        assert!(!error.readiness_reason().contains("hunter2"));
    }
}
