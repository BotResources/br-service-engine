mod handshake;
mod wire;

use br_core_scope::{
    KeyValidationError, ScopeDeclaration, ScopeDeclarationError, ScopeKey, ScopeSpec, ServiceKey,
    ServiceManifest,
};

use crate::error::BoxedError;

pub(crate) use handshake::run_handshake;

pub const REASON_SCOPES_PENDING: &str = "declaring scopes to identity";
pub const REASON_SCOPES_FAILED: &str = "the scope-declaration handshake could not reach identity";

/// One scope as a host declares it to Identity: its key, the i18n keys of its
/// label and description, and whether only the platform may hold it.
/// `ScopeDef::bare(key)` is what a bare key declares: both i18n keys are the
/// scope key, and `platform_only` is false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeDef {
    pub key: &'static str,
    pub label_key: &'static str,
    pub description_key: &'static str,
    pub platform_only: bool,
}

impl ScopeDef {
    pub const fn new(
        key: &'static str,
        label_key: &'static str,
        description_key: &'static str,
    ) -> Self {
        Self {
            key,
            label_key,
            description_key,
            platform_only: false,
        }
    }

    pub const fn bare(key: &'static str) -> Self {
        Self::new(key, key, key)
    }

    pub const fn platform_only(self) -> Self {
        Self {
            platform_only: true,
            ..self
        }
    }
}

/// The i18n keys of the declaring service's own label and description. Absent,
/// both are the service key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeServiceLabels {
    pub label_key: &'static str,
    pub description_key: &'static str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScopeManifest {
    scopes: Vec<&'static str>,
    defs: Vec<ScopeDef>,
    service: Option<ScopeServiceLabels>,
}

impl ScopeManifest {
    pub fn of(groups: &[&[&'static str]]) -> Self {
        let defs: Vec<ScopeDef> = groups
            .iter()
            .flat_map(|group| group.iter().copied().map(ScopeDef::bare))
            .collect();
        Self::from_defs(defs)
    }

    pub fn of_defs(groups: &[&[ScopeDef]]) -> Self {
        let defs: Vec<ScopeDef> = groups
            .iter()
            .flat_map(|group| group.iter().copied())
            .collect();
        Self::from_defs(defs)
    }

    fn from_defs(defs: Vec<ScopeDef>) -> Self {
        Self {
            scopes: defs.iter().map(|def| def.key).collect(),
            defs,
            service: None,
        }
    }

    pub fn with_service_labels(
        mut self,
        label_key: &'static str,
        description_key: &'static str,
    ) -> Self {
        self.service = Some(ScopeServiceLabels {
            label_key,
            description_key,
        });
        self
    }

    pub fn scopes(&self) -> &[&'static str] {
        &self.scopes
    }

    pub fn defs(&self) -> &[ScopeDef] {
        &self.defs
    }

    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }

    pub(crate) fn declaration(&self) -> Result<ScopeDeclaration, ScopeError> {
        if self.defs.is_empty() {
            return Err(ScopeError::Empty);
        }
        let mut seen: Vec<ScopeDef> = Vec::new();
        let mut keys: Vec<(ScopeKey, ScopeDef)> = Vec::new();
        for def in &self.defs {
            if let Some(earlier) = seen.iter().find(|earlier| earlier.key == def.key) {
                if earlier == def {
                    continue;
                }
                return Err(ScopeError::ConflictingScope {
                    key: def.key.to_string(),
                });
            }
            seen.push(*def);
            let key = ScopeKey::new(def.key).map_err(|validation| ScopeError::MalformedKey {
                key: def.key.to_string(),
                validation,
            })?;
            keys.push((key, *def));
        }
        let service_segment = keys[0].0.service_segment().to_string();
        if let Some((other, _)) = keys
            .iter()
            .find(|(key, _)| key.service_segment() != service_segment)
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
        let manifest = match self.service {
            Some(labels) => {
                ServiceManifest::new(service.clone(), labels.label_key, labels.description_key)
            }
            None => ServiceManifest::new(service.clone(), service.as_str(), service.as_str()),
        };
        let specs = keys
            .into_iter()
            .map(|(key, def)| {
                ScopeSpec::new(key, def.label_key, def.description_key, def.platform_only)
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
        "scope {key} is declared twice with different labels or platform_only; one key has one spec"
    )]
    ConflictingScope { key: String },

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
    fn a_bare_key_declares_its_key_as_both_i18n_keys_and_no_platform_restriction() {
        let declaration = ScopeManifest::of(&[&["project:read"]])
            .declaration()
            .expect("a bare key declares");
        assert_eq!(declaration.manifest().label_key, "project");
        assert_eq!(declaration.manifest().description_key, "project");
        let spec = &declaration.scopes()[0];
        assert_eq!(spec.label_key, "project:read");
        assert_eq!(spec.description_key, "project:read");
        assert!(!spec.platform_only);
    }

    #[test]
    fn a_full_spec_reaches_the_declaration_unchanged() {
        const READ: ScopeDef = ScopeDef::new(
            "project:read",
            "scopes.project.read.label",
            "scopes.project.read.description",
        );
        const ADMIN: ScopeDef = ScopeDef::new(
            "project:admin",
            "scopes.project.admin.label",
            "scopes.project.admin.description",
        )
        .platform_only();
        let declaration = ScopeManifest::of_defs(&[&[READ], &[ADMIN]])
            .with_service_labels("services.project.label", "services.project.description")
            .declaration()
            .expect("a full-spec manifest declares");
        assert_eq!(declaration.manifest().key.as_str(), "project");
        assert_eq!(declaration.manifest().label_key, "services.project.label");
        assert_eq!(
            declaration.manifest().description_key,
            "services.project.description"
        );
        let specs: Vec<(&str, &str, &str, bool)> = declaration
            .scopes()
            .iter()
            .map(|spec| {
                (
                    spec.key.as_str(),
                    spec.label_key.as_str(),
                    spec.description_key.as_str(),
                    spec.platform_only,
                )
            })
            .collect();
        assert_eq!(
            specs,
            [
                (
                    "project:read",
                    "scopes.project.read.label",
                    "scopes.project.read.description",
                    false
                ),
                (
                    "project:admin",
                    "scopes.project.admin.label",
                    "scopes.project.admin.description",
                    true
                ),
            ]
        );
    }

    #[test]
    fn one_key_with_two_different_specs_is_refused() {
        let manifest = ScopeManifest::of_defs(&[
            &[ScopeDef::bare("project:read")],
            &[ScopeDef::bare("project:read").platform_only()],
        ]);
        match manifest.declaration() {
            Err(ScopeError::ConflictingScope { key }) => assert_eq!(key, "project:read"),
            other => panic!("expected a conflicting-scope error, got {other:?}"),
        }
    }

    #[test]
    fn full_specs_keep_the_checks_of_bare_keys() {
        assert!(matches!(
            ScopeManifest::of_defs(&[]).declaration(),
            Err(ScopeError::Empty)
        ));
        assert!(matches!(
            ScopeManifest::of_defs(&[&[
                ScopeDef::bare("project:read"),
                ScopeDef::bare("billing:read")
            ]])
            .declaration(),
            Err(ScopeError::MixedServices { .. })
        ));
        assert!(matches!(
            ScopeManifest::of_defs(&[&[ScopeDef::new("project:Read", "l", "d")]]).declaration(),
            Err(ScopeError::MalformedKey { .. })
        ));
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
