use std::any::TypeId;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::erase::{ErasedProjector, erase_projector};
use crate::error::EngineError;
use crate::name::{NounName, ProjectorName};
use crate::principal::{Principal, PrincipalResolver, RlsApplier};
use crate::projector::Projector;
use crate::wire::Noun;

const NO_PROJECTORS: &[ProjectorName] = &[];

pub struct RenderRegistry<P: Principal> {
    projectors: BTreeMap<ProjectorName, Arc<dyn ErasedProjector<P>>>,
    by_noun: BTreeMap<NounName, Vec<ProjectorName>>,
    key_types: BTreeMap<NounName, TypeId>,
    rls: Option<Arc<dyn RlsApplier<P>>>,
    resolver: Option<Arc<dyn PrincipalResolver<P>>>,
}

impl<P: Principal> Default for RenderRegistry<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Principal> std::fmt::Debug for RenderRegistry<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenderRegistry")
            .field("projectors", &self.projectors.keys().collect::<Vec<_>>())
            .field("nouns", &self.key_types.keys().collect::<Vec<_>>())
            .field("rls", &self.rls.is_some())
            .field("resolver", &self.resolver.is_some())
            .finish()
    }
}

impl<P: Principal> RenderRegistry<P> {
    pub fn new() -> Self {
        Self {
            projectors: BTreeMap::new(),
            by_noun: BTreeMap::new(),
            key_types: BTreeMap::new(),
            rls: None,
            resolver: None,
        }
    }

    pub fn bind_noun<N: Noun>(&mut self) {
        self.key_types.insert(N::NAME, TypeId::of::<N::Key>());
    }

    pub fn register_projector<Pr: Projector<Principal = P>>(
        &mut self,
        projector: Pr,
    ) -> Result<(), EngineError> {
        let name = projector.name();
        if self.projectors.contains_key(&name) {
            return Err(EngineError::DuplicateProjectorName { name });
        }
        let nouns = projector.nouns();
        for noun in nouns {
            let bound = self
                .key_types
                .get(noun)
                .ok_or_else(|| EngineError::UnboundNoun {
                    projector: name.clone(),
                    noun: noun.clone(),
                })?;
            if *bound != TypeId::of::<Pr::Key>() {
                return Err(EngineError::NounKeyMismatch {
                    projector: name.clone(),
                    noun: noun.clone(),
                });
            }
        }
        let erased = erase_projector(projector);
        for noun in nouns {
            self.by_noun
                .entry(noun.clone())
                .or_default()
                .push(name.clone());
        }
        self.projectors.insert(name, erased);
        Ok(())
    }

    pub fn add_projector<Pr: Projector<Principal = P>>(
        &mut self,
        projector: Pr,
    ) -> Result<(), EngineError> {
        for noun in projector.nouns() {
            self.key_types
                .entry(noun.clone())
                .or_insert_with(TypeId::of::<Pr::Key>);
        }
        self.register_projector(projector)
    }

    pub fn register_rls<R: RlsApplier<P>>(&mut self, applier: R) {
        self.rls = Some(Arc::new(applier));
    }

    pub fn register_principal_resolver<R: PrincipalResolver<P>>(&mut self, resolver: R) {
        self.resolver = Some(Arc::new(resolver));
    }

    pub fn projector(&self, name: &ProjectorName) -> Option<&Arc<dyn ErasedProjector<P>>> {
        self.projectors.get(name)
    }

    pub fn on_noun(&self, noun: &NounName) -> &[ProjectorName] {
        self.by_noun.get(noun).map_or(NO_PROJECTORS, Vec::as_slice)
    }

    pub fn names(&self) -> impl Iterator<Item = &ProjectorName> {
        self.projectors.keys()
    }

    pub fn all(&self) -> impl Iterator<Item = (&ProjectorName, &Arc<dyn ErasedProjector<P>>)> {
        self.projectors.iter()
    }

    pub fn rls(&self) -> Option<&Arc<dyn RlsApplier<P>>> {
        self.rls.as_ref()
    }

    pub fn resolver(&self) -> Option<&Arc<dyn PrincipalResolver<P>>> {
        self.resolver.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        Assignment, AssignmentKeyProjector, MiskeyedProjector, TwinProjector,
    };
    fn bound() -> RenderRegistry<crate::test_support::TestPrincipal> {
        let mut registry = RenderRegistry::new();
        registry.bind_noun::<Assignment>();
        registry
    }

    #[test]
    fn a_noun_fans_out_to_every_projector_that_renders_it() {
        let mut registry = bound();
        registry.register_projector(AssignmentKeyProjector).unwrap();
        registry.register_projector(TwinProjector).unwrap();
        assert_eq!(registry.on_noun(&Assignment::NAME).len(), 2);
        assert_eq!(registry.names().count(), 2);
    }

    #[test]
    fn a_projector_whose_key_does_not_decode_the_noun_key_is_refused_at_registration() {
        let mut registry = bound();
        let refusal = registry.register_projector(MiskeyedProjector);
        assert!(matches!(
            refusal,
            Err(EngineError::NounKeyMismatch { noun, .. }) if noun == Assignment::NAME
        ));
        assert_eq!(registry.on_noun(&Assignment::NAME).len(), 0);
    }

    #[test]
    fn a_projector_rendering_an_unbound_noun_is_refused_rather_than_silently_unchecked() {
        let mut registry = RenderRegistry::new();
        let refusal = registry.register_projector(AssignmentKeyProjector);
        assert!(matches!(
            refusal,
            Err(EngineError::UnboundNoun { noun, .. }) if noun == Assignment::NAME
        ));
    }

    #[test]
    fn add_projector_binds_the_noun_to_the_first_projectors_key_then_checks_the_rest() {
        let mut registry = RenderRegistry::new();
        registry
            .add_projector(AssignmentKeyProjector)
            .expect("the first projector on a noun binds it with no prior bind_noun");
        registry
            .add_projector(TwinProjector)
            .expect("a second projector whose key matches the bound noun is accepted");
        let mismatch = registry.add_projector(MiskeyedProjector);
        assert!(
            matches!(
                mismatch,
                Err(EngineError::NounKeyMismatch { noun, .. }) if noun == Assignment::NAME
            ),
            "a projector whose key does not decode the already-bound noun key is still refused"
        );
    }

    #[test]
    fn two_projectors_claiming_one_name_would_share_last_sent_so_the_second_is_refused() {
        let mut registry = bound();
        registry.register_projector(AssignmentKeyProjector).unwrap();
        let refusal = registry.register_projector(AssignmentKeyProjector);
        assert!(matches!(
            refusal,
            Err(EngineError::DuplicateProjectorName { name })
                if name == AssignmentKeyProjector.name()
        ));
    }
}
