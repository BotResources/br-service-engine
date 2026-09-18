use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;
use tokio::sync::watch;

use crate::error::EngineError;
use crate::name::MirrorName;
use crate::nats::Nats;
use crate::transport::ImpactTransport;

use super::consumed::{Consumed, ConsumedManifest, is_raw_json};
use super::consumption::Consumption;
use super::handle::MirrorHandle;
use super::leader::{MirrorGate, MirrorLeader};
use super::projection::Project;
use super::required::RequiredKey;
use super::runtime::MirrorRuntime;
use super::shadow::Shadows;

#[derive(Debug, Clone)]
pub struct ConsumedGuard {
    prefix: &'static str,
    manifest: ConsumedManifest,
    is_raw_json: bool,
    escape: bool,
}

impl ConsumedGuard {
    fn of<C: Consumed>() -> Self {
        Self {
            prefix: C::PREFIX,
            manifest: C::manifest(),
            is_raw_json: is_raw_json::<C>(),
            escape: C::RAW_JSON_ESCAPE_HATCH,
        }
    }

    pub fn prefix(&self) -> &'static str {
        self.prefix
    }

    pub fn manifest(&self) -> ConsumedManifest {
        self.manifest
    }

    pub fn refuses_raw_json(&self) -> bool {
        self.is_raw_json && !self.escape
    }
}

pub struct Mirror {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
    required: Vec<RequiredKey>,
}

impl Mirror {
    pub fn new(name: MirrorName) -> Self {
        Self {
            name,
            consumptions: Vec::new(),
            guards: Vec::new(),
            required: Vec::new(),
        }
    }

    pub fn consume<C: Consumed>(mut self) -> Self {
        self.consumptions.push(Consumption::of::<C>());
        self.guards.push(ConsumedGuard::of::<C>());
        self
    }

    pub fn require_key<C: Consumed>(mut self, key: &'static str) -> Self {
        self.required.push(RequiredKey::of::<C>(key));
        self
    }

    pub fn keyed_by<K, F>(self, keyed_by: F) -> MirrorKeyed<K>
    where
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        F: Fn(&Shadows, &super::change::Change) -> Vec<K> + Send + Sync + 'static,
    {
        MirrorKeyed {
            name: self.name,
            consumptions: self.consumptions,
            guards: self.guards,
            required: self.required,
            keyed_by: Arc::new(keyed_by),
        }
    }
}

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &super::change::Change) -> Vec<K> + Send + Sync>;

pub struct MirrorKeyed<K> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
    required: Vec<RequiredKey>,
    keyed_by: KeyedByFn<K>,
}

impl<K> MirrorKeyed<K>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    pub fn project<Pr: Project<K>>(self, project: Pr) -> MirrorReady<K, Pr> {
        MirrorReady {
            name: self.name,
            consumptions: self.consumptions,
            guards: self.guards,
            required: self.required,
            keyed_by: self.keyed_by,
            project: Arc::new(project),
            reconcile_keys: None,
            reconcile_deadline: crate::config::DEFAULT_MIRROR_RECONCILE,
        }
    }
}

pub struct MirrorReady<K, Pr: Project<K>> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
    required: Vec<RequiredKey>,
    keyed_by: KeyedByFn<K>,
    project: Arc<Pr>,
    reconcile_keys: Option<super::consumption::ReconcileKeysFn<K>>,
    reconcile_deadline: Duration,
}

impl<K, Pr: Project<K>> MirrorReady<K, Pr>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    pub fn name(&self) -> &MirrorName {
        &self.name
    }

    pub fn guards(&self) -> &[ConsumedGuard] {
        &self.guards
    }

    pub fn validate(&self) -> Result<(), EngineError> {
        for guard in &self.guards {
            if guard.refuses_raw_json() {
                return Err(EngineError::RawJsonConsumption {
                    mirror: self.name.clone(),
                    prefix: guard.prefix(),
                });
            }
        }
        for required in &self.required {
            if !required.within_prefix() {
                return Err(EngineError::RequiredKeyOutsidePrefix {
                    mirror: self.name.clone(),
                    prefix: required.prefix(),
                    key: required.key(),
                });
            }
        }
        Ok(())
    }

    pub fn reconcile_keys<F>(mut self, reconcile_keys: F) -> Self
    where
        F: Fn(PgPool) -> BoxFuture<'static, Result<Vec<K>, EngineError>> + Send + Sync + 'static,
    {
        self.reconcile_keys = Some(Arc::new(reconcile_keys));
        self
    }

    pub fn with_reconcile_deadline(mut self, reconcile_deadline: Duration) -> Self {
        self.reconcile_deadline = reconcile_deadline;
        self
    }

    pub fn build(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
    ) -> MirrorHandle {
        self.build_with(nats, pool, transport, None)
    }

    pub fn build_led(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        leader: MirrorLeader,
    ) -> MirrorHandle {
        let gate = MirrorGate::new(&self.name, leader);
        self.build_with(nats, pool, transport, Some(gate))
    }

    fn build_with(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        leader: Option<MirrorGate>,
    ) -> MirrorHandle {
        let required_channel = (!self.required.is_empty()).then(|| {
            let initial = self
                .required
                .iter()
                .map(|required| format!("{}: {}", self.name, required.key()))
                .collect::<Vec<_>>();
            watch::channel(initial)
        });
        let (required_tx, required_rx) = match required_channel {
            Some((tx, rx)) => (Some(Arc::new(tx)), Some(rx)),
            None => (None, None),
        };
        let runtime = Arc::new(MirrorRuntime::new(
            self.name.clone(),
            nats,
            pool,
            transport,
            Arc::new(self.consumptions),
            self.keyed_by,
            self.project,
            self.reconcile_keys,
            leader,
            self.reconcile_deadline,
            self.required,
            required_tx,
        ));
        let name = self.name.clone();
        let reconcile = {
            let runtime = runtime.clone();
            move || runtime.clone().reconcile()
        };
        let watch = move || runtime.clone().watch();
        let handle = MirrorHandle::new(name, reconcile, watch);
        match required_rx {
            Some(rx) => handle.with_required_keys(rx),
            None => handle,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mirror::projection::Projection;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct Typed {
        #[allow(dead_code)]
        v: u32,
    }
    impl Consumed for Typed {
        const PREFIX: &'static str = "typed/v1/";
    }

    struct NoProject;
    impl Project<()> for NoProject {
        type Error = EngineError;
        fn project<'a>(
            &'a self,
            _cx: Projection<'a>,
            _key: (),
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn ready<C: Consumed>() -> MirrorReady<(), NoProject> {
        Mirror::new(MirrorName::from_static("guarded"))
            .consume::<C>()
            .keyed_by(|_shadows, _change| Vec::<()>::new())
            .project(NoProject)
    }

    #[test]
    fn a_raw_json_consumption_is_refused_without_the_escape_hatch() {
        let refusal = ready::<serde_json::Value>().validate().unwrap_err();
        assert!(matches!(
            refusal,
            EngineError::RawJsonConsumption {
                prefix: "loose/v1/",
                ..
            }
        ));
    }

    #[test]
    fn a_typed_consumption_validates_and_carries_its_manifest() {
        let ready = ready::<Typed>();
        assert!(ready.validate().is_ok());
        let guard = &ready.guards()[0];
        assert_eq!(guard.prefix(), "typed/v1/");
        assert_eq!(guard.manifest(), Typed::manifest());
    }

    #[test]
    fn a_required_key_outside_the_consumed_prefix_is_refused() {
        let refusal = Mirror::new(MirrorName::from_static("guarded"))
            .consume::<Typed>()
            .require_key::<Typed>("other/v1/needed")
            .keyed_by(|_shadows, _change| Vec::<()>::new())
            .project(NoProject)
            .validate()
            .unwrap_err();
        assert!(matches!(
            refusal,
            EngineError::RequiredKeyOutsidePrefix {
                prefix: "typed/v1/",
                key: "other/v1/needed",
                ..
            }
        ));
    }
}
