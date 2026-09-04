use br_util_nats_fabric::{Fabric, FabricError, KvKey, PublishedLanguagePublisher};
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::error::RelayError;
use crate::name::RelayName;
use crate::relay::{Claim, Discipline, Drained, Relay};
use crate::relays::kv_watermark;

pub const DEFAULT_CAS_RETRIES: usize = 8;

pub trait Versioned {
    fn version(&self) -> u64;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvWrite<V> {
    Put(V),
    Retract,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvChange<V> {
    pub key: KvKey,
    pub version: u64,
    pub write: KvWrite<V>,
}

impl<V> KvChange<V> {
    pub fn retract(key: KvKey, version: u64) -> Self {
        Self {
            key,
            version,
            write: KvWrite::Retract,
        }
    }
}

impl<V: Versioned> KvChange<V> {
    pub fn put(key: KvKey, value: V) -> Self {
        Self {
            key,
            version: value.version(),
            write: KvWrite::Put(value),
        }
    }
}

pub trait KvSource<V>: Send + Sync + 'static {
    fn pending<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        batch: usize,
    ) -> BoxFuture<'a, Result<Vec<KvChange<V>>, RelayError>>;

    fn applied<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        applied: &'a [KvChange<V>],
    ) -> BoxFuture<'a, Result<(), RelayError>>;
}

pub struct KvDrainRelay<V, S> {
    name: RelayName,
    publisher: PublishedLanguagePublisher<V>,
    source: S,
    cas_retries: usize,
}

impl<V, S> KvDrainRelay<V, S>
where
    V: Serialize + DeserializeOwned + PartialEq + Clone + Versioned + Send + Sync + 'static,
    S: KvSource<V>,
{
    pub async fn open(name: RelayName, fabric: &Fabric, source: S) -> Result<Self, RelayError> {
        let publisher = PublishedLanguagePublisher::open(fabric)
            .await
            .map_err(published_language)?;
        Ok(Self {
            name,
            publisher,
            source,
            cas_retries: DEFAULT_CAS_RETRIES,
        })
    }

    pub fn with_cas_retries(mut self, cas_retries: usize) -> Self {
        self.cas_retries = cas_retries;
        self
    }

    pub fn publisher(&self) -> &PublishedLanguagePublisher<V> {
        &self.publisher
    }

    async fn apply(&self, conn: &mut PgConnection, change: &KvChange<V>) -> Result<(), RelayError> {
        let watermark = kv_watermark::read(conn, &self.name, &change.key).await?;
        for _ in 0..=self.cas_retries {
            let observed = self
                .publisher
                .get_with_revision(&change.key)
                .await
                .map_err(published_language)?;
            let published = observed.as_ref().map(|(value, _)| value.version());
            let floor = [watermark, published].into_iter().flatten().max();
            if floor.is_some_and(|floor| !supersedes(floor, change.version)) {
                return Ok(());
            }
            match &change.write {
                KvWrite::Put(value) => match observed {
                    None => self
                        .publisher
                        .put(&change.key, value)
                        .await
                        .map_err(published_language)?,
                    Some((_, revision)) => {
                        match self.publisher.update_if(&change.key, value, revision).await {
                            Ok(_) => {}
                            Err(FabricError::RevisionConflict { .. }) => continue,
                            Err(error) => return Err(published_language(error)),
                        }
                    }
                },
                KvWrite::Retract => match observed {
                    None => {}
                    Some((_, revision)) => {
                        match self.publisher.delete_if(&change.key, revision).await {
                            Ok(()) => {}
                            Err(FabricError::RevisionConflict { .. }) => continue,
                            Err(error) => return Err(published_language(error)),
                        }
                    }
                },
            }
            kv_watermark::raise(conn, &self.name, &change.key, change.version).await?;
            return Ok(());
        }
        Err(RelayError::Publish(format!(
            "{} lost the published-language compare-and-swap on {} after {} retries",
            self.name,
            change.key.as_str(),
            self.cas_retries
        )))
    }
}

impl<V, S> Relay for KvDrainRelay<V, S>
where
    V: Serialize + DeserializeOwned + PartialEq + Clone + Versioned + Send + Sync + 'static,
    S: KvSource<V>,
{
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::Leader
    }

    fn drain<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            let batch = claim.batch().max(1);
            let changes = self.source.pending(conn, batch).await?;
            if changes.is_empty() {
                return Ok(Drained::NOTHING);
            }
            for change in &changes {
                self.apply(conn, change).await?;
            }
            let rows = changes.len();
            self.source.applied(conn, &changes).await?;
            Ok(Drained::rows(rows, rows >= batch))
        })
    }
}

fn supersedes(published: u64, desired: u64) -> bool {
    desired > published
}

fn published_language(error: FabricError) -> RelayError {
    RelayError::Publish(format!("published language: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Roster {
        version: u64,
    }

    impl Versioned for Roster {
        fn version(&self) -> u64 {
            self.version
        }
    }

    fn key() -> KvKey {
        KvKey::new("sample/rosters/one").unwrap()
    }

    #[test]
    fn a_change_carries_the_version_it_publishes_at_so_a_retraction_is_ordered_too() {
        let put = KvChange::put(key(), Roster { version: 3 });
        assert_eq!(put.key, key());
        assert_eq!(put.version, 3);
        assert_eq!(put.write, KvWrite::Put(Roster { version: 3 }));
        let retract = KvChange::<Roster>::retract(key(), 4);
        assert_eq!(retract.version, 4);
        assert_eq!(retract.write, KvWrite::Retract);
    }

    #[test]
    fn publication_is_monotonic_so_a_replayed_change_never_walks_the_entry_backwards() {
        let published = Roster { version: 7 };
        assert!(!supersedes(published.version(), 6));
        assert!(
            !supersedes(published.version(), 7),
            "a redelivered change at the published version is a no-op, not a rewrite"
        );
        assert!(supersedes(published.version(), 8));
    }
}
