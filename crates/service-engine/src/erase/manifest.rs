use crate::accumulator::Accumulator;
use crate::blobs::BlobRef;
use crate::erase::purge::{PurgeManifest, PurgeStream};
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::nats::KvKey;
use crate::presence::{Presence, PresenceKey};
use crate::wire::{KeyBytes, Noun};

#[derive(Default)]
pub struct Erased {
    rows: u64,
    streams: Vec<(AccumulatorName, KeyBytes)>,
    presence: Vec<KvKey>,
    blobs: Vec<BlobRef>,
}

impl Erased {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rows(&mut self, count: u64) -> &mut Self {
        self.rows += count;
        self
    }

    pub fn purge_stream<A: Accumulator>(
        &mut self,
        accumulator: &A,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<&mut Self, EngineError> {
        self.streams
            .push((accumulator.name(), KeyBytes::encode(key)?));
        Ok(self)
    }

    pub fn purge_presence<Pr: Presence>(&mut self, key: &PresenceKey<Pr>) -> &mut Self {
        self.presence.push(Pr::kv_key(key));
        self
    }

    pub fn purge_blob(&mut self, reference: BlobRef) -> &mut Self {
        self.blobs.push(reference);
        self
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.rows
    }

    pub(crate) fn merge(&mut self, other: Erased) {
        self.rows += other.rows;
        self.streams.extend(other.streams);
        self.presence.extend(other.presence);
        self.blobs.extend(other.blobs);
    }

    pub(crate) fn to_purge(&self) -> Result<PurgeManifest, EngineError> {
        let streams = self
            .streams
            .iter()
            .map(|(accumulator, key)| {
                Ok(PurgeStream {
                    accumulator: accumulator.as_str().to_string(),
                    key: key.decode::<serde_json::Value>()?,
                })
            })
            .collect::<Result<Vec<_>, EngineError>>()?;
        Ok(PurgeManifest {
            streams,
            presence: self
                .presence
                .iter()
                .map(|k| k.as_str().to_string())
                .collect(),
            blobs: self
                .blobs
                .iter()
                .map(|reference| reference.as_uuid())
                .collect(),
        })
    }
}
