use crate::accumulator::Accumulator;
use crate::blobs::BlobRef;
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

    pub(crate) fn streams(&self) -> &[(AccumulatorName, KeyBytes)] {
        &self.streams
    }

    pub(crate) fn presence_keys(&self) -> &[KvKey] {
        &self.presence
    }

    pub(crate) fn blob_refs(&self) -> &[BlobRef] {
        &self.blobs
    }

    pub(crate) fn merge(&mut self, other: Erased) {
        self.rows += other.rows;
        self.streams.extend(other.streams);
        self.presence.extend(other.presence);
        self.blobs.extend(other.blobs);
    }
}
