use std::any::TypeId;
use std::sync::Arc;

use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::blobs::store::{BlobRowOp, BlobStore, ReferenceRow};
use crate::blobs::{BlobPolicy, BlobRef, Blobs, UploadUrl};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::time::Timestamp;

pub struct Blob {
    reference: BlobRef,
    upload_url: UploadUrl,
}

impl Blob {
    pub fn reference(&self) -> BlobRef {
        self.reference
    }

    pub fn upload_url(&self) -> UploadUrl {
        self.upload_url.clone()
    }
}

pub(crate) type Kinds = Arc<Vec<(TypeId, &'static str, BlobPolicy)>>;

#[derive(Clone)]
pub(crate) struct BlobHandle {
    kinds: Kinds,
    store: Arc<OnceCell<BlobStore>>,
}

impl BlobHandle {
    pub(crate) fn new(kinds: Kinds, store: Arc<OnceCell<BlobStore>>) -> Self {
        Self { kinds, store }
    }

    fn store(&self) -> Result<&BlobStore, EngineError> {
        self.store.get().ok_or_else(|| {
            EngineError::Blob(
                "the object-storage bucket is not bound yet; run() binds it at boot".into(),
            )
        })
    }

    fn policy_of<B: Blobs>(&self) -> Result<BlobPolicy, EngineError> {
        self.kinds
            .iter()
            .find(|(type_id, _, _)| *type_id == TypeId::of::<B>())
            .map(|(_, _, policy)| *policy)
            .ok_or(EngineError::BlobKindUnregistered { kind: B::KIND })
    }

    pub(crate) fn stage<B: Blobs>(
        &self,
        ops: &mut Vec<BlobRowOp>,
        file_name: String,
        content_type: String,
        owner: Option<PersonId>,
    ) -> Result<Blob, EngineError> {
        let policy = self.policy_of::<B>()?;
        let kind = B::KIND;
        let store = self.store()?;
        let id = Uuid::now_v7();
        let object_key = format!("{}/{}/{}", store.service(), kind, id);
        let upload_url = store.presign_upload(&object_key, policy.max_bytes)?;
        ops.push(BlobRowOp::Insert(ReferenceRow {
            id,
            object_key,
            kind: kind.to_string(),
            service: store.service().to_string(),
            content_type,
            file_name,
            owner: owner.map(|person| person.as_uuid()),
        }));
        Ok(Blob {
            reference: BlobRef(id),
            upload_url,
        })
    }

    pub(crate) fn release(&self, ops: &mut Vec<BlobRowOp>, reference: BlobRef, at: Timestamp) {
        ops.push(BlobRowOp::Orphan(reference, at));
    }
}
