use std::any::TypeId;
use std::sync::Arc;

use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::blobs::store::{BlobRowOp, BlobStore, ReferenceRow};
use crate::blobs::{BlobPolicy, BlobRef, Blobs, UploadExpectation, UploadUrl};
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
        expect: Option<UploadExpectation>,
    ) -> Result<Blob, EngineError> {
        let policy = self.policy_of::<B>()?;
        if let Some(expect) = &expect
            && expect.size > policy.max_bytes
        {
            return Err(EngineError::BlobOverPolicy {
                kind: B::KIND,
                size: expect.size,
                max_bytes: policy.max_bytes,
            });
        }
        let kind = B::KIND;
        let store = self.store()?;
        let id = Uuid::now_v7();
        let object_key = format!("{}/{}/{}", store.service(), kind, id);
        let expect_ref = expect.as_ref().map(|expect| (expect.size, &expect.sha256));
        let upload_url =
            store.presign_upload(&object_key, policy.max_bytes, &content_type, expect_ref)?;
        ops.push(BlobRowOp::Insert(ReferenceRow {
            id,
            object_key,
            kind: kind.to_string(),
            service: store.service().to_string(),
            content_type,
            file_name,
            owner: owner.map(|person| person.as_uuid()),
            expected_size: expect.map(|expect| expect.size as i64),
            expected_sha256: expect.map(|expect| *expect.sha256.as_bytes()),
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
