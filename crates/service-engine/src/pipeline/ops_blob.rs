use crate::blobs::{Blob, BlobHandle, BlobRef, Blobs};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::pipeline::ops::Ops;

impl<'a> Ops<'a> {
    pub fn blob<B: Blobs>(
        &mut self,
        file_name: impl Into<String>,
        content_type: impl Into<String>,
    ) -> Result<Blob, EngineError> {
        self.stage_blob::<B>(file_name.into(), content_type.into(), None)
    }

    pub fn blob_owned<B: Blobs>(
        &mut self,
        file_name: impl Into<String>,
        content_type: impl Into<String>,
        owner: PersonId,
    ) -> Result<Blob, EngineError> {
        self.stage_blob::<B>(file_name.into(), content_type.into(), Some(owner))
    }

    pub fn release_blob(&mut self, reference: BlobRef) -> Result<(), EngineError> {
        let handle = self.blob_handle()?;
        handle.release(&mut self.staged.blob_ops, reference, self.now);
        Ok(())
    }

    fn stage_blob<B: Blobs>(
        &mut self,
        file_name: String,
        content_type: String,
        owner: Option<PersonId>,
    ) -> Result<Blob, EngineError> {
        let handle = self.blob_handle()?;
        handle.stage::<B>(&mut self.staged.blob_ops, file_name, content_type, owner)
    }

    fn blob_handle(&self) -> Result<&'a BlobHandle, EngineError> {
        self.blobs.ok_or_else(|| {
            EngineError::Blob(
                "no object storage is configured; call EngineConfig::with_blob_storage and \
                 register_blobs"
                    .into(),
            )
        })
    }
}
