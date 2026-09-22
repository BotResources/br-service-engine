use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::blobs::head::BlobHead;
use crate::blobs::{BlobRef, Blobs};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::pipeline::{PostSave, Refused};

pub struct Uploaded<'a> {
    pub reference: BlobRef,
    pub kind: &'static str,
    pub head: &'a BlobHead,
    pub file_name: &'a str,
    pub content_type: &'a str,
    pub owner: Option<PersonId>,
}

pub enum PostUpload {
    Refused(Refused),
    Fault(EngineError),
}

impl From<Refused> for PostUpload {
    fn from(refused: Refused) -> Self {
        Self::Refused(refused)
    }
}

impl From<EngineError> for PostUpload {
    fn from(error: EngineError) -> Self {
        Self::Fault(error)
    }
}

impl From<sqlx::Error> for PostUpload {
    fn from(error: sqlx::Error) -> Self {
        Self::Fault(EngineError::from(error))
    }
}

pub(crate) type UploadPolicy = Arc<
    dyn for<'a> Fn(Uploaded<'a>, &'a mut PostSave<'_, '_>) -> BoxFuture<'a, Result<(), PostUpload>>
        + Send
        + Sync,
>;

#[derive(Default, Clone)]
pub(crate) struct UploadPolicies {
    by_kind: BTreeMap<&'static str, UploadPolicy>,
}

impl UploadPolicies {
    pub(crate) fn register<B, F>(&mut self, policy: F) -> Result<(), EngineError>
    where
        B: Blobs,
        F: for<'a> Fn(
                Uploaded<'a>,
                &'a mut PostSave<'_, '_>,
            ) -> BoxFuture<'a, Result<(), PostUpload>>
            + Send
            + Sync
            + 'static,
    {
        if self.by_kind.contains_key(B::KIND) {
            return Err(EngineError::Config(format!(
                "a post-upload policy is already registered for blob kind {}",
                B::KIND
            )));
        }
        self.by_kind.insert(B::KIND, Arc::new(policy));
        Ok(())
    }

    pub(crate) fn get(&self, kind: &str) -> Option<UploadPolicy> {
        self.by_kind.get(kind).cloned()
    }

    pub(crate) fn contains(&self, kind: &str) -> bool {
        self.by_kind.contains_key(kind)
    }

    pub(crate) fn kinds(&self) -> BTreeSet<&'static str> {
        self.by_kind.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Attachment;
    impl Blobs for Attachment {
        const KIND: &'static str = "attachment";
    }

    #[test]
    fn a_second_policy_for_one_kind_is_refused() {
        let mut policies = UploadPolicies::default();
        policies
            .register::<Attachment, _>(|_u, _ps| Box::pin(async { Ok(()) }))
            .expect("first policy registers");
        assert!(matches!(
            policies.register::<Attachment, _>(|_u, _ps| Box::pin(async { Ok(()) })),
            Err(EngineError::Config(_))
        ));
    }

    #[test]
    fn a_policy_is_recovered_by_the_row_kind_string() {
        let mut policies = UploadPolicies::default();
        policies
            .register::<Attachment, _>(|_u, _ps| Box::pin(async { Ok(()) }))
            .unwrap();
        assert!(policies.get("attachment").is_some());
        assert!(policies.contains("attachment"));
        assert!(policies.get("avatar").is_none());
        assert!(policies.kinds().contains("attachment"));
        assert!(!policies.kinds().contains("avatar"));
    }
}
