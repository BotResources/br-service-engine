use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::impact::Dims;
use service_engine::pipeline::{Mutation, MutationFault, MutationInput};
use service_engine::{BlobRef, OneShot, UploadUrl};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::blob::Attachment;
use crate::sample::erase::note::{EraseNote, EraseNoteNoun, EraseNoteStream};
use crate::sample::erase::store::{erase_ledger, erase_memos};
use crate::sample::presence::{Typing, typing_key};
use crate::sample::principal::SamplePrincipal;

#[derive(Debug, thiserror::Error)]
pub enum EraseFault {
    #[error("the store failed")]
    Store(#[from] EngineError),
    #[error("this slice always fails its erase")]
    Boom,
}

impl MutationFault for EraseFault {
    fn reason(&self) -> Option<Reason> {
        None
    }
}

impl From<sqlx::Error> for EraseFault {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(EngineError::from(error))
    }
}

pub struct NoteEraser;

impl Erasable for NoteEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move {
            let owner = person.as_uuid();
            let rows = sqlx::query(
                "SELECT id, owner, tenant, body, blob_ref FROM sample_erase_note WHERE owner = $1",
            )
            .bind(owner)
            .fetch_all(cx.connection())
            .await?;
            let mut out = Erased::new();
            for row in &rows {
                let note = EraseNote {
                    id: row.get("id"),
                    owner,
                    tenant: row.get("tenant"),
                    body: row.get("body"),
                    blob_ref: row.get("blob_ref"),
                };
                cx.dirty_offer(&note)?;
                cx.impact::<EraseNoteNoun>(&note.id, Dims::ALL)?;
                out.purge_stream(&EraseNoteStream, &note.id)?;
                out.purge_presence::<Typing>(&typing_key(note.id, owner));
                if let Some(blob) = note.blob_ref {
                    out.purge_blob(BlobRef(blob));
                }
                out.rows(1);
            }
            sqlx::query("DELETE FROM sample_erase_note WHERE owner = $1")
                .bind(owner)
                .execute(cx.connection())
                .await?;
            Ok(out)
        })
    }
}

pub struct MemoEraser;

impl Erasable for MemoEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move {
            let deleted = erase_memos(cx.connection(), person.as_uuid()).await?;
            let mut out = Erased::new();
            out.rows(deleted);
            Ok(out)
        })
    }
}

pub struct LedgerEraser;

impl Erasable for LedgerEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move {
            let touched = erase_ledger(cx.connection(), person.as_uuid()).await?;
            let mut out = Erased::new();
            out.rows(touched);
            Ok(out)
        })
    }
}

pub struct NoteDeleteEraser;

impl Erasable for NoteDeleteEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move {
            let owner = person.as_uuid();
            let rows = sqlx::query(
                "SELECT id, owner, tenant, body, blob_ref FROM sample_erase_note WHERE owner = $1",
            )
            .bind(owner)
            .fetch_all(cx.connection())
            .await?;
            let mut out = Erased::new();
            for row in &rows {
                let note = EraseNote {
                    id: row.get("id"),
                    owner,
                    tenant: row.get("tenant"),
                    body: row.get("body"),
                    blob_ref: row.get("blob_ref"),
                };
                cx.delete(&note).await?;
                out.rows(1);
            }
            Ok(out)
        })
    }
}

pub struct FailingEraser;

impl Erasable for FailingEraser {
    type Error = EraseFault;

    fn erase<'a>(
        &'a self,
        _cx: &'a mut Erase<'a>,
        _person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, EraseFault>> {
        Box::pin(async move { Err(EraseFault::Boom) })
    }
}

#[derive(Debug, Deserialize)]
pub struct AttachNoteBlob {
    pub note: Uuid,
    pub owner: Uuid,
    pub tenant: Uuid,
    pub body: String,
    pub content_type: String,
}

impl MutationInput for AttachNoteBlob {
    type Output = OneShot<UploadUrl>;
    type Error = EraseFault;
    const NAME: &'static str = "attach_note_blob";
}

pub fn attach_note_blob<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: AttachNoteBlob,
) -> BoxFuture<'m, Result<OneShot<UploadUrl>, EraseFault>> {
    Box::pin(async move {
        let blob = cx.blob::<Attachment>(input.body.clone(), input.content_type)?;
        let note = EraseNote {
            id: input.note,
            owner: input.owner,
            tenant: input.tenant,
            body: input.body,
            blob_ref: Some(blob.reference().as_uuid()),
        };
        cx.create(&note).await?;
        cx.impact::<EraseNoteNoun>(&note.id, Dims::ALL)?;
        Ok(OneShot(blob.upload_url()))
    })
}

pub async fn seed_note(pool: &PgPool, id: Uuid, owner: Uuid, tenant: Uuid, body: &str) {
    sqlx::query(
        "INSERT INTO sample_erase_note (id, owner, tenant, body, blob_ref) \
         VALUES ($1, $2, $3, $4, NULL)",
    )
    .bind(id)
    .bind(owner)
    .bind(tenant)
    .bind(body)
    .execute(pool)
    .await
    .expect("seed the CRUD erase note");
}

pub async fn count_notes(pool: &PgPool, owner: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_erase_note WHERE owner = $1")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("count the person's notes")
}
