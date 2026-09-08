use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use service_engine::{BlobRef, Blobs};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::blob_view::Doc;
use crate::sample::pipeline::SampleFault;
use crate::sample::principal::SamplePrincipal;

pub struct Attachment;

impl Blobs for Attachment {
    const KIND: &'static str = "attachment";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub blob_ref: Option<Uuid>,
}

pub struct DocStore;

impl Persistence for DocStore {
    type Aggregate = DocRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<DocRow>, EngineError>> {
        Box::pin(async move {
            let row =
                sqlx::query("SELECT id, tenant_id, name, blob_ref FROM sample_doc WHERE id = $1")
                    .bind(key)
                    .fetch_optional(conn)
                    .await?;
            Ok(row.map(|row| DocRow {
                id: row.get("id"),
                tenant_id: row.get("tenant_id"),
                name: row.get("name"),
                blob_ref: row.get("blob_ref"),
            }))
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a DocRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_doc (id, tenant_id, name, blob_ref) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, blob_ref = EXCLUDED.blob_ref",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.name)
            .bind(aggregate.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a DocRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_doc (id, tenant_id, name, blob_ref) VALUES ($1, $2, $3, $4)",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.name)
            .bind(aggregate.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

impl Aggregate for DocRow {
    type Store = DocStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

#[derive(Debug, Deserialize)]
pub struct AttachDoc {
    pub id: Uuid,
    pub tenant: Uuid,
    pub name: String,
    pub content_type: String,
    #[serde(default)]
    pub fail: bool,
}

impl MutationInput for AttachDoc {
    type Output = OneShot<String>;
    type Error = SampleFault;
    const NAME: &'static str = "attach_doc";
}

pub fn attach_doc<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: AttachDoc,
) -> BoxFuture<'m, Result<OneShot<String>, SampleFault>> {
    Box::pin(async move {
        let blob = cx.blob::<Attachment>(input.name.clone(), input.content_type)?;
        let doc = DocRow {
            id: input.id,
            tenant_id: input.tenant,
            name: input.name,
            blob_ref: Some(blob.reference().as_uuid()),
        };
        cx.create(&doc).await?;
        cx.impact_caused::<Doc, _>(&doc.id, "attached")?;
        if input.fail {
            return Err(SampleFault::Store(
                "deliberate rollback after staging".into(),
            ));
        }
        Ok(OneShot(blob.upload_url().into_string()))
    })
}

#[derive(Debug, Deserialize)]
pub struct AttachOwnedDoc {
    pub id: Uuid,
    pub tenant: Uuid,
    pub name: String,
    pub content_type: String,
    pub owner: Uuid,
}

impl MutationInput for AttachOwnedDoc {
    type Output = OneShot<String>;
    type Error = SampleFault;
    const NAME: &'static str = "attach_owned_doc";
}

pub fn attach_owned_doc<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: AttachOwnedDoc,
) -> BoxFuture<'m, Result<OneShot<String>, SampleFault>> {
    Box::pin(async move {
        let blob = cx.blob_owned::<Attachment>(
            input.name.clone(),
            input.content_type,
            service_engine::PersonId(input.owner),
        )?;
        let doc = DocRow {
            id: input.id,
            tenant_id: input.tenant,
            name: input.name,
            blob_ref: Some(blob.reference().as_uuid()),
        };
        cx.create(&doc).await?;
        cx.impact_caused::<Doc, _>(&doc.id, "attached")?;
        Ok(OneShot(blob.upload_url().into_string()))
    })
}

#[derive(Debug, Deserialize)]
pub struct DetachDoc {
    pub id: Uuid,
}

impl MutationInput for DetachDoc {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "detach_doc";
}

pub fn detach_doc<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: DetachDoc,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let mut doc = cx
            .load::<DocRow>(&input.id)
            .await?
            .ok_or(SampleFault::NotFound)?;
        if let Some(reference) = doc.blob_ref.take() {
            cx.release_blob(BlobRef(reference))?;
        }
        cx.save(&doc).await?;
        cx.impact_caused::<Doc, _>(&doc.id, "detached")?;
        Ok(())
    })
}
