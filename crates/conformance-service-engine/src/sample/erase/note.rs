use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::accumulator::{Accumulator, ChunkSeq};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{AccumulatorName, NounName, ProjectorName};
use service_engine::nats::{KvKey, Nats};
use service_engine::offer::Offer;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub const NOTE_OFFER: &str = "erase_note_v1";
pub const NOTE_OFFER_PREFIX: &str = "sample/erase_notes/v1/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EraseNote {
    pub id: Uuid,
    pub owner: Uuid,
    pub tenant: Uuid,
    pub body: String,
    pub blob_ref: Option<Uuid>,
}

pub struct EraseNoteNoun;

impl Noun for EraseNoteNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("erase_note");
}

pub struct EraseNoteStore;

impl Persistence for EraseNoteStore {
    type Aggregate = EraseNote;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<EraseNote>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, owner, tenant, body, blob_ref FROM sample_erase_note WHERE id = $1",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.map(row_to_note))
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        note: &'a EraseNote,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_erase_note (id, owner, tenant, body, blob_ref) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, blob_ref = EXCLUDED.blob_ref",
            )
            .bind(note.id)
            .bind(note.owner)
            .bind(note.tenant)
            .bind(&note.body)
            .bind(note.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        note: &'a EraseNote,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_erase_note (id, owner, tenant, body, blob_ref) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(note.id)
            .bind(note.owner)
            .bind(note.tenant)
            .bind(&note.body)
            .bind(note.blob_ref)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

impl Aggregate for EraseNote {
    type Store = EraseNoteStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

fn row_to_note(row: sqlx::postgres::PgRow) -> EraseNote {
    EraseNote {
        id: row.get("id"),
        owner: row.get("owner"),
        tenant: row.get("tenant"),
        body: row.get("body"),
        blob_ref: row.get("blob_ref"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EraseNoteView {
    pub id: Uuid,
    pub body: String,
}

pub struct EraseNoteProjector;

impl EraseNoteProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("erase_notes");
}

impl Projector for EraseNoteProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = std::collections::BTreeMap<Uuid, String>;
    type View = EraseNoteView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[EraseNoteNoun::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows =
                sqlx::query("SELECT id FROM sample_erase_note WHERE tenant = $1 ORDER BY id")
                    .bind(principal.tenant())
                    .fetch_all(pg)
                    .await?;
            Ok(Population::Keys(
                rows.iter().map(|row| row.get::<Uuid, _>("id")).collect(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<Self::Facts, EngineError>> {
        let keys: Vec<Uuid> = scope.keys().to_vec();
        Box::pin(async move {
            let sql = "SELECT id, body FROM sample_erase_note WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => sqlx::query(sql).bind(&keys).fetch_all(pg).await?,
                LoadScope::PerPrincipal { conn, .. } => {
                    sqlx::query(sql).bind(&keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(rows
                .iter()
                .map(|row| (row.get::<Uuid, _>("id"), row.get::<String, _>("body")))
                .collect())
        })
    }

    fn project(
        &self,
        facts: &Self::Facts,
        key: &Uuid,
        _principal: &SamplePrincipal,
    ) -> Option<EraseNoteView> {
        facts.get(key).map(|body| EraseNoteView {
            id: *key,
            body: body.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedNote {
    pub id: Uuid,
    pub body: String,
}

pub struct EraseNoteOffer;

impl Offer for EraseNoteOffer {
    type Row = EraseNote;
    type Published = PublishedNote;

    const NAME: &'static str = NOTE_OFFER;
    const PREFIX: &'static str = NOTE_OFFER_PREFIX;

    fn key(row: &EraseNote) -> Result<KvKey, EngineError> {
        note_offer_key(row.id)
    }

    fn publish(row: &EraseNote) -> Option<PublishedNote> {
        Some(PublishedNote {
            id: row.id,
            body: row.body.clone(),
        })
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<EraseNote>, EngineError>> {
        Box::pin(async move {
            let rows =
                sqlx::query("SELECT id, owner, tenant, body, blob_ref FROM sample_erase_note")
                    .fetch_all(conn)
                    .await?;
            Ok(rows.into_iter().map(row_to_note).collect())
        })
    }
}

pub fn note_offer_key(id: Uuid) -> Result<KvKey, EngineError> {
    KvKey::new(format!("{NOTE_OFFER_PREFIX}{id}"))
        .map_err(|error| EngineError::Config(format!("sample erase offer key: {error}")))
}

pub async fn published_note(nats: &Nats, id: Uuid) -> Option<PublishedNote> {
    let key = note_offer_key(id).expect("a valid offer key");
    nats.published_language::<PublishedNote>()
        .await
        .expect("bind the published-language bucket")
        .get(&key)
        .await
        .expect("read the offered note")
}

pub struct EraseNoteStream;

impl Accumulator for EraseNoteStream {
    type Noun = EraseNoteNoun;
    type Chunk = String;
    type State = String;

    fn name(&self) -> AccumulatorName {
        AccumulatorName::from_static("erase_note_stream")
    }

    fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
        state.push_str(&chunk);
    }
}
