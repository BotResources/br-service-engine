use std::collections::BTreeMap;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::{Dims, ForeignKey};
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub const DIM_BODY: u8 = 0;

pub struct Note;

impl Noun for Note {
    type Key = NoteKey;
    const NAME: NounName = NounName::from_static("note");
}

pub fn body_dim() -> Dims {
    Dims::bit(DIM_BODY).expect("a declared dimension fits the bit set")
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NoteKey {
    pub assignment_id: Uuid,
    pub seq: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteFacts {
    pub bodies: BTreeMap<NoteKey, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteView {
    pub assignment_id: Uuid,
    pub seq: i32,
    pub body: String,
}

pub struct NoteProjector;

impl NoteProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("notes");
}

impl Projector for NoteProjector {
    type Principal = SamplePrincipal;
    type Key = NoteKey;
    type Facts = NoteFacts;
    type View = NoteView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Note::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<NoteKey>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT n.assignment_id, n.seq FROM sample_note n \
                 JOIN sample_assignment a ON a.id = n.assignment_id \
                 WHERE a.tenant_id = $1 ORDER BY n.assignment_id, n.seq DESC",
            )
            .bind(principal.tenant())
            .fetch_all(pg)
            .await?;
            Ok(Population::Ordered {
                keys: rows.iter().map(note_key).collect(),
                open_head: true,
            })
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<NoteKey> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, NoteKey, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<NoteFacts, EngineError>> {
        let assignments: Vec<Uuid> = scope.keys().iter().map(|k| k.assignment_id).collect();
        let seqs: Vec<i32> = scope.keys().iter().map(|k| k.seq).collect();
        Box::pin(async move {
            let sql = "SELECT assignment_id, seq, body FROM sample_note \
                       WHERE (assignment_id, seq) IN (SELECT * FROM unnest($1::uuid[], $2::int[]))";
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => {
                    sqlx::query(sql)
                        .bind(&assignments)
                        .bind(&seqs)
                        .fetch_all(pg)
                        .await?
                }
                LoadScope::PerPrincipal { conn, .. } => {
                    sqlx::query(sql)
                        .bind(&assignments)
                        .bind(&seqs)
                        .fetch_all(&mut *conn)
                        .await?
                }
            };
            Ok(NoteFacts {
                bodies: rows
                    .iter()
                    .map(|row| (note_key(row), row.get::<String, _>("body")))
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &NoteFacts,
        key: &NoteKey,
        _principal: &SamplePrincipal,
    ) -> Option<NoteView> {
        facts.bodies.get(key).map(|body| NoteView {
            assignment_id: key.assignment_id,
            seq: key.seq,
            body: body.clone(),
        })
    }
}

fn note_key(row: &sqlx::postgres::PgRow) -> NoteKey {
    NoteKey {
        assignment_id: row.get("assignment_id"),
        seq: row.get("seq"),
    }
}
