pub mod assignment;
pub mod cron;
pub mod engine;
pub mod gate;
pub mod mirror;
pub mod note;
pub mod outbox;
pub mod principal;
pub mod relays;
pub mod render;
pub mod spy;
pub mod stream;
pub mod titles;
pub mod transport;

use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};

fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    migrator().run(pool).await
}

pub const TABLES: &[&str] = &[
    "sample_member",
    "sample_assignment",
    "sample_note",
    "integration_outbox",
    "sample_relay_row",
    "sample_relay_claim",
    "sample_kv_pending",
    "sample_leader_run",
    "sample_staged_impact",
    "sample_cron_run",
    "sample_backfill",
];

pub use mirror::{
    DIRECTORY_MIRROR, SampleDirectory, backfills, directory_mirror_handle, known_users,
    publish_roster,
};

pub use cron::{SampleCronJob, claimed_slots, completed_slots, cron_pods, cron_runs};

pub use engine::{SAMPLE_JOB, SAMPLE_RELAY, boot_render_engine, boot_sample_engine, engine_config};

pub use assignment::{
    Assignment, AssignmentFacts, AssignmentProjector, AssignmentRow, AssignmentView,
};
pub use gate::Gate;
pub use note::{Note, NoteFacts, NoteKey, NoteProjector, NoteView};
pub use outbox::{Relayed, delivered_event_ids, relayed_coords, stage_outbox_row};
pub use principal::{
    FailingPrincipalResolver, SamplePrincipal, SamplePrincipalResolver, SampleRls,
};
pub use relays::{
    BusySampleRelay, FailingSampleRelay, LeaderRunSampleRelay, RowClaimSampleRelay, SampleKvSource,
    SampleRoster,
};
pub use spy::{CohortMode, Spy, SpyAssignments, WindowMode, assignment_key};
pub use stream::{NoteBody, NoteBodyState, SyntheticSource, note_body_runtime};
pub use titles::{MiskeyedProjector, TitleFacts, TitleProjector, TitleView};
pub use transport::{
    RecordingTransport, SAMPLE_CHANNEL, StagingGate, StagingTransport, staged_impacts,
};
