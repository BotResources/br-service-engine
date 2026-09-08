pub mod assignment;
pub mod cron;
pub mod engine;
pub mod gate;
pub mod gated;
pub mod mirror;
pub mod note;
pub mod outbox;
pub mod pipeline;
pub mod pipeline_support;
pub mod presence;
pub mod principal;
pub mod reactions;
pub mod relays;
pub mod render;
pub mod roster;
pub mod spy;
pub mod stream;
pub mod titles;
pub mod transport;
pub mod widget;

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
    "se_stub_effect",
    "sample_widget",
];

pub use mirror::{
    DIRECTORY_MIRROR, SampleDirectory, SamplePublishedUser, backfills, directory_mirror,
    directory_mirror_handle, known_users, publish_roster, retract_user,
};

pub use cron::{SampleCronJob, claimed_slots, completed_slots, cron_pods, cron_runs};

pub use roster::{KnownUserNoun, RosterUserView, RosterUsers};

pub use engine::{
    SAMPLE_JOB, SAMPLE_RELAY, boot_presence_engine, boot_render_engine, boot_sample_engine,
    engine_config,
};

pub use assignment::{
    Assignment, AssignmentFacts, AssignmentProjector, AssignmentRow, AssignmentView,
};
pub use engine::boot_pipeline_engine;
pub use gate::Gate;
pub use gated::{
    AssignmentVisibility, GatedAssignmentProjector, GatedAssignmentView, Mode, reasons,
};
pub use note::{Note, NoteFacts, NoteKey, NoteProjector, NoteView};
pub use outbox::{Relayed, delivered_event_ids, relayed_coords, stage_outbox_row};
pub use pipeline::{
    CloseWidget, CreateWidget, ImportWidgets, LockWidget, MintSecret, SampleFault,
    SampleReactionFault, ScheduleCreate, WidgetCreated, close_widget, create_widget,
    create_widget_coords, import_widgets, lock_widget, lock_widget_coords, mint_secret,
    schedule_create,
};
pub use presence::{
    Typing, TypingKey, TypingValue, TypingView, typing_key, typing_value, typing_window,
};
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
pub use widget::{Widget, WidgetFacts, WidgetProjector, WidgetRow, WidgetStore, WidgetView};
