pub mod assignment;
pub mod blob;
pub mod blob_view;
pub mod counter;
pub mod cron;
pub mod engine;
pub mod engine_persistence;
pub mod erase;
pub mod gate;
pub mod gated;
pub mod graphql;
pub mod mirror;
pub mod note;
pub mod offer;
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
    "sample_counter_crud",
    "sample_counter_soft",
    "sample_counter_soft_fact",
    "sample_counter_full_event",
    "sample_counter_full_snapshot",
    "sample_doc",
    "sample_erase_note",
    "sample_erase_memo",
    "sample_erase_memo_fact",
    "sample_erase_ledger_event",
    "sample_erase_ledger_snapshot",
];

pub use mirror::{
    DIRECTORY_MIRROR, SampleDirectory, SamplePublishedUser, backfills, directory_mirror,
    directory_mirror_handle, known_users, publish_roster, retract_user,
};

pub use counter::{
    BumpCrud, BumpFull, BumpFullCmd, BumpSoft, CoarseCounterFault, CounterError, CounterEvent,
    CounterFault, CounterState, CounterView, CrudCounter, FullCounter, FullCounterProjector,
    OpenCrud, OpenFull, OpenSoft, SoftCounter, bump_full_coords, erase_author, open_crud,
    open_full, open_soft, replay_from_scratch,
};

pub use cron::{SampleCronJob, claimed_slots, completed_slots, cron_pods, cron_runs};

pub use roster::{KnownUserNoun, RosterUserView, RosterUsers};

pub use blob::{
    AttachDoc, AttachOwnedDoc, Attachment, DeleteDoc, DetachDoc, DocRow, DocStore, RepointDoc,
    attach_doc, attach_owned_doc, delete_doc, detach_doc, repoint_doc,
};
pub use blob_view::{Doc, DocProjector, DocView};
pub use engine::{
    SAMPLE_JOB, SAMPLE_RELAY, boot_blob_engine, boot_offer_engine, boot_offer_engine_reconciling,
    boot_presence_engine, boot_render_engine, boot_sample_engine, engine_config,
};
pub use engine_persistence::{boot_persistence_engine, boot_serialization_engine};

pub use assignment::{
    Assignment, AssignmentFacts, AssignmentProjector, AssignmentRow, AssignmentStore,
    AssignmentView,
};
pub use engine::boot_pipeline_engine;
pub use gate::Gate;
pub use gated::{
    AssignmentVisibility, GatedAssignmentProjector, GatedAssignmentView, Mode, VisibleAssignments,
    reasons,
};
pub use note::{Note, NoteFacts, NoteKey, NoteProjector, NoteView};
pub use offer::{
    MintThenReject, PublishedWidget, WidgetOffer, mint_then_reject, offer_dirty_keys,
    published_widget, seed_bucket, widget_key,
};
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
