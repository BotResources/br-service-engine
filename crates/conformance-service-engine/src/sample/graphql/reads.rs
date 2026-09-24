use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Context, MergedObject, Object, Result};
use service_engine::error::EngineError;
use service_engine::graphql::SliceFragment;
use service_engine::name::ProjectorName;
use service_engine::nats::Nats;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use service_engine::{Engine, Query, Readiness, ReadinessHandle, engine_schema};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentRow, AssignmentStore, AssignmentView};
use crate::sample::gated::all_assignment_keys;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::visible::VisibleAssignments;

use super::boot::{GraphqlService, base_config, free_loopback_addr, sample_prefix};
use super::snapshot::SnapshotReads;

service_engine::open_access!(
    pub TenantPolicy = "sample_assignment rows are filtered by the tenant RLS policy"
);

#[derive(Default)]
pub struct RlsOnlyAssignments;

impl Projector for RlsOnlyAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = Unrestricted<AssignmentRow, SamplePrincipal, TenantPolicy>;

    const NAME: ProjectorName = ProjectorName::from_static("rls_only_assignments");
    const RLS: bool = true;

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            all_assignment_keys(cx.pool(), cx.limit_all()).await?,
        ))
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view_of(row))
    }
}

fn view_of(row: &AssignmentRow) -> AssignmentView {
    AssignmentView {
        id: row.id,
        title: row.title.clone(),
        closed: row.closed,
        can_close: !row.closed,
    }
}

#[derive(MergedObject, Default)]
pub struct ReadsQueryRoot(GatedReads, SnapshotReads);

#[derive(Default)]
pub struct GatedReads;

#[Object]
impl GatedReads {
    async fn sample_visible_assignment(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<AssignmentView>> {
        let row = Query::<SamplePrincipal>::new(ctx)?
            .load_visible::<VisibleAssignments>(&id)
            .await?;
        Ok(row.as_ref().map(view_of))
    }

    async fn sample_rls_visible_assignment(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<AssignmentView>> {
        let row = Query::<SamplePrincipal>::new(ctx)?
            .load_visible::<RlsOnlyAssignments>(&id)
            .await?;
        Ok(row.as_ref().map(view_of))
    }

    async fn sample_assignment_journal(&self, ctx: &Context<'_>) -> Result<Vec<Uuid>> {
        Query::<SamplePrincipal>::new(ctx)?
            .read_under_rls(|conn| {
                Box::pin(async move {
                    Ok(
                        sqlx::query_scalar("SELECT id FROM sample_assignment ORDER BY id")
                            .fetch_all(conn)
                            .await?,
                    )
                })
            })
            .await
    }

    async fn sample_journal_write_attempt(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        tenant: Uuid,
    ) -> Result<bool> {
        Query::<SamplePrincipal>::new(ctx)?
            .read_under_rls(move |conn| {
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO sample_assignment (id, tenant_id, title) VALUES ($1, $2, 'x')",
                    )
                    .bind(id)
                    .bind(tenant)
                    .execute(conn)
                    .await?;
                    Ok(true)
                })
            })
            .await
    }

    async fn sample_assignment_notes(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<Vec<String>>> {
        Query::<SamplePrincipal>::new(ctx)?
            .behind::<VisibleAssignments>(&id)
            .read(|assignment, conn| {
                Box::pin(async move {
                    Ok(sqlx::query_scalar(
                        "SELECT body FROM sample_note WHERE assignment_id = $1 ORDER BY seq",
                    )
                    .bind(assignment.id)
                    .fetch_all(conn)
                    .await?)
                })
            })
            .await
    }

    async fn sample_note_write_attempt(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<bool>> {
        Query::<SamplePrincipal>::new(ctx)?
            .behind::<VisibleAssignments>(&id)
            .read(|assignment, conn| {
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO sample_note (assignment_id, seq, body) VALUES ($1, 1, 'x')",
                    )
                    .bind(assignment.id)
                    .execute(conn)
                    .await?;
                    Ok(true)
                })
            })
            .await
    }
}

fn reads_slice() -> SliceFragment {
    SliceFragment::from_claims(
        "reads",
        [
            "sampleVisibleAssignment",
            "sampleRlsVisibleAssignment",
            "sampleAssignmentJournal",
            "sampleJournalWriteAttempt",
            "sampleAssignmentNotes",
            "sampleNoteWriteAttempt",
            "sampleNotesAcrossLatch",
            "sampleJournalAcrossLatch",
        ]
        .map(str::to_string)
        .into(),
        vec!["AssignmentView".to_string()],
    )
}

pub async fn boot_reads_service(
    db: &crate::infra::TestDb,
    nats: Nats,
    pod: &str,
    with_rls_applier: bool,
) -> GraphqlService {
    let addr = free_loopback_addr().await;
    let config = base_config("se_reads", pod).with_http_addr(addr);
    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the reads engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    if with_rls_applier {
        engine
            .register_rls(SampleRls)
            .expect("register the RLS applier the RLS reads run under");
    }
    engine
        .declare_root_prefix(sample_prefix())
        .expect("declare the sample root prefix");
    engine
        .register_schema_slice(reads_slice())
        .expect("the reads slice owns its root fields and type");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        ReadsQueryRoot::default(),
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());
    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reads engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}
