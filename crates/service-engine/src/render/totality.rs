use crate::principal::Principal;
use crate::render::pass::PassContext;
use crate::render::plan::PlannedWindow;

#[cfg(debug_assertions)]
pub(crate) fn debug_assert_group_totality<P: Principal>(
    ctx: &PassContext<'_, P>,
    window: &PlannedWindow<P>,
) {
    use crate::cohort::CohortKey;

    let Some(projector) = ctx.registry.projector(&window.projector) else {
        return;
    };
    let recomputed = if window.rls {
        CohortKey::principal(window.representative.id())
    } else {
        projector.cohort(&window.representative)
    };
    debug_assert!(
        recomputed == window.cohort,
        "a session must land in exactly one cohort: recomputing the cohort key for its principal \
         gives a value other than the one it was grouped under, so cohort() is not a pure \
         function of the principal and grouping would split or merge sessions wrongly"
    );
}

#[cfg(not(debug_assertions))]
pub(crate) fn debug_assert_group_totality<P: Principal>(
    _ctx: &PassContext<'_, P>,
    _window: &PlannedWindow<P>,
) {
}
