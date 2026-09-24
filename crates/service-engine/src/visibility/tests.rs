use std::collections::BTreeSet;

use uuid::Uuid;

use super::*;

#[derive(Clone)]
struct Project {
    org: Uuid,
    id: Uuid,
    public: bool,
}

struct Viewer {
    orgs: Vec<Uuid>,
    projects: Vec<Uuid>,
}

fn org(id: Uuid) -> Cohort {
    Cohort::uuid("org", id)
}

fn member(id: Uuid) -> Cohort {
    Cohort::uuid("member", id)
}

fn public() -> Cohort {
    Cohort::flag("public", true)
}

struct ProjectVisibility;

impl Visibility for ProjectVisibility {
    type Row = Project;
    type Principal = Viewer;

    fn cohorts(row: &Project) -> Cohorts {
        let mut cohorts = vec![org(row.org), member(row.id)];
        if row.public {
            cohorts.push(public());
        }
        cohorts
    }

    fn memberships(viewer: &Viewer) -> Cohorts {
        viewer
            .orgs
            .iter()
            .map(|id| org(*id))
            .chain(viewer.projects.iter().map(|id| member(*id)))
            .chain([public()])
            .collect()
    }
}

#[test]
fn a_row_is_visible_when_one_of_its_cohorts_meets_a_membership() {
    let home = Uuid::now_v7();
    let project = Project {
        org: home,
        id: Uuid::now_v7(),
        public: false,
    };
    let insider = Viewer {
        orgs: vec![home],
        projects: vec![],
    };
    assert!(ProjectVisibility::visible(&project, &insider));
}

#[test]
fn a_row_is_invisible_when_no_cohort_meets_any_membership() {
    let project = Project {
        org: Uuid::now_v7(),
        id: Uuid::now_v7(),
        public: false,
    };
    let outsider = Viewer {
        orgs: vec![Uuid::now_v7()],
        projects: vec![Uuid::now_v7()],
    };
    assert!(!ProjectVisibility::visible(&project, &outsider));
}

#[test]
fn a_public_cohort_makes_a_row_visible_to_everyone() {
    let project = Project {
        org: Uuid::now_v7(),
        id: Uuid::now_v7(),
        public: true,
    };
    let stranger = Viewer {
        orgs: vec![],
        projects: vec![],
    };
    assert!(ProjectVisibility::visible(&project, &stranger));
}

#[test]
fn losing_the_only_membership_that_matched_turns_a_row_invisible() {
    let home = Uuid::now_v7();
    let project = Project {
        org: home,
        id: Uuid::now_v7(),
        public: false,
    };
    let before = Viewer {
        orgs: vec![home],
        projects: vec![],
    };
    let after = Viewer {
        orgs: vec![Uuid::now_v7()],
        projects: vec![],
    };
    assert!(ProjectVisibility::visible(&project, &before));
    assert!(!ProjectVisibility::visible(&project, &after));
}

fn scene() -> (Viewer, Project, Project) {
    let home = Uuid::now_v7();
    let insider = Viewer {
        orgs: vec![home],
        projects: vec![],
    };
    let mine = Project {
        org: home,
        id: Uuid::now_v7(),
        public: false,
    };
    let theirs = Project {
        org: Uuid::now_v7(),
        id: Uuid::now_v7(),
        public: false,
    };
    (insider, mine, theirs)
}

#[test]
fn the_check_passes_when_the_window_holds_exactly_the_visible_keys() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    let window = BTreeSet::from([mine.id]);
    check_window_matches_visibility::<ProjectVisibility, _, _>(&window, candidates, &viewer)
        .expect("a window of exactly the visible keys agrees with the declaration");
}

#[test]
fn the_check_flags_a_window_that_leaks_an_invisible_key() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    let leaky = BTreeSet::from([mine.id, theirs.id]);
    let mismatch =
        check_window_matches_visibility::<ProjectVisibility, _, _>(&leaky, candidates, &viewer)
            .expect_err("a window holding an invisible key diverges from the declaration");
    assert_eq!(mismatch.only_in_window, vec![theirs.id]);
    assert!(mismatch.only_in_declaration.is_empty());
}

#[test]
fn the_check_flags_a_window_that_drops_a_visible_key() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    let empty = BTreeSet::new();
    let mismatch =
        check_window_matches_visibility::<ProjectVisibility, _, _>(&empty, candidates, &viewer)
            .expect_err("a window missing a visible key diverges from the declaration");
    assert_eq!(mismatch.only_in_declaration, vec![mine.id]);
    assert!(mismatch.only_in_window.is_empty());
}

#[test]
fn a_real_cohort_visibility_declares_no_open_access_reason_and_is_live_by_default() {
    assert_eq!(ProjectVisibility::OPEN_ACCESS_REASON, None);
    let live = ProjectVisibility::LIVE;
    let deps_empty = ProjectVisibility::DEPS.is_empty();
    assert!(live, "a cohort view is a live surface unless it opts out");
    assert!(
        deps_empty,
        "a visibility that reads no principal facts declares no deps"
    );
}

crate::open_access!(TestOpen = "filtered by an external mechanism, not a cohort");

#[test]
fn unrestricted_surfaces_its_access_reason_and_admits_every_row() {
    type Open = Unrestricted<Project, Viewer, TestOpen>;
    assert_eq!(
        <Open as Visibility>::OPEN_ACCESS_REASON,
        Some("filtered by an external mechanism, not a cohort"),
        "the reason is carried through to registration so the opt-out is reviewable"
    );
    assert_eq!(
        TestOpen::REASON,
        "filtered by an external mechanism, not a cohort"
    );
    let stranger = Viewer {
        orgs: vec![],
        projects: vec![],
    };
    let project = Project {
        org: Uuid::now_v7(),
        id: Uuid::now_v7(),
        public: false,
    };
    assert!(<Open as Visibility>::visible(&project, &stranger));
}

#[test]
fn the_check_holds_an_unrestricted_window_to_every_candidate_row() {
    type Open = Unrestricted<Project, Viewer, TestOpen>;
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    check_window_matches_visibility::<Open, _, _>(
        &BTreeSet::from([mine.id, theirs.id]),
        candidates.clone(),
        &viewer,
    )
    .expect("an open-access declaration admits every candidate row");
    let mismatch = check_window_matches_visibility::<Open, _, _>(
        &BTreeSet::from([mine.id]),
        candidates,
        &viewer,
    )
    .expect_err("a window that drops a row diverges from an open-access declaration");
    assert_eq!(mismatch.only_in_declaration, vec![theirs.id]);
}
