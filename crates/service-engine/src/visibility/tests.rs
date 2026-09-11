use std::collections::BTreeSet;

use super::*;
use crate::population::Population;
use uuid::Uuid;

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

fn org(id: Uuid) -> CohortKey {
    CohortKey::of(&[("org", id)])
}

fn member(id: Uuid) -> CohortKey {
    CohortKey::of(&[("member", id)])
}

fn public() -> CohortKey {
    CohortKey::of::<&str>(&["public"])
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
fn the_window_keeps_exactly_the_keys_the_visible_predicate_admits() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    let keys = ProjectVisibility::visible_keys(candidates, &viewer);
    assert_eq!(keys, BTreeSet::from([mine.id]));
    assert!(ProjectVisibility::visible(&mine, &viewer));
    assert!(!ProjectVisibility::visible(&theirs, &viewer));
}

#[test]
fn window_produces_a_key_population_of_the_visible_subset() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    match ProjectVisibility::window(candidates, &viewer) {
        Population::Keys(keys) => assert_eq!(keys, BTreeSet::from([mine.id])),
        other => panic!("the derived window is a key set, got {other:?}"),
    }
}

#[test]
fn the_check_passes_when_the_window_is_derived_from_the_declaration() {
    let (viewer, mine, theirs) = scene();
    let candidates = vec![(mine.id, mine.clone()), (theirs.id, theirs.clone())];
    let window = ProjectVisibility::visible_keys(candidates.clone(), &viewer);
    check_window_matches_visibility::<ProjectVisibility, _, _>(&window, candidates, &viewer)
        .expect("a window built from the declaration agrees with it");
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
