use std::collections::BTreeSet;

use crate::cohort::CohortKey;

pub type Cohorts = Vec<CohortKey>;

pub trait Visibility: Send + Sync + 'static {
    type Row;
    type Principal;

    fn cohorts(row: &Self::Row) -> Cohorts;
    fn memberships(principal: &Self::Principal) -> Cohorts;

    fn visible(row: &Self::Row, principal: &Self::Principal) -> bool {
        let memberships: BTreeSet<CohortKey> = Self::memberships(principal).into_iter().collect();
        Self::cohorts(row)
            .into_iter()
            .any(|cohort| memberships.contains(&cohort))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

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
}
