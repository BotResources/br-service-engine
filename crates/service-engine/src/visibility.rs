use crate::cohort::CohortKey;

pub type Cohorts = Vec<CohortKey>;

pub trait Visibility: Send + Sync + 'static {
    type Row;
    type Principal;

    fn cohorts(row: &Self::Row) -> Cohorts;
    fn memberships(principal: &Self::Principal) -> Cohorts;
}
