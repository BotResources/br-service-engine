use uuid::Uuid;

use super::*;
use crate::mirror::bind::col;
use crate::mirror::known::PrincipalColumn;

struct MemberRow {
    project: Uuid,
    user: Uuid,
    admin: bool,
}

impl KnownRow for MemberRow {
    const TABLE: &'static str = "known_members";
    const NAMESPACE: &'static str = "test.member";
    const KEY: &'static [&'static str] = &["project_id", "user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("project_id", self.project), col("user_id", self.user)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("is_admin", self.admin)]
    }
}

struct StampedRow(chrono::DateTime<chrono::Utc>);

impl KnownRow for StampedRow {
    const TABLE: &'static str = "known_stamps";
    const NAMESPACE: &'static str = "test.stamp";
    const KEY: &'static [&'static str] = &["at"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("at", self.0)]
    }

    fn values(&self) -> Vec<Column> {
        Vec::new()
    }
}

struct PathRow(&'static str, &'static str);

impl KnownRow for PathRow {
    const TABLE: &'static str = "known_paths";
    const NAMESPACE: &'static str = "test.path";
    const KEY: &'static [&'static str] = &["head", "tail"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("head", self.0), col("tail", self.1)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("label", "same")]
    }
}

fn member(project: Uuid, user: Uuid, admin: bool) -> MemberRow {
    MemberRow {
        project,
        user,
        admin,
    }
}

fn refused<T>(outcome: Result<T, EngineError>) -> bool {
    matches!(outcome, Err(EngineError::Config(_)))
}

#[test]
fn an_exact_duplicate_row_is_admitted_once() {
    let (project, user) = (Uuid::now_v7(), Uuid::now_v7());
    let admitted = admit::<MemberRow>(
        &[col("project_id", project)],
        vec![member(project, user, true), member(project, user, true)],
    )
    .expect("an exact duplicate is harmless");
    assert_eq!(admitted.len(), 1);
}

#[test]
fn two_composite_keys_that_render_alike_are_both_admitted() {
    let admitted = admit::<PathRow>(&[], vec![PathRow("a/b", "c"), PathRow("a", "b/c")])
        .expect("two distinct keys are two rows");
    assert_eq!(admitted.len(), 2);
}

#[test]
fn two_rows_with_one_key_and_different_values_are_refused() {
    let (project, user) = (Uuid::now_v7(), Uuid::now_v7());
    assert!(refused(admit::<MemberRow>(
        &[col("project_id", project)],
        vec![member(project, user, true), member(project, user, false)],
    )));
}

#[test]
fn a_row_outside_the_scope_is_refused() {
    let (project, elsewhere) = (Uuid::now_v7(), Uuid::now_v7());
    assert!(refused(admit::<MemberRow>(
        &[col("project_id", project)],
        vec![member(elsewhere, Uuid::now_v7(), false)],
    )));
}

#[test]
fn a_key_whose_sql_text_differs_from_its_rendering_is_refused() {
    assert!(refused(admit::<StampedRow>(
        &[],
        vec![StampedRow(chrono::Utc::now())],
    )));
}

#[test]
fn a_null_scope_column_is_refused_even_with_no_rows() {
    assert!(refused(admit::<MemberRow>(
        &[col("project_id", Option::<Uuid>::None)],
        Vec::new(),
    )));
}
