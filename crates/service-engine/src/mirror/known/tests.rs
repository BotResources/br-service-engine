use super::*;
use crate::mirror::bind::col;

struct KnownPersonRow {
    id: Uuid,
    email: String,
    note: Option<String>,
}

impl KnownRow for KnownPersonRow {
    const TABLE: &'static str = "known_persons";
    const NAMESPACE: &'static str = "example.person";
    const KEY: &'static [&'static str] = &["user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = Some(PrincipalColumn {
        column: "user_id",
        deps: Deps::from_bits(0b10),
    });

    fn key(&self) -> Vec<Column> {
        vec![col("user_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![
            col("email", self.email.clone()),
            col("note", self.note.clone()),
        ]
    }
}

struct MisplacedPrincipalRow;

impl KnownRow for MisplacedPrincipalRow {
    const TABLE: &'static str = "known_badges";
    const NAMESPACE: &'static str = "example.badge";
    const KEY: &'static [&'static str] = &["badge_id"];
    const PRINCIPAL: Option<PrincipalColumn> = Some(PrincipalColumn {
        column: "holder_id",
        deps: Deps::EMPTY,
    });

    fn key(&self) -> Vec<Column> {
        vec![col("badge_id", "b")]
    }

    fn values(&self) -> Vec<Column> {
        Vec::new()
    }
}

#[test]
fn the_foreign_key_of_a_single_key_row_is_the_key_value() {
    let id = Uuid::now_v7();
    let row = KnownPersonRow {
        id,
        email: "a@example.test".to_string(),
        note: None,
    };
    assert_eq!(foreign_key_of(&row.key()), id.to_string());
}

#[test]
fn a_key_that_names_other_columns_than_the_declared_key_is_refused() {
    let refused = keyed::<KnownPersonRow>(vec![col("email", "a@example.test")]);
    assert!(matches!(refused, Err(EngineError::Config(_))));
    let accepted = keyed::<KnownPersonRow>(vec![col("user_id", Uuid::now_v7())]);
    assert!(accepted.is_ok());
}

#[test]
fn a_principal_column_that_is_not_a_uuid_key_column_is_refused() {
    let outside_the_key = keyed::<MisplacedPrincipalRow>(MisplacedPrincipalRow.key());
    assert!(matches!(outside_the_key, Err(EngineError::Config(_))));
    let not_a_uuid = keyed::<KnownPersonRow>(vec![col("user_id", "someone")]);
    assert!(matches!(not_a_uuid, Err(EngineError::Config(_))));
}

#[test]
fn a_touched_row_stages_its_foreign_key_and_its_principal_facts() {
    let id = Uuid::now_v7();
    let impacts = impacts_of::<KnownPersonRow>(&[id.to_string()]).expect("a uuid principal");
    assert_eq!(
        impacts,
        vec![
            Impact::foreign(ForeignKey::new("example.person", &id.to_string()).unwrap()),
            Impact::principal_facts(id.into(), Deps::from_bits(0b10)),
        ]
    );
}

#[test]
fn a_multi_key_foreign_key_joins_its_columns() {
    let key = vec![col("group_id", "g"), col("user_id", "u")];
    assert_eq!(foreign_key_of(&key), "g/u");
}

#[test]
fn a_none_option_binds_null() {
    assert!(matches!(Bind::from(Option::<String>::None), Bind::Null));
    assert!(matches!(Bind::from(Some(4_i64)), Bind::Int(4)));
}
