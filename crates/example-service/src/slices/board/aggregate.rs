use serde::{Deserialize, Serialize};
use service_engine::CohortKey;
use service_engine::gate::{Gate, Reason};
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::kernel::{AppPrincipal, scopes};

pub const NOT_ACTIVE: Reason = Reason::new("board_not_active");
pub const MISSING_SCOPE: Reason = Reason::new("missing_archive_scope");

pub struct Board;

impl Noun for Board {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("board");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoardState {
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardRow {
    pub id: Uuid,
    pub org_id: Uuid,
    pub name: String,
    pub is_public: bool,
    pub state: BoardState,
}

service_engine::gated! {
    BoardRow, AppPrincipal;
    "archive" => fn archive_gate(this, principal) {
        if !principal.has_scope(scopes::BOARD_ARCHIVE) {
            Gate::blocked(MISSING_SCOPE)
        } else if this.state != BoardState::Active {
            Gate::blocked(NOT_ACTIVE)
        } else {
            Gate::allowed()
        }
    }
}

impl BoardRow {
    pub fn archive(&mut self, principal: &AppPrincipal) -> Result<(), Reason> {
        self.archive_gate(principal).require()?;
        self.state = BoardState::Archived;
        Ok(())
    }
}

#[derive(Hash)]
enum Cohort {
    Org(Uuid),
    Member(Uuid),
    Public,
    Super,
}

impl Visibility for Board {
    type Row = BoardRow;
    type Principal = AppPrincipal;

    fn cohorts(row: &BoardRow) -> Cohorts {
        let mut cohorts = vec![
            CohortKey::of(&[Cohort::Org(row.org_id)]),
            CohortKey::of(&[Cohort::Member(row.id)]),
        ];
        if row.is_public {
            cohorts.push(CohortKey::of(&[Cohort::Public]));
        }
        cohorts
    }

    fn memberships(principal: &AppPrincipal) -> Cohorts {
        let mut cohorts = vec![
            CohortKey::of(&[Cohort::Org(principal.org())]),
            CohortKey::of(&[Cohort::Public]),
        ];
        for board in principal.boards() {
            cohorts.push(CohortKey::of(&[Cohort::Member(*board)]));
        }
        if principal.is_super_admin() {
            cohorts.push(CohortKey::of(&[Cohort::Super]));
        }
        cohorts
    }
}
