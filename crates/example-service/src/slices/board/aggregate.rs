use serde::{Deserialize, Serialize};
use service_engine::Cohort;
use service_engine::gate::{Gate, Reason};
use service_engine::impact::Deps;
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use uuid::Uuid;

use super::BOARD_ARCHIVE;
use super::BoardMemberships;
use crate::kernel::AppPrincipal;

pub const NOT_ACTIVE: Reason = Reason::new("BOARD_NOT_ACTIVE");
pub const MISSING_SCOPE: Reason = Reason::new("MISSING_ARCHIVE_SCOPE");
pub const NOT_A_MEMBER: Reason = Reason::new("NOT_A_BOARD_MEMBER");

pub(super) const ORG: &str = "org";
pub(super) const MEMBER: &str = "member";
pub(super) const PUBLIC: &str = "public";

pub(super) const MEMBERSHIP_DEPS: Deps = Deps::from_bits(1);

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
#[serde(tag = "kind")]
pub enum BoardCause {
    Created,
    Archived,
    InviteMinted,
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
        if !principal.has_scope(BOARD_ARCHIVE) {
            Gate::blocked(MISSING_SCOPE)
        } else if this.state != BoardState::Active {
            Gate::blocked(NOT_ACTIVE)
        } else {
            Gate::allowed()
        }
    }
    "manage_members" => fn manage_members_gate(this, principal) {
        let is_member = principal
            .facts()
            .get::<BoardMemberships>()
            .is_some_and(|BoardMemberships(boards)| boards.contains(&this.id));
        if principal.is_super_admin() || is_member {
            Gate::allowed()
        } else {
            Gate::blocked(NOT_A_MEMBER)
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

impl Visibility for Board {
    type Row = BoardRow;
    type Principal = AppPrincipal;

    const DEPS: Deps = MEMBERSHIP_DEPS;

    fn cohorts(row: &BoardRow) -> Cohorts {
        let mut cohorts = vec![Cohort::uuid(ORG, row.org_id), Cohort::uuid(MEMBER, row.id)];
        if row.is_public {
            cohorts.push(Cohort::flag(PUBLIC, true));
        }
        cohorts
    }

    fn memberships(principal: &AppPrincipal) -> Cohorts {
        let mut cohorts = vec![
            Cohort::uuid(ORG, principal.org()),
            Cohort::flag(PUBLIC, true),
        ];
        if let Some(BoardMemberships(boards)) = principal.facts().get::<BoardMemberships>() {
            for board in boards {
                cohorts.push(Cohort::uuid(MEMBER, *board));
            }
        }
        cohorts
    }
}
