use serde::{Deserialize, Serialize};
use service_engine::gate::{Gate, Reason};
use service_engine::name::NounName;
use service_engine::wire::Noun;
use uuid::Uuid;

use super::CARD_ADVANCE;
use crate::kernel::AppPrincipal;

pub const DONE: Reason = Reason::new("card_already_done");
pub const MISSING_SCOPE: Reason = Reason::new("missing_advance_scope");

pub struct Card;

impl Noun for Card {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("card");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Todo,
    Doing,
    Done,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Todo => "todo",
            Status::Doing => "doing",
            Status::Done => "done",
        }
    }

    pub fn of(text: &str) -> Status {
        match text {
            "doing" => Status::Doing,
            "done" => Status::Done,
            _ => Status::Todo,
        }
    }

    fn next(self) -> Status {
        match self {
            Status::Todo => Status::Doing,
            Status::Doing | Status::Done => Status::Done,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CardEvent {
    Created { board_id: Uuid, title: String },
    Advanced { to: Status },
    DeadlinePassed,
}

#[derive(Debug, Clone)]
pub struct CardState {
    pub id: Uuid,
    pub board_id: Uuid,
    pub title: String,
    pub status: Status,
    pub version: i64,
    pending: Vec<CardEvent>,
}

impl CardState {
    pub fn open(id: Uuid, board_id: Uuid, title: String) -> Self {
        let mut card = Self {
            id,
            board_id,
            title: title.clone(),
            status: Status::Todo,
            version: 0,
            pending: Vec::new(),
        };
        card.version += 1;
        card.pending.push(CardEvent::Created { board_id, title });
        card
    }

    pub fn from_row(id: Uuid, board_id: Uuid, title: String, status: Status, version: i64) -> Self {
        Self {
            id,
            board_id,
            title,
            status,
            version,
            pending: Vec::new(),
        }
    }

    pub fn advance_gate(&self, principal: &AppPrincipal) -> Gate {
        if !principal.has_scope(CARD_ADVANCE) {
            Gate::blocked(MISSING_SCOPE)
        } else if self.status == Status::Done {
            Gate::blocked(DONE)
        } else {
            Gate::allowed()
        }
    }

    pub fn advance(&mut self, principal: &AppPrincipal) -> Result<(), Reason> {
        self.advance_gate(principal).require()?;
        self.status = self.status.next();
        self.version += 1;
        self.pending.push(CardEvent::Advanced { to: self.status });
        Ok(())
    }

    pub fn pass_deadline(&mut self) {
        if self.status != Status::Done {
            self.status = Status::Done;
            self.version += 1;
            self.pending.push(CardEvent::DeadlinePassed);
        }
    }

    pub fn pending(&self) -> &[CardEvent] {
        &self.pending
    }

    pub fn base_version(&self) -> i64 {
        self.version - self.pending.len() as i64
    }
}
