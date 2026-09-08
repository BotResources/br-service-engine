use serde::Serialize;
use service_engine::gate::{ActionName, Affordances, Gate, Gated, Reason};
use uuid::Uuid;

use crate::sample::counter::event::CounterEvent;
use crate::sample::principal::SamplePrincipal;

pub const CLOSED: Reason = Reason::new("counter_closed");

const BUMP: ActionName = ActionName::from_static("bump");
const CLOSE: ActionName = ActionName::from_static("close");

#[derive(Debug)]
pub enum CounterError {
    Hydration(String),
}

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hydration(detail) => write!(f, "hydration barrier: {detail}"),
        }
    }
}

impl std::error::Error for CounterError {}

impl From<CounterError> for service_engine::error::EngineError {
    fn from(error: CounterError) -> Self {
        service_engine::error::EngineError::Service(Box::new(error))
    }
}

#[derive(Debug, Clone)]
pub struct CounterState {
    pub key: Uuid,
    pub tenant: Uuid,
    pub total: i64,
    pub closed: bool,
    pub last_author: Option<String>,
    pub version: i64,
    pending: Vec<CounterEvent>,
    bumped_after_close: bool,
}

impl CounterState {
    pub fn open(key: Uuid, tenant: Uuid) -> Self {
        Self {
            key,
            tenant,
            total: 0,
            closed: false,
            last_author: None,
            version: 0,
            pending: Vec::new(),
            bumped_after_close: false,
        }
    }

    pub fn from_row(
        key: Uuid,
        tenant: Uuid,
        total: i64,
        closed: bool,
        last_author: Option<String>,
        version: i64,
    ) -> Self {
        Self {
            key,
            tenant,
            total,
            closed,
            last_author,
            version,
            pending: Vec::new(),
            bumped_after_close: false,
        }
    }

    pub fn bump_gate(&self) -> Gate {
        if self.closed {
            Gate::blocked(CLOSED)
        } else {
            Gate::allowed()
        }
    }

    pub fn close_gate(&self) -> Gate {
        if self.closed {
            Gate::blocked(CLOSED)
        } else {
            Gate::allowed()
        }
    }

    pub fn bump(&mut self, amount: i64, author: String) -> Result<(), Reason> {
        self.bump_gate().require()?;
        self.total += amount;
        self.last_author = Some(author.clone());
        self.version += 1;
        self.pending.push(CounterEvent::Bumped { amount, author });
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), Reason> {
        self.close_gate().require()?;
        self.closed = true;
        self.version += 1;
        self.pending.push(CounterEvent::Closed);
        Ok(())
    }

    pub fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Bumped { amount, author } => {
                if self.closed {
                    self.bumped_after_close = true;
                }
                self.total += amount;
                self.last_author = Some(author.clone());
            }
            CounterEvent::Closed => self.closed = true,
        }
    }

    pub fn check_hydrated(&self) -> Result<(), CounterError> {
        if self.bumped_after_close {
            return Err(CounterError::Hydration(
                "a Bumped event was applied after the counter was Closed".into(),
            ));
        }
        if self.total < 0 {
            return Err(CounterError::Hydration("the total is negative".into()));
        }
        Ok(())
    }

    pub fn pending(&self) -> &[CounterEvent] {
        &self.pending
    }

    pub fn base_version(&self) -> i64 {
        self.version - self.pending.len() as i64
    }
}

impl Gated for CounterState {
    type Principal = SamplePrincipal;

    const ACTIONS: &'static [ActionName] = &[BUMP, CLOSE];

    fn gate(&self, action: ActionName, _principal: &SamplePrincipal) -> Option<Gate> {
        match action.as_str() {
            "bump" => Some(self.bump_gate()),
            "close" => Some(self.close_gate()),
            _ => None,
        }
    }

    fn affordances(&self, _principal: &SamplePrincipal) -> Affordances {
        Affordances::from_pairs([(BUMP, self.bump_gate()), (CLOSE, self.close_gate())])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CounterView {
    pub id: Uuid,
    pub total: i64,
    pub closed: bool,
    pub affordances: Affordances,
}

pub fn view_of(state: &CounterState, principal: &SamplePrincipal) -> CounterView {
    CounterView {
        id: state.key,
        total: state.total,
        closed: state.closed,
        affordances: state.affordances(principal),
    }
}
