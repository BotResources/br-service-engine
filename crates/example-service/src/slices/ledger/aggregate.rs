use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::gate::{Gate, Reason};
use service_engine::name::NounName;
use service_engine::wire::Noun;
use uuid::Uuid;

pub const EVENT_VERSION: i32 = 2;
pub const WOULD_GO_NEGATIVE: Reason = Reason::new("ledger_would_go_negative");

pub struct Ledger;

impl Noun for Ledger {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("ledger");
}

#[derive(Debug)]
pub enum LedgerError {
    Hydration(String),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hydration(detail) => write!(f, "hydration barrier: {detail}"),
        }
    }
}

impl std::error::Error for LedgerError {}

impl From<LedgerError> for EngineError {
    fn from(error: LedgerError) -> Self {
        EngineError::Service(Box::new(error))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum LedgerEvent {
    Recorded { amount: i64, author: Uuid },
}

#[derive(Debug, Clone)]
pub struct LedgerState {
    pub id: Uuid,
    pub org_id: Uuid,
    pub total: i64,
    pub last_author: Option<Uuid>,
    pub version: i64,
    pending: Vec<LedgerEvent>,
    went_negative: bool,
}

impl LedgerState {
    pub fn open(id: Uuid, org_id: Uuid) -> Self {
        Self {
            id,
            org_id,
            total: 0,
            last_author: None,
            version: 0,
            pending: Vec::new(),
            went_negative: false,
        }
    }

    pub fn from_snapshot(
        id: Uuid,
        org_id: Uuid,
        total: i64,
        author: Option<Uuid>,
        version: i64,
    ) -> Self {
        Self {
            id,
            org_id,
            total,
            last_author: author,
            version,
            pending: Vec::new(),
            went_negative: false,
        }
    }

    pub fn record_gate(&self, amount: i64) -> Gate {
        if self.total + amount < 0 {
            Gate::blocked(WOULD_GO_NEGATIVE)
        } else {
            Gate::allowed()
        }
    }

    pub fn record(&mut self, amount: i64, author: Uuid) -> Result<(), Reason> {
        self.record_gate(amount).require()?;
        self.total += amount;
        self.last_author = Some(author);
        self.version += 1;
        self.pending.push(LedgerEvent::Recorded { amount, author });
        Ok(())
    }

    pub fn apply(&mut self, event: &LedgerEvent) {
        match event {
            LedgerEvent::Recorded { amount, author } => {
                self.total += amount;
                self.last_author = Some(*author);
                if self.total < 0 {
                    self.went_negative = true;
                }
            }
        }
    }

    pub fn check_hydrated(&self) -> Result<(), LedgerError> {
        if self.went_negative {
            return Err(LedgerError::Hydration(
                "the running total dipped below zero during replay".into(),
            ));
        }
        if self.total < 0 {
            return Err(LedgerError::Hydration("the total is negative".into()));
        }
        Ok(())
    }

    pub fn pending(&self) -> &[LedgerEvent] {
        &self.pending
    }
}

pub fn upcast(version: i32, payload: &serde_json::Value) -> Result<LedgerEvent, LedgerError> {
    if version >= EVENT_VERSION {
        return serde_json::from_value(payload.clone())
            .map_err(|error| LedgerError::Hydration(format!("event v{version} decode: {error}")));
    }
    if version == 1 {
        let amount = payload
            .get("delta")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| LedgerError::Hydration("v1 Recorded has no integer 'delta'".into()))?;
        let author = payload
            .get("by")
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .unwrap_or(Uuid::nil());
        return Ok(LedgerEvent::Recorded { amount, author });
    }
    Err(LedgerError::Hydration(format!(
        "unknown event version {version}"
    )))
}
