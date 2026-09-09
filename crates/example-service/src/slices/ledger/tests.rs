use uuid::Uuid;

use super::aggregate::{LedgerEvent, LedgerState, upcast};

#[test]
fn recording_entries_accumulates_the_running_total() {
    let mut ledger = LedgerState::open(Uuid::now_v7(), Uuid::now_v7());
    ledger.record(5, Uuid::now_v7()).expect("first entry");
    ledger.record(3, Uuid::now_v7()).expect("second entry");
    assert_eq!(ledger.total, 8);
    assert_eq!(ledger.pending().len(), 2);
}

#[test]
fn an_entry_that_would_drive_the_total_below_zero_is_refused() {
    let mut ledger = LedgerState::open(Uuid::now_v7(), Uuid::now_v7());
    ledger.record(5, Uuid::now_v7()).unwrap();
    let refused = ledger.record(-100, Uuid::now_v7()).expect_err("gated");
    assert_eq!(refused.code(), "ledger_would_go_negative");
    assert_eq!(ledger.total, 5);
}

#[test]
fn replaying_events_reaches_the_same_total_and_passes_the_hydration_barrier() {
    let author = Uuid::now_v7();
    let mut ledger = LedgerState::open(Uuid::now_v7(), Uuid::now_v7());
    for event in [
        LedgerEvent::Recorded { amount: 4, author },
        LedgerEvent::Recorded { amount: 2, author },
    ] {
        ledger.apply(&event);
    }
    assert_eq!(ledger.total, 6);
    assert!(ledger.check_hydrated().is_ok());
}

#[test]
fn a_v1_event_upcasts_its_delta_and_by_fields_into_the_current_shape() {
    let author = Uuid::now_v7();
    let payload = serde_json::json!({ "delta": 7, "by": author.to_string() });
    let event = upcast(1, &payload).expect("v1 upcasts");
    assert_eq!(event, LedgerEvent::Recorded { amount: 7, author });
}
