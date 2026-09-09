use uuid::Uuid;

use super::aggregate::{ReplyCause, ReplyRow};

#[test]
fn a_fresh_reply_opens_in_the_streaming_state_with_no_text() {
    let reply = ReplyRow::open(Uuid::now_v7(), Uuid::now_v7());
    assert_eq!(reply.status, "streaming");
    assert!(reply.text.is_empty());
    assert!(reply.blob_ref.is_none());
}

#[test]
fn completing_a_reply_saves_the_sealed_text_and_reports_its_length() {
    let mut reply = ReplyRow::open(Uuid::now_v7(), Uuid::now_v7());
    let cause = reply.complete("Hello world".to_string());
    assert_eq!(reply.status, "complete");
    assert_eq!(reply.text, "Hello world");
    assert_eq!(cause, ReplyCause::Completed { chars: 11 });
}

#[test]
fn attaching_a_blob_records_the_reference_and_names_the_cause() {
    let mut reply = ReplyRow::open(Uuid::now_v7(), Uuid::now_v7());
    let blob = Uuid::now_v7();
    let cause = reply.attach(blob);
    assert_eq!(reply.blob_ref, Some(blob));
    assert_eq!(cause, ReplyCause::Attached);
}
