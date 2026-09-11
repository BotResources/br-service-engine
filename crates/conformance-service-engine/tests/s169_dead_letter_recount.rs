use conformance_service_engine::infra::TestDb;
use conformance_service_engine::metrics_probe;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use service_engine::metrics::DEAD_LETTERS_TOTAL;
use uuid::Uuid;

#[tokio::test]
async fn s169_a_stuck_worker_re_recorded_every_beat_counts_once_not_once_per_restart() {
    let probe = metrics_probe::install();
    let db = TestDb::fresh().await;
    let dead_letters = DeadLetters::new(db.app_pool().clone());

    let baseline = probe.labelled_total(DEAD_LETTERS_TOTAL, "source", "mirror");
    let reaction = "roster";
    let stuck = Uuid::now_v7();

    dead_letters
        .record_work(DeadLetterSource::Mirror, reaction, reaction, stuck, "stuck")
        .await
        .expect("the first dead letter records");
    let after_first = probe.labelled_total(DEAD_LETTERS_TOTAL, "source", "mirror");
    assert_eq!(
        after_first,
        baseline + 1,
        "a fresh dead letter increments the by-source counter once"
    );

    for _ in 0..4 {
        dead_letters
            .record_work(
                DeadLetterSource::Mirror,
                reaction,
                reaction,
                stuck,
                "still stuck",
            )
            .await
            .expect("a stuck worker re-records the same work on the next beat");
    }
    assert_eq!(
        probe.labelled_total(DEAD_LETTERS_TOTAL, "source", "mirror"),
        after_first,
        "re-recording the same (reaction, message_id) updates the row in place and never \
         re-increments the counter, so a mirror stuck past its threshold does not overcount"
    );
    assert_eq!(
        dead_letters
            .list(Some(DeadLetterSource::Mirror), 16)
            .await
            .expect("list the mirror dead letters")
            .len(),
        1,
        "the table holds exactly one row for the one stuck mirror"
    );

    dead_letters
        .record_work(
            DeadLetterSource::Mirror,
            reaction,
            reaction,
            Uuid::now_v7(),
            "a different stuck mirror",
        )
        .await
        .expect("a genuinely distinct dead letter records");
    assert_eq!(
        probe.labelled_total(DEAD_LETTERS_TOTAL, "source", "mirror"),
        after_first + 1,
        "a distinct dead letter still increments the counter, so the guard is on re-record, not \
         on the source"
    );

    db.cleanup().await;
}
