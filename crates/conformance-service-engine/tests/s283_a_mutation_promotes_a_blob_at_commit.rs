#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;
#[allow(dead_code)]
mod promote_support;

use conformance_service_engine::sample::{PromoteDoc, PromotePolicy, PromoteReference};
use promote_support::{PAYLOAD, Rig, Running, SAME_LENGTH_OTHER_BYTES};
use service_engine::BlobPromotion;
use uuid::Uuid;

#[tokio::test]
async fn s283a_a_verified_match_is_promoted_at_commit_and_the_policy_runs_as_the_service() {
    let rig = Rig::new("s283a").await;
    let running = Running::start(rig.engine("s283a", PromotePolicy::Probe).await).await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    rig.land(upload).await;

    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(answer, BlobPromotion::Promoted);
    assert_eq!(
        rig.state(reference).await.0,
        "uploaded",
        "the row is uploaded the moment the mutation returns, with no reaper sweep",
    );

    let probes = rig.probes(reference).await;
    assert_eq!(
        probes.len(),
        1,
        "the policy ran once, in the mutation's transaction"
    );
    let event = &probes[0];
    let metadata = &event["metadata"];
    assert_eq!(
        metadata["actor_kind"], "service",
        "the policy's emit goes out as the service, never as the calling human: {metadata}",
    );
    assert_eq!(
        metadata["correlation_id"],
        serde_json::Value::String(reference.to_string()),
        "the policy's emit correlates on the blob id, as under the reaper",
    );
    assert!(
        metadata["causation_id"].is_null(),
        "no causation: {metadata}"
    );
    let data = &event["payload"];
    assert_eq!(
        data["doc"],
        serde_json::Value::String(doc.to_string()),
        "the policy reads the referencing row the mutation committed alongside",
    );
    assert!(
        data["tenant_setting"].is_null() && data["user_setting"].is_null(),
        "the policy sees no principal and no RLS session context of the caller: {data}",
    );
    assert_eq!(
        data["role_switched"], false,
        "the policy runs as the service role"
    );

    let again = rig
        .promote(&running.executor, doc)
        .await
        .expect("a second promote commits");
    assert_eq!(
        again,
        BlobPromotion::AlreadyUploaded,
        "a promotion holds once"
    );
    assert_eq!(
        rig.probes(reference).await.len(),
        1,
        "the policy never runs twice"
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s283b_a_checksum_mismatch_fails_the_row_and_deletes_the_object_after_commit() {
    let rig = Rig::new("s283b").await;
    let running = Running::start(rig.engine("s283b", PromotePolicy::Probe).await).await;

    let (doc, reference, _upload) = rig.attach(&running.executor).await;
    let object_key = rig.object_key(reference).await;
    rig.minio
        .put_object(&rig.bucket, &object_key, SAME_LENGTH_OTHER_BYTES.to_vec())
        .await;

    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(
        answer,
        BlobPromotion::Failed {
            reason: "sha256_mismatch".to_string()
        }
    );
    assert_eq!(
        rig.state(reference).await,
        ("failed".to_string(), Some("sha256_mismatch".to_string())),
    );
    assert!(
        !rig.minio.object_exists(&rig.bucket, &object_key).await,
        "the mismatching object is deleted once the mutation committed",
    );
    assert_eq!(
        rig.all_probes().await,
        0,
        "a mismatch never runs the policy"
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s283c_an_object_not_landed_yet_leaves_the_row_pending() {
    let rig = Rig::new("s283c").await;
    let running = Running::start(rig.engine("s283c", PromotePolicy::Probe).await).await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(answer, BlobPromotion::NotLanded);
    assert_eq!(rig.state(reference).await.0, "pending");
    assert_eq!(rig.all_probes().await, 0);

    rig.land(upload).await;
    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(
        answer,
        BlobPromotion::Promoted,
        "once landed, the same reference promotes"
    );

    let unknown = running
        .executor
        .run::<PromoteReference>(
            rig.principal().await,
            PromoteReference {
                reference: Uuid::now_v7(),
            },
        )
        .await
        .expect_err("an unknown reference fails the mutation");
    assert_eq!(
        unknown.code(),
        None,
        "an unknown reference is an internal fault"
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s283d_a_policy_refusal_fails_the_row_and_the_mutation_still_commits() {
    let rig = Rig::new("s283d").await;
    let running = Running::start(rig.engine("s283d", PromotePolicy::Refuse).await).await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    let object_key = rig.object_key(reference).await;
    rig.land(upload).await;

    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(
        answer,
        BlobPromotion::Failed {
            reason: "policy:UPLOAD_REFUSED".to_string()
        }
    );
    assert_eq!(
        rig.state(reference).await,
        (
            "failed".to_string(),
            Some("policy:UPLOAD_REFUSED".to_string())
        ),
    );
    assert!(
        !rig.minio.object_exists(&rig.bucket, &object_key).await,
        "the refused object is deleted once the mutation committed",
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s283e_a_policy_fault_fails_the_mutation_and_leaves_the_row_pending() {
    let rig = Rig::new("s283e").await;
    let running = Running::start(rig.engine("s283e", PromotePolicy::Fault).await).await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    let object_key = rig.object_key(reference).await;
    rig.land(upload).await;

    let error = rig
        .promote(&running.executor, doc)
        .await
        .expect_err("a policy fault fails the mutation");
    assert_eq!(
        error.code(),
        None,
        "a fault answers INTERNAL, not a refusal"
    );
    assert_eq!(rig.state(reference).await.0, "pending");

    let swallowed = running
        .executor
        .run::<PromoteDoc>(
            rig.principal().await,
            PromoteDoc {
                doc,
                swallow_error: true,
            },
        )
        .await
        .expect_err("a handler that swallows the fault still fails the mutation");
    assert_eq!(swallowed.code(), None);
    assert_eq!(rig.state(reference).await.0, "pending");
    assert!(
        rig.minio.object_exists(&rig.bucket, &object_key).await,
        "a fault never deletes the object",
    );

    rig.finish(running).await;
}

#[tokio::test]
async fn s283f_a_kind_without_a_policy_is_promoted_without_one() {
    let rig = Rig::new("s283f").await;
    let running = Running::start(rig.engine("s283f", PromotePolicy::None).await).await;

    let (doc, reference, upload) = rig.attach(&running.executor).await;
    rig.land(upload).await;
    let answer = rig
        .promote(&running.executor, doc)
        .await
        .expect("the promote mutation commits");
    assert_eq!(answer, BlobPromotion::Promoted);
    let (state, _) = rig.state(reference).await;
    assert_eq!(state, "uploaded");
    let (size, sha): (Option<i64>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT size, sha256 FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&rig.pool)
            .await
            .expect("read the recorded head");
    assert_eq!(
        size,
        Some(PAYLOAD.len() as i64),
        "the storage size is recorded"
    );
    assert!(sha.is_some(), "the storage checksum is recorded");

    rig.finish(running).await;
}
