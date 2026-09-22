use std::time::{Duration, Instant};

use service_engine::BlobRef;
use service_engine::blobs::BlobState;
use uuid::Uuid;

use super::{
    PAYLOAD, PAYLOAD_SHA256_HEX, WRONG_SHA256_HEX, attachment_reference, post_form, seed_reply,
};
use crate::harness::{World, WorldOptions, ok, passport};

#[tokio::test]
async fn a_verified_upload_in_the_pending_window_heads_the_storage_checksum() {
    let world = World::start_with(
        "pod-blob-verified",
        WorldOptions {
            blobs: true,
            declare_scopes: false,
        },
    )
    .await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    seed_reply(&world, &pass, board, reply).await;

    let response = world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!,$c:String!,$e:UploadExpectationInput){exampleAttachReply(replyId:$id,name:$n,contentType:$c,expected:$e)}",
            serde_json::json!({
                "id": reply,
                "n": "verified.bin",
                "c": "application/octet-stream",
                "e": { "size": PAYLOAD.len(), "sha256Hex": PAYLOAD_SHA256_HEX },
            }),
        )
        .await;
    let post = &ok(&response)["exampleAttachReply"];
    let endpoint = post["endpoint"].as_str().expect("a presigned endpoint");
    let fields = post["fields"].as_object().expect("presigned post fields");

    let status = post_form(
        &world.http,
        endpoint,
        fields,
        PAYLOAD.to_vec(),
        "verified.bin",
    )
    .await;
    assert!(
        status.is_success(),
        "MinIO accepts the exact bytes that match the pinned checksum: {status}"
    );

    let reference = attachment_reference(&world, &pass, reply).await;
    let head = world
        .service
        .blob_reader()
        .head(BlobRef(reference))
        .await
        .expect("head does not error")
        .expect("the object landed, so head resolves in the pending window");
    assert_eq!(
        head.state,
        BlobState::Pending,
        "the reaper has not swept yet, so the row is still pending"
    );
    assert_eq!(
        head.size,
        PAYLOAD.len() as u64,
        "the size comes from a live storage HEAD"
    );
    assert_eq!(
        head.sha256.map(|d| d.to_hex()),
        Some(PAYLOAD_SHA256_HEX.to_string()),
        "the checksum comes from live storage, not the still-null row column"
    );
    assert_eq!(
        head.verified(),
        Some(true),
        "the storage size and checksum match the expectation"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_verified_upload_round_trips_and_is_promoted_to_uploaded() {
    let world = World::start_blobs_swept("pod-blob-swept", Duration::from_millis(150)).await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    seed_reply(&world, &pass, board, reply).await;

    let response = world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!,$c:String!,$e:UploadExpectationInput){exampleAttachReply(replyId:$id,name:$n,contentType:$c,expected:$e)}",
            serde_json::json!({
                "id": reply,
                "n": "verified.bin",
                "c": "application/octet-stream",
                "e": { "size": PAYLOAD.len(), "sha256Hex": PAYLOAD_SHA256_HEX },
            }),
        )
        .await;
    let post = &ok(&response)["exampleAttachReply"];
    let endpoint = post["endpoint"].as_str().expect("a presigned endpoint");
    let fields = post["fields"].as_object().expect("presigned post fields");

    let status = post_form(
        &world.http,
        endpoint,
        fields,
        PAYLOAD.to_vec(),
        "verified.bin",
    )
    .await;
    assert!(
        status.is_success(),
        "MinIO accepts the exact bytes: {status}"
    );

    let reference = attachment_reference(&world, &pass, reply).await;
    let reader = world.service.blob_reader();
    let deadline = Instant::now() + Duration::from_secs(15);
    let head = loop {
        let head = reader
            .head(BlobRef(reference))
            .await
            .expect("head does not error")
            .expect("head resolves once the object has landed");
        if head.state == BlobState::Uploaded {
            break head;
        }
        assert!(
            Instant::now() < deadline,
            "the reaper never promoted the verified upload to uploaded"
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
    };
    assert_eq!(
        head.verified(),
        Some(true),
        "the promoted upload verifies against its expectation"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn bytes_whose_checksum_does_not_match_the_expectation_are_refused_by_the_store() {
    let world = World::start_with(
        "pod-blob-wrong-sha",
        WorldOptions {
            blobs: true,
            declare_scopes: false,
        },
    )
    .await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    seed_reply(&world, &pass, board, reply).await;

    let response = world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!,$c:String!,$e:UploadExpectationInput){exampleAttachReply(replyId:$id,name:$n,contentType:$c,expected:$e)}",
            serde_json::json!({
                "id": reply,
                "n": "wrong.bin",
                "c": "application/octet-stream",
                "e": { "size": PAYLOAD.len(), "sha256Hex": WRONG_SHA256_HEX },
            }),
        )
        .await;
    let post = &ok(&response)["exampleAttachReply"];
    let endpoint = post["endpoint"].as_str().expect("a presigned endpoint");
    let fields = post["fields"].as_object().expect("presigned post fields");

    let status = post_form(&world.http, endpoint, fields, PAYLOAD.to_vec(), "wrong.bin").await;
    assert!(
        !status.is_success(),
        "MinIO refuses bytes whose checksum does not match the pinned expectation: {status}"
    );

    let reference = attachment_reference(&world, &pass, reply).await;
    let head = world
        .service
        .blob_reader()
        .head(BlobRef(reference))
        .await
        .expect("head does not error");
    assert!(
        head.is_none(),
        "the object never landed, so head resolves to None"
    );

    let download = world
        .gql(
            &pass,
            "query($id:UUID!,$r:UUID!){exampleReplyDownload(replyId:$id,reference:$r)}",
            serde_json::json!({ "id": reply, "r": reference }),
        )
        .await;
    assert!(
        ok(&download)["exampleReplyDownload"].is_null(),
        "a pending row with no object mints no download URL"
    );

    world.cleanup().await;
}
