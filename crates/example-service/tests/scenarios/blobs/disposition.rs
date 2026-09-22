use std::time::{Duration, Instant};

use uuid::Uuid;

use super::{attachment_reference, post_form, seed_reply};
use crate::harness::{World, ok, passport};

#[tokio::test]
async fn the_download_disposition_is_reflected_in_the_presigned_get() {
    let world = World::start_blobs_swept("pod-blob-disposition", Duration::from_millis(150)).await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    seed_reply(&world, &pass, board, reply).await;

    let response = world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!,$c:String!){exampleAttachReply(replyId:$id,name:$n,contentType:$c)}",
            serde_json::json!({ "id": reply, "n": "note.txt", "c": "text/plain" }),
        )
        .await;
    let post = &ok(&response)["exampleAttachReply"];
    let endpoint = post["endpoint"].as_str().expect("a presigned endpoint");
    let fields = post["fields"].as_object().expect("presigned post fields");

    let payload = b"the inline bytes".to_vec();
    let status = post_form(&world.http, endpoint, fields, payload.clone(), "note.txt").await;
    assert!(status.is_success(), "the upload succeeded: {status}");

    let reference = attachment_reference(&world, &pass, reply).await;

    let deadline = Instant::now() + Duration::from_secs(15);
    let inline_url = loop {
        let inline = world
            .gql(
                &pass,
                "query($id:UUID!,$r:UUID!,$d:Disposition!){exampleReplyDownload(replyId:$id,reference:$r,disposition:$d)}",
                serde_json::json!({ "id": reply, "r": reference, "d": "INLINE" }),
            )
            .await;
        if let Some(url) = ok(&inline)["exampleReplyDownload"].as_str() {
            break url.to_string();
        }
        assert!(
            Instant::now() < deadline,
            "a policy-bearing kind becomes downloadable only once promoted; it never promoted",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        inline_url.contains("response-content-disposition=inline"),
        "the inline presign carries an inline content-disposition: {inline_url}"
    );

    let attachment = world
        .gql(
            &pass,
            "query($id:UUID!,$r:UUID!,$d:Disposition!){exampleReplyDownload(replyId:$id,reference:$r,disposition:$d)}",
            serde_json::json!({ "id": reply, "r": reference, "d": "ATTACHMENT" }),
        )
        .await;
    let attachment_url = ok(&attachment)["exampleReplyDownload"]
        .as_str()
        .expect("an attachment presigned download URL")
        .to_string();
    assert!(
        attachment_url.contains("response-content-disposition=attachment"),
        "the attachment presign carries an attachment content-disposition: {attachment_url}"
    );
    assert_ne!(
        inline_url, attachment_url,
        "the two dispositions mint different response-content-disposition URLs"
    );

    let got = world
        .http
        .get(inline_url)
        .send()
        .await
        .expect("GET the inline presigned URL")
        .bytes()
        .await
        .expect("read the downloaded bytes");
    assert_eq!(
        got.as_ref(),
        payload.as_slice(),
        "the inline presigned GET round-trips the bytes"
    );

    world.cleanup().await;
}
