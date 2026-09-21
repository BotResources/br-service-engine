use std::time::{Duration, Instant};

use service_engine::BlobRef;
use service_engine::blobs::BlobState;
use uuid::Uuid;

use crate::harness::{World, WorldOptions, ok, passport};

const PAYLOAD: &[u8] = b"verified-bytes";
const PAYLOAD_SHA256_HEX: &str = "35b1135247e25b36525c4f5bb88038cf310f05ab54aa450e406e9ef5c5fde911";
const WRONG_SHA256_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000000";

async fn post_form(
    http: &reqwest::Client,
    endpoint: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
    bytes: Vec<u8>,
    file_name: &str,
) -> reqwest::StatusCode {
    let mut form = reqwest::multipart::Form::new();
    for (name, value) in fields {
        form = form.text(name.clone(), value.as_str().unwrap().to_string());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(bytes).file_name(file_name.to_string()),
    );
    http.post(endpoint)
        .multipart(form)
        .send()
        .await
        .expect("the presigned upload reaches object storage")
        .status()
}

async fn seed_reply(world: &World, pass: &str, board: Uuid, reply: Uuid) {
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "attachments" }),
        )
        .await);
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);
}

async fn attachment_reference(world: &World, pass: &str, reply: Uuid) -> Uuid {
    let view = world
        .gql(
            pass,
            "query($id:UUID!){exampleReply(id:$id){attachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    let reference = ok(&view)["exampleReply"]["attachment"]
        .as_str()
        .expect("the reply view carries the committed attachment reference");
    Uuid::parse_str(reference).expect("the attachment reference is a UUID")
}

#[tokio::test]
async fn a_blob_is_attached_through_the_presigned_post_policy() {
    let world = World::start_with(
        "pod-blob",
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

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "attachments" }),
        )
        .await);

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

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
    let object_key = fields["key"].as_str().expect("the object key").to_string();

    let mut form = reqwest::multipart::Form::new();
    for (name, value) in fields {
        form = form.text(name.clone(), value.as_str().unwrap().to_string());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(b"the uploaded bytes".to_vec()).file_name("note.txt"),
    );

    let status = world
        .http
        .post(endpoint)
        .multipart(form)
        .send()
        .await
        .expect("the presigned upload reaches object storage")
        .status();
    assert!(
        status.is_success(),
        "the presigned POST upload succeeded, got {status}"
    );

    let bucket = world.blob_bucket.as_deref().unwrap();
    assert!(
        world
            .minio
            .as_ref()
            .unwrap()
            .object_exists(bucket, &object_key)
            .await,
        "the object is present in the bucket after the upload"
    );

    let view = world
        .gql(
            &pass,
            "query($id:UUID!){exampleReply(id:$id){hasAttachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(
        ok(&view)["exampleReply"]["hasAttachment"],
        true,
        "the reply view exposes the committed attachment reference"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_viewer_downloads_an_attachment_a_non_viewer_is_denied_the_same_reference() {
    let world = World::start_with(
        "pod-blob-download",
        WorldOptions {
            blobs: true,
            declare_scopes: false,
        },
    )
    .await;
    let viewer_org = Uuid::now_v7();
    let viewer = passport(Uuid::now_v7(), viewer_org, &[], false);
    let outsider = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();

    ok(&world
        .gql(
            &viewer,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "attachments" }),
        )
        .await);

    ok(&world
        .gql(
            &viewer,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    let response = world
        .gql(
            &viewer,
            "mutation($id:UUID!,$n:String!,$c:String!){exampleAttachReply(replyId:$id,name:$n,contentType:$c)}",
            serde_json::json!({ "id": reply, "n": "secret.txt", "c": "text/plain" }),
        )
        .await;
    let post = &ok(&response)["exampleAttachReply"];
    let endpoint = post["endpoint"].as_str().expect("a presigned endpoint");
    let fields = post["fields"].as_object().expect("presigned post fields");

    let payload = b"the attachment bytes, visible only to the viewer".to_vec();
    let mut form = reqwest::multipart::Form::new();
    for (name, value) in fields {
        form = form.text(name.clone(), value.as_str().unwrap().to_string());
    }
    form = form.part(
        "file",
        reqwest::multipart::Part::bytes(payload.clone()).file_name("secret.txt"),
    );
    let status = world
        .http
        .post(endpoint)
        .multipart(form)
        .send()
        .await
        .expect("the presigned upload reaches object storage")
        .status();
    assert!(
        status.is_success(),
        "the presigned POST upload succeeded, got {status}"
    );

    let view = world
        .gql(
            &viewer,
            "query($id:UUID!){exampleReply(id:$id){attachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    let reference = ok(&view)["exampleReply"]["attachment"]
        .as_str()
        .expect("the reply view carries the attachment reference")
        .to_string();

    let viewer_download = world
        .gql(
            &viewer,
            "query($id:UUID!,$r:UUID!){exampleReplyDownload(replyId:$id,reference:$r)}",
            serde_json::json!({ "id": reply, "r": reference }),
        )
        .await;
    let url = ok(&viewer_download)["exampleReplyDownload"]
        .as_str()
        .expect("a viewer of the reply gets a presigned download URL")
        .to_string();
    let got = world
        .http
        .get(url)
        .send()
        .await
        .expect("GET the presigned download URL")
        .bytes()
        .await
        .expect("read the downloaded bytes");
    assert_eq!(
        got.as_ref(),
        payload.as_slice(),
        "the viewer's presigned GET round-trips the attachment bytes",
    );

    let outsider_view = world
        .gql(
            &outsider,
            "query($id:UUID!){exampleReply(id:$id){attachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert!(
        ok(&outsider_view)["exampleReply"].is_null(),
        "a principal in another org cannot see the reply at all",
    );

    let outsider_download = world
        .gql(
            &outsider,
            "query($id:UUID!,$r:UUID!){exampleReplyDownload(replyId:$id,reference:$r)}",
            serde_json::json!({ "id": reply, "r": reference }),
        )
        .await;
    assert!(
        ok(&outsider_download)["exampleReplyDownload"].is_null(),
        "holding the reference is not a capability: a non-viewer is denied the download URL",
    );

    world.cleanup().await;
}

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

#[tokio::test]
async fn the_download_disposition_is_reflected_in_the_presigned_get() {
    let world = World::start_with(
        "pod-blob-disposition",
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

    let inline = world
        .gql(
            &pass,
            "query($id:UUID!,$r:UUID!,$d:Disposition!){exampleReplyDownload(replyId:$id,reference:$r,disposition:$d)}",
            serde_json::json!({ "id": reply, "r": reference, "d": "INLINE" }),
        )
        .await;
    let inline_url = ok(&inline)["exampleReplyDownload"]
        .as_str()
        .expect("an inline presigned download URL")
        .to_string();
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
