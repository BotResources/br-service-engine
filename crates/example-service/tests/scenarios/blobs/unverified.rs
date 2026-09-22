use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::harness::{World, WorldOptions, ok, passport};

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
    let world = World::start_blobs_swept("pod-blob-download", Duration::from_millis(150)).await;
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

    let deadline = Instant::now() + Duration::from_secs(15);
    let url = loop {
        let viewer_download = world
            .gql(
                &viewer,
                "query($id:UUID!,$r:UUID!){exampleReplyDownload(replyId:$id,reference:$r)}",
                serde_json::json!({ "id": reply, "r": reference }),
            )
            .await;
        if let Some(url) = ok(&viewer_download)["exampleReplyDownload"].as_str() {
            break url.to_string();
        }
        assert!(
            Instant::now() < deadline,
            "a policy-bearing kind is downloadable only once the reaper promotes it, and it never \
             promoted",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
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
