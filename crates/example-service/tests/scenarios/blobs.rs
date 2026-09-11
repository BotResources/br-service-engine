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
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "attachments" }),
        )
        .await);

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$b:UUID!){startReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    let response = world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!,$c:String!){attachReply(replyId:$id,name:$n,contentType:$c)}",
            serde_json::json!({ "id": reply, "n": "note.txt", "c": "text/plain" }),
        )
        .await;
    let post = &ok(&response)["attachReply"];
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
            "query($id:UUID!){reply(id:$id){hasAttachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(
        ok(&view)["reply"]["hasAttachment"],
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
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "attachments" }),
        )
        .await);

    ok(&world
        .gql(
            &viewer,
            "mutation($id:UUID!,$b:UUID!){startReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    let response = world
        .gql(
            &viewer,
            "mutation($id:UUID!,$n:String!,$c:String!){attachReply(replyId:$id,name:$n,contentType:$c)}",
            serde_json::json!({ "id": reply, "n": "secret.txt", "c": "text/plain" }),
        )
        .await;
    let post = &ok(&response)["attachReply"];
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
            "query($id:UUID!){reply(id:$id){attachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    let reference = ok(&view)["reply"]["attachment"]
        .as_str()
        .expect("the reply view carries the attachment reference")
        .to_string();

    let viewer_download = world
        .gql(
            &viewer,
            "query($id:UUID!,$r:UUID!){replyDownload(replyId:$id,reference:$r)}",
            serde_json::json!({ "id": reply, "r": reference }),
        )
        .await;
    let url = ok(&viewer_download)["replyDownload"]
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
            "query($id:UUID!){reply(id:$id){attachment}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert!(
        ok(&outsider_view)["reply"].is_null(),
        "a principal in another org cannot see the reply at all",
    );

    let outsider_download = world
        .gql(
            &outsider,
            "query($id:UUID!,$r:UUID!){replyDownload(replyId:$id,reference:$r)}",
            serde_json::json!({ "id": reply, "r": reference }),
        )
        .await;
    assert!(
        ok(&outsider_download)["replyDownload"].is_null(),
        "holding the reference is not a capability: a non-viewer is denied the download URL",
    );

    world.cleanup().await;
}
