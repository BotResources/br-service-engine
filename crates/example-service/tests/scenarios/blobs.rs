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
