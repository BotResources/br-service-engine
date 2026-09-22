mod disposition;
mod unverified;
mod verified;

use uuid::Uuid;

use crate::harness::{World, ok};

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
