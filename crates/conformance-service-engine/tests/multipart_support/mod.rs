#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use br_core_auth::PassportHeader;
use conformance_service_engine::sample::graphql::{GraphqlService, passport_for, upload_digest};
use reqwest::multipart::{Form, Part};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

pub const VERIFY: &str = "mutation($files: [Upload!]!, $expected: [String!]!) { sampleUploadVerify(files: $files, expected: $expected) }";
pub const BOUNDARY: &str = "s241-boundary";

pub fn operations(expected: &[String]) -> String {
    let files = vec![serde_json::Value::Null; expected.len()];
    serde_json::json!({ "query": VERIFY, "variables": { "files": files, "expected": expected } })
        .to_string()
}

pub fn expected(files: &[(&str, Vec<u8>)]) -> Vec<String> {
    files
        .iter()
        .map(|(name, content)| upload_digest(name, content))
        .collect()
}

pub fn map(uploads: usize) -> String {
    let entries: serde_json::Map<String, serde_json::Value> = (0..uploads)
        .map(|i| {
            (
                i.to_string(),
                serde_json::json!([format!("variables.files.{i}")]),
            )
        })
        .collect();
    serde_json::Value::Object(entries).to_string()
}

pub fn upload_form(files: &[(&str, Vec<u8>)]) -> Form {
    let mut form = Form::new()
        .text("operations", operations(&expected(files)))
        .text("map", map(files.len()));
    for (i, (name, content)) in files.iter().enumerate() {
        form = form.part(
            i.to_string(),
            Part::bytes(content.clone()).file_name(name.to_string()),
        );
    }
    form
}

pub async fn post_multipart(
    service: &GraphqlService,
    passport: Option<&str>,
    form: Form,
) -> (u16, serde_json::Value, String) {
    let mut request = reqwest::Client::new()
        .post(service.http("/graphql"))
        .multipart(form);
    if let Some(passport) = passport {
        request = request.header("x-passport", passport);
    }
    let response = request
        .send()
        .await
        .expect("the multipart POST reaches the server");
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, json, text)
}

/// Sends the head of a multipart body whose `Content-Length` promises one more MiB — under
/// every body limit — that never comes, then waits for the status line. A pod that reads the
/// body before judging the request cannot answer until the timeout.
pub async fn stalled_upload(service: &GraphqlService, passport: Option<&str>) -> Option<String> {
    let addr = service.base_url.trim_start_matches("http://").to_string();
    let head_of_body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n{ops}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"map\"\r\n\r\n{map}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"0\"; filename=\"big.bin\"\r\n\r\n{fill}",
        ops = operations(&[upload_digest("big.bin", b"")]),
        map = map(1),
        fill = "x".repeat(4096),
    );
    let declared = head_of_body.len() + 1024 * 1024;
    let mut head = format!(
        "POST /graphql HTTP/1.1\r\nHost: {addr}\r\n\
         Content-Type: multipart/form-data; boundary={BOUNDARY}\r\nContent-Length: {declared}\r\n"
    );
    if let Some(passport) = passport {
        head.push_str(&format!("x-passport: {passport}\r\n"));
    }
    head.push_str("\r\n");

    let mut stream = TcpStream::connect(&addr).await.expect("connect to the pod");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("send the head");
    stream
        .write_all(head_of_body.as_bytes())
        .await
        .expect("send the start of the body");
    let mut response = vec![0_u8; 2048];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut response)).await;
    match read {
        Ok(Ok(n)) if n > 0 => Some(String::from_utf8_lossy(&response[..n]).into_owned()),
        _ => None,
    }
}

pub fn code(body: &serde_json::Value) -> &serde_json::Value {
    &body["errors"][0]["extensions"]["code"]
}

pub fn spool_is_empty(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .expect("the spool directory is readable")
        .next()
        .is_none()
}

pub fn valid_passport() -> String {
    passport_for(Uuid::now_v7(), Uuid::now_v7()).to_header()
}
