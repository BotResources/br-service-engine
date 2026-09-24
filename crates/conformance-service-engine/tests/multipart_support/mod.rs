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
pub const BOUNDARY: &str = "multipart-boundary";

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

pub const JSON: &str = "application/json";

pub fn multipart_type() -> String {
    format!("multipart/form-data; boundary={BOUNDARY}")
}

pub async fn stalled_body(service: &GraphqlService, passport: Option<&str>) -> Option<String> {
    let head_of_body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n\
         {{\"query\":\"{fill}",
        fill = "x".repeat(4096),
    );
    stalled_request(
        service,
        passport,
        &multipart_type(),
        head_of_body.as_bytes(),
        1024 * 1024,
        Duration::from_secs(5),
    )
    .await
}

pub async fn stalled_request(
    service: &GraphqlService,
    passport: Option<&str>,
    content_type: &str,
    head_of_body: &[u8],
    missing: usize,
    wait: Duration,
) -> Option<String> {
    let addr = service.base_url.trim_start_matches("http://").to_string();
    let declared = head_of_body.len() + missing;
    let mut head = format!(
        "POST /graphql HTTP/1.1\r\nHost: {addr}\r\n\
         Content-Type: {content_type}\r\nContent-Length: {declared}\r\n"
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
        .write_all(head_of_body)
        .await
        .expect("send the start of the body");
    read_status(&mut stream, wait).await
}

pub async fn chunked_multipart(
    service: &GraphqlService,
    passport: &str,
    body: &[u8],
) -> Option<String> {
    chunked_request(service, passport, &multipart_type(), body).await
}

pub async fn chunked_request(
    service: &GraphqlService,
    passport: &str,
    content_type: &str,
    body: &[u8],
) -> Option<String> {
    let addr = service.base_url.trim_start_matches("http://").to_string();
    let head = format!(
        "POST /graphql HTTP/1.1\r\nHost: {addr}\r\n\
         Content-Type: {content_type}\r\n\
         Transfer-Encoding: chunked\r\nx-passport: {passport}\r\n\r\n"
    );
    let mut stream = TcpStream::connect(&addr).await.expect("connect to the pod");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("send the head");
    for chunk in body.chunks(1024) {
        let mut frame = format!("{:x}\r\n", chunk.len()).into_bytes();
        frame.extend_from_slice(chunk);
        frame.extend_from_slice(b"\r\n");
        if stream.write_all(&frame).await.is_err() {
            break;
        }
    }
    let _ = stream.write_all(b"0\r\n\r\n").await;
    read_status(&mut stream, Duration::from_secs(5)).await
}

pub fn raw_upload_body(files: &[&[u8]]) -> Vec<u8> {
    let names: Vec<String> = (0..files.len()).map(|i| format!("f{i}.bin")).collect();
    let digests: Vec<String> = names
        .iter()
        .zip(files)
        .map(|(name, file)| upload_digest(name, file))
        .collect();
    let mut body = format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"operations\"\r\n\r\n{ops}\r\n\
         --{BOUNDARY}\r\nContent-Disposition: form-data; name=\"map\"\r\n\r\n{map}\r\n",
        ops = operations(&digests),
        map = map(files.len()),
    )
    .into_bytes();
    for (i, (name, file)) in names.iter().zip(files).enumerate() {
        body.extend_from_slice(
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{i}\"; \
                 filename=\"{name}\"\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(file);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn read_status(stream: &mut TcpStream, wait: Duration) -> Option<String> {
    let mut response = vec![0_u8; 2048];
    let read = tokio::time::timeout(wait, stream.read(&mut response)).await;
    match read {
        Ok(Ok(n)) if n > 0 => Some(String::from_utf8_lossy(&response[..n]).into_owned()),
        _ => None,
    }
}

pub fn code(body: &serde_json::Value) -> &serde_json::Value {
    &body["errors"][0]["extensions"]["code"]
}

pub fn spool_holds_no_named_file(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .expect("the spool directory is readable")
        .next()
        .is_none()
}

pub fn valid_passport() -> String {
    passport_for(Uuid::now_v7(), Uuid::now_v7()).to_header()
}
