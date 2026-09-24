mod bounds;
mod shape;

use std::io::Read;

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};

use super::refusal::MultipartRefusal;
use super::{MultipartConfig, MultipartPolicy, receive};
use crate::config::DEFAULT_MAX_BODY_BYTES;

const BOUNDARY: &str = "engine-boundary";
const CONTENT_TYPE: &str = "multipart/form-data; boundary=engine-boundary";
const OPERATIONS: &str =
    r#"{"query":"query($files:[Upload!]!){ok}","variables":{"files":[null,null]}}"#;

fn part(name: &str, filename: Option<&str>, content: &[u8]) -> Vec<u8> {
    let disposition = match filename {
        Some(filename) => format!("form-data; name=\"{name}\"; filename=\"{filename}\""),
        None => format!("form-data; name=\"{name}\""),
    };
    let mut part =
        format!("--{BOUNDARY}\r\nContent-Disposition: {disposition}\r\n\r\n").into_bytes();
    part.extend_from_slice(content);
    part.extend_from_slice(b"\r\n");
    part
}

fn body(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut body = parts.concat();
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn chunks(body: Vec<u8>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    stream::iter([Ok(Bytes::from(body))])
}

fn trickle(body: Vec<u8>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    let pieces: Vec<_> = body
        .chunks(37)
        .map(|piece| Ok(Bytes::copy_from_slice(piece)))
        .collect();
    stream::iter(pieces)
}

fn policy(config: MultipartConfig) -> MultipartPolicy {
    MultipartPolicy::new(&config, DEFAULT_MAX_BODY_BYTES, true)
}

fn spooled(config: MultipartConfig) -> (MultipartConfig, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a scratch spool directory");
    (config.with_spool_dir(dir.path()), dir)
}

async fn refusal(
    body: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    declared: Option<u64>,
    policy: &MultipartPolicy,
) -> MultipartRefusal {
    refusal_within(DEFAULT_MAX_BODY_BYTES, body, declared, policy).await
}

async fn refusal_within(
    max_body_bytes: u64,
    body: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    declared: Option<u64>,
    policy: &MultipartPolicy,
) -> MultipartRefusal {
    receive(CONTENT_TYPE, declared, body, max_body_bytes, policy)
        .await
        .expect_err("the request is refused")
}
