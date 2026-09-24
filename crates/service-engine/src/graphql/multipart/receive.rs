use std::collections::HashMap;
use std::path::Path;

use async_graphql::{Request, UploadValue};
use bytes::Bytes;
use futures_util::Stream;
use multer::{Constraints, Field, Multipart, SizeLimit};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::MultipartPolicy;
use super::refusal::MultipartRefusal;

const OPERATIONS: &str = "operations";
const MAP: &str = "map";
const MAP_BYTES_PER_UPLOAD: u64 = 1024;

type UploadMap = HashMap<String, Vec<String>>;

pub(crate) async fn receive<S, E>(
    content_type: &str,
    declared_length: Option<u64>,
    body: S,
    max_body_bytes: u64,
    policy: &MultipartPolicy,
) -> Result<Request, MultipartRefusal>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    let boundary = multer::parse_boundary(content_type).map_err(|_| {
        MultipartRefusal::Malformed("the content type is not multipart/form-data with a boundary")
    })?;
    if let Some(length) = declared_length
        && length > max_body_bytes
    {
        return Err(MultipartRefusal::TooLarge {
            limit: max_body_bytes,
        });
    }
    let map_bytes = MAP_BYTES_PER_UPLOAD
        .saturating_mul(policy.max_files() as u64 + 1)
        .min(policy.max_file_bytes());
    let limits = SizeLimit::new()
        .whole_stream(max_body_bytes)
        .per_field(policy.max_file_bytes())
        .for_field(MAP, map_bytes);
    let mut multipart =
        Multipart::with_constraints(body, boundary, Constraints::new().size_limit(limits));

    let mut request = operations(&mut multipart).await?;
    let mut map = upload_map(&mut multipart, policy.max_files()).await?;
    while let Some(field) = multipart.next_field().await? {
        let name = field
            .name()
            .ok_or(MultipartRefusal::Malformed("a part carries no name"))?;
        let paths = map.remove(name).ok_or(MultipartRefusal::Malformed(
            "a part after `map` is not a file `map` names, or names it twice",
        ))?;
        let filename = field
            .file_name()
            .ok_or(MultipartRefusal::Malformed(
                "a file part carries no filename",
            ))?
            .to_string();
        let content_type = field.content_type().map(ToString::to_string);
        let content = spool(field, policy.spool_dir()).await?;
        let upload = UploadValue {
            filename,
            content_type,
            content,
        };
        for path in paths {
            let bound = upload
                .try_clone()
                .map_err(MultipartRefusal::SpoolUnavailable)?;
            request.set_upload(&path, bound);
        }
    }
    if !map.is_empty() {
        return Err(MultipartRefusal::Malformed(
            "`map` names a file the body does not carry",
        ));
    }
    Ok(request)
}

async fn operations(multipart: &mut Multipart<'_>) -> Result<Request, MultipartRefusal> {
    let field = named_part(multipart, OPERATIONS, "the first part must be `operations`").await?;
    let operations = field.bytes().await?;
    async_graphql::http::receive_json(operations.as_ref())
        .await
        .map_err(|_| MultipartRefusal::Malformed("`operations` is not a single GraphQL request"))
}

async fn upload_map(
    multipart: &mut Multipart<'_>,
    max_files: usize,
) -> Result<UploadMap, MultipartRefusal> {
    let field = named_part(multipart, MAP, "the second part must be `map`").await?;
    let bytes = field.bytes().await.map_err(|error| match error {
        multer::Error::FieldSizeExceeded { .. } => {
            MultipartRefusal::TooManyFiles { limit: max_files }
        }
        other => other.into(),
    })?;
    let map: UploadMap = serde_json::from_slice(&bytes).map_err(|_| {
        MultipartRefusal::Malformed("`map` is not a JSON object of file names to variable paths")
    })?;
    if map.values().any(Vec::is_empty) {
        return Err(MultipartRefusal::Malformed(
            "a `map` entry binds its file to no variable path",
        ));
    }
    if map.values().map(Vec::len).sum::<usize>() > max_files {
        return Err(MultipartRefusal::TooManyFiles { limit: max_files });
    }
    Ok(map)
}

async fn named_part<'r>(
    multipart: &mut Multipart<'r>,
    name: &str,
    out_of_order: &'static str,
) -> Result<Field<'r>, MultipartRefusal> {
    match multipart.next_field().await? {
        Some(field) if field.name() == Some(name) => Ok(field),
        _ => Err(MultipartRefusal::Malformed(out_of_order)),
    }
}

async fn spool(mut field: Field<'_>, dir: &Path) -> Result<std::fs::File, MultipartRefusal> {
    let dir = dir.to_path_buf();
    let file = tokio::task::spawn_blocking(move || tempfile::tempfile_in(dir))
        .await
        .map_err(|join| MultipartRefusal::SpoolUnavailable(std::io::Error::other(join)))?
        .map_err(MultipartRefusal::SpoolUnavailable)?;
    let mut file = tokio::fs::File::from_std(file);
    while let Some(chunk) = field.chunk().await? {
        file.write_all(&chunk)
            .await
            .map_err(MultipartRefusal::SpoolUnavailable)?;
    }
    file.flush()
        .await
        .map_err(MultipartRefusal::SpoolUnavailable)?;
    file.rewind()
        .await
        .map_err(MultipartRefusal::SpoolUnavailable)?;
    Ok(file.into_std().await)
}
