use reqwest::StatusCode;
use reqwest::multipart::{Form, Part};
use service_engine::UploadUrl;

pub async fn post_upload(
    client: &reqwest::Client,
    upload: UploadUrl,
    bytes: Vec<u8>,
) -> StatusCode {
    let (url, fields) = upload.into_parts();
    let mut form = Form::new();
    for (name, value) in fields {
        form = form.text(name, value);
    }
    form = form.part("file", Part::bytes(bytes).file_name("upload.bin"));
    client
        .post(url)
        .multipart(form)
        .send()
        .await
        .expect("POST the presigned upload form to object storage")
        .status()
}
