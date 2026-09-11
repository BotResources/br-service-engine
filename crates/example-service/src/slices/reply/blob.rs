use service_engine::Blobs;

pub struct Attachment;

impl Blobs for Attachment {
    const KIND: &'static str = "reply_attachment";
}
