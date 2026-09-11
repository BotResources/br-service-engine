use service_engine::accumulator::{Accumulator, ChunkSeq};
use service_engine::name::AccumulatorName;

use super::aggregate::Reply;

pub struct ReplyText;

impl ReplyText {
    pub const NAME: AccumulatorName = AccumulatorName::from_static("reply_text");
}

impl Accumulator for ReplyText {
    type Noun = Reply;
    type Chunk = String;
    type State = String;

    fn name(&self) -> AccumulatorName {
        Self::NAME
    }

    fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
        state.push_str(&chunk);
    }
}
