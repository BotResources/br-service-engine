mod integration;
mod published;

pub use integration::{
    CardReady, CreateCard, PersonCreated, ReplyCancelled, ReplyFinished, card_ready_coords,
    create_card_coords, person_created_coords, reply_cancelled_coords, reply_finished_coords,
};
pub use published::{BOARD_PREFIX, PERSON_PREFIX, PublishedBoard, PublishedPerson};

pub const SERVICE: &str = "example";
pub const TWIN: &str = "exampletwin";
pub const REPLY_ACCUMULATOR: &str = "reply_text";
