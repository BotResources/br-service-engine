use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BOARD_PREFIX: &str = "example/boards/v1/";
pub const PERSON_PREFIX: &str = "exampletwin/persons/v1/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedBoard {
    pub id: Uuid,
    pub org_id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedPerson {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
}
