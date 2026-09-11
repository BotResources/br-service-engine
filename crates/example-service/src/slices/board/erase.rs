use futures_util::future::BoxFuture;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use service_engine::impact::Dims;
use sqlx::Row;
use uuid::Uuid;

use super::aggregate::Board;
use crate::kernel::error::ReactionFault;

pub struct BoardEraser;

impl Erasable for BoardEraser {
    type Error = ReactionFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, ReactionFault>> {
        Box::pin(async move {
            let user = person.as_uuid();
            let boards: Vec<Uuid> =
                sqlx::query("SELECT board_id FROM board_member WHERE user_id = $1")
                    .bind(user)
                    .fetch_all(cx.connection())
                    .await?
                    .iter()
                    .map(|row| row.get::<Uuid, _>("board_id"))
                    .collect();
            let mut out = Erased::new();
            for board in &boards {
                cx.impact::<Board>(board, Dims::ALL)?;
                out.rows(1);
            }
            sqlx::query("DELETE FROM board_member WHERE user_id = $1")
                .bind(user)
                .execute(cx.connection())
                .await?;
            Ok(out)
        })
    }
}
