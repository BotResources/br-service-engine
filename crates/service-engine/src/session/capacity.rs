use crate::error::AttachError;
use crate::name::ProjectorName;
use crate::observe::{WINDOW_KEPT, WINDOW_REFUSED, record_window_over_capacity};
use crate::session::SessionId;

pub(crate) fn admit(
    projector: &ProjectorName,
    keys_read: usize,
    capacity: usize,
) -> Result<(), AttachError> {
    if keys_read <= capacity {
        return Ok(());
    }
    record_window_over_capacity(projector, WINDOW_REFUSED);
    Err(AttachError::WindowTooLarge {
        projector: projector.clone(),
        keys_read,
        capacity,
    })
}

pub(crate) fn note_growth(
    session: SessionId,
    projector: &ProjectorName,
    previous: usize,
    next: usize,
    capacity: usize,
) {
    if previous > capacity || next <= capacity {
        return;
    }
    tracing::warn!(
        %session,
        %projector,
        size = next,
        capacity,
        "a live window grew past window_capacity; it stays open, because the capacity refuses an \
         attach and never ends a live window"
    );
    record_window_over_capacity(projector, WINDOW_KEPT);
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROJECTOR: ProjectorName = ProjectorName::from_static("capacity");

    #[test]
    fn a_window_at_capacity_is_admitted_and_one_key_more_is_refused_with_its_numbers() {
        assert!(admit(&PROJECTOR, 4, 4).is_ok());
        assert!(matches!(
            admit(&PROJECTOR, 5, 4),
            Err(AttachError::WindowTooLarge {
                keys_read: 5,
                capacity: 4,
                ..
            })
        ));
    }
}
