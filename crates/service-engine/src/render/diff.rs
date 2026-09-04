use crate::wire::ViewBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Appeared,
    Changed,
    Unchanged,
    Vanished,
    Absent,
}

pub fn transition(last: Option<&ViewBytes>, next: Option<&ViewBytes>) -> Transition {
    match (last, next) {
        (None, Some(_)) => Transition::Appeared,
        (Some(a), Some(b)) if a == b => Transition::Unchanged,
        (Some(_), Some(_)) => Transition::Changed,
        (Some(_), None) => Transition::Vanished,
        (None, None) => Transition::Absent,
    }
}

impl Transition {
    pub fn emits_upsert(self) -> bool {
        matches!(self, Self::Appeared | Self::Changed)
    }

    pub fn emits_remove(self) -> bool {
        matches!(self, Self::Vanished)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn view(value: impl Serialize) -> ViewBytes {
        ViewBytes::encode(&value).expect("a view encodes")
    }

    #[test]
    fn a_view_a_session_has_never_seen_is_an_upsert() {
        let next = view("a");
        assert_eq!(transition(None, Some(&next)), Transition::Appeared);
        assert!(transition(None, Some(&next)).emits_upsert());
    }

    #[test]
    fn a_view_that_changed_is_an_upsert_and_one_that_did_not_is_nothing() {
        let last = view("a");
        let same = view("a");
        let other = view("b");
        assert_eq!(transition(Some(&last), Some(&same)), Transition::Unchanged);
        assert!(!transition(Some(&last), Some(&same)).emits_upsert());
        assert_eq!(transition(Some(&last), Some(&other)), Transition::Changed);
        assert!(transition(Some(&last), Some(&other)).emits_upsert());
    }

    #[test]
    fn a_view_that_became_invisible_is_a_remove_even_with_no_resource_mutation() {
        let last = view("a");
        assert_eq!(transition(Some(&last), None), Transition::Vanished);
        assert!(transition(Some(&last), None).emits_remove());
    }

    #[test]
    fn a_view_that_was_never_visible_and_still_is_not_emits_nothing() {
        assert_eq!(transition(None, None), Transition::Absent);
        assert!(!transition(None, None).emits_upsert());
        assert!(!transition(None, None).emits_remove());
    }
}
