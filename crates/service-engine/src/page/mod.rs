mod size;

use serde::{Deserialize, Serialize};

use crate::population::Population;

pub use size::{WINDOW_SIZE_INVALID_CODE, WindowSize, WindowSizeOutOfRange};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<K> {
    before: Option<K>,
    size: WindowSize,
}

impl<K> Page<K> {
    pub fn new(before: Option<K>, size: WindowSize) -> Self {
        Self { before, size }
    }

    pub fn head(size: WindowSize) -> Self {
        Self::new(None, size)
    }

    pub fn before(cursor: K, size: WindowSize) -> Self {
        Self::new(Some(cursor), size)
    }

    pub fn cursor(&self) -> Option<&K> {
        self.before.as_ref()
    }

    pub fn size(&self) -> WindowSize {
        self.size
    }

    pub fn limit(&self) -> i64 {
        self.size.limit()
    }

    pub fn population(&self, keys: Vec<K>) -> Population<K> {
        debug_assert!(
            self.size.holds(keys.len()),
            "populate returned {} keys for a page of {}: bind page.limit() as the LIMIT of the \
             page's query",
            keys.len(),
            self.size.get()
        );
        Population::Ordered {
            keys,
            open_head: self.before.is_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::session::WindowParams;

    fn size(n: u32) -> WindowSize {
        WindowSize::new(n).expect("a positive window size")
    }

    #[test]
    fn a_head_page_is_an_open_head_that_new_rows_enter() {
        let page = Page::<u32>::head(size(3));
        assert!(matches!(
            page.population(vec![9, 8, 7]),
            Population::Ordered { keys, open_head: true } if keys == [9, 8, 7]
        ));
    }

    #[test]
    fn a_page_behind_a_cursor_keeps_its_rows_and_admits_no_newer_one() {
        let page = Page::before(7_u32, size(2));
        assert_eq!(page.cursor(), Some(&7));
        assert!(matches!(
            page.population(vec![6, 5]),
            Population::Ordered { keys, open_head: false } if keys == [6, 5]
        ));
    }

    #[test]
    fn the_limit_a_populate_binds_is_the_page_size() {
        assert_eq!(Page::<u32>::head(size(40)).limit(), 40);
        assert_eq!(Page::before(1_u32, size(40)).size(), size(40));
    }

    #[test]
    fn a_page_round_trips_through_the_window_arguments_and_refuses_a_smuggled_size() {
        let page = Page::before(12_u32, size(5));
        let params = WindowParams::encode(&page).expect("a page encodes");
        assert_eq!(params.decode::<Page<u32>>().expect("a page decodes"), page);
        let smuggled = WindowParams::encode(&json!({ "before": null, "size": 0 }))
            .expect("raw arguments encode");
        assert!(smuggled.decode::<Page<u32>>().is_err());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "bind page.limit()")]
    fn a_populate_that_ignores_the_page_size_fails_in_a_debug_build() {
        let _ = Page::<u32>::head(size(2)).population(vec![3, 2, 1]);
    }
}
