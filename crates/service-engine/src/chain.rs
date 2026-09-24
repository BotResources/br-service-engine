use std::error::Error as StdError;

pub fn describe(error: &dyn StdError) -> String {
    let mut parent = error.to_string();
    let mut rendered = parent.clone();
    let mut source = error.source();
    while let Some(cause) = source {
        let segment = cause.to_string();
        if !parent_already_renders(&parent, &segment) {
            rendered.push_str(": ");
            rendered.push_str(&segment);
        }
        parent = segment;
        source = cause.source();
    }
    rendered
}

fn parent_already_renders(parent: &str, segment: &str) -> bool {
    parent == segment
        || parent
            .strip_suffix(segment)
            .is_some_and(|head| head.ends_with(": "))
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod tests;
