use std::error::Error as StdError;
use std::fmt;

use async_nats::jetstream::stream::{ConsumerError, ConsumerErrorKind};
use sqlx::error::{DatabaseError, ErrorKind};

use super::describe;
use crate::error::{EngineError, RelayError};

const DENIED: &str = "permission denied for table sample_row";

#[derive(Debug)]
struct DriverError(&'static str);

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for DriverError {}

impl DatabaseError for DriverError {
    fn message(&self) -> &str {
        self.0
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        ErrorKind::Other
    }
}

fn database_error(text: &'static str) -> sqlx::Error {
    sqlx::Error::Database(Box::new(DriverError(text)))
}

#[derive(Debug, thiserror::Error)]
#[error("{label}: {source}")]
struct Labelled {
    label: &'static str,
    #[source]
    source: sqlx::Error,
}

#[derive(Debug)]
struct Layer {
    text: &'static str,
    source: Option<Box<Layer>>,
}

fn layers(texts: &[&'static str]) -> Layer {
    let (root, outer) = texts.split_last().expect("a chain has a root");
    outer.iter().rev().fold(
        Layer {
            text: root,
            source: None,
        },
        |source, text| Layer {
            text,
            source: Some(Box::new(source)),
        },
    )
}

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.text)
    }
}

impl StdError for Layer {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

#[test]
fn a_database_error_renders_the_database_text_once() {
    let error = database_error(DENIED);
    assert_eq!(
        error.to_string(),
        format!("error returned from database: {DENIED}"),
        "precondition: sqlx puts the database text in its own message"
    );
    assert_eq!(
        error.source().map(ToString::to_string).as_deref(),
        Some(DENIED),
        "precondition: sqlx also returns the database error from source()"
    );

    let described = describe(&error);

    assert_eq!(described, format!("error returned from database: {DENIED}"));
    assert_eq!(described.matches(DENIED).count(), 1);
}

#[test]
fn a_transparent_engine_wrapper_renders_the_database_text_once() {
    let engine = EngineError::Db(database_error(DENIED));
    let relayed = RelayError::Relay(Box::new(EngineError::Db(database_error(DENIED))));

    assert_eq!(
        describe(&engine),
        format!("error returned from database: {DENIED}")
    );
    assert_eq!(
        describe(&relayed),
        format!("error returned from database: {DENIED}")
    );
}

#[test]
fn a_label_over_a_database_error_keeps_the_label_and_the_text_once() {
    let error = Labelled {
        label: "claiming the relay row",
        source: database_error(DENIED),
    };

    assert_eq!(
        describe(&error),
        format!("claiming the relay row: error returned from database: {DENIED}")
    );
}

#[test]
fn an_async_nats_error_renders_its_source_once() {
    let error = ConsumerError::with_source(
        ConsumerErrorKind::Request,
        std::io::Error::other("connection reset"),
    );
    assert_eq!(
        error.to_string(),
        "request failed: connection reset",
        "precondition: async-nats puts its source in its own message"
    );

    assert_eq!(describe(&error), "request failed: connection reset");
}

#[test]
fn a_wrapper_that_repeats_its_source_verbatim_renders_it_once() {
    assert_eq!(
        describe(&layers(&["no such stream", "no such stream"])),
        "no such stream"
    );
}

#[test]
fn distinct_segments_are_all_kept_outermost_first() {
    let decoding = serde_json::from_str::<u8>("").expect_err("an empty document is no u8");
    let decoding_text = decoding.to_string();
    let engine = EngineError::Decode {
        what: "an offer",
        source: decoding,
    };

    assert_eq!(
        describe(&layers(&[
            "publishing the outbox",
            "connection reset",
            "broken pipe"
        ])),
        "publishing the outbox: connection reset: broken pipe"
    );
    assert_eq!(
        describe(&engine),
        format!("decoding an offer failed: {decoding_text}")
    );
}

#[test]
fn a_skipped_segment_still_renders_its_own_distinct_cause() {
    assert_eq!(
        describe(&layers(&[
            "relay failed: timeout",
            "timeout",
            "socket closed"
        ])),
        "relay failed: timeout: socket closed"
    );
}

#[test]
fn a_parent_that_ends_with_a_segment_mid_phrase_keeps_both() {
    assert_eq!(
        describe(&layers(&["cannot open config", "config"])),
        "cannot open config: config"
    );
    assert_eq!(
        describe(&layers(&["unknown key:config", "config"])),
        "unknown key:config: config"
    );
}
