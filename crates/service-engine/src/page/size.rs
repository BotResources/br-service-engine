use std::borrow::Cow;

use async_graphql::registry::Registry;
use async_graphql::{InputType, InputValueError, InputValueResult, Value};
use serde::{Deserialize, Serialize};

use crate::graphql::CODE_EXTENSION;

pub const WINDOW_SIZE_INVALID_CODE: &str = "WINDOW_SIZE_INVALID";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct WindowSize(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a window size is a whole number from 1 to {max}, got {got}", max = WindowSize::MAX)]
pub struct WindowSizeOutOfRange {
    pub got: i64,
}

impl WindowSize {
    pub const MAX: u32 = i32::MAX as u32;

    pub fn new(size: u32) -> Result<Self, WindowSizeOutOfRange> {
        if (1..=Self::MAX).contains(&size) {
            Ok(Self(size))
        } else {
            Err(WindowSizeOutOfRange {
                got: i64::from(size),
            })
        }
    }

    pub fn get(self) -> u32 {
        self.0
    }

    pub fn limit(self) -> i64 {
        i64::from(self.0)
    }

    pub(crate) fn holds(self, len: usize) -> bool {
        u32::try_from(len).is_ok_and(|len| len <= self.0)
    }
}

impl TryFrom<u32> for WindowSize {
    type Error = WindowSizeOutOfRange;

    fn try_from(size: u32) -> Result<Self, Self::Error> {
        Self::new(size)
    }
}

impl From<WindowSize> for u32 {
    fn from(size: WindowSize) -> Self {
        size.0
    }
}

impl InputType for WindowSize {
    type RawValueType = Self;

    fn type_name() -> Cow<'static, str> {
        <i32 as InputType>::type_name()
    }

    fn create_type_info(registry: &mut Registry) -> String {
        <i32 as InputType>::create_type_info(registry)
    }

    fn parse(value: Option<Value>) -> InputValueResult<Self> {
        let raw = <i32 as InputType>::parse(value).map_err(InputValueError::propagate)?;
        u32::try_from(raw)
            .ok()
            .and_then(|size| Self::new(size).ok())
            .ok_or_else(|| {
                InputValueError::custom(WindowSizeOutOfRange {
                    got: i64::from(raw),
                })
                .with_extension(CODE_EXTENSION, WINDOW_SIZE_INVALID_CODE)
            })
    }

    fn to_value(&self) -> Value {
        Value::from(self.0)
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }
}

#[cfg(test)]
#[path = "size_tests.rs"]
mod tests;
