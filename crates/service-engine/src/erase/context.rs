use std::ops::{Deref, DerefMut};

use crate::erase::person::PersonId;
use crate::pipeline::Ops;

pub struct Erase<'a> {
    ops: Ops<'a>,
    person: PersonId,
}

impl<'a> Erase<'a> {
    pub(crate) fn new(ops: Ops<'a>, person: PersonId) -> Self {
        Self { ops, person }
    }

    pub fn person(&self) -> PersonId {
        self.person
    }
}

impl<'a> Deref for Erase<'a> {
    type Target = Ops<'a>;

    fn deref(&self) -> &Ops<'a> {
        &self.ops
    }
}

impl DerefMut for Erase<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}
