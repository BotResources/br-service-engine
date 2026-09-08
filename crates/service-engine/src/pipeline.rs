use std::marker::PhantomData;

#[allow(dead_code)]
pub struct Mutation<'a> {
    engine: PhantomData<&'a mut ()>,
}

#[allow(dead_code)]
pub struct Reaction<'a> {
    engine: PhantomData<&'a mut ()>,
}

#[allow(dead_code)]
pub struct Bulk<'a> {
    engine: PhantomData<&'a mut ()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneShot<T>(pub T);

impl<T> OneShot<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}
