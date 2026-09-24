use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CompositionError {
    #[error("two slices contribute the same graphql {kind} `{member}`: {first} and {second}")]
    DuplicateSchemaMember {
        kind: &'static str,
        member: String,
        first: &'static str,
        second: &'static str,
    },

    #[error(
        "the composed schema exposes the graphql root field `{member}` that no slice fragment declared"
    )]
    UndeclaredSchemaMember { member: String },

    #[error(
        "the composed schema exposes the graphql object type `{ty}` that no slice fragment owns \
         and the engine does not inject"
    )]
    UndeclaredSchemaType { ty: String },

    #[error("the composed graphql schema could not be parsed for slice verification: {detail}")]
    SchemaParse { detail: String },

    #[error("root prefix {value:?} is invalid: it {reason}")]
    RootPrefixInvalid { value: String, reason: &'static str },

    #[error(
        "a schema slice is registered but no root prefix is declared; compose_service! must set \
         `prefix =`, and a service that hand-registers fragments must call declare_root_prefix \
         before run"
    )]
    RootPrefixUndeclared,

    #[error(
        "the root prefix is declared as {first:?} and again as {second:?}; a service has one root \
         prefix"
    )]
    RootPrefixRedeclared { first: String, second: String },

    #[error(
        "slice {slice} exposes the graphql root field `{field}`, which is not under the declared \
         root prefix `{prefix}`; every root field of a service must be `<prefix><UpperName>`"
    )]
    RootFieldOutsidePrefix {
        slice: &'static str,
        field: String,
        prefix: String,
    },
}
