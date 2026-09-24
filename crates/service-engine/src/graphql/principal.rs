use br_core_auth::{Passport, PassportHeader};

use crate::principal::Principal;

pub const PASSPORT_HEADER: &str = "x-passport";

pub struct PrincipalRejected(String);

impl PrincipalRejected {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

pub trait PassportPrincipal: Principal + Sized {
    fn from_passport(passport: Passport) -> Result<Self, PrincipalRejected>;
}

pub enum AuthReject {
    Missing,
    Malformed(String),
    Rejected(String),
}

impl AuthReject {
    pub fn message(&self) -> String {
        match self {
            Self::Missing => "the X-Passport header is absent".to_string(),
            Self::Malformed(detail) => format!("the X-Passport header is malformed: {detail}"),
            Self::Rejected(detail) => format!("the passport is rejected: {detail}"),
        }
    }
}

pub(crate) fn resolve<P: PassportPrincipal>(header: Option<&str>) -> Result<P, AuthReject> {
    let header = header.ok_or(AuthReject::Missing)?;
    let passport =
        Passport::from_header(header).map_err(|error| AuthReject::Malformed(error.to_string()))?;
    P::from_passport(passport).map_err(|rejected| AuthReject::Rejected(rejected.0))
}
