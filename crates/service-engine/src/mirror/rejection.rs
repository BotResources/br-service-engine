use crate::nats::Undecodable;

pub(super) fn wire_version_reason(expected: u16, found: u16) -> String {
    format!(
        "mirror value carries wire version {found}, but this consumer is coded against version \
         {expected}; a breaking change moves the version into a new prefix"
    )
}

pub(super) fn undecodable_reason(fault: &Undecodable) -> String {
    fault.reason()
}

pub(super) fn manifest_undecodable_reason(fault: &Undecodable) -> String {
    fault.reason_for("the offer manifest")
}
