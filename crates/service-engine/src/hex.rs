const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Lowercase hexadecimal, two digits per byte: the one encoding the engine
/// writes for digests, signatures, subject tokens and migration checksums.
pub(crate) fn lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_byte_is_two_lowercase_digits() {
        assert_eq!(lower(&[]), "");
        assert_eq!(lower(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
    }
}
