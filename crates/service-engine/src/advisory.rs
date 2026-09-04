pub(crate) const ACCUMULATOR_STREAM: u8 = 0x01;
pub(crate) const LEADER_SLOT: u8 = 0x02;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const SEPARATOR: u8 = 0xff;

const fn mix(hash: u64, byte: u8) -> u64 {
    (hash ^ byte as u64).wrapping_mul(FNV_PRIME)
}

pub(crate) fn lock_id(domain: u8, parts: &[&[u8]]) -> i64 {
    let mut hash = mix(FNV_OFFSET, domain);
    let mut first = true;
    for part in parts {
        if !first {
            hash = mix(hash, SEPARATOR);
        }
        first = false;
        for byte in *part {
            hash = mix(hash, *byte);
        }
    }
    hash as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_input_always_yields_the_same_lock_so_two_pods_serialise_on_it() {
        assert_eq!(
            lock_id(LEADER_SLOT, &[b"kv_drain"]),
            lock_id(LEADER_SLOT, &[b"kv_drain"])
        );
        assert_eq!(
            lock_id(LEADER_SLOT, &[b"kv_drain"]),
            6_153_071_513_050_331_037
        );
    }

    #[test]
    fn two_subsystems_sharing_one_input_claim_two_different_locks() {
        assert_ne!(
            lock_id(LEADER_SLOT, &[b"tokens"]),
            lock_id(ACCUMULATOR_STREAM, &[b"tokens"])
        );
    }

    #[test]
    fn the_parts_are_separated_so_a_split_cannot_collide() {
        assert_ne!(
            lock_id(ACCUMULATOR_STREAM, &[b"tokens", b"a"]),
            lock_id(ACCUMULATOR_STREAM, &[b"token", b"sa"])
        );
    }
}
