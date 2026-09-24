use super::{MultipartConfig, MultipartPolicy, declares_upload_scalar};
use crate::config::DEFAULT_MAX_BODY_BYTES;

const MIB: u64 = 1024 * 1024;

fn part_bound(config: &MultipartConfig, max_body_bytes: u64) -> u64 {
    MultipartPolicy::new(config, max_body_bytes, true).max_file_bytes()
}

#[test]
fn the_defaults_are_documented() {
    let config = MultipartConfig::default();
    assert_eq!(config.max_file_bytes, None);
    assert_eq!(part_bound(&config, DEFAULT_MAX_BODY_BYTES), 8 * MIB);
    assert_eq!(config.max_files, 4);
    assert_eq!(config.spool_dir, None);
    assert_eq!(
        MultipartPolicy::new(&config, DEFAULT_MAX_BODY_BYTES, true).spool_dir(),
        std::env::temp_dir(),
        "no spool_dir spools to the platform temp dir, which honours TMPDIR"
    );
}

#[test]
fn an_unset_part_bound_follows_a_body_bound_below_its_default() {
    let config = MultipartConfig::default();

    assert_eq!(part_bound(&config, MIB), MIB);
}

#[test]
fn an_explicit_part_bound_is_kept_whatever_the_body_bound() {
    let config = MultipartConfig::default().with_max_file_bytes(2 * MIB);

    assert_eq!(part_bound(&config, DEFAULT_MAX_BODY_BYTES), 2 * MIB);
    assert_eq!(part_bound(&config, 32 * MIB), 2 * MIB);
}

#[test]
fn only_an_upload_scalar_turns_file_parts_on() {
    assert!(declares_upload_scalar(
        "type Query { a: Int }\n\nscalar Upload\n"
    ));
    assert!(!declares_upload_scalar(
        "type Query { upload(Upload: Int): Int }\nenum Kind { Upload }\ntype Upload { a: Int }\n"
    ));
    assert!(!declares_upload_scalar("scalar UploadedAt\n"));
}
