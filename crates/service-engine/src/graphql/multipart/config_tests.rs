use super::{MultipartConfig, MultipartPolicy, declares_upload_scalar};

#[test]
fn the_defaults_are_documented_and_valid() {
    let config = MultipartConfig::default();
    assert_eq!(config.max_body_bytes, 16 * 1024 * 1024);
    assert_eq!(config.max_file_bytes, 8 * 1024 * 1024);
    assert_eq!(config.max_files, 4);
    assert_eq!(config.spool_dir, None);
    config.validate().expect("the defaults validate");
    assert_eq!(
        MultipartPolicy::new(&config, true).spool_dir(),
        std::env::temp_dir(),
        "no spool_dir spools to the platform temp dir, which honours TMPDIR"
    );
}

#[test]
fn a_zero_bound_or_a_part_limit_above_the_body_limit_is_refused() {
    assert!(
        MultipartConfig::default()
            .with_max_body_bytes(0)
            .validate()
            .is_err()
    );
    assert!(
        MultipartConfig::default()
            .with_max_file_bytes(0)
            .validate()
            .is_err()
    );
    assert!(
        MultipartConfig::default()
            .with_max_body_bytes(10)
            .with_max_file_bytes(11)
            .validate()
            .is_err()
    );
    MultipartConfig::default()
        .with_max_files(0)
        .validate()
        .expect("zero files is a valid policy: multipart without uploads");
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
