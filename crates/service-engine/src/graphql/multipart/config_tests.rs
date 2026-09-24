use super::{MultipartConfig, MultipartPolicy, declares_upload_scalar};

#[test]
fn the_defaults_are_documented() {
    let config = MultipartConfig::default();
    assert_eq!(config.max_file_bytes, 8 * 1024 * 1024);
    assert_eq!(config.max_files, 4);
    assert_eq!(config.spool_dir, None);
    assert_eq!(
        MultipartPolicy::new(&config, true).spool_dir(),
        std::env::temp_dir(),
        "no spool_dir spools to the platform temp dir, which honours TMPDIR"
    );
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
