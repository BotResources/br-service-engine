use service_engine::ScopeManifest;

pub const BOARD_ARCHIVE: &str = "example:board_archive";
pub const CARD_ADVANCE: &str = "example:card_advance";

pub const BOARD_SCOPES: &[&str] = &[BOARD_ARCHIVE];
pub const CARD_SCOPES: &[&str] = &[CARD_ADVANCE];

pub fn manifest() -> ScopeManifest {
    let groups: &[&[&str]] = &[
        #[cfg(feature = "board")]
        BOARD_SCOPES,
        #[cfg(feature = "card")]
        CARD_SCOPES,
    ];
    ScopeManifest::of(groups)
}
