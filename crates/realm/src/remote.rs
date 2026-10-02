//! Remote-probe plane: code delivery base URL (ADR-0027). Split out of
//! lib.rs per ADR-0029.

use super::Realm;

impl Realm {
    pub fn with_code_base_url(mut self, url: Option<String>) -> Self {
        self.code_base_url = url;
        self
    }

    /// The cursor retention promise (ADR-0039 §2): one engine-wide value
    /// (never per-type — the watermark denominator is a cross-type `min`).
    pub fn with_cursor_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.cursor_ttl = ttl;
        self
    }
}
