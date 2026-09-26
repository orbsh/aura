//! Remote-probe plane: code delivery base URL (ADR-0027). Split out of
//! lib.rs per ADR-0029.

use super::Realm;

impl Realm {
    pub fn with_code_base_url(mut self, url: Option<String>) -> Self {
        self.code_base_url = url;
        self
    }
}
