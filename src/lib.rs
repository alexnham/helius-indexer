pub mod backfill;
pub mod coordinator;
pub mod decoder;
pub mod live;
pub mod model;
mod reconcile;
pub mod repair;
pub mod storage;
pub mod telemetry;

use std::fmt;

/// Deliberately bounded diagnostics: untrusted payloads and paths never enter errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error(pub &'static str);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;
