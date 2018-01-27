use rusoto_s3::{GetObjectError, ListObjectsV2Error};
use std::io::Error as IoError;

pub type S4Result<T> = Result<T, S4Error>;

/// Errors returned by S4 extensions to Rusoto
#[derive(Debug, Error)]
pub enum S4Error {
    /// Unknown error
    #[error(no_from, non_std)]
    Other(&'static str),

    /// I/O Error
    IoError(IoError),

    /// Rusoto GetObjectError
    GetObjectError(GetObjectError),

    // Rusoto ListObjectV2Error
    ListObjectV2Error(ListObjectsV2Error),
}
