use rusoto_s3::GetObjectError;
use std::io::Error as IoError;

pub type S4Result<T> = Result<T, S4Error>;

/// Errors returned by S4 extensions to Rusoto
#[derive(Debug, Error)]
pub enum S4Error {
    /// I/O Error
    IoError(IoError),

    /// Rusoto GetObjectError
    GetObjectError(GetObjectError),
}
