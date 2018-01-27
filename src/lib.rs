#[macro_use]
extern crate derive_error;
extern crate fallible_iterator;
extern crate hyper;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

pub mod iter;
use iter::{GetObjectIter, ObjectIter};
pub mod error;
use error::{S4Error, S4Result};

use hyper::Client;
use rusoto_core::{default_tls_client, DispatchSignedRequest, Region};
use rusoto_credential::{CredentialsError, DefaultCredentialsProvider, ProvideAwsCredentials,
                        StaticProvider};
use rusoto_s3::{GetObjectOutput, GetObjectRequest, S3, S3Client};
use std::convert::AsRef;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

/// Create client using given static access/secret keys
///
/// # Panics
///
/// Panics if TLS cannot be initialized.
pub fn new_s3client_with_credentials(
    region: Region,
    access_key: String,
    secret_key: String,
) -> S3Client<StaticProvider, Client> {
    S3Client::new(
        default_tls_client().expect("failed to initialize TLS client"),
        StaticProvider::new_minimal(access_key, secret_key),
        region,
    )
}

/// Create client with default TLS client and credentials provider
///
/// # Panics
///
/// Panics if TLS cannot be initialized.
pub fn new_s3client_simple(
    region: Region,
) -> Result<S3Client<DefaultCredentialsProvider, Client>, CredentialsError> {
    Ok(S3Client::new(
        default_tls_client().expect("failed to initialize TLS client"),
        DefaultCredentialsProvider::new()?,
        region,
    ))
}

pub trait S4<P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    /// Get object and write it to file `target`
    fn object_to_file<F>(&self, source: &GetObjectRequest, target: F) -> S4Result<GetObjectOutput>
    where
        F: AsRef<Path>;

    /// Get object and write it to `target`
    fn object_to_write<W>(
        &self,
        source: &GetObjectRequest,
        target: &mut W,
    ) -> S4Result<GetObjectOutput>
    where
        W: Write;

    /// Iterator over all objects
    ///
    /// Objects are lexicographically sorted by their key.
    fn iter_objects(&self, bucket: &str) -> ObjectIter<P, D>;

    /// Iterator over objects with given `prefix`
    ///
    /// Objects are lexicographically sorted by their key.
    fn iter_objects_with_prefix(&self, bucket: &str, prefix: &str) -> ObjectIter<P, D>;

    /// Iterator over all objects; fetching objects as needed
    ///
    /// Objects are lexicographically sorted by their key.
    fn iter_get_objects(&self, bucket: &str) -> GetObjectIter<P, D>;

    /// Iterator over all objects; fetching objects as needed
    ///
    /// Objects are lexicographically sorted by their key.
    fn iter_get_objects_with_prefix(&self, bucket: &str, prefix: &str) -> GetObjectIter<P, D>;
}

impl<P, D> S4<P, D> for S3Client<P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    fn object_to_file<F>(
        &self,
        source: &GetObjectRequest,
        target: F,
    ) -> Result<GetObjectOutput, S4Error>
    where
        F: AsRef<Path>,
    {
        let mut resp = self.get_object(source)?;
        let mut body = resp.body.take().expect("no body");
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)?;
        io::copy(&mut body, &mut target)?;
        Ok(resp)
    }

    fn object_to_write<W>(
        &self,
        source: &GetObjectRequest,
        mut target: &mut W,
    ) -> Result<GetObjectOutput, S4Error>
    where
        W: Write,
    {
        let mut resp = self.get_object(source)?;
        let mut body = resp.body.take().expect("no body");
        io::copy(&mut body, &mut target)?;
        Ok(resp)
    }

    #[inline]
    fn iter_objects(&self, bucket: &str) -> ObjectIter<P, D> {
        ObjectIter::new(self, bucket, None)
    }

    #[inline]
    fn iter_objects_with_prefix(&self, bucket: &str, prefix: &str) -> ObjectIter<P, D> {
        ObjectIter::new(self, bucket, Some(prefix))
    }

    #[inline]
    fn iter_get_objects(&self, bucket: &str) -> GetObjectIter<P, D> {
        GetObjectIter::new(self, bucket, None)
    }

    #[inline]
    fn iter_get_objects_with_prefix(&self, bucket: &str, prefix: &str) -> GetObjectIter<P, D> {
        GetObjectIter::new(self, bucket, Some(prefix))
    }
}
