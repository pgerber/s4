#[macro_use]
extern crate derive_error;
extern crate fallible_iterator;
extern crate futures;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;
extern crate tokio_io;

pub mod iter;
use iter::{GetObjectIter, ObjectIter};
pub mod error;
use error::{S4Error, S4Result};

use futures::stream::Stream;
use rusoto_core::reactor::RequestDispatcher;
use rusoto_core::{DispatchSignedRequest, Region};
use rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use rusoto_s3::{GetObjectOutput, GetObjectRequest, PutObjectOutput, PutObjectRequest, S3,
                S3Client, StreamingBody};
use std::convert::AsRef;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Create client using given static access/secret keys
pub fn new_s3client_with_credentials(
    region: Region,
    access_key: String,
    secret_key: String,
) -> S3Client<StaticProvider> {
    S3Client::new(
        RequestDispatcher::default(),
        StaticProvider::new_minimal(access_key, secret_key),
        region,
    )
}

pub trait S4<P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    /// Get object and write it to file `target`
    fn download_to_file<F>(
        &self,
        source: &GetObjectRequest,
        target: F,
    ) -> S4Result<GetObjectOutput>
    where
        F: AsRef<Path>;

    /// Upload content of file to S3
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following limitations apply:
    ///
    /// * The full content content of `source` is copied into memory.
    /// * Content is uploaded at once (no multi-part upload support).
    fn upload_from_file<F>(&self, source: F, target: PutObjectRequest) -> S4Result<PutObjectOutput>
    where
        F: AsRef<Path>;

    /// Get object and write it to `target`
    fn download<W>(&self, source: &GetObjectRequest, target: &mut W) -> S4Result<GetObjectOutput>
    where
        W: Write;

    /// Read `source` and upload it to S3
    ///
    /// # Caveats
    ///
    /// The current implementation is incomplete. For now, the following limitations apply:
    ///
    /// * The full content content of `source` is copied into memory.
    /// * Content is uploaded at once (no multi-part upload support).
    fn upload<R>(&self, source: &mut R, target: PutObjectRequest) -> S4Result<PutObjectOutput>
    where
        R: Read;

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

    /// Iterator over objects with given `prefix`; fetching objects as needed
    ///
    /// Objects are lexicographically sorted by their key.
    fn iter_get_objects_with_prefix(&self, bucket: &str, prefix: &str) -> GetObjectIter<P, D>;
}

impl<'a, P, D> S4<P, D> for S3Client<P, D>
where
    P: 'static + ProvideAwsCredentials,
    D: 'static + DispatchSignedRequest,
{
    fn download_to_file<F>(
        &self,
        source: &GetObjectRequest,
        target: F,
    ) -> Result<GetObjectOutput, S4Error>
    where
        F: AsRef<Path>,
    {
        let mut resp = self.get_object(source).sync()?;
        let mut body = resp.body.take().expect("no body");
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)?;
        copy(&mut body, &mut target)?;
        Ok(resp)
    }

    fn upload_from_file<F>(
        &self,
        source: F,
        mut target: PutObjectRequest,
    ) -> S4Result<PutObjectOutput>
    where
        F: AsRef<Path>,
    {
        let content = fs::read(source)?;
        target.body = Some(content);
        self.put_object(&target).sync().map_err(|e| e.into())
    }

    fn download<W>(
        &self,
        source: &GetObjectRequest,
        mut target: &mut W,
    ) -> S4Result<GetObjectOutput>
    where
        W: Write,
    {
        let mut resp = self.get_object(source).sync()?;
        let mut body = resp.body.take().expect("no body");
        copy(&mut body, &mut target)?;
        Ok(resp)
    }

    fn upload<R>(&self, source: &mut R, mut target: PutObjectRequest) -> S4Result<PutObjectOutput>
    where
        R: Read,
    {
        let mut content = Vec::new();
        source.read_to_end(&mut content)?;
        target.body = Some(content);
        self.put_object(&target).sync().map_err(|e| e.into())
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

fn copy<W>(src: &mut StreamingBody, dest: &mut W) -> S4Result<()>
where
    W: Write,
{
    let src = src.take(524_288).wait();
    for chunk in src {
        dest.write_all(chunk?.as_mut_slice())?;
    }
    Ok(())
}
