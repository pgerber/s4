use hyper::Client as HttpClient;
use rusoto_core::{CredentialsError, DefaultCredentialsProviderSync, DispatchSignedRequest, Region};
use rusoto_core::request::default_tls_client;
use rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use rusoto_s3::S3Client;
use std::ops::{ Deref, DerefMut};

const DEFAULT_MULTIPART_THRESHOLD: usize = 20 * 1024 * 1024;

#[derive(Clone)]
pub struct S4ClientBuilder<P, D> {
    cred_provider: P,
    dispatch: D,
    multipart_threshold: usize,
    default_bucket: Option<String>
}

impl S4ClientBuilder<StaticProvider, HttpClient> {
    pub fn new_login(access_key: String, secret_key: String) -> Self {
        S4ClientBuilder {
            cred_provider: StaticProvider::new_minimal(access_key, secret_key),
            dispatch: default_tls_client().expect("failed to initialize TLS client"),
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            default_bucket: None,
        }
    }
}

impl S4ClientBuilder<DefaultCredentialsProviderSync, HttpClient> {
    pub fn new_simple() -> Result<Self, CredentialsError> {
        Ok(S4ClientBuilder {
            cred_provider: DefaultCredentialsProviderSync::new()?,
            dispatch: default_tls_client().expect("failed to initialize TLS client"),
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            default_bucket: None,
        })
    }
}

pub struct S4Client<P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    inner: S3Client<P, D>,
    multipart_threshold: usize,
    default_bucket: Option<String>
}

impl<D, P> S4ClientBuilder<P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    pub fn build(self, region: Region) -> S4Client<P, D> {
        S4Client {
            inner: S3Client::new(self.dispatch, self.cred_provider, region),
            multipart_threshold: self.multipart_threshold,
            default_bucket: self.default_bucket,
        }
    }

    pub fn multipart_threshold(&mut self, threshold: usize) -> &mut Self {
        self.multipart_threshold = threshold;
        self
    }

    pub fn default_bucket(&mut self, default_bucket: String) -> &mut Self {
        self.default_bucket = Some(default_bucket);
        self
    }
}

impl<D, P> Deref for S4Client<P, D> where P: ProvideAwsCredentials, D: DispatchSignedRequest {
    type Target = S3Client<P, D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<D, P> DerefMut for S4Client<P, D> where P: ProvideAwsCredentials, D: DispatchSignedRequest {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn build() {
        let mut builder = S4ClientBuilder::new_login("akey".to_owned(), "skey".to_owned());
        builder.multipart_threshold(5 * 1024 * 1024)
            .default_bucket("test123".to_owned());
        let _client = builder.build(Region::ApSouth1);
    }
}
