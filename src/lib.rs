extern crate hyper;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

pub mod client_builder;
pub use client_builder::S4ClientBuilder;

use rusoto_core::DispatchSignedRequest;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_s3::S3Client;

trait S4 {
    //    /// Remove bucket including ALL existing objects
    //    ///
    //    /// # Warning
    //    ///
    //    /// All object in the bucket are remove irrevocably.
    //    fn force_remove_bucket_recursively(bucket: String) {
    //
    //    }
}

//impl<D, P> S4 for S3Client<D, P>
//where
//    P: ProvideAwsCredentials,
//    D: DispatchSignedRequest,
//{
//}
