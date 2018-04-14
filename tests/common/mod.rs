#![allow(dead_code)]

extern crate futures;
extern crate rand;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

use self::futures::stream::Stream;
use self::futures::Future;
use self::rand::Rng;
use self::rusoto_core::request::DispatchSignedRequest;
use self::rusoto_core::Region;
use self::rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use self::rusoto_s3::{CreateBucketRequest, GetObjectRequest, PutObjectRequest, S3, S3Client};
use s4::new_s3client_with_credentials;

pub fn create_test_bucket() -> (S3Client<StaticProvider>, String) {
    let client = new_s3client_with_credentials(
        Region::Custom {
            name: "eu-west-1".to_owned(),
            endpoint: "http://localhost:9000".to_owned(),
        },
        "ANTN35UAENTS5UIAEATD".to_owned(),
        "TtnuieannGt2rGuie2t8Tt7urarg5nauedRndrur".to_owned(),
    );
    let bucket: String = self::rand::thread_rng()
        .gen_ascii_chars()
        .take(63)
        .collect();
    let bucket = bucket.to_lowercase();

    client
        .create_bucket(&CreateBucketRequest {
            bucket: bucket.clone(),
            ..Default::default()
        })
        .sync()
        .unwrap();

    (client, bucket)
}

pub fn put_object<P, D>(client: &S3Client<P, D>, bucket: &str, key: &str, data: Vec<u8>)
where
    P: 'static + ProvideAwsCredentials,
    D: 'static + DispatchSignedRequest,
{
    client
        .put_object(&PutObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body: Some(data),
            ..Default::default()
        })
        .sync()
        .unwrap();
}

pub fn get_body<P, D>(client: &S3Client<P, D>, bucket: &str, key: &str) -> Vec<u8>
where
    P: 'static + ProvideAwsCredentials,
    D: 'static + DispatchSignedRequest,
{
    let object = client
        .get_object(&GetObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
        .sync()
        .unwrap();
    object.body.unwrap().concat2().wait().unwrap()
}
