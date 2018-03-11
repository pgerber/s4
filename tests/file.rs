#[macro_use]
extern crate quickcheck;
extern crate rand;
extern crate rusoto_s3;
extern crate s4;
extern crate tempdir;

mod common;

use rand::Rng;
use rusoto_s3::{GetObjectError, GetObjectRequest};
use s4::S4;
use s4::error::S4Error;
use std::fs::File;
use std::io::{ErrorKind, Read};
use tempdir::TempDir;

#[test]
fn target_file_already_exists() {
    let (client, bucket) = common::create_test_bucket();
    let key = "abcd";

    common::put_object(&client, &bucket, key, vec![]);

    let result = client.object_to_file(
        &GetObjectRequest {
            bucket: bucket.clone(),
            key: key.to_owned(),
            ..Default::default()
        },
        file!(),
    );

    match result {
        Err(S4Error::IoError(ref e)) if e.kind() == ErrorKind::AlreadyExists => (),
        e => panic!("unexpected result: {:?}", e),
    }
}

#[test]
fn target_file_not_created_when_object_does_not_exist() {
    let (client, bucket) = common::create_test_bucket();
    let dir = TempDir::new("").unwrap();
    let file = dir.path().join("no_such_file");

    let result = client.object_to_file(
        &GetObjectRequest {
            bucket: bucket.clone(),
            key: "no_such_key".to_owned(),
            ..Default::default()
        },
        &file,
    );

    match result {
        Err(S4Error::GetObjectError(GetObjectError::NoSuchKey(_))) => (),
        e => panic!("unexpected result: {:?}", e),
    }
    assert!(
        !file.exists(),
        "target file created even though getting the object failed"
    );
}

quickcheck! {
    fn write_to_file(data: Vec<u8>) -> () {
        println!("{}", data.len());
        let (client, bucket) = common::create_test_bucket();
        let dir = TempDir::new("").unwrap();
        let file = dir.path().join("data");
        let key = "some_key";

        common::put_object(&client, &bucket, key, data.clone());

        let resp = client
            .object_to_file(
                &GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    ..Default::default()
                },
                &file,
            )
            .unwrap();

        assert_eq!(resp.content_length, Some(data.len() as i64));
        assert_eq!(
            File::open(&file)
                .unwrap()
                .bytes()
                .map(|b| b.unwrap())
                .collect::<Vec<_>>(),
            data
        );
    }
}

quickcheck! {
    fn write_to_write(data: Vec<u8>) -> () {
        let (client, bucket) = common::create_test_bucket();
        let key = "abc/def/ghi";
        let mut target = Vec::new();

        common::put_object(&client, &bucket, key, data.clone());

        let resp = client
            .object_to_write(
                &GetObjectRequest {
                    bucket: bucket.clone(),
                    key: key.to_owned(),
                    ..Default::default()
                },
                &mut target,
            )
            .unwrap();

        assert_eq!(resp.content_length, Some(data.len() as i64));
        assert_eq!(data, target);
    }
}

#[test]
fn write_to_write_large_object() {
    let (client, bucket) = common::create_test_bucket();
    let key = "abc/def/ghi";
    let mut data = vec![0; 104_857_601];
    rand::weak_rng().fill_bytes(data.as_mut());
    let mut target = Vec::new();

    common::put_object(&client, &bucket, key, data.clone());

    let resp = client
        .object_to_write(
            &GetObjectRequest {
                bucket: bucket.clone(),
                key: key.to_owned(),
                ..Default::default()
            },
            &mut target,
        )
        .unwrap();

    assert_eq!(resp.content_length, Some(data.len() as i64));
    assert_eq!(&data[..], &target[..]);
}
