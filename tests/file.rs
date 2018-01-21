extern crate rusoto_s3;
extern crate s4;
extern crate tempdir;

mod common;

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

#[test]
fn write_to_file() {
    let (client, bucket) = common::create_test_bucket();
    let dir = TempDir::new("").unwrap();
    let file = dir.path().join("data");
    let key = "3bytes";
    let data = &[0x00, 0x01, 0x02];

    common::put_object(&client, &bucket, key, data.to_vec());

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

    assert_eq!(resp.content_length, Some(3));
    assert_eq!(
        &File::open(&file)
            .unwrap()
            .bytes()
            .map(|b| b.unwrap())
            .collect::<Vec<_>>(),
        data
    );
}

#[test]
fn write_to_write() {
    let (client, bucket) = common::create_test_bucket();
    let key = "abc/def/ghi";
    let data = &[0x10, 0x20, 0x30, 0x40];
    let mut target = Vec::new();

    common::put_object(&client, &bucket, key, data.to_vec());

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

    assert_eq!(resp.content_length, Some(4));
    assert_eq!(data, &target.as_ref());
}
