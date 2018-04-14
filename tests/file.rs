#[macro_use]
extern crate quickcheck;
extern crate rand;
extern crate rusoto_s3;
extern crate s4;
extern crate tempdir;

mod common;

use rand::Rng;
use rusoto_s3::{GetObjectError, GetObjectRequest, PutObjectRequest};
use s4::error::S4Error;
use s4::S4;
use std::fs::File;
use std::io::{self, ErrorKind, Read};
use tempdir::TempDir;

#[test]
fn target_file_already_exists() {
    let (client, bucket) = common::create_test_bucket();
    let key = "abcd";

    common::put_object(&client, &bucket, key, vec![]);

    let result = client.download_to_file(
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

    let result = client.download_to_file(
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
    fn download_to_file(data: Vec<u8>) -> () {
        println!("{}", data.len());
        let (client, bucket) = common::create_test_bucket();
        let dir = TempDir::new("").unwrap();
        let file = dir.path().join("data");
        let key = "some_key";

        common::put_object(&client, &bucket, key, data.clone());

        let resp = client
            .download_to_file(
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
    fn download(data: Vec<u8>) -> () {
        let (client, bucket) = common::create_test_bucket();
        let key = "abc/def/ghi";
        let mut target = Vec::new();

        common::put_object(&client, &bucket, key, data.clone());

        let resp = client
            .download(
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
fn download_large_object() {
    let (client, bucket) = common::create_test_bucket();
    let key = "abc/def/ghi";
    let mut data = vec![0; 104_857_601];
    rand::weak_rng().fill_bytes(data.as_mut());
    let mut target = Vec::new();

    common::put_object(&client, &bucket, key, data.clone());

    let resp = client
        .download(
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

#[test]
fn no_object_created_when_file_cannot_be_opened_for_upload() {
    let (client, _) = common::create_test_bucket();
    let result = client.upload_from_file(
        "/no_such_file_or_directory_0V185rt1LhV2WwZdveEM",
        PutObjectRequest {
            bucket: "unused_bucket_name".to_string(),
            key: "key".to_string(),
            ..Default::default()
        },
    );
    match result {
        Err(S4Error::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => (),
        r => panic!("unexpected result: {:?}", r),
    }
}

#[test]
fn upload() {
    let (client, bucket) = common::create_test_bucket();
    let mut file = File::open(file!()).unwrap();
    let mut content = Vec::new();
    file.read_to_end(&mut content).unwrap();

    client
        .upload_from_file(
            file!(),
            PutObjectRequest {
                bucket: bucket.clone(),
                key: "from_file".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

    client
        .upload(
            &mut &content[..],
            PutObjectRequest {
                bucket: bucket.clone(),
                key: "from_read".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

    assert_eq!(common::get_body(&client, &bucket, "from_file"), content);
    assert_eq!(common::get_body(&client, &bucket, "from_read"), content);
}


quickcheck! {
    fn upload_arbitrary(body: Vec<u8>) -> bool {
        let (client, bucket) = common::create_test_bucket();
        client.upload(&mut &body[..], PutObjectRequest {
            bucket: bucket.clone(),
            key: "some_key".to_owned(),
            ..Default::default()
        }).unwrap();

        common::get_body(&client, &bucket, "some_key") == body
    }
}