extern crate fallible_iterator;
extern crate rusoto_s3;
extern crate s4;

mod common;
use common::*;

use s4::error::S4Result;
use fallible_iterator::FallibleIterator;
use rusoto_s3::GetObjectOutput;
use s4::S4;
use std::io::Read;

#[test]
fn iter_objects() {
    let (client, bucket) = create_test_bucket();

    for i in (0..2003).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, vec![]);
    }

    let mut iter = client.iter_objects(&bucket);
    for i in (0..2003).map(|i| format!("{:04}", i)) {
        let object = iter.next().unwrap().unwrap();
        assert_eq!(object.key.unwrap(), i);
    }
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn iter_objects_with_prefix() {
    let (client, bucket) = create_test_bucket();

    for i in (0..1005).map(|i| format!("a/{:04}", i)) {
        put_object(&client, &bucket, &i, vec![]);
    }
    put_object(&client, &bucket, "b/1234", vec![]);

    let mut iter = client.iter_objects_with_prefix(&bucket, "a/");
    for i in (0..1005).map(|i| format!("a/{:04}", i)) {
        let object = iter.next().unwrap().unwrap();
        assert_eq!(object.key.unwrap(), i);
    }
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn iter_objects_nth() {
    let (client, bucket) = create_test_bucket();

    for i in (1..2081).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, vec![]);
    }

    let mut iter = client.iter_objects(&bucket);
    let obj = iter.nth(0).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "0001");
    let obj = iter.nth(2).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "0004");
    let obj = iter.nth(1999).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "2004");
    let obj = iter.nth(75).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "2080");
    assert!(iter.nth(0).unwrap().is_none());
    assert!(iter.nth(3).unwrap().is_none());

    let mut iter = client.iter_objects(&bucket);
    let obj = iter.nth(1000).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "1001");
    let obj = iter.nth(997).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "1999");
    let obj = iter.nth(0).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "2000");
    let obj = iter.nth(0).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "2001");

    let mut iter = client.iter_objects(&bucket);
    let obj = iter.nth(2030).unwrap().unwrap();
    assert_eq!(obj.key.unwrap(), "2031");
}

#[test]
fn iter_objects_count() {
    let (client, bucket) = create_test_bucket();

    assert_eq!(client.iter_objects(&bucket).count().unwrap(), 0);

    for i in (0..2122).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, vec![]);
    }

    assert_eq!(client.iter_objects(&bucket).count().unwrap(), 2122);

    let mut iter = client.iter_objects(&bucket);
    iter.nth(1199).unwrap().unwrap();
    assert_eq!(iter.count().unwrap(), 922);

    let mut iter = client.iter_objects(&bucket);
    iter.nth(2120).unwrap().unwrap();
    assert_eq!(iter.count().unwrap(), 1);

    let mut iter = client.iter_objects(&bucket);
    iter.nth(2121).unwrap().unwrap();
    assert_eq!(iter.count().unwrap(), 0);

    let mut iter = client.iter_objects(&bucket);
    assert!(iter.nth(2122).unwrap().is_none());
    assert_eq!(iter.count().unwrap(), 0);
}

#[test]
fn iter_objects_last() {
    let (client, bucket) = create_test_bucket();

    assert!(client.iter_objects(&bucket).last().unwrap().is_none());

    for i in (1..1000).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, vec![]);
    }

    assert_eq!(
        client
            .iter_objects(&bucket)
            .last()
            .unwrap()
            .unwrap()
            .key
            .unwrap(),
        "0999"
    );
    put_object(&client, &bucket, "1000", vec![]);
    assert_eq!(
        client
            .iter_objects(&bucket)
            .last()
            .unwrap()
            .unwrap()
            .key
            .unwrap(),
        "1000"
    );
    put_object(&client, &bucket, "1001", vec![]);
    assert_eq!(
        client
            .iter_objects(&bucket)
            .last()
            .unwrap()
            .unwrap()
            .key
            .unwrap(),
        "1001"
    );
}

#[test]
fn iter_get_objects() {
    let (client, bucket) = create_test_bucket();

    for i in (1..1004).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, i.clone().into_bytes());
    }

    let mut iter = client.iter_get_objects(&bucket);
    for i in (1..1004).map(|i| format!("{:04}", i)) {
        let obj = iter.next().unwrap().unwrap();
        let body: Vec<_> = obj.body.unwrap().bytes().map(|b| b.unwrap()).collect();
        assert_eq!(body, i.as_bytes());
    }
    assert!(iter.next().unwrap().is_none());
}

#[test]
fn iter_get_objects_nth() {
    let (client, bucket) = create_test_bucket();

    for i in (1..1003).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, i.clone().into_bytes());
    }

    let mut iter = client.iter_get_objects(&bucket);
    assert_body(iter.nth(0), b"0001");
    assert_body(iter.nth(997), b"0999");
    assert_body(iter.nth(0), b"1000");
    assert_body(iter.nth(0), b"1001");
    assert_body(iter.nth(0), b"1002");
    assert!(iter.nth(0).unwrap().is_none());
}

#[test]
fn iter_get_objects_with_prefix_count() {
    let (client, bucket) = create_test_bucket();

    put_object(&client, &bucket, "a/0020", vec![]);
    put_object(&client, &bucket, "c/0030", vec![]);
    assert_eq!(
        client
            .iter_get_objects_with_prefix(&bucket, "b/")
            .count()
            .unwrap(),
        0
    );

    for i in (0..533).map(|i| format!("b/{:04}", i)) {
        put_object(&client, &bucket, &i, i.clone().into_bytes());
    }

    assert_eq!(
        client
            .iter_get_objects_with_prefix(&bucket, "b/")
            .count()
            .unwrap(),
        533
    );
}

#[test]
fn iter_get_objects_last() {
    let (client, bucket) = create_test_bucket();

    assert!(client.iter_get_objects(&bucket).last().unwrap().is_none());

    for i in (1..1002).map(|i| format!("{:04}", i)) {
        put_object(&client, &bucket, &i, i.clone().into_bytes());
    }

    assert_body(client.iter_get_objects(&bucket).last(), b"1001");
}

fn assert_body(output: S4Result<Option<GetObjectOutput>>, expected: &[u8]) {
    let mut body = Vec::new();
    output
        .unwrap()
        .unwrap()
        .body
        .unwrap()
        .read_to_end(&mut body)
        .unwrap();
    assert_eq!(body, expected);
}
