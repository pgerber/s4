extern crate fallible_iterator;
extern crate rusoto_s3;
extern crate s4;

mod common;
use common::*;

use fallible_iterator::FallibleIterator;
use s4::S4;

#[test]
fn object_iteration() {
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
fn object_iteration_with_prefix() {
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
fn nth() {
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
fn count() {
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
