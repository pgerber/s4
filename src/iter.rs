use fallible_iterator::FallibleIterator;
use rusoto_core::DispatchSignedRequest;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_s3::{ListObjectsV2Error, ListObjectsV2Request, Object, S3, S3Client};
use std::vec::IntoIter;

/// Iterator over all objects or objects with a given prefix
pub struct ObjectIter<'a, P, D>
where
    P: 'a + ProvideAwsCredentials,
    D: 'a + DispatchSignedRequest,
{
    client: &'a S3Client<P, D>,
    request: ListObjectsV2Request,
    objects: IntoIter<Object>,
    exhausted: bool,
}

impl<'a, P, D> ObjectIter<'a, P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    pub(crate) fn new(client: &'a S3Client<P, D>, bucket: &str, prefix: Option<&str>) -> Self {
        let request = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            max_keys: Some(1000),
            prefix: prefix.map(|s| s.to_owned()),
            ..Default::default()
        };

        ObjectIter {
            client,
            request,
            objects: Vec::new().into_iter(),
            exhausted: false,
        }
    }

    fn next_objects(&mut self) -> Result<(), ListObjectsV2Error> {
        let resp = self.client.list_objects_v2(&self.request)?;
        self.objects = resp.contents.unwrap_or_else(Vec::new).into_iter();
        match resp.next_continuation_token {
            next @ Some(_) => self.request.continuation_token = next,
            None => self.exhausted = true,
        };
        Ok(())
    }
}

impl<'a, P, D> FallibleIterator for ObjectIter<'a, P, D>
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
{
    type Item = Object;
    type Error = ListObjectsV2Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let object @ Some(_) = self.objects.next() {
            return Ok(object);
        }

        if !self.exhausted {
            self.next_objects()?;
            Ok(self.objects.next())
        } else {
            Ok(None)
        }
    }

    fn nth(&mut self, mut n: usize) -> Result<Option<Self::Item>, Self::Error> {
        while self.objects.len() <= n {
            if self.exhausted {
                return Ok(None);
            }
            n -= self.objects.len();
            self.next_objects()?;
        }
        Ok(self.objects.nth(n))
    }

    fn count(mut self) -> Result<usize, Self::Error> {
        let mut count = self.objects.len();
        while !self.exhausted {
            self.next_objects()?;
            count += self.objects.len();
        }
        Ok(count)
    }
}
