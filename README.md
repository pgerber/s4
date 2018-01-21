# S4 - Simpler Simple Storage Service

[![crates.io](https://meritbadge.herokuapp.com/s4)](https://crates.io/crates/s4)
[![Build Status](https://travis-ci.org/pgerber/s4.svg?branch=master)](https://travis-ci.org/pgerber/s4)

:warning: **This create is not yet ready for use. Most features mentioned here are not yet implemented or still very unstable**


## What is S4

S4 is attempt to provide a high-level API for S3. It is based on [Rusoto](https://www.rusoto.org/) and merely extents it's API.


## What is added that *Rusoto* itself doesn't provide

* simple one-command way to create an `S3Client`
* download object to a `Path` or `Write`
* upload object from `Path` or `Read`
* automatic multi-part upload based on object size with automatic clean-up on error
* simple way to iterate through all objects or objects with a given prefix
* …

## Implementation details

All functionality is provided by the `S4` trait which is implemented for *Rusoto*'s `S3Client`.
