use crate::{bucket::Bucket, evt::Event};

/// A get request to get up to single value.
pub struct GetRequest<K> {
    bucket: Bucket,
    key: K,
}

impl<K> GetRequest<K> {
    /// Creates new get request for the bucket.
    pub fn new(bucket: Bucket, key: K) -> Self {
        Self { bucket, key }
    }

    /// Gets the bucket reference.
    pub fn as_bucket(&self) -> &Bucket {
        &self.bucket
    }

    /// Gets the key reference.
    pub fn as_key(&self) -> &K {
        &self.key
    }
}

/// Creates select request handler which uses closures to select and build query string.
///
/// # Arguments
/// - select: Tries to select a single value which uses mutable client object.
/// - builder: Builds select query string.
pub fn select_bytes_new_mut<S, B, C>(
    select: S,
    builder: B,
) -> impl Fn(&GetRequest<Vec<u8>>, &mut C) -> Result<Option<Vec<u8>>, Event>
where
    S: Fn(&mut C, &str, &[u8]) -> Result<Option<Vec<u8>>, Event>,
    B: Fn(&Bucket) -> Result<String, Event>,
{
    move |req: &GetRequest<Vec<u8>>, client: &mut C| {
        let b: &Bucket = req.as_bucket();
        let k: &[u8] = req.as_key();
        let query: String = builder(b)?;
        select(client, query.as_str(), k)
    }
}
