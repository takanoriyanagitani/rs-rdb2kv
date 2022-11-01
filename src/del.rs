use crate::{bucket::Bucket, evt::Event};

pub fn drop_bucket_mut<D, B, C>(
    remove: D,
    builder: B,
) -> impl Fn(&Bucket, &mut C) -> Result<(), Event>
where
    D: Fn(&mut C, &str) -> Result<(), Event>,
    B: Fn(&Bucket) -> Result<String, Event>,
{
    move |b: &Bucket, client: &mut C| {
        let query: String = builder(b)?;
        remove(client, query.as_str())
    }
}
