use crate::bucket::{bucket_checker_new_unchecked, Bucket};
use crate::evt::Event;

/// Creates new bucket dropper which uses closures to drop bucket and build query string.
///
/// # Arguments
/// - remove: Executes drop query.
/// - builder: Builds drop query string.
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

/// Creates default drop query string builder which uses a closure to check bucket name.
pub fn drop_builder_default_checked<C>(checker: C) -> impl Fn(&Bucket) -> Result<String, Event>
where
    C: Fn(&Bucket) -> Result<(), Event>,
{
    move |b: &Bucket| {
        checker(b)?;
        Ok(format!(
            r#"
                DROP TABLE IF EXISTS {}
            "#,
            b.as_str()
        ))
    }
}

/// Creates default drop query builder which does not check a bucket name.
pub fn drop_builder_default_unchecked() -> impl Fn(&Bucket) -> Result<String, Event> {
    let checker = bucket_checker_new_unchecked();
    drop_builder_default_checked(checker)
}
