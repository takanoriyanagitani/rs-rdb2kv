use crate::bucket::{bucket_checker_new_unchecked, Bucket};
use crate::evt::Event;

/// Creates new keys getter which uses closures to list and build select query string.
///
/// # Arguments
/// - list: Select keys.
/// - builder: Builds select query string.
pub fn list_keys_bytes_new_mut<L, B, C>(
    list: L,
    builder: B,
) -> impl Fn(&Bucket, &mut C) -> Result<Vec<Vec<u8>>, Event>
where
    L: Fn(&mut C, &str) -> Result<Vec<Vec<u8>>, Event>,
    B: Fn(&Bucket) -> Result<String, Event>,
{
    move |b: &Bucket, client: &mut C| {
        let query: String = builder(b)?;
        list(client, query.as_str())
    }
}

/// Creates checked list query builder which uses a closure to check the bucket name.
pub fn list_query_builder_checked<C>(checker: C) -> impl Fn(&Bucket) -> Result<String, Event>
where
    C: Fn(&Bucket) -> Result<(), Event>,
{
    move |b: &Bucket| {
        checker(b)?;
        Ok(format!(
            r#"
                SELECT key FROM {}
                ORDER BY key
            "#,
            b.as_str()
        ))
    }
}

/// Creates unchecked list query builder which does not check the bucket name.
pub fn list_query_builder_unchecked() -> impl Fn(&Bucket) -> Result<String, Event> {
    let checker = bucket_checker_new_unchecked();
    list_query_builder_checked(checker)
}

#[cfg(test)]
mod test_list {

    mod list_keys_bytes_new_mut {

        use crate::bucket::Bucket;
        use crate::list::{self, list_query_builder_unchecked};

        struct DummyClient {}

        #[test]
        fn test_empty() {
            let builder = list_query_builder_unchecked();
            let list_getter = |_: &mut DummyClient, _q: &str| Ok(vec![]);
            let f = list::list_keys_bytes_new_mut(list_getter, builder);
            let b: Bucket = Bucket::from(String::from("dates"));
            let mut c = DummyClient {};
            let v: Vec<_> = f(&b, &mut c).unwrap();
            assert_eq!(v.len(), 0);
        }
    }
}
