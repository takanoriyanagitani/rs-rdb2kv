use crate::bucket::{bucket_checker_new_unchecked, Bucket};
use crate::evt::Event;

/// Creates new remover which uses closures to delete rows and build delete query string.
///
/// # Arguments
/// - delete: Delete rows.
/// - builder: Builds delete query string.
pub fn delete_key_bytes_mut<D, B, C>(
    delete: D,
    builder: B,
) -> impl Fn(&Bucket, &[u8], &mut C) -> Result<u64, Event>
where
    D: Fn(&mut C, &str, &[u8]) -> Result<u64, Event>,
    B: Fn(&Bucket) -> Result<String, Event>,
{
    move |b: &Bucket, key: &[u8], client: &mut C| {
        let query: String = builder(b)?;
        delete(client, query.as_str(), key)
    }
}

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

#[cfg(test)]
mod test_del {

    mod drop_builder_default_unchecked {

        use crate::bucket::Bucket;
        use crate::del;

        #[test]
        fn test_short_tablename() {
            let f = del::drop_builder_default_unchecked();
            let b = Bucket::from(String::from("devices_2022_11_01"));
            let s: String = f(&b).unwrap();
            assert_eq!(s.contains("DROP"), true);
            assert_eq!(s.contains("TABLE"), true);
            assert_eq!(s.contains("IF"), true);
            assert_eq!(s.contains("EXISTS"), true);
            assert_eq!(s.contains("devices_2022_11_01"), true);
        }
    }

    mod drop_bucket_mut {
        use crate::bucket::Bucket;
        use crate::del;

        struct DummyClient {}

        #[test]
        fn test_unchecked() {
            let builder = del::drop_builder_default_unchecked();
            let remover = |_: &mut DummyClient, _q: &str| Ok(());
            let f = del::drop_bucket_mut(remover, builder);
            let b = Bucket::from(String::from("dates_cafef00ddeadbeafface86499792458"));
            let mut c = DummyClient {};
            f(&b, &mut c).unwrap();
        }
    }

    mod delete_key_bytes_mut {
        use crate::bucket::Bucket;
        use crate::del;

        struct DummyClient {}

        #[test]
        fn test_dummy() {
            let remover = |_: &mut DummyClient, _q: &str, _k: &[u8]| Ok(42);
            let builder = |_: &Bucket| Ok(String::from(""));
            let f = del::delete_key_bytes_mut(remover, builder);
            let b = Bucket::from(String::from(""));
            let k = b"";
            let mut c = DummyClient {};
            let cnt: u64 = f(&b, k, &mut c).unwrap();
            assert_eq!(cnt, 42);
        }
    }
}
