use crate::bucket::Bucket;
use crate::evt::Event;
use crate::item::Item;

pub struct BulkRequest<K, V> {
    bucket: Bucket,
    items: Vec<Item<K, V>>,
}

impl<K, V> BulkRequest<K, V> {
    pub fn new(bucket: Bucket, items: Vec<Item<K, V>>) -> Self {
        Self { bucket, items }
    }

    pub fn as_bucket(&self) -> &Bucket {
        &self.bucket
    }

    pub fn as_items(&self) -> &[Item<K, V>] {
        &self.items
    }
}

pub trait UpsertBuilder {
    fn build_create(&self, b: &Bucket) -> Result<String, Event>;
    fn build_upsert(&self, b: &Bucket) -> Result<String, Event>;
}

struct UpsertBuilderF<C, U> {
    create: C,
    upsert: U,
}
impl<C, U> UpsertBuilder for UpsertBuilderF<C, U>
where
    C: Fn(&Bucket) -> Result<String, Event>,
    U: Fn(&Bucket) -> Result<String, Event>,
{
    fn build_create(&self, b: &Bucket) -> Result<String, Event> {
        (self.create)(b)
    }
    fn build_upsert(&self, b: &Bucket) -> Result<String, Event> {
        (self.upsert)(b)
    }
}

pub fn upsert_builder_new<C, U>(create: C, upsert: U) -> impl UpsertBuilder
where
    C: Fn(&Bucket) -> Result<String, Event>,
    U: Fn(&Bucket) -> Result<String, Event>,
{
    UpsertBuilderF { create, upsert }
}

fn upsert_bytes_mut<C, U, T>(
    q: &BulkRequest<Vec<u8>, Vec<u8>>,
    create: &C,
    upsert: &U,
    transaction: &mut T,
    query_c: &str,
    query_u: &str,
) -> Result<u64, Event>
where
    C: Fn(&mut T, &str) -> Result<u64, Event>,
    U: Fn(&mut T, &str, &[u8], &[u8]) -> Result<u64, Event>,
{
    let cnt_c: u64 = create(transaction, query_c)?;

    let items: &[Item<Vec<u8>, Vec<u8>>] = q.as_items();
    let cnt_u: u64 = items.iter().try_fold(0, |tot, item| {
        let key: &[u8] = item.as_key();
        let val: &[u8] = item.as_val();
        upsert(transaction, query_u, key, val).map(|cnt| cnt + tot)
    })?;
    Ok(cnt_c + cnt_u)
}

fn upsert_bytes_new_mut<C, U, B, T>(
    create: C,
    upsert: U,
    builder: B,
) -> impl Fn(&BulkRequest<Vec<u8>, Vec<u8>>, &mut T) -> Result<u64, Event>
where
    C: Fn(&mut T, &str) -> Result<u64, Event>,
    U: Fn(&mut T, &str, &[u8], &[u8]) -> Result<u64, Event>,
    B: UpsertBuilder,
{
    move |req: &BulkRequest<_, _>, tx: &mut T| {
        let b: &Bucket = req.as_bucket();
        let query_c: String = builder.build_create(b)?;
        let query_u: String = builder.build_upsert(b)?;
        upsert_bytes_mut(
            req,
            &create,
            &upsert,
            tx,
            query_c.as_str(),
            query_u.as_str(),
        )
    }
}

fn upsert_bytes_all_mut<I, T, F>(mut requests: I, transaction: &mut T, f: &F) -> Result<u64, Event>
where
    I: Iterator<Item = BulkRequest<Vec<u8>, Vec<u8>>>,
    F: Fn(&BulkRequest<Vec<u8>, Vec<u8>>, &mut T) -> Result<u64, Event>,
{
    requests.try_fold(0, |tot, req| f(&req, transaction).map(|cnt| cnt + tot))
}

pub fn upsert_bytes_all_new_mut<C, U, B, I, T>(
    create: C,
    upsert: U,
    builder: B,
) -> impl Fn(I, &mut T) -> Result<u64, Event>
where
    C: Fn(&mut T, &str) -> Result<u64, Event>,
    U: Fn(&mut T, &str, &[u8], &[u8]) -> Result<u64, Event>,
    B: UpsertBuilder,
    I: Iterator<Item = BulkRequest<Vec<u8>, Vec<u8>>>,
{
    let f = upsert_bytes_new_mut(create, upsert, builder);
    move |requests: I, transaction: &mut T| upsert_bytes_all_mut(requests, transaction, &f)
}

pub fn upsert_bytes_all_new_immutable<C, U, B, I, T>(
    create: C,
    upsert: U,
    builder: B,
) -> impl Fn(I, &mut T) -> Result<u64, Event>
where
    C: Fn(&T, &str) -> Result<u64, Event>,
    U: Fn(&T, &str, &[u8], &[u8]) -> Result<u64, Event>,
    B: UpsertBuilder,
    I: Iterator<Item = BulkRequest<Vec<u8>, Vec<u8>>>,
{
    let c = move |mt: &mut T, query: &str| create(mt, query);
    let u = move |mt: &mut T, query: &str, key: &[u8], val: &[u8]| upsert(mt, query, key, val);
    upsert_bytes_all_new_mut(c, u, builder)
}

#[cfg(test)]
mod test_upsert {

    mod upsert_bytes_all_new_immutable {

        use crate::upsert::{upsert_builder_new, Bucket, BulkRequest, Item};

        struct DummyTransaction {}

        #[test]
        fn test_empty_request() {
            let c = |_t: &DummyTransaction, _q: &str| Ok(1);
            let u = |_t: &DummyTransaction, _q: &str, _key: &[u8], _val: &[u8]| Ok(1);
            let b = upsert_builder_new(
                |_: &Bucket| Ok(String::from("")),
                |_: &Bucket| Ok(String::from("")),
            );
            let f = crate::upsert::upsert_bytes_all_new_immutable(c, u, b);
            let req = vec![];
            let mut dt: DummyTransaction = DummyTransaction {};
            let cnt: u64 = f(req.into_iter(), &mut dt).unwrap();
            assert_eq!(cnt, 0);
        }

        #[test]
        fn test_single_request() {
            let c = |_t: &DummyTransaction, _q: &str| Ok(1);
            let u = |_t: &DummyTransaction, _q: &str, _key: &[u8], _val: &[u8]| Ok(1);
            let b = upsert_builder_new(
                |bkt: &Bucket| Ok(format!("CREATE TABLE {}", bkt.as_str())),
                |_bk: &Bucket| Ok(String::from("")),
            );
            let f = crate::upsert::upsert_bytes_all_new_immutable(c, u, b);
            let req = vec![BulkRequest::new(
                Bucket::from(String::from(
                    "data_2022_10_31_cafef00ddeadbeafface864299792458",
                )),
                vec![Item::new(
                    String::from("06:40:28.0Z").into_bytes(),
                    String::from(
                        r#"{
                            "timestamp": "2022-10-31T06:40:28.0Z",
                            "data": [
                            ]
                        }"#,
                    )
                    .into_bytes(),
                )],
            )];
            let mut dt: DummyTransaction = DummyTransaction {};
            let cnt: u64 = f(req.into_iter(), &mut dt).unwrap();
            assert_eq!(cnt, 2);
        }
    }
}
