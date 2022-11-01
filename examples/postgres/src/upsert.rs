use std::env;

use rs_rdb2kv::upsert::{upsert_builder_new, upsert_bytes_all_new_mut, BulkRequest, UpsertBuilder};

use rs_rdb2kv::bucket::Bucket;
use rs_rdb2kv::evt::Event;
use rs_rdb2kv::item::Item;

use postgres::{Client, Config, NoTls, Transaction};

fn pg_upsert_unchecked_new() -> impl UpsertBuilder {
    upsert_builder_new(
        |b: &Bucket| {
            Ok(format!(
                r#"
                    CREATE TABLE IF NOT EXISTS {} (
                        key BYTEA,
                        val BYTEA,
                        CONSTRAINT {}_pkc PRIMARY KEY(key)
                    )
                "#,
                b.as_str(),
                b.as_str(),
            ))
        },
        |b: &Bucket| {
            Ok(format!(
                r#"
                    INSERT INTO {} AS tgt
                    VALUES($1::BYTEA, $2::BYTEA)
                    ON CONFLICT ON CONSTRAINT {}_pkc
                    DO UPDATE
                    SET val = EXCLUDED.val
                    WHERE tgt.val <> EXCLUDED.val
                "#,
                b.as_str(),
                b.as_str(),
            ))
        },
    )
}

fn pg_upsert_all<I>(requests: I, mut t: Transaction) -> Result<u64, Event>
where
    I: Iterator<Item = BulkRequest<Vec<u8>, Vec<u8>>>,
{
    let c = |t: &mut Transaction, query: &str| {
        t.execute(query, &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create bucket: {}", e)))
    };
    let u = |t: &mut Transaction, query: &str, key: &[u8], val: &[u8]| {
        t.execute(query, &[&key, &val])
            .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
    };
    let b = pg_upsert_unchecked_new();
    let f = upsert_bytes_all_new_mut(c, u, b);
    let cnt: u64 = f(requests, &mut t)?;
    t.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

pub fn upsert() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectionError(format!("Unable to connect: {}", e)))?;
    let t: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    let req = vec![BulkRequest::new(
        Bucket::from(String::from(
            "data_2022_10_31_cafef00ddeadbeafface864299792458",
        )),
        vec![Item::new(
            String::from("07:11:04.0Z").into_bytes(),
            String::from(
                r#"{
                    "timestamp": "2022-10-31T07:11:04.0Z",
                    "data": [
                    ]
                }"#,
            )
            .into_bytes(),
        )],
    )];
    let upst_cnt: u64 = pg_upsert_all(req.into_iter(), t)?;
    println!("upserted: {}", upst_cnt);
    Ok(())
}
