use rs_rdb2kv::upsert::{
    upsert_builder_new, upsert_bytes_all_new_immutable, BulkRequest, UpsertBuilder,
};

use rs_rdb2kv::{bucket::Bucket, evt::Event, item::Item};

use rusqlite::{params, Connection, Transaction};

fn upsert_builder_sqlite() -> impl UpsertBuilder {
    upsert_builder_new(
        |b: &Bucket| {
            Ok(format!(
                r#"
                    CREATE TABLE IF NOT EXISTS {} (
                        key BLOB,
                        val BLOB,
                        CONSTRAINT {}_pkc PRIMARY KEY (key)
                    )
                "#,
                b.as_str(),
                b.as_str(),
            ))
        },
        |b: &Bucket| {
            Ok(format!(
                r#"
                    INSERT INTO {}
                    VALUES (?1, ?2)
                    ON CONFLICT (key)
                    DO UPDATE
                    SET val = excluded.val
                    WHERE {}.val <> excluded.val
                "#,
                b.as_str(),
                b.as_str(),
            ))
        },
    )
}

fn upsert_all<I>(requests: I, mut tx: Transaction) -> Result<u64, Event>
where
    I: Iterator<Item = BulkRequest<Vec<u8>, Vec<u8>>>,
{
    let f = upsert_bytes_all_new_immutable(
        |t: &Transaction, query: &str| {
            t.execute(query, params![])
                .map_err(|e| Event::UnexpectedError(format!("Unable to create bucket: {}", e)))
                .map(|cnt: usize| cnt as u64)
        },
        |t: &Transaction, query: &str, key: &[u8], val: &[u8]| {
            t.execute(query, params![key, val])
                .map_err(|e| Event::UnexpectedError(format!("Unable to upsert: {}", e)))
                .map(|cnt: usize| cnt as u64)
        },
        upsert_builder_sqlite(),
    );
    let cnt: u64 = f(requests, &mut tx)?;
    tx.commit()
        .map_err(|e| Event::UnexpectedError(format!("Unable to commit changes: {}", e)))?;
    Ok(cnt)
}

fn sub() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectionError(format!("Unable to open: {}", e)))?;
    let tx: Transaction = c
        .transaction()
        .map_err(|e| Event::UnexpectedError(format!("Unable to start transaction: {}", e)))?;
    let cnt: u64 = upsert_all(
        vec![BulkRequest::new(
            Bucket::from(String::from("devices_2022_11_01")),
            vec![Item::new(
                String::from("cafef00d-dead-beaf-face-864299792458").into_bytes(),
                String::from("").into_bytes(),
            )],
        )]
        .into_iter(),
        tx,
    )?;
    println!("upst cnt: {}", cnt);
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
