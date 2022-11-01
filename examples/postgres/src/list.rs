use std::env;

use rs_rdb2kv::list::{list_keys_bytes_new_mut, list_query_builder_unchecked};
use rs_rdb2kv::{bucket::Bucket, evt::Event};

use postgres::{Client, Config, NoTls, Row};

fn pg_list_query_builder_new() -> impl Fn(&Bucket) -> Result<String, Event> {
    list_query_builder_unchecked()
}

fn row2bytes(r: &Row) -> Result<Vec<u8>, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get a value: {}", e)))
}

fn convert_all<I, C>(mut rows: I, converter: C) -> Result<Vec<Vec<u8>>, Event>
where
    I: Iterator<Item = Row>,
    C: Fn(&Row) -> Result<Vec<u8>, Event>,
{
    rows.try_fold(Vec::new(), |mut v, row| {
        let converted: Vec<u8> = converter(&row)?;
        v.push(converted);
        Ok(v)
    })
}

fn pg_list_new() -> impl Fn(&mut Client, &str) -> Result<Vec<Vec<u8>>, Event> {
    move |c: &mut Client, query: &str| {
        let rows: Vec<Row> = c
            .query(query, &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to get rows: {}", e)))?;
        convert_all(rows.into_iter(), row2bytes)
    }
}

fn pg_list_keys(b: &Bucket, c: &mut Client) -> Result<Vec<Vec<u8>>, Event> {
    let builder = pg_list_query_builder_new();
    let list_getter = pg_list_new();
    let f = list_keys_bytes_new_mut(list_getter, builder);
    f(b, c)
}

pub fn list() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectionError(format!("Unable to connect: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS dates_cafef00ddeadbeafface864299792458 (
                key BYTEA,
                val BYTEA,
                CONSTRAINT dates_cafef00ddeadbeafface864299792458_pkc PRIMARY KEY (key)
            )
        "#,
        &[],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))?;

    let mut inst = |key: &str, val: &str, query: &str| {
        c.execute(query, &[&key.as_bytes(), &val.as_bytes()])
            .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))
    };

    let query = r#"
        INSERT INTO dates_cafef00ddeadbeafface864299792458
        VALUES($1::BYTEA, $2::BYTEA)
        ON CONFLICT ON CONSTRAINT dates_cafef00ddeadbeafface864299792458_pkc
        DO NOTHING
    "#;

    inst("2022/10/31", "", query)?;
    inst("2022/11/01", "", query)?;
    inst("2022/11/02", "", query)?;
    inst("2022/01/01", "", query)?;

    let b: Bucket = Bucket::from(String::from("dates_cafef00ddeadbeafface864299792458"));
    let dates: Vec<Vec<u8>> = pg_list_keys(&b, &mut c)?;
    let strings = dates
        .into_iter()
        .map(|b: Vec<u8>| String::from_utf8(b).unwrap());
    for d in strings {
        println!("date: {}", d);
    }
    Ok(())
}
