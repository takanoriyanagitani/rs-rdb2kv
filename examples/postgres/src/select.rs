use std::env;

use rs_rdb2kv::{bucket::Bucket, evt::Event};

use rs_rdb2kv::get::{select_bytes_new_mut, GetRequest};

use postgres::{Client, Config, NoTls, Row};

fn pg_sel_builder() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                SELECT val FROM {}
                WHERE key = $1::BYTEA
                LIMIT 1
            "#,
            b.as_str(),
        ))
    }
}

fn row2bytes(r: &Row) -> Result<Vec<u8>, Event> {
    r.try_get(0)
        .map_err(|e| Event::UnexpectedError(format!("Unable to get bytes from a row: {}", e)))
}

fn pg_sel() -> impl Fn(&mut Client, &str, &[u8]) -> Result<Option<Vec<u8>>, Event> {
    move |c: &mut Client, query: &str, key: &[u8]| {
        let o: Option<Row> = c
            .query_opt(query, &[&key])
            .map_err(|e| Event::UnexpectedError(format!("Unable to try to get a row: {}", e)))?;
        match o {
            None => Ok(None),
            Some(row) => row2bytes(&row).map(Some),
        }
    }
}

fn sel(q: &GetRequest<Vec<u8>>, c: &mut Client) -> Result<Option<Vec<u8>>, Event> {
    let builder = pg_sel_builder();
    let getter = pg_sel();
    let req2bytes = select_bytes_new_mut(getter, builder);
    req2bytes(q, c)
}

pub fn select() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectionError(format!("Unable to connect: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS devices_2022_11_01 (
                key BYTEA,
                val BYTEA,
                CONSTRAINT devices_2022_11_01_pkc PRIMARY KEY(key)
            )
        "#,
        &[],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))?;

    c.execute(
        r#"
            INSERT INTO devices_2022_11_01 VALUES(
                $1,$2
            )
            ON CONFLICT ON CONSTRAINT devices_2022_11_01_pkc
            DO UPDATE
            SET val = EXCLUDED.val
        "#,
        &[
            &String::from("cafef00d-dead-beaf-face-864299792458").as_bytes(),
            &String::from("42").as_bytes(),
        ],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to insert: {}", e)))?;

    let q: GetRequest<_> = GetRequest::new(
        Bucket::from(String::from("devices_2022_11_01")),
        String::from("cafef00d-dead-beaf-face-864299792458").into_bytes(),
    );
    let ov: Option<_> = sel(&q, &mut c)?;
    let v: Vec<u8> =
        ov.ok_or_else(|| Event::UnexpectedError(String::from("Unable to get a value")))?;
    let s: String = String::from_utf8(v)
        .map_err(|e| Event::UnexpectedError(format!("Unable to convert to string: {}", e)))?;
    println!("selected: {}", s);

    Ok(())
}
