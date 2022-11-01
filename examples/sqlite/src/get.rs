use rs_rdb2kv::get::{select_bytes_new_mut, GetRequest};
use rs_rdb2kv::{bucket::Bucket, evt::Event};

use rusqlite::{params, Connection, OptionalExtension};

fn select_builder_sqlite() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                SELECT val FROM {}
                WHERE key = ?1
                LIMIT 1
            "#,
            b.as_str(),
        ))
    }
}

fn select_row(q: &GetRequest<Vec<u8>>, c: &mut Connection) -> Result<Option<Vec<u8>>, Event> {
    let builder = select_builder_sqlite();
    let f = select_bytes_new_mut(
        |con: &mut Connection, query: &str, key: &[u8]| {
            let r = con.query_row(query, params![key], |row| row.get(0));
            r.optional()
                .map_err(|e| Event::UnexpectedError(format!("Error getting a value: {}", e)))
        },
        builder,
    );
    f(q, c)
}

pub fn select() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectionError(format!("Unable to open: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS devices(
                key BLOB,
                val BLOB,
                CONSTRAINT devices_pkc PRIMARY KEY(key)
            )
        "#,
        params![],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create devices bucket: {}", e)))?;

    c.execute(
        r#"
            INSERT INTO devices VALUES (?1,?2)
        "#,
        params![
            String::from("cafef00d-dead-beaf-face-864299792458").into_bytes(),
            String::from("42").into_bytes(),
        ],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create devices bucket: {}", e)))?;

    let q: GetRequest<Vec<u8>> = GetRequest::new(
        Bucket::from(String::from("devices")),
        String::from("cafef00d-dead-beaf-face-864299792458").into_bytes(),
    );
    let got: Option<Vec<u8>> = select_row(&q, &mut c)?;
    let v: Vec<u8> =
        got.ok_or_else(|| Event::UnexpectedError(String::from("Unable to get a value")))?;
    let s: String = String::from_utf8(v)
        .map_err(|e| Event::UnexpectedError(format!("Unable to convert to string: {}", e)))?;
    println!("got: {}", s);
    Ok(())
}
