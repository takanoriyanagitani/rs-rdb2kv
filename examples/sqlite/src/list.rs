use rs_rdb2kv::list::{list_keys_bytes_new_mut, list_query_builder_unchecked};
use rs_rdb2kv::{bucket::Bucket, evt::Event};

use rusqlite::{params, Connection, Statement};

fn sqlite_list_builder() -> impl Fn(&Bucket) -> Result<String, Event> {
    list_query_builder_unchecked()
}

fn sqlite_list_new() -> impl Fn(&mut Connection, &str) -> Result<Vec<Vec<u8>>, Event> {
    move |c: &mut Connection, query: &str| {
        let mut s: Statement = c
            .prepare(query)
            .map_err(|e| Event::UnexpectedError(format!("Unable to prepare: {}", e)))?;
        let mapd_rows = s
            .query_map(params![], |row| row.get::<usize, Vec<u8>>(0))
            .map_err(|e| Event::UnexpectedError(format!("Unable to get mapped rows: {}", e)))?;
        let rows = mapd_rows.map(|r| {
            r.map_err(|e| Event::UnexpectedError(format!("Unable to get mapd row: {}", e)))
        });
        rows.collect()
    }
}

fn sqlite_list(b: &Bucket, c: &mut Connection) -> Result<Vec<Vec<u8>>, Event> {
    let builder = sqlite_list_builder();
    let list_getter = sqlite_list_new();
    let f = list_keys_bytes_new_mut(list_getter, builder);
    f(b, c)
}

pub fn list() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectionError(format!("Unable to open: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS devices (
                key BLOB,
                val BLOB,
                CONSTRAINT devices_pkc PRIMARY KEY(key)
            )
        "#,
        params![],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create devices bucket: {}", e)))?;

    let inst = |key: &str, val: &str, query: &str| {
        c.execute(query, params![key.as_bytes(), val.as_bytes()])
            .map_err(|e| Event::UnexpectedError(format!("Unable to insert device info: {}", e)))
    };

    let inst_query = r#"
        INSERT INTO devices
        VALUES(?1, ?2)
    "#;

    inst("cafef00d-dead-beaf-face-864299792458", "", inst_query)?;
    inst("dafef00d-dead-beaf-face-864299792458", "", inst_query)?;
    inst("eafef00d-dead-beaf-face-864299792458", "", inst_query)?;
    inst("fafef00d-dead-beaf-face-864299792458", "", inst_query)?;

    let b: Bucket = Bucket::from(String::from("devices"));
    let ids: Vec<Vec<u8>> = sqlite_list(&b, &mut c)?;
    let mapd = ids
        .into_iter()
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    for id in mapd {
        println!("id: {}", id);
    }
    Ok(())
}
