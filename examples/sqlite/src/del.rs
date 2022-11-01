use rs_rdb2kv::del::{delete_key_bytes_mut, drop_bucket_mut};
use rs_rdb2kv::{bucket::Bucket, evt::Event};

use rusqlite::{params, Connection};

fn drop_builder() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                DROP TABLE IF EXISTS {}
            "#,
            b.as_str(),
        ))
    }
}

fn exec_drop_new() -> impl Fn(&mut Connection, &str) -> Result<(), Event> {
    move |c: &mut Connection, query: &str| {
        c.execute(query, params![])
            .map_err(|e| Event::UnexpectedError(format!("Unable to drop the bucket: {}", e)))
            .map(|_| ())
    }
}

fn drop_bucket(b: &Bucket, c: &mut Connection) -> Result<(), Event> {
    let remover = exec_drop_new();
    let builder = drop_builder();
    let f = drop_bucket_mut(remover, builder);
    f(b, c)
}

pub fn remove() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectionError(format!("Unable to open: {}", e)))?;
    let b: Bucket = Bucket::from(String::from(
        "data_2022_11_01_cafef00ddeadbeafface864299792458",
    ));
    drop_bucket(&b, &mut c)?;
    println!("dropped.");
    Ok(())
}

fn delete_builder() -> impl Fn(&Bucket) -> Result<String, Event> {
    move |b: &Bucket| {
        Ok(format!(
            r#"
                DELETE FROM {}
                WHERE key = ?1
            "#,
            b.as_str()
        ))
    }
}

fn sqlite_del_new() -> impl Fn(&mut Connection, &str, &[u8]) -> Result<u64, Event> {
    move |c: &mut Connection, query: &str, key: &[u8]| {
        c.execute(query, params![key])
            .map_err(|e| Event::UnexpectedError(format!("Unable to delete an item: {}", e)))
            .map(|cnt| cnt as u64)
    }
}

fn sqlite_del(b: &Bucket, key: &[u8], c: &mut Connection) -> Result<u64, Event> {
    let builder = delete_builder();
    let remover = sqlite_del_new();
    let f = delete_key_bytes_mut(remover, builder);
    f(b, key, c)
}

pub fn delete() -> Result<(), Event> {
    let mut c: Connection = Connection::open_in_memory()
        .map_err(|e| Event::ConnectionError(format!("Unable to open: {}", e)))?;

    c.execute(
        r#"
            CREATE TABLE IF NOT EXISTS dates_cafef00ddeadbeafface864299792458(
                key BLOB,
                val BLOB,
                CONSTRAINT dates_cafef00ddeadbeafface864299792458_pkc PRIMARY KEY(key)
            )
        "#,
        params![],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))?;

    c.execute(
        r#"
            INSERT INTO dates_cafef00ddeadbeafface864299792458
            VALUES(?1, ?2)
        "#,
        params![b"2022/10/31", b""],
    )
    .map_err(|e| Event::UnexpectedError(format!("Unable to create a bucket: {}", e)))?;

    let b: Bucket = Bucket::from(String::from("dates_cafef00ddeadbeafface864299792458"));
    let key: &[u8] = b"2022/10/31";
    let cnt: u64 = sqlite_del(&b, key, &mut c)?;
    println!("deleted: {}", cnt);
    Ok(())
}
