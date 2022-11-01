use rs_rdb2kv::del::drop_bucket_mut;
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
