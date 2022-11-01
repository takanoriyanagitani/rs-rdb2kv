use std::env;

use rs_rdb2kv::del::{drop_bucket_mut, drop_builder_default_unchecked};
use rs_rdb2kv::{bucket::Bucket, evt::Event};

use postgres::{Client, Config, NoTls};

fn pg_drop_builder_unchecked_new() -> impl Fn(&Bucket) -> Result<String, Event> {
    drop_builder_default_unchecked()
}

fn pg_remover_new() -> impl Fn(&mut Client, &str) -> Result<(), Event> {
    move |c: &mut Client, query: &str| {
        c.execute(query, &[])
            .map_err(|e| Event::UnexpectedError(format!("Unable to drop bucket: {}", e)))
            .map(|_| ())
    }
}

fn pg_remove(b: &Bucket, c: &mut Client) -> Result<(), Event> {
    let builder = pg_drop_builder_unchecked_new();
    let remover = pg_remover_new();
    let f = drop_bucket_mut(remover, builder);
    f(b, c)
}

pub fn remove() -> Result<(), Event> {
    let mut c: Client = Config::new()
        .host(env::var("PGHOST").unwrap().as_str())
        .dbname(env::var("PGDATABASE").unwrap().as_str())
        .user(env::var("PGUSER").unwrap().as_str())
        .password(env::var("PGPASSWORD").unwrap_or_default())
        .connect(NoTls)
        .map_err(|e| Event::ConnectionError(format!("Unable to connect: {}", e)))?;

    let b: Bucket = Bucket::from(String::from("dates_cafef00ddeadbeafface864299792458"));
    pg_remove(&b, &mut c)?;
    println!("dropped.");
    Ok(())
}
