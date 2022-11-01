use rs_rdb2kv::evt::Event;

mod del;
mod get;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    get::select()?;
    del::remove()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
