use rs_rdb2kv::evt::Event;

mod del;
mod list;
mod select;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    select::select()?;
    del::remove()?;
    list::list()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
