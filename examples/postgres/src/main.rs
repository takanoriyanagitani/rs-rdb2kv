use rs_rdb2kv::evt::Event;

mod select;
mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    select::select()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
