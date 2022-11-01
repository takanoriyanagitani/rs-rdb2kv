use rs_rdb2kv::evt::Event;

pub mod get;
pub mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    get::select()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
