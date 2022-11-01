use rs_rdb2kv::evt::Event;

pub mod upsert;

fn sub() -> Result<(), Event> {
    upsert::upsert()?;
    Ok(())
}

fn main() {
    match sub() {
        Ok(_) => {}
        Err(e) => eprintln!("{:#?}", e),
    }
}
