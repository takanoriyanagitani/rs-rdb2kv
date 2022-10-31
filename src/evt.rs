/// A list of request handle results.
#[derive(Debug)]
pub enum Event {
    ConnectionError(String),
    UnexpectedError(String),
}
