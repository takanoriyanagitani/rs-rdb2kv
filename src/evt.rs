#[derive(Debug)]
pub enum Event {
    ConnectionError(String),
    UnexpectedError(String),
}
