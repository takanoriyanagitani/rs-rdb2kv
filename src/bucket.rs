pub struct Bucket {
    name: String,
}

impl Bucket {
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }
}
