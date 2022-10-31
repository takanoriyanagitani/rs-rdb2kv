pub struct Bucket {
    name: String,
}

impl Bucket {
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl From<String> for Bucket {
    fn from(name: String) -> Self {
        Self { name }
    }
}
