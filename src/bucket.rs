use crate::evt::Event;

/// A container(-identifier) which may contain many key/value pairs.
pub struct Bucket {
    name: String,
}

impl Bucket {
    /// Gets the bucket name as `&str`.
    pub fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl From<String> for Bucket {
    fn from(name: String) -> Self {
        Self { name }
    }
}

/// Creates new bucket checker which does not check the bucket name.
pub fn bucket_checker_new_unchecked() -> impl Fn(&Bucket) -> Result<(), Event> {
    move |_: &Bucket| Ok(())
}
