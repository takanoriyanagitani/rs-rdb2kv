/// A key/value pair.
pub struct Item<K, V> {
    key: K,
    val: V,
}

impl<K, V> Item<K, V> {
    /// Creates new key/value pair.
    pub fn new(key: K, val: V) -> Self {
        Self { key, val }
    }

    /// Gets the key reference.
    pub fn as_key(&self) -> &K {
        &self.key
    }

    /// Gets the value reference.
    pub fn as_val(&self) -> &V {
        &self.val
    }

    /// Gets raw key/val(unpack).
    pub fn into_pair(self) -> (K, V) {
        (self.key, self.val)
    }
}
