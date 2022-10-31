pub struct Item<K, V> {
    key: K,
    val: V,
}

impl<K, V> Item<K, V> {
    pub fn new(key: K, val: V) -> Self {
        Self { key, val }
    }

    pub fn as_key(&self) -> &K {
        &self.key
    }
    pub fn as_val(&self) -> &V {
        &self.val
    }
}
