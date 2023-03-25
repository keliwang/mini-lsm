use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self { a, b, use_a: false };
        iter.skip_same_key_in_b()?;
        iter.use_a = iter.should_use_a();
        Ok(iter)
    }

    fn skip_same_key_in_b(&mut self) -> Result<()> {
        if self.a.is_valid() {
            while self.b.is_valid() && self.a.key() == self.b.key() {
                self.b.next()?
            }
        }

        Ok(())
    }

    fn should_use_a(&self) -> bool {
        if !self.a.is_valid() {
            return false;
        }
        if !self.b.is_valid() {
            return true;
        }

        self.a.key() < self.b.key()
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.a.is_valid() {
            return true;
        }

        self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        self.skip_same_key_in_b()?;
        self.use_a = self.should_use_a();
        Ok(())
    }
}
