#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, U16_SIZE};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key is empty");

        // (key_len (2B) | key (keylen) | value_len (2B) | value (varlen)) + offset (2B)
        let extra_size = U16_SIZE + key.len() + U16_SIZE + value.len() + U16_SIZE;
        if self.approximate_size() + extra_size > self.block_size && !self.is_empty() {
            return false;
        }

        // add current offset into the offsets array
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // key overlap part len
        self.data.put_u16(overlap as u16);
        // key non-overlap part len
        self.data.put_u16((key.len() - overlap) as u16);
        // key non-overlap part
        self.data.put(&key.raw_ref()[overlap..]);
        // value len
        self.data.put_u16(value.len() as u16);
        // value content
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    fn approximate_size(&self) -> usize {
        // data (varlen) | offsets (2B * len) | num_of_elements (2B)
        self.data.len() + self.offsets.len() * U16_SIZE + U16_SIZE
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("build empty block")
        }

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

fn compute_overlap(base_key: KeySlice, key: KeySlice) -> usize {
    let mut idx = 0;
    loop {
        if idx >= base_key.len() || idx >= key.len() {
            break;
        }
        if base_key.raw_ref()[idx] != key.raw_ref()[idx] {
            break;
        }
        idx += 1;
    }
    idx
}
