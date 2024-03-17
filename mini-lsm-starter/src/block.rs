mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub const U16_SIZE: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // data section + offsets section + num_of_elements
        let block_size = self.data.len() + self.offsets.len() * U16_SIZE + U16_SIZE;
        let mut buf = Vec::with_capacity(block_size);
        buf.put(self.data.as_slice());
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let offsets_end_idx = data.len() - U16_SIZE;
        let num_of_elements = (&data[offsets_end_idx..]).get_u16() as usize;

        let data_end_idx = data.len() - U16_SIZE - num_of_elements * U16_SIZE;
        let offsets_slice = &data[data_end_idx..offsets_end_idx];
        let offsets = offsets_slice
            .chunks_exact(U16_SIZE)
            .map(|mut chunk| chunk.get_u16())
            .collect();

        Block {
            data: data[..data_end_idx].to_vec(),
            offsets,
        }
    }
}
