mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub const U16_SIZE: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let block_size = self.data.len() + self.offsets.len() * U16_SIZE + U16_SIZE;
        let mut buf = BytesMut::with_capacity(block_size);
        buf.put(self.data.as_slice());
        self.offsets.iter().for_each(|&offset| buf.put_u16(offset));
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        let num_of_elements_start_idx = len - U16_SIZE;
        let num_of_elements = (&data[len - U16_SIZE..]).get_u16();
        let offsets_start_idx = num_of_elements_start_idx - num_of_elements as usize * U16_SIZE;
        let offsets = data[offsets_start_idx..num_of_elements_start_idx]
            .chunks(U16_SIZE)
            .map(|mut chunk| chunk.get_u16())
            .collect();
        let data = data[..offsets_start_idx].to_vec();
        Block { data, offsets }
    }
}

#[cfg(test)]
mod tests;
