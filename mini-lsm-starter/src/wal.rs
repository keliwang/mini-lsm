use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::block::{U16_SIZE, U64_SIZE};
use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .context("create WAL file failed")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        println!("recover wal, wal: {:?}", path.as_ref());
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("recover WAL file failed")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut ptr = buf.as_slice();
        while ptr.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();

            // raw key
            let raw_key_len = ptr.get_u16() as usize;
            let raw_key = Bytes::copy_from_slice(&ptr[..raw_key_len]);
            ptr.advance(raw_key_len);
            hasher.write_u16(raw_key_len as u16);
            hasher.write(&raw_key);

            // key ts
            let ts = ptr.get_u64();
            hasher.write_u64(ts);

            // value
            let value_len = ptr.get_u16() as usize;
            let value = Bytes::copy_from_slice(&ptr[..value_len]);
            ptr.advance(value_len);
            hasher.write_u16(value_len as u16);
            hasher.write(&value);

            // checksum
            let checksum = ptr.get_u32();
            if checksum != hasher.finalize() {
                bail!("wal checksum mismatch")
            }

            skiplist.insert(KeyBytes::from_bytes_with_ts(raw_key, ts), value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::with_capacity(
            key.raw_len() + value.len() + 2 * U16_SIZE + U64_SIZE + std::mem::size_of::<u32>(),
        );
        let mut hasher = crc32fast::Hasher::new();

        // raw key
        buf.put_u16(key.key_len() as u16);
        hasher.write_u16(key.key_len() as u16);
        buf.put_slice(key.key_ref());
        hasher.write(key.key_ref());

        // key ts
        buf.put_u64(key.ts());
        hasher.write_u64(key.ts());

        // value
        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);
        buf.put_slice(value);
        hasher.write(value);

        // checksum
        buf.put_u32(hasher.finalize());
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
