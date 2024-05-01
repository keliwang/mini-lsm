use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
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

            let key_len = ptr.get_u16() as usize;
            let key = Bytes::copy_from_slice(&ptr[..key_len]);
            ptr.advance(key_len);
            hasher.write_u16(key_len as u16);
            hasher.write(&key);

            let value_len = ptr.get_u16() as usize;
            let value = Bytes::copy_from_slice(&ptr[..value_len]);
            ptr.advance(value_len);
            hasher.write_u16(value_len as u16);
            hasher.write(&value);

            let checksum = ptr.get_u32();
            if checksum != hasher.finalize() {
                bail!("wal checksum mismatch")
            }

            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::with_capacity(
            key.len() + value.len() + 2 * std::mem::size_of::<u16>() + std::mem::size_of::<u32>(),
        );
        let mut hasher = crc32fast::Hasher::new();

        buf.put_u16(key.len() as u16);
        hasher.write_u16(key.len() as u16);

        buf.put_slice(key);
        hasher.write(key);

        buf.put_u16(value.len() as u16);
        hasher.write_u16(value.len() as u16);

        buf.put_slice(value);
        hasher.write(value);

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
