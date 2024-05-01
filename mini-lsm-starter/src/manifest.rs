use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .context("create manifest file failed.")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("open manifest failed.")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut ptr = buf.as_slice();
        let mut records = Vec::new();
        while ptr.has_remaining() {
            let len = ptr.get_u64() as usize;
            let data = &ptr[..len];
            ptr.advance(len);
            let checksum = ptr.get_u32();
            if crc32fast::hash(data) != checksum {
                bail!("manifest record checksum mismatch")
            }

            let record = serde_json::from_slice(data)?;
            records.push(record);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut data = serde_json::to_vec(&record)?;
        let len = data.len();
        let checksum = crc32fast::hash(&data);
        data.put_u32(checksum);

        file.write_all(&((len as u64).to_be_bytes()))?;
        file.write_all(&data)?;
        file.sync_all()?;
        Ok(())
    }
}
