#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn range_overlap(
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
    table_first: KeySlice,
    table_last: KeySlice,
) -> bool {
    match user_upper {
        Bound::Included(key) if key < table_first.raw_ref() => {
            return false;
        }
        Bound::Excluded(key) if key <= table_first.raw_ref() => {
            return false;
        }
        _ => {}
    }

    match user_lower {
        Bound::Included(key) if key > table_last.raw_ref() => {
            return false;
        }
        Bound::Excluded(key) if key >= table_last.raw_ref() => {
            return false;
        }
        _ => {}
    }

    true
}

fn key_within(user_key: &[u8], table_first: KeySlice, table_last: KeySlice) -> bool {
    user_key >= table_first.raw_ref() && user_key <= table_last.raw_ref()
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;

        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(handle) = compaction_thread.take() {
            handle.join().map_err(|err| anyhow::anyhow!("{:?}", err))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(handle) = flush_thread.take() {
            handle.join().map_err(|err| anyhow::anyhow!("{:?}", err))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_current_memtable(Arc::new(MemTable::create(self.inner.next_sst_id())))?;
        }
        while !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 20));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let manifest_path = path.join("MANIFEST");
        let manifest;
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let mut memtables = BTreeSet::new();
            let (m, records) = Manifest::recover(&manifest_path)?;
            for record in records {
                match record {
                    crate::manifest::ManifestRecord::NewMemtable(sst_id) => {
                        next_sst_id = next_sst_id.max(sst_id);
                        memtables.insert(sst_id);
                    }
                    crate::manifest::ManifestRecord::Flush(sst_id) => {
                        let exist = memtables.remove(&sst_id);
                        assert!(exist, "memtable not exist");

                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    crate::manifest::ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }

            for &sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, sst_ids)| sst_ids))
            {
                println!("loading sst. sst: {}", sst_id);
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))
                        .with_context(|| format!("open sst failed. sst: {}", sst_id))?,
                )?;
                state.sstables.insert(sst_id, Arc::new(sst));
            }

            next_sst_id += 1;

            if options.enable_wal {
                for &sst_id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(sst_id, Self::path_of_wal_static(path, sst_id))?;
                    if !memtable.is_empty() {
                        println!("recover memtable from wal. id: {}", memtable.id());
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(v) = snapshot.memtable.get(key) {
            if v.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(v));
            }
        }

        for memtable in snapshot.imm_memtables.iter() {
            if let Some(v) = memtable.get(key) {
                if v.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(v));
                }
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if !key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                continue;
            }

            if let Some(bloom) = &table.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(key)) {
                    continue;
                }
            }

            let iter = SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
            l0_iters.push(Box::new(iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids {
                let table = snapshot.sstables[sst_id].clone();
                if !key_within(
                    key,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    continue;
                }
                if let Some(bloom) = &table.bloom {
                    if !bloom.may_contain(farmhash::fingerprint32(key)) {
                        continue;
                    }
                }
                level_ssts.push(table);
            }

            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }

        let sst_iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;
        if sst_iter.is_valid() && sst_iter.key().raw_ref() == key && !sst_iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(sst_iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let approximate_size;
        {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            approximate_size = state.memtable.approximate_size();
        }
        self.try_freeze_memtable(approximate_size)?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn try_freeze_memtable(&self, approximate_size: usize) -> Result<()> {
        if approximate_size >= self.options.num_memtable_limit {
            let state_lock = self.state_lock.lock();
            let state = self.state.read();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let sst_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(sst_id, self.path_of_wal(sst_id))?)
        } else {
            Arc::new(MemTable::create(sst_id))
        };

        self.freeze_current_memtable(memtable)?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(state_lock_observer, ManifestRecord::NewMemtable(sst_id))?;
        self.sync_dir()?;

        Ok(())
    }

    fn freeze_current_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut state = self.state.write();
        let mut snapshot = state.as_ref().clone();
        let prev_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        snapshot.imm_memtables.insert(0, prev_memtable.clone());
        *state = Arc::new(snapshot);

        drop(state);
        prev_memtable.sync_wal()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

        let last_memtable = {
            let guard = self.state.read();
            Arc::clone(guard.imm_memtables.last().unwrap())
        };
        let mut builder = SsTableBuilder::new(self.options.block_size);
        last_memtable.flush(&mut builder)?;
        let sst_id = last_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let memtable = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(sst_id, memtable.id());
            println!(
                "new sst flushed. sst_id: {}, size: {}",
                sst_id,
                sst.table_size()
            );
            snapshot.l0_sstables.insert(0, sst_id);
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, crate::manifest::ManifestRecord::Flush(sst_id))?;

        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if !range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                continue;
            }

            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };
            l0_iters.push(Box::new(iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for sst_id in level_sst_ids {
                let table = snapshot.sstables[sst_id].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, MergeIterator::create(level_iters))?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
