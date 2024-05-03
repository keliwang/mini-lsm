use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|sst_id| snapshot.sstables[sst_id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlapping_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlapping_ssts.push(*sst_id);
            }
        }
        overlapping_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut actual_level_sizes = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            let level_size = snapshot.levels[i]
                .1
                .iter()
                .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size())
                .sum::<u64>() as usize;
            actual_level_sizes.push(level_size);
        }

        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let mut target_level_sizes = vec![0; self.options.max_levels];
        let mut base_level = self.options.max_levels;
        target_level_sizes[self.options.max_levels - 1] =
            actual_level_sizes[self.options.max_levels - 1].max(base_level_size_bytes);
        for i in (0..self.options.max_levels - 1).rev() {
            let next_level_size = target_level_sizes[i + 1];
            let current_level_size = next_level_size / self.options.level_size_multiplier;
            if current_level_size > base_level_size_bytes {
                target_level_sizes[i] = current_level_size;
            }
            if target_level_sizes[i] > 0 {
                base_level = i + 1;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            let priority = actual_level_sizes[i] as f64 / target_level_sizes[i] as f64;
            if priority > 1.0 {
                priorities.push((priority, i + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            println!(
                "target level sizes: {:?}, actual level sizes: {:?}, base level: {}",
                target_level_sizes
                    .iter()
                    .map(|s| format!("{:.3}MB", *s as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                actual_level_sizes
                    .iter()
                    .map(|s| format!("{:.3}MB", *s as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );

            let level = *level;
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            println!("compaction triggered by priority. priorities: {:?}, selected level: {}, selected sst: {}",
                priorities, level, selected_sst);
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_id_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_id_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            let new_upper_level_sst_ids = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|sst_id| {
                    if upper_level_sst_id_set.remove(sst_id) {
                        None
                    } else {
                        Some(*sst_id)
                    }
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_id_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_sst_ids;
        } else {
            let new_l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter_map(|sst_id| {
                    if upper_level_sst_id_set.remove(sst_id) {
                        None
                    } else {
                        Some(*sst_id)
                    }
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_id_set.is_empty());
            snapshot.l0_sstables = new_l0_sstables;
        }

        let mut new_lower_level_sst_ids = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|sst_id| {
                if lower_level_sst_id_set.remove(sst_id) {
                    None
                } else {
                    Some(*sst_id)
                }
            })
            .collect::<Vec<_>>();
        assert!(lower_level_sst_id_set.is_empty());
        new_lower_level_sst_ids.extend(output);
        new_lower_level_sst_ids.sort_by(|a, b| {
            snapshot.sstables[a]
                .first_key()
                .cmp(snapshot.sstables[b].first_key())
        });
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_sst_ids;

        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        (snapshot, files_to_remove)
    }
}
