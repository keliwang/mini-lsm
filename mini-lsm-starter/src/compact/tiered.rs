use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "l0 should disabled when using tired compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // space amplification ratio trigger
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // size ratio trigger
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = size as f64 / next_level_size as f64;
            if current_size_ratio >= size_ratio_trigger && id + 2 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {}",
                    current_size_ratio * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().take(id + 2).cloned().collect(),
                    bottom_tier_included: id + 2 >= snapshot.levels.len(),
                });
            }
        }

        // reduce sorted runs
        let num_tiers_to_compact = snapshot.levels.len() - self.options.num_tiers + 2;
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_compact)
                .cloned()
                .collect(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_compact,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "l0 should disabled when using tired compaction"
        );

        let mut snapshot = snapshot.clone();
        let mut tiers_to_remove = task
            .tiers
            .iter()
            .map(|(k, v)| (*k, v))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(removed_files) = tiers_to_remove.remove(tier_id) {
                assert_eq!(removed_files, files, "files changed during compaction");
                files_to_remove.extend(removed_files.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }
            if tiers_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        assert!(tiers_to_remove.is_empty(), "some tiers not found");
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
